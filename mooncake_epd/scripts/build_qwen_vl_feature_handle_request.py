#!/usr/bin/env python3
"""Build a real Qwen-VL FeatureHandle-backed OpenAI request.

The script loads the real HF Qwen3-VL model, runs the vision encoder for one
image, persists the hidden-state FeatureBundle via either file-backed local
transport or a real Mooncake Store object URI, and writes an OpenAI-compatible chat request containing
``metadata.mooncake_epd_feature_handles``.  The request can be sent through
``vllm_disagg_proxy.py --mm-prefetch-mode feature_handle --prefill-supports-feature-handles``.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import torch
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import EncoderWorker  # noqa: E402
from mooncake_epd.core.state import (  # noqa: E402
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
    publish_feature_bundle_to_dir,
)
from mooncake_epd.tests.dataset import make_image  # noqa: E402


def _data_url_for_image(image: Image.Image) -> str:
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    payload = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{payload}"


def _stable_mm_hash(item: Dict[str, Any]) -> str:
    payload = {k: item.get(k) for k in sorted(item) if k not in {"detail"}}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _load_image(args: argparse.Namespace) -> Tuple[Image.Image, str]:
    if args.image_path:
        path = Path(args.image_path).expanduser()
        return Image.open(path).convert("RGB"), path.stem
    return make_image(args.demo_image).convert("RGB"), args.demo_image


def build_request(args: argparse.Namespace) -> Dict[str, Any]:
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration

    image, image_name = _load_image(args)
    image_url_item = {"type": "image_url", "image_url": {"url": _data_url_for_image(image)}}
    source_mm_hash = _stable_mm_hash(image_url_item)

    messages_for_processor = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": image},
                {"type": "text", "text": args.prompt},
            ],
        }
    ]
    device = torch.device(args.device)
    processor = AutoProcessor.from_pretrained(args.model)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        args.model,
        dtype=torch.bfloat16,
        device_map={"": device},
        low_cpu_mem_usage=True,
    )
    model.eval()

    inputs = processor.apply_chat_template(
        messages_for_processor,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )
    if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
        raise RuntimeError("processor did not produce pixel_values/image_grid_thw")

    encoder = EncoderWorker(model, processor, device=device)
    started = time.perf_counter()
    enc_out = encoder.encode(
        pixel_values=inputs["pixel_values"],
        image_grid_thw=inputs["image_grid_thw"],
        image_id=source_mm_hash,
    )
    handle_metadata = {
        "source_mm_hash": source_mm_hash,
        "source_image_name": image_name,
        "encoder_device": str(device),
        "encode_time_ms": enc_out.encode_time_ms,
        "publish_backend": args.publish_backend,
    }
    if args.publish_backend == "file":
        handle = publish_feature_bundle_to_dir(
            enc_out.bundle,
            args.store_dir,
            checksum=bool(args.checksum),
            metadata=handle_metadata,
        )
    elif args.publish_backend == "mooncake":
        store = MooncakeFeatureBundleStore(
            MooncakeFeatureBundleStoreConfig(
                store_id=args.mooncake_store_id,
                store_url=args.mooncake_store_url,
                config_path=args.mooncake_config,
                timeout_s=float(args.mooncake_timeout_s),
            )
        )
        try:
            handle = store.publish_bundle(
                enc_out.bundle,
                checksum=bool(args.checksum),
                metadata=handle_metadata,
            )
        finally:
            store.close()
    else:
        raise ValueError(f"unsupported publish backend: {args.publish_backend}")
    total_ms = (time.perf_counter() - started) * 1000.0

    request = {
        "model": args.model,
        "messages": [
            {
                "role": "user",
                "content": [
                    image_url_item,
                    {"type": "text", "text": args.prompt},
                ],
            }
        ],
        "max_tokens": int(args.max_tokens),
        "temperature": float(args.temperature),
        "metadata": {
            "workflow_id": args.workflow_id,
            "mooncake_epd_feature_handles": [handle.as_control_payload()],
            "feature_handle_builder": {
                "source_mm_hash": source_mm_hash,
                "store_dir": str(Path(args.store_dir).expanduser()),
                "publish_backend": args.publish_backend,
                "mooncake_store_url": args.mooncake_store_url or "",
                "mooncake_store_id": args.mooncake_store_id,
                "encode_time_ms": enc_out.encode_time_ms,
                "total_build_ms": total_ms,
                "last_hidden_shape": list(enc_out.bundle.last_hidden.shape),
                "last_hidden_dtype": str(enc_out.bundle.last_hidden.dtype),
                "deepstack_layers": [
                    int(layer) for layer, _ in enc_out.bundle.intermediates
                ],
                "deepstack_shapes": [
                    list(tensor.shape) for _, tensor in enc_out.bundle.intermediates
                ],
                "grid_thw": inputs["image_grid_thw"].detach().cpu().tolist(),
            },
        },
    }
    return request


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--device", default="cuda:5")
    ap.add_argument("--store-dir", default="/tmp/mooncake_epd_feature_handle_store")
    ap.add_argument("--publish-backend", choices=["file", "mooncake"], default="file")
    ap.add_argument("--mooncake-store-url", default=None, help="Mooncake store HTTP URL, e.g. http://127.0.0.1:8089")
    ap.add_argument("--mooncake-store-id", default="mooncake-mm-store")
    ap.add_argument("--mooncake-config", default=None, help="MooncakeDistributedStore JSON config when not using HTTP store URL")
    ap.add_argument("--mooncake-timeout-s", type=float, default=30.0)
    ap.add_argument("--output", required=True)
    ap.add_argument("--prompt", default="Describe the image briefly.")
    ap.add_argument("--workflow-id", default="feature-handle-demo")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-path", default=None)
    ap.add_argument("--checksum", action=argparse.BooleanOptionalAction, default=False)
    args = ap.parse_args()

    req = build_request(args)
    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(req, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "output": str(out),
        "store_dir": str(Path(args.store_dir).expanduser()),
        "publish_backend": args.publish_backend,
        "source_mm_hash": req["metadata"]["feature_handle_builder"]["source_mm_hash"],
        "handle_uri": req["metadata"]["mooncake_epd_feature_handles"][0]["uri"],
        "last_hidden_shape": req["metadata"]["feature_handle_builder"]["last_hidden_shape"],
        "deepstack_shapes": req["metadata"]["feature_handle_builder"]["deepstack_shapes"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
