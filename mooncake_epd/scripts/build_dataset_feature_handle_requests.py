#!/usr/bin/env python3
"""Build Mooncake-backed FeatureHandle OpenAI requests from mooncake_test_dataset.

This is the dataset version of build_qwen_vl_feature_handle_request.py.  It uses
real Qwen3-VL vision encoder outputs, publishes FeatureBundles to Mooncake Store,
and writes a JSONL file where every request carries
metadata.mooncake_epd_feature_handles.  It intentionally requires qwen_vl_utils so
image resizing/grid_thw match serving's max_pixels path.
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
from typing import Any, Dict, List

import torch
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import EncoderWorker  # noqa: E402
from mooncake_epd.core.state import MooncakeFeatureBundleStore, MooncakeFeatureBundleStoreConfig  # noqa: E402
from mooncake_epd.scripts.run_vllm_serving_e2e import _convert_dataset_messages, _load_dataset_requests  # noqa: E402


def _stable_mm_hash(item: Dict[str, Any]) -> str:
    payload = {k: item.get(k) for k in sorted(item) if k not in {"detail"}}
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _pil_to_data_url(image: Image.Image, *, fmt: str = "PNG") -> str:
    buf = io.BytesIO()
    image.save(buf, format=fmt)
    payload = base64.b64encode(buf.getvalue()).decode("ascii")
    mime = "image/png" if fmt.upper() == "PNG" else "image/jpeg"
    return f"data:{mime};base64,{payload}"


def _rewrite_request_single_image_url(request: Dict[str, Any], image_url: str) -> Dict[str, Any]:
    rewritten = json.loads(json.dumps(request, ensure_ascii=False))
    replaced = 0
    for msg in list(rewritten.get("messages") or []):
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if isinstance(part, dict) and str(part.get("type")) in {"image_url", "image", "input_image"}:
                part.clear()
                part.update({"type": "image_url", "image_url": {"url": image_url}})
                replaced += 1
    if replaced != 1:
        raise RuntimeError(f"expected to rewrite exactly one image in request, rewritten={replaced}")
    return rewritten


def _request_messages_for_processor(request: Dict[str, Any]) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    for msg in list(request.get("messages") or []):
        role = str(msg.get("role", "user"))
        content_out: List[Dict[str, Any]] = []
        for part in list(msg.get("content") or []):
            if not isinstance(part, dict):
                continue
            if part.get("type") == "image_url":
                image_url = part.get("image_url") or {}
                url = image_url.get("url") if isinstance(image_url, dict) else image_url
                content_out.append({"type": "image", "image": str(url)})
            elif part.get("type") == "image":
                content_out.append(part)
            elif part.get("type") == "text":
                content_out.append({"type": "text", "text": str(part.get("text", ""))})
        messages.append({"role": role, "content": content_out})
    return messages


def _preflight_request_grid(processor: Any, request: Dict[str, Any]) -> List[List[int]]:
    # Emulate the serving-side OpenAI image_url path after we rewrite the request
    # to carry the exact processed image bytes: chat template from the outgoing
    # request, then HF processor over those bytes.  This catches FeatureHandle
    # token-count mismatches at build time instead of crashing vLLM EngineCore.
    qwen_messages = _request_messages_for_processor(request)
    text = processor.apply_chat_template(qwen_messages, tokenize=False, add_generation_prompt=True)
    image_inputs: List[Image.Image] = []
    for msg in qwen_messages:
        for part in list(msg.get("content") or []):
            if part.get("type") != "image":
                continue
            raw = str(part.get("image") or "")
            if raw.startswith("data:image") and "base64," in raw:
                _, b64 = raw.split("base64,", 1)
                image_inputs.append(Image.open(io.BytesIO(base64.b64decode(b64))).convert("RGB"))
            else:
                image_inputs.append(Image.open(raw).convert("RGB"))
    inputs = processor(text=[text], images=image_inputs, padding=True, return_tensors="pt")
    grid = inputs.get("image_grid_thw")
    if grid is None:
        raise RuntimeError("preflight processor did not produce image_grid_thw")
    return grid.detach().cpu().tolist()


def _dataset_messages_for_qwen(sample: Dict[str, Any], dataset_root: Path, *, image_max_pixels: int) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    for msg in list(sample.get("messages") or []):
        role = str(msg.get("role", "user"))
        content_out: List[Dict[str, Any]] = []
        for part in list(msg.get("content") or []):
            if not isinstance(part, dict):
                continue
            if part.get("type") == "image":
                rel = str(part.get("image") or "")
                path = dataset_root / rel
                if not path.exists():
                    raise FileNotFoundError(f"dataset image missing: {path}")
                image_part: Dict[str, Any] = {"type": "image", "image": str(path)}
                if image_max_pixels > 0:
                    image_part["max_pixels"] = int(image_max_pixels)
                content_out.append(image_part)
            elif part.get("type") == "text":
                content_out.append({"type": "text", "text": str(part.get("text", ""))})
        messages.append({"role": role, "content": content_out})
    return messages


def _image_url_items(request: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in list(request.get("messages") or []):
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if isinstance(part, dict) and str(part.get("type")) in {"image_url", "image", "input_image"}:
                out.append(part)
    return out


def build(args: argparse.Namespace) -> Dict[str, Any]:
    try:
        from qwen_vl_utils import process_vision_info  # type: ignore
        from transformers import AutoProcessor, Qwen3VLForConditionalGeneration
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("dataset FeatureHandle build requires qwen_vl_utils + transformers in venv_mooncake") from exc

    dataset_root = Path(args.dataset_root).resolve()
    entries, skipped = _load_dataset_requests(
        dataset_root=str(dataset_root),
        chat_split=str(args.dataset_chat_split),
        max_requests=int(args.max_dataset_requests),
        families=list(args.dataset_families or []),
        model=str(args.model),
        max_input_len=int(args.dataset_max_input_len),
        request_max_tokens=int(args.dataset_request_max_tokens),
        skip_oversized=bool(args.dataset_skip_oversized),
        image_max_pixels=int(args.dataset_image_max_pixels),
    )
    if len(entries) < int(args.max_dataset_requests):
        raise RuntimeError(f"insufficient dataset entries selected={len(entries)} requested={args.max_dataset_requests} skipped={len(skipped)}")

    device = torch.device(args.device)
    processor = AutoProcessor.from_pretrained(args.model)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        args.model,
        dtype=torch.bfloat16,
        device_map={"": device},
        low_cpu_mem_usage=True,
    )
    model.eval()
    encoder = EncoderWorker(model, processor, device=device)
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(
            store_id=args.mooncake_store_id,
            store_url=args.mooncake_store_url,
            config_path=args.mooncake_config,
            timeout_s=float(args.mooncake_timeout_s),
        )
    )

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    try:
        with output.open("w", encoding="utf-8") as fh:
            for idx, entry in enumerate(entries):
                request = json.loads(json.dumps(entry["request"], ensure_ascii=False))
                image_items = _image_url_items(request)
                if len(image_items) != 1:
                    raise RuntimeError(
                        f"current dataset FeatureHandle builder supports exactly one image per request; "
                        f"sample={entry['sample'].get('sample_id')} images={len(image_items)}"
                    )
                qwen_messages = _dataset_messages_for_qwen(
                    entry["sample"], dataset_root, image_max_pixels=int(args.dataset_image_max_pixels)
                )
                text = processor.apply_chat_template(qwen_messages, tokenize=False, add_generation_prompt=True)
                image_inputs, video_inputs = process_vision_info(qwen_messages)
                if len(image_inputs or []) != 1:
                    raise RuntimeError(
                        f"current dataset FeatureHandle builder supports exactly one processed image; "
                        f"sample={entry['sample'].get('sample_id')} processed_images={len(image_inputs or [])}"
                    )
                if bool(args.rewrite_request_to_processed_image):
                    request = _rewrite_request_single_image_url(request, _pil_to_data_url(image_inputs[0].convert("RGB")))
                    request["mm_processor_kwargs"] = {}
                image_items = _image_url_items(request)
                source_mm_hash = _stable_mm_hash(image_items[0])
                inputs = processor(
                    text=[text],
                    images=image_inputs,
                    videos=video_inputs,
                    padding=True,
                    return_tensors="pt",
                    max_pixels=int(args.dataset_image_max_pixels),
                )
                if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
                    raise RuntimeError(f"processor did not produce pixel_values/image_grid_thw for {entry['sample'].get('sample_id')}")
                grid_thw = inputs["image_grid_thw"].detach().cpu().tolist()
                if bool(args.preflight_serving_grid):
                    serving_grid = _preflight_request_grid(processor, request)
                    if serving_grid != grid_thw:
                        raise RuntimeError(
                            "FeatureHandle request grid mismatch before publish: "
                            f"sample={entry['sample'].get('sample_id')} encoder_grid={grid_thw} serving_grid={serving_grid}. "
                            "Enable --rewrite-request-to-processed-image or fix processor kwargs."
                        )
                else:
                    serving_grid = None
                started = time.perf_counter()
                enc_out = encoder.encode(
                    pixel_values=inputs["pixel_values"],
                    image_grid_thw=inputs["image_grid_thw"],
                    image_id=source_mm_hash,
                )
                handle = store.publish_bundle(
                    enc_out.bundle,
                    checksum=bool(args.checksum),
                    metadata={
                        "source_mm_hash": source_mm_hash,
                        "sample_id": entry["sample"].get("sample_id"),
                        "workflow_id": entry["sample"].get("workflow_id"),
                        "workload_family": entry["family"],
                        "encoder_device": str(device),
                        "encode_time_ms": enc_out.encode_time_ms,
                        "publish_backend": "mooncake",
                    },
                )
                metadata = dict(request.get("metadata") or {})
                metadata["mooncake_epd_feature_handles"] = [handle.as_control_payload()]
                metadata["feature_handle_dataset_builder"] = {
                    "source_mm_hash": source_mm_hash,
                    "encode_time_ms": enc_out.encode_time_ms,
                    "last_hidden_shape": list(enc_out.bundle.last_hidden.shape),
                    "deepstack_shapes": [list(t.shape) for _, t in enc_out.bundle.intermediates],
                    "grid_thw": grid_thw,
                    "serving_preflight_grid_thw": serving_grid,
                    "request_rewritten_to_processed_image": bool(args.rewrite_request_to_processed_image),
                    "handle_uri": handle.uri,
                }
                request["metadata"] = metadata
                row = {
                    "index": idx,
                    "request": request,
                    "sample": entry["sample"],
                    "family": entry["family"],
                    "handle_uri": handle.uri,
                    "source_mm_hash": source_mm_hash,
                    "encode_time_ms": enc_out.encode_time_ms,
                    "build_elapsed_ms": (time.perf_counter() - started) * 1000.0,
                }
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                rows.append(row)
    finally:
        store.close()
        try:
            del model
            del processor
            torch.cuda.empty_cache()
        except Exception:
            pass

    summary = {
        "output": str(output),
        "count": len(rows),
        "dataset_root": str(dataset_root),
        "dataset_chat_split": args.dataset_chat_split,
        "families": list(args.dataset_families or []),
        "skipped_count": len(skipped),
        "handle_uris": [r["handle_uri"] for r in rows],
    }
    summary_path = output.with_suffix(output.suffix + ".summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--dataset-root", required=True)
    ap.add_argument("--dataset-chat-split", default="dev-small")
    ap.add_argument("--max-dataset-requests", type=int, default=5)
    ap.add_argument("--dataset-families", nargs="+", default=["W0", "W1", "W2", "W3", "W4"])
    ap.add_argument("--dataset-max-input-len", type=int, default=4096)
    ap.add_argument("--dataset-request-max-tokens", type=int, default=32)
    ap.add_argument("--dataset-image-max-pixels", type=int, default=1_003_520)
    ap.add_argument("--dataset-skip-oversized", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--device", default="cuda:2")
    ap.add_argument("--mooncake-store-url", required=True)
    ap.add_argument("--mooncake-store-id", default="dataset-feature-store")
    ap.add_argument("--mooncake-config", default=None)
    ap.add_argument("--mooncake-timeout-s", type=float, default=30.0)
    ap.add_argument("--checksum", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument(
        "--rewrite-request-to-processed-image",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Rewrite the outgoing OpenAI image_url to the exact processed image bytes "
            "used for FeatureBundle encoding. This keeps vLLM placeholder token "
            "count identical to the external hidden states and avoids serving-time "
            "shape mismatch from independent resize rounding."
        ),
    )
    ap.add_argument(
        "--preflight-serving-grid",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run a build-time processor grid check against the final outgoing request.",
    )
    ap.add_argument("--output", required=True)
    return ap.parse_args()


def main() -> None:
    print(json.dumps(build(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
