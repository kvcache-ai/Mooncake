#!/usr/bin/env python3
"""Standalone real-tensor parity check for full vs vision-only Qwen3-VL loading."""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from PIL import Image
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.epd_encoder_service import (  # noqa: E402
    EncoderServiceConfig,
    _LazyEncoder,
    _image_url_from_item,
    _iter_mm_image_items,
    _processor_inputs,
    _prompt_for_processor,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import _load_dataset_requests  # noqa: E402


def _image_from_request(request: dict) -> Image.Image:
    item = next(iter(_iter_mm_image_items(request)))
    url = str(_image_url_from_item(item) or "")
    if url.startswith("data:"):
        header, encoded = url.split(",", 1)
        payload = base64.b64decode(encoded) if ";base64" in header else encoded.encode()
    elif url.startswith("file://"):
        payload = Path(url[7:]).read_bytes()
    else:
        payload = Path(url).read_bytes()
    return Image.open(io.BytesIO(payload)).convert("RGB")


def _gpu_memory_mib(pid: int) -> int | None:
    lines = subprocess.check_output(
        [
            "nvidia-smi",
            "--query-compute-apps=pid,used_memory",
            "--format=csv,noheader,nounits",
        ],
        text=True,
    ).splitlines()
    for line in lines:
        parts = [part.strip() for part in line.split(",")]
        if len(parts) == 2 and parts[0] == str(pid):
            return int(parts[1])
    return None


def _cpu_bundle(bundle) -> dict:
    return {
        "last_hidden": bundle.last_hidden.detach().cpu(),
        "intermediates": [
            (int(layer), tensor.detach().cpu())
            for layer, tensor in bundle.intermediates
        ],
        "grid_thw": bundle.grid_thw.detach().cpu(),
    }


def _comparison(name: str, reference: torch.Tensor, actual: torch.Tensor) -> dict:
    ref = reference.float()
    got = actual.float()
    delta = (ref - got).abs()
    denom = ref.abs().clamp_min(1e-6)
    return {
        "name": name,
        "shape": list(reference.shape),
        "dtype": str(reference.dtype),
        "exact": bool(torch.equal(reference, actual)),
        "allclose_rtol_1e-2_atol_1e-2": bool(
            torch.allclose(ref, got, rtol=1e-2, atol=1e-2)
        ),
        "max_abs": float(delta.max().item()),
        "mean_abs": float(delta.mean().item()),
        "max_relative": float((delta / denom).max().item()),
        "cosine_similarity": float(
            torch.nn.functional.cosine_similarity(
                ref.reshape(1, -1),
                got.reshape(1, -1),
            ).item()
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--vision-only", action="store_true")
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    entries, _ = _load_dataset_requests(
        dataset_root=args.dataset_root,
        chat_split="dev-small",
        max_requests=1,
        families=["W0"],
        model=args.model,
        max_input_len=4096,
        request_max_tokens=32,
        skip_oversized=True,
        image_max_pixels=1003520,
    )
    entry = entries[0]
    request = entry["request"]
    image = _image_from_request(request)

    config = EncoderServiceConfig(
        model=args.model,
        device="cuda:5",
        dtype="bfloat16",
        encoder_family="qwen3_vl",
        qwen3_vision_only=bool(args.vision_only),
    )
    lazy = _LazyEncoder(config)
    load_started = time.perf_counter()
    worker = lazy._load_sync()
    load_ms = (time.perf_counter() - load_started) * 1000.0
    inputs = _processor_inputs(worker.processor, image, _prompt_for_processor(request))
    encode_started = time.perf_counter()
    output = worker.encode(
        pixel_values=inputs["pixel_values"],
        image_grid_thw=inputs["image_grid_thw"],
        image_id=str(entry["sample"].get("sample_id")),
    )
    torch.cuda.synchronize(torch.device("cuda:5"))
    encode_ms = (time.perf_counter() - encode_started) * 1000.0
    bundle = _cpu_bundle(output.bundle)

    reference_path = Path(args.reference)
    comparisons = []
    if args.vision_only:
        reference = torch.load(reference_path, map_location="cpu", weights_only=True)
        comparisons.append(
            _comparison("last_hidden", reference["last_hidden"], bundle["last_hidden"])
        )
        assert [layer for layer, _ in reference["intermediates"]] == [
            layer for layer, _ in bundle["intermediates"]
        ]
        for (layer, expected), (_actual_layer, actual) in zip(
            reference["intermediates"], bundle["intermediates"]
        ):
            comparisons.append(_comparison(f"intermediate:{layer}", expected, actual))
        comparisons.append(_comparison("grid_thw", reference["grid_thw"], bundle["grid_thw"]))
    else:
        reference_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(bundle, reference_path)

    payload = {
        "schema_version": "qwen3-vision-real-tensor-parity-v1",
        "mock": False,
        "real_model": True,
        "real_dataset": True,
        "mode": "vision_only" if args.vision_only else "full_model",
        "sample_id": entry["sample"].get("sample_id"),
        "load_ms": load_ms,
        "encode_sync_ms": encode_ms,
        "gpu_process_memory_mib": _gpu_memory_mib(os.getpid()),
        "encoder_runtime": lazy.stats(),
        "reference": str(reference_path),
        "comparisons": comparisons,
        "allclose": bool(comparisons) and all(
            item["allclose_rtol_1e-2_atol_1e-2"] for item in comparisons
        ),
    }
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.vision_only and not payload["allclose"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
