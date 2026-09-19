#!/usr/bin/env python3
"""Measure Omni cache-key construction on a real CPU/CUDA tensor.

This is a transport/hash microbenchmark, not a model-throughput benchmark. It
isolates the cost removed by the authoritative StableContentId path and emits
raw per-iteration timings so the result cannot be confused with Qwen serving
TTFT or throughput.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

import torch

from mooncake_epd.core.state.omni_hidden_prefix_cache import (
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
)


def _percentile(values: List[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def _measure(fn: Callable[[], str], *, iterations: int, device: torch.device) -> Dict[str, Any]:
    samples: List[float] = []
    keys: List[str] = []
    for _ in range(iterations):
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        started = time.perf_counter()
        keys.append(fn())
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        samples.append((time.perf_counter() - started) * 1000.0)
    if len(set(keys)) != 1:
        raise RuntimeError("cache-key construction was not deterministic")
    return {
        "iterations": iterations,
        "samples_ms": samples,
        "mean_ms": statistics.fmean(samples),
        "p50_ms": _percentile(samples, 0.50),
        "p95_ms": _percentile(samples, 0.95),
        "min_ms": min(samples),
        "max_ms": max(samples),
        "key": keys[0],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", default="cuda:0" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--rows", type=int, default=65536)
    parser.add_argument("--cols", type=int, default=512)
    parser.add_argument("--dtype", choices=("float16", "bfloat16", "float32"), default="float32")
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if args.rows <= 0 or args.cols <= 0 or args.iterations <= 0:
        raise ValueError("rows, cols, and iterations must be positive")
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA benchmark requested but torch.cuda.is_available() is false")
    dtype = getattr(torch, args.dtype)
    pixel_values = torch.arange(
        args.rows * args.cols,
        device=device,
        dtype=torch.float32,
    ).reshape(args.rows, args.cols).to(dtype=dtype)
    grid = torch.tensor([[1, args.rows, 1]], dtype=torch.long, device=device)
    cache = OmniHiddenPrefixCache(OmniHiddenPrefixCacheConfig(enabled=True))

    stable = _measure(
        lambda: cache._stable_content_key(
            modality="image-batch",
            stable_ids=("sha256:" + "a" * 64,),
            tensor=pixel_values,
            aux=grid,
            namespace="benchmark-model",
            output_tokens=-1,
        ),
        iterations=args.iterations,
        device=device,
    )
    full = _measure(
        lambda: cache._tensor_key(
            modality="image-batch",
            tensor=pixel_values,
            aux=grid,
            namespace="benchmark-model",
            output_tokens=-1,
        ),
        iterations=args.iterations,
        device=device,
    )
    tensor_bytes = int(pixel_values.nelement() * pixel_values.element_size())
    reduction = (
        1.0 - float(stable["mean_ms"]) / float(full["mean_ms"])
        if float(full["mean_ms"]) > 0.0
        else None
    )
    payload = {
        "kind": "omni_cache_key_microbenchmark",
        "claim_scope": "cache_key_construction_only_not_model_serving",
        "timestamp": time.time(),
        "device": str(device),
        "cuda_visible_devices": os.getenv("CUDA_VISIBLE_DEVICES"),
        "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
        "torch_version": torch.__version__,
        "tensor": {
            "shape": [args.rows, args.cols],
            "dtype": str(dtype),
            "bytes": tensor_bytes,
        },
        "stable_content_id": stable,
        "full_tensor_hash": full,
        "mean_time_reduction_ratio": reduction,
        "stable_path_full_pixel_d2h_bytes_by_design": 0,
        "full_hash_pixel_d2h_bytes_per_iteration": tensor_bytes if device.type == "cuda" else 0,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
