#!/usr/bin/env python3
"""Benchmark Mooncake FeatureBundle serialization formats.

This is intentionally CPU-only and store-free: it isolates the EPD hot-path
cost that dominated recent real serving artifacts, namely FeatureBundle
materialization from Mooncake Store payload bytes.  Network/RDMA performance is
measured by separate serving benchmarks; this script proves whether a payload
format change reduces first-load deserialize overhead on the same tensor bundle.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import FeatureBundle, MooncakeFeatureBundleStore, MooncakeFeatureBundleStoreConfig


def _parse_shape(raw: str) -> Tuple[int, ...]:
    dims = tuple(int(item.strip()) for item in str(raw).replace("x", ",").split(",") if item.strip())
    if not dims or any(dim <= 0 for dim in dims):
        raise argparse.ArgumentTypeError(f"invalid shape: {raw!r}")
    return dims


def _dtype(raw: str) -> torch.dtype:
    aliases = {
        "bf16": torch.bfloat16,
        "bfloat16": torch.bfloat16,
        "fp16": torch.float16,
        "float16": torch.float16,
        "fp32": torch.float32,
        "float32": torch.float32,
    }
    key = str(raw).strip().lower()
    if key not in aliases:
        raise argparse.ArgumentTypeError(f"unsupported dtype: {raw!r}")
    return aliases[key]


def _bundle(shape: Tuple[int, ...], dtype: torch.dtype, *, intermediates: int) -> FeatureBundle:
    numel = 1
    for dim in shape:
        numel *= dim
    base = torch.arange(numel, dtype=torch.float32).reshape(shape).to(dtype)
    inter = []
    # Keep intermediates realistic but smaller than the main hidden state so the
    # benchmark stays fast on dev machines while exercising multi-tensor layout.
    if intermediates > 0:
        rows = int(shape[0])
        width = max(1, min(256, int(shape[-1]) // 16))
        for idx in range(intermediates):
            inter.append((idx, torch.full((rows, width), float(idx + 1), dtype=dtype)))
    return FeatureBundle(
        image_hash="bench-feature",
        last_hidden=base,
        intermediates=inter,
        grid_thw=torch.tensor([[1, 1, int(shape[0])]], dtype=torch.long),
        metadata={"model_fingerprint": "bench-model", "processor_fingerprint": "bench-processor"},
    )


def _time_ms(fn, *, repeats: int) -> Tuple[List[float], Any]:
    values: List[float] = []
    result = None
    for _ in range(max(1, repeats)):
        started = time.perf_counter()
        result = fn()
        values.append((time.perf_counter() - started) * 1000.0)
    return values, result


def _measure(fmt: str, bundle: FeatureBundle, *, checksum: bool, repeats: int) -> Dict[str, Any]:
    store = MooncakeFeatureBundleStore(
        MooncakeFeatureBundleStoreConfig(store_id="bench", serialization_format=fmt)
    )
    descriptor = bundle.descriptor(checksum=checksum)
    ser_ms, payload = _time_ms(lambda: store._serialize_bundle(bundle, descriptor=descriptor), repeats=repeats)
    deser_ms, loaded = _time_ms(lambda: store._deserialize_bundle(payload), repeats=repeats)
    descriptor.validate_bundle(loaded, require_checksum=checksum)
    return {
        "format": fmt,
        "payload_bytes": len(payload),
        "serialize_ms": ser_ms,
        "deserialize_ms": deser_ms,
        "serialize_ms_median": statistics.median(ser_ms),
        "deserialize_ms_median": statistics.median(deser_ms),
        "loaded_nbytes": loaded.nbytes(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--shape", type=_parse_shape, default=(945, 16384))
    parser.add_argument("--dtype", type=_dtype, default=torch.bfloat16)
    parser.add_argument("--intermediates", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--checksum", action="store_true")
    args = parser.parse_args()

    bundle = _bundle(args.shape, args.dtype, intermediates=max(0, int(args.intermediates)))
    raw = _measure("raw_v2", bundle, checksum=bool(args.checksum), repeats=int(args.repeats))
    legacy = _measure("torch_pickle", bundle, checksum=bool(args.checksum), repeats=int(args.repeats))
    speedup = (
        legacy["deserialize_ms_median"] / raw["deserialize_ms_median"]
        if raw["deserialize_ms_median"] > 0
        else None
    )
    artifact = {
        "bundle_shape": list(args.shape),
        "dtype": str(args.dtype).replace("torch.", ""),
        "bundle_nbytes": bundle.nbytes(),
        "checksum": bool(args.checksum),
        "raw_v2": raw,
        "torch_pickle": legacy,
        "deserialize_speedup_raw_v2_over_torch_pickle": speedup,
        "pass": bool(speedup is not None and speedup > 1.0),
    }

    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(artifact, ensure_ascii=False, indent=2))
    if not artifact["pass"]:
        raise SystemExit("raw_v2 deserialize did not beat torch_pickle")


if __name__ == "__main__":
    main()
