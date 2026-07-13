#!/usr/bin/env python3
"""Benchmark EPD direct FeatureHandle materialization vs file-backed handles.

The benchmark isolates the TTFT-sensitive prefill-side work before the model
forward: resolving a FeatureHandle into Qwen-compatible ``image_embeds``.  It
uses the production in-process ``DirectFeatureBufferRegistry`` path (the same
path used when Prefill owns peer buffers in the serving process) and compares it
with the legacy file-backed FeatureBundle load path on the same tensor bundle.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import (  # noqa: E402
    DirectFeatureBufferRegistry,
    FeatureBundle,
    FeatureHandle,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    clear_feature_handle_bundle_cache,
    publish_feature_bundle_to_dir,
    register_direct_feature_buffer_registry,
    unregister_direct_feature_buffer_registry,
)


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
    last_hidden = torch.arange(numel, dtype=torch.float32).reshape(shape).to(dtype)
    inter = []
    if intermediates > 0:
        rows = int(shape[0])
        width = max(1, min(256, int(shape[-1]) // 16))
        for layer in range(intermediates):
            inter.append((layer, torch.full((rows, width), float(layer + 1), dtype=dtype)))
    return FeatureBundle(
        image_hash="direct-bench-feature",
        last_hidden=last_hidden,
        intermediates=inter,
        grid_thw=torch.tensor([[1, 1, int(shape[0])]], dtype=torch.long),
        metadata={"model_fingerprint": "bench-model", "processor_fingerprint": "bench-processor"},
    )


def _direct_handle_from_allocation(bundle: FeatureBundle, allocation) -> FeatureHandle:
    return FeatureHandle(
        handle_id="direct-bench",
        feature_id=bundle.image_hash,
        store_id="direct-bench-store",
        uri=f"epd-direct://direct-bench-store/{bundle.image_hash}",
        descriptor=bundle.descriptor(),
        metadata={
            "backend": "direct_engine",
            "direct_plan": {
                "feature_id": bundle.image_hash,
                "targets": [
                    {
                        "name": name,
                        "remote_pointer": int(tensor.data_ptr()),
                        "nbytes": int(tensor.nelement() * tensor.element_size()),
                    }
                    for name, tensor in allocation.tensors.items()
                ],
            },
        },
    )


def _time_ms(fn, repeats: int) -> List[float]:
    out: List[float] = []
    for _ in range(max(1, repeats)):
        started = time.perf_counter()
        resolved = fn()
        if resolved is None:
            raise RuntimeError("FeatureHandle resolve returned None")
        # Force cheap metadata access so lazy failures surface inside timing.
        _ = tuple(resolved.image_embeds.shape)
        out.append((time.perf_counter() - started) * 1000.0)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--shape", type=_parse_shape, default=(945, 16384))
    parser.add_argument("--dtype", type=_dtype, default=torch.bfloat16)
    parser.add_argument("--intermediates", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    clear_feature_handle_bundle_cache()
    bundle = _bundle(args.shape, args.dtype, intermediates=max(0, int(args.intermediates)))
    provider_cfg = FeatureHandleProviderConfig(
        worker_id="direct-bench-prefill",
        device="cpu",
        strict=True,
        bundle_cache_entries=0,
        resolved_cache_entries=0,
        expected_model_fingerprint="bench-model",
        expected_processor_fingerprint="bench-processor",
    )

    with tempfile.TemporaryDirectory(prefix="mooncake-epd-direct-bench-") as tmpdir:
        file_handle = publish_feature_bundle_to_dir(bundle, Path(tmpdir), checksum=False)
        file_provider = FeatureHandleProvider(provider_cfg)

        registry = DirectFeatureBufferRegistry(worker_id="direct-bench-prefill", device="cpu")
        allocation = registry.allocate_for_descriptor(bundle.descriptor(), zero_fill=False)
        allocation.tensors["last_hidden"].copy_(bundle.last_hidden)
        if bundle.grid_thw is not None:
            allocation.tensors["grid_thw"].copy_(bundle.grid_thw)
        for layer, tensor in bundle.intermediates:
            key = next(name for name in allocation.tensors if name.startswith(f"intermediate:{int(layer)}:"))
            allocation.tensors[key].copy_(tensor)
        direct_handle = _direct_handle_from_allocation(bundle, allocation)
        direct_provider = FeatureHandleProvider(provider_cfg)

        register_direct_feature_buffer_registry(registry)
        try:
            file_ms = _time_ms(
                lambda: file_provider.resolve_from_sources(
                    {"mm_feature_handles": [file_handle.as_control_payload()]},
                    device="cpu",
                    dtype=args.dtype,
                ),
                int(args.repeats),
            )
            direct_ms = _time_ms(
                lambda: direct_provider.resolve_from_sources(
                    {"mm_feature_handles": [direct_handle.as_control_payload()]},
                    device="cpu",
                    dtype=args.dtype,
                ),
                int(args.repeats),
            )
        finally:
            unregister_direct_feature_buffer_registry("direct-bench-prefill")

    artifact: Dict[str, Any] = {
        "shape": list(args.shape),
        "dtype": str(args.dtype).replace("torch.", ""),
        "intermediates": int(args.intermediates),
        "bundle_nbytes": bundle.nbytes(),
        "file_materialize_ms": file_ms,
        "direct_materialize_ms": direct_ms,
        "file_materialize_ms_median": statistics.median(file_ms),
        "direct_materialize_ms_median": statistics.median(direct_ms),
        "speedup_direct_over_file": (
            statistics.median(file_ms) / statistics.median(direct_ms)
            if statistics.median(direct_ms) > 0
            else None
        ),
    }
    artifact["pass"] = bool(
        artifact["speedup_direct_over_file"] is not None
        and artifact["speedup_direct_over_file"] > 1.0
    )

    out = Path(args.output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(artifact, ensure_ascii=False, indent=2))
    if not artifact["pass"]:
        raise SystemExit("direct peer-buffer materialization did not beat file-backed FeatureHandle")


if __name__ == "__main__":
    main()
