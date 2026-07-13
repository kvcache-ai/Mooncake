#!/usr/bin/env python3
"""Run real Qwen2.5-Omni hidden-state cache benchmark on mooncake_test_dataset.

This benchmark exercises the actual Qwen2.5-Omni image hidden-state boundary
over a full dataset chat split.  It measures:

* baseline image-encoder latency without the Mooncake hidden cache;
* cached pass-1 latency, including natural cross-step image reuse in the split;
* cached pass-2 latency, where every previously seen image segment should be a
  full hidden-state cache hit;
* correctness spot checks against the original encoder output.

It intentionally benchmarks the real encoder hot path only; it does not mock
model execution or synthesize latency.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import torch
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state.omni_hidden_prefix_cache import (  # noqa: E402
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
    install_qwen2_5_omni_hidden_prefix_cache,
)


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _sync(device: str) -> None:
    if str(device).startswith("cuda") and torch.cuda.is_available():
        torch.cuda.synchronize(torch.device(device))


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * pct
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _stats(values: Sequence[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    xs = [float(v) for v in values]
    return {
        "count": float(len(xs)),
        "avg": float(statistics.fmean(xs)),
        "p50": _percentile(xs, 0.50),
        "p95": _percentile(xs, 0.95),
        "p99": _percentile(xs, 0.99),
        "max": max(xs),
    }


def _iter_jsonl(path: Path, *, limit: int = 0) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if limit > 0 and idx >= limit:
                break
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _load_samples(dataset_root: Path, split: str, *, limit: int = 0) -> List[Dict[str, Any]]:
    path = dataset_root / "chat_splits" / f"{split}.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"chat split not found: {path}")
    samples = list(_iter_jsonl(path, limit=limit))
    if not samples:
        raise RuntimeError(f"no samples loaded from {path}")
    return samples


def _dataset_messages_for_processor(sample: Dict[str, Any], dataset_root: Path) -> Tuple[List[Dict[str, Any]], List[Image.Image], List[str]]:
    messages: List[Dict[str, Any]] = []
    images: List[Image.Image] = []
    image_paths: List[str] = []
    for message in list(sample.get("messages") or []):
        role = str(message.get("role", "user"))
        content_out: List[Dict[str, Any]] = []
        content = message.get("content")
        if isinstance(content, str):
            content_out.append({"type": "text", "text": content})
        elif isinstance(content, list):
            for part in content:
                if not isinstance(part, dict):
                    continue
                ptype = str(part.get("type", ""))
                if ptype == "text":
                    content_out.append({"type": "text", "text": str(part.get("text", ""))})
                elif ptype in {"image", "image_url", "input_image"}:
                    rel = part.get("image")
                    if not rel:
                        image_url = part.get("image_url")
                        rel = image_url.get("url") if isinstance(image_url, dict) else image_url
                    if not rel:
                        continue
                    raw = str(rel)
                    if raw.startswith(("http://", "https://", "data:")):
                        raise RuntimeError(f"dataset benchmark expects local images, got: {raw[:64]}")
                    path = (dataset_root / raw).resolve()
                    if not path.exists():
                        raise FileNotFoundError(f"image not found: {path}")
                    image = Image.open(path).convert("RGB")
                    images.append(image)
                    image_paths.append(str(path))
                    content_out.append({"type": "image", "image": image})
        messages.append({"role": role, "content": content_out})
    if not images:
        raise RuntimeError(f"sample has no image: {sample.get('sample_id')}")
    return messages, images, image_paths


def _prepare_inputs(
    processor: Any,
    sample: Dict[str, Any],
    dataset_root: Path,
    device: str,
) -> Tuple[Dict[str, torch.Tensor], Dict[str, Any]]:
    messages, images, image_paths = _dataset_messages_for_processor(sample, dataset_root)
    text = processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
    inputs = processor(text=[text], images=images, return_tensors="pt", padding=True)
    if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
        raise RuntimeError(f"processor missing pixel_values/image_grid_thw; keys={list(inputs.keys())}")
    tensors = {
        # Keep prepared tensors on CPU.  Full dataset splits contain hundreds of
        # high-resolution document images; preloading all pixel tensors on GPU
        # exhausts A6000 memory before the first encoder call.  The hot path moves
        # only one sample to the target device inside _run_one().
        "pixel_values": inputs["pixel_values"].detach().cpu().contiguous(),
        "image_grid_thw": inputs["image_grid_thw"].detach().cpu().contiguous(),
    }
    metadata = {
        "sample_id": sample.get("sample_id"),
        "workflow_id": sample.get("workflow_id"),
        "family": sample.get("workload_family") or (sample.get("extensions") or {}).get("workload_family"),
        "source_dataset": sample.get("source_dataset"),
        "priority_class": sample.get("priority_class"),
        "image_count": len(images),
        "image_paths": image_paths,
        "grid_thw": inputs["image_grid_thw"].detach().cpu().tolist(),
        "pixel_shape": list(inputs["pixel_values"].shape),
    }
    for image in images:
        try:
            image.close()
        except Exception:
            pass
    return tensors, metadata


def _digest_tensor(tensor: torch.Tensor) -> str:
    cpu = tensor.detach().contiguous().cpu()
    h = hashlib.sha256()
    h.update(str(cpu.dtype).encode())
    h.update(json.dumps(list(cpu.shape), separators=(",", ":")).encode())
    h.update(cpu.view(torch.uint8).numpy().tobytes())
    return h.hexdigest()


def _run_one(original_fn: Any, inputs: Dict[str, torch.Tensor], *, device: str) -> Tuple[float, torch.Tensor]:
    _sync(device)
    gpu_inputs = {key: value.to(device, non_blocking=True) for key, value in inputs.items()}
    started = _now_ms()
    with torch.inference_mode():
        out = original_fn(**gpu_inputs, return_dict=True)
        hidden = getattr(out, "pooler_output", None)
        if hidden is None:
            raise RuntimeError("get_image_features did not return pooler_output")
    _sync(device)
    elapsed = _now_ms() - started
    result = hidden.detach().cpu().contiguous()
    del out
    del hidden
    del gpu_inputs
    if str(device).startswith("cuda"):
        torch.cuda.empty_cache()
    return elapsed, result


def _summarize_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_family = Counter(str(row.get("family") or "unknown") for row in rows)
    by_source = Counter(str(row.get("source_dataset") or "unknown") for row in rows)
    unique_images = set()
    refs = 0
    for row in rows:
        for path in row.get("image_paths") or []:
            refs += 1
            unique_images.add(path)
    return {
        "samples": len(rows),
        "by_family": dict(sorted(by_family.items())),
        "by_source": dict(sorted(by_source.items())),
        "image_references": refs,
        "unique_images": len(unique_images),
        "image_reuse_factor": float(refs / len(unique_images)) if unique_images else 0.0,
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    dataset_root = Path(args.dataset_root).expanduser().resolve()
    samples = _load_samples(dataset_root, args.split, limit=int(args.limit))
    artifact: Dict[str, Any] = {
        "status": "started",
        "dataset_root": str(dataset_root),
        "split": args.split,
        "sample_count": len(samples),
        "model": str(Path(args.model).expanduser()),
        "device": args.device,
        "dtype": args.dtype,
        "timestamp_unix": time.time(),
    }

    from transformers import AutoProcessor, Qwen2_5OmniThinkerForConditionalGeneration

    dtype = {
        "bfloat16": torch.bfloat16,
        "bf16": torch.bfloat16,
        "float16": torch.float16,
        "fp16": torch.float16,
        "float32": torch.float32,
        "fp32": torch.float32,
    }[str(args.dtype).lower()]
    processor = AutoProcessor.from_pretrained(args.model, trust_remote_code=True, local_files_only=True)
    load_started = _now_ms()
    model = Qwen2_5OmniThinkerForConditionalGeneration.from_pretrained(
        args.model,
        dtype=dtype,
        device_map={"": torch.device(args.device)},
        low_cpu_mem_usage=True,
        local_files_only=True,
        trust_remote_code=True,
    )
    model.eval()
    _sync(args.device)
    artifact["model_load_ms"] = _now_ms() - load_started
    artifact["gpu_name"] = torch.cuda.get_device_name(torch.device(args.device)) if args.device.startswith("cuda") else "cpu"

    prepared: List[Tuple[Dict[str, torch.Tensor], Dict[str, Any]]] = []
    prep_errors: List[Dict[str, Any]] = []
    prep_started = _now_ms()
    for sample in samples:
        try:
            prepared.append(_prepare_inputs(processor, sample, dataset_root, args.device))
        except Exception as exc:  # noqa: BLE001
            prep_errors.append({"sample_id": sample.get("sample_id"), "error": f"{type(exc).__name__}: {exc}"})
            if not args.skip_bad_samples:
                raise
    artifact["prepare_ms"] = _now_ms() - prep_started
    artifact["prepare_errors"] = prep_errors[:20]
    artifact["prepare_error_count"] = len(prep_errors)
    if not prepared:
        raise RuntimeError("no prepared samples")
    rows_meta = [meta for _, meta in prepared]
    artifact["dataset_summary"] = _summarize_rows(rows_meta)

    original = model.get_image_features
    if args.warmup:
        _run_one(original, prepared[0][0], device=args.device)

    baseline_lat: List[float] = []
    baseline_digests: Dict[int, str] = {}
    baseline_shapes: Dict[int, List[int]] = {}
    check_n = max(0, int(args.correctness_samples))
    baseline_started = _now_ms()
    for idx, (inputs, _) in enumerate(prepared):
        ms, hidden = _run_one(original, inputs, device=args.device)
        baseline_lat.append(ms)
        if idx < check_n:
            baseline_digests[idx] = _digest_tensor(hidden)
            baseline_shapes[idx] = list(hidden.shape)
    artifact["baseline_wall_ms"] = _now_ms() - baseline_started

    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=int(args.max_entries),
            max_bytes=int(args.max_bytes),
            store_on_gpu=bool(args.store_on_gpu),
            metrics_path=args.metrics_path,
            allow_partial_prefix_reuse=False,
        )
    )
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    cached_pass1_lat: List[float] = []
    correctness: List[Dict[str, Any]] = []
    pass1_started = _now_ms()
    for idx, (inputs, meta) in enumerate(prepared):
        ms, hidden = _run_one(model.get_image_features, inputs, device=args.device)
        cached_pass1_lat.append(ms)
        if idx < check_n:
            digest = _digest_tensor(hidden)
            correctness.append(
                {
                    "index": idx,
                    "sample_id": meta.get("sample_id"),
                    "shape": list(hidden.shape),
                    "baseline_shape": baseline_shapes.get(idx),
                    "digest_matches": digest == baseline_digests.get(idx),
                }
            )
    artifact["cached_pass1_wall_ms"] = _now_ms() - pass1_started
    pass1_stats = dict(cache.stats)

    cached_pass2_lat: List[float] = []
    pass2_started = _now_ms()
    for inputs, _ in prepared:
        ms, _ = _run_one(model.get_image_features, inputs, device=args.device)
        cached_pass2_lat.append(ms)
    artifact["cached_pass2_wall_ms"] = _now_ms() - pass2_started
    pass2_stats = dict(cache.stats)

    baseline_avg = statistics.fmean(baseline_lat) if baseline_lat else 0.0
    pass1_avg = statistics.fmean(cached_pass1_lat) if cached_pass1_lat else 0.0
    pass2_avg = statistics.fmean(cached_pass2_lat) if cached_pass2_lat else 0.0
    pass1_encoder_calls = int(pass1_stats.get("image_encoder_calls", 0) or 0)
    pass2_encoder_calls = int(pass2_stats.get("image_encoder_calls", 0) or 0)
    artifact.update(
        {
            "status": "ok",
            "baseline_latency_ms": _stats(baseline_lat),
            "cached_pass1_latency_ms": _stats(cached_pass1_lat),
            "cached_pass2_latency_ms": _stats(cached_pass2_lat),
            "speedup": {
                "pass1_avg_vs_baseline": baseline_avg / pass1_avg if pass1_avg > 0 else None,
                "pass2_avg_vs_baseline": baseline_avg / pass2_avg if pass2_avg > 0 else None,
            },
            "encoder_skip": {
                "pass1_encoder_calls": pass1_encoder_calls,
                "pass1_skip_rate": 1.0 - (pass1_encoder_calls / len(prepared)),
                "pass2_incremental_encoder_calls": pass2_encoder_calls - pass1_encoder_calls,
                "pass2_incremental_skip_rate": 1.0 - ((pass2_encoder_calls - pass1_encoder_calls) / len(prepared)),
            },
            "correctness": {
                "checked": len(correctness),
                "all_digest_match": all(row.get("digest_matches") for row in correctness),
                "rows": correctness,
            },
            "cache_stats_after_pass1": pass1_stats,
            "cache_stats_after_pass2": pass2_stats,
        }
    )
    if not artifact["correctness"]["all_digest_match"]:
        artifact["status"] = "correctness_failed"
    if int(pass2_stats.get("errors", 0) or 0) != 0:
        artifact["status"] = "cache_errors"
    return artifact


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Real Qwen2.5-Omni hidden cache dataset benchmark")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_OMNI_MODEL", "models/Qwen2.5-Omni-7B"),
    )
    ap.add_argument(
        "--dataset-root",
        default=os.getenv(
            "MOONCAKE_EPD_DATASET_ROOT",
            "datasets/mooncake_test_dataset",
        ),
    )
    ap.add_argument("--split", default="test")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--device", default="cuda:0")
    ap.add_argument("--dtype", default="bfloat16", choices=["bfloat16", "bf16", "float16", "fp16", "float32", "fp32"])
    ap.add_argument("--output", required=True)
    ap.add_argument("--metrics-path", default=None)
    ap.add_argument("--correctness-samples", type=int, default=32)
    ap.add_argument("--max-entries", type=int, default=8192)
    ap.add_argument("--max-bytes", type=int, default=8 * 1024 * 1024 * 1024)
    ap.add_argument("--store-on-gpu", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--warmup", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--skip-bad-samples", action=argparse.BooleanOptionalAction, default=False)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    artifact = run(args)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(artifact, ensure_ascii=False, indent=2))
    if artifact.get("status") != "ok":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
