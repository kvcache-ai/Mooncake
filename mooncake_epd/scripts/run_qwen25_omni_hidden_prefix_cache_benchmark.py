#!/usr/bin/env python3
"""Real Qwen2.5-Omni hidden-state prefix-cache GPU benchmark.

The benchmark exercises the real Qwen2.5-Omni vision hidden-state boundary:
``Qwen2_5OmniThinkerForConditionalGeneration.get_image_features``.  It builds a
three-request workload `[A,B] -> [A,C] -> [A,C]`.  Production defaults use safe
exact-segment reuse, so `[A,C]` is computed once and the repeated `[A,C]` request
is a full hidden-state hit.  Item-level `[A,B] -> [A,C]` partial reuse remains an
explicit experimental mode because real GPU validation showed that Qwen2.5-Omni
image hidden states are not item-independent by default.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import torch
from PIL import Image, ImageDraw

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


def _model_shard_status(model_dir: Path) -> Dict[str, Any]:
    index = model_dir / "model.safetensors.index.json"
    if not index.exists():
        return {"index_exists": False, "missing": [str(index)], "complete": False}
    data = json.loads(index.read_text(encoding="utf-8"))
    names = sorted(set((data.get("weight_map") or {}).values()))
    present: Dict[str, str] = {}
    missing: List[str] = []
    for name in names:
        direct = model_dir / name
        temp = model_dir / "._____temp" / name
        if direct.exists():
            present[name] = str(direct)
        elif temp.exists():
            present[name] = str(temp)
        else:
            missing.append(name)
    return {
        "index_exists": True,
        "expected_shards": names,
        "present": present,
        "missing": missing,
        "complete": not missing,
        "direct_ready": all((model_dir / name).exists() for name in names),
        "temp_ready": all(((model_dir / name).exists() or (model_dir / "._____temp" / name).exists()) for name in names),
    }


def _repair_temp_shards(model_dir: Path) -> Dict[str, Any]:
    status = _model_shard_status(model_dir)
    actions: List[Dict[str, str]] = []
    if not status.get("temp_ready"):
        return {"actions": actions, "status": status}
    for name, src in status.get("present", {}).items():
        dst = model_dir / name
        if dst.exists():
            continue
        src_path = Path(src)
        if src_path.parent.name != "._____temp":
            continue
        os.symlink(src_path, dst)
        actions.append({"symlink": str(dst), "target": str(src_path)})
    return {"actions": actions, "status": _model_shard_status(model_dir)}


def _make_image(label: str, color: Tuple[int, int, int]) -> Image.Image:
    image = Image.new("RGB", (224, 224), color=color)
    draw = ImageDraw.Draw(image)
    draw.rectangle((20, 20, 204, 204), outline=(255, 255, 255), width=4)
    draw.text((36, 96), label, fill=(255, 255, 255))
    return image


def _prepare_two_image_inputs(processor: Any, images: List[Image.Image], device: str) -> Dict[str, torch.Tensor]:
    content = []
    for image in images:
        content.append({"type": "image", "image": image})
    content.append({"type": "text", "text": "Describe the shared visual prefix briefly."})
    messages = [{"role": "user", "content": content}]
    text = processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
    inputs = processor(text=[text], images=images, return_tensors="pt", padding=True)
    required = ["pixel_values", "image_grid_thw"]
    missing = [key for key in required if key not in inputs]
    if missing:
        raise RuntimeError(f"processor output missing keys: {missing}; keys={list(inputs.keys())}")
    return {
        "pixel_values": inputs["pixel_values"].to(device),
        "image_grid_thw": inputs["image_grid_thw"].to(device),
    }


def _time_call(fn, *, device: str, repeats: int = 1) -> Tuple[float, Any]:
    result = None
    _sync(device)
    started = _now_ms()
    for _ in range(max(1, int(repeats))):
        result = fn()
    _sync(device)
    return (_now_ms() - started) / max(1, int(repeats)), result


def run(args: argparse.Namespace) -> Dict[str, Any]:
    model_dir = Path(args.model).expanduser()
    artifact: Dict[str, Any] = {
        "model": str(model_dir),
        "device": args.device,
        "dtype": args.dtype,
        "timestamp_unix": time.time(),
    }
    shard_status = _model_shard_status(model_dir)
    artifact["shards_before"] = shard_status
    if args.repair_temp_shards and shard_status.get("temp_ready") and not shard_status.get("direct_ready"):
        artifact["repair_temp_shards"] = _repair_temp_shards(model_dir)
        shard_status = _model_shard_status(model_dir)
        artifact["shards_after"] = shard_status
    if not shard_status.get("complete") or not shard_status.get("direct_ready"):
        artifact["status"] = "model_incomplete"
        artifact["reason"] = (
            "model safetensor shards are missing from the model root; rerun after download completes "
            "or pass --repair-temp-shards if the complete shards are intentionally staged in ._____temp"
        )
        return artifact

    from transformers import AutoProcessor, Qwen2_5OmniThinkerForConditionalGeneration

    dtype = {
        "bfloat16": torch.bfloat16,
        "bf16": torch.bfloat16,
        "float16": torch.float16,
        "fp16": torch.float16,
        "float32": torch.float32,
        "fp32": torch.float32,
    }[args.dtype.lower()]
    processor = AutoProcessor.from_pretrained(str(model_dir), trust_remote_code=True, local_files_only=True)
    load_started = _now_ms()
    model = Qwen2_5OmniThinkerForConditionalGeneration.from_pretrained(
        str(model_dir),
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

    img_a = _make_image("A shared", (30, 80, 160))
    img_b = _make_image("B old", (120, 50, 40))
    img_c = _make_image("C new", (40, 130, 70))
    inputs_ab = _prepare_two_image_inputs(processor, [img_a, img_b], args.device)
    inputs_ac = _prepare_two_image_inputs(processor, [img_a, img_c], args.device)
    artifact["input_shapes"] = {
        "ab_pixel_values": list(inputs_ab["pixel_values"].shape),
        "ab_grid": inputs_ab["image_grid_thw"].detach().cpu().tolist(),
        "ac_pixel_values": list(inputs_ac["pixel_values"].shape),
        "ac_grid": inputs_ac["image_grid_thw"].detach().cpu().tolist(),
    }

    original = model.get_image_features
    # Warm one original call outside the measured baseline if requested.
    if args.warmup:
        _time_call(lambda: original(**inputs_ab, return_dict=True).pooler_output, device=args.device, repeats=1)

    baseline_ab_ms, baseline_ab = _time_call(
        lambda: original(**inputs_ab, return_dict=True).pooler_output,
        device=args.device,
        repeats=args.repeats,
    )
    baseline_ac_ms, baseline_ac = _time_call(
        lambda: original(**inputs_ac, return_dict=True).pooler_output,
        device=args.device,
        repeats=args.repeats,
    )

    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=args.max_entries,
            max_bytes=args.max_bytes,
            store_on_gpu=args.store_on_gpu,
            metrics_path=args.metrics_path,
            allow_partial_prefix_reuse=bool(args.allow_partial_prefix),
        )
    )
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)
    cached_ab_ms, cached_ab = _time_call(
        lambda: model.get_image_features(**inputs_ab, return_dict=True).pooler_output,
        device=args.device,
        repeats=1,
    )
    cached_ac_second_ms, cached_ac_second = _time_call(
        lambda: model.get_image_features(**inputs_ac, return_dict=True).pooler_output,
        device=args.device,
        repeats=1,
    )
    cached_ac_full_ms, cached_ac_full = _time_call(
        lambda: model.get_image_features(**inputs_ac, return_dict=True).pooler_output,
        device=args.device,
        repeats=max(1, args.repeats),
    )

    artifact.update(
        {
            "status": "ok",
            "baseline": {
                "ab_ms": baseline_ab_ms,
                "ac_ms": baseline_ac_ms,
                "avg_ms": (baseline_ab_ms + baseline_ac_ms) / 2.0,
            },
            "cached": {
                "ab_cold_miss_ms": cached_ab_ms,
                "ac_second_safe_ms": cached_ac_second_ms,
                "ac_full_hit_ms": cached_ac_full_ms,
            },
            "speedup": {
                "ac_second_vs_baseline_ac": baseline_ac_ms / cached_ac_second_ms if cached_ac_second_ms > 0 else None,
                "full_vs_baseline_ac": baseline_ac_ms / cached_ac_full_ms if cached_ac_full_ms > 0 else None,
            },
            "correctness": {
                "cold_matches_baseline_ab": bool(torch.allclose(cached_ab, baseline_ab, atol=args.atol, rtol=args.rtol)),
                "ac_second_matches_baseline_ac": bool(torch.allclose(cached_ac_second, baseline_ac, atol=args.atol, rtol=args.rtol)),
                "full_matches_second_ac": bool(torch.allclose(cached_ac_full, cached_ac_second, atol=args.atol, rtol=args.rtol)),
                "max_abs_diff_second_ac": float((cached_ac_second - baseline_ac).abs().max().detach().cpu()),
            },
            "cache_stats": cache.stats,
        }
    )
    return artifact


def main() -> None:
    ap = argparse.ArgumentParser(description="Run real Qwen2.5-Omni hidden-state prefix cache benchmark")
    ap.add_argument("--model", default="/home/songbinbin/Qwen2.5-Omni-7B")
    ap.add_argument("--device", default="cuda:2")
    ap.add_argument("--dtype", default="bfloat16", choices=["bfloat16", "bf16", "float16", "fp16", "float32", "fp32"])
    ap.add_argument("--output", default="artifacts/qwen25_omni_hidden_prefix_cache_benchmark.json")
    ap.add_argument("--metrics-path", default=None)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmup", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--max-entries", type=int, default=64)
    ap.add_argument("--max-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    ap.add_argument("--store-on-gpu", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--allow-partial-prefix", action=argparse.BooleanOptionalAction, default=False, help="Experimental: allow item-level prefix reuse. Disabled by default because Qwen2.5-Omni real GPU validation showed unsafe image contextualization for [A,B]->[A,C].")
    ap.add_argument("--repair-temp-shards", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--atol", type=float, default=1e-2)
    ap.add_argument("--rtol", type=float, default=1e-2)
    args = ap.parse_args()

    out = run(args)
    output = Path(args.output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if out.get("status") != "ok":
        raise SystemExit(2)
    correctness = out.get("correctness", {})
    if not all(bool(correctness.get(key)) for key in ["cold_matches_baseline_ab", "ac_second_matches_baseline_ac", "full_matches_second_ac"]):
        raise SystemExit(3)


if __name__ == "__main__":
    main()
