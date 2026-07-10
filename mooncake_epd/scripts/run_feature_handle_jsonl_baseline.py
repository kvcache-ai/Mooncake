#!/usr/bin/env python3
"""Run a strict real HF Qwen-VL baseline over FeatureHandle JSONL requests.

The input JSONL is produced by build_dataset_feature_handle_requests.py.  This
baseline uses the final OpenAI-compatible request bytes (including any processed
image data URLs) so latency/quality are compared against the exact same prompts
served by EPD FeatureHandle mode.  It intentionally has no dependency fallback.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import torch
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * pct
    lo = int(rank)
    hi = min(len(xs) - 1, lo + (0 if rank == lo else 1))
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "avg": float(statistics.fmean(values)),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def _openai_to_qwen_messages(request: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for msg in list(request.get("messages") or []):
        role = str(msg.get("role", "user"))
        content_out: List[Dict[str, Any]] = []
        content = msg.get("content")
        if isinstance(content, str):
            out.append({"role": role, "content": content})
            continue
        for part in list(content or []):
            if not isinstance(part, dict):
                continue
            typ = str(part.get("type"))
            if typ == "text":
                content_out.append({"type": "text", "text": str(part.get("text", ""))})
            elif typ in {"image_url", "input_image"}:
                image_url = part.get("image_url") or part.get("input_image") or {}
                url = image_url.get("url") if isinstance(image_url, dict) else image_url
                if not url:
                    raise ValueError("image_url part missing url")
                content_out.append({"type": "image", "image": str(url)})
            elif typ == "image":
                content_out.append(part)
        out.append({"role": role, "content": content_out})
    return out


def _assert_data_urls_decodable(messages: List[Dict[str, Any]]) -> None:
    for msg in messages:
        for part in list(msg.get("content") or []):
            if not isinstance(part, dict) or part.get("type") != "image":
                continue
            raw = str(part.get("image") or "")
            if raw.startswith("data:image") and "base64," in raw:
                _, b64 = raw.split("base64,", 1)
                Image.open(io.BytesIO(base64.b64decode(b64))).verify()
            elif raw.startswith(("http://", "https://")):
                raise ValueError("network image URLs are not allowed in strict baseline")


def _run_one(row: Dict[str, Any], *, processor: Any, model: Any, device: torch.device) -> Dict[str, Any]:
    from qwen_vl_utils import process_vision_info  # type: ignore

    request = dict(row["request"])
    messages = _openai_to_qwen_messages(request)
    _assert_data_urls_decodable(messages)
    max_new_tokens = int(request.get("max_tokens") or request.get("max_completion_tokens") or 32)
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    image_inputs, video_inputs = process_vision_info(messages)
    inputs = processor(
        text=[text],
        images=image_inputs,
        videos=video_inputs,
        padding=True,
        return_tensors="pt",
    )
    prompt_tokens = int(inputs["input_ids"].shape[-1])
    inputs = {k: (v.to(device) if hasattr(v, "to") else v) for k, v in inputs.items()}
    started = time.perf_counter()
    with torch.inference_mode():
        generated_ids = model.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    generated_trimmed = [out_ids[len(in_ids):] for in_ids, out_ids in zip(inputs["input_ids"], generated_ids)]
    output_text = processor.batch_decode(generated_trimmed, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]
    completion_tokens = int(generated_trimmed[0].shape[-1])
    return {
        "index": row.get("index"),
        "sample_id": (row.get("sample") or {}).get("sample_id"),
        "workflow_id": (row.get("sample") or {}).get("workflow_id"),
        "workload_family": row.get("family"),
        "elapsed_ms": elapsed_ms,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "response_text": output_text,
        "response_head": output_text[:500],
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    try:
        import qwen_vl_utils  # noqa: F401
        from transformers import AutoProcessor, Qwen3VLForConditionalGeneration
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("strict baseline requires qwen_vl_utils + transformers") from exc

    rows = _load_jsonl(Path(args.requests_jsonl).expanduser().resolve())
    if args.max_requests > 0:
        rows = rows[: int(args.max_requests)]
    if not rows:
        raise RuntimeError("no requests selected")
    device = torch.device(args.device)
    processor = AutoProcessor.from_pretrained(args.model, trust_remote_code=True)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        args.model,
        dtype=torch.bfloat16,
        device_map={"": device},
        low_cpu_mem_usage=True,
        trust_remote_code=True,
    )
    model.eval()
    try:
        for i in range(max(0, int(args.warmup_requests))):
            _run_one(rows[i % len(rows)], processor=processor, model=model, device=device)
        results = [_run_one(row, processor=processor, model=model, device=device) for row in rows]
    finally:
        try:
            del model
            torch.cuda.empty_cache()
        except Exception:
            pass
    lat = [float(r["elapsed_ms"]) for r in results]
    summary = {
        "requests_jsonl": str(Path(args.requests_jsonl).expanduser().resolve()),
        "model": args.model,
        "device": str(device),
        "request_count": len(results),
        "warmup_requests": int(args.warmup_requests),
        "latency_stats_ms": _stats(lat),
        "results": results,
    }
    out = Path(args.output).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests-jsonl", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--device", default="cuda:2")
    ap.add_argument("--max-requests", type=int, default=0)
    ap.add_argument("--warmup-requests", type=int, default=5)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    print(json.dumps({
        "output": str(Path(args.output).expanduser().resolve()),
        "request_count": summary["request_count"],
        "latency_stats_ms": summary["latency_stats_ms"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
