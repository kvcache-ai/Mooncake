"""Build a Qwen-VL/EPD RFC §8 evaluation JSONL manifest.

The builder prefers real local image assets when a dataset root is supplied and
falls back to text-only control tasks only if no image files are present.  It is
small by design: the generated manifest is consumed by real serving runners and
keeps enough metadata to evaluate EPD split, feature reuse, Agent routing, and
cross-step reuse without embedding large binary payloads.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from pathlib import Path
from typing import Any, Dict, Iterable, List

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}


def _stable_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _iter_images(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in IMAGE_SUFFIXES)


def _message(text: str, image_path: str | None) -> List[Dict[str, Any]]:
    content: List[Dict[str, Any]] = []
    if image_path:
        content.append({"type": "image_url", "image_url": {"url": Path(image_path).resolve().as_uri()}})
    content.append({"type": "text", "text": text})
    return [{"role": "user", "content": content}]


def build_dataset(
    *,
    dataset_root: str | Path | None,
    output: str | Path,
    samples: int = 60,
    seed: int = 20260709,
) -> Dict[str, Any]:
    rng = random.Random(seed)
    root = Path(dataset_root) if dataset_root else None
    images = list(_iter_images(root)) if root is not None else []
    if samples <= 0:
        raise ValueError("samples must be positive")

    rows: List[Dict[str, Any]] = []
    mix = [
        ("interactive", 0.35),
        ("thinking_long_context", 0.25),
        ("thinking_long_output", 0.15),
        ("hybrid", 0.15),
        ("boundary", 0.10),
    ]
    expanded: List[str] = []
    for name, ratio in mix:
        expanded.extend([name] * max(1, round(samples * ratio)))
    expanded = expanded[:samples]
    while len(expanded) < samples:
        expanded.append("interactive")
    rng.shuffle(expanded)

    for idx, family in enumerate(expanded):
        image = images[idx % len(images)] if images else None
        reuse_group = f"rg-{idx % max(1, min(8, len(images) or 4))}"
        if image is not None:
            reuse_group = f"img-{_stable_key(str(image.resolve()))}"
        if family == "interactive":
            agent_type = "interactive"
            query = "用一句话解释这个概念：Prefill/Decode 分离。"
            target = "low_latency_decode_pool"
            sla_ms = 800
            input_bucket = "0-512"
            output_bucket = "0-128"
        elif family == "hybrid":
            agent_type = "hybrid"
            query = "先快速给结论，再解释为什么多模态 EPD 可以降低 TTFT。"
            target = "mixed"
            sla_ms = 5000
            input_bucket = "2k-4k"
            output_bucket = "512-1k"
        elif family == "boundary":
            agent_type = "hybrid"
            query = "判断这是部署问题还是模型问题，并给出最短下一步。"
            target = "mixed"
            sla_ms = 2000
            input_bucket = "512-2k"
            output_bucket = "0-256"
        else:
            agent_type = "thinking"
            query = (
                "请分析以下 Agent PD Disaggregation 方案的瓶颈，并按优先级给出改进策略。"
                + " 背景：多模态请求包含视觉编码、长上下文 Prefill、Decode 流式输出、工具调用和跨步骤 KV 复用。" * 48
            )
            target = "high_prefill_pool"
            sla_ms = 15000
            input_bucket = "8k-16k" if family == "thinking_long_context" else "4k-8k"
            output_bucket = "1k-2k" if family == "thinking_long_output" else "512-1k"
        task_id = f"{family}_{idx:04d}"
        image_path = str(image) if image is not None else None
        row = {
            "task_id": task_id,
            "user_query": query,
            "messages": _message(query, image_path),
            "image_paths": [image_path] if image_path else [],
            "reuse_group_id": reuse_group,
            "agent_type": agent_type,
            "routing_target": target,
            "context_length_level": "long" if "thinking" in family else ("medium" if family == "hybrid" else "short"),
            "input_tokens_bucket": input_bucket,
            "expected_output_tokens_bucket": output_bucket,
            "latency_sensitivity": "high" if agent_type == "interactive" else "medium",
            "quality_priority": "latency" if agent_type == "interactive" else ("balanced" if agent_type == "hybrid" else "accuracy"),
            "sla_ms": sla_ms,
            "expected_cache_key": _stable_key((image_path or "text") + ":" + reuse_group),
        }
        rows.append(row)

    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")
    summary = {
        "output": str(out),
        "samples": len(rows),
        "dataset_root": str(root) if root is not None else None,
        "image_assets": len(images),
        "by_agent_type": {},
        "by_routing_target": {},
    }
    for row in rows:
        summary["by_agent_type"][row["agent_type"]] = summary["by_agent_type"].get(row["agent_type"], 0) + 1
        summary["by_routing_target"][row["routing_target"]] = summary["by_routing_target"].get(row["routing_target"], 0) + 1
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Qwen-VL EPD evaluation JSONL dataset")
    ap.add_argument("--dataset-root", default=None)
    ap.add_argument("--output", required=True)
    ap.add_argument("--samples", type=int, default=60)
    ap.add_argument("--seed", type=int, default=20260709)
    args = ap.parse_args()
    print(json.dumps(build_dataset(dataset_root=args.dataset_root, output=args.output, samples=args.samples, seed=args.seed), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
