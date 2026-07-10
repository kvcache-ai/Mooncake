#!/usr/bin/env python3
"""Build a dedicated Agent PD Disaggregation scheduling dataset.

The dataset is intentionally scheduling-oriented rather than a generic NLP
benchmark.  Every row carries labels that exercise the PD resource policy:

* thinking: high Prefill pressure, quality-first;
* interactive: short latency-sensitive turns;
* thinking_decode_heavy: long output stability;
* hybrid: low-latency conclusion + high-prefill detailed reasoning;
* boundary: ambiguous/interfering samples for classifier robustness.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


LONG_CONTEXT = (
    "Mooncake EPD-Serve 将多模态推理拆分为 Encoder、Prefill、Decode 三阶段。"
    "系统需要在高并发下保证 TTFT、goodput、KVCache 传输稳定性、Agent 多轮状态复用、"
    "工具调用阻塞卸载、A2A handoff 和调度反压。"
)

THINKING_TEMPLATES = [
    "请阅读以下 20 页技术方案摘要，并总结架构风险、性能瓶颈和 P0/P1/P2 优化路线：{context}",
    "分析这个 vLLM 调度模块在 PD 解耦场景下是否存在队列阻塞、KV 传输拥塞和状态复用问题：{context}",
    "根据 10 个约束选择最优模型部署组合，并解释 Prefill/Decode 资源如何拆分：{context}",
    "制定一个 Agent 系统生产部署方案，包括模型、调度、监控、回滚和压测计划：{context}",
]

INTERACTIVE_TEMPLATES = [
    "继续，用更短的话说。",
    "什么是 Prefill？一句话解释。",
    "这是部署问题还是模型问题？",
    "下一步怎么做？",
    "把这句话改得更正式：我们马上修这个问题。",
]

HYBRID_TEMPLATES = [
    "先快速告诉我结论，然后详细解释为什么：{question}",
    "先给结论，如果有必要再展开分析：{question}",
    "先用一句话回答，再列出完整排查步骤：{question}",
]

BOUNDARY_TEMPLATES = [
    "快速判断：这个长上下文调度问题是否需要深入分析？{context}",
    "请先简短回答，然后如果涉及风险再展开：{context}",
    "只回答是否可行，但要考虑以下复杂约束：{context}",
]


def _bucket_mid(bucket: str) -> int:
    left, right = bucket.lower().replace("k", "000").split("-", 1)
    return int((int(left) + int(right)) / 2)


def _long_context(tokens: int) -> str:
    words = LONG_CONTEXT.split()
    if not words:
        words = [LONG_CONTEXT]
    out: List[str] = []
    while len(out) < tokens:
        out.extend(words)
    return " ".join(out[:tokens])


def _row(
    *,
    task_id: str,
    user_query: str,
    agent_type: str,
    routing_target: str,
    reasoning_depth: int,
    context_length_level: str,
    input_tokens_bucket: str,
    expected_output_tokens_bucket: str,
    tool_use_required: bool,
    latency_sensitivity: str,
    quality_priority: str,
    sla_ms: int,
    category: str,
) -> Dict[str, Any]:
    return {
        "task_id": task_id,
        "user_query": user_query,
        "agent_type": agent_type,
        "routing_target": routing_target,
        "reasoning_depth": reasoning_depth,
        "context_length_level": context_length_level,
        "input_tokens_bucket": input_tokens_bucket,
        "expected_output_tokens_bucket": expected_output_tokens_bucket,
        "tool_use_required": tool_use_required,
        "latency_sensitivity": latency_sensitivity,
        "quality_priority": quality_priority,
        "sla_ms": sla_ms,
        "category": category,
        "input_tokens": _bucket_mid(input_tokens_bucket),
        "expected_output_tokens": _bucket_mid(expected_output_tokens_bucket),
    }


def build_rows(total: int, *, seed: int = 20260709) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    counts = {
        "interactive_short": round(total * 0.35),
        "thinking_long_context": round(total * 0.25),
        "thinking_long_output": round(total * 0.15),
        "hybrid": round(total * 0.15),
    }
    counts["boundary"] = max(0, total - sum(counts.values()))
    rows: List[Dict[str, Any]] = []
    for i in range(counts["interactive_short"]):
        query = rng.choice(INTERACTIVE_TEMPLATES)
        rows.append(_row(
            task_id=f"interact_{i:04d}",
            user_query=query,
            agent_type="interactive",
            routing_target="low_latency_decode_pool",
            reasoning_depth=1,
            context_length_level="short",
            input_tokens_bucket="0-512",
            expected_output_tokens_bucket="0-128",
            tool_use_required=False,
            latency_sensitivity="high",
            quality_priority="latency",
            sla_ms=800,
            category="interactive_short",
        ))
    for i in range(counts["thinking_long_context"]):
        ctx_tokens = rng.choice([8192, 12288, 16000])
        query = rng.choice(THINKING_TEMPLATES).format(context=_long_context(512))
        rows.append(_row(
            task_id=f"think_ctx_{i:04d}",
            user_query=query,
            agent_type="thinking",
            routing_target="high_prefill_pool",
            reasoning_depth=rng.choice([4, 5, 6]),
            context_length_level="long",
            input_tokens_bucket="8k-16k" if ctx_tokens >= 8192 else "4k-8k",
            expected_output_tokens_bucket="1k-2k",
            tool_use_required=rng.random() < 0.25,
            latency_sensitivity="medium",
            quality_priority="accuracy",
            sla_ms=15000,
            category="thinking_long_context",
        ))
    for i in range(counts["thinking_long_output"]):
        query = rng.choice(THINKING_TEMPLATES).format(context=_long_context(320))
        rows.append(_row(
            task_id=f"think_decode_{i:04d}",
            user_query=query,
            agent_type="thinking",
            routing_target="high_prefill_pool",
            reasoning_depth=rng.choice([4, 5]),
            context_length_level="medium",
            input_tokens_bucket="2k-4k",
            expected_output_tokens_bucket="2k-4k",
            tool_use_required=False,
            latency_sensitivity="medium",
            quality_priority="accuracy",
            sla_ms=20000,
            category="thinking_long_output",
        ))
    for i in range(counts["hybrid"]):
        question = "Agent PD Disaggregation 如何同时降低 TTFT 并保证复杂任务质量？"
        rows.append(_row(
            task_id=f"hybrid_{i:04d}",
            user_query=rng.choice(HYBRID_TEMPLATES).format(question=question),
            agent_type="hybrid",
            routing_target="mixed",
            reasoning_depth=rng.choice([3, 4]),
            context_length_level="medium",
            input_tokens_bucket="2k-4k",
            expected_output_tokens_bucket="512-1k",
            tool_use_required=False,
            latency_sensitivity="high",
            quality_priority="balanced",
            sla_ms=5000,
            category="hybrid",
        ))
    for i in range(counts["boundary"]):
        rows.append(_row(
            task_id=f"boundary_{i:04d}",
            user_query=rng.choice(BOUNDARY_TEMPLATES).format(context=_long_context(180)),
            agent_type="hybrid",
            routing_target="mixed",
            reasoning_depth=rng.choice([2, 3, 4]),
            context_length_level=rng.choice(["short", "medium", "long"]),
            input_tokens_bucket=rng.choice(["0-512", "512-2k", "2k-4k", "4k-8k"]),
            expected_output_tokens_bucket=rng.choice(["0-128", "128-512", "512-1k"]),
            tool_use_required=rng.random() < 0.35,
            latency_sensitivity=rng.choice(["high", "medium"]),
            quality_priority=rng.choice(["latency", "balanced", "accuracy"]),
            sla_ms=rng.choice([5000, 8000, 10000, 15000]),
            category="boundary",
        ))
    rng.shuffle(rows)
    return rows


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default="artifacts/agent_pd_dataset")
    ap.add_argument("--total", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20260709)
    args = ap.parse_args()
    out_dir = Path(args.output_dir).expanduser().resolve()
    rows = build_rows(max(1, int(args.total)), seed=int(args.seed))
    tasks_path = out_dir / "tasks.jsonl"
    count = write_jsonl(tasks_path, rows)
    summary: Dict[str, Any] = {
        "generated_at_unix": time.time(),
        "tasks_jsonl": str(tasks_path),
        "count": count,
        "mix": {},
    }
    for row in rows:
        summary["mix"][row["category"]] = int(summary["mix"].get(row["category"], 0)) + 1
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
