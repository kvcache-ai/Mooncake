"""Simulate KVCache retrieval scheduling strategies."""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import numpy as np

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from benchmark.metrics import (
    ensure_dir,
    percentile,
    safe_mean,
    setup_logger,
    write_csv,
)

PROJECT_ROOT = _ROOT

SUMMARY_FIELDS = [
    "scheduler",
    "avg_retrieval_latency_ms",
    "p50_latency_ms",
    "p95_latency_ms",
    "p99_latency_ms",
    "remote_traffic_mb",
    "hotspot_ratio",
    "avg_source_load",
    "max_source_load",
    "num_requests",
]

DECISION_FIELDS = [
    "request_id",
    "scheduler",
    "block_id",
    "chosen_source",
    "fetch_latency_ms",
    "size_mb",
    "is_remote",
    "score",
]


@dataclass
class KVBlock:
    block_id: int
    prefix_id: int
    reuse_count: int
    importance_score: float
    location: int
    replica_list: List[int]
    estimated_fetch_latency_ms: Dict[int, float]
    size_mb: float
    source_node: int


@dataclass
class Request:
    request_id: int
    local_node: int
    block_ids: List[int]


@dataclass
class SimResult:
    scheduler: str
    latencies: List[float] = field(default_factory=list)
    remote_traffic_mb: float = 0.0
    source_loads: Dict[int, float] = field(default_factory=dict)
    decisions: List[dict] = field(default_factory=list)


def generate_blocks(
    rng: np.random.Generator,
    num_blocks: int,
    num_nodes: int,
) -> List[KVBlock]:
    blocks: List[KVBlock] = []
    for i in range(num_blocks):
        source = int(rng.integers(0, num_nodes))
        # Hot prefixes get higher reuse / importance
        prefix_id = int(rng.integers(0, max(1, num_blocks // 20)))
        reuse = int(rng.integers(0, 50))
        if prefix_id < 5:
            reuse += int(rng.integers(20, 100))
        importance = float(rng.uniform(0.1, 1.0))
        if prefix_id < 5:
            importance = min(1.0, importance + 0.3)

        n_replicas = int(rng.integers(1, min(4, num_nodes) + 1))
        replica_candidates = list(range(num_nodes))
        rng.shuffle(replica_candidates)
        replicas = sorted(set([source] + replica_candidates[: n_replicas - 1]))

        fetch_lat: Dict[int, float] = {}
        for node in replicas:
            base = 1.0 if node == source else float(rng.uniform(2.0, 25.0))
            # Distance-ish penalty
            dist = abs(node - source)
            fetch_lat[node] = base + 0.5 * dist + float(rng.uniform(0, 2))

        size_mb = float(rng.uniform(0.5, 16.0))
        blocks.append(
            KVBlock(
                block_id=i,
                prefix_id=prefix_id,
                reuse_count=reuse,
                importance_score=importance,
                location=source,
                replica_list=replicas,
                estimated_fetch_latency_ms=fetch_lat,
                size_mb=size_mb,
                source_node=source,
            )
        )
    return blocks


def generate_requests(
    rng: np.random.Generator,
    num_requests: int,
    num_nodes: int,
    num_blocks: int,
    blocks_per_request: Tuple[int, int] = (1, 5),
) -> List[Request]:
    requests: List[Request] = []
    lo, hi = blocks_per_request
    for rid in range(num_requests):
        local = int(rng.integers(0, num_nodes))
        n = int(rng.integers(lo, hi + 1))
        bids = [int(x) for x in rng.integers(0, num_blocks, size=n)]
        requests.append(Request(request_id=rid, local_node=local, block_ids=bids))
    return requests


def _choose_random(
    block: KVBlock,
    local_node: int,
    rng: np.random.Generator,
) -> Tuple[int, float, float]:
    src = int(rng.choice(block.replica_list))
    lat = block.estimated_fetch_latency_ms[src]
    return src, lat, 0.0


def _choose_nearest(block: KVBlock, local_node: int) -> Tuple[int, float, float]:
    # Prefer local replica; else lowest fetch latency
    if local_node in block.replica_list:
        return local_node, block.estimated_fetch_latency_ms[local_node], 0.0
    src = min(block.replica_list, key=lambda n: block.estimated_fetch_latency_ms[n])
    return src, block.estimated_fetch_latency_ms[src], 0.0


def _choose_reuse_aware(block: KVBlock, local_node: int) -> Tuple[int, float, float]:
    # Prefer replicas with lower latency, bias toward high-reuse blocks staying local
    if local_node in block.replica_list and block.reuse_count >= 30:
        return local_node, block.estimated_fetch_latency_ms[local_node], float(block.reuse_count)
    if local_node in block.replica_list:
        return local_node, block.estimated_fetch_latency_ms[local_node], float(block.reuse_count)
    src = min(block.replica_list, key=lambda n: block.estimated_fetch_latency_ms[n])
    return src, block.estimated_fetch_latency_ms[src], float(block.reuse_count)


def _choose_cachepilot(
    block: KVBlock,
    local_node: int,
    *,
    alpha: float,
    beta: float,
    gamma: float,
    max_reuse: float,
    max_lat: float,
    source_loads: Dict[int, float],
) -> Tuple[int, float, float]:
    best_src = block.replica_list[0]
    best_score = -1e18
    best_lat = block.estimated_fetch_latency_ms[best_src]
    norm_reuse = block.reuse_count / max_reuse if max_reuse > 0 else 0.0

    for src in block.replica_list:
        lat = block.estimated_fetch_latency_ms[src]
        # Load-aware penalty: prefer less loaded sources
        load = source_loads.get(src, 0.0)
        load_penalty = 0.05 * load
        local_bonus = 0.15 if src == local_node else 0.0
        norm_lat = lat / max_lat if max_lat > 0 else 0.0
        score = (
            alpha * block.importance_score
            + beta * norm_reuse
            - gamma * norm_lat
            - load_penalty
            + local_bonus
        )
        if score > best_score:
            best_score = score
            best_src = src
            best_lat = lat
    return best_src, best_lat, best_score


def run_scheduler(
    name: str,
    blocks: Sequence[KVBlock],
    requests: Sequence[Request],
    rng: np.random.Generator,
    *,
    alpha: float,
    beta: float,
    gamma: float,
    num_nodes: int,
) -> SimResult:
    result = SimResult(scheduler=name, source_loads={i: 0.0 for i in range(num_nodes)})
    max_reuse = max((b.reuse_count for b in blocks), default=1)
    max_lat = max(
        (lat for b in blocks for lat in b.estimated_fetch_latency_ms.values()),
        default=1.0,
    )
    block_map = {b.block_id: b for b in blocks}

    for req in requests:
        for bid in req.block_ids:
            block = block_map[bid]
            if name == "Random":
                src, lat, score = _choose_random(block, req.local_node, rng)
            elif name == "Nearest":
                src, lat, score = _choose_nearest(block, req.local_node)
            elif name == "Reuse-aware":
                src, lat, score = _choose_reuse_aware(block, req.local_node)
            elif name == "CachePilot":
                src, lat, score = _choose_cachepilot(
                    block,
                    req.local_node,
                    alpha=alpha,
                    beta=beta,
                    gamma=gamma,
                    max_reuse=float(max_reuse),
                    max_lat=float(max_lat),
                    source_loads=result.source_loads,
                )
            else:
                raise ValueError(f"Unknown scheduler: {name}")

            is_remote = src != req.local_node
            result.latencies.append(lat)
            result.source_loads[src] = result.source_loads.get(src, 0.0) + 1.0
            if is_remote:
                result.remote_traffic_mb += block.size_mb

            result.decisions.append(
                {
                    "request_id": req.request_id,
                    "scheduler": name,
                    "block_id": bid,
                    "chosen_source": src,
                    "fetch_latency_ms": round(lat, 6),
                    "size_mb": round(block.size_mb, 6),
                    "is_remote": int(is_remote),
                    "score": round(float(score), 6),
                }
            )
    return result


def summarize(result: SimResult, num_requests: int) -> dict:
    loads = list(result.source_loads.values())
    avg_load = safe_mean(loads) or 0.0
    max_load = max(loads) if loads else 0.0
    hotspot = (max_load / avg_load) if avg_load > 0 else 0.0
    return {
        "scheduler": result.scheduler,
        "avg_retrieval_latency_ms": round(safe_mean(result.latencies) or 0.0, 6),
        "p50_latency_ms": round(percentile(result.latencies, 50) or 0.0, 6),
        "p95_latency_ms": round(percentile(result.latencies, 95) or 0.0, 6),
        "p99_latency_ms": round(percentile(result.latencies, 99) or 0.0, 6),
        "remote_traffic_mb": round(result.remote_traffic_mb, 6),
        "hotspot_ratio": round(hotspot, 6),
        "avg_source_load": round(avg_load, 6),
        "max_source_load": round(max_load, 6),
        "num_requests": num_requests,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate KVCache retrieval schedulers (Random/Nearest/Reuse-aware/CachePilot)."
    )
    parser.add_argument("--num-blocks", type=int, default=1000)
    parser.add_argument("--num-requests", type=int, default=500)
    parser.add_argument("--num-nodes", type=int, default=8)
    parser.add_argument("--alpha", type=float, default=0.5)
    parser.add_argument("--beta", type=float, default=0.3)
    parser.add_argument("--gamma", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-csv",
        default="results/csv/retrieval_scheduler.csv",
    )
    parser.add_argument(
        "--decision-log",
        default="results/logs/retrieval_scheduler_decisions.csv",
    )
    parser.add_argument(
        "--log-file",
        default="results/logs/retrieval_scheduler.log",
    )
    args = parser.parse_args()

    log_file = Path(args.log_file)
    if not log_file.is_absolute():
        log_file = PROJECT_ROOT / log_file
    ensure_dir(log_file.parent)
    logger = setup_logger(log_file, name="retrieval_scheduler")

    rng = np.random.default_rng(args.seed)
    logger.info(
        "Generating workload: blocks=%s requests=%s nodes=%s seed=%s",
        args.num_blocks,
        args.num_requests,
        args.num_nodes,
        args.seed,
    )
    blocks = generate_blocks(rng, args.num_blocks, args.num_nodes)
    requests = generate_requests(rng, args.num_requests, args.num_nodes, args.num_blocks)

    schedulers = ["Random", "Nearest", "Reuse-aware", "CachePilot"]
    # Independent RNG streams per scheduler; deterministic offset (not hash()).
    _seed_offsets = {
        "Random": 1,
        "Nearest": 2,
        "Reuse-aware": 3,
        "CachePilot": 4,
    }
    summary_rows = []
    all_decisions: List[dict] = []

    for name in schedulers:
        child_rng = np.random.default_rng(args.seed + _seed_offsets[name])
        result = run_scheduler(
            name,
            blocks,
            requests,
            child_rng,
            alpha=args.alpha,
            beta=args.beta,
            gamma=args.gamma,
            num_nodes=args.num_nodes,
        )
        row = summarize(result, args.num_requests)
        summary_rows.append(row)
        all_decisions.extend(result.decisions)
        logger.info(
            "%s: avg_lat=%.3f p99=%.3f remote_mb=%.2f hotspot=%.3f",
            name,
            row["avg_retrieval_latency_ms"],
            row["p99_latency_ms"],
            row["remote_traffic_mb"],
            row["hotspot_ratio"],
        )

    output_csv = Path(args.output_csv)
    if not output_csv.is_absolute():
        output_csv = PROJECT_ROOT / output_csv
    write_csv(output_csv, summary_rows, SUMMARY_FIELDS, append=False)
    logger.info("Wrote summary to %s", output_csv)

    decision_log = Path(args.decision_log)
    if not decision_log.is_absolute():
        decision_log = PROJECT_ROOT / decision_log
    ensure_dir(decision_log.parent)
    with decision_log.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
        writer.writeheader()
        writer.writerows(all_decisions)
    logger.info("Wrote %d decisions to %s", len(all_decisions), decision_log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
