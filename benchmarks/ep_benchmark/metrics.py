"""Metrics computation and JSON output assembly for the EP benchmark."""

import json
from typing import Any, Optional

import numpy as np
import torch


def compute_percentiles(values) -> dict[str, float]:
    """Compute p50, p90, p99, p999, and mean from latency values (in ms)."""
    if len(values) == 0:
        return {"p50": 0.0, "p90": 0.0, "p99": 0.0, "p999": 0.0, "mean": 0.0}
    arr = np.asarray(values, dtype=np.float64)
    return {
        "p50": float(np.percentile(arr, 50)),
        "p90": float(np.percentile(arr, 90)),
        "p99": float(np.percentile(arr, 99)),
        "p999": float(np.percentile(arr, 99.9)),
        "mean": float(arr.mean()),
    }


def compute_global_expert_load(
    topk_idx: torch.Tensor,
    num_experts: int,
    group: Optional["torch.distributed.ProcessGroup"] = None,
) -> dict[str, float]:
    """
    Compute global expert load by all-reducing per-rank bincount.

    Each rank computes its local expert assignment counts via bincount,
    then all_reduce(sum) to get global counts.

    Returns:
        Dict with max/min/mean_tokens_per_expert and imbalance_ratio (= max/mean).
    """
    import torch.distributed as dist

    flat = topk_idx.flatten().to(torch.int64)
    local_counts = torch.bincount(flat, minlength=num_experts).to(torch.float64)

    if group is not None and dist.is_initialized():
        global_counts = local_counts.clone()
        dist.all_reduce(global_counts, op=dist.ReduceOp.SUM, group=group)
    else:
        global_counts = local_counts

    max_val = float(global_counts.max().item())
    min_val = float(global_counts.min().item())
    mean_val = float(global_counts.mean().item())
    imbalance_ratio = max_val / mean_val if mean_val > 0 else 0.0

    return {
        "max_tokens_per_expert": max_val,
        "min_tokens_per_expert": min_val,
        "mean_tokens_per_expert": mean_val,
        "imbalance_ratio": imbalance_ratio,
    }


def assemble_json_output(
    backend: str,
    world_size: int,
    num_experts: int,
    hidden_size: int,
    top_k: int,
    num_tokens: int,
    dtype: str,
    routing_mode: str,
    zero_copy: bool,
    async_finish: bool,
    return_recv_hook: bool,
    warmup_iters: int,
    iters: int,
    hot_experts: int | None = None,
    hot_fraction: float | None = None,
    zipf_alpha: float | None = None,
    dispatch_latencies_ms: list[float] | None = None,
    combine_latencies_ms: list[float] | None = None,
    e2e_latencies_ms: list[float] | None = None,
    expert_load: dict[str, float] | None = None,
    pg_backend: str = "nccl",
) -> dict[str, Any]:
    """Assemble the final JSON-serializable output dict matching the RFC schema."""
    result = {
        "benchmark": "mooncake_ep",
        "backend": backend,
        "world_size": world_size,
        "num_experts": num_experts,
        "hidden_size": hidden_size,
        "top_k": top_k,
        "num_tokens": num_tokens,
        "dtype": dtype,
        "routing_mode": routing_mode,
        "zero_copy": zero_copy,
        "async_finish": async_finish,
        "return_recv_hook": return_recv_hook,
        "warmup_iters": warmup_iters,
        "iters": iters,
        "pg_backend": pg_backend,
    }

    if routing_mode == "k_hot":
        result["hot_experts"] = hot_experts
        result["hot_fraction"] = hot_fraction
    elif routing_mode == "zipf":
        result["zipf_alpha"] = zipf_alpha

    dispatch_pct = compute_percentiles(dispatch_latencies_ms or [])
    combine_pct = compute_percentiles(combine_latencies_ms or [])
    e2e_pct = compute_percentiles(e2e_latencies_ms or [])

    mean_e2e_ms = e2e_pct["mean"]
    total_tokens = num_tokens * world_size
    tokens_per_second = (
        total_tokens / (mean_e2e_ms / 1000.0) if mean_e2e_ms > 0 else 0.0
    )

    result["metrics"] = {
        "dispatch_latency_ms": dispatch_pct,
        "combine_latency_ms": combine_pct,
        "end_to_end_latency_ms": e2e_pct,
        "tokens_per_second": tokens_per_second,
        "expert_load": expert_load
        or {
            "max_tokens_per_expert": 0,
            "min_tokens_per_expert": 0,
            "mean_tokens_per_expert": 0,
            "imbalance_ratio": 0,
        },
    }

    return result


def write_json_output(data: dict[str, Any], path: str) -> None:
    """Write the JSON output to a file with pretty formatting."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
