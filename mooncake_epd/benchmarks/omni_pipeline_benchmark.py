#!/usr/bin/env python3
"""Measure the CPU Omni process-pipeline transport without performance claims.

It is an instrumented compatibility benchmark only.  A result from the POSIX
SHM synthetic pipeline cannot support an RDMA, CUDA IPC, real-Omni semantic,
or throughput-improvement claim.  Unsupported transports are emitted as
machine-readable skips rather than silently rerouted to TCP or SHM.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Sequence

from mooncake_epd.scripts.run_omni_worker_pipeline_e2e import (
    _KNOWN_TRANSPORTS,
    run_pipeline,
)


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * float(q))))
    return ordered[index]


def run_benchmark(
    *,
    transport: str = "posix_shm",
    warmup: int = 1,
    rounds: int = 3,
    elements: int = 64,
    queue_capacity: int = 4,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """Run the same worker pipeline repeatedly and retain raw per-round data."""

    if int(rounds) <= 0:
        raise ValueError("rounds must be positive")
    for _ in range(max(0, int(warmup))):
        warm = run_pipeline(
            transport=transport,
            elements=elements,
            queue_capacity=queue_capacity,
            timeout=timeout,
        )
        if warm["status"] != "ok":
            return {
                **warm,
                "benchmark": "omni_pipeline_cpu_compatibility",
                "rounds": [],
                "claim_supported": False,
            }

    results = []
    for index in range(int(rounds)):
        result = run_pipeline(
            transport=transport,
            elements=elements,
            queue_capacity=queue_capacity,
            timeout=timeout,
        )
        if result["status"] != "ok":
            return {
                **result,
                "benchmark": "omni_pipeline_cpu_compatibility",
                "rounds": results,
                "claim_supported": False,
            }
        results.append(result)

    wall_ms = [float(item["wall_ms"]) for item in results]
    pipeline_wall_ms = [float(item["pipeline_timing_ms"].get("wall_ms", 0.0)) for item in results]
    return {
        "status": "ok",
        "benchmark": "omni_pipeline_cpu_compatibility",
        "claim_supported": False,
        "claim_reason": (
            "synthetic CPU POSIX-SHM process measurements are not a real Omni "
            "semantic, GPU, RDMA, or throughput-improvement result"
        ),
        "requested_transport": str(transport),
        "actual_transport": str(transport),
        "rdma_evidence": None,
        "warmup": int(warmup),
        "round_count": len(results),
        "latency_ms": {
            "wall_mean": statistics.fmean(wall_ms),
            "wall_p50": _percentile(wall_ms, 0.50),
            "wall_p95": _percentile(wall_ms, 0.95),
            "pipeline_mean": statistics.fmean(pipeline_wall_ms),
        },
        "rounds": results,
        "created_at_unix": time.time(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=_KNOWN_TRANSPORTS, default="posix_shm")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--elements", type=int, default=64)
    parser.add_argument("--queue-capacity", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args(argv)
    result = run_benchmark(
        transport=args.transport,
        warmup=args.warmup,
        rounds=args.rounds,
        elements=args.elements,
        queue_capacity=args.queue_capacity,
        timeout=args.timeout,
    )
    if args.json_out:
        destination = Path(args.json_out).expanduser()
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
