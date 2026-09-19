#!/usr/bin/env python3
"""Apply a real hot-set/scan workload to the online Qwen3-VL Encoder cache.

The runner uses the repository's admitted multimodal dataset requests, the
live Encoder HTTP service, real CUDA execution, and visibility-safe ticket
discard.  It can stop after preparing cache pressure so a full strict-EPD
benchmark can probe the same hot set without first perturbing it.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import socket
import sys
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.benchmark_encoder_dynamic_batch import (  # noqa: E402
    _get_json,
    _nvidia_smi_snapshot,
    _port_pid,
    _run_requests,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _load_dataset_requests,
    _stats,
)


def _phase_summary(records: list[dict[str, Any]], elapsed_s: float) -> dict[str, Any]:
    successes = [record for record in records if record.get("success")]
    cache_hits = []
    singleflight_waits = []
    admitted = []
    for record in successes:
        outcomes = list(record.get("feature_bundle_outcomes") or [])
        if any(bool(item.get("cache_hit")) for item in outcomes):
            cache_hits.append(record)
        if any(bool(item.get("singleflight_wait")) for item in outcomes):
            singleflight_waits.append(record)
        if any(bool(item.get("cache_admitted")) for item in outcomes):
            admitted.append(record)
    return {
        "requests": len(records),
        "successes": len(successes),
        "failures": len(records) - len(successes),
        "elapsed_s": float(elapsed_s),
        "throughput_rps": len(successes) / elapsed_s if elapsed_s else 0.0,
        "cache_hit_responses": len(cache_hits),
        "cache_hit_response_rate": len(cache_hits) / len(successes) if successes else 0.0,
        "singleflight_wait_responses": len(singleflight_waits),
        "cache_admitted_responses": len(admitted),
        "describe_latency_ms": _stats(
            [float(record["describe_latency_ms"]) for record in successes]
        ),
        "server_encode_ms": _stats(
            [float(record["server_encode_ms"]) for record in successes]
        ),
        "visibility_sync_ms": _stats(
            [float(record["visibility_sync_ms"]) for record in successes]
        ),
    }


def _numeric_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, float]:
    keys = (
        "hits",
        "misses",
        "computes",
        "evictions",
        "admissions",
        "admission_rejections",
        "capacity_rejections",
        "oversize_rejections",
        "value_evictions",
        "singleflight_waiters",
        "frequency_resets",
    )
    return {
        key: float(after.get(key, 0) or 0) - float(before.get(key, 0) or 0)
        for key in keys
    }


def _resolve_expected_hot_promotion_hits(
    *,
    hot_requests: int,
    hot_promotions: int,
    configured: int | None,
) -> int:
    total = int(hot_requests) * int(hot_promotions)
    expected = total if configured is None else int(configured)
    if expected < 0 or expected > total:
        raise ValueError(
            "expected hot-promotion hits must be within the measured phase: "
            f"expected={expected} total={total}"
        )
    return expected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8330")
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-chat-split", default="dev-small")
    parser.add_argument("--dataset-family", default="W0")
    parser.add_argument("--model", required=True)
    parser.add_argument("--hot-requests", type=int, default=8)
    parser.add_argument("--hot-promotions", type=int, default=1)
    parser.add_argument(
        "--expected-hot-promotion-hits",
        type=int,
        default=None,
        help=(
            "Exact cache-hit response count required during hot_promote. "
            "The default requires every promotion request to hit; capacity "
            "experiments may pin a lower expected count for the control arm."
        ),
    )
    parser.add_argument("--scan-requests", type=int, default=40)
    parser.add_argument("--scan-concurrency", type=int, default=8)
    parser.add_argument("--probe-concurrency", type=int, default=8)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--expected-admission-policy",
        choices=["lru", "reuse_density"],
        required=True,
    )
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--image-max-pixels", type=int, default=1003520)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if (
        args.hot_requests <= 0
        or args.hot_promotions <= 0
        or args.scan_requests <= 0
        or args.scan_concurrency <= 0
        or args.probe_concurrency <= 0
    ):
        parser.error("request counts and concurrency must be positive")
    try:
        expected_promotion_hits = _resolve_expected_hot_promotion_hits(
            hot_requests=args.hot_requests,
            hot_promotions=args.hot_promotions,
            configured=args.expected_hot_promotion_hits,
        )
    except ValueError as exc:
        parser.error(str(exc))

    needed = args.hot_requests + args.scan_requests
    entries, skipped = _load_dataset_requests(
        dataset_root=args.dataset_root,
        chat_split=args.dataset_chat_split,
        max_requests=needed,
        families=[args.dataset_family],
        model=args.model,
        max_input_len=args.max_input_len,
        request_max_tokens=args.max_tokens,
        skip_oversized=True,
        image_max_pixels=args.image_max_pixels,
    )
    if len(entries) < needed:
        raise RuntimeError(f"dataset yielded {len(entries)} requests, need {needed}")
    hot_entries = entries[: args.hot_requests]
    scan_entries = entries[args.hot_requests : needed]

    endpoint = args.endpoint.rstrip("/")
    health_initial = _get_json(endpoint + "/health", args.request_timeout)
    cache_initial = dict(health_initial.get("feature_bundle_cache") or {})
    validation_failures: list[str] = []
    if not cache_initial.get("enabled"):
        validation_failures.append("Encoder FeatureBundle cache is disabled")
    if str(cache_initial.get("admission_policy") or "lru") != args.expected_admission_policy:
        validation_failures.append(
            "admission policy mismatch: "
            f"actual={cache_initial.get('admission_policy')} "
            f"expected={args.expected_admission_policy}"
        )

    phases: dict[str, Any] = {}
    sequence = 0
    gpu_before = _nvidia_smi_snapshot()
    seed_records, seed_elapsed = _run_requests(
        endpoint=endpoint,
        entries=hot_entries,
        concurrency=1,
        timeout_s=args.request_timeout,
        sequence_base=sequence,
    )
    sequence += len(hot_entries)
    phases["hot_seed"] = {
        "summary": _phase_summary(seed_records, seed_elapsed),
        "records": seed_records,
    }

    promotion_records: list[dict[str, Any]] = []
    promotion_elapsed = 0.0
    for _ in range(args.hot_promotions):
        records, elapsed = _run_requests(
            endpoint=endpoint,
            entries=hot_entries,
            concurrency=1,
            timeout_s=args.request_timeout,
            sequence_base=sequence,
        )
        sequence += len(hot_entries)
        promotion_records.extend(records)
        promotion_elapsed += elapsed
    phases["hot_promote"] = {
        "summary": _phase_summary(promotion_records, promotion_elapsed),
        "records": promotion_records,
    }
    health_promoted = _get_json(endpoint + "/health", args.request_timeout)

    scan_records, scan_elapsed = _run_requests(
        endpoint=endpoint,
        entries=scan_entries,
        concurrency=args.scan_concurrency,
        timeout_s=args.request_timeout,
        sequence_base=sequence,
    )
    sequence += len(scan_entries)
    phases["cold_scan"] = {
        "summary": _phase_summary(scan_records, scan_elapsed),
        "records": scan_records,
    }
    health_after_scan = _get_json(endpoint + "/health", args.request_timeout)

    if not args.prepare_only:
        probe_records, probe_elapsed = _run_requests(
            endpoint=endpoint,
            entries=hot_entries,
            concurrency=args.probe_concurrency,
            timeout_s=args.request_timeout,
            sequence_base=sequence,
        )
        phases["hot_probe"] = {
            "summary": _phase_summary(probe_records, probe_elapsed),
            "records": probe_records,
        }

    health_final = _get_json(endpoint + "/health", args.request_timeout)
    gpu_after = _nvidia_smi_snapshot()
    for name, phase in phases.items():
        if int(phase["summary"]["failures"]):
            validation_failures.append(
                f"phase={name} failures={phase['summary']['failures']}"
            )
    promoted_summary = phases["hot_promote"]["summary"]
    if int(promoted_summary["cache_hit_responses"]) != expected_promotion_hits:
        validation_failures.append(
            "hot promotion exact-hit count mismatch: "
            f"hits={promoted_summary['cache_hit_responses']} "
            f"expected={expected_promotion_hits}"
        )
    pending = dict(health_final.get("pending_direct_bundles") or {})
    if int(pending.get("tickets", 0) or 0) != 0:
        validation_failures.append(f"pending direct tickets={pending.get('tickets')}")
    cache_final = dict(health_final.get("feature_bundle_cache") or {})
    if int(cache_final.get("bytes", 0) or 0) > int(cache_final.get("max_bytes", 0) or 0):
        validation_failures.append("cache exceeded configured byte capacity")
    if int(cache_final.get("entries", 0) or 0) > int(cache_final.get("max_entries", 0) or 0):
        validation_failures.append("cache exceeded configured entry capacity")

    payload = {
        "schema_version": "encoder-cache-pressure-real-v1",
        "benchmark": {
            "real_model": True,
            "real_dataset": True,
            "mock": False,
            "endpoint": endpoint,
            "model": args.model,
            "dataset_root": str(Path(args.dataset_root).resolve()),
            "dataset_chat_split": args.dataset_chat_split,
            "dataset_family": args.dataset_family,
            "hot_requests": int(args.hot_requests),
            "hot_promotions": int(args.hot_promotions),
            "expected_hot_promotion_hits": int(expected_promotion_hits),
            "scan_requests": int(args.scan_requests),
            "scan_concurrency": int(args.scan_concurrency),
            "probe_concurrency": int(args.probe_concurrency),
            "prepare_only": bool(args.prepare_only),
            "expected_admission_policy": args.expected_admission_policy,
            "hostname": socket.gethostname(),
            "encoder_pid": _port_pid(8330),
            "completed_at_unix_s": time.time(),
        },
        "phases": phases,
        "cache_delta": _numeric_delta(cache_initial, cache_final),
        "cache_delta_after_promotion_to_after_scan": _numeric_delta(
            dict(health_promoted.get("feature_bundle_cache") or {}),
            dict(health_after_scan.get("feature_bundle_cache") or {}),
        ),
        "health_initial": health_initial,
        "health_promoted": health_promoted,
        "health_after_scan": health_after_scan,
        "health_final": health_final,
        "gpu_before": gpu_before,
        "gpu_after": gpu_after,
        "dataset_skipped": skipped,
        "validation": {
            "passed": not validation_failures,
            "failures": validation_failures,
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "validation": payload["validation"],
                "phase_summaries": {
                    name: phase["summary"] for name, phase in phases.items()
                },
                "cache_final": cache_final,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not validation_failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
