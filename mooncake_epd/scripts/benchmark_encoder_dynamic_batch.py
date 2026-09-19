#!/usr/bin/env python3
"""Benchmark the real online Encoder dynamic-batching data path.

The benchmark sends real multimodal dataset requests to ``/describe`` and
explicitly releases every returned GPU bundle through ``/discard_direct``.
It never instantiates a model or substitutes transport/model execution.  The
excluded warmup, deterministic closed-loop concurrency, raw samples, service
counter deltas, process identity, and GPU snapshots make control/treatment
runs directly auditable.
"""

from __future__ import annotations

import argparse
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from typing import Any
from urllib.parse import urlsplit

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _load_dataset_requests,
    _stats,
)


_THREAD_LOCAL = threading.local()


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        _THREAD_LOCAL.session = session
    return session


def _get_json(url: str, timeout_s: float) -> dict[str, Any]:
    response = _session().get(url, timeout=timeout_s)
    response.raise_for_status()
    return dict(response.json())


def _nvidia_smi_snapshot() -> list[dict[str, Any]]:
    command = [
        "nvidia-smi",
        "--query-gpu=index,uuid,name,memory.total,memory.used,memory.free,utilization.gpu",
        "--format=csv,noheader,nounits",
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception as exc:
        return [{"error": f"{type(exc).__name__}: {exc}"}]
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) != 7:
            continue
        rows.append(
            {
                "index": int(parts[0]),
                "uuid": parts[1],
                "name": parts[2],
                "memory_total_mib": int(parts[3]),
                "memory_used_mib": int(parts[4]),
                "memory_free_mib": int(parts[5]),
                "utilization_gpu_pct": int(parts[6]),
            }
        )
    return rows


def _port_pid(port: int) -> int | None:
    completed = subprocess.run(
        ["fuser", "-n", "tcp", str(port)],
        capture_output=True,
        text=True,
        check=False,
    )
    for token in f"{completed.stdout} {completed.stderr}".split():
        if token.isdigit():
            return int(token)
    return None


def _counter_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    scalar_keys = (
        "submitted",
        "completed",
        "batches",
        "singleton_batches",
        "multi_item_batches",
        "queue_full_waits",
        "failures",
        "queue_wait_ms_total",
        "batch_encode_ms_total",
    )
    delta: dict[str, Any] = {
        key: float(after.get(key, 0) or 0) - float(before.get(key, 0) or 0)
        for key in scalar_keys
    }
    before_counts = dict(before.get("batch_size_counts") or {})
    after_counts = dict(after.get("batch_size_counts") or {})
    delta["batch_size_counts"] = {
        key: int(after_counts.get(key, 0) or 0) - int(before_counts.get(key, 0) or 0)
        for key in sorted(set(before_counts) | set(after_counts), key=int)
        if int(after_counts.get(key, 0) or 0) - int(before_counts.get(key, 0) or 0)
    }
    completed = int(delta["completed"])
    batches = int(delta["batches"])
    delta["avg_completed_batch_size"] = completed / batches if batches else 0.0
    delta["avg_queue_wait_ms"] = (
        float(delta["queue_wait_ms_total"]) / completed if completed else 0.0
    )
    delta["avg_batch_encode_ms"] = (
        float(delta["batch_encode_ms_total"]) / batches if batches else 0.0
    )
    return delta


def _request_payload(entry: dict[str, Any], sequence: int) -> dict[str, Any]:
    payload = copy.deepcopy(entry["request"])
    metadata = dict(payload.get("metadata") or {})
    workflow = str(metadata.get("workflow_id") or entry["sample"].get("sample_id") or "encoder")
    metadata["workflow_id"] = f"{workflow}-encoder-bench-{sequence}"
    metadata["benchmark_sequence"] = int(sequence)
    payload["metadata"] = metadata
    return payload


def _execute(
    *,
    endpoint: str,
    payload: dict[str, Any],
    sequence: int,
    timeout_s: float,
    measured_origin: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    record: dict[str, Any] = {
        "sequence": int(sequence),
        "workflow_id": (payload.get("metadata") or {}).get("workflow_id"),
        "started_offset_ms": (started - measured_origin) * 1000.0,
        "success": False,
    }
    ticket: str | None = None
    try:
        response = _session().post(endpoint + "/describe", json=payload, timeout=timeout_s)
        describe_done = time.perf_counter()
        record["http_status"] = int(response.status_code)
        record["describe_latency_ms"] = (describe_done - started) * 1000.0
        response.raise_for_status()
        body = dict(response.json())
        ticket = str(body.get("ticket") or "")
        if not ticket:
            raise RuntimeError("/describe returned no ticket")
        record.update(
            {
                "ticket": ticket,
                "described_records": int(body.get("count", 0) or 0),
                "server_encode_ms": float(body.get("encode_time_ms", 0.0) or 0.0),
                "server_total_ms": float(body.get("total_time_ms", 0.0) or 0.0),
                "descriptors": list(body.get("descriptors") or []),
                "feature_bundle_cache": dict(body.get("feature_bundle_cache") or {}),
                "feature_bundle_outcomes": list(
                    body.get("feature_bundle_outcomes") or []
                ),
            }
        )
        cleanup_started = time.perf_counter()
        cleanup = _session().post(
            endpoint + "/discard_direct",
            json={"ticket": ticket},
            timeout=timeout_s,
        )
        cleanup_done = time.perf_counter()
        record["discard_latency_ms"] = (cleanup_done - cleanup_started) * 1000.0
        cleanup.raise_for_status()
        cleanup_body = dict(cleanup.json())
        if cleanup_body.get("discarded") != [ticket]:
            raise RuntimeError(f"ticket cleanup mismatch: {cleanup_body}")
        record["released_bytes"] = int(cleanup_body.get("released_bytes", 0) or 0)
        record["visibility_sync_ms"] = float(
            cleanup_body.get("visibility_sync_ms", 0.0) or 0.0
        )
        record["success"] = True
        record["completed_offset_ms"] = (cleanup_done - measured_origin) * 1000.0
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["completed_offset_ms"] = (time.perf_counter() - measured_origin) * 1000.0
        if ticket:
            try:
                cleanup = _session().post(
                    endpoint + "/discard_direct",
                    json={"ticket": ticket},
                    timeout=timeout_s,
                )
                cleanup.raise_for_status()
                cleanup_body = dict(cleanup.json())
                if ticket not in list(cleanup_body.get("discarded") or []) and ticket not in list(
                    cleanup_body.get("unknown") or []
                ):
                    raise RuntimeError(f"best-effort ticket cleanup mismatch: {cleanup_body}")
            except Exception as cleanup_exc:
                record["cleanup_error"] = (
                    f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                )
    return record


def _run_requests(
    *,
    endpoint: str,
    entries: list[dict[str, Any]],
    concurrency: int,
    timeout_s: float,
    sequence_base: int,
) -> tuple[list[dict[str, Any]], float]:
    origin = time.perf_counter()
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(
                _execute,
                endpoint=endpoint,
                payload=_request_payload(entry, sequence_base + index),
                sequence=sequence_base + index,
                timeout_s=timeout_s,
                measured_origin=origin,
            )
            for index, entry in enumerate(entries)
        ]
        for future in as_completed(futures):
            records.append(future.result())
    elapsed_s = time.perf_counter() - origin
    records.sort(key=lambda item: int(item["sequence"]))
    return records, elapsed_s


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://127.0.0.1:8330")
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-chat-split", default="dev-small")
    parser.add_argument("--dataset-family", default="W0")
    parser.add_argument("--model", required=True)
    parser.add_argument("--requests", type=int, default=16)
    parser.add_argument("--warmup-requests", type=int, default=2)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--image-max-pixels", type=int, default=1003520)
    parser.add_argument(
        "--allow-feature-cache",
        action="store_true",
        help="Allow and report real Encoder FeatureStore hits instead of requiring cache-off A/B.",
    )
    parser.add_argument(
        "--expected-batcher-state",
        choices=["enabled", "disabled", "any"],
        default="enabled",
        help=(
            "Fail closed when the service dynamic-batcher state does not match "
            "the benchmark arm. Baseline/executor experiments must explicitly "
            "select disabled instead of silently passing without batching."
        ),
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.requests <= 0 or args.concurrency <= 0 or args.warmup_requests < 0:
        parser.error("requests/concurrency must be positive and warmup non-negative")

    endpoint = args.endpoint.rstrip("/")
    needed = max(args.requests, args.warmup_requests)
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

    health_initial = _get_json(endpoint + "/health", args.request_timeout)
    if str(health_initial.get("publish_backend")) != "direct_engine":
        raise RuntimeError("benchmark requires Encoder publish_backend=direct_engine")
    initial_batcher_enabled = bool(
        (health_initial.get("qwen3_dynamic_batcher") or {}).get("enabled")
    )
    if args.expected_batcher_state == "enabled" and not initial_batcher_enabled:
        raise RuntimeError("benchmark requires qwen3_dynamic_batcher.enabled=true")
    if args.expected_batcher_state == "disabled" and initial_batcher_enabled:
        raise RuntimeError("baseline benchmark requires qwen3_dynamic_batcher.enabled=false")

    warmup_records: list[dict[str, Any]] = []
    warmup_elapsed_s = 0.0
    if args.warmup_requests:
        warmup_records, warmup_elapsed_s = _run_requests(
            endpoint=endpoint,
            entries=entries[-args.warmup_requests :],
            concurrency=1,
            timeout_s=args.request_timeout,
            sequence_base=-args.warmup_requests,
        )
        if not all(record.get("success") for record in warmup_records):
            raise RuntimeError(f"warmup failed: {warmup_records}")

    health_before = _get_json(endpoint + "/health", args.request_timeout)
    gpu_before = _nvidia_smi_snapshot()
    records, elapsed_s = _run_requests(
        endpoint=endpoint,
        entries=entries[: args.requests],
        concurrency=args.concurrency,
        timeout_s=args.request_timeout,
        sequence_base=0,
    )
    gpu_after = _nvidia_smi_snapshot()
    health_after = _get_json(endpoint + "/health", args.request_timeout)

    successes = [record for record in records if record.get("success")]
    failures = [record for record in records if not record.get("success")]
    batcher_before = dict(health_before.get("qwen3_dynamic_batcher") or {})
    batcher_after = dict(health_after.get("qwen3_dynamic_batcher") or {})
    batcher_delta = (
        _counter_delta(batcher_before, batcher_after)
        if batcher_after.get("enabled")
        else {"enabled": False}
    )
    pending_final = dict(health_after.get("pending_direct_bundles") or {})
    validation_failures: list[str] = []
    if failures:
        validation_failures.append(f"request failures={len(failures)}")
    if int(pending_final.get("tickets", 0) or 0) != 0:
        validation_failures.append(
            f"pending direct tickets={pending_final.get('tickets')} expected=0"
        )
    if (
        not args.allow_feature_cache
        and bool((health_after.get("feature_bundle_cache") or {}).get("enabled"))
    ):
        validation_failures.append("feature bundle cache must be disabled for paired A/B")
    if batcher_after.get("enabled") and int(batcher_delta.get("failures", 0) or 0):
        validation_failures.append(
            f"dynamic batch failures={batcher_delta.get('failures')} expected=0"
        )
    if args.expected_batcher_state == "enabled":
        if not batcher_after.get("enabled"):
            validation_failures.append("dynamic batcher became disabled during benchmark")
        if int(batcher_delta.get("submitted", 0) or 0) <= 0:
            validation_failures.append("dynamic batch submitted delta must be positive")
        if int(batcher_delta.get("batches", 0) or 0) <= 0:
            validation_failures.append("dynamic batch count delta must be positive")
    elif args.expected_batcher_state == "disabled" and batcher_after.get("enabled"):
        validation_failures.append("dynamic batcher became enabled during baseline benchmark")

    parsed_endpoint = urlsplit(endpoint)
    endpoint_port = parsed_endpoint.port
    if endpoint_port is None:
        endpoint_port = 443 if parsed_endpoint.scheme == "https" else 80

    payload = {
        "schema_version": "encoder-dynamic-batch-benchmark-v1",
        "benchmark": {
            "real_model": True,
            "real_dataset": True,
            "mock": False,
            "closed_loop": True,
            "warmup_excluded": True,
            "endpoint": endpoint,
            "model": args.model,
            "dataset_root": str(Path(args.dataset_root).resolve()),
            "dataset_chat_split": args.dataset_chat_split,
            "dataset_family": args.dataset_family,
            "requests": int(args.requests),
            "warmup_requests": int(args.warmup_requests),
            "concurrency": int(args.concurrency),
            "image_max_pixels": int(args.image_max_pixels),
            "feature_cache_allowed": bool(args.allow_feature_cache),
            "expected_batcher_state": args.expected_batcher_state,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "encoder_host": parsed_endpoint.hostname,
            "encoder_port": int(endpoint_port),
            "encoder_pid": _port_pid(int(endpoint_port)),
            "started_at_unix_s": time.time() - elapsed_s,
        },
        "summary": {
            "elapsed_s": float(elapsed_s),
            "successes": len(successes),
            "failures": len(failures),
            "success_rate": len(successes) / len(records) if records else 0.0,
            "throughput_rps": len(successes) / elapsed_s if elapsed_s else 0.0,
            "describe_latency_ms": _stats(
                [float(record["describe_latency_ms"]) for record in successes]
            ),
            "server_encode_ms": _stats(
                [float(record["server_encode_ms"]) for record in successes]
            ),
            "server_total_ms": _stats(
                [float(record["server_total_ms"]) for record in successes]
            ),
            "discard_latency_ms": _stats(
                [float(record["discard_latency_ms"]) for record in successes]
            ),
            "visibility_sync_ms": _stats(
                [float(record["visibility_sync_ms"]) for record in successes]
            ),
            "released_bytes": sum(int(record["released_bytes"]) for record in successes),
        },
        "dynamic_batcher_delta": batcher_delta,
        "feature_bundle_cache_delta": {
            key: int((health_after.get("feature_bundle_cache") or {}).get(key, 0) or 0)
            - int((health_before.get("feature_bundle_cache") or {}).get(key, 0) or 0)
            for key in ("hits", "misses", "computes", "evictions")
        },
        "health_initial": health_initial,
        "health_before": health_before,
        "health_after": health_after,
        "gpu_before": gpu_before,
        "gpu_after": gpu_after,
        "warmup": {
            "elapsed_s": float(warmup_elapsed_s),
            "records": warmup_records,
        },
        "records": records,
        "dataset_skipped": skipped,
        "validation": {
            "passed": not validation_failures,
            "failures": validation_failures,
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    if validation_failures:
        print(json.dumps({"validation_failures": validation_failures}, indent=2), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
