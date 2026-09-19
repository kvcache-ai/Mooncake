#!/usr/bin/env python3
"""Benchmark a real OpenAI-compatible multimodal serving endpoint.

The runner deliberately reuses the repository's real dataset admission and
SSE timing path.  It performs excluded warmups followed by deterministic
closed-loop concurrency sweeps and persists both aggregate percentiles and raw
request records.  It does not instantiate a model, replace the endpoint, or
mock transport/model execution.
"""

from __future__ import annotations

import argparse
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import platform
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _execute_dataset_request,
    _load_dataset_requests,
    _stats,
)


def _parse_positive_ints(raw: str) -> list[int]:
    values = sorted({int(value.strip()) for value in str(raw).split(",") if value.strip()})
    if not values or any(value <= 0 for value in values):
        raise ValueError("concurrency must contain positive integers")
    return values


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


def _metrics_snapshot(url: str | None) -> dict[str, Any] | None:
    if not url:
        return None
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _request_entry(
    source: dict[str, Any],
    *,
    run_index: int,
    sequence: int,
    workflow_mode: str,
    benchmark_id: str | None = None,
) -> dict[str, Any]:
    entry = copy.deepcopy(source)
    if workflow_mode == "unique":
        request = dict(entry["request"])
        metadata = dict(request.get("metadata") or {})
        original = str(metadata.get("workflow_id") or entry["sample"].get("sample_id") or "bench")
        namespace = str(benchmark_id or "local")
        metadata["workflow_id"] = (
            f"{original}-bench{namespace}-run{run_index}-seq{sequence}"
        )
        request["metadata"] = metadata
        entry["request"] = request
        sample = dict(entry["sample"])
        sample["workflow_id"] = metadata["workflow_id"]
        entry["sample"] = sample
    return entry


def _iter_request_mm_items(request: dict[str, Any]):
    messages = request.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if isinstance(item, dict) and str(
                    item.get("type") or ""
                ).strip().lower() in {
                    "image",
                    "image_url",
                    "input_image",
                    "audio",
                    "audio_url",
                    "input_audio",
                    "video",
                    "video_url",
                    "input_video",
                }:
                    yield item


def _assign_stable_mm_uuids(
    entries: list[dict[str, Any]],
) -> int:
    """Assign content-derived UUIDs that stay stable across benchmark runs.

    Per-invocation UUIDs defeat the cache contract under test: repeated A/B
    runs would recompute identical media, evict hot feature buffers, and bias
    later blocks with allocator/cache churn.  A versioned 128-bit content
    digest gives the same media item the same identity across processes while
    keeping collision risk negligible for serving datasets.
    """

    assigned = 0
    for entry in entries:
        request = entry.get("request")
        if not isinstance(request, dict):
            continue
        for item in _iter_request_mm_items(request):
            if not item.get("uuid"):
                canonical = {
                    key: item.get(key)
                    for key in sorted(item)
                    if key != "uuid"
                }
                raw = json.dumps(
                    canonical,
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                item["uuid"] = (
                    "epd-mm-v1-" + hashlib.sha256(raw).hexdigest()[:32]
                )
            assigned += 1
    return assigned


def _compact_mm_uuid_entry(entry: dict[str, Any]) -> tuple[dict[str, Any], int]:
    compact = copy.deepcopy(entry)
    request = compact.get("request")
    if not isinstance(request, dict):
        return compact, 0
    count = 0
    for item in _iter_request_mm_items(request):
        if not item.get("uuid"):
            raise ValueError("cannot compact a multimodal item without a stable uuid")
        item_type = str(item.get("type") or "").strip().lower()
        if item_type in {"image", "image_url", "input_image"}:
            if "image_url" in item:
                item["image_url"] = None
            elif "image" in item:
                item["image"] = None
            else:
                item["url"] = None
        elif item_type in {"audio", "audio_url", "input_audio"}:
            for key in ("audio_url", "input_audio", "audio", "url"):
                if key in item:
                    item[key] = None
                    break
        elif item_type in {"video", "video_url", "input_video"}:
            for key in ("video_url", "video", "url"):
                if key in item:
                    item[key] = None
                    break
        count += 1
    return compact, count


def _target_decode_worker_entry(
    entry: dict[str, Any],
    worker_id: str,
) -> dict[str, Any]:
    targeted = copy.deepcopy(entry)
    request = dict(targeted.get("request") or {})
    metadata = dict(request.get("metadata") or {})
    metadata["mooncake_epd_decode_worker_id"] = str(worker_id)
    request["metadata"] = metadata
    targeted["request"] = request
    return targeted


def _decode_worker_ids_from_metrics(
    snapshot: dict[str, Any] | None,
) -> list[str]:
    if not isinstance(snapshot, dict):
        return []
    workers = dict(snapshot.get("workers") or {})
    worker_ids: list[str] = []
    for worker in list(workers.get("decode") or []):
        if isinstance(worker, dict):
            worker_id = str(worker.get("worker_id") or "").strip()
        else:
            # Older proxy metrics exposed worker IDs directly.
            worker_id = str(worker or "").strip()
        if worker_id and worker_id not in worker_ids:
            worker_ids.append(worker_id)
    return worker_ids


def _successful(row: dict[str, Any]) -> bool:
    return (
        200 <= int(row.get("status_code", 0) or 0) < 300
        and not row.get("error")
        and int(row.get("response_content_len", 0) or 0) > 0
        and int(row.get("completion_tokens", 0) or 0) > 0
    )


def _proxy_metric_payload(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(snapshot, dict) or snapshot.get("error"):
        return {}
    metrics = snapshot.get("metrics")
    return dict(metrics) if isinstance(metrics, dict) else dict(snapshot)


def _counter_delta(
    before: dict[str, Any],
    after: dict[str, Any],
    key: str,
) -> int:
    return int(after.get(key, 0) or 0) - int(before.get(key, 0) or 0)


def _measured_proxy_stage_timing(
    *,
    metrics_after: dict[str, Any] | None,
    runs: list[dict[str, Any]],
    workflow_mode: str,
) -> dict[str, Any]:
    """Build window-local stage timing from successful measured requests.

    Proxy histogram snapshots are process-cumulative and therefore include
    cold start, warmup, and earlier traffic. Unique benchmark workflow IDs let
    us join measured rows to bounded recent traces without changing serving.
    """

    expected = sum(int(run.get("successful", 0) or 0) for run in runs)
    evidence: dict[str, Any] = {
        "supported": False,
        "source": "metrics.request_timing_recent",
        "workflow_mode": str(workflow_mode),
        "expected_successful": expected,
        "matched_requests": 0,
        "coverage": 0.0 if expected else 1.0,
        "runs": [],
    }
    if str(workflow_mode) != "unique":
        evidence["reason"] = (
            "window-local proxy timing requires unique workflow IDs; stable IDs "
            "cannot distinguish warmup from measured requests"
        )
        return evidence

    metrics = _proxy_metric_payload(metrics_after)
    recent = metrics.get("request_timing_recent")
    if not isinstance(recent, list):
        evidence["reason"] = "proxy metrics do not expose request_timing_recent"
        return evidence

    traces_by_workflow: dict[str, list[dict[str, Any]]] = {}
    for trace in recent:
        if not isinstance(trace, dict):
            continue
        workflow_id = str(trace.get("workflow_id") or "")
        if workflow_id:
            traces_by_workflow.setdefault(workflow_id, []).append(trace)

    expected_workflow_ids = {
        str(row.get("workflow_id") or "")
        for run in runs
        for row in list(run.get("raw_results") or [])
        if _successful(row) and row.get("workflow_id")
    }
    duplicate_trace_ids = sorted(
        workflow_id
        for workflow_id, traces in traces_by_workflow.items()
        if workflow_id in expected_workflow_ids and len(traces) > 1
    )
    all_missing: list[str] = []
    total_matched = 0
    run_summaries: list[dict[str, Any]] = []
    for run in runs:
        workflow_ids = [
            str(row.get("workflow_id") or "")
            for row in list(run.get("raw_results") or [])
            if _successful(row) and row.get("workflow_id")
        ]
        matched: list[dict[str, Any]] = []
        missing: list[str] = []
        for workflow_id in workflow_ids:
            traces = traces_by_workflow.get(workflow_id, [])
            if len(traces) == 1:
                matched.append(traces[0])
            else:
                missing.append(workflow_id)

        stage_values: dict[str, list[float]] = {}
        for trace in matched:
            for stage, value in dict(trace.get("stage_ms") or {}).items():
                if isinstance(value, (int, float)):
                    stage_values.setdefault(str(stage), []).append(float(value))

        conservation_ok = sum(
            1 for trace in matched if bool(trace.get("stage_conservation_ok"))
        )
        matched_count = len(matched)
        total_matched += matched_count
        all_missing.extend(missing)
        run_summaries.append(
            {
                "concurrency": int(run.get("concurrency", 0) or 0),
                "expected_successful": len(workflow_ids),
                "matched_requests": matched_count,
                "coverage": matched_count / len(workflow_ids) if workflow_ids else 1.0,
                "missing_workflow_ids": missing,
                "critical_path_ms": _stats(
                    [
                        float(trace["critical_path_ms"])
                        for trace in matched
                        if isinstance(trace.get("critical_path_ms"), (int, float))
                    ]
                ),
                "accounted_union_ms": _stats(
                    [
                        float(trace["accounted_union_ms"])
                        for trace in matched
                        if isinstance(trace.get("accounted_union_ms"), (int, float))
                    ]
                ),
                "unattributed_ms": _stats(
                    [
                        float(trace["unattributed_ms"])
                        for trace in matched
                        if isinstance(trace.get("unattributed_ms"), (int, float))
                    ]
                ),
                "overlap_ms": _stats(
                    [
                        float(trace["overlap_ms"])
                        for trace in matched
                        if isinstance(trace.get("overlap_ms"), (int, float))
                    ]
                ),
                "stage_conservation": {
                    "ok_count": conservation_ok,
                    "failed_count": matched_count - conservation_ok,
                    "ok_rate": conservation_ok / matched_count if matched_count else None,
                },
                "stages_ms": {
                    stage: _stats(values)
                    for stage, values in sorted(stage_values.items())
                },
            }
        )

    evidence.update(
        {
            "supported": True,
            "recent_trace_records": len(recent),
            "matched_requests": total_matched,
            "coverage": total_matched / expected if expected else 1.0,
            "missing_workflow_ids": all_missing,
            "duplicate_trace_workflow_ids": duplicate_trace_ids,
            "runs": run_summaries,
        }
    )
    if total_matched < expected:
        evidence["reason"] = (
            "request_timing_recent retention did not cover every successful "
            "measured request"
        )
    return evidence


def _strict_epd_metric_gate(
    *,
    metrics_before: dict[str, Any] | None,
    metrics_after: dict[str, Any] | None,
    expected_successful: int,
    require_decode_mm_hash_cache: bool = False,
    require_client_mm_uuid_cache: bool = False,
    require_client_mm_uuid_full: bool = False,
) -> tuple[list[str], dict[str, Any]]:
    before = _proxy_metric_payload(metrics_before)
    after = _proxy_metric_payload(metrics_after)
    failures: list[str] = []
    if not before or not after:
        return ["strict EPD metrics gate requires readable before/after metrics"], {}

    exact_counters = ("requests_total", "handoff_prepared", "handoff_committed")
    deltas = {key: _counter_delta(before, after, key) for key in exact_counters}
    for key, delta in deltas.items():
        if delta != int(expected_successful):
            failures.append(
                f"metric {key} delta={delta} expected={int(expected_successful)}"
            )

    zero_counters = (
        "handoff_rolled_back",
        "fallback_batches",
        "fallback_bytes",
        "layered_receive_failures",
        "layered_transfer_failed_batches",
        "remote_transfer_backend_failures",
    )
    for key in zero_counters:
        delta = _counter_delta(before, after, key)
        deltas[key] = delta
        if delta != 0:
            failures.append(f"metric {key} delta={delta} expected=0")

    transfer_success_delta = _counter_delta(before, after, "kv_transfer_successes")
    deltas["kv_transfer_successes"] = transfer_success_delta
    if expected_successful > 0 and transfer_success_delta <= 0:
        failures.append("metric kv_transfer_successes did not increase")

    before_degrade = dict(before.get("degrade_level_counts") or {})
    after_degrade = dict(after.get("degrade_level_counts") or {})
    degrade_deltas = {
        str(level): int(after_degrade.get(level, 0) or 0)
        - int(before_degrade.get(level, 0) or 0)
        for level in set(before_degrade) | set(after_degrade)
    }
    non_none = {key: value for key, value in degrade_deltas.items() if key != "NONE" and value}
    if non_none:
        failures.append(f"non-NONE degrade metric deltas: {non_none}")

    cache = dict((metrics_after or {}).get("direct_feature_handle_cache") or {})
    if int(cache.get("inflight", 0) or 0) != 0:
        failures.append(f"direct feature handle cache inflight={cache.get('inflight')} expected=0")
    render_cache = dict((metrics_after or {}).get("prefill_render_cache") or {})
    if int(render_cache.get("inflight", 0) or 0) != 0:
        failures.append(f"prefill render cache inflight={render_cache.get('inflight')} expected=0")

    decode_mm_hash_cache: dict[str, Any] = {}
    decode_mm_hash_deltas: dict[str, Any] = {}
    if require_decode_mm_hash_cache:
        before_hash = dict((metrics_before or {}).get("decode_mm_hash_cache") or {})
        decode_mm_hash_cache = dict(
            (metrics_after or {}).get("decode_mm_hash_cache") or {}
        )
        if not bool(decode_mm_hash_cache.get("enabled")):
            failures.append("Decode MM hash cache is required but metrics report disabled")
        if not bool(decode_mm_hash_cache.get("epoch_monitor_enabled")):
            failures.append("Decode MM hash cache epoch monitor is required but disabled")
        decode_workers = _decode_worker_ids_from_metrics(metrics_after)
        entries_by_worker = {
            str(worker_id): int(count or 0)
            for worker_id, count in dict(
                decode_mm_hash_cache.get("entries_by_worker") or {}
            ).items()
        }
        missing_warm_workers = [
            str(worker_id)
            for worker_id in decode_workers
            if int(entries_by_worker.get(str(worker_id), 0)) <= 0
        ]
        if missing_warm_workers:
            failures.append(
                "Decode MM hash cache has no confirmed warm entries for workers: "
                f"{missing_warm_workers}"
            )
        for key in (
            "lookups",
            "hash_only_requests",
            "full_requests",
            "invalidations",
            "rejected_cold_metadata_only",
            "avoided_serialized_bytes",
            "epoch_changes",
            "epoch_probe_failures",
            "epoch_probe_responses",
            "epoch_probe_response_bytes",
            "epoch_guarded_hash_only_requests",
            "epoch_guard_rejections",
            "worker_unavailable_events",
            "epoch_observations",
            "synchronous_epoch_probes",
            "synchronous_epoch_probe_dispatches",
            "synchronous_epoch_probe_collapsed",
            "epoch_freshness_skips",
        ):
            decode_mm_hash_deltas[key] = int(decode_mm_hash_cache.get(key, 0) or 0) - int(
                before_hash.get(key, 0) or 0
            )
        probe_responses = int(decode_mm_hash_deltas["epoch_probe_responses"])
        decode_mm_hash_deltas["epoch_probe_response_bytes_per_response"] = (
            float(decode_mm_hash_deltas["epoch_probe_response_bytes"])
            / float(probe_responses)
            if probe_responses > 0
            else None
        )
        if decode_mm_hash_deltas["hash_only_requests"] != int(expected_successful):
            failures.append(
                "Decode MM hash-only request delta="
                f"{decode_mm_hash_deltas['hash_only_requests']} "
                f"expected={int(expected_successful)}"
            )
        if decode_mm_hash_deltas["full_requests"] != 0:
            failures.append(
                "Decode MM full feature requests occurred after warmup: "
                f"{decode_mm_hash_deltas['full_requests']}"
            )
        if decode_mm_hash_deltas["invalidations"] != 0:
            failures.append(
                "Decode MM hash assumptions invalidated during benchmark: "
                f"{decode_mm_hash_deltas['invalidations']}"
            )
        if decode_mm_hash_deltas["rejected_cold_metadata_only"] != 0:
            failures.append("Decode MM cache rejected cold metadata-only requests")
        if decode_mm_hash_deltas["epoch_guard_rejections"] != 0:
            failures.append(
                "Decode MM epoch guard rejected requests during stable benchmark: "
                f"{decode_mm_hash_deltas['epoch_guard_rejections']}"
            )
        if (
            bool(decode_mm_hash_cache.get("epoch_guard_enabled"))
            and decode_mm_hash_deltas["epoch_guarded_hash_only_requests"]
            != int(expected_successful)
        ):
            failures.append(
                "Decode MM epoch-guarded hash-only request delta="
                f"{decode_mm_hash_deltas['epoch_guarded_hash_only_requests']} "
                f"expected={int(expected_successful)}"
            )
        for key in (
            "epoch_changes",
            "epoch_probe_failures",
            "worker_unavailable_events",
        ):
            if decode_mm_hash_deltas[key] != 0:
                failures.append(
                    f"Decode MM epoch monitor {key} delta="
                    f"{decode_mm_hash_deltas[key]} expected=0"
                )
        if expected_successful > 0 and decode_mm_hash_deltas["avoided_serialized_bytes"] <= 0:
            failures.append("Decode MM hash cache avoided no serialized feature bytes")

    client_uuid_cache: dict[str, Any] = {}
    client_uuid_deltas: dict[str, int] = {}
    if require_client_mm_uuid_cache:
        before_uuid = dict(
            (metrics_before or {}).get("client_mm_uuid_references") or {}
        )
        client_uuid_cache = dict(
            (metrics_after or {}).get("client_mm_uuid_references") or {}
        )
        if not bool(client_uuid_cache.get("enabled")):
            failures.append("client multimodal UUID references are required but disabled")
        for key in (
            "legacy_full_requests",
            "uuid_full_requests",
            "compact_requests",
            "mixed_requests",
            "compact_items",
            "compact_request_body_bytes",
            "cold_misses",
        ):
            client_uuid_deltas[key] = int(client_uuid_cache.get(key, 0) or 0) - int(
                before_uuid.get(key, 0) or 0
            )
        if client_uuid_deltas["compact_requests"] != int(expected_successful):
            failures.append(
                "client multimodal compact request delta="
                f"{client_uuid_deltas['compact_requests']} "
                f"expected={int(expected_successful)}"
            )
        if client_uuid_deltas["compact_items"] < int(expected_successful):
            failures.append(
                "client multimodal compact item delta="
                f"{client_uuid_deltas['compact_items']} "
                f"expected_at_least={int(expected_successful)}"
            )
        for key in (
            "legacy_full_requests",
            "uuid_full_requests",
            "mixed_requests",
            "cold_misses",
        ):
            if client_uuid_deltas[key] != 0:
                failures.append(
                    f"client multimodal UUID {key} delta="
                    f"{client_uuid_deltas[key]} expected=0"
                )
    elif require_client_mm_uuid_full:
        before_uuid = dict(
            (metrics_before or {}).get("client_mm_uuid_references") or {}
        )
        client_uuid_cache = dict(
            (metrics_after or {}).get("client_mm_uuid_references") or {}
        )
        if not bool(client_uuid_cache.get("enabled")):
            failures.append("client multimodal UUID full control is enabled but proxy support is disabled")
        for key in (
            "legacy_full_requests",
            "uuid_full_requests",
            "compact_requests",
            "mixed_requests",
            "compact_items",
            "cold_misses",
        ):
            client_uuid_deltas[key] = int(client_uuid_cache.get(key, 0) or 0) - int(
                before_uuid.get(key, 0) or 0
            )
        if client_uuid_deltas["uuid_full_requests"] != int(expected_successful):
            failures.append(
                "client multimodal UUID full request delta="
                f"{client_uuid_deltas['uuid_full_requests']} "
                f"expected={int(expected_successful)}"
            )
        for key in (
            "legacy_full_requests",
            "compact_requests",
            "mixed_requests",
            "compact_items",
            "cold_misses",
        ):
            if client_uuid_deltas[key] != 0:
                failures.append(
                    f"client multimodal UUID full control {key} delta="
                    f"{client_uuid_deltas[key]} expected=0"
                )

    config = dict((metrics_after or {}).get("config") or {})
    if config and not bool(config.get("strict_no_fallback")):
        failures.append("proxy metrics report strict_no_fallback=false")
    return failures, {
        "expected_successful": int(expected_successful),
        "counter_deltas": deltas,
        "degrade_level_deltas": degrade_deltas,
        "direct_feature_handle_cache": cache,
        "prefill_render_cache": render_cache,
        "decode_mm_hash_cache": decode_mm_hash_cache,
        "decode_mm_hash_cache_deltas": decode_mm_hash_deltas,
        "client_mm_uuid_references": client_uuid_cache,
        "client_mm_uuid_reference_deltas": client_uuid_deltas,
        "strict_no_fallback": config.get("strict_no_fallback"),
    }


def summarize_run(
    rows: list[dict[str, Any]],
    *,
    concurrency: int,
    elapsed_s: float,
    goodput_ttft_ms: float,
    goodput_tpot_ms: float,
    expected_routing_path: str | None,
) -> dict[str, Any]:
    successful = [row for row in rows if _successful(row)]
    failed = [row for row in rows if not _successful(row)]
    ttft = [float(row["ttft_ms"]) for row in successful if row.get("ttft_ms") is not None]
    tpot = [float(row["tpot_ms"]) for row in successful if row.get("tpot_ms") is not None]
    latency = [float(row["elapsed_ms"]) for row in successful if row.get("elapsed_ms") is not None]
    prompt_tokens = sum(int(row.get("prompt_tokens", 0) or 0) for row in successful)
    completion_tokens = sum(int(row.get("completion_tokens", 0) or 0) for row in successful)
    route_mismatches = [
        row
        for row in successful
        if expected_routing_path
        and str(row.get("routing_path") or "") != expected_routing_path
    ]
    admission_mismatches = [
        row for row in successful if str(row.get("admission") or "") != "ADMIT"
    ]
    degrade_mismatches = [
        row for row in successful if str(row.get("degrade_level") or "") != "NONE"
    ]
    good = [
        row
        for row in successful
        if (goodput_ttft_ms <= 0 or float(row.get("ttft_ms", float("inf"))) <= goodput_ttft_ms)
        and (goodput_tpot_ms <= 0 or float(row.get("tpot_ms", float("inf"))) <= goodput_tpot_ms)
    ]
    family_stats: dict[str, dict[str, Any]] = {}
    for family in sorted({str(row.get("workload_family") or "unknown") for row in rows}):
        family_rows = [row for row in successful if str(row.get("workload_family") or "unknown") == family]
        family_stats[family] = {
            "requests": len(family_rows),
            "ttft_ms": _stats([float(row["ttft_ms"]) for row in family_rows if row.get("ttft_ms") is not None]),
            "tpot_ms": _stats([float(row["tpot_ms"]) for row in family_rows if row.get("tpot_ms") is not None]),
            "latency_ms": _stats([float(row["elapsed_ms"]) for row in family_rows if row.get("elapsed_ms") is not None]),
        }
    decode_worker_counts: dict[str, int] = {}
    for row in successful:
        worker_id = str(row.get("decode_worker") or "").strip()
        if worker_id:
            decode_worker_counts[worker_id] = (
                int(decode_worker_counts.get(worker_id, 0)) + 1
            )
    elapsed_s = max(1e-9, float(elapsed_s))
    return {
        "concurrency": int(concurrency),
        "requested": len(rows),
        "successful": len(successful),
        "failed": len(failed),
        "success_rate": len(successful) / len(rows) if rows else 0.0,
        "route_mismatch_count": len(route_mismatches),
        "admission_mismatch_count": len(admission_mismatches),
        "degrade_mismatch_count": len(degrade_mismatches),
        "benchmark_elapsed_s": elapsed_s,
        "request_throughput_rps": len(successful) / elapsed_s,
        "prompt_token_throughput_tps": prompt_tokens / elapsed_s,
        "output_token_throughput_tps": completion_tokens / elapsed_s,
        "total_token_throughput_tps": (prompt_tokens + completion_tokens) / elapsed_s,
        "goodput_rps": len(good) / elapsed_s,
        "goodput_count": len(good),
        "goodput_ttft_slo_ms": float(goodput_ttft_ms),
        "goodput_tpot_slo_ms": float(goodput_tpot_ms),
        "ttft_ms": _stats(ttft),
        "tpot_ms": _stats(tpot),
        "latency_ms": _stats(latency),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "request_payload_bytes": _stats(
            [
                float(row["request_payload_bytes"])
                for row in rows
                if isinstance(row.get("request_payload_bytes"), (int, float))
            ]
        ),
        "request_payload_bytes_total": sum(
            int(row.get("request_payload_bytes", 0) or 0) for row in rows
        ),
        "family_stats": family_stats,
        "decode_worker_counts": decode_worker_counts,
        "failures": failed,
    }


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    benchmark_id = uuid.uuid4().hex
    entries, skipped = _load_dataset_requests(
        dataset_root=str(Path(args.dataset_root).resolve()),
        chat_split=str(args.dataset_chat_split),
        max_requests=int(args.max_dataset_requests),
        families=list(args.dataset_families or []),
        model=str(args.model),
        max_input_len=int(args.max_input_len),
        request_max_tokens=int(args.max_tokens),
        skip_oversized=bool(args.skip_oversized),
        image_max_pixels=int(args.image_max_pixels),
    )
    if len(entries) < int(args.max_dataset_requests):
        raise RuntimeError(
            f"only {len(entries)} admissible real requests; expected {args.max_dataset_requests}"
        )
    assigned_mm_uuids = 0
    use_client_mm_uuids = bool(
        args.use_client_mm_uuids or args.require_client_mm_uuid_cache
    )
    if use_client_mm_uuids:
        assigned_mm_uuids = _assign_stable_mm_uuids(entries)
        if assigned_mm_uuids < len(entries):
            raise RuntimeError(
                "strict client multimodal UUID benchmark requires at least one "
                "media item per dataset request"
            )

    metrics_pre_warmup = _metrics_snapshot(args.metrics_url)
    decode_workers = _decode_worker_ids_from_metrics(metrics_pre_warmup)
    if bool(args.require_decode_mm_hash_cache) and not decode_workers:
        raise RuntimeError(
            "strict Decode MM hash benchmark requires proxy metrics to expose "
            "at least one Decode worker before warmup"
        )

    warmup_rows: list[dict[str, Any]] = []
    warmup_targets: list[str | None] = (
        [str(worker_id) for worker_id in decode_workers]
        if bool(args.require_decode_mm_hash_cache)
        else [None]
    )
    warmup_index = 0
    for target_worker in warmup_targets:
        for index in range(int(args.warmup_requests)):
            entry = _request_entry(
                entries[index % len(entries)],
                run_index=-1,
                sequence=warmup_index,
                workflow_mode=str(args.workflow_mode),
                benchmark_id=benchmark_id,
            )
            if target_worker is not None:
                entry = _target_decode_worker_entry(entry, target_worker)
            warmup_rows.append(
                _execute_dataset_request(
                    proxy_url=str(args.endpoint),
                    entry=entry,
                    index=warmup_index,
                    request_timeout=float(args.request_timeout),
                    stream_metrics=True,
                )
            )
            warmup_index += 1
    warmup_failures = [row for row in warmup_rows if not _successful(row)]
    if warmup_failures:
        raise RuntimeError(
            "real endpoint warmup failed: "
            + json.dumps(warmup_failures[0], ensure_ascii=False)[:4000]
        )
    if use_client_mm_uuids:
        unexpected_warmup_modes = [
            row.get("client_mm_uuid_mode")
            for row in warmup_rows
            if row.get("client_mm_uuid_mode") != "full"
        ]
        if unexpected_warmup_modes:
            raise RuntimeError(
                "client multimodal UUID warmup did not use full media payloads: "
                f"{unexpected_warmup_modes[:4]}"
            )
    targeted_warmup_mismatches = [
        {
            "requested": row.get("requested_decode_worker"),
            "actual": row.get("decode_worker"),
        }
        for row in warmup_rows
        if row.get("requested_decode_worker")
        and row.get("decode_worker") != row.get("requested_decode_worker")
    ]
    if targeted_warmup_mismatches:
        raise RuntimeError(
            "Decode worker-targeted warmup routed to the wrong worker: "
            f"{targeted_warmup_mismatches[:4]}"
        )

    metrics_before = _metrics_snapshot(args.metrics_url)
    gpu_before = _nvidia_smi_snapshot()
    runs: list[dict[str, Any]] = []
    gate_failures: list[str] = []
    for run_index, concurrency in enumerate(_parse_positive_ints(args.concurrency)):
        workload = [
            (
                _compact_mm_uuid_entry(
                    _request_entry(
                        entries[index % len(entries)],
                        run_index=run_index,
                        sequence=index,
                        workflow_mode=str(args.workflow_mode),
                        benchmark_id=benchmark_id,
                    )
                )[0]
                if bool(args.require_client_mm_uuid_cache)
                else _request_entry(
                    entries[index % len(entries)],
                    run_index=run_index,
                    sequence=index,
                    workflow_mode=str(args.workflow_mode),
                    benchmark_id=benchmark_id,
                )
            )
            for index in range(len(entries) * int(args.repeats))
        ]
        started = time.perf_counter()
        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(
                    _execute_dataset_request,
                    proxy_url=str(args.endpoint),
                    entry=entry,
                    index=index,
                    request_timeout=float(args.request_timeout),
                    stream_metrics=True,
                ): index
                for index, entry in enumerate(workload)
            }
            for future in as_completed(futures):
                rows.append(future.result())
        elapsed_s = time.perf_counter() - started
        rows.sort(key=lambda row: int(row.get("index", 0) or 0))
        summary = summarize_run(
            rows,
            concurrency=concurrency,
            elapsed_s=elapsed_s,
            goodput_ttft_ms=float(args.goodput_ttft_ms),
            goodput_tpot_ms=float(args.goodput_tpot_ms),
            expected_routing_path=args.expected_routing_path,
        )
        summary["raw_results"] = rows
        runs.append(summary)
        if float(summary["success_rate"]) < float(args.min_success_rate):
            gate_failures.append(
                f"concurrency={concurrency} success_rate={summary['success_rate']:.4f} "
                f"< {float(args.min_success_rate):.4f}"
            )
        if int(summary["route_mismatch_count"]) > 0:
            gate_failures.append(
                f"concurrency={concurrency} route mismatches={summary['route_mismatch_count']}"
            )
        if bool(args.require_full_goodput) and int(summary["goodput_count"]) != int(
            summary["successful"]
        ):
            gate_failures.append(
                f"concurrency={concurrency} goodput={summary['goodput_count']} "
                f"successful={summary['successful']}"
            )
        if bool(args.strict_epd_metrics_gate):
            if int(summary["admission_mismatch_count"]) > 0:
                gate_failures.append(
                    f"concurrency={concurrency} admission mismatches={summary['admission_mismatch_count']}"
                )
            if int(summary["degrade_mismatch_count"]) > 0:
                gate_failures.append(
                    f"concurrency={concurrency} degrade mismatches={summary['degrade_mismatch_count']}"
                )
        if bool(args.require_decode_mm_hash_cache):
            unexpected_modes = [
                str(row.get("decode_mm_features"))
                for row in rows
                if _successful(row) and row.get("decode_mm_features") != "hash-only"
            ]
            if unexpected_modes:
                gate_failures.append(
                    f"concurrency={concurrency} Decode MM feature modes are not all "
                    f"hash-only: {unexpected_modes[:4]}"
                )
            if len(decode_workers) > 1:
                observed_workers = set(summary["decode_worker_counts"])
                missing_workers = sorted(set(decode_workers) - observed_workers)
                if missing_workers:
                    gate_failures.append(
                        f"concurrency={concurrency} did not exercise Decode workers: "
                        f"{missing_workers}"
                    )
        if bool(args.require_client_mm_uuid_cache):
            unexpected_uuid_modes = [
                str(row.get("client_mm_uuid_mode"))
                for row in rows
                if _successful(row) and row.get("client_mm_uuid_mode") != "compact"
            ]
            if unexpected_uuid_modes:
                gate_failures.append(
                    f"concurrency={concurrency} client multimodal UUID modes are "
                    f"not all compact: {unexpected_uuid_modes[:4]}"
                )
        elif bool(args.use_client_mm_uuids):
            unexpected_uuid_modes = [
                str(row.get("client_mm_uuid_mode"))
                for row in rows
                if _successful(row) and row.get("client_mm_uuid_mode") != "full"
            ]
            if unexpected_uuid_modes:
                gate_failures.append(
                    f"concurrency={concurrency} client multimodal UUID control modes "
                    f"are not all full: {unexpected_uuid_modes[:4]}"
                )

    metrics_after = _metrics_snapshot(args.metrics_url)
    measured_proxy_stage_timing = _measured_proxy_stage_timing(
        metrics_after=metrics_after,
        runs=runs,
        workflow_mode=str(args.workflow_mode),
    )
    if (
        bool(args.strict_epd_metrics_gate)
        and str(args.workflow_mode) == "unique"
        and float(measured_proxy_stage_timing.get("coverage", 0.0) or 0.0) < 1.0
    ):
        gate_failures.append(
            "measured proxy stage timing coverage="
            f"{float(measured_proxy_stage_timing.get('coverage', 0.0) or 0.0):.4f} "
            "expected=1.0000"
        )
    metric_gate: dict[str, Any] = {}
    if bool(args.strict_epd_metrics_gate):
        metric_failures, metric_gate = _strict_epd_metric_gate(
            metrics_before=metrics_before,
            metrics_after=metrics_after,
            expected_successful=sum(int(run["successful"]) for run in runs),
            require_decode_mm_hash_cache=bool(args.require_decode_mm_hash_cache),
            require_client_mm_uuid_cache=bool(args.require_client_mm_uuid_cache),
            require_client_mm_uuid_full=bool(
                args.use_client_mm_uuids and not args.require_client_mm_uuid_cache
            ),
        )
        gate_failures.extend(metric_failures)

    return {
        "schema_version": 1,
        "benchmark": "real_openai_multimodal_closed_loop",
        "benchmark_id": benchmark_id,
        "created_at": time.time(),
        "methodology": {
            "warmup_excluded": True,
            "arrival_model": "closed_loop_fixed_concurrency",
            "streaming": True,
            "ttft_definition": "client arrival to first non-empty SSE text delta",
            "tpot_definition": "(request latency - TTFT) / (completion_tokens - 1)",
            "mock_used": False,
            "workflow_mode": str(args.workflow_mode),
            "client_mm_uuid_compaction": bool(args.require_client_mm_uuid_cache),
            "client_mm_uuids": use_client_mm_uuids,
            "assigned_mm_uuids": int(assigned_mm_uuids),
        },
        "environment": {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "gpu_before": gpu_before,
            "gpu_after": _nvidia_smi_snapshot(),
        },
        "config": vars(args) | {"output": str(args.output)},
        "dataset_skipped": skipped,
        "warmup_results": warmup_rows,
        "metrics_before": metrics_before,
        "metrics_after": metrics_after,
        "measured_proxy_stage_timing": measured_proxy_stage_timing,
        "runs": runs,
        "gate": {
            "passed": not gate_failures,
            "failures": gate_failures,
            "strict_epd_metrics": metric_gate,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--metrics-url")
    parser.add_argument("--model", default="/data01/LWX/Qwen3-VL-8B-Instruct")
    parser.add_argument("--dataset-root", required=True)
    parser.add_argument("--dataset-chat-split", default="dev-small")
    parser.add_argument("--dataset-families", nargs="+", default=["W0", "W1", "W2", "W3", "W4"])
    parser.add_argument("--max-dataset-requests", type=int, default=10)
    parser.add_argument("--warmup-requests", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--concurrency", default="1,2,4")
    parser.add_argument("--request-timeout", type=float, default=300.0)
    parser.add_argument("--max-input-len", type=int, default=4096)
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--image-max-pixels", type=int, default=1_003_520)
    parser.add_argument("--skip-oversized", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--workflow-mode", choices=("unique", "stable"), default="unique")
    parser.add_argument("--expected-routing-path")
    parser.add_argument("--goodput-ttft-ms", type=float, default=5_000.0)
    parser.add_argument("--goodput-tpot-ms", type=float, default=100.0)
    parser.add_argument("--min-success-rate", type=float, default=1.0)
    parser.add_argument(
        "--require-full-goodput",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--strict-epd-metrics-gate",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--require-decode-mm-hash-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Require every measured request to use hash-only Decode features, "
            "with zero full sends, invalidations, or cold metadata rejections."
        ),
    )
    parser.add_argument(
        "--require-client-mm-uuid-cache",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Warm every real media item with a stable vLLM UUID, omit media payloads "
            "during measurement, and require strict compact-cache metrics."
        ),
    )
    parser.add_argument(
        "--use-client-mm-uuids",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Attach stable UUIDs while retaining full media payloads. This is the "
            "strict same-identity control for UUID compaction A/B."
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.max_dataset_requests <= 0 or args.warmup_requests < 0 or args.repeats <= 0:
        parser.error("request/repeat counts must be positive and warmup non-negative")
    if not 0.0 <= float(args.min_success_rate) <= 1.0:
        parser.error("min-success-rate must be in [0, 1]")
    if args.require_decode_mm_hash_cache:
        if not args.strict_epd_metrics_gate or not args.metrics_url:
            parser.error(
                "require-decode-mm-hash-cache requires metrics URL and strict EPD gate"
            )
        if args.warmup_requests < args.max_dataset_requests:
            parser.error(
                "require-decode-mm-hash-cache requires at least one warmup per dataset request"
            )
    if args.require_client_mm_uuid_cache:
        if not args.strict_epd_metrics_gate or not args.metrics_url:
            parser.error(
                "require-client-mm-uuid-cache requires metrics URL and strict EPD gate"
            )
        if args.warmup_requests < args.max_dataset_requests:
            parser.error(
                "require-client-mm-uuid-cache requires at least one warmup per dataset request"
            )
    _parse_positive_ints(args.concurrency)
    return args


def main() -> int:
    args = parse_args()
    payload = run_benchmark(args)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "runs": [
                    {key: run[key] for key in ("concurrency", "successful", "request_throughput_rps", "goodput_rps", "ttft_ms", "tpot_ms")}
                    for run in payload["runs"]
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if bool(payload["gate"]["passed"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
