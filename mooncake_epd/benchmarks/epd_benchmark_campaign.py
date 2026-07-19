"""Manifest-driven, fail-closed EPD benchmark campaign evaluation.

The existing runners deliberately focus on one real path at a time.  This
module composes their artifacts into a reproducible campaign without turning a
successful HTTP response into a performance claim.  It distinguishes:

* correctness/data-plane evidence for an individual scenario;
* fair, same-resource transport A/B comparisons;
* scale-out capacity observations (which are *not* efficiency claims); and
* blocked hardware-dependent work such as RDMA or multi-node validation.

Commands are optional and are executed without a shell when explicitly
requested.  A normal CI invocation can therefore evaluate checked-in or
previously generated artifacts without accidentally starting a model server.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import re
import shlex
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence

from mooncake_epd.scripts.check_epd_artifact_gates import (
    check_agent_clone_summary,
    check_feature_summary,
    check_serving_summary,
)
from mooncake_epd.scripts.check_epd_transport_evidence import (
    evaluate_transport_evidence,
)


CAMPAIGN_SCHEMA_VERSION = 1
SCENARIO_KINDS = frozenset(
    {
        "single_baseline",
        "pd_transport",
        "full_epd",
        "agent_clone",
        "hidden_cache",
        "scheduler",
        "omni_pipeline",
        "fault",
        "cross_node",
    }
)
GATE_KINDS = frozenset({"none", "serving", "feature", "agent_clone"})
SERVING_SCENARIO_KINDS = frozenset(
    {"single_baseline", "pd_transport", "full_epd", "hidden_cache", "cross_node"}
)
COMPARISON_KINDS = frozenset(
    {
        "transport_ab",
        "same_resource_epd",
        "scale_out_capacity",
        "tuning_ab",
        "cache_ab",
    }
)
QUALITY_MODES = frozenset({"status_only", "structural", "exact"})
TRANSPORTS = frozenset({"tcp", "rdma", "nvlink_intra", "posix_shm", "cuda_ipc"})
CACHE_OBSERVATION_KEYS = frozenset(
    {
        "direct_feature_cache_hits",
        "direct_feature_cache_hit_requests",
        "proxy_handle_cache_hits",
        "proxy_handle_cache_misses",
        "direct_buffer_persistent_cache",
        "direct_buffer_allocations",
        "hidden_cache_hits",
        "hidden_cache_misses",
        "hidden_cache_full_miss_batches",
        "native_encoder_cache_hits",
        "precomputed_hits",
        "rendered_prefill_cache_enabled",
        "rendered_prefill_cache_hits",
        "rendered_prefill_cache_misses",
        "rendered_prefill_cache_hit_requests",
        "rendered_prefill_cache_miss_requests",
    }
)
# A tuning comparison is fair only when its configuration delta is explicit
# and restricted to a reviewed set of knobs.  Do not turn this into a blanket
# "ignore config mismatch" escape hatch: topology, workload, sampling and
# cache state still have to match exactly.
TUNING_CONFIG_DIFFERENCE_PATHS = frozenset(
    {
        "epd.scheduler_policy",
        "epd.layers_per_group",
        "epd.max_group_bytes",
        "epd.max_transfer_descriptors",
        "epd.max_transfer_bytes",
        "epd.transfer_workers",
        "serving.max_num_batched_tokens",
        "serving.max_num_seqs",
    }
)


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _load_json(path: Path) -> Dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"expected JSON object: {path}")
    return dict(value)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * float(percentile)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _stats(values: Sequence[float]) -> Dict[str, float]:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return {"count": 0, "mean": 0.0, "median": 0.0, "p95": 0.0, "stddev": 0.0}
    return {
        "count": len(clean),
        "mean": float(statistics.fmean(clean)),
        "median": float(statistics.median(clean)),
        "p95": float(_percentile(clean, 0.95)),
        "stddev": float(statistics.stdev(clean)) if len(clean) > 1 else 0.0,
    }


def _nested(payload: Mapping[str, Any], *path: str) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _number(payload: Mapping[str, Any], *paths: Sequence[str] | str) -> float | None:
    for path in paths:
        value = _nested(payload, *path) if isinstance(path, tuple) else payload.get(path)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _metric_summary(payload: Mapping[str, Any]) -> Dict[str, Any]:
    for key in (
        "measured_online_direct_metric_summary",
        "online_direct_metric_summary",
        "feature_handle_metric_summary",
        "real_epd_metric_summary",
    ):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    metrics = _nested(payload, "metrics", "metrics")
    if isinstance(metrics, Mapping):
        return dict(metrics)
    metrics = payload.get("metrics")
    return dict(metrics) if isinstance(metrics, Mapping) else {}


def _responses(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = payload.get("responses")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    response = payload.get("response")
    return [dict(response)] if isinstance(response, Mapping) else []


def _response_timing_average(
    payload: Mapping[str, Any],
    *timing_keys: str,
) -> float | None:
    """Return the mean request-visible timing for one timing or a sum of them.

    EPD's proxy already emits phase timings on each response.  Aggregating
    those raw samples here avoids inferring a dominant stage from an unrelated
    end-to-end counter.  A zero is a valid cache-hit value, while an absent key
    means the scenario did not instrument that phase.
    """

    values: List[float] = []
    for response in _responses(payload):
        timings = _as_dict(response.get("epd_timing_ms"))
        if not any(key in timings for key in timing_keys):
            continue
        total = 0.0
        for key in timing_keys:
            try:
                total += float(timings.get(key, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
        values.append(total)
    return float(statistics.fmean(values)) if values else None


def _response_failures(payload: Mapping[str, Any], *, expected_path: str | None) -> List[str]:
    rows = _responses(payload)
    if not rows:
        return ["no_response_rows"]
    failures: List[str] = []
    for index, row in enumerate(rows):
        if int(row.get("status_code", 0) or 0) != 200 or row.get("error"):
            failures.append(f"response[{index}]_not_http_200")
        content_len = int(row.get("response_content_len", 0) or 0)
        if content_len <= 0 and not str(row.get("response_text", "") or "").strip():
            failures.append(f"response[{index}]_empty")
        if expected_path:
            headers = _as_dict(row.get("headers"))
            observed = str(row.get("routing_path") or headers.get("x-epd-routing-path") or "")
            if observed != expected_path:
                failures.append(
                    f"response[{index}]_routing_path_{observed or 'missing'}_not_{expected_path}"
                )
    return failures


def dispatch_balance_observations(payload: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Normalize stage dispatch coverage from real multi-worker artifacts.

    A scale-out result is not credible if a configured worker receives no
    measured traffic.  New runners provide ``worker_dispatch_balance``;
    retain a deterministic fallback for historical raw summaries that only
    expose the per-worker dispatch maps.
    """

    topology = _as_dict(_as_dict(payload.get("benchmark_config")).get("topology"))
    reported = _as_dict(payload.get("worker_dispatch_balance"))
    result: Dict[str, Dict[str, Any]] = {}
    for stage, topology_key, dispatch_key in (
        ("prefill", "prefill_gpus", "prefill_worker_dispatches"),
        ("decode", "decode_gpus", "decode_worker_dispatches"),
    ):
        raw = _as_dict(reported.get(stage))
        counts = _as_dict(raw.get("counts"))
        if not counts:
            counts = _as_dict(payload.get(dispatch_key))
        normalized_counts: Dict[str, int] = {}
        for worker_id, raw_count in counts.items():
            try:
                normalized_counts[str(worker_id)] = max(0, int(raw_count))
            except (TypeError, ValueError):
                continue
        configured_from_topology = _as_list(topology.get(topology_key))
        configured = len(configured_from_topology)
        try:
            configured = max(configured, int(raw.get("configured_workers", 0) or 0))
        except (TypeError, ValueError):
            pass
        configured = max(configured, len(normalized_counts))
        total = sum(normalized_counts.values())
        active = sum(1 for count in normalized_counts.values() if count > 0)
        if configured <= 1 and total > 0:
            entropy = 1.0
        elif configured > 1 and total > 0:
            probabilities = [count / total for count in normalized_counts.values() if count > 0]
            # Include implicit zero-dispatch configured workers in the
            # denominator.  They add no entropy and therefore surface the
            # under-utilization rather than disappearing from the score.
            entropy = -sum(value * math.log(value) for value in probabilities) / math.log(configured)
        else:
            entropy = 0.0
        try:
            reported_entropy = float(raw["normalized_entropy"])
        except (KeyError, TypeError, ValueError):
            reported_entropy = entropy
        try:
            reported_max_share = float(raw["max_share"])
        except (KeyError, TypeError, ValueError):
            reported_max_share = max(normalized_counts.values()) / total if total else 0.0
        result[stage] = {
            "configured_workers": configured,
            "active_workers": active,
            "total_dispatches": total,
            "counts": normalized_counts,
            "normalized_entropy": reported_entropy,
            "max_share": reported_max_share,
        }
    return result


def _dispatch_balance_failures(
    observations: Mapping[str, Mapping[str, Any]],
    *,
    require_all_workers: bool,
    min_entropy: float | None,
) -> List[str]:
    failures: List[str] = []
    for stage in ("prefill", "decode"):
        values = _as_dict(observations.get(stage))
        configured = int(values.get("configured_workers", 0) or 0)
        active = int(values.get("active_workers", 0) or 0)
        entropy = float(values.get("normalized_entropy", 0.0) or 0.0)
        if require_all_workers and configured > 1 and active < configured:
            failures.append(
                f"{stage}_dispatch_coverage_incomplete:active={active},configured={configured}"
            )
        if min_entropy is not None and configured > 1 and entropy < min_entropy:
            failures.append(
                f"{stage}_dispatch_entropy_below_min:{entropy:.6f}<{min_entropy:.6f}"
            )
    return failures


def _raw_artifact_failures(
    payload: Mapping[str, Any],
    *,
    require_hardware_inventory: bool = False,
) -> List[str]:
    raw = payload.get("raw_artifacts")
    if not isinstance(raw, Mapping):
        return ["missing_raw_artifacts"]
    failures: List[str] = []
    for key in ("requests_jsonl", "responses_jsonl", "service_logs", "metrics", "environment"):
        value = raw.get(key)
        if not value or not Path(str(value)).exists():
            failures.append(f"missing_raw_artifact:{key}")
    if require_hardware_inventory:
        hardware = raw.get("hardware")
        if not hardware or not Path(str(hardware)).exists():
            failures.append("missing_raw_artifact:hardware")
        else:
            try:
                inventory = _load_json(Path(str(hardware)))
            except (OSError, ValueError, json.JSONDecodeError):
                failures.append("unreadable_raw_artifact:hardware")
            else:
                probes = _as_dict(inventory.get("commands"))
                topology = _as_dict(probes.get("topology"))
                query = _as_dict(probes.get("gpu_query"))
                if not bool(topology.get("available")) or topology.get("returncode") != 0:
                    failures.append("hardware_inventory_topology_unavailable")
                if not bool(query.get("available")) or query.get("returncode") != 0:
                    failures.append("hardware_inventory_gpu_query_unavailable")
    service_logs = raw.get("service_logs")
    if service_logs and Path(str(service_logs)).exists():
        try:
            log_manifest = _load_json(Path(str(service_logs)))
        except (OSError, ValueError, json.JSONDecodeError):
            failures.append("unreadable_raw_artifact:service_logs")
        else:
            missing = _as_list(log_manifest.get("missing"))
            if missing:
                failures.append("raw_service_logs_have_missing_sources")
    return failures


def _nvidia_topology_link(topology_text: str, source_gpu: int, target_gpu: int) -> str | None:
    """Read one GPU↔GPU cell from ``nvidia-smi topo -m`` output."""

    # Recent nvidia-smi versions can underline the topology header with ANSI
    # SGR escapes.  Strip those control bytes before tokenizing so a real
    # ``GPU0`` header cannot be mistaken for a missing GPU by this gate.
    ansi_sgr = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
    lines = [
        ansi_sgr.sub("", line).strip()
        for line in str(topology_text or "").splitlines()
        if ansi_sgr.sub("", line).strip()
    ]
    header_index = None
    header: List[str] = []
    target_name = f"GPU{int(target_gpu)}"
    for index, line in enumerate(lines):
        tokens = line.split()
        if f"GPU{int(source_gpu)}" in tokens and target_name in tokens:
            header_index = index
            header = tokens
            break
    if header_index is None:
        return None
    try:
        column = header.index(target_name)
    except ValueError:
        return None
    source_name = f"GPU{int(source_gpu)}"
    for line in lines[header_index + 1 :]:
        tokens = line.split()
        if not tokens:
            continue
        if tokens[0] == source_name:
            # The first column is the row label, while the header contains
            # one entry per data column.  GPU columns precede CPU/NIC fields.
            return tokens[column + 1] if len(tokens) > column + 1 else None
        if tokens[0].startswith("Legend"):
            break
    return None


def _required_nvlink_pair_failures(
    payload: Mapping[str, Any],
    pairs: Sequence[Any],
) -> tuple[List[str], List[Dict[str, Any]]]:
    """Require physical NVLink topology for every claimed NVLink edge."""

    raw = _as_dict(payload.get("raw_artifacts"))
    hardware_path = raw.get("hardware")
    observed: List[Dict[str, Any]] = []
    failures: List[str] = []
    if not hardware_path or not Path(str(hardware_path)).exists():
        return ["nvlink_topology_hardware_artifact_missing"], observed
    try:
        inventory = _load_json(Path(str(hardware_path)))
    except (OSError, ValueError, json.JSONDecodeError):
        return ["nvlink_topology_hardware_artifact_unreadable"], observed
    topology = _as_dict(_as_dict(inventory.get("commands")).get("topology"))
    if not bool(topology.get("available")) or topology.get("returncode") != 0:
        return ["nvlink_topology_probe_unavailable"], observed
    text = str(topology.get("stdout", "") or "")
    for raw_pair in pairs:
        if not isinstance(raw_pair, (list, tuple)) or len(raw_pair) != 2:
            failures.append(f"invalid_required_nvlink_pair:{raw_pair!r}")
            continue
        try:
            source_gpu, target_gpu = int(raw_pair[0]), int(raw_pair[1])
        except (TypeError, ValueError):
            failures.append(f"invalid_required_nvlink_pair:{raw_pair!r}")
            continue
        link = _nvidia_topology_link(text, source_gpu, target_gpu)
        observed.append({"source_gpu": source_gpu, "target_gpu": target_gpu, "link": link})
        if not str(link or "").upper().startswith("NV"):
            failures.append(
                f"nvlink_topology_pair_{source_gpu}_{target_gpu}_is_{link or 'missing'}"
            )
    return failures, observed


def _summary_path_from_stdout(stdout: str) -> str | None:
    """Find the summary emitted by repository runners, including pretty JSON."""

    for line in reversed(str(stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, Mapping) and row.get("summary"):
            candidate = Path(str(row["summary"])).expanduser()
            if candidate.exists():
                return str(candidate)
    marker = '"summary"'
    offset = str(stdout or "").rfind(marker)
    if offset < 0:
        return None
    suffix = str(stdout or "")[offset + len(marker) :]
    if ":" not in suffix:
        return None
    value = suffix.split(":", 1)[1].lstrip()
    if not value.startswith('"'):
        return None
    try:
        candidate_text = json.loads(value.splitlines()[0].rstrip(","))
    except json.JSONDecodeError:
        return None
    candidate = Path(str(candidate_text)).expanduser()
    return str(candidate) if candidate.exists() else None


def _run_command(command: str, *, cwd: Path, timeout_s: float | None) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        proc = subprocess.run(
            shlex.split(str(command)),
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "executed": True,
            "returncode": None,
            "timed_out": True,
            "elapsed_s": time.monotonic() - started,
            "stdout_tail": str(exc.stdout or "")[-4000:],
            "stderr_tail": str(exc.stderr or "")[-4000:],
        }
    result = {
        "executed": True,
        "returncode": int(proc.returncode),
        "timed_out": False,
        "elapsed_s": time.monotonic() - started,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    summary = _summary_path_from_stdout(proc.stdout)
    if summary:
        result["summary_path"] = summary
    return result


def validate_campaign_manifest(manifest: Mapping[str, Any]) -> List[str]:
    """Return schema errors rather than silently accepting an incomplete suite."""

    failures: List[str] = []
    if int(manifest.get("schema_version", 0) or 0) != CAMPAIGN_SCHEMA_VERSION:
        failures.append(f"schema_version must be {CAMPAIGN_SCHEMA_VERSION}")
    if not str(manifest.get("campaign_id", "") or "").strip():
        failures.append("campaign_id is required")
    scenarios = _as_list(manifest.get("scenarios"))
    if not scenarios:
        failures.append("at least one scenario is required")
    identifiers: set[str] = set()
    for index, raw in enumerate(scenarios):
        if not isinstance(raw, Mapping):
            failures.append(f"scenario[{index}] must be an object")
            continue
        scenario = dict(raw)
        identifier = str(scenario.get("id", "") or "").strip()
        if not identifier:
            failures.append(f"scenario[{index}] id is required")
        elif identifier in identifiers:
            failures.append(f"duplicate scenario id: {identifier}")
        else:
            identifiers.add(identifier)
        kind = str(scenario.get("kind", "") or "")
        if kind not in SCENARIO_KINDS:
            failures.append(f"scenario[{identifier or index}] unknown kind: {kind}")
        gate = str(scenario.get("gate", "none") or "none")
        if gate not in GATE_KINDS:
            failures.append(f"scenario[{identifier or index}] unknown gate: {gate}")
        transport = scenario.get("transport")
        if transport is not None and str(transport) not in TRANSPORTS:
            failures.append(f"scenario[{identifier or index}] unsupported transport: {transport}")
        min_dispatch_entropy = scenario.get("min_dispatch_entropy")
        if min_dispatch_entropy is not None:
            try:
                entropy = float(min_dispatch_entropy)
            except (TypeError, ValueError):
                failures.append(
                    f"scenario[{identifier or index}] min_dispatch_entropy must be numeric"
                )
            else:
                if entropy < 0.0 or entropy > 1.0:
                    failures.append(
                        f"scenario[{identifier or index}] min_dispatch_entropy must be in [0, 1]"
                    )
        if not scenario.get("blocked_reason") and not scenario.get("summary") and not scenario.get("command"):
            failures.append(
                f"scenario[{identifier or index}] needs summary, command, or blocked_reason"
            )
    for index, raw in enumerate(_as_list(manifest.get("comparisons"))):
        if not isinstance(raw, Mapping):
            failures.append(f"comparison[{index}] must be an object")
            continue
        comparison = dict(raw)
        identifier = str(comparison.get("id", "") or "").strip()
        if not identifier:
            failures.append(f"comparison[{index}] id is required")
        kind = str(comparison.get("kind", "") or "")
        if kind not in COMPARISON_KINDS:
            failures.append(f"comparison[{identifier or index}] unknown kind: {kind}")
        quality = str(comparison.get("quality", "structural") or "structural")
        if quality not in QUALITY_MODES:
            failures.append(f"comparison[{identifier or index}] unknown quality: {quality}")
        raw_allowed_differences = comparison.get("allowed_config_differences")
        if raw_allowed_differences is not None and not isinstance(
            raw_allowed_differences, list
        ):
            failures.append(
                f"comparison[{identifier or index}] allowed_config_differences must be a list"
            )
        allowed_differences = _as_list(raw_allowed_differences)
        if allowed_differences and kind != "tuning_ab":
            failures.append(
                f"comparison[{identifier or index}] allowed_config_differences only applies to tuning_ab"
            )
        for path in allowed_differences:
            normalized = str(path)
            if normalized not in TUNING_CONFIG_DIFFERENCE_PATHS:
                failures.append(
                    f"comparison[{identifier or index}] unsupported allowed_config_difference: {normalized}"
                )
        for role in ("baseline", "candidate"):
            ids = _as_list(comparison.get(role))
            if not ids:
                failures.append(f"comparison[{identifier or index}] {role} is required")
            for scenario_id in ids:
                if str(scenario_id) not in identifiers:
                    failures.append(
                        f"comparison[{identifier or index}] references unknown scenario: {scenario_id}"
                    )
        if int(comparison.get("min_samples", 3) or 0) < 1:
            failures.append(f"comparison[{identifier or index}] min_samples must be >= 1")
    return failures


def _resolve_path(value: str, *, manifest_dir: Path) -> Path:
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else (manifest_dir / path).resolve()


def _extract_metrics(payload: Mapping[str, Any]) -> Dict[str, float | None]:
    metric = _metric_summary(payload)
    return {
        "avg_ttft_ms": _number(
            payload,
            "avg_ttft_ms",
            ("ttft_stats_ms", "avg"),
            ("ttft_ms", "avg"),
            "ttft_ms",
        ),
        "p95_ttft_ms": _number(payload, ("ttft_stats_ms", "p95")),
        "request_throughput_rps": _number(
            payload, "request_throughput_rps", "goodput_rps", "throughput_rps"
        ),
        "output_token_goodput_tps": _number(
            payload,
            "output_token_goodput_tps",
            "completion_token_throughput_tps",
        ),
        "avg_tpot_ms": _number(payload, "avg_tpot_ms"),
        "peer_buffer_write_ms_avg": _number(metric, "peer_buffer_write_ms_avg"),
        "peer_buffer_write_bandwidth_gbps": _number(
            metric, "peer_buffer_write_bandwidth_gbps"
        ),
        "layered_receive_kv_worker_ms_avg": _number(
            metric, "layered_receive_kv_worker_ms_avg"
        ),
        "decode_engine_first_token_latency_ms_avg": _number(
            payload,
            "decode_engine_first_token_latency_ms_avg",
            ("measured_online_direct_metric_summary", "decode_engine_first_token_latency_ms_avg"),
        ),
        "proxy_prefill_to_decode_dispatch_ms_avg": _number(
            payload, "avg_proxy_prefill_to_decode_dispatch_ms"
        ),
        "proxy_request_to_decode_stream_open_ms_avg": _number(
            payload, "avg_proxy_request_to_decode_stream_open_ms"
        ),
        "client_to_proxy_first_content_gap_ms_avg": _number(
            payload, "avg_client_to_proxy_first_content_gap_ms"
        ),
        "mm_prepare_ms_avg": _response_timing_average(payload, "mm_prepare_ms"),
        "prefill_ms_avg": _response_timing_average(payload, "prefill_ms"),
        "ep_direct_publish_control_ms_avg": _response_timing_average(
            payload,
            "direct_describe_ms",
            "direct_allocate_ms",
            "direct_publish_ms",
            "direct_mark_ready_ms",
        ),
        "decode_first_content_ms_avg": _response_timing_average(
            payload, "decode_first_content_ms"
        ),
        "route_correct_rate": _number(payload, "route_correct_rate"),
        "sla_satisfied_rate": _number(payload, "sla_satisfied_rate"),
    }


def diagnose_bottlenecks(payload: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Rank observed latency indicators without pretending they are additive."""

    metrics = _extract_metrics(payload)
    ttft = float(metrics.get("avg_ttft_ms") or 0.0)
    candidates = (
        ("prefill", "prefill_ms_avg", "Prefill render/generate timing"),
        ("decode_first_token", "decode_engine_first_token_latency_ms_avg", "Decode engine first-token timing"),
        ("decode_first_content", "decode_first_content_ms_avg", "Proxy-observed Decode first-content timing"),
        ("pd_receive", "layered_receive_kv_worker_ms_avg", "Decode-side P→D KV receive worker timing"),
        ("pd_write", "peer_buffer_write_ms_avg", "Per P→D peer-buffer write batch timing"),
        ("ep_direct_publish", "ep_direct_publish_control_ms_avg", "E→P direct FeatureBundle control/publish timing"),
        ("mm_prepare", "mm_prepare_ms_avg", "Multimodal FeatureHandle preparation timing"),
        ("proxy_prefill_to_decode", "proxy_prefill_to_decode_dispatch_ms_avg", "Proxy Prefill→Decode dispatch timing"),
        ("proxy_decode_stream_open", "proxy_request_to_decode_stream_open_ms_avg", "Proxy request→Decode stream-open timing"),
        ("client_proxy_gap", "client_to_proxy_first_content_gap_ms_avg", "Client/proxy residual before first content"),
    )
    findings: List[Dict[str, Any]] = []
    for name, key, label in candidates:
        value = metrics.get(key)
        if value is None or float(value) <= 0.0:
            continue
        findings.append(
            {
                "component": name,
                "label": label,
                "observed_ms": float(value),
                "ttft_fraction": (float(value) / ttft) if ttft > 0.0 else None,
                "not_additive": True,
            }
        )
    return sorted(findings, key=lambda item: float(item["observed_ms"]), reverse=True)


def cache_observations(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize cache evidence from serving summaries for cache-specific gates.

    A cache benchmark is only meaningful when it states both the requested
    cache mode and the observed hit/miss behavior.  Keep these observations
    separate from generic performance metrics: a cache hit is an explanatory
    condition, not proof that an end-to-end gain generalizes to cold traffic.
    """

    metric = _metric_summary(payload)
    handle_cache = _as_dict(metric.get("direct_feature_handle_cache"))
    rendered_cache = _as_dict(metric.get("rendered_prefill_cache"))
    if not rendered_cache:
        rendered_cache = _as_dict(
            _as_dict(payload.get("metrics")).get("rendered_prefill_cache")
        )
    direct_stats = _as_dict(payload.get("direct_buffer_stats_after_release"))
    epd_config = _as_dict(_as_dict(payload.get("benchmark_config")).get("epd"))
    return {
        "direct_feature_cache_hits": _number(payload, "direct_feature_cache_hits"),
        "direct_feature_cache_hit_requests": _number(
            payload, "direct_feature_cache_hit_requests"
        ),
        "proxy_handle_cache_hits": _number(handle_cache, "hits"),
        "proxy_handle_cache_misses": _number(handle_cache, "misses"),
        "direct_buffer_persistent_cache": direct_stats.get("persistent_cache"),
        "direct_buffer_allocations": _number(direct_stats, "allocations"),
        "hidden_cache_hits": _number(metric, "hidden_cache_hits"),
        "hidden_cache_misses": _number(metric, "hidden_cache_misses"),
        "hidden_cache_full_miss_batches": _number(
            metric, "hidden_cache_full_miss_batches"
        ),
        "native_encoder_cache_hits": _number(metric, "native_encoder_cache_hits"),
        "precomputed_hits": _number(metric, "precomputed_hits"),
        "rendered_prefill_cache_enabled": epd_config.get("rendered_prefill_cache"),
        "rendered_prefill_cache_hits": _number(rendered_cache, "hits"),
        "rendered_prefill_cache_misses": _number(rendered_cache, "misses"),
        "rendered_prefill_cache_hit_requests": _number(
            payload, "rendered_prefill_cache_hit_requests"
        ),
        "rendered_prefill_cache_miss_requests": _number(
            payload, "rendered_prefill_cache_miss_requests"
        ),
    }


def _cache_expectation_failures(
    payload: Mapping[str, Any],
    expectations: Mapping[str, Any],
) -> tuple[List[str], Dict[str, Any]]:
    """Evaluate explicit cache hit/miss expectations fail-closed."""

    observations = cache_observations(payload)
    failures: List[str] = []
    for key, rule in expectations.items():
        observed = observations.get(str(key))
        if str(key) not in CACHE_OBSERVATION_KEYS:
            failures.append(f"unknown_cache_observation:{key}")
            continue
        if isinstance(rule, Mapping):
            if observed is None:
                failures.append(f"cache_observation_missing:{key}")
                continue
            if "equals" in rule and observed != rule["equals"]:
                failures.append(
                    f"cache_observation_{key}_equals_{rule['equals']!r}_got_{observed!r}"
                )
            if "min" in rule:
                try:
                    if float(observed) < float(rule["min"]):
                        failures.append(
                            f"cache_observation_{key}_below_min_{rule['min']}_got_{observed}"
                        )
                except (TypeError, ValueError):
                    failures.append(f"cache_observation_{key}_not_numeric")
            if "max" in rule:
                try:
                    if float(observed) > float(rule["max"]):
                        failures.append(
                            f"cache_observation_{key}_above_max_{rule['max']}_got_{observed}"
                        )
                except (TypeError, ValueError):
                    failures.append(f"cache_observation_{key}_not_numeric")
        elif observed != rule:
            failures.append(
                f"cache_observation_{key}_equals_{rule!r}_got_{observed!r}"
            )
    return failures, observations


def _evaluate_summary(
    *,
    scenario: Mapping[str, Any],
    summary_path: Path,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    payload = _load_json(summary_path)
    failures: List[str] = []
    kind = str(scenario.get("kind"))
    gate = str(scenario.get("gate", "none") or "none")
    expected_path = scenario.get("expected_routing_path")
    if expected_path is None:
        if kind == "pd_transport":
            expected_path = "PD"
        elif kind == "full_epd":
            expected_path = "EPD"
    # State/clone, scheduler, fault and worker-pipeline summaries are not
    # necessarily OpenAI serving responses.  Make response evidence mandatory
    # only for serving-style scenario kinds; non-serving scenarios can still
    # opt in when their workload has request/response rows.
    if bool(scenario.get("require_responses", kind in SERVING_SCENARIO_KINDS)):
        failures.extend(_response_failures(payload, expected_path=str(expected_path) if expected_path else None))
    if bool(
        scenario.get(
            "require_raw_artifacts",
            kind in {"single_baseline", "pd_transport", "full_epd", "hidden_cache"},
        )
    ):
        failures.extend(
            _raw_artifact_failures(
                payload,
                require_hardware_inventory=bool(
                    scenario.get(
                        "require_hardware_inventory",
                        kind in {"single_baseline", "pd_transport", "full_epd", "hidden_cache"},
                    )
                ),
            )
        )
    cache_expectations = scenario.get("cache_expectations")
    if cache_expectations is not None:
        if not isinstance(cache_expectations, Mapping):
            failures.append("cache_expectations_must_be_an_object")
        else:
            cache_failures, cache_values = _cache_expectation_failures(
                payload, cache_expectations
            )
            failures.extend(cache_failures)
    else:
        cache_values = cache_observations(payload)
    dispatch_balance = dispatch_balance_observations(payload)
    min_dispatch_entropy_raw = scenario.get("min_dispatch_entropy")
    min_dispatch_entropy: float | None = None
    if min_dispatch_entropy_raw is not None:
        try:
            min_dispatch_entropy = float(min_dispatch_entropy_raw)
        except (TypeError, ValueError):
            failures.append("min_dispatch_entropy_not_numeric")
    failures.extend(
        _dispatch_balance_failures(
            dispatch_balance,
            require_all_workers=bool(
                scenario.get("require_all_stage_workers_dispatched", False)
            ),
            min_entropy=min_dispatch_entropy,
        )
    )
    required_nvlink_pairs = _as_list(scenario.get("required_nvlink_pairs"))
    if required_nvlink_pairs:
        topology_failures, topology_links = _required_nvlink_pair_failures(
            payload, required_nvlink_pairs
        )
        failures.extend(topology_failures)
    else:
        topology_links = []
    if gate == "serving":
        failures.extend(check_serving_summary(str(summary_path)))
    elif gate == "feature":
        failures.extend(check_feature_summary(str(summary_path)))
    elif gate == "agent_clone":
        failures.extend(check_agent_clone_summary(str(summary_path)))
    transport = scenario.get("transport")
    if transport:
        transport_result = evaluate_transport_evidence(payload, requested=str(transport))
        failures.extend(str(item) for item in transport_result.get("failures") or [])
    else:
        transport_result = None
    direct_transport = str(payload.get("direct_engine_protocol", "") or "").strip().lower()
    if direct_transport and not bool(payload.get("text_only", False)):
        direct_transport_result = evaluate_transport_evidence(
            payload,
            requested=direct_transport,
            runtime_evidence_key="direct_transport_runtime_evidence",
        )
        failures.extend(
            "direct_" + str(item)
            for item in direct_transport_result.get("failures") or []
        )
    else:
        direct_transport_result = None
    result = {
        "status": "passed" if not failures else "failed",
        "summary_path": str(summary_path),
        "failures": failures,
        "metrics": _extract_metrics(payload),
        "benchmark_config": _as_dict(payload.get("benchmark_config")),
        "response_count": len(_responses(payload)),
        "transport_evidence": transport_result,
        "direct_transport_evidence": direct_transport_result,
        "cache_observations": cache_values,
        "worker_dispatch_balance": dispatch_balance,
        "required_nvlink_links": topology_links,
        "bottlenecks": diagnose_bottlenecks(payload),
    }
    return result, payload


def _run_scenario(
    scenario: Mapping[str, Any],
    *,
    manifest_dir: Path,
    execute_commands: bool,
) -> tuple[Dict[str, Any], Dict[str, Any] | None]:
    identifier = str(scenario.get("id"))
    base = {
        "id": identifier,
        "kind": str(scenario.get("kind")),
        "required": bool(scenario.get("required", True)),
        "gate": str(scenario.get("gate", "none") or "none"),
        "transport": scenario.get("transport"),
    }
    if scenario.get("blocked_reason"):
        return ({**base, "status": "blocked", "reason": str(scenario["blocked_reason"])}, None)
    summary_value = scenario.get("summary")
    command_result: Dict[str, Any] | None = None
    if not summary_value and scenario.get("command"):
        if not execute_commands:
            return ({**base, "status": "not_run", "reason": "command_execution_not_requested"}, None)
        command_cwd = _resolve_path(str(scenario.get("cwd", ".")), manifest_dir=manifest_dir)
        command_result = _run_command(
            str(scenario["command"]),
            cwd=command_cwd,
            timeout_s=(float(scenario["timeout_s"]) if scenario.get("timeout_s") else None),
        )
        if command_result.get("returncode") != 0:
            return (
                {
                    **base,
                    "status": "failed",
                    "reason": "command_failed",
                    "command": command_result,
                },
                None,
            )
        summary_value = command_result.get("summary_path")
        if not summary_value:
            return (
                {
                    **base,
                    "status": "failed",
                    "reason": "command_did_not_expose_summary",
                    "command": command_result,
                },
                None,
            )
    if not summary_value:
        return ({**base, "status": "not_run", "reason": "no_summary_or_command"}, None)
    summary_path = _resolve_path(str(summary_value), manifest_dir=manifest_dir)
    if not summary_path.exists():
        return ({**base, "status": "failed", "reason": f"summary_not_found:{summary_path}"}, None)
    try:
        evaluated, payload = _evaluate_summary(scenario=scenario, summary_path=summary_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return ({**base, "status": "failed", "reason": f"summary_unreadable:{exc}"}, None)
    if command_result is not None:
        evaluated["command"] = command_result
    return ({**base, **evaluated}, payload)


def _comparison_config(
    payload: Mapping[str, Any],
    *,
    same_resource: bool,
    ignore_cache_controls: bool = False,
    ignore_config_paths: Sequence[str] = (),
) -> Dict[str, Any]:
    config = copy.deepcopy(_as_dict(payload.get("benchmark_config")))
    epd = _as_dict(config.get("epd"))
    epd.pop("protocol", None)
    epd.pop("direct_engine_protocol", None)
    if ignore_cache_controls:
        for key in (
            "direct_persistent_cache",
            "direct_proxy_handle_cache",
            "direct_proxy_handle_cache_requested",
            "direct_proxy_handle_cache_effective",
            "vllm_mm_hidden_cache",
            "release_direct_feature_buffers_after_prefill",
            "rendered_prefill_cache",
            "rendered_prefill_cache_max_entries",
            "rendered_prefill_cache_max_bytes",
            "rendered_prefill_cache_ttl_s",
        ):
            epd.pop(key, None)
        # A cache-miss control generally needs a nonce while an exact-hit
        # workload must retain the original prefix.  This difference is
        # intentional and is declared in the cache expectations, not hidden
        # as a transport-tuning mismatch.
        config.pop("request_variation", None)
    if epd:
        config["epd"] = epd
    for raw_path in ignore_config_paths:
        # Paths are schema-validated against TUNING_CONFIG_DIFFERENCE_PATHS
        # before this function is called.  Still traverse defensively so a
        # partially populated historical artifact stays comparable only on the
        # explicitly named leaf, never via a broad section removal.
        parent: Dict[str, Any] = config
        parts = str(raw_path).split(".")
        for part in parts[:-1]:
            child = parent.get(part)
            if not isinstance(child, dict):
                parent = {}
                break
            parent = child
        if parent:
            parent.pop(parts[-1], None)
    topology = _as_dict(config.get("topology"))
    if not same_resource and topology:
        config.pop("topology", None)
    return config


def _response_signature(payload: Mapping[str, Any], *, quality: str) -> Any:
    rows = sorted(_responses(payload), key=lambda item: int(item.get("index", 0) or 0))
    if quality == "status_only":
        return [(int(row.get("status_code", 0) or 0), bool(row.get("error"))) for row in rows]
    if quality == "structural":
        return [
            (
                int(row.get("status_code", 0) or 0),
                str(row.get("finish_reason", "") or ""),
                int((row.get("usage") or {}).get("prompt_tokens", row.get("prompt_tokens", 0)) or 0),
            )
            for row in rows
        ]
    return [str(row.get("response_text", "") or "") for row in rows]


def _quality_gate(
    baseline: Sequence[Mapping[str, Any]],
    candidate: Sequence[Mapping[str, Any]],
    *,
    quality: str,
) -> Dict[str, Any]:
    baseline_signatures = [_response_signature(payload, quality=quality) for payload in baseline]
    candidate_signatures = [_response_signature(payload, quality=quality) for payload in candidate]
    if quality == "status_only" and not any(
        _responses(payload) for payload in list(baseline) + list(candidate)
    ):
        # Scheduler/fault/worker-pipeline scenarios can be valid production
        # control-plane traces without OpenAI response rows.  Their scenarios
        # have already passed their dedicated gates; status-only comparisons
        # are diagnostic and must not fabricate a response signature.
        return {
            "pass": True,
            "mode": quality,
            "baseline_signature_count": 0,
            "candidate_signature_count": 0,
            "semantic_strength": "diagnostic_only",
            "non_serving_trace_comparison": True,
        }
    all_signatures = baseline_signatures + candidate_signatures
    if not all_signatures:
        return {"pass": False, "mode": quality, "reason": "no_response_signatures"}
    # Exact quality is intentionally stringent.  Structural/status checks are
    # useful for transport diagnostics but are labelled as such in the report;
    # they cannot independently justify a semantic model-quality claim.
    canonical = all_signatures[0]
    return {
        "pass": all(signature == canonical for signature in all_signatures),
        "mode": quality,
        "baseline_signature_count": len({_canonical_json(value) for value in baseline_signatures}),
        "candidate_signature_count": len({_canonical_json(value) for value in candidate_signatures}),
        "semantic_strength": "strong" if quality == "exact" else "diagnostic_only",
    }


def _bootstrap_improvement(
    baseline: Sequence[float],
    candidate: Sequence[float],
    *,
    higher_is_better: bool,
    seed: int,
    resamples: int = 4000,
) -> Dict[str, float | None]:
    if not baseline or not candidate or any(value <= 0.0 for value in baseline):
        return {"point_pct": None, "ci95_low_pct": None, "ci95_high_pct": None}
    baseline_mean = statistics.fmean(baseline)
    candidate_mean = statistics.fmean(candidate)
    point = (
        (candidate_mean - baseline_mean) / baseline_mean
        if higher_is_better
        else (baseline_mean - candidate_mean) / baseline_mean
    )
    rng = random.Random(seed)
    bootstrap: List[float] = []
    for _ in range(max(100, int(resamples))):
        base_mean = statistics.fmean(rng.choice(baseline) for _ in baseline)
        candidate_mean_resampled = statistics.fmean(rng.choice(candidate) for _ in candidate)
        if base_mean <= 0.0:
            continue
        bootstrap.append(
            (
                (candidate_mean_resampled - base_mean) / base_mean
                if higher_is_better
                else (base_mean - candidate_mean_resampled) / base_mean
            )
        )
    return {
        "point_pct": float(point * 100.0),
        "ci95_low_pct": float(_percentile(bootstrap, 0.025) * 100.0) if bootstrap else None,
        "ci95_high_pct": float(_percentile(bootstrap, 0.975) * 100.0) if bootstrap else None,
    }


def _metric_values(payloads: Sequence[Mapping[str, Any]], key: str) -> List[float]:
    values: List[float] = []
    for payload in payloads:
        value = _extract_metrics(payload).get(key)
        if value is not None and float(value) > 0.0:
            values.append(float(value))
    return values


def _evaluate_comparison(
    comparison: Mapping[str, Any],
    *,
    results: Mapping[str, Mapping[str, Any]],
    payloads: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    identifier = str(comparison.get("id"))
    kind = str(comparison.get("kind"))
    baseline_ids = [str(value) for value in _as_list(comparison.get("baseline"))]
    candidate_ids = [str(value) for value in _as_list(comparison.get("candidate"))]
    min_samples = max(1, int(comparison.get("min_samples", 3) or 3))
    quality = str(comparison.get("quality", "structural") or "structural")
    allowed_config_differences = [
        str(value)
        for value in _as_list(comparison.get("allowed_config_differences"))
    ]
    same_resource = kind in {"transport_ab", "same_resource_epd", "tuning_ab", "cache_ab"}
    failures: List[str] = []
    for role, identifiers in (("baseline", baseline_ids), ("candidate", candidate_ids)):
        for scenario_id in identifiers:
            result = results.get(scenario_id)
            if not result or result.get("status") != "passed" or scenario_id not in payloads:
                failures.append(f"{role}_scenario_not_passed:{scenario_id}")
    if failures:
        return {
            "id": identifier,
            "kind": kind,
            "status": "incomplete",
            "failures": failures,
            "required_samples": min_samples,
        }
    baseline_payloads = [payloads[item] for item in baseline_ids]
    candidate_payloads = [payloads[item] for item in candidate_ids]
    if len(baseline_payloads) < min_samples or len(candidate_payloads) < min_samples:
        failures.append(
            f"insufficient_samples:baseline={len(baseline_payloads)},candidate={len(candidate_payloads)},required={min_samples}"
        )
    baseline_configs = [
        _comparison_config(
            payload,
            same_resource=same_resource,
            ignore_cache_controls=kind == "cache_ab",
            ignore_config_paths=(
                allowed_config_differences if kind == "tuning_ab" else ()
            ),
        )
        for payload in baseline_payloads
    ]
    candidate_configs = [
        _comparison_config(
            payload,
            same_resource=same_resource,
            ignore_cache_controls=kind == "cache_ab",
            ignore_config_paths=(
                allowed_config_differences if kind == "tuning_ab" else ()
            ),
        )
        for payload in candidate_payloads
    ]
    config_signatures = {_canonical_json(value) for value in baseline_configs + candidate_configs}
    if len(config_signatures) != 1:
        failures.append("benchmark_config_mismatch_after_allowed_protocol_difference")
    if same_resource:
        topologies = [_as_dict(_as_dict(payload.get("benchmark_config")).get("topology")) for payload in baseline_payloads + candidate_payloads]
        if len({_canonical_json(value) for value in topologies}) != 1:
            failures.append("resource_topology_mismatch")
    expected_baseline_transport = comparison.get("baseline_transport")
    expected_candidate_transport = comparison.get("candidate_transport")
    for role, expected, values in (
        ("baseline", expected_baseline_transport, baseline_payloads),
        ("candidate", expected_candidate_transport, candidate_payloads),
    ):
        if not expected:
            continue
        for payload in values:
            evidence = evaluate_transport_evidence(payload, requested=str(expected))
            if not evidence.get("pass"):
                failures.append(f"{role}_transport_evidence_failed:{expected}")
                break
    quality_result = _quality_gate(baseline_payloads, candidate_payloads, quality=quality)
    if not quality_result.get("pass"):
        failures.append(f"quality_gate_failed:{quality}")
    metrics: Dict[str, Any] = {}
    for key, higher_is_better in (
        ("avg_ttft_ms", False),
        ("request_throughput_rps", True),
        ("output_token_goodput_tps", True),
        ("route_correct_rate", True),
        ("sla_satisfied_rate", True),
    ):
        baseline_values = _metric_values(baseline_payloads, key)
        candidate_values = _metric_values(candidate_payloads, key)
        metrics[key] = {
            "baseline": _stats(baseline_values),
            "candidate": _stats(candidate_values),
            "improvement": _bootstrap_improvement(
                baseline_values,
                candidate_values,
                higher_is_better=higher_is_better,
                seed=int(hashlib.sha256(f"{identifier}:{key}".encode("utf-8")).hexdigest()[:8], 16),
            ),
        }
    primary_metric = str(comparison.get("primary_metric", "avg_ttft_ms") or "avg_ttft_ms")
    primary = _as_dict(metrics.get(primary_metric))
    improvement = _as_dict(primary.get("improvement"))
    # A cache experiment can prove a cache-specific diagnostic but never a
    # general cold-traffic efficiency claim.  Likewise, a single restart has
    # no meaningful uncertainty estimate even when bootstrap resampling emits
    # an apparently positive interval.  Keep the implementation aligned with
    # the published protocol: a strong claim needs three independent runs,
    # same resources, exact outputs and a positive lower confidence bound.
    strong_claim = bool(
        kind not in {"cache_ab", "scale_out_capacity"}
        and min_samples >= 3
        and not failures
        and len(baseline_payloads) >= 3
        and len(candidate_payloads) >= 3
        and quality == "exact"
        and improvement.get("ci95_low_pct") is not None
        and float(improvement["ci95_low_pct"]) > 0.0
    )
    return {
        "id": identifier,
        "kind": kind,
        "status": "passed" if not failures else "incomplete",
        "failures": failures,
        "baseline_ids": baseline_ids,
        "candidate_ids": candidate_ids,
        "required_samples": min_samples,
        "quality": quality_result,
        "same_resource": same_resource,
        "allowed_config_differences": allowed_config_differences,
        "metrics": metrics,
        "strong_performance_claim_supported": strong_claim,
        "claim_boundary": (
            "cache-specific diagnostic; cache hit/miss evidence does not prove cold-traffic efficiency"
            if kind == "cache_ab"
            else
            "statistically repeated, exact-output same-resource evidence"
            if strong_claim
            else "diagnostic/scale-out evidence only; do not claim a proven efficiency gain"
        ),
    }


def run_campaign(
    manifest: Mapping[str, Any],
    *,
    manifest_dir: Path,
    execute_commands: bool = False,
) -> Dict[str, Any]:
    """Evaluate or explicitly execute every scenario declared by a manifest."""

    schema_failures = validate_campaign_manifest(manifest)
    if schema_failures:
        raise ValueError("invalid benchmark campaign manifest: " + "; ".join(schema_failures))
    results: Dict[str, Dict[str, Any]] = {}
    payloads: Dict[str, Dict[str, Any]] = {}
    for raw in _as_list(manifest.get("scenarios")):
        scenario = dict(raw)
        result, payload = _run_scenario(
            scenario,
            manifest_dir=manifest_dir,
            execute_commands=execute_commands,
        )
        identifier = str(scenario["id"])
        results[identifier] = result
        if payload is not None:
            payloads[identifier] = payload
    comparisons = [
        _evaluate_comparison(dict(raw), results=results, payloads=payloads)
        for raw in _as_list(manifest.get("comparisons"))
    ]
    required = [result for result in results.values() if bool(result.get("required"))]
    required_passed = [result for result in required if result.get("status") == "passed"]
    blocked = [result for result in required if result.get("status") == "blocked"]
    incomplete = [
        result
        for result in required
        if result.get("status") not in {"passed", "blocked"}
    ]
    bottlenecks: List[Dict[str, Any]] = []
    for identifier, result in results.items():
        for item in _as_list(result.get("bottlenecks")):
            if isinstance(item, Mapping):
                bottlenecks.append({"scenario_id": identifier, **dict(item)})
    bottlenecks.sort(key=lambda item: float(item.get("ttft_fraction") or 0.0), reverse=True)
    report = {
        "schema_version": CAMPAIGN_SCHEMA_VERSION,
        "campaign_id": str(manifest.get("campaign_id")),
        "generated_at_unix": time.time(),
        "manifest_sha256": hashlib.sha256(_canonical_json(manifest).encode("utf-8")).hexdigest(),
        "execute_commands": bool(execute_commands),
        "scenarios": list(results.values()),
        "comparisons": comparisons,
        "coverage": {
            "required": len(required),
            "required_passed": len(required_passed),
            "required_blocked": len(blocked),
            "required_incomplete_or_failed": len(incomplete),
            "complete": len(required_passed) == len(required),
            "blocked_scenarios": [result["id"] for result in blocked],
            "incomplete_scenarios": [result["id"] for result in incomplete],
        },
        "bottleneck_rankings": bottlenecks,
        "claim_policy": {
            "minimum_independent_runs": 3,
            "strong_claim_requires": "same_resource + exact output + positive 95% bootstrap lower bound",
            "scale_out_policy": "capacity observations are reported but never labelled same-resource efficiency gains",
        },
    }
    return report


def load_and_run_campaign(
    manifest_path: str | Path,
    *,
    execute_commands: bool = False,
) -> Dict[str, Any]:
    path = Path(manifest_path).expanduser().resolve()
    return run_campaign(_load_json(path), manifest_dir=path.parent, execute_commands=execute_commands)
