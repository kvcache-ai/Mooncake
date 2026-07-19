#!/usr/bin/env python3
"""Run strict real vLLM online EPD direct-buffer E2E validation.

Unlike ``run_vllm_feature_handle_e2e.py`` this runner does not prebuild a
FeatureHandle request.  It starts an online E-stage encoder service, lets the
proxy call ``/describe`` + Prefill ``/allocate`` + encoder ``/publish_direct``,
and gates the result on vLLM consuming direct FeatureHandles with no transfer
fallback and no Prefill-side vision encoder execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import (  # noqa: E402
    SCHEDULER_POLICIES,
    VENV_ROOT,
    VLLMDisaggConfig,
    _common_env_block,
    _pick_free_port,
    generate_configs,
)
from mooncake_epd.scripts.run_vllm_feature_handle_e2e import summarize_feature_handle_metrics  # noqa: E402
from mooncake_epd.scripts.benchmark_request_variants import (  # noqa: E402
    REQUEST_VARIATION_NONE,
    REQUEST_VARIATION_UNIQUE_PREFIX,
    REQUEST_VARIATION_UNIQUE_SUFFIX,
    VLLM_PREFIX_CACHE_ISOLATE,
    VLLM_PREFIX_CACHE_REUSE,
    apply_request_variation,
    apply_vllm_prefix_cache_policy,
    request_variation_spec,
    vllm_prefix_cache_spec,
)
from mooncake_epd.scripts.benchmark_artifact_io import (  # noqa: E402
    write_raw_benchmark_artifacts,
)
from mooncake_epd.core.transfer import (  # noqa: E402
    collect_mooncake_worker_transport_evidence,
    require_mooncake_protocol_support,
    validate_mooncake_protocol_pair,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _data_url_for_demo_image,
    _dataset_warmup_coverage,
    _dataset_replay_context,
    _dataset_request_evidence_row,
    _ensure_process_running,
    _execute_dataset_request,
    _extract_choice_text,
    _extract_port,
    _load_dataset_requests,
    _launch,
    _materialize_dataset_replay_entry,
    _resolve_dataset_warmup_requests,
    _proc_env,
    _tail_text,
    _terminate_all,
    _wait_for_metrics_settle,
    _wait_ready,
)


def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * float(pct)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _latency_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {
            "count": 0,
            "avg": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }
    return {
        "count": len(values),
        "avg": _avg(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _effective_direct_engine_protocol(args: argparse.Namespace) -> str:
    """Return the E→P protocol that generated workers will actually use."""

    protocol = str(
        getattr(args, "mooncake_protocol", "tcp") or "tcp"
    ).strip().lower()
    direct = str(
        getattr(args, "direct_engine_protocol", "") or ""
    ).strip().lower()
    return direct or ("tcp" if protocol == "shm" else protocol)


def _validate_requested_transport_capabilities(args: argparse.Namespace) -> None:
    """Reject unavailable native transports before creating artifacts/processes."""

    protocol = str(
        getattr(args, "mooncake_protocol", "tcp") or "tcp"
    ).strip().lower()
    direct = _effective_direct_engine_protocol(args)
    text_only = bool(getattr(args, "text_only", False))
    if (
        str(args.mooncake_protocol).strip().lower() == "tcp"
        and not bool(getattr(args, "tcp_write_completion_ack", True))
    ):
        raise ValueError(
            "strict online EPD over TCP requires --tcp-write-completion-ack; "
            "run an explicitly non-strict transport experiment for the unsafe "
            "sender-completion-only ablation"
        )
    vllm_prefix_cache_mode = str(
        getattr(args, "vllm_prefix_cache_mode", VLLM_PREFIX_CACHE_ISOLATE)
        or VLLM_PREFIX_CACHE_ISOLATE
    )
    # Validate once up front so a malformed policy fails before any service is
    # launched or benchmark artifact is created.
    vllm_prefix_cache_spec(vllm_prefix_cache_mode)
    if not text_only:
        validate_mooncake_protocol_pair(protocol, direct)
    requested_protocols = (protocol, direct if not text_only else "")
    for requested in dict.fromkeys(requested_protocols):
        if requested:
            require_mooncake_protocol_support(requested)


def _worker_transport_logs(
    logs: Dict[str, Path],
) -> Dict[str, Path]:
    """Return one native EngineCore log per Prefill/Decode worker."""

    return {
        key.replace("_", "-", 1): path
        for key, path in logs.items()
        if key.startswith(("prefill_", "decode_"))
    }


def _direct_engine_transport_logs(logs: Dict[str, Path]) -> Dict[str, Path]:
    """Return the E→P native-engine participants for a multimodal run."""

    selected: Dict[str, Path] = {}
    if "encoder" in logs:
        selected["encoder"] = logs["encoder"]
    selected.update(
        {
            key.replace("_", "-", 1): path
            for key, path in logs.items()
            if key.startswith("prefill_")
        }
    )
    return selected


def _record_runtime_transport_evidence(
    summary: Dict[str, Any],
    *,
    logs: Dict[str, Path],
    requested: str,
    evidence_key: str = "transport_runtime_evidence",
    worker_logs: Dict[str, Path] | None = None,
) -> None:
    """Persist and enforce worker-side native transport selection evidence."""

    evidence = collect_mooncake_worker_transport_evidence(
        worker_logs if worker_logs is not None else _worker_transport_logs(logs),
        requested=requested,
    )
    summary[evidence_key] = evidence
    if bool(evidence.get("applicable", False)) and not bool(evidence.get("pass", False)):
        details = "; ".join(str(item) for item in evidence.get("failures", ()))
        raise AssertionError(
            "native Mooncake transport evidence failed; refusing protocol claim"
            + (f": {details}" if details else "")
        )


def _summarize_direct_cache_timing(
    responses: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Summarize request-visible persistent direct-feature cache behavior."""

    hit_values = [
        max(
            0.0,
            float(
                dict(item.get("epd_timing_ms") or {}).get("direct_cache_hits", 0.0)
                or 0.0
            ),
        )
        for item in responses
    ]
    lookup_values = [
        max(
            0.0,
            float(
                dict(item.get("epd_timing_ms") or {}).get("direct_cache_lookup_ms", 0.0)
                or 0.0
            ),
        )
        for item in responses
        if "direct_cache_lookup_ms" in dict(item.get("epd_timing_ms") or {})
    ]
    summary: Dict[str, Any] = {
        "direct_feature_cache_hits": int(sum(hit_values)),
        "direct_feature_cache_hit_requests": sum(
            1 for value in hit_values if value > 0.0
        ),
    }
    if lookup_values:
        summary["direct_feature_cache_lookup_ms"] = sum(lookup_values)
        summary["direct_feature_cache_lookup_ms_avg"] = _avg(lookup_values)
        summary["direct_feature_cache_lookup_ms_stats"] = _latency_stats(lookup_values)
    return summary


def _summarize_rendered_prefill_cache_timing(
    responses: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Summarize measured request-visible ``/render`` cache behavior."""

    hit_values = [
        max(
            0.0,
            float(
                dict(item.get("epd_timing_ms") or {}).get(
                    "prefill_render_cache_hit", 0.0
                )
                or 0.0
            ),
        )
        for item in responses
    ]
    return {
        "rendered_prefill_cache_hit_requests": sum(
            1 for value in hit_values if value > 0.0
        ),
        "rendered_prefill_cache_miss_requests": sum(
            1 for value in hit_values if value <= 0.0
        ),
    }


_PROXY_WIRE_BYTE_METRICS = (
    "proxy_request_body_bytes",
    "prefill_render_request_bytes",
    "prefill_render_response_bytes",
    "prefill_generate_request_bytes",
    "prefill_generate_response_bytes",
    "prefill_openai_request_bytes",
    "prefill_openai_response_bytes",
    "prefill_openai_prompt_only_request_bytes",
    "prefill_openai_prompt_only_response_bytes",
    "decode_request_bytes",
    "decode_response_bytes",
)


def _summarize_proxy_wire_metrics(
    responses: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Aggregate actual request/response sizes captured by the proxy."""

    summary: Dict[str, Any] = {}
    for metric_name in _PROXY_WIRE_BYTE_METRICS:
        values = [
            max(
                0.0,
                float(
                    dict(item.get("epd_timing_ms") or {}).get(metric_name, 0.0)
                    or 0.0
                ),
            )
            for item in responses
            if metric_name in dict(item.get("epd_timing_ms") or {})
        ]
        if not values:
            continue
        summary[f"{metric_name}_total"] = sum(values)
        summary[f"{metric_name}_avg"] = _avg(values)
        summary[f"{metric_name}_stats"] = _latency_stats(values)
    return summary


def _summarize_direct_feature_delivery(
    responses: Sequence[Dict[str, Any]],
) -> Dict[str, int]:
    """Prove each measured E→P feature reached Prefill via direct delivery.

    ``precomputed_hits`` is emitted by the optional worker hidden-cache
    instrumentation.  Deliberately disabling that cache for a no-cache
    control also disables this counter, even though the proxy still allocates,
    publishes and Prefill consumes the FeatureHandle.  Request-visible direct
    publish timings are independent evidence for that control path.
    """

    direct_publish_requests = 0
    direct_cache_hit_requests = 0
    for response in responses:
        timings = dict(response.get("epd_timing_ms") or {})
        if float(timings.get("direct_cache_hits", 0.0) or 0.0) > 0.0:
            direct_cache_hit_requests += 1
            continue
        publish_bytes = float(
            timings.get("direct_publish_engine_nbytes", 0.0) or 0.0
        )
        required_events = (
            "direct_describe_ms",
            "direct_allocate_ms",
            "direct_publish_ms",
            "direct_mark_ready_ms",
        )
        if publish_bytes > 0.0 and all(event in timings for event in required_events):
            direct_publish_requests += 1
    return {
        "measured_requests": len(responses),
        "direct_publish_requests": direct_publish_requests,
        "direct_cache_hit_requests": direct_cache_hit_requests,
        "direct_feature_delivery_requests": direct_publish_requests
        + direct_cache_hit_requests,
    }


def _summarize_worker_dispatches(
    responses: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, int]]:
    """Count measured requests by the stage owners reported by the proxy."""

    summary: Dict[str, Dict[str, int]] = {
        "prefill_worker_dispatches": {},
        "decode_worker_dispatches": {},
    }
    for response in responses:
        for response_key, summary_key in (
            ("prefill_worker_id", "prefill_worker_dispatches"),
            ("decode_worker_id", "decode_worker_dispatches"),
        ):
            worker_id = str(response.get(response_key) or "").strip()
            if not worker_id:
                continue
            counts = summary[summary_key]
            counts[worker_id] = int(counts.get(worker_id, 0) or 0) + 1
    return summary


def _summarize_worker_dispatch_balance(
    dispatches: Mapping[str, Mapping[str, int]],
    *,
    prefill_worker_ids: Sequence[str],
    decode_worker_ids: Sequence[str],
) -> Dict[str, Dict[str, float | int | Dict[str, int]]]:
    """Expose multi-worker traffic balance without treating it as throughput.

    A 2P2D experiment can pass every serving gate while leaving one replica
    almost idle.  Include zero-dispatch configured workers so the artifact
    makes that capacity bottleneck visible rather than letting a sparse map
    look balanced by omission.  Normalized entropy is one for an even split
    and zero when all observed traffic lands on one worker.
    """

    def summarize(counts: Mapping[str, int], worker_ids: Sequence[str]) -> Dict[str, float | int | Dict[str, int]]:
        ids = sorted({str(worker_id) for worker_id in worker_ids} | {str(key) for key in counts})
        observed = {worker_id: max(0, int(counts.get(worker_id, 0) or 0)) for worker_id in ids}
        total = sum(observed.values())
        workers = len(ids)
        active = sum(1 for value in observed.values() if value > 0)
        if total <= 0 or workers <= 0:
            entropy = 0.0
            max_share = 0.0
        else:
            probabilities = [value / total for value in observed.values() if value > 0]
            entropy = (
                1.0
                if workers == 1
                else -sum(value * math.log(value) for value in probabilities) / math.log(workers)
            )
            max_share = max(observed.values()) / total
        return {
            "configured_workers": workers,
            "active_workers": active,
            "total_dispatches": total,
            "counts": observed,
            "normalized_entropy": float(entropy),
            "max_share": float(max_share),
        }

    return {
        "prefill": summarize(
            dispatches.get("prefill_worker_dispatches") or {}, prefill_worker_ids
        ),
        "decode": summarize(
            dispatches.get("decode_worker_dispatches") or {}, decode_worker_ids
        ),
    }


def _resolve_warmup_concurrency(
    *,
    requested: int,
    effective_warmups: int,
    measured_concurrency: int,
    prefill_workers: int,
    decode_workers: int,
) -> int:
    """Resolve warmup concurrency.

    ``requested <= 0`` means auto: cover first-forward/JIT shapes across the
    deployed worker fanout before measured traffic.  This is critical for real
    EPD serving because sequential warmup can leave some Decode workers cold and
    inflate the first measured TTFT.
    """

    if int(effective_warmups) <= 0:
        return 0
    if int(requested) > 0:
        return min(int(requested), int(effective_warmups))
    target = max(
        1,
        int(measured_concurrency),
        int(prefill_workers),
        int(decode_workers),
    )
    return min(int(effective_warmups), target)


_COMPILE_LOG_MARKERS = (
    "triton kernel jit compilation during inference",
    "jit compilation during inference",
    "torch.compile during inference",
)


def _measurement_quiescence_failures(metrics_payload: Mapping[str, Any]) -> List[str]:
    """Return serving work that must drain before measurement starts."""

    failures: List[str] = []
    workers = dict(metrics_payload.get("workers") or {})
    for stage in ("prefill", "decode"):
        for worker in list(workers.get(stage) or []):
            worker_data = dict(worker or {})
            worker_id = str(worker_data.get("worker_id") or f"{stage}-unknown")
            current_load = int(worker_data.get("current_load", 0) or 0)
            queue_size = int(worker_data.get("queue_size", 0) or 0)
            if current_load:
                failures.append(f"{worker_id}.current_load={current_load}")
            if queue_size:
                failures.append(f"{worker_id}.queue_size={queue_size}")
    active_requests = list(metrics_payload.get("active_requests") or [])
    if active_requests:
        failures.append(f"active_requests={len(active_requests)}")
    release = dict(metrics_payload.get("direct_feature_release") or {})
    release_inflight = int(release.get("inflight", 0) or 0)
    if release_inflight:
        failures.append(f"direct_feature_release.inflight={release_inflight}")
    mm_store = dict(metrics_payload.get("mm_store") or {})
    mm_queue_size = int(mm_store.get("queue_size", 0) or 0)
    mm_inflight = int(mm_store.get("inflight_events", 0) or 0)
    if mm_queue_size:
        failures.append(f"mm_store.queue_size={mm_queue_size}")
    if mm_inflight:
        failures.append(f"mm_store.inflight_events={mm_inflight}")
    return failures


def _read_log_growth(
    log_paths: Mapping[str, Path],
    offsets: Dict[str, int],
) -> tuple[List[Dict[str, str]], Dict[str, int]]:
    """Read appended log bytes and return compile events without rescanning."""

    compile_events: List[Dict[str, str]] = []
    next_offsets = dict(offsets)
    for name, path in log_paths.items():
        key = str(name)
        try:
            size = path.stat().st_size
        except OSError:
            continue
        offset = max(0, int(offsets.get(key, 0) or 0))
        if size < offset:
            offset = 0
        if size == offset:
            next_offsets[key] = size
            continue
        try:
            with path.open("rb") as handle:
                handle.seek(offset)
                appended = handle.read(max(0, size - offset))
        except OSError:
            continue
        next_offsets[key] = size
        text = appended.decode("utf-8", errors="replace")
        for line in text.splitlines():
            lowered = line.lower()
            if any(marker in lowered for marker in _COMPILE_LOG_MARKERS):
                compile_events.append({"log": key, "line": line[-1000:]})
    return compile_events, next_offsets


def _wait_for_measurement_purity(
    session,
    metrics_url: str,
    *,
    log_paths: Mapping[str, Path],
    initial_metrics: Mapping[str, Any] | None = None,
    timeout_s: float = 20.0,
    poll_s: float = 0.5,
    stable_polls: int = 2,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Wait for drained queues and consecutive compile-quiet windows."""

    started_at = time.monotonic()
    deadline = started_at + max(1.0, float(timeout_s))
    required_stable_polls = max(1, int(stable_polls))
    offsets = {
        str(name): (path.stat().st_size if path.exists() else 0)
        for name, path in log_paths.items()
    }
    stable_count = 0
    polls = 0
    observed_compile_events: List[Dict[str, str]] = []
    last_metrics = dict(initial_metrics or {})
    last_failures = _measurement_quiescence_failures(last_metrics)

    while time.monotonic() < deadline:
        time.sleep(max(0.05, float(poll_s)))
        polls += 1
        try:
            last_metrics = dict(session.get(metrics_url, timeout=30).json() or {})
            last_failures = _measurement_quiescence_failures(last_metrics)
        except Exception as exc:
            last_failures = [f"metrics_fetch={type(exc).__name__}: {exc}"]
        compile_events, offsets = _read_log_growth(log_paths, offsets)
        if compile_events:
            observed_compile_events.extend(compile_events)
        if not last_failures and not compile_events:
            stable_count += 1
        else:
            stable_count = 0
        if stable_count >= required_stable_polls:
            return (
                {
                    "pass": True,
                    "polls": polls,
                    "stable_polls": stable_count,
                    "required_stable_polls": required_stable_polls,
                    "wait_ms": (time.monotonic() - started_at) * 1000.0,
                    "quiescence_failures": [],
                    "compile_events_after_warmup": observed_compile_events,
                },
                last_metrics,
            )
    return (
        {
            "pass": False,
            "polls": polls,
            "stable_polls": stable_count,
            "required_stable_polls": required_stable_polls,
            "wait_ms": (time.monotonic() - started_at) * 1000.0,
            "quiescence_failures": last_failures,
            "compile_events_after_warmup": observed_compile_events,
        },
        last_metrics,
    )


def _session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _int_tuple(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(int(item) for item in value)
    return (int(value),)


def _worker_affinity_tuples(value: Any) -> tuple[tuple[str, str], ...]:
    """Parse benchmark CLI P->D locality pairs before generating scripts."""

    if not value:
        return ()
    parsed: list[tuple[str, str]] = []
    seen: dict[str, str] = {}
    for raw in value:
        prefill, separator, decode = str(raw or "").partition("=")
        prefill = prefill.strip()
        decode = decode.strip()
        if not separator or not prefill or not decode:
            raise ValueError(
                "--prefill-decode-affinity entries must use prefill-worker=decode-worker"
            )
        previous = seen.get(prefill)
        if previous is not None and previous != decode:
            raise ValueError(
                f"conflicting Decode affinity targets for {prefill}: {previous} and {decode}"
            )
        if previous is None:
            seen[prefill] = decode
            parsed.append((prefill, decode))
    return tuple(parsed)


def _request_image_urls(args: argparse.Namespace) -> List[str]:
    """Resolve one or more image inputs without changing the legacy CLI.

    ``--image-urls`` exists for the multi-image E->P batch benchmark.  It is
    intentionally distinct from repeated request traffic: all returned URLs
    belong to one multimodal request and therefore exercise one Prefill-owned
    direct-buffer allocation/publication transaction.
    """

    explicit_many = [
        str(value).strip()
        for value in list(getattr(args, "image_urls", None) or [])
        if str(value).strip()
    ]
    explicit_one = str(getattr(args, "image_url", None) or "").strip()
    if explicit_many:
        if explicit_one:
            raise ValueError("--image-url and --image-urls are mutually exclusive")
        return explicit_many
    if explicit_one:
        return [explicit_one]
    return [_data_url_for_demo_image(str(args.demo_image))]


def _ports_from_scripts(scripts: Sequence[str]) -> List[int]:
    return [_extract_port(Path(script)) for script in scripts]


def _write_online_request(args: argparse.Namespace, request_path: Path) -> None:
    text_only = bool(getattr(args, "text_only", False))
    prompt_file = str(getattr(args, "prompt_file", "") or "").strip()
    prompt = (
        Path(prompt_file).expanduser().read_text(encoding="utf-8")
        if prompt_file
        else str(args.prompt)
    )
    if not prompt.strip():
        raise ValueError("prompt must not be empty")
    content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
    if not text_only:
        content = [
            {"type": "image_url", "image_url": {"url": image_url}}
            for image_url in _request_image_urls(args)
        ] + content
    body = {
        "model": str(args.model),
        "messages": [
            {
                "role": "user",
                "content": content,
            }
        ],
        "max_tokens": int(args.max_tokens),
        "temperature": float(args.temperature),
        "metadata": {"workflow_id": str(args.workflow_id)},
    }
    request_path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")


def _request_fingerprint(request_path: Path) -> str:
    """Hash the stable inference payload without per-request workflow metadata."""

    payload = json.loads(request_path.read_text(encoding="utf-8"))
    payload.pop("metadata", None)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _optional_positive_int(value: Any) -> int | None:
    parsed = int(value or 0)
    return parsed if parsed > 0 else None


def _online_direct_entry(
    request_path: Path | None,
    *,
    repeat_idx: int,
    workflow_prefix: str,
    request_variation: str = REQUEST_VARIATION_NONE,
    vllm_prefix_cache_mode: str = VLLM_PREFIX_CACHE_ISOLATE,
    phase: str = "measure",
    dataset_entries: Sequence[Dict[str, Any]] | None = None,
    dataset_workflow_id_mode: str = "unique",
) -> Dict[str, Any]:
    if dataset_entries:
        return _materialize_dataset_replay_entry(
            list(dataset_entries),
            repeat_idx=repeat_idx,
            workflow_prefix=workflow_prefix,
            workflow_id_mode=dataset_workflow_id_mode,
            request_variation=request_variation,
            phase=phase,
            vllm_prefix_cache_mode=vllm_prefix_cache_mode,
        )
    if request_path is None:
        raise ValueError("request_path is required when no dataset replay is selected")
    request_body = json.loads(request_path.read_text(encoding="utf-8"))
    metadata = dict(request_body.get("metadata") or {})
    metadata["workflow_id"] = f"{workflow_prefix}-r{repeat_idx}"
    request_body["metadata"] = metadata
    variation_id = apply_request_variation(
        request_body,
        mode=request_variation,
        phase=phase,
        repeat_idx=repeat_idx,
    )
    prefix_cache_id = apply_vllm_prefix_cache_policy(
        request_body,
        mode=vllm_prefix_cache_mode,
        phase=phase,
        repeat_idx=repeat_idx,
    )
    return {
        "request": request_body,
        "family": "online_direct_e2e",
        "request_variation_id": variation_id,
        "vllm_prefix_cache_id": prefix_cache_id,
        "sample": {
            "sample_id": f"online-direct-e2e-r{repeat_idx}",
            "workflow_id": metadata.get("workflow_id", f"online-direct-e2e-r{repeat_idx}"),
            "source_dataset": "online_direct_e2e",
            "request_variation_id": variation_id,
            "vllm_prefix_cache_id": prefix_cache_id,
        },
    }


def _run_online_direct_request(
    *,
    proxy_url: str,
    request_path: Path | None,
    repeat_idx: int,
    workflow_prefix: str,
    request_timeout: float,
    request_variation: str = REQUEST_VARIATION_NONE,
    vllm_prefix_cache_mode: str = VLLM_PREFIX_CACHE_ISOLATE,
    phase: str = "measure",
    dataset_entries: Sequence[Dict[str, Any]] | None = None,
    dataset_workflow_id_mode: str = "unique",
    raise_on_failure: bool = True,
) -> Dict[str, Any]:
    response = _execute_dataset_request(
        proxy_url=proxy_url,
        entry=_online_direct_entry(
            request_path,
            repeat_idx=repeat_idx,
            workflow_prefix=workflow_prefix,
            request_variation=request_variation,
            vllm_prefix_cache_mode=vllm_prefix_cache_mode,
            phase=phase,
            dataset_entries=dataset_entries,
            dataset_workflow_id_mode=dataset_workflow_id_mode,
        ),
        index=repeat_idx,
        request_timeout=float(request_timeout),
        stream_metrics=True,
    )
    response["headers"] = {
        "x-epd-routing-path": response.get("routing_path"),
        "x-epd-admission": response.get("admission"),
        "x-epd-degrade-level": response.get("degrade_level"),
        "x-epd-prefill-worker": response.get("prefill_worker_id"),
        "x-epd-decode-worker": response.get("decode_worker_id"),
        "x-epd-timing-ms": response.get("epd_timing_ms_header"),
    }
    if int(response.get("status_code") or 0) >= 400 or response.get("error"):
        # Under concurrent load, retain every terminal response before failing
        # the benchmark.  A first failure used to escape the worker future,
        # discarding its peers' routing/timing evidence and making an
        # intermittent transport issue much harder to localize.  Sequential
        # and warmup calls keep fail-fast behavior by default.
        response["online_direct_request_failed"] = True
        if raise_on_failure:
            raise RuntimeError(
                f"online direct request failed: {json.dumps(response, ensure_ascii=False)[:4000]}"
            )
    return response


def _write_encoder_script(
    *,
    workdir: Path,
    cfg: VLLMDisaggConfig,
    encoder_port: int,
    args: argparse.Namespace,
    mooncake_json: Path,
) -> Path:
    script = workdir / "start_encoder.sh"
    script.write_text(
        "#!/bin/bash\n"
        + _common_env_block(cfg, mooncake_json)
        + "\n"
        + f"export MOONCAKE_EPD_ENCODER_HOST={cfg.local_hostname}\n"
        + f"export MOONCAKE_EPD_ENCODER_PORT={encoder_port}\n"
        + f"export MOONCAKE_EPD_ENCODER_MODEL={args.model}\n"
        + f"export MOONCAKE_EPD_ENCODER_DEVICE={args.encoder_device}\n"
        + f"export MOONCAKE_EPD_ENCODER_RUNTIME={getattr(args, 'encoder_runtime', 'transformers')}\n"
        + "export MOONCAKE_EPD_ENCODER_PUBLISH_BACKEND=direct_engine\n"
        + f"export MOONCAKE_EPD_DIRECT_SOURCE_MODE={args.direct_source_mode}\n"
        + f"{VENV_ROOT}/bin/python {REPO_ROOT / 'scripts' / 'epd_encoder_service.py'} "
        + f"--model {args.model} "
        + f"--device {args.encoder_device} "
        + f"--dtype {args.encoder_dtype} "
        + f"--encoder-family {args.encoder_family} "
        + f"--encoder-runtime {getattr(args, 'encoder_runtime', 'transformers')} "
        + "--publish-backend direct_engine "
        + f"--direct-source-mode {args.direct_source_mode} "
        + (
            "--checksum "
            if bool(getattr(args, "feature_handle_checksum", False))
            else ""
        )
        + f"--request-timeout-s {float(args.encoder_request_timeout_s)}\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _direct_buffer_stats(
    base_url: str,
    *,
    auth_token: str | None = None,
) -> Dict[str, Any]:
    """Read Prefill-owned buffer stats through the same control auth path.

    Direct-buffer routes live inside the vLLM API application.  Deployments
    with vLLM API authentication therefore reject an uncredentialed GET even
    though the proxy's allocate/release calls succeed.  The runner must use
    the per-run internal token when collecting terminal leak evidence.
    """

    sess = _session()
    try:
        if str(auth_token or "").strip():
            sess.headers.update({"X-Mooncake-EPD-Token": str(auth_token)})
        for path in ("/mooncake_epd/direct_feature_buffer/stats", "/direct_feature_buffer_stats"):
            try:
                resp = sess.get(f"{base_url}{path}", timeout=10)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                continue
    finally:
        sess.close()
    return {}


def _flush_proxy_direct_feature_lease_prefetch(
    base_url: str,
    *,
    auth_token: str | None = None,
) -> Dict[str, Any]:
    """Return spare Proxy lease reservations before terminal leak checks.

    This is deliberately a post-measurement action: it cannot affect TTFT or
    throughput samples, but it proves that a bounded lease-prefetch run leaves
    no live Prefill references before the artifact is accepted.
    """

    sess = _session()
    try:
        if str(auth_token or "").strip():
            sess.headers.update({"X-Mooncake-EPD-Token": str(auth_token)})
        response = sess.post(
            f"{base_url}/mooncake_epd/direct_feature_lease_prefetch/flush",
            timeout=20.0,
        )
        response.raise_for_status()
        payload = response.json()
        return dict(payload) if isinstance(payload, dict) else {}
    finally:
        sess.close()


def _merge_direct_buffer_stats(stats_by_worker: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {"workers": stats_by_worker}
    numeric_fields = {
        "allocations",
        "bytes",
        "managed_buffers",
        "ready",
        "ref_count",
        "ref_count_total",
        "cache_hits",
        "cache_misses",
        "evictions",
    }
    for field in numeric_fields:
        total = 0
        seen = False
        for stats in stats_by_worker.values():
            if not isinstance(stats, dict) or field not in stats:
                continue
            try:
                total += int(stats.get(field, 0) or 0)
                seen = True
            except Exception:
                continue
        if seen:
            merged[field] = total
    persistent_values = [
        bool(stats.get("persistent_cache", False))
        for stats in stats_by_worker.values()
        if isinstance(stats, dict) and "persistent_cache" in stats
    ]
    if persistent_values:
        merged["persistent_cache"] = all(persistent_values)
    return merged


def _wait_for_direct_buffer_release_settle(
    stats_urls: Sequence[str],
    *,
    timeout_s: float = 30.0,
    poll_s: float = 0.25,
    auth_token: str | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Wait until every Prefill direct-buffer service has no live request refs.

    Direct-buffer cleanup is deliberately queued off the user-visible Decode
    path. A benchmark must still wait at the terminal boundary before reading
    leak metrics; otherwise it races the release worker and can report a false
    allocation/refcount leak.
    """

    deadline = time.monotonic() + max(0.1, float(timeout_s))
    latest: Dict[str, Dict[str, Any]] = {}
    while True:
        latest = {
            str(url): (
                _direct_buffer_stats(str(url), auth_token=auth_token)
                if str(auth_token or "").strip()
                else _direct_buffer_stats(str(url))
            )
            for url in stats_urls
        }
        settled = True
        for stats in latest.values():
            if not stats:
                settled = False
                break
            persistent = bool(stats.get("persistent_cache", False))
            if int(stats.get("ref_count", 0) or 0) != 0:
                settled = False
                break
            if not persistent and int(stats.get("allocations", 0) or 0) != 0:
                settled = False
                break
        if settled or time.monotonic() >= deadline:
            return latest
        time.sleep(max(0.01, float(poll_s)))




_MEASURED_WINDOW_GAUGES = {
    "connector_metric_workers",
    "mm_hidden_cache_workers",
    "mm_hidden_cache_enabled_workers",
    "mm_hidden_cache_bytes",
    "mm_hidden_cache_entries",
    "decode_engine_timing_workers",
}
_CACHE_SNAPSHOT_GAUGES = {
    "entries",
    "bytes",
    "held_leases",
    "held_references",
}


def _cache_snapshot_delta(after: Any, before: Any) -> Dict[str, Any]:
    """Subtract monotonic cache counters while preserving cache footprint."""

    after_snapshot = dict(after or {}) if isinstance(after, dict) else {}
    before_snapshot = dict(before or {}) if isinstance(before, dict) else {}
    delta: Dict[str, Any] = {}
    for key, value in after_snapshot.items():
        if isinstance(value, bool):
            delta[key] = value
        elif isinstance(value, (int, float)):
            if key in _CACHE_SNAPSHOT_GAUGES:
                delta[key] = value
                continue
            baseline = before_snapshot.get(key, 0)
            if isinstance(baseline, bool) or not isinstance(baseline, (int, float)):
                baseline = 0
            delta[key] = value - baseline
        else:
            delta[key] = value
    return delta


def _numeric_metric_delta(after: Dict[str, Any], before: Dict[str, Any]) -> Dict[str, Any]:
    """Build a measured-window view from cumulative counters and gauges.

    Connector transfer counters are monotonic, whereas worker counts, cache
    footprint, rates, averages, and update timestamps are snapshots. Blindly
    subtracting every numeric field turns a valid two-worker snapshot into zero
    and produces negative timestamps. Keep gauges at their after value and
    recompute known derived values from measured counters.
    """

    after_metrics = dict(after.get("metrics") or after or {})
    before_metrics = dict(before.get("metrics") or before or {})
    delta: Dict[str, Any] = {}
    for key, value in after_metrics.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            if key in _MEASURED_WINDOW_GAUGES:
                delta[key] = value
                continue
            if key.endswith(("_avg", "_rate", "_updated_at")):
                continue
            base = before_metrics.get(key, 0)
            if isinstance(base, bool) or not isinstance(base, (int, float)):
                base = 0
            delta[key] = value - base
        elif key == "remote_transfer_backend_counts" and isinstance(value, dict):
            before_counts = dict(before_metrics.get(key) or {})
            delta[key] = {
                str(name): int(count or 0) - int(before_counts.get(name, 0) or 0)
                for name, count in value.items()
            }
    def _set_avg(avg_key: str, total_key: str, count_key: str) -> None:
        count = float(delta.get(count_key, 0) or 0)
        total = float(delta.get(total_key, 0.0) or 0.0)
        delta[avg_key] = (total / count) if count > 0 else 0.0

    _set_avg("layered_receive_kv_worker_ms_avg", "layered_receive_kv_worker_ms", "layered_receive_kv_worker_roundtrips")
    _set_avg("layered_receive_kv_first_response_ms_avg", "layered_receive_kv_first_response_ms", "layered_receive_kv_first_response_count")
    _set_avg("layered_receive_kv_last_response_ms_avg", "layered_receive_kv_last_response_ms", "layered_receive_kv_last_response_count")
    _set_avg("layered_receive_kv_response_process_ms_avg", "layered_receive_kv_response_process_ms", "layered_receive_kv_response_process_count")
    _set_avg("layered_receive_kv_first_group_ms_avg", "layered_receive_kv_first_group_ms", "layered_receive_kv_first_group_count")
    _set_avg("layered_receive_kv_finished_ms_avg", "layered_receive_kv_finished_ms", "layered_receive_kv_finished_count")
    _set_avg("peer_buffer_dispatch_ms_avg", "peer_buffer_dispatch_ms", "peer_buffer_batches")
    _set_avg("peer_buffer_prepare_ms_avg", "peer_buffer_prepare_ms", "peer_buffer_batches")
    _set_avg("peer_buffer_write_ms_avg", "peer_buffer_write_ms", "peer_buffer_batches")
    _set_avg(
        "peer_buffer_native_engine_lock_wait_ms_avg",
        "peer_buffer_native_engine_lock_wait_ms",
        "peer_buffer_batches",
    )
    _set_avg(
        "layered_source_ready_event_wait_ms_avg",
        "layered_source_ready_event_wait_ms",
        "layered_source_ready_event_waits",
    )
    peer_buffer_bytes = float(delta.get("peer_buffer_bytes", 0) or 0)
    peer_buffer_write_ms = float(delta.get("peer_buffer_write_ms", 0.0) or 0.0)
    delta["peer_buffer_write_bandwidth_gbps"] = (
        peer_buffer_bytes * 8.0 / peer_buffer_write_ms / 1_000_000.0
        if peer_buffer_write_ms > 0.0
        else 0.0
    )
    _set_avg("decode_engine_first_token_latency_ms_avg", "decode_engine_first_token_latency_ms_total", "decode_engine_first_token_requests")
    _set_avg("decode_engine_kv_first_token_latency_ms_avg", "decode_engine_kv_first_token_latency_ms_total", "decode_engine_kv_first_token_requests")
    _set_avg("mm_hidden_cache_vision_compute_ms_avg", "mm_hidden_cache_vision_compute_ms_total", "mm_hidden_cache_misses")
    _set_avg("mm_hidden_cache_load_ms_avg", "mm_hidden_cache_load_ms_total", "mm_hidden_cache_hits")
    lookups = float(delta.get("mm_hidden_cache_lookups", 0) or 0)
    hits = float(delta.get("mm_hidden_cache_hits", 0) or 0)
    delta["mm_hidden_cache_hit_rate"] = hits / lookups if lookups > 0 else 0.0
    delta["mm_hidden_cache_miss_rate"] = (
        float(delta.get("mm_hidden_cache_misses", 0) or 0) / lookups
        if lookups > 0
        else 0.0
    )
    result: Dict[str, Any] = {"metrics": delta}
    for cache_name in (
        "direct_feature_handle_cache",
        "direct_feature_lease_prefetch",
        "rendered_prefill_cache",
    ):
        after_cache = after.get(cache_name)
        if not isinstance(after_cache, dict):
            after_cache = after_metrics.get(cache_name)
        if not isinstance(after_cache, dict):
            continue
        before_cache = before.get(cache_name)
        if not isinstance(before_cache, dict):
            before_cache = before_metrics.get(cache_name)
        result[cache_name] = _cache_snapshot_delta(after_cache, before_cache)
    return result

def summarize_online_direct_metrics(
    metrics_payload: Dict[str, Any],
    *,
    direct_buffer_stats: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    summary = summarize_feature_handle_metrics(metrics_payload)
    # These proxy-side structures are snapshots rather than monotonically
    # increasing connector counters, so ``summarize_feature_handle_metrics``
    # intentionally does not flatten them.  Preserve them in the benchmark
    # summary: otherwise a persistent Prefill cache can look like an ordinary
    # cold direct-buffer run even when the request trace reports cache hits.
    outer_payload = dict(metrics_payload or {})
    source_metrics = dict(outer_payload.get("metrics") or outer_payload)
    for name in (
        "direct_feature_handle_cache",
        "direct_feature_lease_prefetch",
        "direct_feature_singleflight",
        "direct_feature_release",
        "rendered_prefill_cache",
    ):
        # ``/metrics`` keeps control-plane counters under ``metrics`` while
        # proxy-local cache/release snapshots live at the top level.  Accept
        # either shape so the compact benchmark summary cannot silently omit
        # the very cache evidence it is meant to expose.
        value = outer_payload.get(name)
        if not isinstance(value, dict):
            value = source_metrics.get(name)
        if isinstance(value, dict):
            summary[name] = dict(value)
    direct_stats = dict(direct_buffer_stats or {})
    summary["direct_buffer_allocations"] = int(direct_stats.get("allocations", 0) or 0)
    summary["direct_buffer_bytes"] = int(direct_stats.get("bytes", 0) or 0)
    summary["direct_buffer_managed_buffers"] = int(direct_stats.get("managed_buffers", 0) or 0)
    return summary


def _summarize_deterministic_response_consistency(
    responses: Sequence[Dict[str, Any]],
    *,
    required: bool,
) -> Dict[str, Any]:
    """Record exact-output consistency for repeated deterministic requests.

    Transport corruption can still produce a syntactically valid HTTP response
    and satisfy all data-plane counters.  For a temperature-zero benchmark
    that repeats one model-visible request, the output must be identical.  We
    retain only SHA-256 digests in the compact verdict so the artifact is
    auditable without duplicating potentially large model output.
    """

    groups: Dict[str, Dict[str, Any]] = {}
    for response in responses:
        sample_id = str(response.get("sample_id") or "").strip()
        # Non-dataset runs repeatedly materialize the same request, while a
        # dataset replay needs one consistency group per source sample.
        group_id = sample_id or "__single_request__"
        text = response.get("response_text")
        if not isinstance(text, str):
            text = "" if text is None else str(text)
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        group = groups.setdefault(
            group_id,
            {
                "response_count": 0,
                "hash_counts": {},
            },
        )
        group["response_count"] += 1
        hash_counts = group["hash_counts"]
        hash_counts[digest] = int(hash_counts.get(digest, 0) or 0) + 1

    comparable_group_ids = [
        group_id
        for group_id, group in groups.items()
        if int(group["response_count"]) >= 2
    ]
    inconsistent_group_ids = [
        group_id
        for group_id in comparable_group_ids
        if len(dict(groups[group_id]["hash_counts"])) != 1
    ]
    applicable = bool(comparable_group_ids)
    return {
        "schema_version": 1,
        "mode": "exact_response_sha256",
        "required": bool(required),
        "applicable": applicable,
        "pass": (
            (not required)
            or (applicable and not inconsistent_group_ids)
        ),
        "measured_response_count": len(responses),
        "comparable_group_count": len(comparable_group_ids),
        "inconsistent_group_ids": sorted(inconsistent_group_ids),
        "groups": groups,
    }


_MISSING_REFERENCE_VALUE = object()


def _nested_reference_value(
    payload: Mapping[str, Any],
    path: Sequence[str],
) -> Any:
    value: Any = payload
    for key in path:
        if not isinstance(value, Mapping) or key not in value:
            return _MISSING_REFERENCE_VALUE
        value = value[key]
    return value


def _reference_response_config_mismatches(
    reference_config: Mapping[str, Any],
    benchmark_config: Mapping[str, Any],
) -> List[str]:
    """Return fail-closed incompatibilities for a baseline response oracle.

    A concurrent vLLM batch can legitimately choose among a small set of
    numerically equivalent-looking greedy continuations.  The oracle therefore
    compares sets of completions rather than forcing a scheduling-dependent
    single hash, but only when both runs used the same request and serving
    shape.  Do not weaken these checks: a completion set from a different
    prompt, cache policy, or batching limit is not a correctness reference.
    """

    mismatches: List[str] = []
    exact_paths = (
        ("request_fingerprint",),
        ("request",),
        ("request_variation",),
        ("vllm_prefix_cache",),
        ("load",),
    )
    serving_paths = (
        ("serving", "max_model_len"),
        ("serving", "gpu_memory_utilization"),
        ("serving", "max_num_batched_tokens"),
        ("serving", "max_num_seqs"),
        ("serving", "generation_config"),
        ("serving", "tensor_parallel_size"),
    )
    for path in (*exact_paths, *serving_paths):
        reference_value = _nested_reference_value(reference_config, path)
        candidate_value = _nested_reference_value(benchmark_config, path)
        label = ".".join(path)
        if (
            reference_value is _MISSING_REFERENCE_VALUE
            or candidate_value is _MISSING_REFERENCE_VALUE
        ):
            mismatches.append(f"missing {label}")
        elif reference_value != candidate_value:
            mismatches.append(f"mismatched {label}")
    return mismatches


def _response_hash_groups(
    responses: Sequence[Mapping[str, Any]],
    *,
    require_successful_text: bool,
) -> Dict[str, Dict[str, Any]]:
    """Build hash-only response groups without leaking completion text."""

    groups: Dict[str, Dict[str, Any]] = {}
    for index, response in enumerate(responses):
        if not isinstance(response, Mapping):
            raise ValueError(f"response row {index} is not an object")
        status_code = int(response.get("status_code", 0) or 0)
        text = response.get("response_text")
        if require_successful_text:
            if status_code < 200 or status_code >= 300 or response.get("error"):
                raise ValueError(f"reference response {index} is not successful")
            if not isinstance(text, str) or not text:
                raise ValueError(f"reference response {index} has no completion text")
        if not isinstance(text, str):
            text = "" if text is None else str(text)
        sample_id = str(response.get("sample_id") or "").strip()
        group_id = sample_id or "__single_request__"
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        group = groups.setdefault(
            group_id,
            {"response_count": 0, "hash_counts": {}},
        )
        group["response_count"] += 1
        hash_counts = group["hash_counts"]
        hash_counts[digest] = int(hash_counts.get(digest, 0) or 0) + 1
    return groups


def _load_reference_response_summary(
    reference_summary_path: str | Path,
    *,
    benchmark_config: Mapping[str, Any],
) -> Dict[str, Any]:
    """Load a compatible single-vLLM response oracle before launching EPD."""

    path = Path(reference_summary_path).expanduser()
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read reference response summary {path}: {exc}") from exc
    try:
        reference_summary = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid reference response summary {path}: {exc}") from exc
    if not isinstance(reference_summary, Mapping):
        raise ValueError("reference response summary must be a JSON object")
    if reference_summary.get("mode") != "single_vllm_baseline":
        raise ValueError(
            "reference response summary must come from run_vllm_single_baseline_e2e"
        )
    reference_config = reference_summary.get("benchmark_config")
    if not isinstance(reference_config, Mapping):
        raise ValueError("reference response summary has no benchmark_config")
    mismatches = _reference_response_config_mismatches(
        reference_config,
        benchmark_config,
    )
    if mismatches:
        raise ValueError(
            "reference response summary is incompatible with this benchmark: "
            + "; ".join(mismatches)
        )

    reference_responses: List[Mapping[str, Any]] = []
    for phase in ("warmup_responses", "responses"):
        rows = reference_summary.get(phase)
        if rows is None:
            continue
        if not isinstance(rows, list):
            raise ValueError(f"reference response summary field {phase} is not a list")
        reference_responses.extend(rows)
    if not reference_responses:
        raise ValueError("reference response summary has no warmup or measured responses")
    # Validate the source now, before expensive EPD services are launched.
    _response_hash_groups(reference_responses, require_successful_text=True)
    return {
        "summary_basename": path.name,
        "summary_sha256": hashlib.sha256(raw).hexdigest(),
        "request_fingerprint": str(reference_config.get("request_fingerprint") or ""),
        "responses": reference_responses,
    }


def _summarize_reference_response_equivalence(
    responses: Sequence[Mapping[str, Any]],
    *,
    reference: Mapping[str, Any],
    required: bool,
) -> Dict[str, Any]:
    """Check candidate response hashes are covered by a compatible baseline.

    This intentionally compares set inclusion, not response frequency.  Under
    concurrent dynamic batching, valid scheduling can change how often an
    otherwise known greedy continuation appears; a novel hash is the signal
    that remains actionable for transport/data-lifetime corruption.
    """

    candidate_groups = _response_hash_groups(
        responses,
        require_successful_text=False,
    )
    reference_responses = reference.get("responses")
    if not isinstance(reference_responses, Sequence) or isinstance(
        reference_responses, (str, bytes)
    ):
        raise ValueError("reference response oracle has no response rows")
    reference_groups = _response_hash_groups(
        reference_responses,
        require_successful_text=True,
    )
    group_summaries: Dict[str, Dict[str, Any]] = {}
    missing_reference_group_ids: List[str] = []
    unexpected_candidate_hashes_by_group: Dict[str, List[str]] = {}
    for group_id in sorted(candidate_groups):
        candidate_group = candidate_groups[group_id]
        reference_group = reference_groups.get(group_id)
        candidate_hashes = sorted(dict(candidate_group["hash_counts"]))
        reference_hashes = (
            sorted(dict(reference_group["hash_counts"])) if reference_group else []
        )
        unexpected = sorted(set(candidate_hashes).difference(reference_hashes))
        if reference_group is None:
            missing_reference_group_ids.append(group_id)
        if unexpected:
            unexpected_candidate_hashes_by_group[group_id] = unexpected
        group_summaries[group_id] = {
            "candidate_response_count": int(candidate_group["response_count"]),
            "candidate_hashes": candidate_hashes,
            "reference_response_count": (
                0 if reference_group is None else int(reference_group["response_count"])
            ),
            "reference_hashes": reference_hashes,
            "unexpected_candidate_hashes": unexpected,
        }

    applicable = bool(candidate_groups) and bool(reference_groups)
    passes = (
        applicable
        and not missing_reference_group_ids
        and not unexpected_candidate_hashes_by_group
    )
    return {
        "schema_version": 1,
        "mode": "reference_response_sha256_set_inclusion",
        "required": bool(required),
        "applicable": applicable,
        "pass": (not required) or passes,
        "candidate_response_count": len(responses),
        "reference_response_count": len(reference_responses),
        "reference_summary_basename": str(reference.get("summary_basename") or ""),
        "reference_summary_sha256": str(reference.get("summary_sha256") or ""),
        "reference_request_fingerprint": str(
            reference.get("request_fingerprint") or ""
        ),
        "missing_reference_group_ids": missing_reference_group_ids,
        "unexpected_candidate_hashes_by_group": unexpected_candidate_hashes_by_group,
        "groups": group_summaries,
    }


def validate_online_direct_summary(summary: Dict[str, Any]) -> None:
    failures: List[str] = []
    response = dict(summary.get("response") or {})
    headers = dict(response.get("headers") or {})
    metrics = dict(summary.get("online_direct_metric_summary") or {})

    if int(response.get("status_code", 0) or 0) != 200:
        failures.append(f"HTTP status is not 200: {response.get('status_code')}")
    routing_path = headers.get("x-epd-routing-path") or response.get("routing_path")
    text_only = bool(summary.get("text_only", False))
    expected_path = "PD" if text_only else "EPD"
    if routing_path != expected_path:
        failures.append(f"request did not route through {expected_path}: {routing_path}")
    if int(response.get("response_content_len", 0) or 0) <= 0:
        failures.append("response content is empty")
    responses = list(summary.get("responses") or [])
    decode_dispatch_mode = str(
        dict(
            dict(summary.get("benchmark_config") or {}).get("epd") or {}
        ).get("decode_dispatch_mode")
        or "legacy"
    )
    if responses:
        expected_requests = max(1, int(summary.get("repeat_requests", len(responses)) or len(responses)))
        if len(responses) != expected_requests:
            failures.append(
                f"measured response count mismatch: got={len(responses)} expected={expected_requests}"
            )
        for index, item in enumerate(responses):
            if int(item.get("status_code", 0) or 0) != 200 or item.get("error"):
                failures.append(f"measured request {index} failed")
            if int(item.get("response_content_len", 0) or 0) <= 0:
                failures.append(f"measured request {index} returned empty content")
            item_headers = dict(item.get("headers") or {})
            if (item.get("routing_path") or item_headers.get("x-epd-routing-path")) != expected_path:
                failures.append(f"measured request {index} did not route through {expected_path}")
            timings = dict(item.get("epd_timing_ms") or {})
            if decode_dispatch_mode == "shadow" and float(
                timings.get("decode_token_envelope_validated", 0.0) or 0.0
            ) != 1.0:
                failures.append(
                    f"measured request {index} did not validate Decode token envelope"
                )
            if decode_dispatch_mode == "token_ids":
                if float(
                    timings.get("decode_token_fast_path", 0.0) or 0.0
                ) != 1.0:
                    failures.append(
                        f"measured request {index} did not use Decode token fast path"
                    )
                if float(
                    timings.get("decode_token_media_stripped", 0.0) or 0.0
                ) != 1.0:
                    failures.append(
                        f"measured request {index} retained media on Decode token path"
                    )
                if float(
                    timings.get("decode_token_legacy_fallback", 0.0) or 0.0
                ) > 0.0:
                    failures.append(
                        f"measured request {index} used forbidden Decode legacy fallback"
                    )
    # A cold request is still a serving request. Use the all-phase verdict
    # when present so a corruption hidden by a later warm cache cannot be
    # relabelled as a valid throughput result. Older artifacts only carry the
    # measured-phase field and remain readable through the fallback.
    response_consistency = summary.get(
        "all_response_consistency", summary.get("response_consistency")
    )
    if isinstance(response_consistency, Mapping) and bool(
        response_consistency.get("required", False)
    ):
        if not bool(response_consistency.get("applicable", False)):
            failures.append(
                "deterministic response-consistency gate was required but no request repeated"
            )
        elif not bool(response_consistency.get("pass", False)):
            inconsistent = list(
                response_consistency.get("inconsistent_group_ids") or []
            )
            failures.append(
                "deterministic response-consistency failure"
                + (f": groups={inconsistent}" if inconsistent else "")
            )
    reference_equivalence = summary.get("reference_response_equivalence")
    if isinstance(reference_equivalence, Mapping) and bool(
        reference_equivalence.get("required", False)
    ):
        if not bool(reference_equivalence.get("applicable", False)):
            failures.append(
                "reference response-equivalence gate was required but no comparable responses exist"
            )
        elif not bool(reference_equivalence.get("pass", False)):
            unexpected = dict(
                reference_equivalence.get("unexpected_candidate_hashes_by_group")
                or {}
            )
            missing = list(
                reference_equivalence.get("missing_reference_group_ids") or []
            )
            details: List[str] = []
            if missing:
                details.append(f"missing_groups={missing}")
            if unexpected:
                details.append(f"unexpected_hash_groups={sorted(unexpected)}")
            failures.append(
                "reference response-equivalence failure"
                + (f": {', '.join(details)}" if details else "")
            )
    measurement_purity = summary.get("measurement_purity")
    if isinstance(measurement_purity, Mapping) and bool(
        measurement_purity.get("required", False)
    ):
        if not bool(measurement_purity.get("pass", False)):
            failures.append(
                "measurement purity gate failed"
                + (
                    ": "
                    + "; ".join(
                        str(item)
                        for item in list(
                            measurement_purity.get("quiescence_failures") or []
                        )
                    )
                    if measurement_purity.get("quiescence_failures")
                    else ""
                )
            )
    if text_only:
        if int(metrics.get("requests_text", 0) or 0) < 1:
            failures.append("requests_text < 1")
    else:
        if int(metrics.get("requests_multimodal", 0) or 0) < 1:
            failures.append("requests_multimodal < 1")
        instrumented_precomputed = int(metrics.get("precomputed_hits", 0) or 0)
        direct_delivery = dict(summary.get("direct_feature_delivery") or {})
        delivered = int(direct_delivery.get("direct_feature_delivery_requests", 0) or 0)
        required_delivery = len(responses)
        # With cache instrumentation enabled, retain the original metric
        # requirement.  With it explicitly disabled, all measured requests
        # must instead carry direct allocation/publish or direct-cache timing
        # evidence; accepting a single weak counter would make the cold-cache
        # control vulnerable to a false positive.
        if instrumented_precomputed < 1 and (
            required_delivery <= 0 or delivered < required_delivery
        ):
            failures.append(
                "Prefill FeatureHandle consumption lacks both instrumented "
                "precomputed-hit and per-request direct-delivery evidence"
            )
        if float(metrics.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0) != 0.0:
            failures.append("Prefill vision encoder ran instead of using direct FeatureHandle")
        if int(metrics.get("hidden_cache_errors", 0) or 0) != 0:
            failures.append("hidden cache errors observed")
        if int(metrics.get("hidden_cache_full_miss_batches", 0) or 0) != 0:
            failures.append("hidden cache full misses observed")
    if int(metrics.get("fallback_batches", 0) or 0) != 0 or int(metrics.get("fallback_bytes", 0) or 0) != 0:
        failures.append("Mooncake transfer fallback observed")
    if int(metrics.get("layered_receive_failures", 0) or 0) != 0:
        failures.append("layered receive failures observed")
    if int(metrics.get("layered_transfer_failed_batches", 0) or 0) != 0:
        failures.append("layered transfer failed batches observed")
    backend_counts = dict(metrics.get("backend_counts") or {})
    if int(backend_counts.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("peer_buffer_direct backend did not run")
    unexpected = {
        str(k): int(v or 0)
        for k, v in backend_counts.items()
        if str(k) != "peer_buffer_direct" and int(v or 0) != 0
    }
    if unexpected:
        failures.append(f"unexpected transfer backends: {unexpected}")
    direct_stats = dict(summary.get("direct_buffer_stats_after_release") or {})
    if not text_only:
        stats_by_worker = direct_stats.get("workers")
        if not isinstance(stats_by_worker, dict) or not stats_by_worker or any(
            not isinstance(stats, dict) or not stats
            for stats in stats_by_worker.values()
        ):
            failures.append("Prefill direct feature-buffer stats unavailable")
        else:
            persistent_cache = bool(direct_stats.get("persistent_cache", False))
            if int(direct_stats.get("ref_count", 0) or 0) != 0:
                failures.append("Prefill direct feature buffers still have live references")
            if (not persistent_cache) and int(
                metrics.get("direct_buffer_allocations", 0) or 0
            ) != 0:
                failures.append("Prefill direct feature buffers were not released")
    if failures:
        raise AssertionError("; ".join(failures))


def run(args: argparse.Namespace) -> Dict[str, Any]:
    # Do this before touching the workdir or launching any child process. A
    # requested native protocol is a correctness contract, not a best-effort
    # optimization, so a binary without NVLink support must leave no artifact
    # that could be mistaken for a real run.
    _validate_requested_transport_capabilities(args)
    # ``_launch`` deliberately switches child processes to the package root.
    # Resolve here so generated scripts/logs remain addressable when callers
    # pass a repo-relative artifact directory.
    workdir = Path(args.workdir).expanduser().resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    for name in (
        "online_direct_e2e_summary.json",
        "online_direct_request.json",
        "dataset_replay_selection.json",
        "dataset_replay_skipped.json",
    ):
        path = workdir / name
        if path.exists():
            path.unlink()

    text_only = bool(getattr(args, "text_only", False))
    vllm_prefix_cache_mode = str(
        getattr(args, "vllm_prefix_cache_mode", VLLM_PREFIX_CACHE_ISOLATE)
        or VLLM_PREFIX_CACHE_ISOLATE
    )
    dataset_root = str(getattr(args, "dataset_root", "") or "").strip()
    max_dataset_requests = max(0, int(getattr(args, "max_dataset_requests", 0) or 0))
    dataset_families = list(getattr(args, "dataset_families", None) or [])
    if dataset_root and max_dataset_requests <= 0:
        raise ValueError("--dataset-root requires --max-dataset-requests > 0")
    if max_dataset_requests > 0 and not dataset_root:
        raise ValueError("--max-dataset-requests requires --dataset-root")
    if dataset_root and text_only:
        raise ValueError(
            "strict online direct dataset replay is multimodal; --text-only is unsupported"
        )
    dataset_max_input_len = int(getattr(args, "dataset_max_input_len", 0) or 0)
    if dataset_max_input_len <= 0:
        dataset_max_input_len = int(args.max_model_len)
    dataset_request_max_tokens = int(
        getattr(args, "dataset_request_max_tokens", 0) or 0
    )
    if dataset_request_max_tokens <= 0:
        dataset_request_max_tokens = int(args.max_tokens)
    dataset_skip_oversized = bool(
        getattr(args, "dataset_skip_oversized", True)
    )
    dataset_image_max_pixels = max(
        0, int(getattr(args, "dataset_image_max_pixels", 0) or 0)
    )
    dataset_agent_pd_labels = bool(
        getattr(args, "dataset_agent_pd_labels", False)
    )
    dataset_workflow_id_mode = str(
        getattr(args, "dataset_workflow_id_mode", "unique") or "unique"
    ).strip().lower()
    dataset_entries: List[Dict[str, Any]] = []
    dataset_skipped: List[Dict[str, Any]] = []
    dataset_context: Dict[str, Any] | None = None
    if dataset_root:
        dataset_entries, dataset_skipped = _load_dataset_requests(
            dataset_root=dataset_root,
            chat_split=str(getattr(args, "dataset_chat_split", "dev-small")),
            max_requests=max_dataset_requests,
            families=dataset_families or None,
            model=str(args.model),
            max_input_len=dataset_max_input_len,
            request_max_tokens=dataset_request_max_tokens,
            skip_oversized=dataset_skip_oversized,
            image_max_pixels=dataset_image_max_pixels,
            agent_pd_labels=dataset_agent_pd_labels,
        )
        if len(dataset_entries) < max_dataset_requests:
            raise RuntimeError(
                "insufficient admissible dataset requests: "
                f"selected={len(dataset_entries)} requested={max_dataset_requests} "
                f"skipped={len(dataset_skipped)}"
            )
        dataset_context = _dataset_replay_context(
            dataset_entries,
            dataset_root=dataset_root,
            chat_split=str(getattr(args, "dataset_chat_split", "dev-small")),
            families=dataset_families or None,
            max_input_len=dataset_max_input_len,
            request_max_tokens=dataset_request_max_tokens,
            skip_oversized=dataset_skip_oversized,
            image_max_pixels=dataset_image_max_pixels,
            agent_pd_labels=dataset_agent_pd_labels,
            workflow_id_mode=dataset_workflow_id_mode,
        )
        (workdir / "dataset_replay_selection.json").write_text(
            json.dumps(dataset_context, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (workdir / "dataset_replay_skipped.json").write_text(
            json.dumps(dataset_skipped, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    repeated_request_count = max(1, int(args.repeat_requests))
    request_temperature = float(args.temperature)
    if dataset_entries:
        request_temperature = max(
            float(dict(entry.get("request") or {}).get("temperature", 0.0) or 0.0)
            for entry in dataset_entries
        )
    response_consistency_request_shape_eligible = (
        repeated_request_count >= 2
        and len(dataset_entries) <= 1
        and request_temperature == 0.0
        and str(args.request_variation) == REQUEST_VARIATION_NONE
        and max(1, int(args.concurrency)) == 1
    )
    requested_response_consistency = getattr(
        args, "require_deterministic_response_consistency", None
    )
    # The warmup scheduler is resolved after workers are created. Until then,
    # retain only an explicit user requirement; the automatic exact-hash gate
    # is enabled later only when *both* warmup and measured traffic are
    # sequential. Concurrent vLLM can select more than one valid greedy
    # completion, which is checked against the baseline oracle instead.
    auto_response_consistency_required = False
    deterministic_response_consistency_required = bool(
        requested_response_consistency
    ) if requested_response_consistency is not None else False
    reference_response_summary_path = str(
        getattr(args, "reference_response_summary", "") or ""
    ).strip()
    durable_workflow_registry = bool(
        getattr(args, "durable_workflow_registry", False)
    )
    # A proxy-side FeatureHandle cache retains the direct-buffer lease.  Once
    # the runner releases that lease immediately after Prefill, a later proxy
    # cache hit would point at a released buffer and is therefore deliberately
    # disabled by the proxy.  Do not keep populating that unreachable cache:
    # Prefill's persistent direct-buffer cache remains the safe hot path in
    # release-after-Prefill mode.
    release_direct_feature_buffers_after_prefill = bool(
        getattr(args, "release_direct_feature_buffers_after_prefill", True)
    )
    direct_proxy_handle_cache_requested = bool(args.direct_proxy_handle_cache)
    direct_proxy_handle_cache_effective = (
        direct_proxy_handle_cache_requested
        and not release_direct_feature_buffers_after_prefill
        and not text_only
    )
    request_image_urls = (
        [] if text_only or dataset_entries else _request_image_urls(args)
    )
    encoder_port = _pick_free_port(int(args.encoder_port), str(args.local_hostname)) if not text_only else 0
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=str(args.local_hostname),
        mm_prefetch_mode="feature_handle" if not text_only else "asset_bytes",
        prefill_supports_feature_handles=not text_only,
        enable_prefill_direct_feature_buffer_routes=not text_only,
        enable_direct_feature_handle_cache=direct_proxy_handle_cache_effective,
        direct_feature_handle_cache_max_entries=int(args.direct_proxy_handle_cache_max_entries),
        direct_feature_handle_cache_ttl_s=float(args.direct_proxy_handle_cache_ttl_s),
        direct_feature_lease_prefetch=max(1, min(1024, int(args.direct_feature_lease_prefetch))),
        direct_feature_lease_prefetch_max_entries=max(0, int(args.direct_feature_lease_prefetch_max_entries)),
        direct_feature_lease_prefetch_ttl_s=max(0.0, float(args.direct_feature_lease_prefetch_ttl_s)),
        direct_feature_adaptive_lease_prefetch=bool(
            args.enable_direct_feature_adaptive_lease_prefetch
        ),
        direct_feature_adaptive_lease_prefetch_max=max(
            1, min(1024, int(args.direct_feature_adaptive_lease_prefetch_max))
        ),
        enable_rendered_prefill_cache=bool(args.enable_rendered_prefill_cache),
        rendered_prefill_cache_max_entries=int(args.rendered_prefill_cache_max_entries),
        rendered_prefill_cache_max_bytes=int(args.rendered_prefill_cache_max_bytes),
        rendered_prefill_cache_ttl_s=float(args.rendered_prefill_cache_ttl_s),
        direct_feature_target_mode=str(args.direct_target_mode),
        direct_feature_register_memory=bool(args.direct_register_memory),
        direct_feature_persistent_cache=bool(args.direct_persistent_cache),
        direct_feature_cache_max_entries=int(args.direct_cache_max_entries),
        direct_feature_cache_max_bytes=int(args.direct_cache_max_bytes),
        # Integrity validation is intentionally an explicit diagnostic mode:
        # it computes full descriptor checksums on both sides of E->P and is
        # therefore unsuitable for the normal TTFT hot path.  Wiring the same
        # switch into the Encoder descriptor and Prefill resolver prevents a
        # false sense of validation where only one side checks an absent hash.
        feature_handle_require_checksum=bool(
            getattr(args, "feature_handle_checksum", False)
        ),
        vllm_mm_hidden_cache=getattr(args, "vllm_mm_hidden_cache", None),
        release_direct_feature_buffers_after_prefill=release_direct_feature_buffers_after_prefill,
        direct_feature_release_batch_max_jobs=max(
            1, min(1024, int(args.direct_feature_release_batch_max_jobs))
        ),
        prefill_dispatch_mode=str(args.prefill_dispatch_mode),
        decode_dispatch_mode=str(
            getattr(args, "decode_dispatch_mode", "legacy")
        ),
        allow_decode_token_fallback=bool(
            getattr(args, "allow_decode_token_fallback", False)
        ),
        prefill_http_keepalive=bool(args.prefill_http_keepalive),
        allow_unverified_openai_prompt_only=bool(
            getattr(args, "allow_unverified_openai_prompt_only", False)
        ),
        encoder_service_url=(f"http://{args.local_hostname}:{encoder_port}" if not text_only else None),
        strict_no_fallback=True,
        protocol=str(args.mooncake_protocol),
        tcp_write_completion_ack=bool(
            getattr(args, "tcp_write_completion_ack", True)
        ),
        tcp_connection_pool=bool(
            getattr(args, "tcp_connection_pool", False)
        ),
        direct_engine_protocol=(
            str(args.direct_engine_protocol)
            if getattr(args, "direct_engine_protocol", None)
            else None
        ),
        enable_workflow_registry_wal=durable_workflow_registry,
        workflow_registry_wal_path=(
            str(workdir / "proxy_workflow_registry.jsonl")
            if durable_workflow_registry
            else None
        ),
        connector_metrics_dir=str(workdir / "connector_metrics"),
        max_group_bytes=int(args.max_group_bytes),
        layers_per_group=int(args.layers_per_group),
        max_transfer_descriptors=int(args.max_transfer_descriptors),
        max_transfer_bytes=int(args.max_transfer_bytes),
        transfer_workers=int(args.transfer_workers),
        scheduler_policy=str(args.scheduler_policy),
        prefill_decode_affinity=_worker_affinity_tuples(
            getattr(args, "prefill_decode_affinity", None)
        ),
        owner_shards=max(1, int(args.owner_shards)),
        kv_directory_rpc_url=args.kv_directory_rpc_url,
        prefill_gpu=int(args.prefill_gpu),
        prefill_gpus=_int_tuple(args.prefill_gpus),
        prefill_ports=_int_tuple(args.prefill_ports),
        decode_gpu=int(args.decode_gpu),
        decode_gpus=_int_tuple(args.decode_gpus),
        decode_ports=_int_tuple(args.decode_ports),
        gpu_memory_utilization=float(args.gpu_memory_utilization),
        max_model_len=int(args.max_model_len),
        max_num_batched_tokens=_optional_positive_int(args.max_num_batched_tokens),
        max_num_seqs=_optional_positive_int(args.max_num_seqs),
        generation_config=(str(args.generation_config).strip() or None),
    )
    files = generate_configs(str(workdir), cfg)
    mooncake_json = Path(files["mooncake_json"])
    if not text_only:
        encoder_script = _write_encoder_script(
            workdir=workdir,
            cfg=cfg,
            encoder_port=encoder_port,
            args=args,
            mooncake_json=mooncake_json,
        )
        files["encoder"] = str(encoder_script)

    request_path = workdir / "online_direct_request.json"
    if dataset_entries:
        # Preserve the legacy request file as a human-inspectable reference;
        # actual measurement requests are materialized from the immutable
        # dataset selection above and may cycle through multiple samples.
        request_path.write_text(
            json.dumps(dataset_entries[0]["request"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        _write_online_request(args, request_path)
    prefill_scripts = [str(item) for item in list(files.get("prefill_scripts") or [files["prefill"]])]
    prefill_ports = _ports_from_scripts(prefill_scripts)
    if not prefill_ports:
        raise RuntimeError("no prefill scripts generated")
    prefill_port = prefill_ports[0]
    decode_scripts = [str(item) for item in list(files.get("decode_scripts") or [files["decode"]])]
    decode_ports = _ports_from_scripts(decode_scripts)
    if not decode_ports:
        raise RuntimeError("no decode scripts generated")
    decode_port = decode_ports[0]
    proxy_port = _extract_port(Path(files["proxy"]))

    logs = {
        "metadata": workdir / "metadata.log",
        "master": workdir / "master.log",
        "prefill": workdir / "prefill.log",
        "decode": workdir / "decode.log",
        "proxy": workdir / "proxy.log",
    }
    if not text_only:
        logs["encoder"] = workdir / "encoder.log"
    for idx in range(len(prefill_scripts)):
        logs[f"prefill_{idx}"] = workdir / ("prefill.log" if idx == 0 else f"prefill_{idx}.log")
    for idx in range(len(decode_scripts)):
        logs[f"decode_{idx}"] = workdir / ("decode.log" if idx == 0 else f"decode_{idx}.log")
    procs: List[subprocess.Popen] = []
    if dataset_context is not None:
        benchmark_request_fingerprint = str(dataset_context["request_fingerprint"])
        benchmark_request_config: Dict[str, Any] = {
            "model": str(args.model),
            "workload": "dataset_chat_split",
            # Keep this explicit even though the dataset fields below identify
            # the source split.  The fail-closed paired-claim checker compares
            # the common request surface of baseline and EPD artifacts.
            "prompt_source": "dataset_chat_split",
            "dataset_chat_split": dataset_context["chat_split"],
            "dataset_split_sha256": dataset_context["split_sha256"],
            "dataset_families": dataset_context["families_requested"],
            "selected_samples": dataset_context["selected_samples"],
            "selected_count": dataset_context["selected_count"],
            "entry_cycle": dataset_context["entry_cycle"],
            "workflow_id_mode": dataset_context["workflow_id_mode"],
            "max_tokens": dataset_request_max_tokens,
            "temperature": 0.0,
            "image_max_pixels": dataset_image_max_pixels,
            "agent_pd_labels": dataset_agent_pd_labels,
        }
        dataset_summary: Dict[str, Any] | None = {
            **dataset_context,
            "requested_count": max_dataset_requests,
            "skipped_count": len(dataset_skipped),
            "selection_artifact": str(workdir / "dataset_replay_selection.json"),
            "skipped_artifact": str(workdir / "dataset_replay_skipped.json"),
        }
    else:
        benchmark_request_fingerprint = _request_fingerprint(request_path)
        benchmark_request_config = {
            "model": str(args.model),
            "max_tokens": int(args.max_tokens),
            "temperature": float(args.temperature),
            "image_source": (
                "none"
                if text_only
                else (
                    "explicit_multi"
                    if list(getattr(args, "image_urls", None) or [])
                    else ("explicit" if getattr(args, "image_url", None) else str(args.demo_image))
                )
            ),
            "image_count": len(request_image_urls),
            "prompt_source": "file" if getattr(args, "prompt_file", None) else "inline",
        }
        dataset_summary = None
    summary: Dict[str, Any] = {
        "workdir": str(workdir),
        "request_path": str(request_path),
        "ports": {
            "encoder": encoder_port,
            "prefill": prefill_port,
            "prefill_ports": prefill_ports,
            "decode": decode_port,
            "decode_ports": decode_ports,
            "proxy": proxy_port,
        },
        "prefill_workers": [
            {
                "index": idx,
                "port": int(port),
                "script": str(script),
                "log": str(logs[f"prefill_{idx}"]),
            }
            for idx, (port, script) in enumerate(zip(prefill_ports, prefill_scripts))
        ],
        "decode_workers": [
            {
                "index": idx,
                "port": int(port),
                "script": str(script),
                "log": str(logs[f"decode_{idx}"]),
            }
            for idx, (port, script) in enumerate(zip(decode_ports, decode_scripts))
        ],
        "logs": {k: str(v) for k, v in logs.items()},
        "strict_no_fallback": True,
        "scheduler_policy": str(cfg.scheduler_policy),
        "mooncake_protocol": str(args.mooncake_protocol),
        "direct_engine_protocol": (
            None if text_only else _effective_direct_engine_protocol(args)
        ),
        "text_only": text_only,
        "dataset": dataset_summary,
        "benchmark_config": {
            "schema_version": 1,
            "request_fingerprint": benchmark_request_fingerprint,
            "request": benchmark_request_config,
            "request_variation": request_variation_spec(str(args.request_variation)),
            "vllm_prefix_cache": vllm_prefix_cache_spec(vllm_prefix_cache_mode),
            "validation": {
                "deterministic_response_consistency": {
                    "required": deterministic_response_consistency_required,
                    "auto_eligible": auto_response_consistency_required,
                    "request_shape_eligible": response_consistency_request_shape_eligible,
                    "mode": "exact_response_sha256",
                    "scope": "warmup_and_measure",
                },
                "reference_response_equivalence": {
                    "required": bool(reference_response_summary_path),
                    "mode": "reference_response_sha256_set_inclusion",
                    "scope": "warmup_and_measure",
                },
            },
            "load": {
                "repeat_requests": max(1, int(args.repeat_requests)),
                "concurrency": max(1, int(args.concurrency)),
                "requested_warmup_requests": max(0, int(args.warmup_requests)),
                "requested_warmup_concurrency": max(0, int(args.warmup_concurrency)),
                "warmup_cover_dataset_cycle": bool(
                    getattr(args, "warmup_cover_dataset_cycle", False)
                ),
                "between_repeat_sleep_s": max(0.0, float(args.between_repeat_sleep_s)),
            },
            "serving": {
                "max_model_len": int(args.max_model_len),
                "gpu_memory_utilization": float(args.gpu_memory_utilization),
                "max_num_batched_tokens": _optional_positive_int(args.max_num_batched_tokens),
                "max_num_seqs": _optional_positive_int(args.max_num_seqs),
                "generation_config": str(args.generation_config),
                "tensor_parallel_size": int(cfg.tensor_parallel_size),
            },
            "topology": {
                "encoder_device": None if text_only else str(args.encoder_device),
                "encoder_runtime": (
                    None if text_only else str(getattr(args, "encoder_runtime", "transformers"))
                ),
                "prefill_gpus": list(cfg.prefill_gpus or (cfg.prefill_gpu,)),
                "decode_gpus": list(cfg.decode_gpus or (cfg.decode_gpu,)),
                "total_gpus": (0 if text_only else 1) + len(cfg.prefill_gpus or (cfg.prefill_gpu,)) + len(cfg.decode_gpus or (cfg.decode_gpu,)),
            },
            "epd": {
                "protocol": str(args.mooncake_protocol),
                "tcp_write_completion_ack": bool(
                    getattr(args, "tcp_write_completion_ack", True)
                ),
                "tcp_connection_pool": bool(
                    getattr(args, "tcp_connection_pool", False)
                ),
                "layers_per_group": int(args.layers_per_group),
                "max_group_bytes": int(args.max_group_bytes),
                "max_transfer_descriptors": int(args.max_transfer_descriptors),
                "max_transfer_bytes": int(args.max_transfer_bytes),
                "transfer_workers": int(cfg.effective_transfer_workers),
                "scheduler_policy": str(cfg.scheduler_policy),
                "direct_persistent_cache": bool(args.direct_persistent_cache),
                "feature_handle_checksum": bool(
                    getattr(args, "feature_handle_checksum", False)
                ),
                "direct_proxy_handle_cache": direct_proxy_handle_cache_effective,
                "direct_proxy_handle_cache_requested": direct_proxy_handle_cache_requested,
                "direct_proxy_handle_cache_effective": direct_proxy_handle_cache_effective,
                "direct_feature_lease_prefetch": int(
                    cfg.direct_feature_lease_prefetch
                ),
                "direct_feature_lease_prefetch_max_entries": int(
                    cfg.direct_feature_lease_prefetch_max_entries
                ),
                "direct_feature_lease_prefetch_ttl_s": float(
                    cfg.direct_feature_lease_prefetch_ttl_s
                ),
                "direct_feature_adaptive_lease_prefetch": bool(
                    cfg.direct_feature_adaptive_lease_prefetch
                ),
                "direct_feature_adaptive_lease_prefetch_max": int(
                    cfg.direct_feature_adaptive_lease_prefetch_max
                ),
                "rendered_prefill_cache": bool(args.enable_rendered_prefill_cache),
                "rendered_prefill_cache_max_entries": int(
                    args.rendered_prefill_cache_max_entries
                ),
                "rendered_prefill_cache_max_bytes": int(
                    args.rendered_prefill_cache_max_bytes
                ),
                "rendered_prefill_cache_ttl_s": float(
                    args.rendered_prefill_cache_ttl_s
                ),
                # A one-call prompt-only probe changes the Prefill execution
                # semantics, so record it independently from cache controls.
                # This prevents a benchmark report from comparing it with the
                # verified render+generate path as though the two were equal.
                "prefill_dispatch_mode": str(args.prefill_dispatch_mode),
                "decode_dispatch_mode": str(
                    getattr(args, "decode_dispatch_mode", "legacy")
                ),
                "allow_decode_token_fallback": bool(
                    getattr(args, "allow_decode_token_fallback", False)
                ),
                "prefill_http_keepalive": bool(args.prefill_http_keepalive),
                "allow_unverified_openai_prompt_only": bool(
                    getattr(args, "allow_unverified_openai_prompt_only", False)
                ),
                "release_direct_feature_buffers_after_prefill": (
                    release_direct_feature_buffers_after_prefill
                ),
                "direct_feature_release_batch_max_jobs": int(
                    cfg.direct_feature_release_batch_max_jobs
                ),
                "vllm_mm_hidden_cache": cfg.vllm_mm_hidden_cache,
                "prefill_decode_affinity": [
                    [prefill, decode]
                    for prefill, decode in cfg.resolved_prefill_decode_affinity
                ],
            },
        },
    }
    reference_response_source: Dict[str, Any] | None = None
    try:
        if reference_response_summary_path:
            reference_response_source = _load_reference_response_summary(
                reference_response_summary_path,
                benchmark_config=summary["benchmark_config"],
            )
            summary["benchmark_config"]["validation"][
                "reference_response_equivalence"
            ].update(
                {
                    "reference_summary_basename": reference_response_source[
                        "summary_basename"
                    ],
                    "reference_summary_sha256": reference_response_source[
                        "summary_sha256"
                    ],
                }
            )
        named: Dict[str, subprocess.Popen] = {}
        startup_items: List[tuple[str, str, Path]] = [
            ("metadata", str(files["metadata"]), logs["metadata"]),
            ("master", str(files["master"]), logs["master"]),
        ]
        if not text_only:
            startup_items.append(("encoder", str(files["encoder"]), logs["encoder"]))
        startup_items.extend(
            (f"prefill_{idx}", script, logs[f"prefill_{idx}"])
            for idx, script in enumerate(prefill_scripts)
        )
        startup_items.extend(
            (f"decode_{idx}", script, logs[f"decode_{idx}"])
            for idx, script in enumerate(decode_scripts)
        )
        startup_items.append(("proxy", str(files["proxy"]), logs["proxy"]))

        for key, script, log_path in startup_items:
            proc = _launch(script, log_path)
            procs.append(proc)
            named[key] = proc
            time.sleep(2.0 if key in {"metadata", "master", "encoder"} else 4.0)
            _ensure_process_running(key, proc, log_path)

        timeout_s = max(60.0, float(args.timeout))
        startup_timeout_s = max(
            60.0,
            float(getattr(args, "startup_timeout", timeout_s)),
        )
        if not text_only:
            _wait_ready("encoder", f"http://{args.local_hostname}:{encoder_port}", proc=named["encoder"], log_path=logs["encoder"], timeout_s=startup_timeout_s, paths=("/health",))
        for idx, port in enumerate(prefill_ports):
            key = f"prefill_{idx}"
            _wait_ready(key, f"http://{args.local_hostname}:{port}", proc=named[key], log_path=logs[key], timeout_s=startup_timeout_s, paths=("/health",))
        for idx, port in enumerate(decode_ports):
            key = f"decode_{idx}"
            _wait_ready(key, f"http://{args.local_hostname}:{port}", proc=named[key], log_path=logs[key], timeout_s=startup_timeout_s, paths=("/health",))
        _wait_ready("proxy", f"http://{args.local_hostname}:{proxy_port}", proc=named["proxy"], log_path=logs["proxy"], timeout_s=min(180.0, startup_timeout_s), paths=("/health", "/healthcheck"))

        request_url = f"http://{args.local_hostname}:{proxy_port}/v1/chat/completions"
        warmup_responses: List[Dict[str, Any]] = []
        workflow_prefix = str(args.workflow_id or "online-direct-e2e")
        requested_warmups = max(0, int(args.warmup_requests))
        effective_warmups = _resolve_dataset_warmup_requests(
            requested_warmups=requested_warmups,
            dataset_entries=dataset_entries,
            cover_dataset_cycle=bool(
                getattr(args, "warmup_cover_dataset_cycle", False)
            ),
        )
        if len(decode_ports) > 1 and requested_warmups > 0:
            effective_warmups = max(
                requested_warmups,
                len(decode_ports) * max(0, int(args.min_warmup_per_decode)),
            )
        summary["requested_warmup_requests"] = requested_warmups
        summary["effective_warmup_requests"] = effective_warmups
        summary["min_warmup_per_decode"] = int(args.min_warmup_per_decode)
        warmup_concurrency = _resolve_warmup_concurrency(
            requested=int(getattr(args, "warmup_concurrency", 0) or 0),
            effective_warmups=int(effective_warmups),
            measured_concurrency=max(1, int(args.concurrency)),
            prefill_workers=len(prefill_ports),
            decode_workers=len(decode_ports),
        )
        summary["requested_warmup_concurrency"] = int(getattr(args, "warmup_concurrency", 0) or 0)
        summary["warmup_concurrency"] = warmup_concurrency
        summary["warmup_concurrency_mode"] = "auto" if int(getattr(args, "warmup_concurrency", 0) or 0) <= 0 else "explicit"
        auto_response_consistency_required = (
            response_consistency_request_shape_eligible
            and warmup_concurrency <= 1
        )
        if requested_response_consistency is None:
            deterministic_response_consistency_required = (
                auto_response_consistency_required
            )
        summary["benchmark_config"]["validation"][
            "deterministic_response_consistency"
        ].update(
            {
                "required": deterministic_response_consistency_required,
                "auto_eligible": auto_response_consistency_required,
                "warmup_concurrency": warmup_concurrency,
            }
        )
        if effective_warmups > 0 and warmup_concurrency > 1:
            with ThreadPoolExecutor(max_workers=min(warmup_concurrency, effective_warmups)) as pool:
                future_map = {
                    pool.submit(
                        _run_online_direct_request,
                        proxy_url=request_url,
                        request_path=request_path,
                        repeat_idx=-(warmup_idx + 1),
                        workflow_prefix=f"{workflow_prefix}-warmup",
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                        phase="warmup",
                        dataset_entries=dataset_entries or None,
                        dataset_workflow_id_mode=dataset_workflow_id_mode,
                    ): warmup_idx
                    for warmup_idx in range(effective_warmups)
                }
                for future in as_completed(future_map):
                    warmup_responses.append(future.result())
            warmup_responses.sort(key=lambda item: int(item.get("index", 0) or 0))
        else:
            for warmup_idx in range(effective_warmups):
                warmup_responses.append(
                    _run_online_direct_request(
                        proxy_url=request_url,
                        request_path=request_path,
                        repeat_idx=-(warmup_idx + 1),
                        workflow_prefix=f"{workflow_prefix}-warmup",
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                        phase="warmup",
                        dataset_entries=dataset_entries or None,
                        dataset_workflow_id_mode=dataset_workflow_id_mode,
                    )
                )
                if float(args.between_repeat_sleep_s) > 0:
                    time.sleep(max(0.0, float(args.between_repeat_sleep_s)))

        summary["dataset_warmup_coverage"] = _dataset_warmup_coverage(
            dataset_entries,
            warmup_responses,
            cover_dataset_cycle=bool(
                getattr(args, "warmup_cover_dataset_cycle", False)
            ),
        )

        metrics_before_measured: Dict[str, Any] = {}
        require_measurement_purity = bool(
            getattr(args, "require_measurement_purity", True)
        )
        measurement_purity: Dict[str, Any]
        if warmup_responses:
            proxy_url = f"http://{args.local_hostname}:{proxy_port}"
            sess = _session()
            try:
                metrics_before_measured = _wait_for_metrics_settle(
                    sess, f"{proxy_url}/metrics", timeout_s=20.0, poll_s=0.5
                )
                measurement_purity, metrics_before_measured = (
                    _wait_for_measurement_purity(
                        sess,
                        f"{proxy_url}/metrics",
                        log_paths=logs,
                        initial_metrics=metrics_before_measured,
                        timeout_s=float(
                            getattr(args, "measurement_purity_timeout_s", 20.0)
                        ),
                        poll_s=float(
                            getattr(args, "measurement_purity_poll_s", 0.5)
                        ),
                        stable_polls=int(
                            getattr(args, "measurement_purity_stable_polls", 2)
                        ),
                    )
                )
            finally:
                sess.close()
            summary["metrics_before_measured"] = metrics_before_measured
        else:
            measurement_purity = {
                "pass": False,
                "polls": 0,
                "stable_polls": 0,
                "required_stable_polls": max(
                    1,
                    int(getattr(args, "measurement_purity_stable_polls", 2)),
                ),
                "wait_ms": 0.0,
                "quiescence_failures": ["no successful warmup requests"],
                "compile_events_after_warmup": [],
            }
        measurement_purity["required"] = require_measurement_purity
        summary["measurement_purity"] = measurement_purity
        summary["benchmark_config"]["validation"]["measurement_purity"] = {
            "required": require_measurement_purity,
            "pass": bool(measurement_purity.get("pass", False)),
            "stable_polls": int(measurement_purity.get("stable_polls", 0) or 0),
            "required_stable_polls": int(
                measurement_purity.get("required_stable_polls", 0) or 0
            ),
        }
        if require_measurement_purity and not bool(
            measurement_purity.get("pass", False)
        ):
            failures = "; ".join(
                str(item)
                for item in list(
                    measurement_purity.get("quiescence_failures") or []
                )
            )
            raise AssertionError(
                "measurement purity gate failed before measured traffic"
                + (f": {failures}" if failures else "")
            )

        responses: List[Dict[str, Any]] = []
        benchmark_started = time.perf_counter()
        total_requests = max(1, int(args.repeat_requests))
        concurrency = max(1, int(args.concurrency))
        if concurrency == 1:
            for repeat_idx in range(total_requests):
                responses.append(
                    _run_online_direct_request(
                        proxy_url=request_url,
                        request_path=request_path,
                        repeat_idx=repeat_idx,
                        workflow_prefix=workflow_prefix,
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                        phase="measure",
                        dataset_entries=dataset_entries or None,
                        dataset_workflow_id_mode=dataset_workflow_id_mode,
                    )
                )
                if repeat_idx + 1 < total_requests:
                    time.sleep(max(0.0, float(args.between_repeat_sleep_s)))
        else:
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                future_map = {
                    pool.submit(
                        _run_online_direct_request,
                        proxy_url=request_url,
                        request_path=request_path,
                        repeat_idx=repeat_idx,
                        workflow_prefix=workflow_prefix,
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                        phase="measure",
                        dataset_entries=dataset_entries or None,
                        dataset_workflow_id_mode=dataset_workflow_id_mode,
                        raise_on_failure=False,
                    ): repeat_idx
                    for repeat_idx in range(total_requests)
                }
                for future in as_completed(future_map):
                    responses.append(future.result())
            responses.sort(key=lambda item: int(item.get("index", 0) or 0))
        benchmark_elapsed_s = max(1e-6, time.perf_counter() - benchmark_started)
        failed_responses = [
            item
            for item in responses
            if int(item.get("status_code") or 0) >= 400 or item.get("error")
        ]
        if failed_responses:
            # Preserve the full measured terminal set in the failure artifact.
            # These fields are intentionally written before any cleanup so a
            # real multi-worker failure still carries dispatch and response
            # evidence for every request that was allowed to complete.
            summary["warmup_responses"] = warmup_responses
            summary["responses"] = responses
            summary["failed_responses"] = failed_responses
            summary["benchmark_elapsed_s"] = benchmark_elapsed_s
            proxy_url = f"http://{args.local_hostname}:{proxy_port}"
            sess = _session()
            try:
                summary["metrics_at_measured_failure"] = sess.get(
                    f"{proxy_url}/metrics", timeout=10
                ).json()
            except Exception as metrics_exc:
                summary["metrics_at_measured_failure_error"] = (
                    f"{type(metrics_exc).__name__}: {metrics_exc}"
                )
            finally:
                sess.close()
            raise RuntimeError(
                "online direct measured request failed: "
                f"{json.dumps(failed_responses[0], ensure_ascii=False)[:4000]}"
            )
        response = responses[-1]
        summary["warmup_responses"] = warmup_responses
        summary["responses"] = responses
        summary["response"] = response
        summary["response_consistency"] = _summarize_deterministic_response_consistency(
            responses,
            required=deterministic_response_consistency_required,
        )
        summary["warmup_response_consistency"] = _summarize_deterministic_response_consistency(
            warmup_responses,
            required=deterministic_response_consistency_required,
        )
        summary["all_response_consistency"] = _summarize_deterministic_response_consistency(
            [*warmup_responses, *responses],
            required=deterministic_response_consistency_required,
        )
        if reference_response_source is not None:
            summary[
                "reference_response_equivalence"
            ] = _summarize_reference_response_equivalence(
                [*warmup_responses, *responses],
                reference=reference_response_source,
                required=True,
            )
        ok_responses = [
            item
            for item in responses
            if int(item.get("status_code", 0) or 0) < 400 and not item.get("error")
        ]
        ttft_values = [
            float(item.get("ttft_ms", 0.0) or 0.0)
            for item in ok_responses
            if item.get("ttft_ms") is not None
        ]
        elapsed_values = [
            float(item.get("elapsed_ms", 0.0) or 0.0)
            for item in ok_responses
            if item.get("elapsed_ms") is not None
        ]
        completion_tokens = sum(
            int(item.get("completion_tokens", 0) or 0)
            for item in ok_responses
        )
        tpot_values = [
            float(item.get("tpot_ms", 0.0) or 0.0)
            for item in ok_responses
            if item.get("tpot_ms") is not None
        ]
        summary["benchmark_elapsed_s"] = benchmark_elapsed_s
        summary["request_throughput_rps"] = len(ok_responses) / benchmark_elapsed_s
        summary["goodput_rps"] = len(ok_responses) / benchmark_elapsed_s
        summary["completion_tokens"] = completion_tokens
        summary["completion_token_throughput_tps"] = completion_tokens / benchmark_elapsed_s
        summary["output_token_goodput_tps"] = completion_tokens / benchmark_elapsed_s
        summary["avg_tpot_ms"] = _avg(tpot_values)
        summary["avg_ttft_ms"] = _avg(ttft_values)
        summary["ttft_ms"] = float(response.get("ttft_ms", 0.0) or 0.0)
        summary["ttft_stats_ms"] = _latency_stats(ttft_values)
        summary["avg_latency_ms"] = _avg(elapsed_values)
        summary["latency_stats_ms"] = _latency_stats(elapsed_values)
        worker_dispatches = _summarize_worker_dispatches(ok_responses)
        summary.update(worker_dispatches)
        summary["worker_dispatch_balance"] = _summarize_worker_dispatch_balance(
            worker_dispatches,
            prefill_worker_ids=[f"prefill-{index}" for index in range(len(prefill_ports))],
            decode_worker_ids=[f"decode-{index}" for index in range(len(decode_ports))],
        )
        warmup_ttft_values = [
            float(item.get("ttft_ms", 0.0) or 0.0)
            for item in warmup_responses
            if item.get("ttft_ms") is not None and not item.get("error")
        ]
        summary["warmup_ttft_stats_ms"] = _latency_stats(warmup_ttft_values)
        residual_values: List[float] = []
        for item in ok_responses:
            timings = dict(item.get("epd_timing_ms") or {})
            ttft = item.get("ttft_ms")
            if ttft is None:
                continue
            residual_values.append(
                max(
                    0.0,
                    float(ttft)
                    - float(timings.get("mm_prepare_ms", 0.0) or 0.0)
                    - float(timings.get("prefill_ms", 0.0) or 0.0),
                )
            )
        summary["avg_decode_handoff_to_ttft_ms"] = _avg(residual_values)
        for timing_name in (
            "proxy_request_body_read_ms",
            "proxy_request_json_decode_ms",
            "proxy_request_parse_ms",
            "proxy_request_to_mm_prepare_start_ms",
            "proxy_prefill_to_decode_dispatch_ms",
            "proxy_request_to_decode_dispatch_ms",
            "proxy_request_to_decode_stream_open_ms",
            "proxy_request_to_first_event_ms",
            "proxy_request_to_first_content_ms",
            "decode_stream_open_ms",
            "decode_http_ms",
            "decode_total_ms",
            "decode_first_event_ms",
            "decode_first_content_ms",
            "prefill_render_cache_lookup_ms",
            "prefill_render_json_encode_ms",
            "prefill_render_json_decode_ms",
            "prefill_render_upstream_ms",
            "prefill_render_ms",
            "prefill_generate_json_encode_ms",
            "prefill_generate_json_decode_ms",
            "prefill_generate_ms",
            "prefill_render_generate_ms",
            "prefill_openai_json_encode_ms",
            "prefill_openai_json_decode_ms",
            "prefill_openai_http_ms",
            "prefill_openai_prompt_only_json_encode_ms",
            "prefill_openai_prompt_only_json_decode_ms",
            "decode_json_encode_ms",
            "decode_json_decode_ms",
            "decode_token_envelope_build_ms",
        ):
            values = [
                float(dict(item.get("epd_timing_ms") or {}).get(timing_name, 0.0) or 0.0)
                for item in ok_responses
                if timing_name in dict(item.get("epd_timing_ms") or {})
            ]
            if values:
                summary[f"avg_{timing_name}"] = _avg(values)
                summary[f"{timing_name}_stats_ms"] = _latency_stats(values)
        summary.update(_summarize_direct_cache_timing(ok_responses))
        summary.update(_summarize_rendered_prefill_cache_timing(ok_responses))
        summary.update(_summarize_proxy_wire_metrics(ok_responses))
        summary["direct_feature_delivery"] = _summarize_direct_feature_delivery(
            ok_responses
        )
        decode_gap_values: List[float] = []
        client_proxy_gap_values: List[float] = []
        for item in ok_responses:
            timings = dict(item.get("epd_timing_ms") or {})
            if "decode_first_content_ms" not in timings:
                continue
            ttft = item.get("ttft_ms")
            if ttft is None:
                continue
            pre_decode = float(timings.get("mm_prepare_ms", 0.0) or 0.0) + float(timings.get("prefill_ms", 0.0) or 0.0)
            residual = max(0.0, float(ttft) - pre_decode)
            decode_first_content = float(timings.get("decode_first_content_ms", 0.0) or 0.0)
            decode_gap_values.append(max(0.0, decode_first_content - float(timings.get("decode_stream_open_ms", 0.0) or 0.0)))
            client_proxy_gap_values.append(residual - decode_first_content)
        if decode_gap_values:
            summary["avg_decode_stream_open_to_first_content_ms"] = _avg(decode_gap_values)
            summary["decode_stream_open_to_first_content_stats_ms"] = _latency_stats(decode_gap_values)
        if client_proxy_gap_values:
            summary["avg_client_proxy_ttft_gap_ms"] = _avg(client_proxy_gap_values)
            summary["client_proxy_ttft_gap_stats_ms"] = _latency_stats(client_proxy_gap_values)
        client_to_proxy_first_content_gap_values: List[float] = []
        for item in ok_responses:
            timings = dict(item.get("epd_timing_ms") or {})
            proxy_first_content = timings.get("proxy_request_to_first_content_ms")
            ttft = item.get("ttft_ms")
            if proxy_first_content is None or ttft is None:
                continue
            client_to_proxy_first_content_gap_values.append(
                max(0.0, float(ttft) - float(proxy_first_content))
            )
        if client_to_proxy_first_content_gap_values:
            summary["avg_client_to_proxy_first_content_gap_ms"] = _avg(
                client_to_proxy_first_content_gap_values
            )
            summary["client_to_proxy_first_content_gap_stats_ms"] = _latency_stats(
                client_to_proxy_first_content_gap_values
            )
        proxy_url = f"http://{args.local_hostname}:{proxy_port}"
        if (
            not text_only
            and release_direct_feature_buffers_after_prefill
            and (
                int(cfg.direct_feature_lease_prefetch) > 1
                or bool(cfg.direct_feature_adaptive_lease_prefetch)
            )
        ):
            summary["direct_feature_lease_prefetch_flush"] = (
                _flush_proxy_direct_feature_lease_prefetch(
                    proxy_url,
                    auth_token=str(cfg.direct_feature_buffer_auth_token or ""),
                )
            )
        sess = _session()
        try:
            metrics_payload = _wait_for_metrics_settle(sess, f"{proxy_url}/metrics", timeout_s=20.0, poll_s=0.5)
        finally:
            sess.close()
        if text_only:
            # Text-only mode intentionally does not start the E-stage or the
            # Prefill direct-buffer route; do not probe absent services or add
            # an artificial release timeout to the P->D benchmark.
            direct_stats_by_prefill = {}
        else:
            settled_direct_stats = _wait_for_direct_buffer_release_settle(
                [f"http://{args.local_hostname}:{port}" for port in prefill_ports],
                timeout_s=min(30.0, max(1.0, float(args.request_timeout))),
                auth_token=str(cfg.direct_feature_buffer_auth_token or ""),
            )
            direct_stats_by_prefill = {
                f"prefill-{idx}": settled_direct_stats.get(
                    f"http://{args.local_hostname}:{port}", {}
                )
                for idx, port in enumerate(prefill_ports)
            }
        direct_stats = _merge_direct_buffer_stats(direct_stats_by_prefill)
        summary["metrics"] = metrics_payload
        summary["direct_buffer_stats_by_prefill"] = direct_stats_by_prefill
        summary["direct_buffer_stats_after_release"] = direct_stats
        summary["online_direct_metric_summary"] = summarize_online_direct_metrics(
            metrics_payload,
            direct_buffer_stats=direct_stats,
        )
        if metrics_before_measured:
            metrics_delta_payload = _numeric_metric_delta(metrics_payload, metrics_before_measured)
            summary["metrics_measured_delta"] = metrics_delta_payload
            summary["measured_online_direct_metric_summary"] = summarize_online_direct_metrics(
                metrics_delta_payload,
                direct_buffer_stats=direct_stats,
            )
        summary["feature_handle_metric_summary"] = dict(summary["online_direct_metric_summary"])
        for metric_name in (
            "layered_receive_kv_requests",
            "layered_receive_kv_worker_roundtrips",
            "layered_receive_kv_worker_ms_avg",
            "layered_receive_kv_response_messages",
            "layered_receive_kv_first_response_count",
            "layered_receive_kv_first_response_ms_avg",
            "layered_receive_kv_last_response_count",
            "layered_receive_kv_last_response_ms_avg",
            "layered_receive_kv_response_process_count",
            "layered_receive_kv_response_process_ms_avg",
            "layered_receive_kv_first_group_count",
            "layered_receive_kv_first_group_ms_avg",
            "layered_receive_kv_finished_count",
            "layered_receive_kv_finished_ms_avg",
            "layered_source_ready_event_waits",
            "layered_source_ready_event_wait_ms",
            "layered_source_ready_event_wait_ms_avg",
            "layered_source_ready_sync_fallbacks",
            "decode_engine_timing_workers",
            "decode_engine_first_token_requests",
            "decode_engine_first_token_latency_ms_avg",
            "decode_engine_kv_first_token_requests",
            "decode_engine_kv_first_token_latency_ms_avg",
            "decode_engine_kv_first_token_output_tokens",
            "decode_engine_scheduler_update_calls",
        ):
            if metric_name in summary["online_direct_metric_summary"]:
                summary[metric_name] = summary["online_direct_metric_summary"][metric_name]
        summary["precomputed_hits"] = int(
            summary["online_direct_metric_summary"].get("precomputed_hits", 0) or 0
        )
        summary["fallback_batches"] = int(
            summary["online_direct_metric_summary"].get("fallback_batches", 0) or 0
        )
        summary["layered_receive_failures"] = int(
            summary["online_direct_metric_summary"].get("layered_receive_failures", 0) or 0
        )
        summary["layered_transfer_failed_batches"] = int(
            summary["online_direct_metric_summary"].get("layered_transfer_failed_batches", 0) or 0
        )
        summary["workflow_registry_wal"] = (
            str(workdir / "proxy_workflow_registry.jsonl")
            if durable_workflow_registry
            else None
        )
        summary["workflow_registry_durability"] = (
            "fsync_wal" if durable_workflow_registry else "in_memory"
        )
        summary["connector_metrics_dir"] = str(workdir / "connector_metrics")
        summary["repeat_requests"] = total_requests
        _record_runtime_transport_evidence(
            summary,
            logs=logs,
            requested=str(args.mooncake_protocol),
        )
        if not text_only:
            _record_runtime_transport_evidence(
                summary,
                logs=logs,
                requested=_effective_direct_engine_protocol(args),
                evidence_key="direct_transport_runtime_evidence",
                worker_logs=_direct_engine_transport_logs(logs),
            )
        raw_request_rows: List[Dict[str, Any]] = []
        raw_response_rows: List[Dict[str, Any]] = []
        for phase, phase_responses, phase_prefix in (
            ("warmup", warmup_responses, f"{workflow_prefix}-warmup"),
            ("measure", responses, workflow_prefix),
        ):
            for item in phase_responses:
                repeat_idx = int(item.get("index", 0) or 0)
                entry = _online_direct_entry(
                    request_path,
                    repeat_idx=repeat_idx,
                    workflow_prefix=phase_prefix,
                    request_variation=str(args.request_variation),
                    vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                    phase=phase,
                    dataset_entries=dataset_entries or None,
                    dataset_workflow_id_mode=dataset_workflow_id_mode,
                )
                if dataset_entries:
                    evidence = _dataset_request_evidence_row(
                        entry,
                        index=repeat_idx,
                        phase=phase,
                    )
                    evidence.update(
                        {
                            "dataset_cycle_index": entry.get("dataset_cycle_index"),
                            "dataset_source_index": entry.get("dataset_source_index"),
                            "dataset_workflow_id_mode": entry.get(
                                "dataset_workflow_id_mode"
                            ),
                            "request_variation_id": entry.get(
                                "request_variation_id"
                            ),
                            "vllm_prefix_cache_id": entry.get(
                                "vllm_prefix_cache_id"
                            ),
                        }
                    )
                    raw_request_rows.append(evidence)
                else:
                    raw_request_rows.append(
                        {"phase": phase, "index": repeat_idx, **entry}
                    )
                raw_response_rows.append({**dict(item), "phase": phase})
        summary["raw_artifacts"] = write_raw_benchmark_artifacts(
            workdir=workdir,
            request_rows=raw_request_rows,
            response_rows=raw_response_rows,
            service_logs=logs,
            metrics={
                "proxy_metrics": metrics_payload,
                "proxy_metrics_measured_delta": summary.get("metrics_measured_delta", {}),
                "online_direct_metric_summary": summary["online_direct_metric_summary"],
                "measured_online_direct_metric_summary": summary.get(
                    "measured_online_direct_metric_summary", {}
                ),
                "direct_buffer_stats_after_release": direct_stats,
                "ttft_stats_ms": summary.get("ttft_stats_ms", {}),
                "latency_stats_ms": summary.get("latency_stats_ms", {}),
                "response_consistency": summary.get("response_consistency", {}),
                "warmup_response_consistency": summary.get(
                    "warmup_response_consistency", {}
                ),
                "all_response_consistency": summary.get(
                    "all_response_consistency", {}
                ),
                "reference_response_equivalence": summary.get(
                    "reference_response_equivalence", {}
                ),
            },
            runtime={
                "runner": "run_vllm_online_direct_e2e",
                "model": str(args.model),
                "mooncake_protocol": str(args.mooncake_protocol),
                "strict_no_fallback": True,
                "workflow_registry_durability": summary[
                    "workflow_registry_durability"
                ],
                "benchmark_config": summary["benchmark_config"],
            },
        )
        validate_online_direct_summary(summary)
        return summary
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        summary["log_tails"] = {key: _tail_text(path, 120) for key, path in logs.items()}
        raise
    finally:
        _terminate_all(procs)
        out = workdir / "online_direct_e2e_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run strict online direct-buffer vLLM EPD E2E validation.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_online_direct_e2e")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--encoder-device", default="cuda:5")
    ap.add_argument("--encoder-dtype", default="bfloat16")
    ap.add_argument(
        "--encoder-family",
        choices=["auto", "qwen3_vl", "qwen2_5_vl", "qwen2_5_omni"],
        default="auto",
    )
    ap.add_argument(
        "--encoder-runtime",
        choices=["transformers", "vllm_native"],
        default=os.getenv("MOONCAKE_EPD_ENCODER_RUNTIME", "transformers"),
        help=(
            "E-stage implementation; vllm_native is the Qwen3-VL-native "
            "visual ABI path used for numerical parity validation."
        ),
    )
    ap.add_argument(
        "--mooncake-protocol",
        choices=["tcp", "shm", "rdma", "nvlink_intra"],
        default="tcp",
        help="P-D KV transport; nvlink_intra requires USE_INTRA_NVLINK=ON.",
    )
    ap.add_argument(
        "--direct-engine-protocol",
        choices=["tcp", "rdma", "nvlink_intra"],
        default=None,
        help="Optional E→P FeatureBundle transport override; P→D keeps --mooncake-protocol.",
    )
    ap.add_argument("--direct-source-mode", choices=["registered_tensor", "managed_buffer"], default="registered_tensor")
    ap.add_argument("--direct-target-mode", choices=["registered_tensor", "managed_buffer", "auto"], default="registered_tensor")
    ap.add_argument("--direct-register-memory", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--direct-persistent-cache", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument(
        "--feature-handle-checksum",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Verify full E->P FeatureBundle checksums at Prefill. Diagnostic "
            "only: it adds device-to-host validation work to TTFT."
        ),
    )
    ap.add_argument("--direct-proxy-handle-cache", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument(
        "--release-direct-feature-buffers-after-prefill",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Release E→P direct-buffer leases after Prefill (default). "
            "Disable only when intentionally retaining valid direct-buffer "
            "leases for the proxy FeatureHandle cache."
        ),
    )
    ap.add_argument(
        "--direct-feature-release-batch-max-jobs",
        type=int,
        default=16,
        help=(
            "Bounded number of post-Prefill release jobs merged per control "
            "RPC; preserves every direct-buffer reference release."
        ),
    )
    ap.add_argument("--direct-proxy-handle-cache-max-entries", type=int, default=4096)
    ap.add_argument("--direct-proxy-handle-cache-ttl-s", type=float, default=600.0)
    ap.add_argument(
        "--direct-feature-lease-prefetch",
        type=int,
        default=1,
        help=(
            "Reserve this many generation-fenced E->P direct-buffer references "
            "per hot descriptor tuple; 1 keeps serial requests at one reference."
        ),
    )
    ap.add_argument("--direct-feature-lease-prefetch-max-entries", type=int, default=64)
    ap.add_argument("--direct-feature-lease-prefetch-ttl-s", type=float, default=30.0)
    ap.add_argument(
        "--enable-direct-feature-adaptive-lease-prefetch",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Opt in to reserving only the references needed by already-"
            "concurrent identical direct-cache lookups when fixed lease=1."
        ),
    )
    ap.add_argument(
        "--direct-feature-adaptive-lease-prefetch-max",
        type=int,
        default=2,
    )
    ap.add_argument(
        "--enable-rendered-prefill-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Reuse generation-fenced /render outputs for repeated immutable "
            "requests; use --no-enable-rendered-prefill-cache for A/B."
        ),
    )
    ap.add_argument("--rendered-prefill-cache-max-entries", type=int, default=64)
    ap.add_argument(
        "--rendered-prefill-cache-max-bytes",
        type=int,
        default=256 * 1024 * 1024,
    )
    ap.add_argument("--rendered-prefill-cache-ttl-s", type=float, default=300.0)
    ap.add_argument("--direct-cache-max-entries", type=int, default=64)
    ap.add_argument("--direct-cache-max-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    ap.add_argument(
        "--vllm-mm-hidden-cache",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Pin vLLM worker multimodal hidden-state cache on/off for a fair "
            "cache A/B; omitted preserves the deployment environment default."
        ),
    )
    ap.add_argument("--prefill-dispatch-mode", choices=["render_generate", "openai_prompt_only"], default="render_generate")
    ap.add_argument(
        "--decode-dispatch-mode",
        choices=["legacy", "shadow", "token_ids"],
        default="legacy",
        help=(
            "Validate the v1 P->D token envelope in shadow or use the compact "
            "media-free token-ID Decode request."
        ),
    )
    ap.add_argument(
        "--allow-decode-token-fallback",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Explicit non-strict ablation only; the online runner itself uses "
            "strict serving, so token-ID incompatibilities normally fail."
        ),
    )
    ap.add_argument(
        "--prefill-http-keepalive",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Reuse idle Proxy->Prefill HTTP connections. Disabled by default "
            "to avoid stale-connection failures on non-idempotent Prefill POSTs."
        ),
    )
    ap.add_argument(
        "--allow-unverified-openai-prompt-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Only for a version/workload pair with recorded output-equivalence evidence.",
    )
    ap.add_argument("--encoder-port", type=int, default=8330)
    ap.add_argument("--encoder-request-timeout-s", type=float, default=30.0)
    ap.add_argument("--prefill-gpu", type=int, default=3)
    ap.add_argument("--prefill-gpus", type=int, nargs="+", default=None, help="Launch multiple Prefill vLLM workers on these physical GPU ids. Overrides --prefill-gpu when set.")
    ap.add_argument("--prefill-ports", type=int, nargs="+", default=None, help="Optional preferred Prefill ports, one per --prefill-gpus entry.")
    ap.add_argument("--decode-gpu", type=int, default=4)
    ap.add_argument("--decode-gpus", type=int, nargs="+", default=None, help="Launch multiple Decode vLLM workers on these physical GPU ids. Overrides --decode-gpu when set.")
    ap.add_argument("--decode-ports", type=int, nargs="+", default=None, help="Optional preferred Decode ports, one per --decode-gpus entry.")
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--max-num-batched-tokens", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument("--max-num-seqs", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument(
        "--generation-config",
        default="vllm",
        help="vLLM --generation-config value; benchmark default avoids model-repository sampling defaults.",
    )
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument(
        "--startup-timeout",
        type=float,
        default=900.0,
        help="Maximum seconds to wait for vLLM cold start and compilation.",
    )
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--warmup-requests", type=int, default=0)
    ap.add_argument(
        "--require-measurement-purity",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Require at least one successful warmup, drained serving queues, "
            "and consecutive compile-quiet polls before measured traffic."
        ),
    )
    ap.add_argument("--measurement-purity-timeout-s", type=float, default=20.0)
    ap.add_argument("--measurement-purity-poll-s", type=float, default=0.5)
    ap.add_argument("--measurement-purity-stable-polls", type=int, default=2)
    ap.add_argument(
        "--warmup-cover-dataset-cycle",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "For dataset replay, warm every selected sample at least once before "
            "measurement so image/prompt shapes are not left cold."
        ),
    )
    ap.add_argument("--min-warmup-per-decode", type=int, default=2, help="When multiple Decode workers are launched and warmup is enabled, ensure at least this many warmup requests per Decode worker.")
    ap.add_argument("--warmup-concurrency", type=int, default=0, help="Warmup concurrency; 0 means auto-cover deployed Prefill/Decode fanout and measured concurrency.")
    ap.add_argument("--repeat-requests", type=int, default=1)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--between-repeat-sleep-s", type=float, default=0.2)
    ap.add_argument(
        "--require-deterministic-response-consistency",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Require byte-identical response SHA-256s for repeated deterministic "
            "requests. Auto-enabled only for fully sequential temperature=0, "
            "unmodified single-request workloads repeated at least twice."
        ),
    )
    ap.add_argument(
        "--reference-response-summary",
        default=None,
        help=(
            "single_baseline_summary.json from an identical single-vLLM run. "
            "For concurrent EPD, every warmup/measure completion SHA-256 must "
            "belong to the compatible baseline response set."
        ),
    )
    ap.add_argument("--prompt", default="Describe the image briefly.")
    ap.add_argument(
        "--prompt-file",
        default=None,
        help="UTF-8 prompt file; supersedes --prompt and keeps long benchmark prompts out of shell history.",
    )
    ap.add_argument(
        "--request-variation",
        choices=[
            REQUEST_VARIATION_NONE,
            REQUEST_VARIATION_UNIQUE_PREFIX,
            REQUEST_VARIATION_UNIQUE_SUFFIX,
        ],
        default=REQUEST_VARIATION_NONE,
        help="Use unique_prefix to prevent vLLM prefix-cache reuse; unique_suffix is a compatibility alias.",
    )
    ap.add_argument(
        "--vllm-prefix-cache-mode",
        choices=[VLLM_PREFIX_CACHE_REUSE, VLLM_PREFIX_CACHE_ISOLATE],
        default=VLLM_PREFIX_CACHE_ISOLATE,
        help=(
            "isolate is the strict EPD default and uses per-request cache_salt "
            "without changing model-visible prompt tokens; reuse is an explicit "
            "prefix-cache ablation that requires output-equivalence evidence."
        ),
    )
    ap.add_argument("--workflow-id", default="online-direct-e2e")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-url", default=None, help="One explicit image URL/data URL.")
    ap.add_argument(
        "--image-urls",
        nargs="+",
        default=None,
        help="Two or more image URLs/data URLs in one request; mutually exclusive with --image-url.",
    )
    ap.add_argument("--text-only", action="store_true", help="Exercise only the real P->D KV path; do not start Encoder or FeatureBuffer services.")
    ap.add_argument(
        "--dataset-root",
        default=None,
        help=(
            "Root of the versioned mooncake_test_dataset. Enables paired real "
            "multimodal chat-split replay instead of the one-off demo prompt."
        ),
    )
    ap.add_argument("--dataset-chat-split", default="dev-small")
    ap.add_argument(
        "--max-dataset-requests",
        type=int,
        default=0,
        help="Required with --dataset-root; selected requests are cycled round-robin.",
    )
    ap.add_argument("--dataset-families", nargs="+", default=None)
    ap.add_argument(
        "--dataset-workflow-id-mode",
        choices=["unique", "source"],
        default="unique",
        help=(
            "unique isolates every request from workflow-state reuse; source "
            "preserves the source workflow id for explicit cache/workflow studies."
        ),
    )
    ap.add_argument(
        "--dataset-agent-pd-labels",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Attach deterministic Agent-PD labels derived from source workload shape.",
    )
    ap.add_argument(
        "--dataset-max-input-len",
        type=int,
        default=0,
        help="Admission limit; 0 inherits --max-model-len.",
    )
    ap.add_argument(
        "--dataset-request-max-tokens",
        type=int,
        default=0,
        help="Per-dataset request output cap; 0 inherits --max-tokens.",
    )
    ap.add_argument(
        "--dataset-skip-oversized",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reject selected samples whose processor-estimated input cannot fit.",
    )
    ap.add_argument(
        "--dataset-image-max-pixels",
        type=int,
        default=0,
        help="Optional per-image preprocessing cap recorded in the paired artifact.",
    )
    ap.add_argument("--layers-per-group", type=int, default=32)
    ap.add_argument("--max-group-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=512)
    ap.add_argument("--max-transfer-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument(
        "--tcp-write-completion-ack",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "For TCP transfers, wait for the receiver's post-GPU-copy ACK "
            "before publishing P→D KV completion. Disable only for an "
            "explicit unsafe transport-overlap ablation."
        ),
    )
    ap.add_argument(
        "--tcp-connection-pool",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Experimental TCP socket reuse. Disabled by default because it "
            "must pass a workload-specific deterministic response-consistency "
            "campaign before it can be used for performance claims."
        ),
    )
    ap.add_argument(
        "--prefill-decode-affinity",
        nargs="*",
        default=None,
        metavar="PREFILL=DECODE",
        help="Optional P->D transport-locality pairs; nvlink_intra defaults to list-aligned pairs.",
    )
    ap.add_argument(
        "--scheduler-policy",
        choices=sorted(SCHEDULER_POLICIES),
        default="agent_aware",
        help=(
            "Proxy P/D worker-selection policy. Record this explicitly for "
            "multi-worker capacity and routing-policy A/B tests."
        ),
    )
    ap.add_argument(
        "--transfer-workers",
        type=int,
        default=0,
        help="KV sender workers; 0 selects 2 for nvlink_intra and 4 for TCP/RDMA.",
    )
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    ap.add_argument(
        "--durable-workflow-registry",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Use fsync-backed workflow WAL for restart-safe Agent-state recovery. "
            "Disabled by default for serving benchmarks because it is control-plane "
            "durability, not required P→D data-plane integrity."
        ),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    # A benchmark may be interrupted while worker processes are still loading
    # a model.  Convert SIGINT/SIGTERM into SystemExit so ``run`` reaches its
    # existing ``finally`` block and terminates every owned process group;
    # otherwise a stale EngineCore can consume a GPU or a Bootstrap port and
    # corrupt the next performance measurement.
    previous_handlers = _install_graceful_termination_handlers()
    try:
        summary = run(args)
    finally:
        _restore_termination_handlers(previous_handlers)
    out = Path(args.workdir).expanduser() / "online_direct_e2e_summary.json"
    print(
        json.dumps(
            {
                "summary": str(out),
                "response": summary.get("response", {}),
                "online_direct_metric_summary": summary.get("online_direct_metric_summary", {}),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _termination_signal_to_system_exit(signum: int, _frame: Any) -> None:
    """Let :func:`run` clean up child process groups after a CLI interrupt."""

    raise SystemExit(128 + int(signum))


def _install_graceful_termination_handlers() -> Dict[int, Any]:
    previous: Dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[int(signum)] = signal.getsignal(signum)
        signal.signal(signum, _termination_signal_to_system_exit)
    return previous


def _restore_termination_handlers(previous: Mapping[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(int(signum), handler)


if __name__ == "__main__":
    main()
