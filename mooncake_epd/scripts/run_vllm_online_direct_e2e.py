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
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Sequence

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import (  # noqa: E402
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
    apply_request_variation,
    request_variation_spec,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _data_url_for_demo_image,
    _ensure_process_running,
    _execute_dataset_request,
    _extract_choice_text,
    _extract_port,
    _launch,
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
        image_url = str(args.image_url or "")
        if not image_url:
            image_url = _data_url_for_demo_image(str(args.demo_image))
        content.insert(0, {"type": "image_url", "image_url": {"url": image_url}})
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
    request_path: Path,
    *,
    repeat_idx: int,
    workflow_prefix: str,
    request_variation: str = REQUEST_VARIATION_NONE,
    phase: str = "measure",
) -> Dict[str, Any]:
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
    return {
        "request": request_body,
        "family": "online_direct_e2e",
        "sample": {
            "sample_id": f"online-direct-e2e-r{repeat_idx}",
            "workflow_id": metadata.get("workflow_id", f"online-direct-e2e-r{repeat_idx}"),
            "source_dataset": "online_direct_e2e",
            "request_variation_id": variation_id,
        },
    }


def _run_online_direct_request(
    *,
    proxy_url: str,
    request_path: Path,
    repeat_idx: int,
    workflow_prefix: str,
    request_timeout: float,
    request_variation: str = REQUEST_VARIATION_NONE,
    phase: str = "measure",
) -> Dict[str, Any]:
    response = _execute_dataset_request(
        proxy_url=proxy_url,
        entry=_online_direct_entry(
            request_path,
            repeat_idx=repeat_idx,
            workflow_prefix=workflow_prefix,
            request_variation=request_variation,
            phase=phase,
        ),
        index=repeat_idx,
        request_timeout=float(request_timeout),
        stream_metrics=True,
    )
    response["headers"] = {
        "x-epd-routing-path": response.get("routing_path"),
        "x-epd-admission": response.get("admission"),
        "x-epd-degrade-level": response.get("degrade_level"),
        "x-epd-timing-ms": response.get("epd_timing_ms_header"),
    }
    if int(response.get("status_code") or 0) >= 400 or response.get("error"):
        raise RuntimeError(f"online direct request failed: {json.dumps(response, ensure_ascii=False)[:4000]}")
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
        + "export MOONCAKE_EPD_ENCODER_PUBLISH_BACKEND=direct_engine\n"
        + f"export MOONCAKE_EPD_DIRECT_SOURCE_MODE={args.direct_source_mode}\n"
        + f"{VENV_ROOT}/bin/python {REPO_ROOT / 'scripts' / 'epd_encoder_service.py'} "
        + f"--model {args.model} "
        + f"--device {args.encoder_device} "
        + f"--dtype {args.encoder_dtype} "
        + f"--encoder-family {args.encoder_family} "
        + "--publish-backend direct_engine "
        + f"--direct-source-mode {args.direct_source_mode} "
        + f"--request-timeout-s {float(args.encoder_request_timeout_s)}\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _direct_buffer_stats(base_url: str) -> Dict[str, Any]:
    sess = _session()
    try:
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


def _merge_direct_buffer_stats(stats_by_worker: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {"workers": stats_by_worker}
    numeric_fields = {
        "allocations",
        "bytes",
        "managed_buffers",
        "ready",
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




_MEASURED_WINDOW_GAUGES = {
    "connector_metric_workers",
    "mm_hidden_cache_workers",
    "mm_hidden_cache_enabled_workers",
    "mm_hidden_cache_bytes",
    "mm_hidden_cache_entries",
    "decode_engine_timing_workers",
}


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
    return {"metrics": delta}

def summarize_online_direct_metrics(
    metrics_payload: Dict[str, Any],
    *,
    direct_buffer_stats: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    summary = summarize_feature_handle_metrics(metrics_payload)
    direct_stats = dict(direct_buffer_stats or {})
    summary["direct_buffer_allocations"] = int(direct_stats.get("allocations", 0) or 0)
    summary["direct_buffer_bytes"] = int(direct_stats.get("bytes", 0) or 0)
    summary["direct_buffer_managed_buffers"] = int(direct_stats.get("managed_buffers", 0) or 0)
    return summary


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
    if text_only:
        if int(metrics.get("requests_text", 0) or 0) < 1:
            failures.append("requests_text < 1")
    else:
        if int(metrics.get("requests_multimodal", 0) or 0) < 1:
            failures.append("requests_multimodal < 1")
        if int(metrics.get("precomputed_hits", 0) or 0) < 1:
            failures.append("Prefill did not consume precomputed image embeds")
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
    persistent_cache = bool(direct_stats.get("persistent_cache", False))
    if (not persistent_cache) and int(metrics.get("direct_buffer_allocations", 0) or 0) != 0:
        failures.append("Prefill direct feature buffers were not released")
    if failures:
        raise AssertionError("; ".join(failures))


def run(args: argparse.Namespace) -> Dict[str, Any]:
    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    for name in ("online_direct_e2e_summary.json", "online_direct_request.json"):
        path = workdir / name
        if path.exists():
            path.unlink()

    text_only = bool(getattr(args, "text_only", False))
    encoder_port = _pick_free_port(int(args.encoder_port), str(args.local_hostname)) if not text_only else 0
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=str(args.local_hostname),
        mm_prefetch_mode="feature_handle" if not text_only else "asset_bytes",
        prefill_supports_feature_handles=not text_only,
        enable_prefill_direct_feature_buffer_routes=not text_only,
        enable_direct_feature_handle_cache=bool(args.direct_proxy_handle_cache) and not text_only,
        direct_feature_handle_cache_max_entries=int(args.direct_proxy_handle_cache_max_entries),
        direct_feature_handle_cache_ttl_s=float(args.direct_proxy_handle_cache_ttl_s),
        direct_feature_target_mode=str(args.direct_target_mode),
        direct_feature_register_memory=bool(args.direct_register_memory),
        direct_feature_persistent_cache=bool(args.direct_persistent_cache),
        direct_feature_cache_max_entries=int(args.direct_cache_max_entries),
        direct_feature_cache_max_bytes=int(args.direct_cache_max_bytes),
        release_direct_feature_buffers_after_prefill=True,
        prefill_dispatch_mode=str(args.prefill_dispatch_mode),
        allow_unverified_openai_prompt_only=bool(
            getattr(args, "allow_unverified_openai_prompt_only", False)
        ),
        encoder_service_url=(f"http://{args.local_hostname}:{encoder_port}" if not text_only else None),
        strict_no_fallback=True,
        protocol=str(args.mooncake_protocol),
        direct_engine_protocol=(
            str(args.direct_engine_protocol)
            if getattr(args, "direct_engine_protocol", None)
            else None
        ),
        workflow_registry_wal_path=str(workdir / "proxy_workflow_registry.jsonl"),
        connector_metrics_dir=str(workdir / "connector_metrics"),
        max_group_bytes=int(args.max_group_bytes),
        layers_per_group=int(args.layers_per_group),
        max_transfer_descriptors=int(args.max_transfer_descriptors),
        max_transfer_bytes=int(args.max_transfer_bytes),
        transfer_workers=int(args.transfer_workers),
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
        "mooncake_protocol": str(args.mooncake_protocol),
        "text_only": text_only,
        "benchmark_config": {
            "schema_version": 1,
            "request_fingerprint": _request_fingerprint(request_path),
            "request": {
                "model": str(args.model),
                "max_tokens": int(args.max_tokens),
                "temperature": float(args.temperature),
                "image_source": "none" if text_only else ("explicit" if args.image_url else str(args.demo_image)),
                "prompt_source": "file" if getattr(args, "prompt_file", None) else "inline",
            },
            "request_variation": request_variation_spec(str(args.request_variation)),
            "load": {
                "repeat_requests": max(1, int(args.repeat_requests)),
                "concurrency": max(1, int(args.concurrency)),
                "requested_warmup_requests": max(0, int(args.warmup_requests)),
                "requested_warmup_concurrency": max(0, int(args.warmup_concurrency)),
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
                "prefill_gpus": list(cfg.prefill_gpus or (cfg.prefill_gpu,)),
                "decode_gpus": list(cfg.decode_gpus or (cfg.decode_gpu,)),
                "total_gpus": (0 if text_only else 1) + len(cfg.prefill_gpus or (cfg.prefill_gpu,)) + len(cfg.decode_gpus or (cfg.decode_gpu,)),
            },
            "epd": {
                "protocol": str(args.mooncake_protocol),
                "layers_per_group": int(args.layers_per_group),
                "max_group_bytes": int(args.max_group_bytes),
                "max_transfer_descriptors": int(args.max_transfer_descriptors),
                "max_transfer_bytes": int(args.max_transfer_bytes),
                "transfer_workers": int(cfg.effective_transfer_workers),
                "prefill_decode_affinity": [
                    [prefill, decode]
                    for prefill, decode in cfg.resolved_prefill_decode_affinity
                ],
            },
        },
    }
    try:
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
        if not text_only:
            _wait_ready("encoder", f"http://{args.local_hostname}:{encoder_port}", proc=named["encoder"], log_path=logs["encoder"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        for idx, port in enumerate(prefill_ports):
            key = f"prefill_{idx}"
            _wait_ready(key, f"http://{args.local_hostname}:{port}", proc=named[key], log_path=logs[key], timeout_s=min(300.0, timeout_s), paths=("/health",))
        for idx, port in enumerate(decode_ports):
            key = f"decode_{idx}"
            _wait_ready(key, f"http://{args.local_hostname}:{port}", proc=named[key], log_path=logs[key], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("proxy", f"http://{args.local_hostname}:{proxy_port}", proc=named["proxy"], log_path=logs["proxy"], timeout_s=min(180.0, timeout_s), paths=("/health", "/healthcheck"))

        request_url = f"http://{args.local_hostname}:{proxy_port}/v1/chat/completions"
        warmup_responses: List[Dict[str, Any]] = []
        workflow_prefix = str(args.workflow_id or "online-direct-e2e")
        requested_warmups = max(0, int(args.warmup_requests))
        effective_warmups = requested_warmups
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
                        phase="warmup",
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
                        phase="warmup",
                    )
                )
                if float(args.between_repeat_sleep_s) > 0:
                    time.sleep(max(0.0, float(args.between_repeat_sleep_s)))

        metrics_before_measured: Dict[str, Any] = {}
        if warmup_responses:
            proxy_url = f"http://{args.local_hostname}:{proxy_port}"
            sess = _session()
            try:
                metrics_before_measured = _wait_for_metrics_settle(
                    sess, f"{proxy_url}/metrics", timeout_s=20.0, poll_s=0.5
                )
            finally:
                sess.close()
            summary["metrics_before_measured"] = metrics_before_measured

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
                        phase="measure",
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
                        phase="measure",
                    ): repeat_idx
                    for repeat_idx in range(total_requests)
                }
                for future in as_completed(future_map):
                    responses.append(future.result())
            responses.sort(key=lambda item: int(item.get("index", 0) or 0))
        benchmark_elapsed_s = max(1e-6, time.perf_counter() - benchmark_started)
        response = responses[-1]
        summary["warmup_responses"] = warmup_responses
        summary["responses"] = responses
        summary["response"] = response
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
            "decode_stream_open_ms",
            "decode_http_ms",
            "decode_total_ms",
            "decode_first_event_ms",
            "decode_first_content_ms",
        ):
            values = [
                float(dict(item.get("epd_timing_ms") or {}).get(timing_name, 0.0) or 0.0)
                for item in ok_responses
                if timing_name in dict(item.get("epd_timing_ms") or {})
            ]
            if values:
                summary[f"avg_{timing_name}"] = _avg(values)
                summary[f"{timing_name}_stats_ms"] = _latency_stats(values)
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
        proxy_url = f"http://{args.local_hostname}:{proxy_port}"
        sess = _session()
        try:
            metrics_payload = _wait_for_metrics_settle(sess, f"{proxy_url}/metrics", timeout_s=20.0, poll_s=0.5)
        finally:
            sess.close()
        direct_stats_by_prefill = {
            f"prefill-{idx}": _direct_buffer_stats(f"http://{args.local_hostname}:{port}")
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
        summary["workflow_registry_wal"] = str(workdir / "proxy_workflow_registry.jsonl")
        summary["connector_metrics_dir"] = str(workdir / "connector_metrics")
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
    ap.add_argument("--encoder-family", choices=["auto", "qwen3_vl", "qwen2_5_omni"], default="auto")
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
    ap.add_argument("--direct-proxy-handle-cache", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--direct-proxy-handle-cache-max-entries", type=int, default=4096)
    ap.add_argument("--direct-proxy-handle-cache-ttl-s", type=float, default=600.0)
    ap.add_argument("--direct-cache-max-entries", type=int, default=64)
    ap.add_argument("--direct-cache-max-bytes", type=int, default=2 * 1024 * 1024 * 1024)
    ap.add_argument("--prefill-dispatch-mode", choices=["render_generate", "openai_prompt_only"], default="render_generate")
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
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--warmup-requests", type=int, default=0)
    ap.add_argument("--min-warmup-per-decode", type=int, default=2, help="When multiple Decode workers are launched and warmup is enabled, ensure at least this many warmup requests per Decode worker.")
    ap.add_argument("--warmup-concurrency", type=int, default=0, help="Warmup concurrency; 0 means auto-cover deployed Prefill/Decode fanout and measured concurrency.")
    ap.add_argument("--repeat-requests", type=int, default=1)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--between-repeat-sleep-s", type=float, default=0.2)
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
    ap.add_argument("--workflow-id", default="online-direct-e2e")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-url", default=None)
    ap.add_argument("--text-only", action="store_true", help="Exercise only the real P->D KV path; do not start Encoder or FeatureBuffer services.")
    ap.add_argument("--layers-per-group", type=int, default=32)
    ap.add_argument("--max-group-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=512)
    ap.add_argument("--max-transfer-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument(
        "--prefill-decode-affinity",
        nargs="*",
        default=None,
        metavar="PREFILL=DECODE",
        help="Optional P->D transport-locality pairs; nvlink_intra defaults to list-aligned pairs.",
    )
    ap.add_argument(
        "--transfer-workers",
        type=int,
        default=0,
        help="KV sender workers; 0 selects 1 for nvlink_intra and 4 for TCP/RDMA.",
    )
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
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


if __name__ == "__main__":
    main()
