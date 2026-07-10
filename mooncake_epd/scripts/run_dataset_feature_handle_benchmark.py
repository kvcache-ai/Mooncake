#!/usr/bin/env python3
"""Run dataset FeatureHandle-backed EPD serving benchmark.

Input is the JSONL produced by build_dataset_feature_handle_requests.py.  This
runner starts real vLLM Prefill/Decode in feature_handle mode, sends all dataset
requests with streaming enabled, and validates the hot path skips Vision Encoder
(no fallback, no full miss batches, peer-buffer direct KV transfer).
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs  # noqa: E402
from mooncake_epd.scripts.run_vllm_feature_handle_e2e import summarize_feature_handle_metrics  # noqa: E402
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _ensure_process_running,
    _execute_dataset_request,
    _extract_port,
    _launch,
    _stats,
    _tail_text,
    _terminate_all,
    _validate_local_hostname,
    _wait_for_metrics_settle,
    _wait_ready,
)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _iter_feature_handles(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    handles: List[Dict[str, Any]] = []
    for row in rows:
        request = row.get("request") if isinstance(row, dict) else None
        metadata = request.get("metadata") if isinstance(request, dict) else None
        if not isinstance(metadata, dict):
            continue
        raw = (
            metadata.get("mooncake_epd_feature_handles")
            or metadata.get("mm_feature_handles")
            or metadata.get("feature_handles")
            or []
        )
        if isinstance(raw, list):
            handles.extend([dict(item) for item in raw if isinstance(item, dict)])
    return handles


def _store_id_and_key_from_uri(uri: str) -> Tuple[Optional[str], Optional[str]]:
    parsed = urlparse(str(uri or ""))
    if parsed.scheme != "mooncake":
        return None, None
    return parsed.netloc or None, parsed.path.lstrip("/") or None


def _http_store_has_key(store_url: str, key: str, *, timeout_s: float = 5.0) -> bool:
    sess = requests.Session()
    sess.trust_env = False
    try:
        resp = sess.get(
            f"{store_url.rstrip('/')}/api/get/{quote(str(key), safe='')}",
            timeout=timeout_s,
        )
        return resp.status_code == 200 and bool(resp.content)
    except Exception:
        return False
    finally:
        sess.close()


def _infer_feature_handle_store(rows: List[Dict[str, Any]], args: argparse.Namespace) -> Tuple[Optional[str], Optional[str]]:
    """Infer the real Mooncake FeatureBundle store endpoint for vLLM workers.

    Dataset JSONL carries stable ``mooncake://<store_id>/<object_key>`` handles,
    but the EngineCore worker also needs the concrete HTTP store endpoint or a
    Python SDK config.  Without this, vLLM falls into MooncakeDistributedStore
    client retries and eventually re-runs the vision encoder in non-strict
    mode.  This helper is intentionally conservative: it only auto-selects the
    local default service when the first handle can be fetched successfully.
    """

    handles = _iter_feature_handles(rows)
    mooncake_handles = [h for h in handles if str(h.get("uri") or "").startswith("mooncake://")]
    if not mooncake_handles:
        return None, None

    store_ids = set()
    first_key: Optional[str] = None
    for handle in mooncake_handles:
        store_id, key = _store_id_and_key_from_uri(str(handle.get("uri") or ""))
        if store_id:
            store_ids.add(store_id)
        if first_key is None and key:
            first_key = key
    if len(store_ids) > 1:
        raise RuntimeError(f"dataset carries multiple Mooncake feature store ids: {sorted(store_ids)}")
    store_id = next(iter(store_ids), None)

    explicit_url = (
        args.mooncake_store_url
        or os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL")
        or os.getenv("MOONCAKE_STORE_URL")
    )
    if explicit_url:
        return str(explicit_url), store_id
    if args.mooncake_config or os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_CONFIG") or os.getenv("MOONCAKE_CONFIG_PATH"):
        return None, store_id

    default_url = "http://127.0.0.1:8089"
    if first_key and _http_store_has_key(default_url, first_key):
        return default_url, store_id
    raise RuntimeError(
        "dataset contains mooncake:// FeatureHandles but no FeatureBundle store endpoint was configured. "
        "Pass --mooncake-store-url (for example http://127.0.0.1:8089) or --mooncake-config; "
        "strict feature-handle mode will not silently re-run the vision encoder."
    )


def _validate(summary: Dict[str, Any]) -> None:
    failures: List[str] = []
    rows = list(summary.get("results") or [])
    requested = int(summary.get("request_count", 0) or 0)
    metric_summary = dict(summary.get("feature_handle_metric_summary") or {})
    if len(rows) != requested:
        failures.append(f"result_count {len(rows)} != request_count {requested}")
    for row in rows:
        if int(row.get("status_code") or 0) != 200:
            failures.append(f"request {row.get('index')} status={row.get('status_code')} error={row.get('error')}")
        if row.get("routing_path") != "EPD":
            failures.append(f"request {row.get('index')} did not route EPD: {row.get('routing_path')}")
        if int(row.get("response_content_len", 0) or 0) <= 0:
            failures.append(f"request {row.get('index')} empty response")
    if int(metric_summary.get("vision_skip_hits", 0) or 0) <= 0:
        failures.append("no precomputed/hidden-cache vision-skip hit observed")
    if float(metric_summary.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0) != 0.0:
        failures.append("vision_compute_ms_avg != 0")
    if int(metric_summary.get("hidden_cache_full_miss_batches", 0) or 0) != 0:
        failures.append("hidden_cache_full_miss_batches != 0")
    if int(metric_summary.get("hidden_cache_errors", 0) or 0) != 0:
        failures.append("hidden_cache_errors != 0")
    if int(metric_summary.get("fallback_batches", 0) or 0) != 0:
        failures.append("fallback_batches != 0")
    if int(metric_summary.get("fallback_bytes", 0) or 0) != 0:
        failures.append("fallback_bytes != 0")
    if int(metric_summary.get("layered_receive_failures", 0) or 0) != 0:
        failures.append("layered_receive_failures != 0")
    if int(metric_summary.get("layered_transfer_failed_batches", 0) or 0) != 0:
        failures.append("layered_transfer_failed_batches != 0")
    if int(metric_summary.get("peer_buffer_batches", 0) or 0) <= 0:
        failures.append("peer_buffer_batches <= 0")
    backend_counts = dict(metric_summary.get("backend_counts") or {})
    if int(backend_counts.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("peer_buffer_direct <= 0")
    unexpected = {k: v for k, v in backend_counts.items() if k != "peer_buffer_direct" and int(v or 0) != 0}
    if unexpected:
        failures.append(f"unexpected backend counts: {unexpected}")
    if failures:
        raise AssertionError("; ".join(failures))


def run(args: argparse.Namespace) -> Dict[str, Any]:
    requests_path = Path(args.requests_jsonl).expanduser().resolve()
    rows = _load_jsonl(requests_path)
    if not rows:
        raise RuntimeError(f"no requests in {requests_path}")
    if args.max_requests > 0:
        rows = rows[: int(args.max_requests)]
    workdir = Path(args.workdir).expanduser().resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    local_hostname = _validate_local_hostname(args.local_hostname, allow_loopback=True)
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=local_hostname,
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        workflow_registry_wal_path=str(workdir / "proxy_workflow_registry.jsonl"),
        connector_metrics_dir=str(workdir / "connector_metrics"),
        max_group_bytes=int(args.max_group_bytes),
        max_transfer_descriptors=int(args.max_transfer_descriptors),
        max_transfer_bytes=int(args.max_transfer_bytes),
        owner_shards=max(1, int(args.owner_shards)),
        prefill_gpu=int(args.prefill_gpu),
        decode_gpu=int(args.decode_gpu),
        gpu_memory_utilization=float(args.gpu_memory_utilization),
        max_model_len=int(args.max_model_len),
        strict_no_fallback=bool(args.strict_no_fallback),
    )
    store_url, store_id = _infer_feature_handle_store(rows, args)
    cfg.feature_handle_store_url = store_url
    cfg.feature_handle_store_id = args.mooncake_store_id or store_id
    cfg.feature_handle_store_config = args.mooncake_config
    cfg.feature_handle_store_timeout_s = float(args.mooncake_timeout_s)
    cfg.feature_handle_require_checksum = bool(args.require_checksum)
    files = generate_configs(str(workdir), cfg)
    prefill_port = _extract_port(Path(files["prefill"]))
    decode_port = _extract_port(Path(files["decode"]))
    proxy_port = _extract_port(Path(files["proxy"]))
    logs = {k: workdir / f"{k}.log" for k in ("metadata", "master", "prefill", "decode", "proxy")}
    procs: List[subprocess.Popen] = []
    summary: Dict[str, Any] = {
        "requests_jsonl": str(requests_path),
        "workdir": str(workdir),
        "request_count": len(rows),
        "ports": {"prefill": prefill_port, "decode": decode_port, "proxy": proxy_port},
        "logs": {k: str(v) for k, v in logs.items()},
        "feature_handle_store": {
            "url": cfg.feature_handle_store_url,
            "store_id": cfg.feature_handle_store_id,
            "config": cfg.feature_handle_store_config,
            "timeout_s": cfg.feature_handle_store_timeout_s,
            "require_checksum": cfg.feature_handle_require_checksum,
            "strict_no_fallback": cfg.strict_no_fallback,
        },
    }
    try:
        named: Dict[str, subprocess.Popen] = {}
        for key in ("metadata", "master", "prefill", "decode", "proxy"):
            proc = _launch(files[key], logs[key])
            procs.append(proc)
            named[key] = proc
            time.sleep(2.0 if key in {"metadata", "master"} else 4.0)
            _ensure_process_running(key, proc, logs[key])
        timeout_s = max(60.0, float(args.timeout))
        _wait_ready("prefill", f"http://{local_hostname}:{prefill_port}", proc=named["prefill"], log_path=logs["prefill"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("decode", f"http://{local_hostname}:{decode_port}", proc=named["decode"], log_path=logs["decode"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("proxy", f"http://{local_hostname}:{proxy_port}", proc=named["proxy"], log_path=logs["proxy"], timeout_s=min(180.0, timeout_s), paths=("/health", "/healthcheck"))
        proxy_url = f"http://{local_hostname}:{proxy_port}/v1/chat/completions"
        # Warm up with feature-handle requests as well; all warmups are counted in
        # metrics, but result stats below only include benchmark rows.
        for i in range(max(0, int(args.warmup_requests))):
            warm = rows[i % len(rows)]
            res = _execute_dataset_request(proxy_url=proxy_url, entry=warm, index=-1 - i, request_timeout=float(args.request_timeout), stream_metrics=True)
            if int(res.get("status_code") or 0) >= 400 or res.get("error"):
                raise RuntimeError(f"warmup failed: {json.dumps(res, ensure_ascii=False)[:2000]}")
        results: List[Dict[str, Any]] = []
        started = time.perf_counter()
        for idx, row in enumerate(rows):
            res = _execute_dataset_request(proxy_url=proxy_url, entry=row, index=idx, request_timeout=float(args.request_timeout), stream_metrics=True)
            results.append(res)
            if int(res.get("status_code") or 0) >= 400 or res.get("error"):
                raise RuntimeError(f"feature-handle dataset request failed: {json.dumps(res, ensure_ascii=False)[:4000]}")
        elapsed_s = max(1e-6, time.perf_counter() - started)
        sess = requests.Session(); sess.trust_env = False
        try:
            metrics_payload = _wait_for_metrics_settle(sess, f"http://{local_hostname}:{proxy_port}/metrics", timeout_s=30.0, poll_s=0.5)
        finally:
            sess.close()
        lat = [float(r.get("elapsed_ms", 0.0) or 0.0) for r in results]
        ttft = [float(r.get("ttft_ms", 0.0) or 0.0) for r in results if r.get("ttft_ms") is not None]
        tpot = [float(r.get("tpot_ms", 0.0) or 0.0) for r in results if r.get("tpot_ms") is not None]
        summary.update({
            "results": results,
            "latency_stats_ms": _stats(lat),
            "ttft_stats_ms": _stats(ttft),
            "tpot_stats_ms": _stats(tpot),
            "request_throughput_rps": len(results) / elapsed_s,
            "metrics": metrics_payload,
            "feature_handle_metric_summary": summarize_feature_handle_metrics(metrics_payload),
            "connector_metrics_dir": str(workdir / "connector_metrics"),
        })
        _validate(summary)
        return summary
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        summary["log_tails"] = {k: _tail_text(v, 120) for k, v in logs.items()}
        raise
    finally:
        _terminate_all(procs)
        out = workdir / "dataset_feature_handle_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests-jsonl", required=True)
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_dataset_feature_handle")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--max-requests", type=int, default=0)
    ap.add_argument("--warmup-requests", type=int, default=1)
    ap.add_argument("--prefill-gpu", type=int, default=3)
    ap.add_argument("--decode-gpu", type=int, default=4)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--owner-shards", type=int, default=4)
    ap.add_argument("--mooncake-store-url", default=None)
    ap.add_argument("--mooncake-store-id", default=None)
    ap.add_argument("--mooncake-config", default=None)
    ap.add_argument("--mooncake-timeout-s", type=float, default=30.0)
    ap.add_argument("--require-checksum", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=True)
    return ap.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(json.dumps({
        "workdir": summary.get("workdir"),
        "request_count": summary.get("request_count"),
        "latency_stats_ms": summary.get("latency_stats_ms"),
        "ttft_stats_ms": summary.get("ttft_stats_ms"),
        "tpot_stats_ms": summary.get("tpot_stats_ms"),
        "feature_handle_metric_summary": summary.get("feature_handle_metric_summary"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
