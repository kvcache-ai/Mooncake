#!/usr/bin/env python3
"""Run a real vLLM EPD FeatureHandle hidden-state E2E validation.

This runner formalizes the previously manual flow:

1. Build or reuse a FeatureHandle-backed OpenAI chat request by running the real
   Qwen-VL vision encoder in a short-lived process.
2. Start real vLLM Prefill/Decode servers with MooncakeConnector and the proxy in
   ``--mm-prefetch-mode feature_handle``.
3. Send the request through the proxy.
4. Gate success on metrics that prove the Prefill hot path consumed external
   hidden states and skipped the vision encoder:
   ``mm_hidden_cache_precomputed_image_embeds_hits >= 1`` and
   ``mm_hidden_cache_vision_compute_ms_avg == 0`` for the feature-handle request.

The script is intentionally not a mock benchmark.  It launches real services and
always tears them down on exit.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs  # noqa: E402
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _ensure_process_running,
    _extract_choice_text,
    _extract_port,
    _launch,
    _proc_env,
    _tail_text,
    _terminate_all,
    _wait_for_metrics_settle,
    _wait_ready,
)


def _session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _run_build_request(args: argparse.Namespace, request_path: Path, store_dir: Path) -> Dict[str, Any]:
    if args.request:
        request = json.loads(Path(args.request).expanduser().read_text(encoding="utf-8"))
        request_path.parent.mkdir(parents=True, exist_ok=True)
        request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "mode": "reuse_request",
            "request": str(Path(args.request).expanduser()),
            "copied_request": str(request_path),
        }

    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "build_qwen_vl_feature_handle_request.py"),
        "--model",
        str(args.model),
        "--device",
        str(args.encoder_device),
        "--store-dir",
        str(store_dir),
        "--output",
        str(request_path),
        "--prompt",
        str(args.prompt),
        "--workflow-id",
        str(args.workflow_id),
        "--max-tokens",
        str(args.max_tokens),
        "--temperature",
        str(args.temperature),
        "--demo-image",
        str(args.demo_image),
    ]
    if args.image_path:
        cmd.extend(["--image-path", str(args.image_path)])
    if args.checksum:
        cmd.append("--checksum")
    started = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=_proc_env(),
        text=True,
        capture_output=True,
        timeout=max(60.0, float(args.encoder_timeout)),
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if proc.returncode != 0:
        raise RuntimeError(
            "feature-handle request builder failed\n"
            f"cmd={' '.join(cmd)}\nstdout={proc.stdout[-4000:]}\nstderr={proc.stderr[-4000:]}"
        )
    builder_summary: Dict[str, Any] = {
        "mode": "build_request",
        "cmd": cmd,
        "elapsed_ms": elapsed_ms,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }
    try:
        builder_summary["builder_json"] = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:
        pass
    return builder_summary


def _post_feature_handle_request(proxy_url: str, request_path: Path, *, timeout_s: float) -> Dict[str, Any]:
    body = json.loads(request_path.read_text(encoding="utf-8"))
    sess = _session()
    started = time.perf_counter()
    try:
        resp = sess.post(
            f"{proxy_url}/v1/chat/completions",
            json=body,
            timeout=max(1.0, float(timeout_s)),
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        result: Dict[str, Any] = {
            "status_code": resp.status_code,
            "elapsed_ms": elapsed_ms,
            "headers": {
                key: resp.headers.get(key)
                for key in [
                    "x-request-id",
                    "x-epd-routing-path",
                    "x-epd-admission",
                    "x-epd-degrade-level",
                ]
            },
            "response_head": resp.text[:2000],
        }
        try:
            payload = resp.json()
            result["response_json"] = payload
            choices = list(payload.get("choices") or [])
            if choices:
                choice = dict(choices[0] or {})
                text = _extract_choice_text(choice)
                result["response_text"] = text
                result["response_content_len"] = len(text)
                result["finish_reason"] = choice.get("finish_reason")
            result["usage"] = payload.get("usage")
        except Exception as exc:
            result["response_parse_error"] = f"{type(exc).__name__}: {exc}"
        if resp.status_code >= 400:
            raise RuntimeError(f"feature-handle request failed: status={resp.status_code} body={resp.text[:2000]}")
        return result
    finally:
        sess.close()


def summarize_feature_handle_metrics(metrics_payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = dict(metrics_payload.get("metrics") or {})
    precomputed_hits = int(metrics.get("mm_hidden_cache_precomputed_image_embeds_hits", 0) or 0)
    hidden_cache_hits = int(metrics.get("mm_hidden_cache_hits", 0) or 0)
    native_encoder_cache_hits = int(metrics.get("mm_hidden_cache_native_encoder_cache_hits", 0) or 0)
    return {
        "requests_total": int(metrics.get("requests_total", 0) or 0),
        "requests_multimodal": int(metrics.get("requests_multimodal", 0) or 0),
        "requests_text": int(metrics.get("requests_text", 0) or 0),
        "mm_prefetch_attempted": int(metrics.get("mm_prefetch_attempted", 0) or 0),
        "mm_prefetch_completed": int(metrics.get("mm_prefetch_completed", 0) or 0),
        "precomputed_hits": precomputed_hits,
        "hidden_cache_hits": hidden_cache_hits,
        "native_encoder_cache_hits": native_encoder_cache_hits,
        "vision_skip_hits": max(precomputed_hits, hidden_cache_hits + native_encoder_cache_hits),
        "hidden_cache_errors": int(metrics.get("mm_hidden_cache_errors", 0) or 0),
        "hidden_cache_full_miss_batches": int(metrics.get("mm_hidden_cache_full_miss_batches", 0) or 0),
        "hidden_cache_vision_compute_ms_avg": float(metrics.get("mm_hidden_cache_vision_compute_ms_avg", 0.0) or 0.0),
        "fallback_batches": int(metrics.get("fallback_batches", 0) or 0),
        "fallback_bytes": int(metrics.get("fallback_bytes", 0) or 0),
        "layered_receive_failures": int(metrics.get("layered_receive_failures", 0) or 0),
        "layered_transfer_failed_batches": int(metrics.get("layered_transfer_failed_batches", 0) or 0),
        "peer_buffer_batches": int(metrics.get("peer_buffer_batches", 0) or 0),
        "peer_buffer_bytes": int(metrics.get("peer_buffer_bytes", 0) or 0),
        "peer_buffer_dispatch_ms": float(metrics.get("peer_buffer_dispatch_ms", 0.0) or 0.0),
        "peer_buffer_dispatch_ms_avg": float(metrics.get("peer_buffer_dispatch_ms_avg", 0.0) or 0.0),
        "peer_buffer_prepare_ms": float(metrics.get("peer_buffer_prepare_ms", 0.0) or 0.0),
        "peer_buffer_prepare_ms_avg": float(metrics.get("peer_buffer_prepare_ms_avg", 0.0) or 0.0),
        "peer_buffer_write_ms": float(metrics.get("peer_buffer_write_ms", 0.0) or 0.0),
        "peer_buffer_write_ms_avg": float(metrics.get("peer_buffer_write_ms_avg", 0.0) or 0.0),
        "peer_buffer_write_bandwidth_gbps": float(metrics.get("peer_buffer_write_bandwidth_gbps", 0.0) or 0.0),
        "layered_receive_kv_requests": int(metrics.get("layered_receive_kv_requests", 0) or 0),
        "layered_receive_kv_worker_roundtrips": int(metrics.get("layered_receive_kv_worker_roundtrips", 0) or 0),
        "layered_receive_kv_worker_ms_avg": float(metrics.get("layered_receive_kv_worker_ms_avg", 0.0) or 0.0),
        "layered_receive_kv_response_messages": int(metrics.get("layered_receive_kv_response_messages", 0) or 0),
        "layered_receive_kv_first_response_count": int(metrics.get("layered_receive_kv_first_response_count", 0) or 0),
        "layered_receive_kv_first_response_ms_avg": float(metrics.get("layered_receive_kv_first_response_ms_avg", 0.0) or 0.0),
        "layered_receive_kv_last_response_count": int(metrics.get("layered_receive_kv_last_response_count", 0) or 0),
        "layered_receive_kv_last_response_ms_avg": float(metrics.get("layered_receive_kv_last_response_ms_avg", 0.0) or 0.0),
        "layered_receive_kv_response_process_count": int(metrics.get("layered_receive_kv_response_process_count", 0) or 0),
        "layered_receive_kv_response_process_ms_avg": float(metrics.get("layered_receive_kv_response_process_ms_avg", 0.0) or 0.0),
        "layered_receive_kv_first_group_count": int(metrics.get("layered_receive_kv_first_group_count", 0) or 0),
        "layered_receive_kv_first_group_ms_avg": float(metrics.get("layered_receive_kv_first_group_ms_avg", 0.0) or 0.0),
        "layered_receive_kv_finished_count": int(metrics.get("layered_receive_kv_finished_count", 0) or 0),
        "layered_receive_kv_finished_ms_avg": float(metrics.get("layered_receive_kv_finished_ms_avg", 0.0) or 0.0),
        "backend_counts": dict(metrics.get("remote_transfer_backend_counts") or {}),
        "decode_engine_timing_workers": int(metrics.get("decode_engine_timing_workers", 0) or 0),
        "decode_engine_first_token_requests": int(metrics.get("decode_engine_first_token_requests", 0) or 0),
        "decode_engine_first_token_latency_ms_total": float(metrics.get("decode_engine_first_token_latency_ms_total", 0.0) or 0.0),
        "decode_engine_first_token_latency_ms_avg": float(metrics.get("decode_engine_first_token_latency_ms_avg", 0.0) or 0.0),
        "decode_engine_kv_first_token_requests": int(metrics.get("decode_engine_kv_first_token_requests", 0) or 0),
        "decode_engine_kv_first_token_latency_ms_total": float(metrics.get("decode_engine_kv_first_token_latency_ms_total", 0.0) or 0.0),
        "decode_engine_kv_first_token_latency_ms_avg": float(metrics.get("decode_engine_kv_first_token_latency_ms_avg", 0.0) or 0.0),
        "decode_engine_kv_first_token_output_tokens": int(metrics.get("decode_engine_kv_first_token_output_tokens", 0) or 0),
        "decode_engine_scheduler_update_calls": int(metrics.get("decode_engine_scheduler_update_calls", 0) or 0),
    }


def validate_feature_handle_summary(summary: Dict[str, Any]) -> None:
    failures: List[str] = []
    response = dict(summary.get("response") or {})
    metric_summary = dict(summary.get("feature_handle_metric_summary") or {})
    headers = dict(response.get("headers") or {})

    if int(response.get("status_code", 0) or 0) != 200:
        failures.append(f"HTTP status is not 200: {response.get('status_code')}")
    if headers.get("x-epd-routing-path") != "EPD":
        failures.append(f"request did not route through EPD: {headers.get('x-epd-routing-path')}")
    if int(response.get("response_content_len", 0) or 0) <= 0:
        failures.append("response content is empty")
    if int(metric_summary.get("requests_multimodal", 0) or 0) < 1:
        failures.append("requests_multimodal < 1")
    if int(metric_summary.get("precomputed_hits", 0) or 0) < 1:
        failures.append("mm_hidden_cache_precomputed_image_embeds_hits < 1")
    if int(metric_summary.get("hidden_cache_errors", 0) or 0) != 0:
        failures.append("mm_hidden_cache_errors != 0")
    if int(metric_summary.get("hidden_cache_full_miss_batches", 0) or 0) != 0:
        failures.append("hidden cache full miss batches observed; vision encoder likely ran")
    if float(metric_summary.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0) != 0.0:
        failures.append("vision_compute_ms_avg != 0 for feature-handle request")
    if int(metric_summary.get("mm_prefetch_attempted", 0) or 0) != 0:
        failures.append("feature_handle mode should not run asset-bytes MM prefetch")
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
        failures.append("peer_buffer_direct backend count <= 0")
    unexpected = {
        str(k): v
        for k, v in backend_counts.items()
        if str(k) != "peer_buffer_direct" and int(v or 0) != 0
    }
    if unexpected:
        failures.append(f"unexpected transfer backend counts: {unexpected}")
    if failures:
        raise AssertionError("; ".join(failures))


def run(args: argparse.Namespace) -> Dict[str, Any]:
    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    request_path = workdir / "feature_handle_request.json"
    store_dir = Path(args.store_dir).expanduser() if args.store_dir else workdir / "feature_handle_store"
    if not args.keep_feature_store and store_dir.exists() and store_dir.is_dir() and store_dir.is_relative_to(workdir):
        import shutil

        shutil.rmtree(store_dir)

    builder = _run_build_request(args, request_path, store_dir)

    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=str(args.local_hostname),
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        workflow_registry_wal_path=str(workdir / "proxy_workflow_registry.jsonl"),
        connector_metrics_dir=str(workdir / "connector_metrics"),
        max_group_bytes=int(args.max_group_bytes),
        max_transfer_descriptors=int(args.max_transfer_descriptors),
        max_transfer_bytes=int(args.max_transfer_bytes),
        owner_shards=max(1, int(args.owner_shards)),
        kv_directory_rpc_url=args.kv_directory_rpc_url,
        prefill_gpu=int(args.prefill_gpu),
        decode_gpu=int(args.decode_gpu),
        gpu_memory_utilization=float(args.gpu_memory_utilization),
        max_model_len=int(args.max_model_len),
    )
    files = generate_configs(str(workdir), cfg)
    prefill_port = _extract_port(Path(files["prefill"]))
    decode_port = _extract_port(Path(files["decode"]))
    proxy_port = _extract_port(Path(files["proxy"]))

    logs = {
        "metadata": workdir / "metadata.log",
        "master": workdir / "master.log",
        "prefill": workdir / "prefill.log",
        "decode": workdir / "decode.log",
        "proxy": workdir / "proxy.log",
    }
    procs: List[subprocess.Popen] = []
    summary: Dict[str, Any] = {
        "workdir": str(workdir),
        "request_path": str(request_path),
        "feature_store_dir": str(store_dir),
        "builder": builder,
        "ports": {"prefill": prefill_port, "decode": decode_port, "proxy": proxy_port},
        "logs": {k: str(v) for k, v in logs.items()},
    }
    try:
        named_procs: Dict[str, subprocess.Popen] = {}
        for key in ("metadata", "master", "prefill", "decode", "proxy"):
            proc = _launch(files[key], logs[key])
            procs.append(proc)
            named_procs[key] = proc
            time.sleep(2.0 if key in {"metadata", "master"} else 4.0)
            _ensure_process_running(key, proc, logs[key])

        timeout_s = max(60.0, float(args.timeout))
        _wait_ready(
            "prefill",
            f"http://{args.local_hostname}:{prefill_port}",
            proc=named_procs["prefill"],
            log_path=logs["prefill"],
            timeout_s=min(300.0, timeout_s),
            paths=("/health",),
        )
        _wait_ready(
            "decode",
            f"http://{args.local_hostname}:{decode_port}",
            proc=named_procs["decode"],
            log_path=logs["decode"],
            timeout_s=min(300.0, timeout_s),
            paths=("/health",),
        )
        _wait_ready(
            "proxy",
            f"http://{args.local_hostname}:{proxy_port}",
            proc=named_procs["proxy"],
            log_path=logs["proxy"],
            timeout_s=min(180.0, timeout_s),
            paths=("/health", "/healthcheck"),
        )

        proxy_url = f"http://{args.local_hostname}:{proxy_port}"
        response = _post_feature_handle_request(
            proxy_url,
            request_path,
            timeout_s=float(args.request_timeout),
        )
        summary["response"] = response

        sess = _session()
        try:
            metrics_payload = _wait_for_metrics_settle(
                sess,
                f"{proxy_url}/metrics",
                timeout_s=20.0,
                poll_s=0.5,
            )
        finally:
            sess.close()
        summary["metrics"] = metrics_payload
        summary["feature_handle_metric_summary"] = summarize_feature_handle_metrics(metrics_payload)
        summary["workflow_registry_wal"] = str(workdir / "proxy_workflow_registry.jsonl")
        summary["connector_metrics_dir"] = str(workdir / "connector_metrics")
        validate_feature_handle_summary(summary)
        return summary
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        summary["log_tails"] = {key: _tail_text(path, 120) for key, path in logs.items()}
        raise
    finally:
        _terminate_all(procs)
        out = workdir / "feature_handle_e2e_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real vLLM FeatureHandle EPD serving E2E validation.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_feature_handle_e2e")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--request", default=None, help="Reuse an existing FeatureHandle-backed request JSON.")
    ap.add_argument("--store-dir", default=None, help="FeatureBundle store dir; defaults to <workdir>/feature_handle_store.")
    ap.add_argument("--keep-feature-store", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--encoder-device", default="cuda:5")
    ap.add_argument("--encoder-timeout", type=float, default=420.0)
    ap.add_argument("--prefill-gpu", type=int, default=3)
    ap.add_argument("--decode-gpu", type=int, default=4)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--prompt", default="Describe the image briefly.")
    ap.add_argument("--workflow-id", default="feature-handle-e2e")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-path", default=None)
    ap.add_argument("--checksum", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    out = Path(args.workdir).expanduser() / "feature_handle_e2e_summary.json"
    print(json.dumps({
        "summary": str(out),
        "response": summary.get("response", {}),
        "feature_handle_metric_summary": summary.get("feature_handle_metric_summary", {}),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
