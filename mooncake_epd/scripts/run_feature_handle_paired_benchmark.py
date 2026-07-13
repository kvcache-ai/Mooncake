#!/usr/bin/env python3
"""Run a real paired EPD benchmark: asset-bytes multimodal vs FeatureHandle hidden-state.

The runner is intentionally sequential and non-mock:

1. Reuse one FeatureHandle-backed OpenAI chat request.  The request still
   contains the original image_url, so removing the control-plane handle yields
   an equivalent asset-bytes multimodal request.
2. Start a real vLLM EPD stack in ``asset_bytes`` mode, send the asset request
   with streaming enabled, collect TTFT/elapsed and serving metrics, then tear it
   down.
3. Start a real vLLM EPD stack in ``feature_handle`` mode, send the original
   request, collect TTFT/elapsed and metrics, then tear it down.
4. Emit a paired summary proving whether FeatureHandle skipped Prefill vision
   encoder work and what happened to TTFT / goodput for the exact same prompt.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs  # noqa: E402
from mooncake_epd.scripts.run_vllm_feature_handle_e2e import (  # noqa: E402
    summarize_feature_handle_metrics,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _ensure_process_running,
    _execute_dataset_request,
    _extract_port,
    _proc_env,
    _tail_text,
    _terminate_all,
    _wait_for_metrics_settle,
    _wait_ready,
)
from mooncake_epd.scripts.run_serving_baseline_compare import _string_metrics  # noqa: E402


def _session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _launch(script_path: str, log_path: Path) -> subprocess.Popen:
    with log_path.open("wb") as fh:
        return subprocess.Popen(
            ["bash", script_path],
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(REPO_ROOT),
            env=_proc_env(),
            preexec_fn=__import__("os").setsid,
        )


def _strip_feature_handles(request: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(request)
    metadata = dict(payload.get("metadata") or {})
    metadata.pop("mooncake_epd_feature_handles", None)
    metadata.pop("feature_handle_builder", None)
    base_workflow = str(metadata.get("workflow_id") or "paired")
    metadata["workflow_id"] = f"{base_workflow}-asset-bytes"
    payload["metadata"] = metadata
    return payload


def _feature_request(request: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(request)
    metadata = dict(payload.get("metadata") or {})
    base_workflow = str(metadata.get("workflow_id") or "paired")
    metadata["workflow_id"] = f"{base_workflow}-feature-handle"
    payload["metadata"] = metadata
    return payload


def _entry_for_request(request: Dict[str, Any], *, mode: str) -> Dict[str, Any]:
    metadata = dict(request.get("metadata") or {})
    return {
        "request": request,
        "sample": {
            "sample_id": f"paired-{mode}",
            "workflow_id": metadata.get("workflow_id", f"paired-{mode}"),
            "source_dataset": "feature_handle_paired_benchmark",
        },
        "family": "feature_handle_paired",
    }


def _start_stack(args: argparse.Namespace, *, mode: str, workdir: Path) -> Dict[str, Any]:
    _cleanup_previous_run_artifacts(workdir)
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=str(args.local_hostname),
        mm_prefetch_mode=mode,
        prefill_supports_feature_handles=(mode == "feature_handle"),
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
        strict_no_fallback=(mode == "feature_handle" and bool(args.strict_no_fallback)),
        feature_handle_store_url=(
            args.mooncake_store_url
            or os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL")
            or os.getenv("MOONCAKE_STORE_URL")
        ),
        feature_handle_store_id=args.mooncake_store_id,
        feature_handle_store_config=args.mooncake_config,
        feature_handle_store_timeout_s=float(args.mooncake_timeout_s),
        feature_handle_require_checksum=bool(args.require_checksum),
    )
    files = generate_configs(str(workdir), cfg)
    ports = {
        "prefill": _extract_port(Path(files["prefill"])),
        "decode": _extract_port(Path(files["decode"])),
        "proxy": _extract_port(Path(files["proxy"])),
    }
    logs = {
        "metadata": workdir / "metadata.log",
        "master": workdir / "master.log",
        "prefill": workdir / "prefill.log",
        "decode": workdir / "decode.log",
        "proxy": workdir / "proxy.log",
    }
    procs: List[subprocess.Popen] = []
    named: Dict[str, subprocess.Popen] = {}
    for key in ("metadata", "master", "prefill", "decode", "proxy"):
        proc = _launch(files[key], logs[key])
        procs.append(proc)
        named[key] = proc
        time.sleep(2.0 if key in {"metadata", "master"} else 4.0)
        _ensure_process_running(f"{mode}:{key}", proc, logs[key])
    timeout_s = max(60.0, float(args.timeout))
    _wait_ready(
        f"{mode}:prefill",
        f"http://{args.local_hostname}:{ports['prefill']}",
        proc=named["prefill"],
        log_path=logs["prefill"],
        timeout_s=min(300.0, timeout_s),
        paths=("/health",),
    )
    _wait_ready(
        f"{mode}:decode",
        f"http://{args.local_hostname}:{ports['decode']}",
        proc=named["decode"],
        log_path=logs["decode"],
        timeout_s=min(300.0, timeout_s),
        paths=("/health",),
    )
    _wait_ready(
        f"{mode}:proxy",
        f"http://{args.local_hostname}:{ports['proxy']}",
        proc=named["proxy"],
        log_path=logs["proxy"],
        timeout_s=min(180.0, timeout_s),
        paths=("/health", "/healthcheck"),
    )
    return {
        "files": files,
        "ports": ports,
        "logs": logs,
        "procs": procs,
    }


def _run_one_mode(
    args: argparse.Namespace,
    *,
    mode: str,
    request: Dict[str, Any],
    workdir: Path,
) -> Dict[str, Any]:
    stack: Dict[str, Any] | None = None
    summary: Dict[str, Any] = {"mode": mode, "workdir": str(workdir)}
    try:
        workdir.mkdir(parents=True, exist_ok=True)
        (workdir / "request.json").write_text(
            json.dumps(request, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stack = _start_stack(args, mode=mode, workdir=workdir)
        proxy_url = f"http://{args.local_hostname}:{stack['ports']['proxy']}/v1/chat/completions"
        entry = _entry_for_request(request, mode=mode)
        started = time.perf_counter()
        result = _execute_dataset_request(
            proxy_url=proxy_url,
            entry=entry,
            index=0,
            request_timeout=float(args.request_timeout),
            stream_metrics=True,
        )
        benchmark_elapsed_s = max(1e-6, time.perf_counter() - started)
        if int(result.get("status_code") or 0) >= 400 or result.get("error"):
            raise RuntimeError(f"{mode} request failed: {json.dumps(result, ensure_ascii=False)[:4000]}")
        sess = _session()
        try:
            metrics_payload = _wait_for_metrics_settle(
                sess,
                f"http://{args.local_hostname}:{stack['ports']['proxy']}/metrics",
                timeout_s=20.0,
                poll_s=0.5,
            )
        finally:
            sess.close()
        metric_summary = summarize_feature_handle_metrics(metrics_payload)
        summary.update(
            {
                "ports": dict(stack["ports"]),
                "logs": {name: str(path) for name, path in dict(stack["logs"]).items()},
                "result": result,
                "benchmark_elapsed_s": benchmark_elapsed_s,
                "goodput_rps": 1.0 / benchmark_elapsed_s,
                "metrics": metrics_payload,
                "metric_summary": metric_summary,
                "workflow_registry_wal": str(Path(stack["files"]["proxy_workflow_registry"])),
                "connector_metrics_dir": str(stack["files"]["connector_metrics_dir"]),
            }
        )
        return summary
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        if stack:
            summary["log_tails"] = {
                name: _tail_text(path, 120)
                for name, path in dict(stack.get("logs") or {}).items()
            }
        raise
    finally:
        if stack:
            _terminate_all(list(stack.get("procs") or []))
        (workdir / f"{mode}_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _avg(values: List[float]) -> float:
    return float(statistics.fmean(values)) if values else 0.0


def build_paired_summary(asset: Dict[str, Any], feature: Dict[str, Any]) -> Dict[str, Any]:
    asset_result = dict(asset.get("result") or {})
    feature_result = dict(feature.get("result") or {})
    asset_metrics = dict(asset.get("metric_summary") or {})
    feature_metrics = dict(feature.get("metric_summary") or {})
    asset_text = str(asset_result.get("response_text") or "")
    feature_text = str(feature_result.get("response_text") or "")
    text_metrics = _string_metrics(asset_text, feature_text)
    asset_ttft = float(asset_result.get("ttft_ms", 0.0) or 0.0)
    feature_ttft = float(feature_result.get("ttft_ms", 0.0) or 0.0)
    asset_elapsed = float(asset_result.get("elapsed_ms", 0.0) or 0.0)
    feature_elapsed = float(feature_result.get("elapsed_ms", 0.0) or 0.0)
    asset_goodput = float(asset.get("goodput_rps", 0.0) or 0.0)
    feature_goodput = float(feature.get("goodput_rps", 0.0) or 0.0)
    backend_counts = dict(feature_metrics.get("backend_counts") or {})
    gates = {
        "asset_http_ok": int(asset_result.get("status_code", 0) or 0) == 200,
        "feature_http_ok": int(feature_result.get("status_code", 0) or 0) == 200,
        "asset_epd_route_ok": asset_result.get("routing_path") == "EPD",
        "feature_epd_route_ok": feature_result.get("routing_path") == "EPD",
        "feature_precomputed_hit_ok": int(feature_metrics.get("precomputed_hits", 0) or 0) >= 1,
        "feature_vision_compute_skipped": float(feature_metrics.get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0) == 0.0,
        "feature_full_miss_zero": int(feature_metrics.get("hidden_cache_full_miss_batches", 0) or 0) == 0,
        "feature_fallback_zero": int(feature_metrics.get("fallback_batches", 0) or 0) == 0,
        "feature_transfer_failures_zero": int(feature_metrics.get("layered_receive_failures", 0) or 0) == 0
        and int(feature_metrics.get("layered_transfer_failed_batches", 0) or 0) == 0,
        "feature_peer_buffer_direct_ok": int(backend_counts.get("peer_buffer_direct", 0) or 0) > 0,
        "same_finish_reason": str(asset_result.get("finish_reason") or "") == str(feature_result.get("finish_reason") or ""),
        "non_empty_outputs": len(asset_text) > 0 and len(feature_text) > 0,
    }
    failed_gates = [key for key, ok in gates.items() if not ok]
    return {
        "available": True,
        "pass": not failed_gates,
        "failed_gates": failed_gates,
        "gates": gates,
        "asset_ttft_ms": asset_ttft,
        "feature_ttft_ms": feature_ttft,
        "ttft_delta_ms": feature_ttft - asset_ttft,
        "ttft_reduction_pct": ((asset_ttft - feature_ttft) / asset_ttft * 100.0) if asset_ttft > 0 else 0.0,
        "asset_elapsed_ms": asset_elapsed,
        "feature_elapsed_ms": feature_elapsed,
        "elapsed_delta_ms": feature_elapsed - asset_elapsed,
        "elapsed_reduction_pct": ((asset_elapsed - feature_elapsed) / asset_elapsed * 100.0) if asset_elapsed > 0 else 0.0,
        "asset_goodput_rps": asset_goodput,
        "feature_goodput_rps": feature_goodput,
        "goodput_gain_pct": ((feature_goodput - asset_goodput) / asset_goodput * 100.0) if asset_goodput > 0 else 0.0,
        "asset_metric_summary": asset_metrics,
        "feature_metric_summary": feature_metrics,
        "text_metrics": text_metrics,
        "asset_result": {
            "status_code": asset_result.get("status_code"),
            "routing_path": asset_result.get("routing_path"),
            "ttft_ms": asset_result.get("ttft_ms"),
            "elapsed_ms": asset_result.get("elapsed_ms"),
            "tpot_ms": asset_result.get("tpot_ms"),
            "finish_reason": asset_result.get("finish_reason"),
            "usage": asset_result.get("usage"),
            "response_head": asset_result.get("response_head"),
        },
        "feature_result": {
            "status_code": feature_result.get("status_code"),
            "routing_path": feature_result.get("routing_path"),
            "ttft_ms": feature_result.get("ttft_ms"),
            "elapsed_ms": feature_result.get("elapsed_ms"),
            "tpot_ms": feature_result.get("tpot_ms"),
            "finish_reason": feature_result.get("finish_reason"),
            "usage": feature_result.get("usage"),
            "response_head": feature_result.get("response_head"),
        },
    }


def run(args: argparse.Namespace) -> Dict[str, Any]:
    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)
    source_request = json.loads(Path(args.request).expanduser().read_text(encoding="utf-8"))
    asset_request = _strip_feature_handles(source_request)
    feature_request = _feature_request(source_request)
    (workdir / "asset_request.json").write_text(json.dumps(asset_request, ensure_ascii=False, indent=2), encoding="utf-8")
    (workdir / "feature_request.json").write_text(json.dumps(feature_request, ensure_ascii=False, indent=2), encoding="utf-8")

    asset = _run_one_mode(
        args,
        mode="asset_bytes",
        request=asset_request,
        workdir=workdir / "asset_bytes",
    )
    # Give CUDA/vLLM processes a short grace window to release ports/GPU memory
    # before starting the second stack on the same devices.
    time.sleep(max(0.0, float(args.between_mode_sleep_s)))
    feature = _run_one_mode(
        args,
        mode="feature_handle",
        request=feature_request,
        workdir=workdir / "feature_handle",
    )
    paired = build_paired_summary(asset, feature)
    summary = {
        "workdir": str(workdir),
        "request": str(Path(args.request).expanduser()),
        "asset": asset,
        "feature_handle": feature,
        "paired_summary": paired,
    }
    out = Path(args.output).expanduser() if args.output else workdir / "feature_handle_paired_benchmark.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real paired asset-bytes vs FeatureHandle EPD benchmark.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_feature_handle_paired")
    ap.add_argument("--output", default=None)
    ap.add_argument("--request", required=True, help="FeatureHandle-backed OpenAI request JSON.")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--prefill-gpu", type=int, default=3)
    ap.add_argument("--decode-gpu", type=int, default=4)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--max-transfer-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    ap.add_argument("--between-mode-sleep-s", type=float, default=8.0)
    ap.add_argument("--mooncake-store-url", default=None)
    ap.add_argument("--mooncake-store-id", default=None)
    ap.add_argument("--mooncake-config", default=None)
    ap.add_argument("--mooncake-timeout-s", type=float, default=30.0)
    ap.add_argument("--require-checksum", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--strict-no-fallback", action=argparse.BooleanOptionalAction, default=True)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    out = Path(args.output).expanduser() if args.output else Path(args.workdir).expanduser() / "feature_handle_paired_benchmark.json"
    print(json.dumps({
        "summary": str(out),
        "paired_summary": summary.get("paired_summary", {}),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
