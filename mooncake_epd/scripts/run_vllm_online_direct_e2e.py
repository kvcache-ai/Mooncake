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
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import (  # noqa: E402
    MODEL_PATH,
    VENV_ROOT,
    VLLMDisaggConfig,
    _common_env_block,
    _pick_free_port,
    generate_configs,
)
from mooncake_epd.scripts.run_vllm_feature_handle_e2e import (  # noqa: E402
    _post_feature_handle_request,
    summarize_feature_handle_metrics,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _data_url_for_demo_image,
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


def _write_online_request(args: argparse.Namespace, request_path: Path) -> None:
    image_url = str(args.image_url or "")
    if not image_url:
        image_url = _data_url_for_demo_image(str(args.demo_image))
    body = {
        "model": str(args.model),
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": image_url}},
                    {"type": "text", "text": str(args.prompt)},
                ],
            }
        ],
        "max_tokens": int(args.max_tokens),
        "temperature": float(args.temperature),
        "metadata": {"workflow_id": str(args.workflow_id)},
    }
    request_path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")


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
        + "export MOONCAKE_EPD_DIRECT_SOURCE_MODE=managed_buffer\n"
        + f"{VENV_ROOT}/bin/python {REPO_ROOT / 'scripts' / 'epd_encoder_service.py'} "
        + f"--model {args.model} "
        + f"--device {args.encoder_device} "
        + f"--dtype {args.encoder_dtype} "
        + f"--encoder-family {args.encoder_family} "
        + "--publish-backend direct_engine "
        + "--direct-source-mode managed_buffer "
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
    if headers.get("x-epd-routing-path") != "EPD":
        failures.append(f"request did not route through EPD: {headers.get('x-epd-routing-path')}")
    if int(response.get("response_content_len", 0) or 0) <= 0:
        failures.append("response content is empty")
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
    if int(metrics.get("direct_buffer_allocations", 0) or 0) != 0:
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

    encoder_port = _pick_free_port(int(args.encoder_port), str(args.local_hostname))
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=str(args.local_hostname),
        mm_prefetch_mode="feature_handle",
        prefill_supports_feature_handles=True,
        enable_prefill_direct_feature_buffer_routes=True,
        encoder_service_url=f"http://{args.local_hostname}:{encoder_port}",
        strict_no_fallback=True,
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
    mooncake_json = Path(files["mooncake_json"])
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
    prefill_port = _extract_port(Path(files["prefill"]))
    decode_port = _extract_port(Path(files["decode"]))
    proxy_port = _extract_port(Path(files["proxy"]))

    logs = {
        "metadata": workdir / "metadata.log",
        "master": workdir / "master.log",
        "encoder": workdir / "encoder.log",
        "prefill": workdir / "prefill.log",
        "decode": workdir / "decode.log",
        "proxy": workdir / "proxy.log",
    }
    procs: List[subprocess.Popen] = []
    summary: Dict[str, Any] = {
        "workdir": str(workdir),
        "request_path": str(request_path),
        "ports": {
            "encoder": encoder_port,
            "prefill": prefill_port,
            "decode": decode_port,
            "proxy": proxy_port,
        },
        "logs": {k: str(v) for k, v in logs.items()},
        "strict_no_fallback": True,
    }
    try:
        named: Dict[str, subprocess.Popen] = {}
        for key in ("metadata", "master", "encoder", "prefill", "decode", "proxy"):
            proc = _launch(files[key], logs[key])
            procs.append(proc)
            named[key] = proc
            time.sleep(2.0 if key in {"metadata", "master", "encoder"} else 4.0)
            _ensure_process_running(key, proc, logs[key])

        timeout_s = max(60.0, float(args.timeout))
        _wait_ready("encoder", f"http://{args.local_hostname}:{encoder_port}", proc=named["encoder"], log_path=logs["encoder"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("prefill", f"http://{args.local_hostname}:{prefill_port}", proc=named["prefill"], log_path=logs["prefill"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("decode", f"http://{args.local_hostname}:{decode_port}", proc=named["decode"], log_path=logs["decode"], timeout_s=min(300.0, timeout_s), paths=("/health",))
        _wait_ready("proxy", f"http://{args.local_hostname}:{proxy_port}", proc=named["proxy"], log_path=logs["proxy"], timeout_s=min(180.0, timeout_s), paths=("/health", "/healthcheck"))

        proxy_url = f"http://{args.local_hostname}:{proxy_port}"
        response = _post_feature_handle_request(
            proxy_url,
            request_path,
            timeout_s=float(args.request_timeout),
        )
        summary["response"] = response
        sess = _session()
        try:
            metrics_payload = _wait_for_metrics_settle(sess, f"{proxy_url}/metrics", timeout_s=20.0, poll_s=0.5)
        finally:
            sess.close()
        direct_stats = _direct_buffer_stats(f"http://{args.local_hostname}:{prefill_port}")
        summary["metrics"] = metrics_payload
        summary["direct_buffer_stats_after_release"] = direct_stats
        summary["online_direct_metric_summary"] = summarize_online_direct_metrics(
            metrics_payload,
            direct_buffer_stats=direct_stats,
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
    ap.add_argument("--model", default=MODEL_PATH)
    ap.add_argument("--encoder-device", default="cuda:5")
    ap.add_argument("--encoder-dtype", default="bfloat16")
    ap.add_argument("--encoder-family", choices=["auto", "qwen3_vl", "qwen2_5_omni"], default="auto")
    ap.add_argument("--encoder-port", type=int, default=8330)
    ap.add_argument("--encoder-request-timeout-s", type=float, default=30.0)
    ap.add_argument("--prefill-gpu", type=int, default=3)
    ap.add_argument("--decode-gpu", type=int, default=4)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--prompt", default="Describe the image briefly.")
    ap.add_argument("--workflow-id", default="online-direct-e2e")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-url", default=None)
    ap.add_argument("--max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=64)
    ap.add_argument("--max-transfer-bytes", type=int, default=16 * 1024 * 1024)
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
