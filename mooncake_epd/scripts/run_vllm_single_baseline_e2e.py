#!/usr/bin/env python3
"""Run a real single-node vLLM multimodal serving baseline.

This is the baseline counterpart to ``run_vllm_online_direct_e2e.py``.  It
starts one ordinary vLLM OpenAI server with the same Qwen-VL model, sends the
same streaming multimodal request repeatedly, and writes a compact artifact
with TTFT / latency / goodput fields that the RFC benchmark matrix can compare
against EPD direct serving.

The script intentionally does not enable Mooncake KV connectors, EPD proxy, or
FeatureHandle injection.  It measures the colocated baseline hot path that must
run the normal vLLM multimodal processor/vision encoder inside one server.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import (  # noqa: E402
    VENV_ROOT,
    _pick_free_port,
)
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _data_url_for_demo_image,
    _ensure_process_running,
    _execute_dataset_request,
    _launch,
    _tail_text,
    _terminate_all,
    _wait_ready,
)
from mooncake_epd.scripts.benchmark_request_variants import (  # noqa: E402
    REQUEST_VARIATION_NONE,
    REQUEST_VARIATION_UNIQUE_PREFIX,
    REQUEST_VARIATION_UNIQUE_SUFFIX,
    apply_request_variation,
    request_variation_spec,
)


def _session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


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
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "avg": _avg(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _write_request(args: argparse.Namespace, request_path: Path) -> None:
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


def _baseline_entry(
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
        "family": "single_vllm_baseline",
        "sample": {
            "sample_id": f"single-baseline-r{repeat_idx}",
            "workflow_id": metadata["workflow_id"],
            "source_dataset": "single_baseline_e2e",
            "request_variation_id": variation_id,
        },
    }


def _run_baseline_request(
    *,
    request_url: str,
    request_path: Path,
    repeat_idx: int,
    workflow_prefix: str,
    request_timeout: float,
    request_variation: str = REQUEST_VARIATION_NONE,
    phase: str = "measure",
) -> Dict[str, Any]:
    response = _execute_dataset_request(
        proxy_url=request_url,
        entry=_baseline_entry(
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
    if int(response.get("status_code") or 0) >= 400 or response.get("error"):
        raise RuntimeError(
            f"single baseline request failed: {json.dumps(response, ensure_ascii=False)[:4000]}"
        )
    return response


def _write_server_script(*, workdir: Path, port: int, args: argparse.Namespace) -> Path:
    script = workdir / "start_single_vllm.sh"
    repo_parent = str(REPO_ROOT.parent)
    model = shlex.quote(str(args.model))
    scheduler_flags = ""
    max_num_batched_tokens = _optional_positive_int(args.max_num_batched_tokens)
    max_num_seqs = _optional_positive_int(args.max_num_seqs)
    generation_config = str(args.generation_config or "").strip()
    generation_config_flag = (
        f"--generation-config {shlex.quote(generation_config)} "
        if generation_config
        else ""
    )
    if max_num_batched_tokens is not None:
        scheduler_flags += f"--max-num-batched-tokens {max_num_batched_tokens} "
    if max_num_seqs is not None:
        scheduler_flags += f"--max-num-seqs {max_num_seqs} "
    script.write_text(
        "#!/bin/bash\n"
        "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY\n"
        "export NO_PROXY=127.0.0.1,localhost\n"
        f"export PYTHONPATH={shlex.quote(repo_parent)}:${{PYTHONPATH:-}}\n"
        f"source {shlex.quote(str(VENV_ROOT / 'bin' / 'activate'))}\n"
        "export OPENAI_API_KEY=sk-local\n"
        # Do not expose EPD direct-buffer routes on the single-server baseline.
        "export MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=0\n"
        "export MOONCAKE_EPD_VLLM_FEATURE_HANDLE_STRICT=0\n"
        f"CUDA_VISIBLE_DEVICES={int(args.gpu)} vllm serve {model} "
        f"--port {int(port)} "
        "--tensor-parallel-size 1 "
        f"--max-model-len {int(args.max_model_len)} "
        f"--gpu-memory-utilization {float(args.gpu_memory_utilization)} "
        f"{generation_config_flag}"
        f"{scheduler_flags}\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def run(args: argparse.Namespace) -> Dict[str, Any]:
    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    for name in ("single_baseline_summary.json", "single_baseline_request.json"):
        path = workdir / name
        if path.exists():
            path.unlink()

    port = _pick_free_port(int(args.port), str(args.local_hostname))
    request_path = workdir / "single_baseline_request.json"
    _write_request(args, request_path)
    server_script = _write_server_script(workdir=workdir, port=port, args=args)
    log_path = workdir / "single_vllm.log"
    proc: subprocess.Popen | None = None
    summary: Dict[str, Any] = {
        "workdir": str(workdir),
        "request_path": str(request_path),
        "port": port,
        "log": str(log_path),
        "mode": "single_vllm_baseline",
        "gpu": int(args.gpu),
        "benchmark_config": {
            "schema_version": 1,
            "request_fingerprint": _request_fingerprint(request_path),
            "request": {
                "model": str(args.model),
                "max_tokens": int(args.max_tokens),
                "temperature": float(args.temperature),
                "image_source": "none" if bool(getattr(args, "text_only", False)) else (
                    "explicit" if args.image_url else str(args.demo_image)
                ),
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
                "tensor_parallel_size": 1,
            },
            "topology": {
                "baseline_kind": "single_vllm",
                "gpus": [int(args.gpu)],
                "total_gpus": 1,
            },
        },
    }
    try:
        proc = _launch(str(server_script), log_path)
        time.sleep(4.0)
        _ensure_process_running("single-vllm", proc, log_path)
        _wait_ready(
            "single-vllm",
            f"http://{args.local_hostname}:{port}",
            proc=proc,
            log_path=log_path,
            timeout_s=min(300.0, max(60.0, float(args.timeout))),
            paths=("/health",),
        )

        request_url = f"http://{args.local_hostname}:{port}/v1/chat/completions"
        workflow_prefix = str(args.workflow_id or "single-baseline-e2e")
        warmup_responses: List[Dict[str, Any]] = []
        requested_warmups = max(0, int(args.warmup_requests))
        warmup_concurrency = min(
            requested_warmups,
            max(1, int(args.warmup_concurrency) or max(1, int(args.concurrency))),
        ) if requested_warmups else 0
        summary["requested_warmup_requests"] = requested_warmups
        summary["effective_warmup_requests"] = requested_warmups
        summary["requested_warmup_concurrency"] = int(args.warmup_concurrency)
        summary["warmup_concurrency"] = warmup_concurrency
        summary["warmup_concurrency_mode"] = "auto" if int(args.warmup_concurrency) <= 0 else "explicit"
        if requested_warmups > 0 and warmup_concurrency > 1:
            with ThreadPoolExecutor(max_workers=warmup_concurrency) as pool:
                future_map = {
                    pool.submit(
                        _run_baseline_request,
                        request_url=request_url,
                        request_path=request_path,
                        repeat_idx=-(warmup_idx + 1),
                        workflow_prefix=f"{workflow_prefix}-warmup",
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        phase="warmup",
                    ): warmup_idx
                    for warmup_idx in range(requested_warmups)
                }
                for future in as_completed(future_map):
                    warmup_responses.append(future.result())
            warmup_responses.sort(key=lambda item: int(item.get("index", 0) or 0))
        else:
            for warmup_idx in range(requested_warmups):
                warmup_responses.append(
                    _run_baseline_request(
                        request_url=request_url,
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

        responses: List[Dict[str, Any]] = []
        benchmark_started = time.perf_counter()
        total_requests = max(1, int(args.repeat_requests))
        concurrency = max(1, int(args.concurrency))
        if concurrency == 1:
            for repeat_idx in range(total_requests):
                responses.append(
                    _run_baseline_request(
                        request_url=request_url,
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
                        _run_baseline_request,
                        request_url=request_url,
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
        summary.update(
            {
                "responses": responses,
                "warmup_responses": warmup_responses,
                "response": responses[-1] if responses else {},
                "benchmark_elapsed_s": benchmark_elapsed_s,
                "request_throughput_rps": len(ok_responses) / benchmark_elapsed_s,
                "goodput_rps": len(ok_responses) / benchmark_elapsed_s,
                "completion_tokens": completion_tokens,
                "completion_token_throughput_tps": completion_tokens / benchmark_elapsed_s,
                "output_token_goodput_tps": completion_tokens / benchmark_elapsed_s,
                "avg_tpot_ms": _avg(tpot_values),
                "avg_ttft_ms": _avg(ttft_values),
                "ttft_ms": float((responses[-1] if responses else {}).get("ttft_ms", 0.0) or 0.0),
                "ttft_stats_ms": _latency_stats(ttft_values),
                "avg_latency_ms": _avg(elapsed_values),
                "latency_stats_ms": _latency_stats(elapsed_values),
                "warmup_ttft_stats_ms": _latency_stats([
                    float(item.get("ttft_ms", 0.0) or 0.0)
                    for item in warmup_responses
                    if item.get("ttft_ms") is not None and not item.get("error")
                ]),
            }
        )
        if int((summary.get("response") or {}).get("response_content_len", 0) or 0) <= 0:
            raise AssertionError("single baseline response content is empty")
        return summary
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        summary["log_tail"] = _tail_text(log_path, 160)
        raise
    finally:
        if proc is not None:
            _terminate_all([proc])
        out = workdir / "single_baseline_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real single-node vLLM serving baseline.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_single_vllm_baseline")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
    )
    ap.add_argument("--gpu", type=int, default=3)
    ap.add_argument("--port", type=int, default=8400)
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--max-num-batched-tokens", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument("--max-num-seqs", type=int, default=0, help="0 keeps the vLLM default")
    ap.add_argument(
        "--generation-config",
        default="vllm",
        help="vLLM --generation-config value; benchmark default avoids model-repository sampling defaults.",
    )
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--warmup-requests", type=int, default=0)
    ap.add_argument("--warmup-concurrency", type=int, default=0, help="0 matches measured concurrency")
    ap.add_argument("--repeat-requests", type=int, default=2)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--between-repeat-sleep-s", type=float, default=0.5)
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
    ap.add_argument("--workflow-id", default="single-baseline-e2e")
    ap.add_argument("--max-tokens", type=int, default=32)
    ap.add_argument("--temperature", type=float, default=0.0)
    ap.add_argument("--demo-image", default="room")
    ap.add_argument("--image-url", default=None)
    ap.add_argument("--text-only", action="store_true", help="Send a text-only request to isolate P->D KV behavior.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    summary = run(args)
    out = Path(args.workdir).expanduser() / "single_baseline_summary.json"
    print(
        json.dumps(
            {
                "summary": str(out),
                "response": summary.get("response", {}),
                "avg_ttft_ms": summary.get("avg_ttft_ms"),
                "goodput_rps": summary.get("goodput_rps"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
