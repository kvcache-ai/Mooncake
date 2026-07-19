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
from typing import Any, Dict, List, Sequence

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
    _dataset_warmup_coverage,
    _dataset_replay_context,
    _dataset_request_evidence_row,
    _ensure_process_running,
    _execute_dataset_request,
    _launch,
    _load_dataset_requests,
    _materialize_dataset_replay_entry,
    _resolve_dataset_warmup_requests,
    _tail_text,
    _terminate_all,
    _wait_ready,
)
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


def _request_image_urls(args: argparse.Namespace) -> List[str]:
    """Resolve the exact multi-image workload used by the EPD runner."""

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


def _startup_timeout_s(args: argparse.Namespace) -> float:
    """Resolve the cold-start budget independently of request timing.

    A paired EPD run has a distinct startup timeout because three services may
    need model loading and compilation before the benchmark begins.  The
    colocated baseline needs the same control so a slow cold start never turns
    an otherwise valid paired workload into a one-sided artifact.  This value
    is deliberately excluded from hot-path fairness checks: it only governs
    readiness polling before warmup.
    """

    requested = getattr(args, "startup_timeout", None)
    if requested is None:
        requested = getattr(args, "timeout", 900.0)
    return max(60.0, float(requested))


def _baseline_entry(
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
        "family": "single_vllm_baseline",
        "request_variation_id": variation_id,
        "vllm_prefix_cache_id": prefix_cache_id,
        "sample": {
            "sample_id": f"single-baseline-r{repeat_idx}",
            "workflow_id": metadata["workflow_id"],
            "source_dataset": "single_baseline_e2e",
            "request_variation_id": variation_id,
            "vllm_prefix_cache_id": prefix_cache_id,
        },
    }


def _run_baseline_request(
    *,
    request_url: str,
    request_path: Path | None,
    repeat_idx: int,
    workflow_prefix: str,
    request_timeout: float,
    request_variation: str = REQUEST_VARIATION_NONE,
    vllm_prefix_cache_mode: str = VLLM_PREFIX_CACHE_ISOLATE,
    phase: str = "measure",
    dataset_entries: Sequence[Dict[str, Any]] | None = None,
    dataset_workflow_id_mode: str = "unique",
) -> Dict[str, Any]:
    response = _execute_dataset_request(
        proxy_url=request_url,
        entry=_baseline_entry(
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
    if int(response.get("status_code") or 0) >= 400 or response.get("error"):
        raise RuntimeError(
            f"single baseline request failed: {json.dumps(response, ensure_ascii=False)[:4000]}"
        )
    return response


def _write_server_script(*, workdir: Path, port: int, args: argparse.Namespace) -> Path:
    script = workdir / "start_single_vllm.sh"
    repo_parent = str(REPO_ROOT.parent)
    # Keep compilation artifacts beside the checkout (or on the explicitly
    # selected high-capacity volume), just like the EPD launcher.  A baseline
    # that silently falls back to a full system cache can both exhaust the
    # root filesystem and recompile between paired EPD runs.
    vllm_cache_root = os.getenv(
        "MOONCAKE_EPD_VLLM_CACHE_ROOT",
        str(REPO_ROOT.parent / ".cache" / "vllm"),
    )
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
    # Remote model code is intentionally opt-in.  Some checkpoints (for
    # example MiniCPM-O) declare a local ``auto_map`` even when this vLLM
    # build has a native model implementation; preserve the explicit audit
    # boundary rather than silently executing repository code for every
    # baseline.
    trust_remote_code_flag = (
        "--trust-remote-code "
        if bool(getattr(args, "trust_remote_code", False))
        else ""
    )
    if max_num_batched_tokens is not None:
        scheduler_flags += f"--max-num-batched-tokens {max_num_batched_tokens} "
    if max_num_seqs is not None:
        scheduler_flags += f"--max-num-seqs {max_num_seqs} "
    script.write_text(
        "#!/bin/bash\n"
        "unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY\n"
        "export NO_PROXY=127.0.0.1,localhost,::1\n"
        "export no_proxy=${NO_PROXY}\n"
        f"export PYTHONPATH={shlex.quote(repo_parent)}:${{PYTHONPATH:-}}\n"
        f"source {shlex.quote(str(VENV_ROOT / 'bin' / 'activate'))}\n"
        "export OPENAI_API_KEY=sk-local\n"
        f"export VLLM_CACHE_ROOT={shlex.quote(vllm_cache_root)}\n"
        "export TORCHINDUCTOR_CACHE_DIR=${VLLM_CACHE_ROOT}/torch_compile_cache\n"
        "export TRITON_CACHE_DIR=${VLLM_CACHE_ROOT}/triton\n"
        "mkdir -p \"${VLLM_CACHE_ROOT}\" \"${TORCHINDUCTOR_CACHE_DIR}\" \"${TRITON_CACHE_DIR}\"\n"
        # Do not expose EPD direct-buffer routes on the single-server baseline.
        "export MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=0\n"
        "export MOONCAKE_EPD_VLLM_FEATURE_HANDLE_STRICT=0\n"
        f"CUDA_VISIBLE_DEVICES={int(args.gpu)} vllm serve {model} "
        f"--port {int(port)} "
        "--tensor-parallel-size 1 "
        f"--max-model-len {int(args.max_model_len)} "
        f"--gpu-memory-utilization {float(args.gpu_memory_utilization)} "
        f"{generation_config_flag}"
        f"{trust_remote_code_flag}"
        f"{scheduler_flags}\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def run(args: argparse.Namespace) -> Dict[str, Any]:
    # The shared launcher runs scripts from the package root, so relative
    # artifact paths must be normalized before they are handed to it.
    workdir = Path(args.workdir).expanduser().resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    for name in (
        "single_baseline_summary.json",
        "single_baseline_request.json",
        "dataset_replay_selection.json",
        "dataset_replay_skipped.json",
    ):
        path = workdir / name
        if path.exists():
            path.unlink()

    vllm_prefix_cache_mode = str(
        getattr(args, "vllm_prefix_cache_mode", VLLM_PREFIX_CACHE_ISOLATE)
        or VLLM_PREFIX_CACHE_ISOLATE
    )
    # Validate before launching vLLM so an invalid cache policy never leaves a
    # partial baseline artifact that appears comparable.
    vllm_prefix_cache_spec(vllm_prefix_cache_mode)
    dataset_root = str(getattr(args, "dataset_root", "") or "").strip()
    max_dataset_requests = max(0, int(getattr(args, "max_dataset_requests", 0) or 0))
    dataset_families = list(getattr(args, "dataset_families", None) or [])
    if dataset_root and max_dataset_requests <= 0:
        raise ValueError("--dataset-root requires --max-dataset-requests > 0")
    if max_dataset_requests > 0 and not dataset_root:
        raise ValueError("--max-dataset-requests requires --dataset-root")
    if dataset_root and bool(getattr(args, "text_only", False)):
        raise ValueError(
            "dataset replay is multimodal; --text-only is unsupported for this baseline"
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

    request_image_urls = (
        []
        if bool(getattr(args, "text_only", False)) or dataset_entries
        else _request_image_urls(args)
    )
    port = _pick_free_port(int(args.port), str(args.local_hostname))
    request_path = workdir / "single_baseline_request.json"
    if dataset_entries:
        request_path.write_text(
            json.dumps(dataset_entries[0]["request"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        _write_request(args, request_path)
    server_script = _write_server_script(workdir=workdir, port=port, args=args)
    log_path = workdir / "single_vllm.log"
    proc: subprocess.Popen | None = None
    if dataset_context is not None:
        benchmark_request_fingerprint = str(dataset_context["request_fingerprint"])
        benchmark_request_config: Dict[str, Any] = {
            "model": str(args.model),
            "workload": "dataset_chat_split",
            # Match the strict EPD runner's explicit paired-workload marker.
            # The claim checker treats a missing source field as an invalid
            # comparison rather than guessing from dataset-only metadata.
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
                if bool(getattr(args, "text_only", False))
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
        "port": port,
        "log": str(log_path),
        "mode": "single_vllm_baseline",
        "gpu": int(args.gpu),
        "dataset": dataset_summary,
        "benchmark_config": {
            "schema_version": 1,
            "request_fingerprint": benchmark_request_fingerprint,
            "request": benchmark_request_config,
            "request_variation": request_variation_spec(str(args.request_variation)),
            "vllm_prefix_cache": vllm_prefix_cache_spec(vllm_prefix_cache_mode),
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
                "trust_remote_code": bool(getattr(args, "trust_remote_code", False)),
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
            timeout_s=_startup_timeout_s(args),
            paths=("/health",),
        )

        request_url = f"http://{args.local_hostname}:{port}/v1/chat/completions"
        workflow_prefix = str(args.workflow_id or "single-baseline-e2e")
        warmup_responses: List[Dict[str, Any]] = []
        requested_warmups = max(0, int(args.warmup_requests))
        effective_warmups = _resolve_dataset_warmup_requests(
            requested_warmups=requested_warmups,
            dataset_entries=dataset_entries,
            cover_dataset_cycle=bool(
                getattr(args, "warmup_cover_dataset_cycle", False)
            ),
        )
        warmup_concurrency = min(
            effective_warmups,
            max(1, int(args.warmup_concurrency) or max(1, int(args.concurrency))),
        ) if effective_warmups else 0
        summary["requested_warmup_requests"] = requested_warmups
        summary["effective_warmup_requests"] = effective_warmups
        summary["requested_warmup_concurrency"] = int(args.warmup_concurrency)
        summary["warmup_concurrency"] = warmup_concurrency
        summary["warmup_concurrency_mode"] = "auto" if int(args.warmup_concurrency) <= 0 else "explicit"
        if effective_warmups > 0 and warmup_concurrency > 1:
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
                    _run_baseline_request(
                        request_url=request_url,
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
                        _run_baseline_request,
                        request_url=request_url,
                        request_path=request_path,
                        repeat_idx=repeat_idx,
                        workflow_prefix=workflow_prefix,
                        request_timeout=float(args.request_timeout),
                        request_variation=str(args.request_variation),
                        vllm_prefix_cache_mode=vllm_prefix_cache_mode,
                        phase="measure",
                        dataset_entries=dataset_entries or None,
                        dataset_workflow_id_mode=dataset_workflow_id_mode,
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
        raw_request_rows: List[Dict[str, Any]] = []
        raw_response_rows: List[Dict[str, Any]] = []
        for phase, phase_responses, phase_prefix in (
            ("warmup", warmup_responses, f"{workflow_prefix}-warmup"),
            ("measure", responses, workflow_prefix),
        ):
            for item in phase_responses:
                repeat_idx = int(item.get("index", 0) or 0)
                entry = _baseline_entry(
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
            service_logs={"single_vllm": log_path, "launch_script": server_script},
            metrics={
                "request_throughput_rps": summary["request_throughput_rps"],
                "goodput_rps": summary["goodput_rps"],
                "completion_token_throughput_tps": summary[
                    "completion_token_throughput_tps"
                ],
                "ttft_stats_ms": summary["ttft_stats_ms"],
                "latency_stats_ms": summary["latency_stats_ms"],
            },
            runtime={
                "runner": "run_vllm_single_baseline_e2e",
                "model": str(args.model),
                "gpu": int(args.gpu),
                "benchmark_config": summary["benchmark_config"],
            },
        )
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
    ap.add_argument(
        "--trust-remote-code",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Allow checkpoint-provided model code for this explicit baseline run (default: false).",
    )
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument(
        "--startup-timeout",
        type=float,
        default=None,
        help=(
            "Maximum seconds to wait for vLLM cold start and compilation; "
            "defaults to --timeout."
        ),
    )
    ap.add_argument("--request-timeout", type=float, default=300.0)
    ap.add_argument("--warmup-requests", type=int, default=0)
    ap.add_argument(
        "--warmup-cover-dataset-cycle",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "For dataset replay, warm every selected sample at least once before "
            "measurement so image/prompt shapes are not left cold."
        ),
    )
    ap.add_argument("--warmup-concurrency", type=int, default=0, help="0 matches measured concurrency")
    ap.add_argument("--repeat-requests", type=int, default=2)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument(
        "--between-repeat-sleep-s",
        type=float,
        default=0.2,
        help="Inter-request delay; matches the strict EPD runner default.",
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
            "isolate is the paired strict-benchmark default and uses per-request "
            "cache_salt without changing model-visible prompt tokens; reuse is "
            "an explicit prefix-cache ablation."
        ),
    )
    ap.add_argument("--workflow-id", default="single-baseline-e2e")
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
    ap.add_argument("--text-only", action="store_true", help="Send a text-only request to isolate P->D KV behavior.")
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
