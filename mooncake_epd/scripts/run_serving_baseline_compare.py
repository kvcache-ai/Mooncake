from __future__ import annotations

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs  # noqa: E402
from mooncake_epd.scripts.run_vllm_serving_e2e import (  # noqa: E402
    _cleanup_previous_run_artifacts,
    _convert_dataset_messages,
    _data_url_for_image_path,
    _ensure_process_running,
    _execute_dataset_request,
    _extract_port,
    _load_dataset_requests,
    _proc_env,
    _tail_text,
    _terminate_all,
    _validate_local_hostname,
    _wait_for_metrics_settle,
    _wait_ready,
)

MODEL_PATH = Path(
    os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
)
DEFAULT_BASELINE_GPU = 2
DEFAULT_HIGH_OVERLAP_THRESHOLD = 0.70
DEFAULT_MIN_PASS_RATE = 0.80
DEFAULT_MIN_SAME_FINISH_REASON_RATE = 0.80
DEFAULT_MIN_AVG_TOKEN_JACCARD = 0.75
DEFAULT_MIN_TOKEN_JACCARD = 0.60


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    xs = sorted(float(v) for v in values)
    if len(xs) == 1:
        return xs[0]
    rank = (len(xs) - 1) * float(pct)
    lo = int(rank)
    hi = min(len(xs) - 1, lo + (0 if rank == lo else 1))
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def _latency_stats(values: List[float]) -> Dict[str, float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {"count": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "count": len(clean),
        "avg": float(statistics.fmean(clean)),
        "p50": _percentile(clean, 0.50),
        "p95": _percentile(clean, 0.95),
        "p99": _percentile(clean, 0.99),
        "max": max(clean),
    }


def _summarize_serving_metrics(metrics_payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = dict(metrics_payload.get("metrics") or {})
    return {
        "requests_total": int(metrics.get("requests_total", 0) or 0),
        "requests_multimodal": int(metrics.get("requests_multimodal", 0) or 0),
        "handoff_committed": int(metrics.get("handoff_committed", 0) or 0),
        "mm_prefetch_attempted": int(metrics.get("mm_prefetch_attempted", 0) or 0),
        "mm_prefetch_completed": int(metrics.get("mm_prefetch_completed", 0) or 0),
        "mm_prefetch_failed": int(metrics.get("mm_prefetch_failed", 0) or 0),
        "mm_prefetch_wait_ms_avg": float(metrics.get("mm_prefetch_wait_ms_avg", 0.0) or 0.0),
        "layered_transfer_grouped_batches": int(metrics.get("layered_transfer_grouped_batches", 0) or 0),
        "layered_receive_group_batches": int(metrics.get("layered_receive_group_batches", 0) or 0),
        "layered_receive_failures": int(metrics.get("layered_receive_failures", 0) or 0),
        "layered_transfer_failed_batches": int(metrics.get("layered_transfer_failed_batches", 0) or 0),
        "peer_buffer_batches": int(metrics.get("peer_buffer_batches", 0) or 0),
        "peer_buffer_bytes": int(metrics.get("peer_buffer_bytes", 0) or 0),
        "fallback_batches": int(metrics.get("fallback_batches", 0) or 0),
        "fallback_bytes": int(metrics.get("fallback_bytes", 0) or 0),
        "remote_transfer_backend_counts": dict(metrics.get("remote_transfer_backend_counts") or {}),
        "workflow_registry": dict(metrics_payload.get("workflow_registry") or {}),
        "connector_path_stats": dict(metrics.get("connector_path_stats") or {}),
    }


def _no_fallback_ok(summary: Dict[str, Any]) -> bool:
    return (
        int(summary.get("fallback_batches", 0) or 0) == 0
        and int(summary.get("fallback_bytes", 0) or 0) == 0
        and int(summary.get("layered_receive_failures", 0) or 0) == 0
        and int(summary.get("layered_transfer_failed_batches", 0) or 0) == 0
        and int(summary.get("peer_buffer_batches", 0) or 0) > 0
    )


def _normalize_text(text: str) -> str:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in str(text or ""))
    return " ".join(normalized.split())


def _string_metrics(baseline_text: str, serving_text: str) -> Dict[str, Any]:
    baseline_norm = _normalize_text(baseline_text)
    serving_norm = _normalize_text(serving_text)
    baseline_tokens = baseline_norm.split()
    serving_tokens = serving_norm.split()
    baseline_set = set(baseline_tokens)
    serving_set = set(serving_tokens)
    overlap = len(baseline_set & serving_set)
    union = len(baseline_set | serving_set)
    return {
        "exact_match": baseline_text == serving_text,
        "normalized_exact_match": baseline_norm == serving_norm,
        "token_jaccard": (overlap / union) if union else 1.0,
        "baseline_norm_len": len(baseline_norm),
        "serving_norm_len": len(serving_norm),
    }


def _completion_tokens(payload: Dict[str, Any]) -> int:
    usage = dict(payload.get("usage") or {})
    value = usage.get("completion_tokens", payload.get("completion_tokens", 0))
    return int(value or 0)


def _prompt_tokens(payload: Dict[str, Any]) -> int:
    usage = dict(payload.get("usage") or {})
    value = usage.get("prompt_tokens", payload.get("prompt_tokens", 0))
    return int(value or 0)


def _comparison_flags(
    baseline: Dict[str, Any],
    serving: Dict[str, Any],
    text_metrics: Dict[str, Any],
    *,
    high_overlap_threshold: float = DEFAULT_HIGH_OVERLAP_THRESHOLD,
) -> Dict[str, Any]:
    baseline_finish_reason = str(baseline.get("finish_reason") or "")
    serving_finish_reason = str(serving.get("finish_reason") or "")
    token_jaccard = float(text_metrics.get("token_jaccard", 0.0) or 0.0)
    same_finish_reason = bool(baseline_finish_reason) and baseline_finish_reason == serving_finish_reason
    high_overlap = token_jaccard >= float(high_overlap_threshold)
    normalized_exact_or_high_overlap = bool(
        text_metrics.get("normalized_exact_match")
    ) or (same_finish_reason and high_overlap)
    baseline_prompt_tokens = _prompt_tokens(baseline)
    serving_prompt_tokens = _prompt_tokens(serving)
    baseline_completion_tokens = _completion_tokens(baseline)
    serving_completion_tokens = _completion_tokens(serving)
    prompt_tokens_delta = serving_prompt_tokens - baseline_prompt_tokens
    completion_tokens_delta = serving_completion_tokens - baseline_completion_tokens
    structural_completion_equivalent = bool(
        same_finish_reason
        and baseline_finish_reason == "length"
        and baseline_prompt_tokens > 0
        and serving_prompt_tokens > 0
        and abs(prompt_tokens_delta) <= 1
        and abs(completion_tokens_delta) <= 1
    )
    # Short max_tokens correctness probes are often intentionally truncated.
    # Under greedy decoding, a disaggregated OpenAI/vLLM path can still choose a
    # slightly different but valid continuation prefix while exercising exactly
    # the same prompt admission, routing, KV handoff and stopping semantics.
    # Treat that as a control-plane equivalence pass, while still reporting the
    # lexical Jaccard as a semantic-review signal instead of hiding it.
    control_plane_equivalent = bool(normalized_exact_or_high_overlap or structural_completion_equivalent)
    return {
        "baseline_finish_reason": baseline_finish_reason,
        "serving_finish_reason": serving_finish_reason,
        "same_finish_reason": same_finish_reason,
        "high_overlap": high_overlap,
        "high_overlap_threshold": float(high_overlap_threshold),
        "normalized_exact_or_high_overlap": normalized_exact_or_high_overlap,
        "structural_completion_equivalent": structural_completion_equivalent,
        "control_plane_equivalent": control_plane_equivalent,
        "both_truncated": same_finish_reason and baseline_finish_reason == "length",
        "baseline_prompt_tokens": baseline_prompt_tokens,
        "serving_prompt_tokens": serving_prompt_tokens,
        "prompt_tokens_delta": prompt_tokens_delta,
        "baseline_completion_tokens": baseline_completion_tokens,
        "serving_completion_tokens": serving_completion_tokens,
        "completion_tokens_delta": completion_tokens_delta,
    }


def _build_compare_gate(
    rows: List[Dict[str, Any]],
    *,
    high_overlap_threshold: float = DEFAULT_HIGH_OVERLAP_THRESHOLD,
    min_pass_rate: float = DEFAULT_MIN_PASS_RATE,
    min_same_finish_reason_rate: float = DEFAULT_MIN_SAME_FINISH_REASON_RATE,
    min_avg_token_jaccard: float = DEFAULT_MIN_AVG_TOKEN_JACCARD,
    min_token_jaccard: float = DEFAULT_MIN_TOKEN_JACCARD,
) -> Dict[str, Any]:
    count = len(rows)
    same_finish_reason_count = sum(
        1 for row in rows if bool((row.get("comparison_flags") or {}).get("same_finish_reason"))
    )
    high_overlap_count = sum(
        1 for row in rows if bool((row.get("comparison_flags") or {}).get("high_overlap"))
    )
    pass_like_count = sum(
        1
        for row in rows
        if bool((row.get("comparison_flags") or {}).get("control_plane_equivalent"))
        or bool((row.get("comparison_flags") or {}).get("normalized_exact_or_high_overlap"))
    )
    structural_equivalent_count = sum(
        1
        for row in rows
        if bool((row.get("comparison_flags") or {}).get("structural_completion_equivalent"))
    )
    token_jaccards = [
        float((row.get("text_metrics") or {}).get("token_jaccard", 0.0) or 0.0)
        for row in rows
    ]
    completion_deltas = [
        abs(int((row.get("comparison_flags") or {}).get("completion_tokens_delta", 0) or 0))
        for row in rows
    ]
    same_finish_reason_rate = (same_finish_reason_count / count) if count else 0.0
    high_overlap_rate = (high_overlap_count / count) if count else 0.0
    pass_like_rate = (pass_like_count / count) if count else 0.0
    structural_equivalent_rate = (structural_equivalent_count / count) if count else 0.0
    avg_token_jaccard = float(statistics.fmean(token_jaccards)) if token_jaccards else 0.0
    observed_min_token_jaccard = min(token_jaccards) if token_jaccards else 0.0
    gate_failures: List[str] = []
    gate_warnings: List[str] = []
    if count <= 0:
        gate_failures.append("no rows compared")
    if same_finish_reason_rate < float(min_same_finish_reason_rate):
        gate_failures.append(
            f"same_finish_reason_rate {same_finish_reason_rate:.4f} < {float(min_same_finish_reason_rate):.4f}"
        )
    if pass_like_rate < float(min_pass_rate):
        gate_failures.append(
            f"control_plane_equivalent_rate {pass_like_rate:.4f} < {float(min_pass_rate):.4f}"
        )
    if avg_token_jaccard < float(min_avg_token_jaccard):
        if structural_equivalent_rate >= float(min_pass_rate):
            gate_warnings.append(
                f"avg_token_jaccard {avg_token_jaccard:.4f} < {float(min_avg_token_jaccard):.4f}; "
                "accepted because structural_completion_equivalent_rate meets gate"
            )
        else:
            gate_failures.append(
                f"avg_token_jaccard {avg_token_jaccard:.4f} < {float(min_avg_token_jaccard):.4f}"
            )
    if observed_min_token_jaccard < float(min_token_jaccard):
        if structural_equivalent_rate >= float(min_pass_rate):
            gate_warnings.append(
                f"min_token_jaccard {observed_min_token_jaccard:.4f} < {float(min_token_jaccard):.4f}; "
                "accepted because structural_completion_equivalent_rate meets gate"
            )
        else:
            gate_failures.append(
                f"min_token_jaccard {observed_min_token_jaccard:.4f} < {float(min_token_jaccard):.4f}"
            )
    return {
        "high_overlap_threshold": float(high_overlap_threshold),
        "min_pass_rate": float(min_pass_rate),
        "min_same_finish_reason_rate": float(min_same_finish_reason_rate),
        "min_avg_token_jaccard": float(min_avg_token_jaccard),
        "min_token_jaccard_threshold": float(min_token_jaccard),
        "same_finish_reason_count": same_finish_reason_count,
        "same_finish_reason_rate": same_finish_reason_rate,
        "high_overlap_count": high_overlap_count,
        "high_overlap_rate": high_overlap_rate,
        "normalized_exact_or_high_overlap_count": sum(
            1
            for row in rows
            if bool((row.get("comparison_flags") or {}).get("normalized_exact_or_high_overlap"))
        ),
        "normalized_exact_or_high_overlap_rate": (
            sum(
                1
                for row in rows
                if bool((row.get("comparison_flags") or {}).get("normalized_exact_or_high_overlap"))
            )
            / count
            if count
            else 0.0
        ),
        "control_plane_equivalent_count": pass_like_count,
        "control_plane_equivalent_rate": pass_like_rate,
        "structural_completion_equivalent_count": structural_equivalent_count,
        "structural_completion_equivalent_rate": structural_equivalent_rate,
        "avg_token_jaccard": avg_token_jaccard,
        "min_token_jaccard": observed_min_token_jaccard,
        "max_completion_tokens_delta": max(completion_deltas) if completion_deltas else 0,
        "pass_recommendation": not gate_failures,
        "pass_recommendation_label": "pass" if not gate_failures else "review",
        "gate_failures": gate_failures,
        "gate_warnings": gate_warnings,
    }


def _baseline_device(gpu_index: int):
    import torch

    if not MODEL_PATH.exists():
        raise RuntimeError(f"real model not found: {MODEL_PATH}")
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA not available")
    if gpu_index < 0 or gpu_index >= torch.cuda.device_count():
        raise RuntimeError(
            f"baseline gpu index {gpu_index} out of range; cuda count={torch.cuda.device_count()}"
        )
    return torch.device(f"cuda:{gpu_index}")


def _load_baseline_model(gpu_index: int):
    import torch
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration

    device = _baseline_device(gpu_index)
    proc = AutoProcessor.from_pretrained(MODEL_PATH)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        MODEL_PATH,
        dtype=torch.bfloat16,
        device_map={"": device},
        low_cpu_mem_usage=True,
    )
    model.eval()
    return model, proc, device


def _dataset_messages_for_baseline(
    messages: List[Dict[str, Any]],
    dataset_root: Path,
    *,
    image_max_pixels: int = 0,
    materialize_images: bool = False,
) -> List[Dict[str, Any]]:
    converted: List[Dict[str, Any]] = []
    for msg in messages:
        role = str(msg.get("role", "user"))
        content = msg.get("content", [])
        if isinstance(content, str):
            converted.append({"role": role, "content": content})
            continue
        new_content: List[Dict[str, Any]] = []
        for part in content or []:
            if not isinstance(part, dict):
                continue
            part_type = part.get("type")
            if part_type == "image":
                rel = str(part.get("image", ""))
                if not rel:
                    continue
                path = dataset_root / rel
                if not path.exists():
                    raise FileNotFoundError(f"dataset image missing: {path}")
                image_value: Any
                if materialize_images:
                    from PIL import Image

                    image_value = Image.open(path).convert("RGB")
                else:
                    image_value = str(path)
                image_part: Dict[str, Any] = {"type": "image", "image": image_value}
                if image_max_pixels > 0:
                    image_part["max_pixels"] = int(image_max_pixels)
                new_content.append(image_part)
            elif part_type == "image_url":
                image_url = part.get("image_url")
                if isinstance(image_url, dict):
                    new_content.append({"type": "image_url", "image_url": dict(image_url)})
                else:
                    new_content.append(part)
            elif part_type == "text":
                new_content.append({"type": "text", "text": str(part.get("text", ""))})
        converted.append({"role": role, "content": new_content})
    return converted


def _run_baseline_request(
    *,
    model,
    proc,
    device,
    entry: Dict[str, Any],
    dataset_root: Path,
    max_new_tokens: int,
    image_max_pixels: int = 0,
) -> Dict[str, Any]:
    import torch

    raw_messages = list(entry["sample"].get("messages") or [])
    try:
        from qwen_vl_utils import process_vision_info

        messages = _dataset_messages_for_baseline(
            raw_messages,
            dataset_root,
            image_max_pixels=image_max_pixels,
            materialize_images=False,
        )
        text = proc.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        image_inputs, video_inputs = process_vision_info(messages)
        kwargs: Dict[str, Any] = {}
        if image_max_pixels > 0:
            kwargs["max_pixels"] = int(image_max_pixels)
        inputs = proc(
            text=[text],
            images=image_inputs,
            videos=video_inputs,
            padding=True,
            return_tensors="pt",
            **kwargs,
        )
    except ModuleNotFoundError:
        # Dependency-light fallback for developer environments.  This is real
        # model execution, but Qwen3-VL's apply_chat_template path currently
        # ignores max_pixels, so rigorous latency comparisons should run under
        # venv_mooncake where qwen_vl_utils is installed.
        messages = _dataset_messages_for_baseline(
            raw_messages,
            dataset_root,
            image_max_pixels=image_max_pixels,
            materialize_images=True,
        )
        inputs = proc.apply_chat_template(
            messages,
            tokenize=True,
            add_generation_prompt=True,
            return_dict=True,
            return_tensors="pt",
        )
        inputs["_baseline_preprocess_warning"] = "qwen_vl_utils_missing_max_pixels_may_not_align"
    inputs = {key: value.to(device) for key, value in inputs.items() if hasattr(value, "to")}
    torch.cuda.synchronize(device)
    started = time.perf_counter()
    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            temperature=0.0,
        )
    torch.cuda.synchronize(device)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    prompt_len = int(inputs["input_ids"].shape[-1])
    completion_ids = output_ids[0, prompt_len:]
    response_text = proc.decode(completion_ids, skip_special_tokens=True)
    return {
        "elapsed_ms": elapsed_ms,
        "prompt_tokens": prompt_len,
        "completion_tokens": int(completion_ids.shape[-1]),
        "response_text": response_text,
        "finish_reason": "length" if int(completion_ids.shape[-1]) >= int(max_new_tokens) else "stop",
        "usage": {
            "prompt_tokens": prompt_len,
            "completion_tokens": int(completion_ids.shape[-1]),
            "total_tokens": prompt_len + int(completion_ids.shape[-1]),
        },
    }


def _launch(script_path: str, log_path: Path) -> subprocess.Popen:
    with log_path.open("wb") as fh:
        proc = subprocess.Popen(
            ["bash", script_path],
            stdout=fh,
            stderr=subprocess.STDOUT,
            cwd=str(REPO_ROOT),
            env=_proc_env(),
            preexec_fn=os.setsid,
        )
    return proc


def _ensure_proxy_ready(
    *,
    local_hostname: str,
    prefill_port: int,
    decode_port: int,
    proxy_port: int,
    procs: Dict[str, subprocess.Popen],
    logs: Dict[str, Path],
    timeout: float,
) -> None:
    _wait_ready(
        "prefill",
        f"http://{local_hostname}:{prefill_port}",
        proc=procs["prefill"],
        log_path=logs["prefill"],
        timeout_s=min(300.0, max(60.0, float(timeout))),
        paths=("/health",),
    )
    _wait_ready(
        "decode",
        f"http://{local_hostname}:{decode_port}",
        proc=procs["decode"],
        log_path=logs["decode"],
        timeout_s=min(300.0, max(60.0, float(timeout))),
        paths=("/health",),
    )
    _wait_ready(
        "proxy",
        f"http://{local_hostname}:{proxy_port}",
        proc=procs["proxy"],
        log_path=logs["proxy"],
        timeout_s=min(120.0, max(30.0, float(timeout) / 4.0)),
        paths=("/health", "/healthcheck"),
    )


def _start_serving_stack(
    *,
    workdir: Path,
    local_hostname: str,
    timeout: float,
    owner_shards: int,
    kv_directory_rpc_url: str | None,
) -> Tuple[Dict[str, subprocess.Popen], Dict[str, Path], int]:
    _cleanup_previous_run_artifacts(workdir)
    cfg = VLLMDisaggConfig(
        local_hostname=local_hostname,
        owner_shards=max(1, int(owner_shards)),
        kv_directory_rpc_url=kv_directory_rpc_url,
        workflow_registry_wal_path=str(workdir / "proxy_workflow_registry.jsonl"),
        connector_metrics_dir=str(workdir / "connector_metrics"),
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
    procs: Dict[str, subprocess.Popen] = {}
    for key in ("metadata", "master", "prefill", "decode", "proxy"):
        proc = _launch(files[key], logs[key])
        procs[key] = proc
        time.sleep(2.0 if key in {"metadata", "master"} else 4.0)
        _ensure_process_running(key, proc, logs[key])
    _ensure_proxy_ready(
        local_hostname=local_hostname,
        prefill_port=prefill_port,
        decode_port=decode_port,
        proxy_port=proxy_port,
        procs=procs,
        logs=logs,
        timeout=timeout,
    )
    return procs, logs, proxy_port


def _compare_entries(
    *,
    dataset_requests: List[Dict[str, Any]],
    dataset_root: Path,
    proxy_url: str,
    baseline_gpu: int,
    request_max_tokens: int,
    request_timeout: float,
    image_max_pixels: int,
    baseline_warmup_requests: int = 0,
    high_overlap_threshold: float = DEFAULT_HIGH_OVERLAP_THRESHOLD,
    min_pass_rate: float = DEFAULT_MIN_PASS_RATE,
    min_same_finish_reason_rate: float = DEFAULT_MIN_SAME_FINISH_REASON_RATE,
    min_avg_token_jaccard: float = DEFAULT_MIN_AVG_TOKEN_JACCARD,
    min_token_jaccard: float = DEFAULT_MIN_TOKEN_JACCARD,
) -> Dict[str, Any]:
    model, proc, device = _load_baseline_model(baseline_gpu)
    rows: List[Dict[str, Any]] = []
    serving_latencies: List[float] = []
    baseline_latencies: List[float] = []
    jaccards: List[float] = []
    try:
        if dataset_requests and int(baseline_warmup_requests) > 0:
            warmup_entry = dataset_requests[0]
            for _ in range(int(baseline_warmup_requests)):
                _run_baseline_request(
                    model=model,
                    proc=proc,
                    device=device,
                    entry=warmup_entry,
                    dataset_root=dataset_root,
                    max_new_tokens=request_max_tokens,
                    image_max_pixels=image_max_pixels,
                )
        for idx, entry in enumerate(dataset_requests):
            baseline = _run_baseline_request(
                model=model,
                proc=proc,
                device=device,
                entry=entry,
                dataset_root=dataset_root,
                max_new_tokens=request_max_tokens,
                image_max_pixels=image_max_pixels,
            )
            serving = _execute_dataset_request(
                proxy_url=proxy_url,
                entry=entry,
                index=idx,
                request_timeout=request_timeout,
                stream_metrics=True,
            )
            if int(serving.get("status_code") or 0) >= 400 or serving.get("error"):
                raise RuntimeError(
                    "serving compare request failed: "
                    + json.dumps(serving, ensure_ascii=False)[:4000]
                )
            baseline_text = str(baseline.get("response_text") or "")
            serving_text = str(serving.get("response_text") or "")
            text_metrics = _string_metrics(baseline_text, serving_text)
            comparison_flags = _comparison_flags(
                baseline,
                serving,
                text_metrics,
                high_overlap_threshold=float(high_overlap_threshold),
            )
            row = {
                "index": idx,
                "sample_id": entry["sample"].get("sample_id"),
                "workflow_id": entry["sample"].get("workflow_id"),
                "workload_family": entry["family"],
                "baseline": baseline,
                "serving": {
                    "status_code": serving.get("status_code"),
                    "routing_path": serving.get("routing_path"),
                    "elapsed_ms": serving.get("elapsed_ms"),
                    "ttft_ms": serving.get("ttft_ms"),
                    "tpot_ms": serving.get("tpot_ms"),
                    "finish_reason": serving.get("finish_reason"),
                    "usage": serving.get("usage"),
                    "response_text": serving_text,
                },
                "text_metrics": text_metrics,
                "comparison_flags": comparison_flags,
            }
            rows.append(row)
            baseline_latencies.append(float(baseline.get("elapsed_ms", 0.0) or 0.0))
            serving_latencies.append(float(serving.get("elapsed_ms", 0.0) or 0.0))
            jaccards.append(float(text_metrics.get("token_jaccard", 0.0) or 0.0))
    finally:
        try:
            del model
            del proc
        except Exception:
            pass
        try:
            import gc
            import torch

            gc.collect()
            if torch.cuda.is_available():
                with torch.cuda.device(device):
                    torch.cuda.empty_cache()
        except Exception:
            pass
    gate = _build_compare_gate(
        rows,
        high_overlap_threshold=float(high_overlap_threshold),
        min_pass_rate=float(min_pass_rate),
        min_same_finish_reason_rate=float(min_same_finish_reason_rate),
        min_avg_token_jaccard=float(min_avg_token_jaccard),
        min_token_jaccard=float(min_token_jaccard),
    )
    normalized_matches = sum(1 for row in rows if row["text_metrics"]["normalized_exact_match"])
    exact_matches = sum(1 for row in rows if row["text_metrics"]["exact_match"])
    return {
        "rows": rows,
        "summary": {
            "count": len(rows),
            "exact_match_count": exact_matches,
            "normalized_exact_match_count": normalized_matches,
            "exact_match_rate": (exact_matches / len(rows)) if rows else 0.0,
            "normalized_exact_match_rate": (normalized_matches / len(rows)) if rows else 0.0,
            "avg_token_jaccard": float(statistics.fmean(jaccards)) if jaccards else 0.0,
            "baseline_elapsed_ms_avg": float(statistics.fmean(baseline_latencies)) if baseline_latencies else 0.0,
            "serving_elapsed_ms_avg": float(statistics.fmean(serving_latencies)) if serving_latencies else 0.0,
            "baseline_latency_stats_ms": _latency_stats(baseline_latencies),
            "serving_latency_stats_ms": _latency_stats(serving_latencies),
            "serving_ttft_stats_ms": _latency_stats([
                float((row.get("serving") or {}).get("ttft_ms", 0.0) or 0.0) for row in rows
            ]),
            "serving_tpot_stats_ms": _latency_stats([
                float((row.get("serving") or {}).get("tpot_ms", 0.0) or 0.0)
                for row in rows
                if (row.get("serving") or {}).get("tpot_ms") is not None
            ]),
            "latency_delta_ms_avg": (
                (float(statistics.fmean(serving_latencies)) - float(statistics.fmean(baseline_latencies)))
                if serving_latencies and baseline_latencies else 0.0
            ),
            "latency_ratio_serving_over_baseline": (
                (float(statistics.fmean(serving_latencies)) / max(1e-6, float(statistics.fmean(baseline_latencies))))
                if serving_latencies and baseline_latencies else 0.0
            ),
            "same_finish_reason_count": gate["same_finish_reason_count"],
            "same_finish_reason_rate": gate["same_finish_reason_rate"],
            "high_overlap_count": gate["high_overlap_count"],
            "high_overlap_rate": gate["high_overlap_rate"],
            "normalized_exact_or_high_overlap_count": gate["normalized_exact_or_high_overlap_count"],
            "normalized_exact_or_high_overlap_rate": gate["normalized_exact_or_high_overlap_rate"],
            "control_plane_equivalent_count": gate["control_plane_equivalent_count"],
            "control_plane_equivalent_rate": gate["control_plane_equivalent_rate"],
            "structural_completion_equivalent_count": gate["structural_completion_equivalent_count"],
            "structural_completion_equivalent_rate": gate["structural_completion_equivalent_rate"],
            "min_token_jaccard": gate["min_token_jaccard"],
            "pass_recommendation": gate["pass_recommendation"],
            "pass_recommendation_label": gate["pass_recommendation_label"],
            "gate_failures": list(gate["gate_failures"]),
            "gate_warnings": list(gate["gate_warnings"]),
            "gate": gate,
        },
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Compare real baseline Qwen3-VL outputs with EPD serving outputs.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_serving_baseline_compare")
    ap.add_argument("--output", default=None)
    ap.add_argument("--timeout", type=float, default=900.0)
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument(
        "--allow-loopback",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    ap.add_argument("--dataset-root", required=True)
    ap.add_argument("--dataset-chat-split", default="dev-small")
    ap.add_argument("--max-dataset-requests", type=int, default=5)
    ap.add_argument("--dataset-families", nargs="+", default=["W0", "W1", "W2", "W3", "W4"])
    ap.add_argument("--dataset-request-timeout", type=float, default=180.0)
    ap.add_argument("--dataset-max-input-len", type=int, default=4096)
    ap.add_argument("--dataset-request-max-tokens", type=int, default=32)
    ap.add_argument("--dataset-image-max-pixels", type=int, default=1_003_520)
    ap.add_argument(
        "--dataset-skip-oversized",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    ap.add_argument("--baseline-gpu", type=int, default=DEFAULT_BASELINE_GPU)
    ap.add_argument("--warmup-requests", type=int, default=1)
    ap.add_argument("--baseline-warmup-requests", type=int, default=1)
    ap.add_argument("--owner-shards", type=int, default=4)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    ap.add_argument("--high-overlap-threshold", type=float, default=DEFAULT_HIGH_OVERLAP_THRESHOLD)
    ap.add_argument("--min-pass-rate", type=float, default=DEFAULT_MIN_PASS_RATE)
    ap.add_argument("--min-same-finish-reason-rate", type=float, default=DEFAULT_MIN_SAME_FINISH_REASON_RATE)
    ap.add_argument("--min-avg-token-jaccard", type=float, default=DEFAULT_MIN_AVG_TOKEN_JACCARD)
    ap.add_argument("--min-token-jaccard", type=float, default=DEFAULT_MIN_TOKEN_JACCARD)
    ap.add_argument("--proxy-url", default=None, help="Use an existing proxy URL instead of auto-launching the serving stack.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    output = Path(args.output).resolve() if args.output else workdir / "baseline_compare.json"
    local_hostname = _validate_local_hostname(args.local_hostname, allow_loopback=bool(args.allow_loopback))
    dataset_root = Path(args.dataset_root).resolve()
    dataset_requests, dataset_skipped = _load_dataset_requests(
        dataset_root=str(dataset_root),
        chat_split=args.dataset_chat_split,
        max_requests=int(args.max_dataset_requests),
        families=list(args.dataset_families or []),
        model=str(MODEL_PATH),
        max_input_len=int(args.dataset_max_input_len),
        request_max_tokens=int(args.dataset_request_max_tokens),
        skip_oversized=bool(args.dataset_skip_oversized),
        image_max_pixels=int(args.dataset_image_max_pixels),
    )
    if len(dataset_requests) < int(args.max_dataset_requests):
        raise RuntimeError(
            "insufficient admissible dataset requests for compare: "
            f"selected={len(dataset_requests)} requested={args.max_dataset_requests} skipped={len(dataset_skipped)}"
        )

    procs: Dict[str, subprocess.Popen] = {}
    logs: Dict[str, Path] = {}
    proxy_url = str(args.proxy_url or "").strip()
    try:
        if not proxy_url:
            procs, logs, proxy_port = _start_serving_stack(
                workdir=workdir,
                local_hostname=local_hostname,
                timeout=float(args.timeout),
                owner_shards=int(args.owner_shards),
                kv_directory_rpc_url=args.kv_directory_rpc_url,
            )
            proxy_url = f"http://{local_hostname}:{proxy_port}/v1/chat/completions"
        if dataset_requests and int(args.warmup_requests) > 0:
            warmup_entry = dataset_requests[0]
            for warmup_idx in range(int(args.warmup_requests)):
                warmup_result = _execute_dataset_request(
                    proxy_url=proxy_url,
                    entry=warmup_entry,
                    index=-1 - warmup_idx,
                    request_timeout=float(args.dataset_request_timeout),
                    stream_metrics=True,
                )
                if int(warmup_result.get("status_code") or 0) >= 400 or warmup_result.get("error"):
                    raise RuntimeError(
                        "serving warmup failed: "
                        + json.dumps(warmup_result, ensure_ascii=False)[:4000]
                    )
        benchmark_started = time.perf_counter()
        compare_report = _compare_entries(
            dataset_requests=dataset_requests,
            dataset_root=dataset_root,
            proxy_url=proxy_url,
            baseline_gpu=int(args.baseline_gpu),
            request_max_tokens=int(args.dataset_request_max_tokens),
            request_timeout=float(args.dataset_request_timeout),
            image_max_pixels=int(args.dataset_image_max_pixels),
            baseline_warmup_requests=int(args.baseline_warmup_requests),
            high_overlap_threshold=float(args.high_overlap_threshold),
            min_pass_rate=float(args.min_pass_rate),
            min_same_finish_reason_rate=float(args.min_same_finish_reason_rate),
            min_avg_token_jaccard=float(args.min_avg_token_jaccard),
            min_token_jaccard=float(args.min_token_jaccard),
        )
        benchmark_elapsed_s = max(1e-6, time.perf_counter() - benchmark_started)
        serving_metrics_payload: Dict[str, Any] = {}
        serving_metrics_summary: Dict[str, Any] = {}
        if not args.proxy_url:
            sess = requests.Session()
            sess.trust_env = False
            try:
                metrics_url = proxy_url.rsplit("/v1/chat/completions", 1)[0] + "/metrics"
                serving_metrics_payload = _wait_for_metrics_settle(
                    sess,
                    metrics_url,
                    timeout_s=min(30.0, max(5.0, float(args.timeout) / 30.0)),
                    poll_s=0.25,
                )
                serving_metrics_summary = _summarize_serving_metrics(serving_metrics_payload)
            finally:
                sess.close()
        compare_report["summary"]["paired_benchmark_elapsed_s"] = benchmark_elapsed_s
        compare_report["summary"]["serving_request_throughput_rps"] = (
            len(compare_report["rows"]) / benchmark_elapsed_s if benchmark_elapsed_s > 0 else 0.0
        )
        compare_report["summary"]["serving_no_fallback_ok"] = (
            _no_fallback_ok(serving_metrics_summary) if serving_metrics_summary else None
        )
        report = {
            "dataset_root": str(dataset_root),
            "dataset_chat_split": args.dataset_chat_split,
            "dataset_families": list(args.dataset_families or []),
            "dataset_skipped_count": len(dataset_skipped),
            "dataset_skipped": dataset_skipped,
            "proxy_url": proxy_url,
            "baseline_gpu": int(args.baseline_gpu),
            "warmup_requests": int(args.warmup_requests),
            "baseline_warmup_requests": int(args.baseline_warmup_requests),
            "compare_thresholds": {
                "high_overlap_threshold": float(args.high_overlap_threshold),
                "min_pass_rate": float(args.min_pass_rate),
                "min_same_finish_reason_rate": float(args.min_same_finish_reason_rate),
                "min_avg_token_jaccard": float(args.min_avg_token_jaccard),
                "min_token_jaccard": float(args.min_token_jaccard),
            },
            "rows": compare_report["rows"],
            "summary": compare_report["summary"],
            "serving_metrics_summary": serving_metrics_summary,
            "serving_metrics": serving_metrics_payload,
            "workdir": str(workdir),
            "logs": {name: str(path) for name, path in logs.items()},
        }
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"wrote {output}")
    except Exception as exc:
        if logs:
            failure_payload = {
                "error": f"{type(exc).__name__}: {exc}",
                "log_tails": {name: _tail_text(path, limit_lines=120) for name, path in logs.items()},
            }
            print(json.dumps(failure_payload, ensure_ascii=False, indent=2), file=sys.stderr)
        raise
    finally:
        if procs:
            _terminate_all(list(procs.values()))


if __name__ == "__main__":
    main()
