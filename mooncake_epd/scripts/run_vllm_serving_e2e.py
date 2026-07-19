from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import copy
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Any
import shutil

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.demo.vllm_integration import (  # noqa: E402
    SCHEDULER_POLICIES,
    VLLMDisaggConfig,
    generate_configs,
)
from mooncake_epd.core.transfer import (  # noqa: E402
    collect_mooncake_worker_transport_evidence,
)
from mooncake_epd.scripts.benchmark_artifact_io import (  # noqa: E402
    write_raw_benchmark_artifacts,
)
from mooncake_epd.scripts.benchmark_request_variants import (  # noqa: E402
    apply_request_variation,
    apply_vllm_prefix_cache_policy,
    VLLM_PREFIX_CACHE_REUSE,
)
from mooncake_epd.tests.dataset import make_image  # noqa: E402


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _merge_packet_timings(result: Dict[str, Any], packet: Dict[str, Any]) -> None:
    raw = packet.get("_mooncake_epd_proxy_timings_ms")
    if not isinstance(raw, dict) or not raw:
        return
    timings = dict(result.get("epd_timing_ms") or {})
    for key, value in raw.items():
        try:
            timings[str(key)] = float(value)
        except Exception:
            continue
    result["epd_timing_ms"] = timings
    result["epd_timing_ms_header"] = json.dumps(
        timings,
        ensure_ascii=True,
        separators=(",", ":"),
    )


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


def _stats(values: List[float]) -> Dict[str, float]:
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
        "avg": float(statistics.fmean(values)),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _optional_positive_int(value: object) -> int | None:
    """Normalize CLI capacity knobs without treating zero as a real limit."""

    try:
        normalized = int(value or 0)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _stable_digest(value: object) -> str:
    """Return a content digest without duplicating images/data URLs in evidence."""

    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _dataset_request_evidence_row(
    entry: Dict[str, Any],
    *,
    index: int,
    phase: str,
) -> Dict[str, Any]:
    """Create a reproducible request trace without storing base64 image bytes.

    Dataset OpenAI payloads include inline image URLs.  Storing them again in
    every benchmark artifact is both needlessly large and makes the evidence
    bundle awkward to review.  The selected dataset sample identity, serving
    metadata and deterministic full-payload digest are sufficient to bind a
    response row to the exact request while the source split remains named in
    ``benchmark_config``.
    """

    request = dict(entry.get("request") or {})
    metadata = dict(request.get("metadata") or {})
    sample = dict(entry.get("sample") or {})
    return {
        "phase": str(phase),
        "index": int(index),
        "sample_id": sample.get("sample_id"),
        "workflow_id": metadata.get("workflow_id") or sample.get("workflow_id"),
        "source_workflow_id": (
            sample.get("source_workflow_id")
            or metadata.get("benchmark_source_workflow_id")
            or sample.get("workflow_id")
        ),
        "workload_family": entry.get("family"),
        "source_dataset": sample.get("source_dataset"),
        "model": request.get("model"),
        "max_tokens": request.get("max_tokens"),
        "temperature": request.get("temperature"),
        "estimated_prompt_len": metadata.get("estimated_prompt_len"),
        "admission_method": metadata.get("admission_method"),
        "request_sha256": _stable_digest(request),
    }


def _file_sha256(path: Path) -> str | None:
    """Digest an input split when available, returning explicit absence otherwise."""

    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _dataset_cycle_index(repeat_idx: int) -> int:
    """Map measurement and negative warmup indices onto one stable cycle.

    Both the strict EPD and colocated-vLLM runners use negative request indices
    for warmup.  Keeping this mapping here prevents the two runners from
    silently selecting a different dataset sample for the same benchmark
    phase/index pair.
    """

    index = int(repeat_idx)
    return index if index >= 0 else -index - 1


def _resolve_dataset_warmup_requests(
    *,
    requested_warmups: int,
    dataset_entries: List[Dict[str, Any]],
    cover_dataset_cycle: bool,
) -> int:
    """Return the warmup count required by an explicit dataset protocol.

    A single warmup request only primes the first selected sample.  For a
    mixed multimodal replay that leaves later image shapes, prompt lengths,
    and output shapes cold, which can both distort measured TTFT and make a
    cold-versus-hot output comparison look like a serving regression.  The
    opt-in coverage mode warms every selected entry at least once while
    preserving the caller's larger explicit warmup budget.
    """

    requested = max(0, int(requested_warmups))
    if not cover_dataset_cycle or not dataset_entries:
        return requested
    return max(requested, len(dataset_entries))


def _dataset_warmup_coverage(
    dataset_entries: List[Dict[str, Any]],
    warmup_responses: List[Dict[str, Any]],
    *,
    cover_dataset_cycle: bool,
) -> Dict[str, Any]:
    """Summarize which selected dataset samples actually received warmup.

    The response rows are the authoritative evidence because a request can
    fail before reaching a worker.  Keep the output compact and JSON-safe so
    it can live in both baseline and EPD benchmark summaries.
    """

    if not dataset_entries:
        return {
            "applicable": False,
            "coverage_requested": bool(cover_dataset_cycle),
            "selected_sample_count": 0,
            "warmed_sample_count": 0,
            "complete": False,
        }

    selected_ids = {
        str((entry.get("sample") or {}).get("sample_id"))
        for entry in dataset_entries
        if (entry.get("sample") or {}).get("sample_id") is not None
    }
    warmed_ids = {
        str(response.get("sample_id"))
        for response in warmup_responses
        if isinstance(response, dict)
        and response.get("sample_id") is not None
        and int(response.get("status_code", 0) or 0) < 400
        and not response.get("error")
    }
    covered_ids = selected_ids & warmed_ids
    missing_ids = sorted(selected_ids - covered_ids)
    return {
        "applicable": True,
        "coverage_requested": bool(cover_dataset_cycle),
        "selected_sample_count": len(selected_ids),
        "warmed_sample_count": len(covered_ids),
        "warmup_response_count": len(warmup_responses),
        "complete": bool(selected_ids) and not missing_ids,
        "missing_sample_ids": missing_ids,
    }


def _materialize_dataset_replay_entry(
    dataset_entries: List[Dict[str, Any]],
    *,
    repeat_idx: int,
    workflow_prefix: str,
    workflow_id_mode: str,
    request_variation: str,
    phase: str,
    vllm_prefix_cache_mode: str = VLLM_PREFIX_CACHE_REUSE,
) -> Dict[str, Any]:
    """Clone one canonical dataset request for a paired serving replay.

    The source chat-split entry is never mutated: the same selected entries
    can be replayed by the EPD and baseline runners in one Python process.
    ``unique`` makes each request a distinct workflow, which isolates serving
    latency from workflow-state reuse.  ``source`` intentionally preserves the
    dataset workflow id for explicit cache/workflow studies.  In both cases we
    retain the original id as evidence, so a unique benchmark workflow cannot
    be mistaken for a source-workflow replay.
    """

    if not dataset_entries:
        raise ValueError("dataset replay requires at least one selected entry")
    normalized_mode = str(workflow_id_mode or "unique").strip().lower()
    if normalized_mode not in {"unique", "source"}:
        raise ValueError(
            "dataset workflow_id_mode must be 'unique' or 'source', "
            f"got {workflow_id_mode!r}"
        )

    cycle_index = _dataset_cycle_index(repeat_idx)
    source_index = cycle_index % len(dataset_entries)
    entry = copy.deepcopy(dataset_entries[source_index])
    request = dict(entry.get("request") or {})
    sample = dict(entry.get("sample") or {})
    metadata = dict(request.get("metadata") or {})
    source_workflow_id = str(
        metadata.get("workflow_id")
        or sample.get("workflow_id")
        or sample.get("sample_id")
        or f"dataset-{source_index}"
    )
    workflow_id = (
        source_workflow_id
        if normalized_mode == "source"
        else f"{workflow_prefix}-r{int(repeat_idx)}"
    )
    metadata.update(
        {
            "workflow_id": workflow_id,
            "benchmark_source_workflow_id": source_workflow_id,
            "benchmark_dataset_cycle_index": cycle_index,
            "benchmark_dataset_source_index": source_index,
        }
    )
    request["metadata"] = metadata
    entry["request"] = request
    sample["source_workflow_id"] = source_workflow_id
    sample["workflow_id"] = workflow_id
    entry["sample"] = sample
    entry["dataset_cycle_index"] = cycle_index
    entry["dataset_source_index"] = source_index
    entry["dataset_workflow_id_mode"] = normalized_mode
    entry["request_variation_id"] = apply_request_variation(
        request,
        mode=request_variation,
        phase=phase,
        repeat_idx=repeat_idx,
    )
    entry["vllm_prefix_cache_id"] = apply_vllm_prefix_cache_policy(
        request,
        mode=vllm_prefix_cache_mode,
        phase=phase,
        repeat_idx=repeat_idx,
    )
    return entry


def _dataset_replay_context(
    dataset_entries: List[Dict[str, Any]],
    *,
    dataset_root: str,
    chat_split: str,
    families: List[str] | None,
    max_input_len: int,
    request_max_tokens: int,
    skip_oversized: bool,
    image_max_pixels: int,
    agent_pd_labels: bool,
    workflow_id_mode: str,
) -> Dict[str, Any]:
    """Return a compact, comparable manifest for paired dataset replays."""

    root = Path(dataset_root).resolve()
    split_path = root / "chat_splits" / f"{chat_split}.jsonl"
    selected_rows = [
        _dataset_request_evidence_row(entry, index=index, phase="selection")
        for index, entry in enumerate(dataset_entries)
    ]
    selected_samples = [
        {
            "sample_id": row.get("sample_id"),
            "workflow_id": row.get("workflow_id"),
            "workload_family": row.get("workload_family"),
            "source_dataset": row.get("source_dataset"),
            "request_sha256": row.get("request_sha256"),
        }
        for row in selected_rows
    ]
    split_sha256 = _file_sha256(split_path)
    request_fingerprint = _stable_digest(
        {
            "dataset_split_sha256": split_sha256,
            "selected_samples": selected_samples,
            "request_max_tokens": int(request_max_tokens),
            "temperature": 0.0,
            "image_max_pixels": int(image_max_pixels),
            "workflow_id_mode": str(workflow_id_mode),
        }
    )
    return {
        "root": str(root),
        "chat_split": str(chat_split),
        "split_path": str(split_path),
        "split_sha256": split_sha256,
        "selected_count": len(dataset_entries),
        "selected_samples": selected_samples,
        "entry_cycle": "round_robin",
        "workflow_id_mode": str(workflow_id_mode),
        "admission": {
            "max_input_len": int(max_input_len),
            "request_max_tokens": int(request_max_tokens),
            "skip_oversized": bool(skip_oversized),
            "image_max_pixels": int(image_max_pixels),
            "agent_pd_labels": bool(agent_pd_labels),
        },
        "families_requested": list(families or []),
        "request_fingerprint": request_fingerprint,
    }


def _worker_dispatch_balance(
    results: List[Dict[str, Any]],
    *,
    stage: str,
    configured_workers: int,
) -> Dict[str, Any]:
    """Expose real worker coverage/entropy for multi-worker serving runs."""

    stage = str(stage)
    configured_workers = max(0, int(configured_workers))
    counts: Dict[str, int] = {
        f"{stage}-{idx}": 0 for idx in range(configured_workers)
    }
    key = f"{stage}_worker_id"
    for result in results:
        worker_id = str(result.get(key) or "").strip()
        if worker_id:
            counts[worker_id] = int(counts.get(worker_id, 0)) + 1
    total = sum(counts.values())
    active = sum(1 for value in counts.values() if value > 0)
    cardinality = max(configured_workers, len(counts))
    if cardinality <= 1 and total > 0:
        entropy = 1.0
    elif cardinality > 1 and total > 0:
        entropy = -sum(
            (value / total) * math.log(value / total)
            for value in counts.values()
            if value > 0
        ) / math.log(cardinality)
    else:
        entropy = 0.0
    return {
        "configured_workers": configured_workers,
        "active_workers": active,
        "total_dispatches": total,
        "counts": counts,
        "normalized_entropy": entropy,
        "max_share": max(counts.values()) / total if total else 0.0,
    }


def _worker_transport_logs(logs: Dict[str, Path]) -> Dict[str, Path]:
    """Return exactly the native Prefill/Decode worker logs for P→D proof."""

    return {
        key: path
        for key, path in logs.items()
        if key.startswith(("prefill-", "decode-"))
    }


def _record_runtime_transport_evidence(
    summary: Dict[str, Any],
    *,
    logs: Dict[str, Path],
    requested: str,
    enforce: bool,
) -> Dict[str, Any]:
    """Persist native P→D transport evidence and fail closed in strict mode."""

    evidence = collect_mooncake_worker_transport_evidence(
        _worker_transport_logs(logs),
        requested=str(requested),
    )
    summary["transport_runtime_evidence"] = evidence
    if enforce and bool(evidence.get("applicable", False)) and not bool(evidence.get("pass", False)):
        details = "; ".join(str(item) for item in evidence.get("failures", ()))
        raise AssertionError(
            "native Mooncake transport evidence failed; refusing strict protocol claim"
            + (f": {details}" if details else "")
        )
    return evidence


def _extract_port(start_script: Path, flag: str = "--port") -> int:
    text = start_script.read_text(encoding="utf-8")
    tokens = text.replace("\n", " ").split()
    for idx, tok in enumerate(tokens):
        if tok == flag and idx + 1 < len(tokens):
            return int(tokens[idx + 1])
    raise RuntimeError(f"failed to extract {flag} from {start_script}")


def _as_str_list(value: object, fallback: List[str] | None = None) -> List[str]:
    if value is None:
        return list(fallback or [])
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    return [str(value)]


def _as_int_list(value: object, fallback: List[int] | None = None) -> List[int]:
    if value is None:
        return list(fallback or [])
    if isinstance(value, (list, tuple)):
        return [int(item) for item in value]
    return [int(value)]


def _proc_env() -> Dict[str, str]:
    env = os.environ.copy()
    # See ``_common_env_block`` in demo.vllm_integration.  Clear proxy
    # variables before launching every generated script as a second boundary:
    # a script may spawn an EngineCore after its shell exits, and httpx honors
    # ALL_PROXY even when HTTP(S)_PROXY is absent.
    for key in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        env.pop(key, None)
    env["NO_PROXY"] = "127.0.0.1,localhost,::1"
    env["no_proxy"] = env["NO_PROXY"]
    env.setdefault("OPENAI_API_KEY", "sk-local")
    env["PYTHONPATH"] = f"{REPO_ROOT.parent}:{env.get('PYTHONPATH', '')}"
    return env


def _data_url_for_demo_image(image_name: str = "room") -> str:
    image = make_image(image_name)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    payload = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{payload}"


def _data_url_for_image_path(path: Path) -> str:
    suffix = path.suffix.lower()
    mime = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
    payload = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{payload}"


def _convert_dataset_messages(
    messages: List[Dict[str, Any]],
    dataset_root: Path,
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
                new_content.append({"type": "image_url", "image_url": {"url": _data_url_for_image_path(path)}})
            elif part_type == "image_url":
                new_content.append(part)
            elif part_type == "text":
                new_content.append({"type": "text", "text": str(part.get("text", ""))})
        converted.append({"role": role, "content": new_content})
    return converted


def _dataset_messages_for_processor(
    messages: List[Dict[str, Any]],
    dataset_root: Path,
    *,
    image_max_pixels: int = 0,
) -> List[Dict[str, Any]]:
    """Convert dataset chat messages to Qwen processor input with local image paths.

    The serving request itself uses OpenAI-compatible data URLs.  For context-length
    admission we intentionally run the same lightweight HF/Qwen preprocessing logic
    on local image paths before sending the request.  This avoids classifying a
    model-context 400 as a Mooncake transfer failure during real serving runs.
    """

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
                image_part: Dict[str, Any] = {"type": "image", "image": str(path)}
                if image_max_pixels > 0:
                    image_part["max_pixels"] = int(image_max_pixels)
                new_content.append(image_part)
            elif part_type == "image_url":
                new_content.append(part)
            elif part_type == "text":
                new_content.append({"type": "text", "text": str(part.get("text", ""))})
        converted.append({"role": role, "content": new_content})
    return converted


_PROCESSOR_CACHE: Dict[str, Any] = {}


def _estimate_dataset_prompt_len(
    item: Dict[str, Any],
    dataset_root: Path,
    *,
    model: str,
    image_max_pixels: int = 0,
) -> tuple[int | None, str, Dict[str, Any]]:
    """Estimate processed prompt length for a real dataset sample.

    Prefer exact HF processor tokenization because Qwen3-VL expands each image into
    many special tokens after resize.  If the local processor stack is unavailable,
    fall back to a conservative Qwen-style image-grid estimate so the runner can
    still make an explicit admission decision instead of sending known-bad samples.
    """

    raw_messages = list(item.get("messages") or [])
    try:
        from qwen_vl_utils import process_vision_info  # type: ignore
        from transformers import AutoProcessor  # type: ignore

        cache_key = str(Path(model).resolve() if Path(model).exists() else model)
        processor = _PROCESSOR_CACHE.get(cache_key)
        if processor is None:
            processor = AutoProcessor.from_pretrained(
                model,
                trust_remote_code=True,
            )
            _PROCESSOR_CACHE[cache_key] = processor
        processor_messages = _dataset_messages_for_processor(
            raw_messages,
            dataset_root,
            image_max_pixels=image_max_pixels,
        )
        text = processor.apply_chat_template(
            processor_messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        image_inputs, video_inputs = process_vision_info(processor_messages)
        kwargs: Dict[str, Any] = {}
        if image_max_pixels > 0:
            kwargs["max_pixels"] = int(image_max_pixels)
        inputs = processor(
            text=[text],
            images=image_inputs,
            videos=video_inputs,
            padding=True,
            return_tensors="pt",
            **kwargs,
        )
        input_ids = inputs.get("input_ids")
        prompt_len = int(input_ids.shape[-1])
        details: Dict[str, Any] = {"method": "hf_processor_exact"}
        image_grid = inputs.get("image_grid_thw")
        if image_grid is not None:
            details["image_grid_thw"] = image_grid.tolist()
        return prompt_len, "hf_processor_exact", details
    except Exception as exc:
        # Fallback estimate mirrors Qwen3-VL vLLM placeholder expansion:
        # smart-resize to a multiple of patch_size * merge_size, then count
        # merged vision tokens as resized_pixels / (patch_size * merge_size)^2.
        estimated = int(item.get("prompt_token_len", 0) or 0)
        image_details: List[Dict[str, Any]] = []
        try:
            from PIL import Image  # type: ignore

            max_pixels = int(image_max_pixels or 16_777_216)
            factor = 32
            for image_meta in item.get("images") or []:
                if not isinstance(image_meta, dict):
                    continue
                rel = str(image_meta.get("path_or_url") or image_meta.get("path") or "")
                if not rel:
                    continue
                path = dataset_root / rel
                if not path.exists():
                    continue
                with Image.open(path) as image:
                    width, height = image.size
                h_bar = max(factor, round(height / factor) * factor)
                w_bar = max(factor, round(width / factor) * factor)
                if h_bar * w_bar > max_pixels:
                    beta = math.sqrt((height * width) / max_pixels)
                    h_bar = max(factor, math.floor(height / beta / factor) * factor)
                    w_bar = max(factor, math.floor(width / beta / factor) * factor)
                vision_tokens = max(1, (h_bar * w_bar) // (factor * factor))
                estimated += vision_tokens
                image_details.append(
                    {
                        "path": rel,
                        "width": width,
                        "height": height,
                        "resized_width": w_bar,
                        "resized_height": h_bar,
                        "vision_tokens": vision_tokens,
                    }
                )
            return (
                estimated if estimated > 0 else None,
                "qwen_static_estimate",
                {
                    "method": "qwen_static_estimate",
                    "estimator_error": f"{type(exc).__name__}: {exc}",
                    "images": image_details,
                },
            )
        except Exception as fallback_exc:
            return (
                None,
                "unavailable",
                {
                    "method": "unavailable",
                    "estimator_error": f"{type(exc).__name__}: {exc}",
                    "fallback_error": f"{type(fallback_exc).__name__}: {fallback_exc}",
                },
            )


def _load_dataset_requests(
    *,
    dataset_root: str | None,
    chat_split: str,
    max_requests: int,
    families: List[str] | None,
    model: str,
    max_input_len: int,
    request_max_tokens: int = 32,
    skip_oversized: bool = True,
    image_max_pixels: int = 0,
    agent_pd_labels: bool = False,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not dataset_root or max_requests <= 0:
        return [], []
    root = Path(dataset_root).resolve()
    path = root / "chat_splits" / f"{chat_split}.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"dataset chat split not found: {path}")
    wanted_order = list(dict.fromkeys(str(family) for family in (families or [])))
    wanted = set(wanted_order)
    by_family: Dict[str, Dict[str, Any]] = {}
    sequential: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    target_family_coverage = min(max_requests, len(wanted_order)) if wanted_order else 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            item = json.loads(line)
            family = str(item.get("workload_family") or (item.get("extensions") or {}).get("workload_family") or "unknown")
            if wanted and family not in wanted:
                continue
            try:
                messages = _convert_dataset_messages(list(item.get("messages") or []), root)
            except FileNotFoundError:
                raise
            prompt_len, admission_method, admission_details = _estimate_dataset_prompt_len(
                item,
                root,
                model=model,
                image_max_pixels=image_max_pixels,
            )
            max_prompt_len = max(1, int(max_input_len) - max(0, int(request_max_tokens)))
            if (
                skip_oversized
                and prompt_len is not None
                and int(max_input_len) > 0
                and prompt_len > max_prompt_len
            ):
                skipped.append(
                    {
                        "sample_id": item.get("sample_id"),
                        "workflow_id": item.get("workflow_id"),
                        "workload_family": family,
                        "source_dataset": item.get("source_dataset"),
                        "reason": "estimated_context_too_long",
                        "estimated_prompt_len": prompt_len,
                        "request_max_tokens": int(request_max_tokens),
                        "max_input_len": int(max_input_len),
                        "max_prompt_len": max_prompt_len,
                        "admission_method": admission_method,
                        "admission_details": admission_details,
                    }
                )
                continue
            metadata = {
                "workflow_id": str(item.get("workflow_id") or item.get("sample_id") or f"dataset-{len(sequential)}"),
                "sample_id": item.get("sample_id"),
                "workload_family": family,
                "source_dataset": item.get("source_dataset"),
                "estimated_prompt_len": prompt_len,
                "admission_method": admission_method,
            }
            if agent_pd_labels:
                metadata.update(_agent_pd_metadata_for_dataset(item, family, prompt_len))
            request = {
                "model": model,
                "messages": messages,
                "max_tokens": int(request_max_tokens),
                "temperature": 0.0,
                "metadata": metadata,
            }
            if image_max_pixels > 0:
                request["mm_processor_kwargs"] = {"max_pixels": int(image_max_pixels)}
            sequential.append({"request": request, "sample": item, "family": family})
            by_family.setdefault(family, {"request": request, "sample": item, "family": family})
            if len(by_family) >= max_requests and not wanted:
                break
            if (
                wanted
                and len(by_family) >= target_family_coverage
                and len(sequential) >= max_requests
            ):
                break
    if not wanted:
        selected = list(by_family.values())
    else:
        selected = [
            by_family[family]
            for family in wanted_order
            if family in by_family
        ][:max_requests]
    if len(selected) < max_requests:
        seen = {entry["sample"].get("sample_id") for entry in selected}
        for entry in sequential:
            sid = entry["sample"].get("sample_id")
            if sid in seen:
                continue
            selected.append(entry)
            seen.add(sid)
            if len(selected) >= max_requests:
                break
    return selected[:max_requests], skipped


def _bucket_for_tokens(tokens: int | None, *, output: bool = False) -> str:
    value = int(tokens or 0)
    if output:
        if value <= 128:
            return "0-128"
        if value <= 512:
            return "128-512"
        if value <= 1024:
            return "512-1k"
        return "1k-2k"
    if value <= 512:
        return "0-512"
    if value <= 1024:
        return "512-1k"
    if value <= 2048:
        return "1k-2k"
    if value <= 4096:
        return "2k-4k"
    return "4k-8k"


def _agent_pd_metadata_for_dataset(
    item: Dict[str, Any],
    family: str,
    prompt_len: int | None,
) -> Dict[str, Any]:
    """Derive Agent-PD labels from real dataset workload shape.

    This is not a quality labeler. It only supplies scheduling intent for real
    serving experiments based on observable pressure: long multimodal context
    and multi-step workflow families are prefill-heavy; short one-shot samples
    are latency-sensitive; medium/multi-turn cases are hybrid.
    """

    prompt_tokens = int(prompt_len or item.get("prompt_token_len") or 0)
    family = str(family)
    source_dataset = str(item.get("source_dataset") or "")
    if prompt_tokens >= 1536 or family in {"W3"}:
        agent_type = "thinking"
        routing_target = "high_prefill_pool"
        reasoning_depth = 5
        latency_sensitivity = "medium"
        quality_priority = "accuracy"
        expected_output_bucket = "128-512"
        sla_ms = 15000
    elif family in {"W4"} or prompt_tokens >= 768:
        agent_type = "hybrid"
        routing_target = "mixed"
        reasoning_depth = 4
        latency_sensitivity = "high"
        quality_priority = "balanced"
        expected_output_bucket = "128-512"
        sla_ms = 5000
    else:
        agent_type = "interactive"
        routing_target = "low_latency_decode_pool"
        reasoning_depth = 1
        latency_sensitivity = "high"
        quality_priority = "latency"
        expected_output_bucket = "0-128"
        sla_ms = 1200 if source_dataset != "docvqa" else 2000
    return {
        "agent_type": agent_type,
        "routing_target": routing_target,
        "reasoning_depth": reasoning_depth,
        "context_length_level": (
            "long" if prompt_tokens >= 1536 else "medium" if prompt_tokens >= 768 else "short"
        ),
        "input_tokens_bucket": _bucket_for_tokens(prompt_tokens),
        "expected_output_tokens_bucket": expected_output_bucket,
        "latency_sensitivity": latency_sensitivity,
        "quality_priority": quality_priority,
        "sla_ms": sla_ms,
        "agent_pd_label_source": "dataset_workload_shape",
    }


def _validate_local_hostname(local_hostname: str, *, allow_loopback: bool) -> str:
    host = str(local_hostname or "").strip()
    if not host:
        raise ValueError("local_hostname must not be empty")
    if not allow_loopback and host in {"127.0.0.1", "localhost"}:
        raise ValueError(
            "loopback local_hostname is disabled for this run; pass --allow-loopback "
            "or provide a real routable host/IP"
        )
    return host


def _resolve_schedule_path(dataset_root: str | None, schedule: str | None) -> Path | None:
    if not schedule:
        return None
    candidate = Path(schedule)
    if candidate.exists():
        return candidate.resolve()
    if dataset_root:
        rooted = Path(dataset_root).resolve() / "schedules" / schedule
        if rooted.exists():
            return rooted
    raise FileNotFoundError(f"dataset arrival schedule not found: {schedule}")


def _load_schedule_events(path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    events.sort(key=lambda item: float(item.get("arrival_ms", 0.0) or 0.0))
    return events


def _build_dataset_dispatch_plan(
    dataset_requests: List[Dict[str, Any]],
    *,
    dataset_root: str | None,
    arrival_schedule: str | None,
    qps: float,
    deadline_ms: float,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    plan: List[Dict[str, Any]] = []
    schedule_path = _resolve_schedule_path(dataset_root, arrival_schedule)
    schedule_summary: Dict[str, Any] = {
        "source": None if schedule_path is None else str(schedule_path),
        "mode": "serial",
        "normalized_load_ratio": None,
        "matched_events": 0,
    }

    if schedule_path is not None:
        events = _load_schedule_events(schedule_path)
        by_workflow: Dict[str, List[Dict[str, Any]]] = {}
        for event in events:
            workflow_id = str(event.get("workflow_id") or "")
            by_workflow.setdefault(workflow_id, []).append(event)
        base_arrival_ms: float | None = None
        unmatched: List[Dict[str, Any]] = []
        for entry in dataset_requests:
            workflow_id = str(
                entry["sample"].get("workflow_id")
                or (entry["request"].get("metadata") or {}).get("workflow_id")
                or ""
            )
            bucket = by_workflow.get(workflow_id) or []
            if bucket:
                event = bucket.pop(0)
                arrival_ms = float(event.get("arrival_ms", 0.0) or 0.0)
                base_arrival_ms = arrival_ms if base_arrival_ms is None else min(base_arrival_ms, arrival_ms)
                enriched = dict(entry)
                enriched["arrival_ms"] = arrival_ms
                enriched["schedule_event"] = event
                plan.append(enriched)
            else:
                unmatched.append(entry)
        if base_arrival_ms is not None:
            for item in plan:
                item["arrival_ms"] = max(0.0, float(item.get("arrival_ms", 0.0)) - base_arrival_ms)
        tail_start_ms = max((float(item.get("arrival_ms", 0.0)) for item in plan), default=0.0)
        spacing_ms = 1000.0 / qps if qps > 0 else 0.0
        for idx, entry in enumerate(unmatched, start=1):
            enriched = dict(entry)
            enriched["arrival_ms"] = tail_start_ms + spacing_ms * idx
            plan.append(enriched)
        if events:
            first = events[0]
            schedule_summary["mode"] = str(first.get("arrival_mode") or "scheduled")
            schedule_summary["normalized_load_ratio"] = first.get("normalized_load_ratio")
        schedule_summary["matched_events"] = len(dataset_requests) - len(unmatched)
    else:
        spacing_ms = 1000.0 / qps if qps > 0 else 0.0
        for idx, entry in enumerate(dataset_requests):
            enriched = dict(entry)
            enriched["arrival_ms"] = spacing_ms * idx
            plan.append(enriched)
        if qps > 0:
            schedule_summary["mode"] = "fixed_qps"
            schedule_summary["normalized_load_ratio"] = None

    for entry in plan:
        metadata = dict((entry["request"].get("metadata") or {}))
        if deadline_ms > 0:
            metadata["deadline_ms"] = float(deadline_ms)
        entry["request"]["metadata"] = metadata
    plan.sort(key=lambda item: float(item.get("arrival_ms", 0.0) or 0.0))
    return plan, schedule_summary


def _extract_stream_text(payload: Dict[str, Any]) -> str:
    choices = list(payload.get("choices") or [])
    if not choices:
        return ""
    return _extract_choice_text(dict(choices[0] or {}))


def _extract_choice_text(choice: Dict[str, Any]) -> str:
    def _flatten_content(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
            return "".join(parts)
        return ""

    delta = choice.get("delta")
    if isinstance(delta, dict):
        return _flatten_content(delta.get("content"))
    message = choice.get("message")
    if isinstance(message, dict):
        return _flatten_content(message.get("content"))
    return ""


def _http_error_evidence(exc: BaseException) -> Dict[str, Any]:
    """Return bounded, response-side evidence for a failed HTTP request.

    Benchmark callers previously called ``raise_for_status`` and then retained
    only the exception string.  For a proxy-generated 4xx/5xx that discards
    the useful FastAPI ``detail`` field, making a real failure impossible to
    distinguish from a client/network error in the benchmark artifact.  Keep
    only the status and a bounded response-side diagnostic; never retain or
    replay the request payload here.
    """

    response = getattr(exc, "response", None)
    if response is None:
        return {}
    evidence: Dict[str, Any] = {}
    try:
        evidence["status_code"] = int(response.status_code)
    except (AttributeError, TypeError, ValueError):
        pass

    detail: Any = None
    try:
        payload = response.json()
    except (AttributeError, TypeError, ValueError):
        payload = None
    if isinstance(payload, dict):
        detail = payload.get("detail", payload.get("error"))
    elif isinstance(payload, str):
        detail = payload
    if detail is None:
        try:
            detail = response.text
        except AttributeError:
            detail = None
    if detail is not None:
        text = str(detail).strip()
        if text:
            evidence["error_response_detail"] = text[:1000]
    return evidence


def _execute_dataset_request(
    *,
    proxy_url: str,
    entry: Dict[str, Any],
    index: int,
    request_timeout: float,
    stream_metrics: bool,
) -> Dict[str, Any]:
    started = time.perf_counter()
    result: Dict[str, Any] = {
        "index": index,
        "sample_id": entry["sample"].get("sample_id"),
        "workflow_id": entry["sample"].get("workflow_id"),
        "workload_family": entry["family"],
        "source_dataset": entry["sample"].get("source_dataset"),
        "estimated_prompt_len": (entry["request"].get("metadata") or {}).get("estimated_prompt_len"),
        "admission_method": (entry["request"].get("metadata") or {}).get("admission_method"),
        "arrival_ms": float(entry.get("arrival_ms", 0.0) or 0.0),
        "schedule_event": entry.get("schedule_event"),
    }
    payload = dict(entry["request"])
    session = requests.Session()
    session.trust_env = False
    try:
        if stream_metrics:
            payload["stream"] = True
            payload["stream_options"] = {"include_usage": True}
            resp = session.post(
                proxy_url,
                json=payload,
                timeout=max(1.0, float(request_timeout)),
                stream=True,
            )
            resp.raise_for_status()
            result.update(
                {
                    "status_code": resp.status_code,
                    "routing_path": resp.headers.get("x-epd-routing-path"),
                    "admission": resp.headers.get("x-epd-admission"),
                    "degrade_level": resp.headers.get("x-epd-degrade-level"),
                    "prefill_worker_id": resp.headers.get("x-epd-prefill-worker"),
                    "decode_worker_id": resp.headers.get("x-epd-decode-worker"),
                    "epd_timing_ms_header": resp.headers.get("x-epd-timing-ms"),
                }
            )
            if result.get("epd_timing_ms_header"):
                try:
                    result["epd_timing_ms"] = json.loads(str(result["epd_timing_ms_header"]))
                except Exception:
                    result["epd_timing_ms_parse_error"] = str(result.get("epd_timing_ms_header"))
            first_event_ms: float | None = None
            first_chunk_ms: float | None = None
            last_chunk_at: float | None = None
            finish_reason = None
            usage = None
            chunks = 0
            text_parts: List[str] = []
            line_buffer: List[str] = []
            for raw_line in resp.iter_lines(decode_unicode=True):
                now = time.perf_counter()
                if not raw_line:
                    continue
                if not raw_line.startswith("data:"):
                    continue
                data = raw_line[5:].strip()
                if data == "[DONE]":
                    break
                line_buffer.append(data)
                try:
                    packet = json.loads(data)
                except Exception:
                    continue
                _merge_packet_timings(result, packet)
                chunks += 1
                if first_event_ms is None:
                    first_event_ms = (now - started) * 1000.0
                last_chunk_at = now
                delta_text = _extract_stream_text(packet)
                text_parts.append(delta_text)
                # OpenAI-compatible vLLM streams often send an initial role-only
                # chunk with empty content. Counting that as TTFT hides the real
                # model first-token latency and makes single-server baselines
                # look artificially good. TTFT here means first non-empty content
                # delta; first_event_ms remains available for transport debugging.
                if first_chunk_ms is None and delta_text:
                    first_chunk_ms = (now - started) * 1000.0
                choices = list(packet.get("choices") or [])
                if choices:
                    finish_reason = dict(choices[0] or {}).get("finish_reason") or finish_reason
                usage = packet.get("usage") or usage
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            completion_tokens = 0
            if isinstance(usage, dict):
                completion_tokens = int(usage.get("completion_tokens", 0) or 0)
            response_text = "".join(text_parts)
            result.update(
                {
                    "elapsed_ms": elapsed_ms,
                    "ttft_ms": elapsed_ms if first_chunk_ms is None else first_chunk_ms,
                    "first_event_ms": first_event_ms,
                    "tpot_ms": (
                        max(0.0, elapsed_ms - first_chunk_ms) / max(1, completion_tokens)
                        if first_chunk_ms is not None and completion_tokens > 0
                        else None
                    ),
                    "stream_chunk_count": chunks,
                    "finish_reason": finish_reason,
                    "usage": usage,
                    "completion_tokens": completion_tokens,
                    "prompt_tokens": (
                        int(usage.get("prompt_tokens", 0) or 0)
                        if isinstance(usage, dict)
                        else 0
                    ),
                    "total_tokens": (
                        int(usage.get("total_tokens", 0) or 0)
                        if isinstance(usage, dict)
                        else 0
                    ),
                    "response_text": response_text,
                    "response_content_len": len(response_text),
                    "response_head": response_text[:500],
                    "response_lines": line_buffer[:64],
                }
            )
        else:
            resp = session.post(
                proxy_url,
                json=payload,
                timeout=max(1.0, float(request_timeout)),
            )
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            result.update(
                {
                    "status_code": resp.status_code,
                    "elapsed_ms": elapsed_ms,
                    "ttft_ms": elapsed_ms,
                    "routing_path": resp.headers.get("x-epd-routing-path"),
                    "admission": resp.headers.get("x-epd-admission"),
                    "degrade_level": resp.headers.get("x-epd-degrade-level"),
                    "prefill_worker_id": resp.headers.get("x-epd-prefill-worker"),
                    "decode_worker_id": resp.headers.get("x-epd-decode-worker"),
                    "epd_timing_ms_header": resp.headers.get("x-epd-timing-ms"),
                    "response_head": resp.text[:500],
                }
            )
            if result.get("epd_timing_ms_header"):
                try:
                    result["epd_timing_ms"] = json.loads(str(result["epd_timing_ms_header"]))
                except Exception:
                    result["epd_timing_ms_parse_error"] = str(result.get("epd_timing_ms_header"))
            try:
                response_json = resp.json()
                result["response_json"] = response_json
                choices = list(response_json.get("choices") or [])
                if choices:
                    first_choice = dict(choices[0] or {})
                    message = dict(first_choice.get("message") or {})
                    result["finish_reason"] = first_choice.get("finish_reason")
                    response_text = _extract_choice_text(first_choice)
                    result["response_text"] = response_text
                    result["response_content_len"] = len(str(message.get("content") or ""))
                    result["response_head"] = response_text[:500] if response_text else result.get("response_head")
                result["usage"] = response_json.get("usage")
                usage = result.get("usage")
                if isinstance(usage, dict):
                    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
                    result["completion_tokens"] = completion_tokens
                    result["prompt_tokens"] = int(usage.get("prompt_tokens", 0) or 0)
                    result["total_tokens"] = int(usage.get("total_tokens", 0) or 0)
                    if completion_tokens > 0:
                        result["tpot_ms"] = elapsed_ms / completion_tokens
            except Exception as parse_exc:
                result["response_parse_error"] = f"{type(parse_exc).__name__}: {parse_exc}"
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        http_evidence = _http_error_evidence(exc)
        result.update(
            {
                "status_code": http_evidence.get("status_code"),
                "elapsed_ms": elapsed_ms,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        result.update(http_evidence)
    finally:
        session.close()
    return result


def _agent_pd_probe_entries(model: str, count: int) -> List[Dict[str, Any]]:
    """Build real OpenAI-compatible Agent-PD probe requests.

    These are intentionally small enough for a fast serving smoke run, but carry
    the same scheduling metadata shape as the dedicated Agent-PD dataset.  The
    proxy/control-plane must route them through the normal admission hot path;
    no test doubles or synthetic metrics are used.
    """

    if count <= 0:
        return []
    long_context = (
        "Agent PD Disaggregation 调度需要同时考虑 Prefill 压力、Decode 首 token "
        "延迟、工作流跨步骤复用、工具调用阻塞、SLA 与资源池健康度。"
    )
    templates: List[Dict[str, Any]] = [
        {
            "task_id": "agent_pd_thinking_probe",
            "family": "agent_pd_thinking",
            "query": "请分析以下 Agent PD Disaggregation 策略的潜在瓶颈并给出优先级建议："
            + long_context * 20,
            "metadata": {
                "agent_type": "thinking",
                "routing_target": "high_prefill_pool",
                "reasoning_depth": 5,
                "context_length_level": "long",
                "input_tokens_bucket": "2k-4k",
                "expected_output_tokens_bucket": "512-1k",
                "latency_sensitivity": "medium",
                "quality_priority": "accuracy",
                "sla_ms": 15000,
            },
        },
        {
            "task_id": "agent_pd_interactive_probe",
            "family": "agent_pd_interactive",
            "query": "继续，用一句话说。",
            "metadata": {
                "agent_type": "interactive",
                "routing_target": "low_latency_decode_pool",
                "reasoning_depth": 1,
                "context_length_level": "short",
                "input_tokens_bucket": "0-512",
                "expected_output_tokens_bucket": "0-128",
                "latency_sensitivity": "high",
                "quality_priority": "latency",
                "sla_ms": 800,
            },
        },
        {
            "task_id": "agent_pd_hybrid_probe",
            "family": "agent_pd_hybrid",
            "query": "先快速告诉我结论，然后简要解释为什么 Agent PD 调度需要区分 Prefill 和 Decode 资源。",
            "metadata": {
                "agent_type": "hybrid",
                "routing_target": "mixed",
                "reasoning_depth": 4,
                "context_length_level": "medium",
                "input_tokens_bucket": "512-1k",
                "expected_output_tokens_bucket": "128-512",
                "latency_sensitivity": "high",
                "quality_priority": "balanced",
                "sla_ms": 5000,
            },
        },
    ]
    entries: List[Dict[str, Any]] = []
    for idx in range(count):
        template = templates[idx % len(templates)]
        task_id = f"{template['task_id']}_{idx:03d}"
        metadata = dict(template["metadata"])
        metadata["workflow_id"] = f"wf-{task_id}"
        request = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": template["query"]}],
                }
            ],
            "max_tokens": 24,
            "temperature": 0.0,
            "metadata": metadata,
        }
        entries.append(
            {
                "request": request,
                "sample": {
                    "sample_id": task_id,
                    "workflow_id": metadata["workflow_id"],
                    "source_dataset": "agent_pd_probe",
                },
                "family": template["family"],
                "arrival_ms": 0.0,
            }
        )
    return entries


def _cleanup_previous_run_artifacts(workdir: Path) -> None:
    metrics_dir = workdir / "connector_metrics"
    if metrics_dir.exists():
        shutil.rmtree(metrics_dir)
    for path in (
        workdir / "proxy_workflow_registry.jsonl",
        workdir / "serving_e2e_summary.json",
    ):
        if path.exists():
            path.unlink()


def _tail_text(path: Path, limit_lines: int = 80) -> str:
    if not path.exists():
        return "<log missing>"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-limit_lines:]) if lines else "<log empty>"


def _ensure_process_running(name: str, proc: subprocess.Popen, log_path: Path) -> None:
    rc = proc.poll()
    if rc is None:
        return
    raise RuntimeError(
        f"{name} exited early with code={rc}. recent log tail:\n{_tail_text(log_path)}"
    )


def _wait_ready(
    name: str,
    base_url: str,
    *,
    proc: subprocess.Popen,
    log_path: Path,
    timeout_s: float = 300.0,
    paths: tuple[str, ...] = ("/health",),
) -> None:
    sess = requests.Session()
    sess.trust_env = False
    deadline = time.time() + max(1.0, float(timeout_s))
    last_error = "<not started>"
    while time.time() < deadline:
        _ensure_process_running(name, proc, log_path)
        for path in paths:
            try:
                resp = sess.get(f"{base_url}{path}", timeout=5)
                if resp.status_code == 200:
                    return
                last_error = f"{path} -> {resp.status_code}: {resp.text[:200]}"
            except Exception as exc:  # pragma: no cover - exercised in e2e
                last_error = f"{path} -> {exc}"
        time.sleep(2.0)
    _ensure_process_running(name, proc, log_path)
    raise RuntimeError(f"{name} not ready at {base_url}: {last_error}")


def _connector_metrics_settled(metrics_payload: Dict[str, object]) -> tuple[bool, List[str]]:
    metrics = dict(metrics_payload.get("metrics") or {})
    remote_backend_counts = dict(metrics.get("remote_transfer_backend_counts") or {})
    connector_path_stats = dict(metrics.get("connector_path_stats") or {})
    failures: List[str] = []
    if int(metrics.get("connector_metric_workers", 0) or 0) < 2:
        failures.append("connector_metric_workers < 2")
    if int(metrics.get("layered_transfer_grouped_batches", 0) or 0) <= 0:
        failures.append("layered_transfer_grouped_batches <= 0")
    if int(metrics.get("layered_receive_group_batches", 0) or 0) <= 0:
        failures.append("layered_receive_group_batches <= 0")
    if int(metrics.get("layered_transfer_grouped_batches", 0) or 0) < int(
        metrics.get("layered_receive_group_batches", 0) or 0
    ):
        failures.append("producer/consumer grouped batch parity broken")
    requests_total = int(metrics.get("requests_total", 0) or 0)
    finished_reqs = int(metrics.get("layered_receive_finished_reqs", 0) or 0)
    if finished_reqs < requests_total:
        failures.append("layered_receive_finished_reqs < requests_total")
    if int(metrics.get("layered_receive_failures", 0) or 0) != 0:
        failures.append("layered_receive_failures != 0")
    if int(metrics.get("layered_transfer_failed_batches", 0) or 0) != 0:
        failures.append("layered_transfer_failed_batches != 0")
    if int(metrics.get("fallback_batches", 0) or 0) != 0:
        failures.append("fallback_batches != 0")
    if int(metrics.get("fallback_bytes", 0) or 0) != 0:
        failures.append("fallback_bytes != 0")
    if int(metrics.get("peer_buffer_batches", 0) or 0) != int(
        remote_backend_counts.get("peer_buffer_direct", 0) or 0
    ):
        failures.append("peer_buffer_batches != peer_buffer_direct count")
    unexpected_backends = {
        str(key): value
        for key, value in remote_backend_counts.items()
        if str(key) != "peer_buffer_direct" and int(value or 0) != 0
    }
    if unexpected_backends:
        failures.append(f"unexpected remote transfer backends: {unexpected_backends}")
    path_sum_fields = {
        "grouped_batches": "layered_transfer_grouped_batches",
        "grouped_bytes": "layered_transfer_grouped_bytes",
        "grouped_descriptors": "layered_transfer_grouped_descriptors",
        "peer_buffer_batches": "peer_buffer_batches",
        "peer_buffer_bytes": "peer_buffer_bytes",
        "received_group_batches": "layered_receive_group_batches",
        "received_finished_reqs": "layered_receive_finished_reqs",
    }
    for path_field, total_field in path_sum_fields.items():
        total = int(metrics.get(total_field, 0) or 0)
        path_sum = sum(
            int(dict(values or {}).get(path_field, 0) or 0)
            for values in connector_path_stats.values()
        )
        if path_sum != total:
            failures.append(
                f"connector_path_stats.{path_field} sum {path_sum} != {total_field} {total}"
            )
    return len(failures) == 0, failures


def _wait_for_metrics_settle(
    session,
    metrics_url: str,
    *,
    timeout_s: float = 15.0,
    poll_s: float = 0.25,
    stable_polls: int = 2,
):
    """Wait for a valid and quiescent connector metrics snapshot.

    Worker counters cross process boundaries through atomic files. A single
    parity-valid response may still precede a final producer/consumer flush,
    which makes before/after deltas undercount the measured window. Require
    consecutive identical metric payloads after all correctness invariants are
    satisfied. This is benchmark-only control-plane work, never serving-path
    synchronization.
    """

    deadline = time.time() + max(1.0, float(timeout_s))
    last_payload: Dict[str, object] = {}
    last_failures: List[str] = ["metrics not fetched"]
    required_stable_polls = max(1, int(stable_polls))
    stable_count = 0
    previous_signature = ""
    while time.time() < deadline:
        payload = session.get(metrics_url, timeout=30).json()
        ok, failures = _connector_metrics_settled(payload)
        last_payload = payload
        last_failures = failures
        if ok:
            metrics = dict(payload.get("metrics") or {})
            signature = json.dumps(metrics, ensure_ascii=False, sort_keys=True)
            stable_count = stable_count + 1 if signature == previous_signature else 1
            previous_signature = signature
            if stable_count >= required_stable_polls:
                return payload
        else:
            stable_count = 0
            previous_signature = ""
        time.sleep(max(0.05, float(poll_s)))
    detail = "; ".join(last_failures) if last_failures else "metrics remained unstable"
    raise AssertionError("metrics did not settle before timeout: " + detail)


def _validate_summary(summary: Dict[str, object]) -> None:
    text_probe_stdout = str(summary.get("text_probe_stdout", ""))
    mm_probe_stdout = str(summary.get("mm_probe_stdout", ""))
    metrics_payload = dict(summary.get("metrics") or {})
    metrics = dict(metrics_payload.get("metrics") or {})
    path_stats = dict(metrics.get("path_stats") or {})
    connector_path_stats = dict(metrics.get("connector_path_stats") or {})
    pd_stats = dict(path_stats.get("PD") or {})
    epd_stats = dict(path_stats.get("EPD") or {})
    pd_connector_stats = dict(connector_path_stats.get("PD") or {})
    epd_connector_stats = dict(connector_path_stats.get("EPD") or {})
    workflow_registry = dict(metrics_payload.get("workflow_registry") or {})
    connector_metrics_dir = Path(str(summary.get("connector_metrics_dir", "")))

    failures: List[str] = []
    if "'x-epd-routing-path': 'PD'" not in text_probe_stdout:
        failures.append("text request did not route through PD path")
    if "'x-epd-routing-path': 'EPD'" not in mm_probe_stdout:
        failures.append("multimodal request did not route through EPD path")
    if metrics.get("requests_total", 0) < 2:
        failures.append("proxy metrics requests_total < 2")
    if metrics.get("requests_multimodal", 0) < 1:
        failures.append("proxy metrics requests_multimodal < 1")
    if metrics.get("handoff_committed", 0) < 2:
        failures.append("handoff_committed < 2")
    if int(pd_stats.get("requests_total", 0) or 0) < 1:
        failures.append("PD path requests_total < 1")
    if int(epd_stats.get("requests_total", 0) or 0) < 1:
        failures.append("EPD path requests_total < 1")
    if int(pd_stats.get("handoff_committed", 0) or 0) < 1:
        failures.append("PD path handoff_committed < 1")
    if int(epd_stats.get("handoff_committed", 0) or 0) < 1:
        failures.append("EPD path handoff_committed < 1")
    if int(epd_stats.get("mm_prefetch_announced", 0) or 0) < 1:
        failures.append("EPD path mm_prefetch_announced < 1")
    if int(pd_connector_stats.get("grouped_descriptors", 0) or 0) <= 0:
        failures.append("PD connector_path_stats grouped_descriptors <= 0")
    if int(epd_connector_stats.get("grouped_descriptors", 0) or 0) <= 0:
        failures.append("EPD connector_path_stats grouped_descriptors <= 0")
    if int(pd_connector_stats.get("grouped_bytes", 0) or 0) <= 0:
        failures.append("PD connector_path_stats grouped_bytes <= 0")
    if int(epd_connector_stats.get("grouped_bytes", 0) or 0) <= 0:
        failures.append("EPD connector_path_stats grouped_bytes <= 0")
    if not bool(workflow_registry.get("enabled")):
        failures.append("workflow registry not enabled")
    if int(workflow_registry.get("tracked_states", 0) or 0) < 2:
        failures.append("workflow registry tracked_states < 2")
    if int(summary.get("workflow_registry_events", 0) or 0) <= 0:
        failures.append("workflow registry WAL empty")
    if not connector_metrics_dir.exists():
        failures.append("connector metrics dir missing")
    else:
        metric_files = sorted(connector_metrics_dir.glob("*.json"))
        if not metric_files:
            failures.append("connector metrics files missing")
    settled, settled_failures = _connector_metrics_settled(metrics_payload)
    if not settled:
        failures.extend(settled_failures)
    # ``layer_load_wait_calls`` is diagnostic only.  A progressive native KV
    # receive can complete through grouped receive callbacks without invoking
    # the legacy blocking layer-wait path.  The invariants above already prove
    # producer/consumer grouped batches, finished receives, direct backend
    # selection and zero failures; rejecting a zero wait count would turn a
    # successful lower-latency path into a false negative.
    if int(metrics.get("peer_buffer_batches", 0) or 0) <= 0:
        failures.append("peer_buffer_batches <= 0")
    remote_backend_counts = dict(metrics.get("remote_transfer_backend_counts") or {})
    if int(remote_backend_counts.get("peer_buffer_direct", 0) or 0) <= 0:
        failures.append("peer_buffer_direct backend count <= 0")
    dataset_requested = int(summary.get("dataset_probe_requested", 0) or 0)
    if dataset_requested > 0:
        dataset_count = int(summary.get("dataset_probe_count", 0) or 0)
        dataset_success = int(summary.get("dataset_probe_success", 0) or 0)
        if dataset_count != dataset_requested:
            failures.append(
                f"dataset_probe_count {dataset_count} != requested {dataset_requested}"
            )
        if dataset_success != dataset_requested:
            failures.append(
                f"dataset_probe_success {dataset_success} != requested {dataset_requested}"
            )
        latency_stats = dict(summary.get("dataset_latency_stats_ms") or {})
        if int(latency_stats.get("count", 0) or 0) != dataset_success:
            failures.append("dataset_latency_stats_ms.count != dataset_probe_success")
        ttft_stats = dict(summary.get("dataset_ttft_stats_ms") or {})
        if int(ttft_stats.get("count", 0) or 0) < dataset_success:
            failures.append("dataset_ttft_stats_ms.count < dataset_probe_success")
        requested_families = [
            str(family)
            for family in list(summary.get("dataset_families_requested") or [])
        ]
        family_counts = dict(summary.get("dataset_probe_family_counts") or {})
        if requested_families and dataset_requested >= len(set(requested_families)):
            missing = [
                family
                for family in dict.fromkeys(requested_families)
                if int(family_counts.get(family, 0) or 0) <= 0
            ]
            if missing:
                failures.append(f"dataset requested families not covered: {missing}")
        for idx, item in enumerate(list(summary.get("dataset_probe_results") or [])):
            item_dict = dict(item)
            status_code = int(item_dict.get("status_code", 0) or 0)
            if status_code >= 400 or status_code <= 0:
                failures.append(f"dataset probe {idx} status_code={status_code}")
            if not item_dict.get("routing_path"):
                failures.append(f"dataset probe {idx} missing routing_path")
            if int(item_dict.get("response_content_len", 1) or 0) <= 0:
                failures.append(f"dataset probe {idx} empty response content")
            if item_dict.get("response_parse_error"):
                failures.append(
                    f"dataset probe {idx} response_parse_error={item_dict.get('response_parse_error')}"
                )

    if failures:
        raise AssertionError("; ".join(failures))


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
    # ``setsid`` makes the launcher's pid the persistent service process-group
    # id.  Keep it explicitly because a shell leader can exit before cleanup
    # while a vLLM child remains alive in that group.
    proc._mooncake_epd_process_group = proc.pid  # type: ignore[attr-defined]
    return proc


def _owned_process_groups(procs: List[subprocess.Popen]) -> List[int]:
    """Return unique process groups created by :func:`_launch`.

    Every caller launches its shell in a new session, so the shell pid is also
    the service group id.  We intentionally retain that id even when the shell
    has already exited: vLLM may leave an EngineCore child in the group after a
    request failure.
    """

    groups: List[int] = []
    seen: set[int] = set()
    for proc in reversed(procs):
        try:
            pgid = int(getattr(proc, "_mooncake_epd_process_group", proc.pid))
        except (TypeError, ValueError, AttributeError):
            continue
        if pgid <= 0 or pgid in seen:
            continue
        seen.add(pgid)
        groups.append(pgid)
    return groups


def _signal_process_groups(process_groups: List[int], sig: int) -> None:
    for pgid in process_groups:
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            # The group ended between probing it and signaling it.
            continue
        except OSError:
            # Cleanup must never mask the original E2E failure.
            continue


def _process_group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # This should not occur for an owned test group, but it means the group
        # still exists and needs no unsafe fallback action here.
        return True
    except OSError:
        return False
    return True


def _terminate_all(procs: List[subprocess.Popen]) -> None:
    process_groups = _owned_process_groups(procs)
    _signal_process_groups(process_groups, signal.SIGTERM)
    deadline = time.time() + 20
    while time.time() < deadline:
        if not any(_process_group_alive(pgid) for pgid in process_groups):
            break
        time.sleep(0.5)
    _signal_process_groups(
        [pgid for pgid in process_groups if _process_group_alive(pgid)],
        signal.SIGKILL,
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real vLLM PD/EPD serving e2e validation.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_serving_e2e")
    ap.add_argument(
        "--model",
        default=os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct"),
        help="Explicit model path recorded in the benchmark artifact.",
    )
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument(
        "--startup-timeout",
        type=float,
        default=900.0,
        help="Maximum seconds to wait for vLLM cold start and compilation.",
    )
    ap.add_argument("--local-hostname", default="127.0.0.1")
    ap.add_argument(
        "--mooncake-protocol",
        choices=["tcp", "shm", "rdma", "nvlink_intra"],
        default="tcp",
        help="P→D KV transport. Strict runs verify the selected native transport in every worker log.",
    )
    ap.add_argument(
        "--scheduler-policy",
        choices=sorted(SCHEDULER_POLICIES),
        default="agent_aware",
        help="Proxy P/D worker-selection policy; recorded for policy and capacity comparisons.",
    )
    ap.add_argument(
        "--strict-no-fallback",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reject proxy/data-plane fallbacks and require native P→D transport evidence.",
    )
    ap.add_argument("--max-model-len", type=int, default=4096)
    ap.add_argument("--gpu-memory-utilization", type=float, default=0.65)
    ap.add_argument(
        "--max-num-batched-tokens",
        type=int,
        default=0,
        help="0 retains vLLM's default; positive values are recorded as a serving capacity setting.",
    )
    ap.add_argument(
        "--max-num-seqs",
        type=int,
        default=0,
        help="0 retains vLLM's default; positive values are recorded as a serving capacity setting.",
    )
    ap.add_argument(
        "--generation-config",
        default="vllm",
        help="vLLM generation configuration, pinned to avoid model-repository sampling defaults.",
    )
    ap.add_argument(
        "--prefill-gpus",
        type=int,
        nargs="+",
        default=None,
        help="CUDA device ids for real Prefill workers. Multiple ids start multiple vLLM Prefill instances.",
    )
    ap.add_argument(
        "--decode-gpus",
        type=int,
        nargs="+",
        default=None,
        help="CUDA device ids for real Decode workers. Multiple ids start multiple vLLM Decode instances.",
    )
    ap.add_argument(
        "--allow-loopback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Allow loopback hostname/IP for single-node validation. Disable for routable network validation.",
    )
    ap.add_argument("--dataset-root", default=None, help="Optional mooncake_test_dataset root for real chat-split probes.")
    ap.add_argument("--dataset-chat-split", default="dev-small")
    ap.add_argument("--max-dataset-requests", type=int, default=0)
    ap.add_argument("--dataset-families", nargs="+", default=None)
    ap.add_argument(
        "--dataset-agent-pd-labels",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Derive Agent-PD scheduling metadata from real dataset workload shape "
            "(prompt length and workload family) and require route metrics."
        ),
    )
    ap.add_argument("--dataset-request-timeout", type=float, default=180.0)
    ap.add_argument("--dataset-concurrency", type=int, default=1)
    ap.add_argument("--dataset-qps", type=float, default=0.0)
    ap.add_argument("--dataset-arrival-schedule", default=None)
    ap.add_argument("--dataset-deadline-ms", type=float, default=0.0)
    ap.add_argument("--dataset-goodput-slo-ms", type=float, default=30000.0)
    ap.add_argument(
        "--dataset-streaming-metrics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use OpenAI streaming responses for dataset probes so TTFT/TPOT can be measured on real serving.",
    )
    ap.add_argument("--dataset-max-input-len", type=int, default=4096)
    ap.add_argument("--dataset-request-max-tokens", type=int, default=128)
    ap.add_argument(
        "--dataset-skip-oversized",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip real dataset samples whose processed multimodal prompt cannot fit the serving max_model_len.",
    )
    ap.add_argument(
        "--dataset-image-max-pixels",
        type=int,
        default=1_003_520,
        help=(
            "Per-image max_pixels passed to Qwen/VLLM multimodal preprocessing for dataset probes. "
            "Keeps real dataset samples within the serving context without mocking requests."
        ),
    )
    ap.add_argument("--warmup-requests", type=int, default=0)
    ap.add_argument(
        "--agent-pd-probes",
        type=int,
        default=0,
        help=(
            "Send N real OpenAI-compatible Agent-PD metadata probes through the "
            "proxy/vLLM hot path and require agent_pd_route metrics to increase."
        ),
    )
    ap.add_argument(
        "--warmup-unique-workflows",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Rewrite each warmup workflow_id to avoid proxy cross-step reuse when validating vLLM-internal cache paths.",
    )
    ap.add_argument(
        "--warmup-vary-pre-image-text",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "For repeated warmup multimodal requests, insert a unique text prefix before the same image. "
            "This breaks vLLM prefix/KV cache while preserving the image mm_hash, isolating encoder hidden-cache reuse."
        ),
    )
    ap.add_argument("--mm-prefetch-mode", choices=["asset_bytes", "feature_handle"], default="asset_bytes")
    ap.add_argument("--prefill-supports-feature-handles", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument(
        "--prefill-http-keepalive",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Reuse idle Proxy->Prefill HTTP connections. Disabled by default "
            "to avoid unsafe retry ambiguity for Prefill POSTs."
        ),
    )
    ap.add_argument(
        "--enable-mm-prefetch",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Forward --enable/--no-enable-mm-prefetch to the EPD proxy. Disable for vLLM-internal hidden-cache ablations.",
    )
    # Match the measured direct-serving defaults.  The earlier 8-layer/16MiB
    # values multiply P→D handshakes for Qwen3-VL and were retained here only
    # as a stale runner default; callers can still select smaller groups for
    # an explicit latency/overlap ablation.
    ap.add_argument("--layers-per-group", type=int, default=32)
    ap.add_argument("--max-group-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=512)
    ap.add_argument("--max-transfer-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--owner-shards", type=int, default=1)
    ap.add_argument("--kv-directory-rpc-url", default=None)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    _cleanup_previous_run_artifacts(workdir)
    local_hostname = _validate_local_hostname(
        args.local_hostname,
        allow_loopback=bool(args.allow_loopback),
    )
    cfg = VLLMDisaggConfig(
        model=str(args.model),
        local_hostname=local_hostname,
        protocol=str(args.mooncake_protocol),
        scheduler_policy=str(args.scheduler_policy),
        strict_no_fallback=bool(args.strict_no_fallback),
        prefill_gpus=tuple(int(x) for x in (args.prefill_gpus or ())),
        decode_gpus=tuple(int(x) for x in (args.decode_gpus or ())),
        max_model_len=max(1, int(args.max_model_len)),
        gpu_memory_utilization=float(args.gpu_memory_utilization),
        max_num_batched_tokens=_optional_positive_int(args.max_num_batched_tokens),
        max_num_seqs=_optional_positive_int(args.max_num_seqs),
        generation_config=str(args.generation_config),
        owner_shards=max(1, int(args.owner_shards)),
        kv_directory_rpc_url=args.kv_directory_rpc_url,
        workflow_registry_wal_path=str(workdir / "proxy_workflow_registry.jsonl"),
        connector_metrics_dir=str(workdir / "connector_metrics"),
        layers_per_group=int(args.layers_per_group),
        max_group_bytes=args.max_group_bytes,
        max_transfer_descriptors=args.max_transfer_descriptors,
        max_transfer_bytes=args.max_transfer_bytes,
        mm_prefetch_mode=args.mm_prefetch_mode,
        prefill_supports_feature_handles=bool(args.prefill_supports_feature_handles),
        prefill_http_keepalive=bool(args.prefill_http_keepalive),
    )
    files = generate_configs(str(workdir), cfg)
    if not bool(args.enable_mm_prefetch):
        proxy_script = Path(files["proxy"])
        proxy_text = proxy_script.read_text(encoding="utf-8")
        if "--no-enable-mm-prefetch" not in proxy_text:
            proxy_text = proxy_text.replace("--port ", "--no-enable-mm-prefetch --port ", 1)
            proxy_script.write_text(proxy_text, encoding="utf-8")
    prefill_scripts = _as_str_list(files.get("prefill_scripts"), [str(files["prefill"])])
    decode_scripts = _as_str_list(files.get("decode_scripts"), [str(files["decode"])])
    prefill_ports = _as_int_list(files.get("prefill_ports"), [_extract_port(Path(str(files["prefill"])))])
    decode_ports = _as_int_list(files.get("decode_ports"), [_extract_port(Path(str(files["decode"])))])
    prefill_port = int(prefill_ports[0])
    decode_port = int(decode_ports[0])
    proxy_port = _extract_port(Path(str(files["proxy"])))

    logs = {
        "metadata": workdir / "metadata.log",
        "master": workdir / "master.log",
        "proxy": workdir / "proxy.log",
    }
    for idx in range(len(prefill_scripts)):
        logs[f"prefill-{idx}"] = workdir / ("prefill.log" if idx == 0 else f"prefill_{idx}.log")
    for idx in range(len(decode_scripts)):
        logs[f"decode-{idx}"] = workdir / ("decode.log" if idx == 0 else f"decode_{idx}.log")
    procs: List[subprocess.Popen] = []
    summary: Dict[str, object] = {}
    try:
        named_procs: Dict[str, subprocess.Popen] = {}
        launch_items: List[tuple[str, str]] = [
            ("metadata", str(files["metadata"])),
            ("master", str(files["master"])),
        ]
        launch_items.extend((f"prefill-{idx}", script) for idx, script in enumerate(prefill_scripts))
        launch_items.extend((f"decode-{idx}", script) for idx, script in enumerate(decode_scripts))
        launch_items.append(("proxy", str(files["proxy"])))
        for key, script_path in launch_items:
            proc = _launch(script_path, logs[key])
            procs.append(proc)
            named_procs[key] = proc
            time.sleep(2.0 if key in {"metadata", "master"} else 4.0)
            _ensure_process_running(key, proc, logs[key])

        startup_timeout_s = max(
            60.0,
            float(getattr(args, "startup_timeout", args.timeout)),
        )
        for idx, port in enumerate(prefill_ports):
            key = f"prefill-{idx}"
            _wait_ready(
                key,
                f"http://{local_hostname}:{port}",
                proc=named_procs[key],
                log_path=logs[key],
                timeout_s=startup_timeout_s,
                paths=("/health",),
            )
        for idx, port in enumerate(decode_ports):
            key = f"decode-{idx}"
            _wait_ready(
                key,
                f"http://{local_hostname}:{port}",
                proc=named_procs[key],
                log_path=logs[key],
                timeout_s=startup_timeout_s,
                paths=("/health",),
            )
        _wait_ready(
            "proxy",
            f"http://{local_hostname}:{proxy_port}",
            proc=named_procs["proxy"],
            log_path=logs["proxy"],
            timeout_s=min(120.0, max(30.0, float(args.timeout) / 4.0)),
            paths=("/health", "/healthcheck"),
        )

        req_text = workdir / "text_request.json"
        req_text.write_text(
            json.dumps(
                {
                    "model": cfg.model,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": "Please summarize Mooncake PD disaggregation in two sentences."}]}],
                    "max_tokens": 32,
                    "temperature": 0.0,
                    "metadata": {"workflow_id": "serving-text-registry"},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        req_mm = workdir / "mm_request.json"
        req_mm.write_text(
            json.dumps(
                {
                    "model": cfg.model,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {"type": "image_url", "image_url": {"url": _data_url_for_demo_image("room")}},
                                {"type": "text", "text": "Describe the likely room scene briefly."},
                            ],
                        }
                    ],
                    "max_tokens": 32,
                    "temperature": 0.0,
                    "metadata": {"workflow_id": "serving-mm-registry"},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        checker = REPO_ROOT / "scripts" / "check_vllm_disagg.py"
        cmd_env = _proc_env()
        text_probe = subprocess.run(
            [
                sys.executable,
                str(checker),
                "--prefill-url",
                f"http://{local_hostname}:{prefill_port}",
                "--decode-url",
                f"http://{local_hostname}:{decode_port}",
                "--proxy-url",
                f"http://{local_hostname}:{proxy_port}",
                "--request",
                str(req_text),
            ],
            cwd=str(REPO_ROOT),
            env=cmd_env,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
        mm_probe = subprocess.run(
            [
                sys.executable,
                str(checker),
                "--prefill-url",
                f"http://{local_hostname}:{prefill_port}",
                "--decode-url",
                f"http://{local_hostname}:{decode_port}",
                "--proxy-url",
                f"http://{local_hostname}:{proxy_port}",
                "--request",
                str(req_mm),
            ],
            cwd=str(REPO_ROOT),
            env=cmd_env,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )

        proxy_request_url = f"http://{local_hostname}:{proxy_port}/v1/chat/completions"
        agent_pd_probe_results: List[Dict[str, Any]] = []
        agent_pd_entries = _agent_pd_probe_entries(cfg.model, int(args.agent_pd_probes))
        for idx, entry in enumerate(agent_pd_entries):
            result = _execute_dataset_request(
                proxy_url=proxy_request_url,
                entry=entry,
                index=idx,
                request_timeout=float(args.dataset_request_timeout),
                stream_metrics=bool(args.dataset_streaming_metrics),
            )
            if int(result.get("status_code") or 0) >= 400 or result.get("error"):
                raise RuntimeError(
                    f"agent-pd serving probe failed: {json.dumps(result, ensure_ascii=False)[:4000]}"
                )
            agent_pd_probe_results.append(result)

        dataset_probe_results: List[Dict[str, Any]] = []
        dataset_split_path = (
            Path(args.dataset_root).resolve()
            / "chat_splits"
            / f"{args.dataset_chat_split}.jsonl"
            if args.dataset_root
            else None
        )
        dataset_requests, dataset_skipped = _load_dataset_requests(
            dataset_root=args.dataset_root,
            chat_split=args.dataset_chat_split,
            max_requests=args.max_dataset_requests,
            families=args.dataset_families,
            model=cfg.model,
            max_input_len=args.dataset_max_input_len,
            request_max_tokens=args.dataset_request_max_tokens,
            skip_oversized=bool(args.dataset_skip_oversized),
            image_max_pixels=args.dataset_image_max_pixels,
            agent_pd_labels=bool(args.dataset_agent_pd_labels),
        )
        dataset_artifacts_dir = workdir / "dataset_probe_results"
        dataset_artifacts_dir.mkdir(parents=True, exist_ok=True)
        dataset_admission_config = {
            "max_input_len": int(args.dataset_max_input_len),
            "request_max_tokens": int(args.dataset_request_max_tokens),
            "skip_oversized": bool(args.dataset_skip_oversized),
            "image_max_pixels": int(args.dataset_image_max_pixels),
            "streaming_metrics": bool(args.dataset_streaming_metrics),
        }
        (dataset_artifacts_dir / "dataset_admission_config.json").write_text(
            json.dumps(dataset_admission_config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (dataset_artifacts_dir / "dataset_skipped.json").write_text(
            json.dumps(dataset_skipped, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if args.dataset_root and args.max_dataset_requests > 0 and len(dataset_requests) < args.max_dataset_requests:
            raise RuntimeError(
                "insufficient admissible dataset requests: "
                f"selected={len(dataset_requests)} requested={args.max_dataset_requests} "
                f"skipped={len(dataset_skipped)}; see {dataset_artifacts_dir / 'dataset_skipped.json'}"
            )
        dispatch_plan, arrival_summary = _build_dataset_dispatch_plan(
            dataset_requests,
            dataset_root=args.dataset_root,
            arrival_schedule=args.dataset_arrival_schedule,
            qps=float(args.dataset_qps),
            deadline_ms=float(args.dataset_deadline_ms),
        )
        if int(args.warmup_requests) > 0:
            warmup_entry = dispatch_plan[0] if dispatch_plan else {
                "request": _read_json(req_mm),
                "sample": {"sample_id": "warmup", "workflow_id": "warmup"},
                "family": "warmup",
            }
            for warmup_idx in range(int(args.warmup_requests)):
                entry_for_warmup = warmup_entry
                if bool(args.warmup_unique_workflows) or bool(args.warmup_vary_pre_image_text):
                    entry_for_warmup = copy.deepcopy(warmup_entry)
                if bool(args.warmup_unique_workflows):
                    req_meta = dict((entry_for_warmup["request"].get("metadata") or {}))
                    base_workflow = str(req_meta.get("workflow_id") or "warmup")
                    req_meta["workflow_id"] = f"{base_workflow}-unique-{warmup_idx}"
                    entry_for_warmup["request"]["metadata"] = req_meta
                if bool(args.warmup_vary_pre_image_text):
                    messages = list(entry_for_warmup["request"].get("messages") or [])
                    if messages and isinstance(messages[0].get("content"), list):
                        original_parts = list(messages[0].get("content") or [])
                        image_parts = [p for p in original_parts if isinstance(p, dict) and p.get("type") == "image_url"]
                        text_parts = [p for p in original_parts if isinstance(p, dict) and p.get("type") == "text"]
                        unique_prefix = {
                            "type": "text",
                            "text": f"Warmup variant {warmup_idx}: inspect the following image independently. ",
                        }
                        if image_parts:
                            messages[0]["content"] = unique_prefix, *image_parts, *(text_parts or [{"type": "text", "text": "Describe the image briefly."}])
                            messages[0]["content"] = list(messages[0]["content"])
                            entry_for_warmup["request"]["messages"] = messages
                warmup_result = _execute_dataset_request(
                    proxy_url=proxy_request_url,
                    entry=entry_for_warmup,
                    index=-1 - warmup_idx,
                    request_timeout=float(args.dataset_request_timeout),
                    stream_metrics=bool(args.dataset_streaming_metrics),
                )
                if int(warmup_result.get("status_code") or 0) >= 400 or warmup_result.get("error"):
                    raise RuntimeError(
                        f"warmup request failed: {json.dumps(warmup_result, ensure_ascii=False)[:4000]}"
                    )

        benchmark_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max(1, int(args.dataset_concurrency))) as pool:
            future_map = {}
            plan_started = time.perf_counter()
            for idx, entry in enumerate(dispatch_plan):
                scheduled_at = max(0.0, float(entry.get("arrival_ms", 0.0) or 0.0)) / 1000.0

                def _task(run_entry=entry, run_idx=idx, run_scheduled_at=scheduled_at):
                    target = plan_started + run_scheduled_at
                    remaining = target - time.perf_counter()
                    if remaining > 0:
                        time.sleep(remaining)
                    return _execute_dataset_request(
                        proxy_url=proxy_request_url,
                        entry=run_entry,
                        index=run_idx,
                        request_timeout=float(args.dataset_request_timeout),
                        stream_metrics=bool(args.dataset_streaming_metrics),
                    )

                future = pool.submit(_task)
                future_map[future] = idx

            for future in as_completed(future_map):
                result = future.result()
                idx = int(result.get("index", future_map[future]))
                (dataset_artifacts_dir / f"dataset_probe_{idx:03d}.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                dataset_probe_results.append(result)

        dataset_probe_results.sort(key=lambda item: int(item.get("index", 0) or 0))
        benchmark_elapsed_s = max(1e-6, time.perf_counter() - benchmark_started)
        sess = requests.Session()
        sess.trust_env = False
        for result in dataset_probe_results:
            if int(result.get("status_code") or 0) >= 400 or result.get("error"):
                try:
                    result["metrics_at_failure"] = sess.get(
                        f"http://{local_hostname}:{proxy_port}/metrics", timeout=10
                    ).json()
                except Exception as metrics_exc:
                    result["metrics_error"] = f"{type(metrics_exc).__name__}: {metrics_exc}"
                result["log_tails"] = {name: _tail_text(path, limit_lines=120) for name, path in logs.items()}
                raise RuntimeError(f"dataset probe failed: {json.dumps(result, ensure_ascii=False)[:4000]}")

        metrics = _wait_for_metrics_settle(
            sess,
            f"http://{local_hostname}:{proxy_port}/metrics",
            timeout_s=min(30.0, max(5.0, float(args.timeout) / 30.0)),
            poll_s=0.25,
        )
        expected_agent_pd_requests = int(args.agent_pd_probes) + (
            len(dataset_probe_results) if bool(args.dataset_agent_pd_labels) else 0
        )
        if expected_agent_pd_requests > 0:
            metric_values = dict(dict(metrics.get("metrics") or {}))
            expected_stage_routes = expected_agent_pd_requests * 2
            actual_stage_routes = int(metric_values.get("agent_pd_route_total", 0) or 0)
            actual_correct = int(metric_values.get("agent_pd_route_correct", 0) or 0)
            if actual_stage_routes < expected_stage_routes:
                raise AssertionError(
                    "agent_pd_route_total did not include all real serving probes: "
                    f"actual={actual_stage_routes} expected>={expected_stage_routes}"
                )
            if actual_correct < expected_stage_routes:
                raise AssertionError(
                    "agent_pd_route_correct did not include all real serving probes: "
                    f"actual={actual_correct} expected>={expected_stage_routes}"
                )
        registry_path = Path(files["proxy_workflow_registry"])
        registry_lines = registry_path.read_text(encoding="utf-8").splitlines() if registry_path.exists() else []

        dataset_elapsed_values = [
            float(item.get("elapsed_ms", 0.0) or 0.0)
            for item in dataset_probe_results
            if int(item.get("status_code", 0) or 0) < 400 and not item.get("error")
        ]
        dataset_ttft_values = [
            float(item.get("ttft_ms", 0.0) or 0.0)
            for item in dataset_probe_results
            if item.get("ttft_ms") is not None and int(item.get("status_code", 0) or 0) < 400
        ]
        dataset_tpot_values = [
            float(item.get("tpot_ms", 0.0) or 0.0)
            for item in dataset_probe_results
            if item.get("tpot_ms") is not None and int(item.get("status_code", 0) or 0) < 400
        ]
        dataset_success = [
            item
            for item in dataset_probe_results
            if int(item.get("status_code", 0) or 0) < 400 and not item.get("error")
        ]
        deadline_ms = max(0.0, float(args.dataset_deadline_ms))
        goodput_slo_ms = max(0.0, float(args.dataset_goodput_slo_ms))
        goodput_count = sum(
            1
            for item in dataset_success
            if goodput_slo_ms <= 0 or float(item.get("elapsed_ms", 0.0) or 0.0) <= goodput_slo_ms
        )
        deadline_miss_count = sum(
            1
            for item in dataset_success
            if deadline_ms > 0 and float(item.get("elapsed_ms", 0.0) or 0.0) > deadline_ms
        )

        summary = {
            "workdir": str(workdir),
            "model": str(cfg.model),
            "mooncake_protocol": str(cfg.protocol),
            # This runner uses proxy asset prefetch; it must not claim the
            # direct Encoder→Prefill native engine path exercised by the
            # dedicated online-direct runner.
            "direct_engine_protocol": None,
            "text_only": False,
            "strict_no_fallback": bool(cfg.strict_no_fallback),
            "scheduler_policy": str(cfg.scheduler_policy),
            "ports": {
                "prefill": prefill_port,
                "decode": decode_port,
                "prefill_all": prefill_ports,
                "decode_all": decode_ports,
                "proxy": proxy_port,
            },
            "worker_scripts": {
                "prefill": prefill_scripts,
                "decode": decode_scripts,
            },
            "worker_gpus": {
                "prefill": _as_int_list(files.get("prefill_gpus"), []),
                "decode": _as_int_list(files.get("decode_gpus"), []),
            },
            "local_hostname": local_hostname,
            "allow_loopback": bool(args.allow_loopback),
            "text_probe_stdout": text_probe.stdout,
            "mm_probe_stdout": mm_probe.stdout,
            "dataset_probe_results": dataset_probe_results,
            "dataset_probe_count": len(dataset_probe_results),
            "dataset_probe_success": sum(1 for item in dataset_probe_results if int(item.get("status_code", 0) or 0) < 400),
            "agent_pd_probe_results": agent_pd_probe_results,
            "agent_pd_probe_count": len(agent_pd_probe_results),
            "agent_pd_probe_success": sum(1 for item in agent_pd_probe_results if int(item.get("status_code", 0) or 0) < 400),
            "dataset_probe_requested": int(args.max_dataset_requests),
            "dataset_probe_skipped": dataset_skipped,
            "dataset_probe_skipped_count": len(dataset_skipped),
            "dataset_families_requested": list(args.dataset_families or []),
            "dataset_arrival_config": {
                "schedule": args.dataset_arrival_schedule,
                "concurrency": int(args.dataset_concurrency),
                "qps": float(args.dataset_qps),
                "deadline_ms": deadline_ms,
                "goodput_slo_ms": goodput_slo_ms,
                "warmup_requests": int(args.warmup_requests),
                **arrival_summary,
            },
            "dataset_probe_family_counts": {
                family: sum(
                    1
                    for item in dataset_probe_results
                    if str(item.get("workload_family")) == str(family)
                )
                for family in sorted({str(item.get("workload_family")) for item in dataset_probe_results})
            },
            "dataset_admission_config": dataset_admission_config,
            "dataset_agent_pd_labels": bool(args.dataset_agent_pd_labels),
            "dataset_latency_stats_ms": _stats(dataset_elapsed_values),
            "dataset_ttft_stats_ms": _stats(dataset_ttft_values),
            "dataset_tpot_stats_ms": _stats(dataset_tpot_values),
            "dataset_request_throughput_rps": len(dataset_probe_results) / benchmark_elapsed_s,
            "dataset_goodput_rps": goodput_count / benchmark_elapsed_s,
            "dataset_goodput_count": goodput_count,
            "dataset_success_rate": (
                len(dataset_success) / len(dataset_probe_results) if dataset_probe_results else 0.0
            ),
            "dataset_deadline_miss_ratio": (
                deadline_miss_count / len(dataset_success) if dataset_success and deadline_ms > 0 else 0.0
            ),
            "metrics": metrics,
            "workflow_registry_wal": str(registry_path),
            "workflow_registry_events": len(registry_lines),
            "connector_metrics_dir": files["connector_metrics_dir"],
            "logs": {k: str(v) for k, v in logs.items()},
        }
        # ``responses`` is the common campaign surface.  Keep the richer
        # dataset/Agent-PD collections above as the runner-specific evidence.
        summary["responses"] = list(dataset_probe_results)
        measured_dispatch_results = list(dataset_probe_results) + list(agent_pd_probe_results)
        summary["worker_dispatch_balance"] = {
            "prefill": _worker_dispatch_balance(
                measured_dispatch_results,
                stage="prefill",
                configured_workers=len(prefill_scripts),
            ),
            "decode": _worker_dispatch_balance(
                measured_dispatch_results,
                stage="decode",
                configured_workers=len(decode_scripts),
            ),
        }
        selected_request_rows = [
            _dataset_request_evidence_row(entry, index=index, phase="measure")
            for index, entry in enumerate(dispatch_plan)
        ]
        selected_samples = [
            {
                "sample_id": row.get("sample_id"),
                "workflow_id": row.get("workflow_id"),
                "workload_family": row.get("workload_family"),
                "source_dataset": row.get("source_dataset"),
                "request_sha256": row.get("request_sha256"),
            }
            for row in selected_request_rows
        ]
        summary["benchmark_config"] = {
            "schema_version": 1,
            "request_fingerprint": _stable_digest(
                {
                    "model": str(cfg.model),
                    "dataset_split_sha256": (
                        _file_sha256(dataset_split_path)
                        if dataset_split_path is not None
                        else None
                    ),
                    "selected_samples": selected_samples,
                }
            ),
            "request": {
                "model": str(cfg.model),
                "workload": "dataset_chat_split",
                "dataset_chat_split": str(args.dataset_chat_split),
                "dataset_split_sha256": (
                    _file_sha256(dataset_split_path)
                    if dataset_split_path is not None
                    else None
                ),
                "dataset_families": list(args.dataset_families or []),
                "selected_samples": selected_samples,
                "max_tokens": int(args.dataset_request_max_tokens),
                "temperature": 0.0,
                "image_max_pixels": int(args.dataset_image_max_pixels),
                "agent_pd_labels": bool(args.dataset_agent_pd_labels),
            },
            "load": {
                "requested_dataset_requests": int(args.max_dataset_requests),
                "warmup_requests": int(args.warmup_requests),
                "concurrency": int(args.dataset_concurrency),
                "qps": float(args.dataset_qps),
                "arrival_schedule": args.dataset_arrival_schedule,
                "deadline_ms": deadline_ms,
                "goodput_slo_ms": goodput_slo_ms,
                "streaming_metrics": bool(args.dataset_streaming_metrics),
            },
            "serving": {
                "max_model_len": int(cfg.max_model_len),
                "gpu_memory_utilization": float(cfg.gpu_memory_utilization),
                "max_num_batched_tokens": cfg.max_num_batched_tokens,
                "max_num_seqs": cfg.max_num_seqs,
                "generation_config": cfg.generation_config,
                "tensor_parallel_size": int(cfg.tensor_parallel_size),
            },
            "topology": {
                "prefill_gpus": list(cfg.prefill_gpus or (cfg.prefill_gpu,)),
                "decode_gpus": list(cfg.decode_gpus or (cfg.decode_gpu,)),
                "total_gpus": len(cfg.prefill_gpus or (cfg.prefill_gpu,))
                + len(cfg.decode_gpus or (cfg.decode_gpu,)),
            },
            "epd": {
                "protocol": str(cfg.protocol),
                "layers_per_group": int(cfg.layers_per_group),
                "max_group_bytes": int(cfg.max_group_bytes),
                "max_transfer_descriptors": int(cfg.max_transfer_descriptors),
                "max_transfer_bytes": int(cfg.max_transfer_bytes),
                "transfer_workers": int(cfg.effective_transfer_workers),
                "scheduler_policy": str(cfg.scheduler_policy),
                "mm_prefetch_mode": str(cfg.mm_prefetch_mode),
                "prefill_supports_feature_handles": bool(cfg.prefill_supports_feature_handles),
                "prefill_http_keepalive": bool(cfg.prefill_http_keepalive),
                "vllm_mm_hidden_cache": cfg.vllm_mm_hidden_cache,
                "workflow_registry_durability": "fsync_wal",
            },
        }
        _record_runtime_transport_evidence(
            summary,
            logs=logs,
            requested=str(cfg.protocol),
            enforce=bool(cfg.strict_no_fallback),
        )
        _validate_summary(summary)
        summary["raw_artifacts"] = write_raw_benchmark_artifacts(
            workdir=workdir,
            request_rows=selected_request_rows,
            response_rows=[
                {"phase": "measure", **dict(result)}
                for result in dataset_probe_results
            ],
            service_logs=logs,
            metrics={
                "proxy_metrics": metrics,
                "dataset_latency_stats_ms": summary["dataset_latency_stats_ms"],
                "dataset_ttft_stats_ms": summary["dataset_ttft_stats_ms"],
                "dataset_tpot_stats_ms": summary["dataset_tpot_stats_ms"],
                "worker_dispatch_balance": summary["worker_dispatch_balance"],
                "transport_runtime_evidence": summary["transport_runtime_evidence"],
            },
            runtime={
                "runner": "run_vllm_serving_e2e",
                "model": str(cfg.model),
                "strict_no_fallback": bool(cfg.strict_no_fallback),
                "mooncake_protocol": str(cfg.protocol),
                "scheduler_policy": str(cfg.scheduler_policy),
                "benchmark_config": summary["benchmark_config"],
            },
        )
        out = workdir / "serving_e2e_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"wrote {out}")
    finally:
        _terminate_all(procs)


if __name__ == "__main__":
    main()
