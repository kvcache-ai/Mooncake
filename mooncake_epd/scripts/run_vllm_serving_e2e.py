from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from mooncake_epd.demo.vllm_integration import VLLMDisaggConfig, generate_configs  # noqa: E402
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
        result.update(
            {
                "status_code": None,
                "elapsed_ms": elapsed_ms,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
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
    if int(metrics.get("layer_load_wait_calls", 0) or 0) <= 0:
        failures.append("layer_load_wait_calls <= 0")
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
    return proc


def _terminate_all(procs: List[subprocess.Popen]) -> None:
    for proc in reversed(procs):
        if proc.poll() is not None:
            continue
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            pass
    deadline = time.time() + 20
    while time.time() < deadline:
        if all(proc.poll() is not None for proc in procs):
            break
        time.sleep(0.5)
    for proc in reversed(procs):
        if proc.poll() is not None:
            continue
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real vLLM PD/EPD serving e2e validation.")
    ap.add_argument("--workdir", default="/tmp/mooncake_epd_serving_e2e")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--local-hostname", default="127.0.0.1")
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
        "--enable-mm-prefetch",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Forward --enable/--no-enable-mm-prefetch to the EPD proxy. Disable for vLLM-internal hidden-cache ablations.",
    )
    ap.add_argument("--layers-per-group", type=int, default=8)
    ap.add_argument("--max-group-bytes", type=int, default=16 * 1024 * 1024)
    ap.add_argument("--max-transfer-descriptors", type=int, default=128)
    ap.add_argument("--max-transfer-bytes", type=int, default=16 * 1024 * 1024)
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
        local_hostname=local_hostname,
        prefill_gpus=tuple(int(x) for x in (args.prefill_gpus or ())),
        decode_gpus=tuple(int(x) for x in (args.decode_gpus or ())),
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

        for idx, port in enumerate(prefill_ports):
            key = f"prefill-{idx}"
            _wait_ready(
                key,
                f"http://{local_hostname}:{port}",
                proc=named_procs[key],
                log_path=logs[key],
                timeout_s=min(300.0, max(60.0, float(args.timeout))),
                paths=("/health",),
            )
        for idx, port in enumerate(decode_ports):
            key = f"decode-{idx}"
            _wait_ready(
                key,
                f"http://{local_hostname}:{port}",
                proc=named_procs[key],
                log_path=logs[key],
                timeout_s=min(300.0, max(60.0, float(args.timeout))),
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
        for idx, entry in enumerate(_agent_pd_probe_entries(cfg.model, int(args.agent_pd_probes))):
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
        _validate_summary(summary)
        out = workdir / "serving_e2e_summary.json"
        out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"wrote {out}")
    finally:
        _terminate_all(procs)


if __name__ == "__main__":
    main()
