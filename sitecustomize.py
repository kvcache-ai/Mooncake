"""Temporary compatibility carrier for Mooncake EPD vLLM hooks.

This module is loaded automatically by Python when the workspace root is on
``PYTHONPATH``. The generated vLLM serving scripts already export that path,
so the patch stays local to this project instead of mutating site-packages.

New EPD workers must use ``mooncake_epd.integrations.vllm.launcher``.  This
module remains only for existing generated scripts that explicitly set
``MOONCAKE_EPD_ENABLE_VLLM_PATCHES=1``; it never patches a normal Python/vLLM
process.  The explicit adapter owns capability probing and installation.

Legacy hook scope:
1. Allow ``SamplingParams(max_tokens=0)`` for prompt-only prefill.
2. Finalize prompt-only prefill requests after the prompt forward completes so
   KV handoff metadata is emitted without sampling a first decode token.
3. Install safe Mooncake FeatureHandle helpers for vLLM Qwen-VL multimodal
   hidden-state reuse.  The helper is fail-open by default for compatibility,
   but re-raises in MOONCAKE_EPD_STRICT / strict-no-fallback evaluation mode.
"""

from __future__ import annotations

import atexit
import asyncio
import inspect
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from typing import Any, Callable


logger = logging.getLogger("mooncake_epd.sitecustomize")


def _strict_no_fallback() -> bool:
    try:
        from mooncake_epd.core.strict_mode import strict_no_fallback_enabled

        return strict_no_fallback_enabled()
    except Exception:
        return False


def _vllm_patches_enabled() -> bool:
    """Keep workspace import side effects opt-in for generated EPD workers."""

    return str(os.getenv("MOONCAKE_EPD_ENABLE_VLLM_PATCHES", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }



# ---------------------------------------------------------------------------
# vLLM Decode Engine first-token timing metrics
# ---------------------------------------------------------------------------
def _decode_engine_timing_enabled() -> bool:
    import os

    if not bool(str(os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "")).strip()):
        return False
    # Only the real Decode/consumer vLLM process should report request->first-token
    # timing.  Producer-side prompt-only Prefill may still sample an internal token
    # in some vLLM versions; including it pollutes the TTFT decomposition.
    kv_role = str(os.getenv("MOONCAKE_EPD_KV_ROLE", "") or "").strip().lower()
    explicit_role = str(os.getenv("MOONCAKE_EPD_VLLM_ROLE", "") or "").strip().lower()
    if kv_role == "kv_producer" or explicit_role == "prefill":
        return False
    if kv_role and kv_role != "kv_consumer":
        return False
    if explicit_role and explicit_role != "decode":
        return False
    return True


def _decode_engine_metrics_path() -> "Path | None":
    import os
    from pathlib import Path

    metrics_dir = str(os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "")).strip()
    if not metrics_dir:
        return None
    path = Path(metrics_dir).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    role = str(os.getenv("MOONCAKE_EPD_VLLM_ROLE", "decode") or "decode")
    engine_id = str(os.getenv("VLLM_ENGINE_ID", os.getenv("MOONCAKE_EPD_ENGINE_ID", "vllm")) or "vllm")
    worker_id = str(os.getenv("MOONCAKE_EPD_WORKER_ID", "") or "")
    import os as _os

    component = "-".join(
        _sanitize_metrics_component(item)
        for item in ("decode-engine", role, engine_id, worker_id or f"pid{_os.getpid()}")
        if item
    )
    return path / f"{component}.json"


def _sanitize_metrics_component(value: object) -> str:
    raw = "unknown" if value is None else str(value).strip()
    raw = raw or "unknown"
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)


def _kv_transfer_params_present(value: object) -> bool:
    if not value:
        return False
    if isinstance(value, dict):
        # Any dict means the request is on the disaggregated KV path.  Keep this
        # broad because vLLM / connector versions use different key layouts.
        return True
    return True


_DECODE_ENGINE_TIMING_LOCK = threading.RLock()
_DECODE_ENGINE_TIMING_METRICS = {
    "first_token_requests": 0,
    "first_token_latency_ms_total": 0.0,
    "kv_first_token_requests": 0,
    "kv_first_token_latency_ms_total": 0.0,
    "kv_first_token_output_tokens": 0,
    "scheduler_update_calls": 0,
}
_DECODE_ENGINE_TIMING_SEEN: "OrderedDict[str, None]" = OrderedDict()
_DECODE_ENGINE_TIMING_LAST_FLUSH = 0.0
_DECODE_ENGINE_TIMING_DIRTY_SEQUENCE = 0
_DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE = 0
_DECODE_ENGINE_TIMING_FLUSH_THREAD: threading.Thread | None = None
_DECODE_ENGINE_TIMING_FLUSH_STOP: threading.Event | None = None
_DECODE_ENGINE_TIMING_METRICS_PATH: Any = None
_DECODE_ENGINE_TIMING_ATEXIT_REGISTERED = False


def _decode_engine_timing_flush_interval_s() -> float:
    try:
        value = float(os.getenv("MOONCAKE_EPD_DECODE_TIMING_FLUSH_INTERVAL_S", "1.0"))
    except (TypeError, ValueError):
        value = 1.0
    return min(60.0, max(0.1, value))


def _decode_engine_timing_seen_limit() -> int:
    try:
        value = int(os.getenv("MOONCAKE_EPD_DECODE_TIMING_SEEN_MAX_ENTRIES", "65536"))
    except (TypeError, ValueError):
        value = 65536
    return min(1_000_000, max(1024, value))


def _mark_decode_engine_first_token_seen_locked(request_id: str) -> bool:
    if request_id in _DECODE_ENGINE_TIMING_SEEN:
        _DECODE_ENGINE_TIMING_SEEN.move_to_end(request_id)
        return False
    _DECODE_ENGINE_TIMING_SEEN[request_id] = None
    while len(_DECODE_ENGINE_TIMING_SEEN) > _decode_engine_timing_seen_limit():
        _DECODE_ENGINE_TIMING_SEEN.popitem(last=False)
    return True


def _decode_engine_timing_flusher(stop_event: threading.Event) -> None:
    interval_s = _decode_engine_timing_flush_interval_s()
    while not stop_event.wait(interval_s):
        _flush_decode_engine_timing(force=False)


def _flush_decode_engine_timing_at_exit() -> None:
    try:
        _flush_decode_engine_timing(force=True)
    except Exception:
        # Metrics must never turn process teardown into a serving failure.
        logger.debug("Mooncake decode engine final timing flush skipped", exc_info=True)


def _ensure_decode_engine_timing_flusher_locked() -> None:
    global _DECODE_ENGINE_TIMING_ATEXIT_REGISTERED
    global _DECODE_ENGINE_TIMING_FLUSH_STOP
    global _DECODE_ENGINE_TIMING_FLUSH_THREAD

    if (
        _DECODE_ENGINE_TIMING_FLUSH_THREAD is not None
        and _DECODE_ENGINE_TIMING_FLUSH_THREAD.is_alive()
    ):
        return
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_decode_engine_timing_flusher,
        args=(stop_event,),
        name="mooncake-epd-decode-timing",
        daemon=True,
    )
    _DECODE_ENGINE_TIMING_FLUSH_STOP = stop_event
    _DECODE_ENGINE_TIMING_FLUSH_THREAD = thread
    thread.start()
    if not _DECODE_ENGINE_TIMING_ATEXIT_REGISTERED:
        atexit.register(_flush_decode_engine_timing_at_exit)
        _DECODE_ENGINE_TIMING_ATEXIT_REGISTERED = True


def _mark_decode_engine_timing_dirty_locked() -> None:
    global _DECODE_ENGINE_TIMING_DIRTY_SEQUENCE
    _DECODE_ENGINE_TIMING_DIRTY_SEQUENCE += 1


def _record_decode_engine_scheduler_update() -> None:
    if not _decode_engine_timing_enabled():
        return
    with _DECODE_ENGINE_TIMING_LOCK:
        _DECODE_ENGINE_TIMING_METRICS["scheduler_update_calls"] += 1
        _mark_decode_engine_timing_dirty_locked()
        _ensure_decode_engine_timing_flusher_locked()


def _record_decode_engine_first_token(
    *,
    request_id: str,
    arrival_time_s: float | None,
    kv_transfer_params: object,
    output_tokens: int,
) -> None:
    if not _decode_engine_timing_enabled() or not request_id:
        return
    now_s = time.time()
    start_s = float(arrival_time_s or now_s)
    latency_ms = max(0.0, (now_s - start_s) * 1000.0)
    with _DECODE_ENGINE_TIMING_LOCK:
        if not _mark_decode_engine_first_token_seen_locked(request_id):
            return
        _DECODE_ENGINE_TIMING_METRICS["first_token_requests"] += 1
        _DECODE_ENGINE_TIMING_METRICS["first_token_latency_ms_total"] += latency_ms
        if _kv_transfer_params_present(kv_transfer_params):
            _DECODE_ENGINE_TIMING_METRICS["kv_first_token_requests"] += 1
            _DECODE_ENGINE_TIMING_METRICS["kv_first_token_latency_ms_total"] += latency_ms
            _DECODE_ENGINE_TIMING_METRICS["kv_first_token_output_tokens"] += max(0, int(output_tokens))
        _mark_decode_engine_timing_dirty_locked()
        _ensure_decode_engine_timing_flusher_locked()


def _flush_decode_engine_timing(*, force: bool) -> None:
    global _DECODE_ENGINE_TIMING_LAST_FLUSH
    global _DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE

    now = time.time()
    with _DECODE_ENGINE_TIMING_LOCK:
        if _DECODE_ENGINE_TIMING_DIRTY_SEQUENCE == _DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE:
            return
        if not force and (now - float(_DECODE_ENGINE_TIMING_LAST_FLUSH)) < _decode_engine_timing_flush_interval_s():
            return
        sequence = _DECODE_ENGINE_TIMING_DIRTY_SEQUENCE
        metrics = dict(_DECODE_ENGINE_TIMING_METRICS)
    path = _decode_engine_metrics_path()
    if path is None:
        return
    payload = {
        "version": 1,
        "kind": "decode_engine_timing",
        "updated_at": now,
        "identity": {
            "pid": os.getpid(),
            "role": os.getenv("MOONCAKE_EPD_VLLM_ROLE", "decode"),
            "engine_id": os.getenv("VLLM_ENGINE_ID", os.getenv("MOONCAKE_EPD_ENGINE_ID", "vllm")),
            "worker_id": os.getenv("MOONCAKE_EPD_WORKER_ID", ""),
        },
        "metrics": metrics,
    }
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(tmp_path, path)
        with _DECODE_ENGINE_TIMING_LOCK:
            _DECODE_ENGINE_TIMING_LAST_FLUSH = now
            _DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE = max(
                _DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE,
                sequence,
            )
    except Exception as exc:
        logger.debug("Mooncake decode engine timing flush skipped: %s", exc)


def _flush_decode_engine_timing_locked(*, force: bool) -> None:
    """Compatibility wrapper for older local instrumentation callers."""

    _flush_decode_engine_timing(force=force)


def _remove_prompt_only_sample_outputs(
    outputs: list[Any],
    request_id: str,
) -> list[Any]:
    """Remove only sampled-token outputs for one prompt-only request.

    vLLM computes prompt logits and may materialize one sampled token before
    the scheduler patch finalizes ``max_tokens=0``.  That token has not been
    forwarded through the model and is not part of the published prompt KV.
    It must not escape through the OpenAI response, otherwise Prefill appears
    to have generated a semantic continuation that Decode will independently
    sample again.
    """

    retained: list[Any] = []
    removed: list[Any] = []
    for output in list(outputs or []):
        same_request = str(getattr(output, "request_id", "") or "") == str(
            request_id
        )
        sampled_tokens = list(getattr(output, "new_token_ids", None) or [])
        if same_request and sampled_tokens:
            removed.append(output)
        else:
            retained.append(output)
    outputs[:] = retained
    return removed


def _patch_vllm_prompt_envelope_metadata() -> None:
    """Capture exact MM placeholder spans from the already-executed render.

    The prompt-only OpenAI response natively exposes prompt token IDs but not
    the multimodal placeholder positions used to build its EngineInput.
    Re-rendering in the proxy would repeat the expensive MM preprocessing that
    stage B removes.  A task-local capture around the existing render call
    exports only token IDs, hashes, and ranges; it never exports media bytes or
    tensors.
    """

    try:
        import contextvars
        import hashlib
        from vllm.entrypoints.openai.chat_completion.serving import (
            OpenAIServingChat,
        )
    except Exception:
        return
    if getattr(
        OpenAIServingChat.render_chat_request,
        "_mooncake_epd_prompt_envelope_patch",
        False,
    ):
        return

    capture: contextvars.ContextVar[dict[str, Any] | None] = (
        contextvars.ContextVar(
            "mooncake_epd_prompt_envelope_capture", default=None
        )
    )
    original_render = OpenAIServingChat.render_chat_request
    original_create = OpenAIServingChat.create_chat_completion
    structural_metadata_by_hash: OrderedDict[str, dict[str, Any]] = (
        OrderedDict()
    )
    structural_metadata_limit = 4096

    def _range_dict(value: Any) -> dict[str, int]:
        if isinstance(value, dict):
            offset = value.get("offset", 0)
            length = value.get("length", 0)
        else:
            offset = getattr(value, "offset", 0)
            length = getattr(value, "length", 0)
        return {
            # Preserve the exact renderer values.  The versioned envelope
            # performs fail-closed range validation; coercing negatives here
            # would turn corrupted metadata into a different valid range.
            "offset": int(offset or 0),
            "length": int(length or 0),
        }

    def _small_mm_metadata(engine_input: dict[str, Any]) -> dict[str, Any]:
        allowed_keys = {
            "image_grid_thw",
            "video_grid_thw",
            "second_per_grid_ts",
            "image_sizes",
        }
        result: dict[str, list[dict[str, Any]]] = {}
        raw_kwargs = engine_input.get("mm_kwargs") or {}
        for modality, items in dict(raw_kwargs).items():
            serialized_items: list[dict[str, Any]] = []
            for item in list(items or []):
                values = (
                    item.get_data()
                    if hasattr(item, "get_data")
                    else dict(item or {})
                )
                serialized: dict[str, Any] = {}
                for key in allowed_keys:
                    if key not in values:
                        continue
                    value = values[key]
                    if hasattr(value, "detach"):
                        value = value.detach().cpu().tolist()
                    elif hasattr(value, "tolist"):
                        value = value.tolist()
                    serialized[key] = value
                serialized_items.append(serialized)
            if serialized_items:
                result[str(modality)] = serialized_items
        return result

    async def _patched_render(self: Any, request: Any) -> Any:
        result = await original_render(self, request)
        try:
            if not isinstance(result, tuple) or len(result) != 2:
                return result
            engine_inputs = list(result[1] or [])
            if len(engine_inputs) != 1:
                return result
            engine_input = engine_inputs[0]
            if not isinstance(engine_input, dict):
                return result
            token_ids = [
                int(token_id)
                for token_id in list(
                    engine_input.get("prompt_token_ids") or []
                )
            ]
            raw_placeholders = dict(
                engine_input.get("mm_placeholders") or {}
            )
            placeholders = {
                str(modality): [
                    _range_dict(item) for item in list(ranges or [])
                ]
                for modality, ranges in raw_placeholders.items()
            }
            raw_hashes = dict(engine_input.get("mm_hashes") or {})
            mm_hashes = {
                str(modality): [
                    str(item) for item in list(items or [])
                ]
                for modality, items in raw_hashes.items()
            }
            mm_metadata = _small_mm_metadata(engine_input)
            for modality, hashes in mm_hashes.items():
                metadata_items = list(mm_metadata.get(modality) or [])
                restored_items: list[dict[str, Any]] = []
                for index, mm_hash in enumerate(hashes):
                    item = (
                        dict(metadata_items[index])
                        if index < len(metadata_items)
                        and metadata_items[index]
                        else {}
                    )
                    if item:
                        structural_metadata_by_hash[mm_hash] = dict(item)
                        structural_metadata_by_hash.move_to_end(mm_hash)
                        while (
                            len(structural_metadata_by_hash)
                            > structural_metadata_limit
                        ):
                            structural_metadata_by_hash.popitem(last=False)
                    else:
                        cached = structural_metadata_by_hash.get(mm_hash)
                        if cached is not None:
                            item = dict(cached)
                            structural_metadata_by_hash.move_to_end(mm_hash)
                    restored_items.append(item)
                if restored_items:
                    mm_metadata[modality] = restored_items
            capture.set(
                {
                    "version": 1,
                    "prompt_token_ids": token_ids,
                    "prompt_token_sha256": hashlib.sha256(
                        json.dumps(
                            token_ids, separators=(",", ":")
                        ).encode("utf-8")
                    ).hexdigest(),
                    "mm_placeholders": placeholders,
                    "mm_hashes": mm_hashes,
                    "mm_metadata": mm_metadata,
                }
            )
        except Exception:
            if _strict_no_fallback():
                raise
            logger.exception("Mooncake prompt envelope capture skipped")
        return result

    async def _patched_create(
        self: Any,
        request: Any,
        raw_request: Any = None,
    ) -> Any:
        token = capture.set(None)
        try:
            response = await original_create(self, request, raw_request)
            metadata = getattr(request, "metadata", None)
            is_prompt_only = bool(
                isinstance(metadata, dict)
                and metadata.get("mooncake_epd_prompt_only_prefill")
            )
            captured = capture.get()
            if is_prompt_only and captured is not None:
                setattr(
                    response,
                    "mooncake_epd_prompt_envelope",
                    dict(captured),
                )
            return response
        finally:
            capture.reset(token)

    _patched_render._mooncake_epd_prompt_envelope_patch = True  # type: ignore[attr-defined]
    _patched_create._mooncake_epd_prompt_envelope_patch = True  # type: ignore[attr-defined]
    OpenAIServingChat.render_chat_request = _patched_render  # type: ignore[method-assign]
    OpenAIServingChat.create_chat_completion = _patched_create  # type: ignore[method-assign]
    logger.info("enabled Mooncake EPD prompt envelope metadata capture")


def _patch_vllm_decode_token_multimodal_metadata() -> None:
    """Restore MM request identity without restoring media on Decode.

    vLLM renders a Completion token array as a text ``EngineInput``.  Remote
    KV produced by a multimodal Chat request is keyed and scheduled with MM
    placeholder/hash metadata; dropping that identity can produce valid HTTP
    responses from the wrong continuation state.  The proxy's checksummed
    envelope carries only Prefill-rendered hashes and token ranges.  Rebuild a
    multimodal ``EngineInput`` with inert item descriptors so the scheduler
    sees the same identity while never receiving image/video/audio bytes.

    The inert descriptors are safe only for remote-prefill requests where all
    MM placeholder tokens are covered by transferred KV.  If vLLM attempts to
    schedule the encoder, the empty descriptor fails rather than recomputing.
    """

    try:
        import hashlib
        from vllm.entrypoints.openai.completion.serving import (
            OpenAIServingCompletion,
        )
        from vllm.multimodal.inputs import (
            MultiModalFieldElem,
            MultiModalKwargsItem,
            MultiModalSharedField,
            PlaceholderRange,
        )
        import torch
    except Exception:
        return
    if getattr(
        OpenAIServingCompletion.render_completion_request,
        "_mooncake_epd_decode_token_mm_patch",
        False,
    ):
        return

    original_render = OpenAIServingCompletion.render_completion_request

    def _canonical_sha256(value: Any) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    async def _patched_render(self: Any, request: Any) -> Any:
        result = await original_render(self, request)
        metadata = getattr(request, "metadata", None)
        if not isinstance(metadata, dict) or not bool(
            metadata.get("mooncake_epd_decode_media_stripped")
        ):
            return result
        envelope = metadata.get("mooncake_epd_decode_token_envelope")
        if not isinstance(envelope, dict):
            raise ValueError("missing Mooncake Decode token envelope metadata")
        if (
            envelope.get("kind")
            != "mooncake_epd.decode_token_envelope"
            or int(envelope.get("version", 0) or 0) != 1
        ):
            raise ValueError("unsupported Mooncake Decode token envelope")
        if not isinstance(result, list) or len(result) != 1:
            raise ValueError(
                "Mooncake Decode token path requires one rendered prompt"
            )
        engine_input = result[0]
        if not isinstance(engine_input, dict):
            raise ValueError("Decode renderer did not return an EngineInput")
        token_ids = [
            int(token_id)
            for token_id in list(
                engine_input.get("prompt_token_ids") or []
            )
        ]
        if int(envelope.get("prompt_token_count", -1)) != len(token_ids):
            raise ValueError("Decode renderer prompt length mismatch")
        if str(envelope.get("prompt_token_sha256") or "") != (
            _canonical_sha256(token_ids)
        ):
            raise ValueError("Decode renderer prompt token digest mismatch")

        spans = envelope.get("multimodal_placeholder_spans") or {}
        mm_hashes = envelope.get("render_multimodal_hashes") or {}
        mm_metadata = envelope.get("render_multimodal_metadata") or {}
        if not spans and not mm_hashes:
            return result
        if not isinstance(spans, dict) or not isinstance(mm_hashes, dict):
            raise ValueError("Decode renderer MM metadata must be objects")
        if str(
            envelope.get("multimodal_placeholder_spans_sha256") or ""
        ) != _canonical_sha256(spans):
            raise ValueError("Decode renderer MM placeholder digest mismatch")
        if str(envelope.get("render_multimodal_hashes_sha256") or "") != (
            _canonical_sha256(mm_hashes)
        ):
            raise ValueError("Decode renderer MM hash digest mismatch")
        if str(
            envelope.get("render_multimodal_metadata_sha256") or ""
        ) != _canonical_sha256(mm_metadata):
            raise ValueError("Decode renderer MM structural metadata mismatch")
        if set(spans) != set(mm_hashes):
            raise ValueError("Decode renderer MM modalities mismatch")
        if set(spans) != set(mm_metadata):
            raise ValueError(
                "Decode renderer MM structural metadata modalities mismatch"
            )

        placeholders: dict[str, list[Any]] = {}
        kwargs_items: dict[str, list[Any]] = {}
        normalized_hashes: dict[str, list[str]] = {}
        for modality, raw_ranges in spans.items():
            hashes = [str(value) for value in list(mm_hashes[modality])]
            ranges = list(raw_ranges or [])
            metadata_items = list(mm_metadata[modality] or [])
            if (
                not ranges
                or len(hashes) != len(ranges)
                or len(metadata_items) != len(ranges)
            ):
                raise ValueError(
                    "Decode renderer MM item count mismatch for "
                    f"{modality!r}"
                )
            placeholders[str(modality)] = [
                PlaceholderRange(
                    offset=int(item["offset"]),
                    length=int(item["length"]),
                )
                for item in ranges
            ]
            normalized_hashes[str(modality)] = hashes
            rebuilt_items: list[Any] = []
            for item in metadata_items:
                if not isinstance(item, dict) or not item:
                    raise ValueError(
                        "Decode renderer MM structural item is empty"
                    )
                fields = {}
                for key, value in item.items():
                    tensor = torch.tensor(value)
                    fields[str(key)] = MultiModalFieldElem(
                        data=tensor,
                        field=MultiModalSharedField(batch_size=1),
                    )
                rebuilt_items.append(MultiModalKwargsItem(fields))
            kwargs_items[str(modality)] = rebuilt_items
        rebuilt = dict(engine_input)
        rebuilt["type"] = "multimodal"
        rebuilt["mm_placeholders"] = placeholders
        rebuilt["mm_hashes"] = normalized_hashes
        rebuilt["mm_kwargs"] = kwargs_items
        result[0] = rebuilt
        return result

    _patched_render._mooncake_epd_decode_token_mm_patch = True  # type: ignore[attr-defined]
    OpenAIServingCompletion.render_completion_request = _patched_render  # type: ignore[method-assign]
    logger.info("enabled Mooncake EPD media-free Decode MM identity")


def _patch_vllm_prompt_only_prefill() -> None:
    try:
        from vllm.sampling_params import SamplingParams
        from vllm.v1.core.sched.scheduler import Scheduler
        from vllm.v1.core.sched.utils import remove_all
        from vllm.v1.engine import EngineCoreOutput, EngineCoreOutputs
        from vllm.v1.request import RequestStatus
    except Exception:
        return

    if getattr(SamplingParams, "_mooncake_epd_prompt_only_patch", False):
        return

    original_verify_args = SamplingParams._verify_args

    def _patched_verify_args(self: SamplingParams) -> None:
        if self.max_tokens == 0:
            if self.min_tokens != 0:
                raise ValueError(
                    "min_tokens must be 0 when prompt-only prefill uses max_tokens=0."
                )
            original_max_tokens = self.max_tokens
            self.max_tokens = 1
            try:
                original_verify_args(self)
            finally:
                self.max_tokens = original_max_tokens
            return
        original_verify_args(self)

    SamplingParams._verify_args = _patched_verify_args  # type: ignore[method-assign]

    original_update_from_output = Scheduler.update_from_output

    def _patched_update_from_output(
        self: Scheduler,
        scheduler_output: Any,
        model_runner_output: Any,
    ) -> dict[int, EngineCoreOutputs]:
        # Snapshot request timing metadata before upstream mutates/free's request
        # state.  This lets the proxy expose the true Decode Engine request->first
        # token segment separately from P->D receive and HTTP streaming overhead.
        timing_enabled = _decode_engine_timing_enabled()
        timing_info: dict[str, tuple[float | None, object]] = {}
        if timing_enabled:
            try:
                for req_id in getattr(scheduler_output, "num_scheduled_tokens", {}) or {}:
                    request = self.requests.get(req_id)
                    if request is None:
                        continue
                    timing_info[str(req_id)] = (
                        getattr(request, "arrival_time", None),
                        getattr(request, "kv_transfer_params", None),
                    )
            except Exception as exc:
                logger.debug("Mooncake decode timing pre-snapshot skipped: %s", exc)
                if _strict_no_fallback():
                    raise

        engine_core_outputs = original_update_from_output(
            self,
            scheduler_output,
            model_runner_output,
        )

        if timing_enabled:
            try:
                _record_decode_engine_scheduler_update()
                for outputs in engine_core_outputs.values():
                    for output in getattr(outputs, "outputs", []) or []:
                        new_token_ids = getattr(output, "new_token_ids", None) or []
                        if not new_token_ids:
                            continue
                        req_id = str(getattr(output, "request_id", "") or "")
                        arrival_time_s, kv_transfer_params = timing_info.get(req_id, (None, None))
                        if kv_transfer_params is None:
                            kv_transfer_params = getattr(output, "kv_transfer_params", None)
                        _record_decode_engine_first_token(
                            request_id=req_id,
                            arrival_time_s=arrival_time_s,
                            kv_transfer_params=kv_transfer_params,
                            output_tokens=len(new_token_ids),
                        )
            except Exception as exc:
                logger.debug("Mooncake decode timing record skipped: %s", exc)
                if _strict_no_fallback():
                    raise

        prompt_only_requests = []
        for req_id in scheduler_output.num_scheduled_tokens:
            request = self.requests.get(req_id)
            if request is None or request.is_finished():
                continue
            if getattr(request, "max_tokens", None) != 0:
                continue
            if request.num_output_tokens != 0:
                continue
            if request.num_computed_tokens < request.num_tokens:
                continue
            prompt_only_requests.append(request)

        for request in prompt_only_requests:
            eco = engine_core_outputs.setdefault(
                request.client_index,
                EngineCoreOutputs(),
            )
            discarded_outputs = _remove_prompt_only_sample_outputs(
                eco.outputs,
                request.request_id,
            )
            status_before_stop = request.status
            request.status = RequestStatus.FINISHED_LENGTH_CAPPED
            finish_reason = request.get_finished_reason()
            finished = self._handle_stopped_request(request)
            kv_transfer_params = self._free_request(request) if finished else None

            if status_before_stop == RequestStatus.RUNNING:
                self.running = remove_all(self.running, {request})
            else:
                self.waiting.remove_requests({request})

            # Preserve events/prefill stats already detached by upstream while
            # replacing only its sampled-token payload with the prompt-only
            # terminal output.
            discarded = discarded_outputs[-1] if discarded_outputs else None
            events = request.take_events()
            if not events and discarded is not None:
                discarded_events = getattr(discarded, "events", None)
                if discarded_events is not None:
                    events = discarded_events
            prefill_stats = request.take_prefill_stats()
            if prefill_stats is None and discarded is not None:
                prefill_stats = getattr(discarded, "prefill_stats", None)
            eco.outputs.append(
                EngineCoreOutput(
                    request_id=request.request_id,
                    new_token_ids=[],
                    finish_reason=finish_reason,
                    events=events,
                    prefill_stats=prefill_stats,
                    kv_transfer_params=kv_transfer_params,
                    trace_headers=request.trace_headers,
                    num_nans_in_logits=request.num_nans_in_logits,
                )
            )

        return engine_core_outputs

    Scheduler.update_from_output = _patched_update_from_output  # type: ignore[method-assign]
    _patched_update_from_output._mooncake_epd_prompt_only_patch = True  # type: ignore[attr-defined]
    _patched_update_from_output._mooncake_epd_prompt_only_protocol_version = 2  # type: ignore[attr-defined]
    SamplingParams._mooncake_epd_prompt_only_patch = True  # type: ignore[attr-defined]
    SamplingParams._mooncake_epd_prompt_only_protocol_version = 2  # type: ignore[attr-defined]
    logger.info("enabled Mooncake EPD prompt-only prefill patch for vLLM")


def _has_var_kwargs(fn: Callable[..., Any]) -> bool:
    try:
        return any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in inspect.signature(fn).parameters.values()
        )
    except Exception:
        return False


def _accepts_kw(fn: Callable[..., Any], name: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return True
    return name in sig.parameters or _has_var_kwargs(fn)


def _patch_callable_for_feature_handles(cls: type, method_name: str) -> bool:
    original = getattr(cls, method_name, None)
    if original is None or getattr(original, "_mooncake_epd_feature_handle_patch", False):
        return False
    if not callable(original):
        return False

    def _patched(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            from mooncake_epd.core.state.vllm_feature_handle_provider import (
                maybe_inject_feature_handle_kwargs,
            )

            if "image_embeds" not in kwargs and (
                kwargs.get("kv_transfer_params") is not None
                or kwargs.get("mm_feature_handles") is not None
                or kwargs.get("mooncake_epd_feature_handles") is not None
            ):
                injected = maybe_inject_feature_handle_kwargs(
                    kwargs,
                    device=getattr(getattr(self, "device", None), "type", None) or getattr(self, "device", None),
                    dtype=getattr(self, "dtype", None),
                )
                if injected is not kwargs:
                    # Only forward keys accepted by this vLLM version.  Unknown
                    # names are removed before calling upstream code.
                    kwargs = {
                        key: value
                        for key, value in injected.items()
                        if key in kwargs or _accepts_kw(original, key)
                    }
        except Exception as exc:
            logger.debug("Mooncake FeatureHandle injection skipped: %s", exc)
            if _strict_no_fallback():
                raise
        return original(self, *args, **kwargs)

    _patched._mooncake_epd_feature_handle_patch = True  # type: ignore[attr-defined]
    setattr(cls, method_name, _patched)
    return True


def _patch_vllm_feature_handle_injection() -> None:
    """Best-effort vLLM multimodal hidden-state injection.

    vLLM model internals move across versions.  We patch only known Qwen-VL
    multimodal methods and only when the method can accept an ``image_embeds``
    style kwarg (or arbitrary kwargs).  If no compatible method exists, this
    leaves serving untouched; ``scripts/check_vllm_feature_handle_patch.py`` can
    be used to make that explicit before a real EPD hidden-state run.
    """

    targets = (
        ("vllm.model_executor.models.qwen3_vl", "Qwen3VLForConditionalGeneration"),
        ("vllm.model_executor.models.qwen3_vl", "Qwen3VLModel"),
        ("vllm.model_executor.models.qwen2_5_vl", "Qwen2_5_VLForConditionalGeneration"),
        # Qwen2.5-Omni Thinker's image input accepts the native
        # ``image_embeds`` + ``image_grid_thw`` form.  This patch covers only
        # the Thinker multimodal path; it does not imply full Omni talker /
        # diffusion-pipeline support.
        (
            "vllm.model_executor.models.qwen2_5_omni_thinker",
            "Qwen2_5OmniThinkerForConditionalGeneration",
        ),
        ("vllm.model_executor.models.qwen2_vl", "Qwen2VLForConditionalGeneration"),
    )
    methods = (
        "embed_multimodal",
        "get_multimodal_embeddings",
        "get_input_embeddings",
        "forward",
    )
    patched = []
    for module_name, class_name in targets:
        try:
            module = __import__(module_name, fromlist=[class_name])
            cls = getattr(module, class_name)
        except Exception:
            continue
        for method_name in methods:
            method = getattr(cls, method_name, None)
            if method is None:
                continue
            if not (_accepts_kw(method, "image_embeds") or _has_var_kwargs(method)):
                continue
            if _patch_callable_for_feature_handles(cls, method_name):
                patched.append(f"{module_name}.{class_name}.{method_name}")
    if patched:
        logger.info("enabled Mooncake EPD FeatureHandle vLLM hooks: %s", ", ".join(patched))
    elif _strict_no_fallback():
        raise RuntimeError(
            "Mooncake EPD strict mode requires a compatible Qwen-VL multimodal image_embeds hook"
        )


def _patch_vllm_qwen25_omni_precomputed_image_embed_contract() -> bool:
    """Normalize and observe vLLM 0.23 Qwen2.5-Omni image embeddings.

    ``Qwen2_5OmniConditionalGenerationMixin._process_image_input`` returns a
    single tensor directly for its ``image_embeds`` branch.  vLLM's
    ``GPUModelRunner`` treats the return value as a sequence of per-image
    outputs, so a ``[tokens, hidden]`` tensor is iterated as ``tokens`` image
    outputs and the strict encoder-output sanity check fails.  Raw pixels take
    the normal ``Tensor.split`` branch and are already a tuple.

    Mooncake EPD intentionally supplies the same post-vision hidden tensor via
    a FeatureHandle.  Wrap only that precomputed branch in a one-item tuple and
    record it *after* vLLM has accepted the input.  The latter is deliberately
    at ``_process_image_input`` rather than the earlier FeatureHandle injection
    boundary: a positive metric then proves that the model consumed native
    ``image_embeds`` rather than merely that the proxy constructed them.

    Raw-pixel, video, and audio behavior is untouched.  This is a compatibility
    repair and observability hook for the Thinker image path, not a claim of
    full Omni talker/diffusion support.
    """

    try:
        module = __import__(
            "vllm.model_executor.models.qwen2_5_omni_thinker",
            fromlist=["Qwen2_5OmniThinkerForConditionalGeneration"],
        )
        cls = getattr(module, "Qwen2_5OmniThinkerForConditionalGeneration")
    except Exception:
        return False

    original = getattr(cls, "_process_image_input", None)
    if original is None or getattr(
        original,
        "_mooncake_epd_qwen25_omni_image_embed_contract_patch",
        False,
    ):
        return False
    if not callable(original):
        return False

    def _patched(self: Any, image_input: Any, *args: Any, **kwargs: Any) -> Any:
        result = original(self, image_input, *args, **kwargs)
        try:
            input_type = image_input.get("type") if hasattr(image_input, "get") else None
            if input_type == "image_embeds":
                import torch

                # This is the exact vLLM model-side branch that skips
                # ``self.visual(...)``.  Keep the accounting after `original`
                # returns so malformed/failed model inputs cannot become false
                # positive EPD evidence.
                try:
                    from mooncake_epd.core.state.vllm_mm_hidden_cache import (
                        get_current_mm_hidden_cache_keys,
                        record_vllm_precomputed_image_embeds_hit,
                    )

                    grid_thw = image_input.get("image_grid_thw")
                    count = 1
                    if isinstance(grid_thw, torch.Tensor) and grid_thw.ndim >= 2:
                        count = max(1, int(grid_thw.shape[0]))
                    record_vllm_precomputed_image_embeds_hit(
                        count,
                        stable_keys=get_current_mm_hidden_cache_keys(),
                    )
                except Exception:
                    # Observability must not change vLLM model semantics.  The
                    # strict runner separately requires a positive persisted
                    # metric before accepting an EPD result.
                    pass

                if isinstance(result, torch.Tensor):
                    return (result,)
        except Exception:
            # Preserve vLLM's native behavior if an unexpected custom input
            # object cannot be inspected.  Strict FeatureHandle resolution is
            # still enforced earlier in the injection path.
            pass
        return result

    _patched._mooncake_epd_qwen25_omni_image_embed_contract_patch = True  # type: ignore[attr-defined]
    _patched._mooncake_epd_feature_handle_patch = True  # type: ignore[attr-defined]
    setattr(cls, "_process_image_input", _patched)
    logger.info("enabled Mooncake EPD Qwen2.5-Omni precomputed image-embed contract patch")
    return True


def _patch_vllm_qwen25_vl_precomputed_image_embed_observability() -> bool:
    """Record Qwen2.5-VL's actual native ``image_embeds`` consumption.

    vLLM 0.23's Qwen2.5-VL implementation supports precomputed embeddings but
    does not emit the hidden-state-skip metric used by the strict EPD gate.
    The wrapper is intentionally limited to the successful model-side
    ``_process_image_input`` call.  It neither changes tensors nor alters the
    raw-pixel branch, so a non-zero metric remains evidence that the vision
    tower was bypassed for that invocation.
    """

    try:
        module = __import__(
            "vllm.model_executor.models.qwen2_5_vl",
            fromlist=["Qwen2_5_VLForConditionalGeneration"],
        )
        cls = getattr(module, "Qwen2_5_VLForConditionalGeneration")
    except Exception:
        return False

    original = getattr(cls, "_process_image_input", None)
    if original is None or getattr(
        original,
        "_mooncake_epd_qwen25_vl_precomputed_image_embed_metric_patch",
        False,
    ):
        return False
    if not callable(original):
        return False

    def _patched(self: Any, image_input: Any, *args: Any, **kwargs: Any) -> Any:
        result = original(self, image_input, *args, **kwargs)
        try:
            input_type = image_input.get("type") if hasattr(image_input, "get") else None
            if input_type == "image_embeds":
                import torch
                from mooncake_epd.core.state.vllm_mm_hidden_cache import (
                    get_current_mm_hidden_cache_keys,
                    record_vllm_precomputed_image_embeds_hit,
                )

                grid_thw = image_input.get("image_grid_thw")
                count = 1
                if isinstance(grid_thw, torch.Tensor) and grid_thw.ndim >= 2:
                    count = max(1, int(grid_thw.shape[0]))
                record_vllm_precomputed_image_embeds_hit(
                    count,
                    stable_keys=get_current_mm_hidden_cache_keys(),
                )
        except Exception:
            # The strict result gate will reject missing evidence.  Never turn
            # a telemetry write error into a model-serving error here.
            pass
        return result

    _patched._mooncake_epd_qwen25_vl_precomputed_image_embed_metric_patch = True  # type: ignore[attr-defined]
    _patched._mooncake_epd_feature_handle_patch = True  # type: ignore[attr-defined]
    setattr(cls, "_process_image_input", _patched)
    logger.info("enabled Mooncake EPD Qwen2.5-VL precomputed image-embed metric patch")
    return True


def _patch_vllm_gpu_model_runner_feature_handles() -> None:
    try:
        from vllm.v1.worker.gpu_model_runner import GPUModelRunner
    except Exception as exc:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD strict mode requires vLLM GPUModelRunner") from exc
        return
    original = getattr(GPUModelRunner, "_batch_mm_inputs_from_scheduler", None)
    if original is None:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD strict mode requires GPUModelRunner._batch_mm_inputs_from_scheduler")
        return
    if getattr(original, "_mooncake_epd_feature_handle_patch", False):
        return
    try:
        sig = inspect.signature(original)
        if "scheduler_output" not in sig.parameters and len(sig.parameters) < 2:
            raise TypeError(f"unexpected signature: {sig}")
    except Exception as exc:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD FeatureHandle hook is incompatible with this vLLM GPUModelRunner") from exc

    def _patched_batch_mm_inputs_from_scheduler(self: Any, scheduler_output: Any) -> Any:
        result = original(self, scheduler_output)
        try:
            from mooncake_epd.core.state.vllm_feature_handle_provider import (
                inject_feature_handles_into_vllm_mm_kwargs,
            )

            mm_hashes, mm_kwargs, mm_lora_refs = result
            return inject_feature_handles_into_vllm_mm_kwargs(
                mm_hashes=mm_hashes,
                mm_kwargs=mm_kwargs,
                mm_lora_refs=mm_lora_refs,
                requests=getattr(self, "requests", {}),
                device=getattr(self, "device", None),
                dtype=getattr(self, "dtype", None),
            )
        except Exception as exc:
            logger.debug("Mooncake FeatureHandle runner injection skipped: %s", exc)
            if _strict_no_fallback():
                raise
            return result

    _patched_batch_mm_inputs_from_scheduler._mooncake_epd_feature_handle_patch = True  # type: ignore[attr-defined]
    GPUModelRunner._batch_mm_inputs_from_scheduler = _patched_batch_mm_inputs_from_scheduler  # type: ignore[method-assign]
    logger.info("enabled Mooncake EPD FeatureHandle GPUModelRunner hook")


def _patch_vllm_gpu_model_runner_kv_params() -> None:
    try:
        from vllm.v1.worker.gpu_model_runner import GPUModelRunner
    except Exception as exc:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD strict mode requires vLLM GPUModelRunner") from exc
        return
    original = getattr(GPUModelRunner, "_update_states", None)
    if original is None:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD strict mode requires GPUModelRunner._update_states")
        return
    if getattr(original, "_mooncake_epd_kv_params_patch", False):
        return
    try:
        sig = inspect.signature(original)
        if "scheduler_output" not in sig.parameters and len(sig.parameters) < 2:
            raise TypeError(f"unexpected signature: {sig}")
    except Exception as exc:
        if _strict_no_fallback():
            raise RuntimeError("Mooncake EPD kv_transfer_params hook is incompatible with this vLLM GPUModelRunner") from exc

    def _patched_update_states(self: Any, scheduler_output: Any) -> Any:
        result = original(self, scheduler_output)
        try:
            for new_req_data in getattr(scheduler_output, "scheduled_new_reqs", []) or []:
                req_id = getattr(new_req_data, "req_id", None)
                if req_id is None:
                    continue
                req_state = getattr(self, "requests", {}).get(req_id)
                if req_state is None:
                    continue
                sampling_params = getattr(new_req_data, "sampling_params", None)
                extra_args = getattr(sampling_params, "extra_args", None) if sampling_params is not None else None
                kv_transfer_params = None
                if isinstance(extra_args, dict):
                    kv_transfer_params = extra_args.get("kv_transfer_params")
                if kv_transfer_params:
                    setattr(req_state, "kv_transfer_params", dict(kv_transfer_params))
                    try:
                        from mooncake_epd.core.state.vllm_mm_hidden_cache import trace_vllm_mm_hidden_event

                        trace_vllm_mm_hidden_event(
                            "gpu_runner_request_kv_params_attached",
                            req_id=req_id,
                            has_feature_handles=bool(kv_transfer_params.get("mm_feature_handles")),
                        )
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("Mooncake kv_transfer_params attach skipped: %s", exc)
            if _strict_no_fallback():
                raise
        return result

    _patched_update_states._mooncake_epd_kv_params_patch = True  # type: ignore[attr-defined]
    GPUModelRunner._update_states = _patched_update_states  # type: ignore[method-assign]
    logger.info("enabled Mooncake EPD GPUModelRunner kv_transfer_params attach hook")


# ---------------------------------------------------------------------------
# Prefill-owned E→P direct FeatureBundle allocation routes
# ---------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    import os

    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _install_vllm_capability_route(app: Any) -> None:
    """Expose the installed adapter protocol from the actual API process."""

    if getattr(app, "_mooncake_epd_capability_route", False):
        return
    try:
        import hashlib
        from mooncake_epd.integrations.vllm.adapter import (
            adapter_installation_report,
        )
        from mooncake_epd.integrations.vllm.capabilities import (
            PROMPT_ONLY_PROTOCOL_VERSION,
        )
        from mooncake_epd.core.control.decode_token_envelope import (
            DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION,
        )
        from mooncake_epd.core.state.kv_transfer_manifest_v2 import (
            KV_TRANSFER_MANIFEST_SCHEMA_V2,
        )
    except Exception as exc:
        logger.error("Mooncake vLLM capability route install failed: %s", exc)
        if _strict_no_fallback():
            raise
        return

    model_contract: dict[str, str] | None = None

    def _file_sha256(paths: list[Any]) -> str:
        digest = hashlib.sha256()
        included = 0
        for path in paths:
            try:
                if not path.is_file():
                    continue
                payload = path.read_bytes()
            except OSError:
                continue
            digest.update(path.name.encode("utf-8"))
            digest.update(b"\0")
            digest.update(payload)
            digest.update(b"\0")
            included += 1
        return digest.hexdigest() if included else ""

    def _model_contract() -> dict[str, str]:
        nonlocal model_contract
        if model_contract is not None:
            return dict(model_contract)
        from pathlib import Path

        model_id = str(os.getenv("MOONCAKE_EPD_MODEL_ID", "") or "")
        model_path = Path(model_id).expanduser()
        config: dict[str, Any] = {}
        tokenizer_config: dict[str, Any] = {}
        try:
            config = json.loads((model_path / "config.json").read_text())
        except (OSError, TypeError, ValueError):
            config = {}
        try:
            tokenizer_config = json.loads(
                (model_path / "tokenizer_config.json").read_text()
            )
        except (OSError, TypeError, ValueError):
            tokenizer_config = {}
        architecture = ""
        architectures = config.get("architectures")
        if isinstance(architectures, list) and architectures:
            architecture = str(architectures[0] or "")
        model_family = (
            str(config.get("model_type") or "")
            or architecture
            or model_path.name
        )
        chat_template = tokenizer_config.get("chat_template")
        if isinstance(chat_template, dict):
            chat_template = json.dumps(
                chat_template,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
        if not isinstance(chat_template, str):
            chat_template = ""
        if not chat_template:
            try:
                raw_template = json.loads(
                    (model_path / "chat_template.json").read_text()
                )
                if isinstance(raw_template, dict):
                    raw_template = raw_template.get("chat_template")
                if isinstance(raw_template, str):
                    chat_template = raw_template
            except (OSError, TypeError, ValueError):
                pass
        tokenizer_paths = [
            model_path / filename
            for filename in (
                "tokenizer.json",
                "tokenizer_config.json",
                "special_tokens_map.json",
                "vocab.json",
                "merges.txt",
                "tokenizer.model",
                "sentencepiece.bpe.model",
            )
        ]
        model_contract = {
            "model_family": model_family,
            "model_config_sha256": _file_sha256(
                [model_path / "config.json"]
            ),
            "tokenizer_id_sha256": _file_sha256(tokenizer_paths),
            "chat_template_sha256": (
                hashlib.sha256(chat_template.encode("utf-8")).hexdigest()
                if chat_template
                else ""
            ),
        }
        return dict(model_contract)

    async def _capabilities() -> dict[str, Any]:
        report = dict(adapter_installation_report() or {})
        model_id = str(os.getenv("MOONCAKE_EPD_MODEL_ID", "") or "")
        contract = _model_contract()
        installed_protocol = int(
            report.get("prompt_only_protocol_version", 0) or 0
        )
        report.update(
            {
                "role": str(os.getenv("MOONCAKE_EPD_VLLM_ROLE", "") or ""),
                "kv_role": str(os.getenv("MOONCAKE_EPD_KV_ROLE", "") or ""),
                "worker_id": str(os.getenv("MOONCAKE_EPD_WORKER_ID", "") or ""),
                "engine_id": str(os.getenv("MOONCAKE_EPD_ENGINE_ID", "") or ""),
                "model_id_sha256": (
                    hashlib.sha256(model_id.encode("utf-8")).hexdigest()
                    if model_id
                    else ""
                ),
                **contract,
                "kv_manifest_schema_version": (
                    KV_TRANSFER_MANIFEST_SCHEMA_V2
                ),
                "prompt_only_protocol_expected": PROMPT_ONLY_PROTOCOL_VERSION,
                "prompt_only_verified": bool(
                    report.get("installed", False)
                    and report.get("prompt_only_ready", False)
                    and report.get("prompt_only_patch_installed", False)
                    and installed_protocol == PROMPT_ONLY_PROTOCOL_VERSION
                ),
                # vLLM's Completion API accepts ``prompt`` as token IDs.  EPD
                # uses that native path to avoid sending the original
                # multimodal Chat body to Decode after the envelope has been
                # validated by the proxy.
                "decode_token_envelope_protocol_version": (
                    DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION
                ),
                "decode_token_prompt_ready": bool(
                    report.get("installed", False)
                    and report.get(
                        "decode_token_multimodal_patch_installed", False
                    )
                    and str(
                        os.getenv("MOONCAKE_EPD_VLLM_ROLE", "") or ""
                    ).strip().lower()
                    == "decode"
                    and str(
                        os.getenv("MOONCAKE_EPD_KV_ROLE", "") or ""
                    ).strip().lower()
                    == "kv_consumer"
                    and getattr(app.state, "openai_serving_completion", None)
                    is not None
                ),
                "decode_token_endpoint": "/v1/completions",
            }
        )
        return report

    app.get("/mooncake_epd/capabilities")(_capabilities)
    setattr(app, "_mooncake_epd_capability_route", True)
    logger.info("enabled Mooncake EPD vLLM capability route")


def _install_direct_feature_buffer_routes(app: Any) -> None:
    """Install Prefill-process-owned direct FeatureBundle allocation routes.

    This must run inside the same vLLM Prefill API process that later executes
    the FeatureHandleProvider hook.  It is disabled unless
    ``MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER=1`` to avoid exposing extra
    endpoints on unrelated vLLM servers.
    """

    if getattr(app, "_mooncake_epd_direct_feature_buffer_routes", False):
        return
    if not _env_bool("MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER", False):
        return
    try:
        import os
        import hmac
        from fastapi import Depends, Header, HTTPException
        from mooncake_epd.core.state import (
            DirectFeatureBufferRegistry,
            FeatureBundleDescriptor,
            register_direct_feature_buffer_registry,
        )
        from mooncake_epd.core.transfer import TransferEngine
    except Exception as exc:
        logger.error("Mooncake direct feature buffer route install failed: %s", exc)
        if _strict_no_fallback():
            raise
        return

    worker_id = os.getenv(
        "MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID",
        os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_WORKER_ID", "prefill"),
    )
    device = os.getenv(
        "MOONCAKE_EPD_DIRECT_BUFFER_DEVICE",
        os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_DEVICE", "cuda"),
    )
    auth_token = str(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN", "")).strip()
    if not auth_token:
        raise RuntimeError(
            "MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN is required when direct "
            "FeatureBuffer routes are enabled"
        )

    def _authorize_direct_buffer(
        presented: str = Header(default="", alias="X-Mooncake-EPD-Token"),
    ) -> None:
        if not hmac.compare_digest(auth_token, str(presented or "")):
            raise HTTPException(status_code=403, detail="invalid direct FeatureBuffer token")

    route_dependencies = [Depends(_authorize_direct_buffer)]
    engine = TransferEngine(
        protocol=os.getenv("MOONCAKE_EPD_DIRECT_ENGINE_PROTOCOL", os.getenv("MOONCAKE_PROTOCOL", "tcp")),
        local_hostname=os.getenv(
            "MOONCAKE_EPD_DIRECT_LOCAL_HOSTNAME",
            os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost"),
        ),
        metadata_server=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
        device_name=os.getenv("MOONCAKE_DEVICE_NAME", ""),
    )
    registry = DirectFeatureBufferRegistry(
        worker_id=worker_id,
        device=device,
        transfer_engine=engine,
        remote_session=os.getenv("MOONCAKE_EPD_DIRECT_REMOTE_SESSION"),
        register_memory=_env_bool("MOONCAKE_EPD_DIRECT_REGISTER_MEMORY", True),
        target_memory_mode=os.getenv("MOONCAKE_EPD_DIRECT_TARGET_MODE", "auto"),
        persistent_cache=_env_bool("MOONCAKE_EPD_DIRECT_BUFFER_PERSISTENT_CACHE", False),
        max_cache_entries=int(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_ENTRIES", "64")),
        max_cache_bytes=int(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_BYTES", str(2 * 1024 * 1024 * 1024))),
    )
    try:
        # Warm up Mooncake direct engine during vLLM app construction rather
        # than during the first Prefill /allocate call.  In real serving this
        # removes topology discovery/RPC setup from the user-visible TTFT path.
        engine.initialize()
        try:
            import torch

            warmup_tensor = torch.empty((1,), dtype=torch.uint8, device=device)
            if registry.register_memory:
                warmup_handle = engine.register_tensor_memory(warmup_tensor)
                engine.unregister_tensor_memory(warmup_handle)
            del warmup_tensor
        except Exception:
            if _strict_no_fallback():
                raise
            logger.exception("Mooncake direct FeatureBuffer tensor/register warmup failed")
    except Exception:
        if _strict_no_fallback():
            raise
        logger.exception("Mooncake direct FeatureBuffer engine warmup failed")
    register_direct_feature_buffer_registry(registry)
    setattr(app, "_mooncake_epd_direct_feature_buffer_registry", registry)

    def _allocate_targets(
        raw_descriptors: list[object],
        *,
        zero_fill: bool,
        reuse_ready: bool,
    ) -> list[dict[str, Any]]:
        targets: list[dict[str, Any]] = []
        for raw in raw_descriptors:
            descriptor = FeatureBundleDescriptor.from_dict(dict(raw or {}))
            allocation = registry.allocate_for_descriptor(
                descriptor,
                zero_fill=zero_fill,
                reuse_ready=reuse_ready,
            )
            should_publish = False
            with registry._lock:  # noqa: SLF001 - atomic publish reservation
                if not allocation.ready and not allocation.publish_started:
                    allocation.publish_started = True
                    should_publish = True
            target = allocation.as_direct_target()
            target["cache_hit"] = bool(allocation.ready)
            target["publish_required"] = bool(should_publish)
            target["publish_pending"] = bool(not allocation.ready and not should_publish)
            target["ref_count"] = int(allocation.ref_count)
            targets.append(target)
        return targets

    def _wait_for_ready(feature_ids: list[str], timeout_s: float) -> None:
        for feature_id in feature_ids:
            registry.wait_ready(feature_id, timeout_s=timeout_s)

    def _release_features(feature_ids: list[str]) -> None:
        for feature_id in feature_ids:
            registry.release(feature_id)

    def _lookup_ready_targets(
        feature_ids: list[object],
        *,
        lease_count: int,
    ) -> tuple[list[dict[str, Any]], list[str], float]:
        """Resolve ready direct targets without blocking the ASGI event loop.

        ``lookup_ready`` only holds the registry's narrow thread lock, but
        exporting a direct target also reads CUDA-backed tensor pointers and
        serializes the descriptor.  At concurrency that short synchronous
        section previously ran on the Prefill vLLM API loop, where it could
        delay unrelated ``/inference/v1/generate`` and release requests.  The
        registry is explicitly thread-safe, so execute the whole lookup/export
        unit in the default worker pool and report its service time separately
        from proxy-to-server RTT.
        """

        started = time.perf_counter()
        hits: list[dict[str, Any]] = []
        misses: list[str] = []
        for feature_id in feature_ids:
            allocation = registry.lookup_ready(str(feature_id), lease_count=lease_count)
            if allocation is None:
                misses.append(str(feature_id))
                continue
            target = allocation.as_direct_target()
            target["cache_hit"] = True
            target["publish_required"] = False
            target["ref_count"] = int(allocation.ref_count)
            target["lease_count"] = lease_count
            hits.append(
                {
                    "feature_id": str(feature_id),
                    "descriptor": allocation.descriptor.to_dict(),
                    "target": target,
                }
            )
        return hits, misses, max(0.0, (time.perf_counter() - started) * 1000.0)

    async def _allocate(payload: dict[str, Any]) -> dict[str, Any]:
        raw_descriptors = payload.get("descriptors")
        if raw_descriptors is None and isinstance(payload.get("descriptor"), dict):
            raw_descriptors = [payload.get("descriptor")]
        if not isinstance(raw_descriptors, list) or not raw_descriptors:
            raise HTTPException(status_code=400, detail="allocate requires descriptors[]")
        try:
            # Native registration remains serialized by the registry's narrow
            # registration lock. Do not serialize independent tensor allocation
            # and HTTP requests here: doing so turns concurrent E->P publishes
            # into a proxy-side TTFT queue.
            targets = await asyncio.to_thread(
                _allocate_targets,
                list(raw_descriptors),
                zero_fill=bool(payload.get("zero_fill", False)),
                reuse_ready=bool(payload.get("reuse_ready", True)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"direct buffer allocation failed: {exc}") from exc
        return {"targets": targets, "count": len(targets), "worker_id": registry.worker_id}

    async def _lookup(payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="lookup requires feature_ids[]")
        try:
            lease_count = int(payload.get("lease_count", 1) or 1)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="lookup lease_count must be an integer") from exc
        # A lease batch is a bounded Proxy-side reservation.  It is only used
        # with generation fencing and is released on expiry/eviction/shutdown.
        if lease_count <= 0 or lease_count > 1024:
            raise HTTPException(status_code=400, detail="lookup lease_count must be in [1, 1024]")
        try:
            hits, misses, server_lookup_ms = await asyncio.to_thread(
                _lookup_ready_targets,
                list(raw),
                lease_count=lease_count,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"direct buffer lookup failed: {exc}") from exc
        return {
            "hits": hits,
            "misses": misses,
            "count": len(hits),
            "all_hit": len(hits) == len(raw),
            "worker_id": registry.worker_id,
            "lease_count": lease_count,
            "server_lookup_ms": server_lookup_ms,
        }

    async def _mark_ready(payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="mark_ready requires feature_ids[]")
        for feature_id in raw:
            registry.mark_ready(str(feature_id))
        return {"ready": len(raw), "stats": dict(registry.stats())}

    async def _wait_ready(payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="wait_ready requires feature_ids[]")
        timeout_s = float(payload.get("timeout_s", 30.0) or 30.0)
        try:
            await asyncio.to_thread(
                _wait_for_ready,
                [str(feature_id) for feature_id in raw],
                timeout_s,
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ready": len(raw), "stats": dict(registry.stats())}

    async def _release(payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            raise HTTPException(status_code=400, detail="release requires feature_ids[]")
        try:
            await asyncio.to_thread(_release_features, [str(feature_id) for feature_id in raw])
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"direct buffer release failed: {exc}") from exc
        return {"released": len(raw), "stats": dict(registry.stats())}

    async def _stats() -> dict[str, Any]:
        return dict(registry.stats())

    prefix = "/mooncake_epd/direct_feature_buffer"
    app.post(f"{prefix}/allocate", dependencies=route_dependencies)(_allocate)
    app.post(f"{prefix}/lookup", dependencies=route_dependencies)(_lookup)
    app.post(f"{prefix}/mark_ready", dependencies=route_dependencies)(_mark_ready)
    app.post(f"{prefix}/wait_ready", dependencies=route_dependencies)(_wait_ready)
    app.post(f"{prefix}/release", dependencies=route_dependencies)(_release)
    app.get(f"{prefix}/stats", dependencies=route_dependencies)(_stats)
    if _env_bool("MOONCAKE_EPD_DIRECT_BUFFER_ROOT_ROUTES", False):
        app.post("/allocate", dependencies=route_dependencies)(_allocate)
        app.post("/lookup", dependencies=route_dependencies)(_lookup)
        app.post("/mark_ready", dependencies=route_dependencies)(_mark_ready)
        app.post("/wait_ready", dependencies=route_dependencies)(_wait_ready)
        app.post("/release", dependencies=route_dependencies)(_release)
        app.get("/direct_feature_buffer_stats", dependencies=route_dependencies)(_stats)
    setattr(app, "_mooncake_epd_direct_feature_buffer_routes", True)
    logger.info(
        "enabled Mooncake EPD direct FeatureBuffer routes in vLLM process: worker=%s device=%s",
        worker_id,
        device,
    )


def _patch_vllm_openai_app_direct_feature_buffer_routes() -> None:
    try:
        from vllm.entrypoints.openai import api_server
    except Exception:
        return
    original = getattr(api_server, "build_app", None)
    if original is None or getattr(original, "_mooncake_epd_direct_feature_buffer_patch", False):
        return

    def _patched_build_app(*args: Any, **kwargs: Any) -> Any:
        app = original(*args, **kwargs)
        try:
            _install_vllm_capability_route(app)
            _install_direct_feature_buffer_routes(app)
        except Exception:
            if _strict_no_fallback():
                raise
            logger.exception("Mooncake direct FeatureBuffer route install skipped")
        return app

    _patched_build_app._mooncake_epd_direct_feature_buffer_patch = True  # type: ignore[attr-defined]
    api_server.build_app = _patched_build_app  # type: ignore[assignment]
    logger.info("enabled Mooncake EPD vLLM build_app direct FeatureBuffer hook")


def _install_legacy_vllm_adapter() -> None:
    """Bridge old env-based launchers to the explicit adapter exactly once.

    Keeping this thin avoids a global ``sitecustomize`` side effect in normal
    Python processes while allowing already generated EPD launch scripts to
    remain usable during the migration.
    """

    if not _vllm_patches_enabled():
        return
    if str(os.getenv("MOONCAKE_EPD_ADAPTER_COMPAT_BOOTSTRAP", "1")).strip() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return
    try:
        from mooncake_epd.integrations.vllm.adapter import install_vllm_epd_adapter

        install_vllm_epd_adapter()
    except Exception:
        if _strict_no_fallback():
            raise
        logger.exception(
            "Mooncake EPD legacy sitecustomize compatibility adapter failed; "
            "use mooncake_epd.integrations.vllm.launcher for a fail-closed startup"
        )


_install_legacy_vllm_adapter()
