"""Workspace-scoped runtime patches for Mooncake EPD real vLLM serving.

This module is loaded automatically by Python when the workspace root is on
``PYTHONPATH``. The generated vLLM serving scripts already export that path,
so the patch stays local to this project instead of mutating site-packages.

Patch scope:
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
            status_before_stop = request.status
            request.status = RequestStatus.FINISHED_LENGTH_CAPPED
            finish_reason = request.get_finished_reason()
            finished = self._handle_stopped_request(request)
            kv_transfer_params = self._free_request(request) if finished else None

            if status_before_stop == RequestStatus.RUNNING:
                self.running = remove_all(self.running, {request})
            else:
                self.waiting.remove_requests({request})

            eco = engine_core_outputs.setdefault(
                request.client_index,
                EngineCoreOutputs(),
            )
            eco.outputs.append(
                EngineCoreOutput(
                    request_id=request.request_id,
                    new_token_ids=[],
                    finish_reason=finish_reason,
                    events=request.take_events(),
                    prefill_stats=request.take_prefill_stats(),
                    kv_transfer_params=kv_transfer_params,
                    trace_headers=request.trace_headers,
                    num_nans_in_logits=request.num_nans_in_logits,
                )
            )

        return engine_core_outputs

    Scheduler.update_from_output = _patched_update_from_output  # type: ignore[method-assign]
    SamplingParams._mooncake_epd_prompt_only_patch = True  # type: ignore[attr-defined]
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


if _vllm_patches_enabled():
    _patch_vllm_prompt_only_prefill()
    _patch_vllm_feature_handle_injection()
    _patch_vllm_gpu_model_runner_kv_params()
    _patch_vllm_gpu_model_runner_feature_handles()

# ---------------------------------------------------------------------------
# Prefill-owned E→P direct FeatureBundle allocation routes
# ---------------------------------------------------------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    import os

    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


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
        hits = []
        misses = []
        for feature_id in raw:
            allocation = registry.lookup_ready(str(feature_id))
            if allocation is None:
                misses.append(str(feature_id))
                continue
            target = allocation.as_direct_target()
            target["cache_hit"] = True
            target["publish_required"] = False
            target["ref_count"] = int(allocation.ref_count)
            hits.append(
                {
                    "feature_id": str(feature_id),
                    "descriptor": allocation.descriptor.to_dict(),
                    "target": target,
                }
            )
        return {
            "hits": hits,
            "misses": misses,
            "count": len(hits),
            "all_hit": len(hits) == len(raw),
            "worker_id": registry.worker_id,
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
            _install_direct_feature_buffer_routes(app)
        except Exception:
            if _strict_no_fallback():
                raise
            logger.exception("Mooncake direct FeatureBuffer route install skipped")
        return app

    _patched_build_app._mooncake_epd_direct_feature_buffer_patch = True  # type: ignore[attr-defined]
    api_server.build_app = _patched_build_app  # type: ignore[assignment]
    logger.info("enabled Mooncake EPD vLLM build_app direct FeatureBuffer hook")


if _vllm_patches_enabled():
    _patch_vllm_openai_app_direct_feature_buffer_routes()
