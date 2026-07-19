"""vLLM multimodal hidden-state cache for Mooncake EPD-Serve.

This module is intentionally importable from vLLM worker processes via the
repo-level PYTHONPATH injected by ``demo/vllm_integration.py``.  It provides a
small production-oriented cache at the real Qwen3-VL vision-encoder boundary.

Two keying modes are supported:

* stable vLLM feature identifiers (``MultiModalFeatureSpec.mm_hash`` or
  ``identifier``) propagated by patched vLLM worker runners; this is the hot
  path and enables hidden-state reuse across repeated Agent steps/requests even
  when processor tensors are re-materialized differently.
* conservative tensor-byte hashing fallback for unpatched runners or unusual
  call sites.

On a hit vLLM reuses cached image hidden states and skips ``self.visual(...)``
for that image item while preserving Qwen3-VL's downstream split/postprocess
semantics.  Cache failures are contained and must never affect serving
correctness.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import os
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Sequence, Tuple

import torch

from .hidden_cache_key import HiddenCacheKeyError, HiddenStateCacheKeyV2
from .hidden_cache_policy import HiddenCachePolicy
from .mooncake_hidden_state_store import MooncakeHiddenStateStore

_CURRENT_MM_HIDDEN_CACHE_KEYS: contextvars.ContextVar[Optional[Tuple[str, ...]]] = (
    contextvars.ContextVar("mooncake_epd_vllm_mm_hidden_cache_keys", default=None)
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(str(os.getenv(name, default)).strip())
    except Exception:
        value = int(default)
    return max(int(minimum), value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(str(os.getenv(name, default)).strip())
    except Exception:
        value = float(default)
    return max(float(minimum), value)


def _sanitize_component(text: Any) -> str:
    raw = "unknown" if text is None else str(text).strip()
    raw = raw or "unknown"
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)


def _normalize_stable_keys(keys: Optional[Iterable[Any]]) -> Optional[Tuple[str, ...]]:
    if keys is None:
        return None
    out: list[str] = []
    for key in keys:
        if key is None:
            out.append("")
        else:
            out.append(str(key))
    return tuple(out)



_TRACE_LOCK = threading.RLock()
_TRACE_PATH: Optional[Path] = None


def _trace_enabled() -> bool:
    return _env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_TRACE", False)


def _safe_trace_value(value: Any) -> Any:
    try:
        if isinstance(value, torch.Tensor):
            return {
                "tensor_shape": [int(x) for x in value.shape],
                "tensor_dtype": str(value.dtype),
                "tensor_device": str(value.device),
            }
        if isinstance(value, (str, int, float, bool)) or value is None:
            if isinstance(value, str) and len(value) > 160:
                return value[:80] + "..." + value[-32:]
            return value
        if isinstance(value, dict):
            return {str(k): _safe_trace_value(v) for k, v in list(value.items())[:32]}
        if isinstance(value, (list, tuple, set)):
            return [_safe_trace_value(v) for v in list(value)[:64]]
        return str(value)[:160]
    except Exception:
        return "<unserializable>"


def _trace_path() -> Optional[Path]:
    global _TRACE_PATH
    if not _trace_enabled():
        return None
    if _TRACE_PATH is not None:
        return _TRACE_PATH
    metrics_dir = os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "").strip()
    if not metrics_dir:
        return None
    path = Path(metrics_dir).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    engine_id = _sanitize_component(os.getenv("MOONCAKE_EPD_ENGINE_ID", "vllm"))
    role = _sanitize_component(os.getenv("MOONCAKE_EPD_KV_ROLE", "unknown"))
    _TRACE_PATH = path / f"mm-hidden-trace-{engine_id}-{role}-pid{os.getpid()}.jsonl"
    return _TRACE_PATH


def trace_vllm_mm_hidden_event(event: str, **payload: Any) -> None:
    """Append a bounded JSONL event for vLLM multimodal hidden-cache diagnosis.

    This is intentionally best-effort and disabled by default.  It must never
    affect serving correctness or latency-sensitive code when disabled.
    """

    path = _trace_path()
    if path is None:
        return
    record = {
        "ts": time.time(),
        "pid": os.getpid(),
        "engine_id": os.getenv("MOONCAKE_EPD_ENGINE_ID", ""),
        "kv_role": os.getenv("MOONCAKE_EPD_KV_ROLE", ""),
        "event": str(event),
        **{str(k): _safe_trace_value(v) for k, v in payload.items()},
    }
    try:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with _TRACE_LOCK:
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception:
        pass

@contextlib.contextmanager
def use_mm_hidden_cache_keys(keys: Optional[Iterable[Any]]) -> Iterator[None]:
    """Temporarily expose vLLM multimodal feature keys to model code.

    Patched vLLM worker runners wrap each ``model.embed_multimodal`` call with
    this context.  The model-specific insertion point can then consume the keys
    without changing upstream vLLM public method signatures or multimodal kwargs
    validation.
    """

    normalized = _normalize_stable_keys(keys)
    trace_vllm_mm_hidden_event("context_keys_enter", keys=normalized)
    token = _CURRENT_MM_HIDDEN_CACHE_KEYS.set(normalized)
    try:
        yield
    finally:
        _CURRENT_MM_HIDDEN_CACHE_KEYS.reset(token)


def get_current_mm_hidden_cache_keys() -> Optional[Tuple[str, ...]]:
    return _CURRENT_MM_HIDDEN_CACHE_KEYS.get()


@dataclass
class MMHiddenCacheEntry:
    tensor: torch.Tensor
    shape: Tuple[int, ...]
    dtype: str
    device_type: str
    num_bytes: int
    created_at: float
    last_access_at: float
    hits: int = 0


class VLLMMMHiddenStateCache:
    """Thread-safe LRU cache for vLLM image hidden states.

    Values are stored detached.  CPU storage is the safe default because it does
    not permanently consume GPU memory; enabling GPU storage is supported for
    low-latency single-worker deployments.  The cache has no correctness
    authority: on any error or unsafe shape mismatch, callers recompute through
    the original vision encoder path.
    """

    def __init__(
        self,
        *,
        l2_store: Optional[MooncakeHiddenStateStore] = None,
        l2_policy: Optional[HiddenCachePolicy] = None,
    ) -> None:
        self.l2_policy = l2_policy or HiddenCachePolicy.from_env()
        self.l2_store = l2_store
        self.enabled = bool(
            self.l2_policy.enable_l1
            and _env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", True)
        )
        self.max_entries = _env_int("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES", 64, minimum=1)
        self.max_bytes = _env_int(
            "MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES",
            2 * 1024 * 1024 * 1024,
            minimum=1,
        )
        self.store_on_gpu = _env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_GPU", False)
        self.hash_sample_bytes = _env_int(
            "MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_HASH_SAMPLE_BYTES",
            0,
            minimum=0,
        )
        self.debug = _env_bool("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_DEBUG", False)
        self.allow_tensor_fallback = bool(self.l2_policy.allow_tensor_hash_fallback)
        self.metrics_flush_interval_s = _env_float(
            "MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S",
            0.25,
            minimum=0.0,
        )
        self._lock = threading.RLock()
        self._entries: "OrderedDict[str, MMHiddenCacheEntry]" = OrderedDict()
        self._bytes = 0
        self._last_flush = 0.0
        # Worker inference can finish before the normal metrics interval
        # elapses.  Keep one daemon timer so a model-side image_embeds hit is
        # persisted shortly after the hot-path update without synchronously
        # writing a JSON file on every request.
        self._flush_timer_lock = threading.Lock()
        self._flush_timer: Optional[threading.Timer] = None
        self._recent_keys: "deque[dict[str, Any]]" = deque(maxlen=16)
        self._metrics: Dict[str, Any] = {
            "enabled": bool(self.enabled),
            "lookups": 0,
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "evictions": 0,
            "bytes": 0,
            "entries": 0,
            "stable_key_lookups": 0,
            "tensor_key_lookups": 0,
            "skipped_unkeyed_calls": 0,
            "native_encoder_cache_hits": 0,
            "precomputed_image_embeds_hits": 0,
            "partial_hit_batches": 0,
            "full_hit_batches": 0,
            "full_miss_batches": 0,
            "vision_compute_ms_total": 0.0,
            "cache_load_ms_total": 0.0,
            "hash_ms_total": 0.0,
            "l2_lookups": 0,
            "l2_hits": 0,
            "l2_misses": 0,
            "l2_stores": 0,
            "l2_errors": 0,
            "errors": 0,
            "last_error": "",
        }
        self._metrics_path = self._build_metrics_path()
        self._flush_metrics(force=True)

    @property
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            out = dict(self._metrics)
            out["bytes"] = int(self._bytes)
            out["entries"] = int(len(self._entries))
            out["max_entries"] = int(self.max_entries)
            out["max_bytes"] = int(self.max_bytes)
            out["store_on_gpu"] = bool(self.store_on_gpu)
            out["allow_tensor_fallback"] = bool(self.allow_tensor_fallback)
            out["l2_enabled"] = bool(
                self.l2_store is not None and self.l2_policy.has_l2_identity
            )
            out["l2_partial_enabled"] = bool(self.l2_policy.partial_enabled)
            out["hit_rate"] = (
                float(out["hits"]) / float(out["lookups"])
                if int(out.get("lookups", 0) or 0) > 0
                else 0.0
            )
            if self.debug:
                out["recent_keys"] = list(self._recent_keys)
            return out

    def configure_l2_store(
        self,
        store: Optional[MooncakeHiddenStateStore],
        *,
        policy: Optional[HiddenCachePolicy] = None,
    ) -> None:
        """Install an explicitly configured exact L2 Store adapter.

        The adapter is injected by process bootstrap; this cache never creates
        a Mooncake client from environment at import time.  Missing identity
        fields leave L2 disabled/fail-closed while preserving exact L1.
        """

        with self._lock:
            self.l2_store = store
            if policy is not None:
                self.l2_policy = policy

    def get_or_compute(
        self,
        *,
        pixel_values: torch.Tensor,
        grid_thw: torch.Tensor,
        compute_fn: Callable[[], torch.Tensor],
        namespace: str,
    ) -> torch.Tensor:
        """Fallback whole-tensor cache keyed by exact processed tensor bytes."""

        if not self.enabled:
            return compute_fn()

        hash_started = time.perf_counter()
        try:
            key = self._tensor_key(pixel_values=pixel_values, grid_thw=grid_thw, namespace=namespace)
        except Exception as exc:
            self._record_error(exc)
            return compute_fn()
        hash_ms = (time.perf_counter() - hash_started) * 1000.0

        trace_vllm_mm_hidden_event(
            "tensor_cache_lookup",
            key_prefix=key[:16],
            pixel_shape=list(pixel_values.shape),
            grid_shape=list(grid_thw.shape),
        )
        with self._lock:
            self._metrics["lookups"] += 1
            self._metrics["tensor_key_lookups"] += 1
            self._metrics["hash_ms_total"] += float(hash_ms)
        cached = self._lookup(key, target_device=pixel_values.device, target_dtype=None)
        if cached is not None:
            with self._lock:
                self._metrics["full_hit_batches"] += 1
            self._flush_metrics()
            return cached

        compute_started = time.perf_counter()
        result = compute_fn()
        compute_ms = (time.perf_counter() - compute_started) * 1000.0
        try:
            self._store(key, result)
            trace_vllm_mm_hidden_event(
                "tensor_cache_compute",
                key_prefix=key[:16],
                compute_ms=compute_ms,
                result_shape=list(result.shape) if hasattr(result, "shape") else None,
            )
            with self._lock:
                self._metrics["vision_compute_ms_total"] += float(compute_ms)
                self._metrics["full_miss_batches"] += 1
        except Exception as exc:
            self._record_error(exc)
        self._flush_metrics()
        return result

    def get_or_compute_qwen3vl_items(
        self,
        *,
        pixel_values: torch.Tensor,
        grid_thw: torch.Tensor,
        visual: Any,
        compute_fn: Callable[[], torch.Tensor],
        namespace: str,
        stable_keys: Optional[Sequence[str]],
    ) -> torch.Tensor:
        """Qwen3-VL per-image hidden-state cache using stable vLLM keys.

        ``grid_thw`` has one row per image item.  Qwen3-VL's vision tower accepts
        all image patches concatenated and returns all hidden states
        concatenated.  We split inputs/outputs by item so a repeated image can
        be served from cache even when neighboring batch items differ.
        """

        if not self.enabled:
            return compute_fn()
        keys = tuple(stable_keys or ())
        num_items = int(grid_thw.shape[0]) if grid_thw.ndim >= 2 else 0
        trace_vllm_mm_hidden_event(
            "qwen3vl_cache_entry",
            stable_keys=keys,
            num_items=num_items,
            pixel_shape=list(pixel_values.shape),
            grid_shape=list(grid_thw.shape),
        )
        if num_items <= 0 or len(keys) != num_items or any(not k for k in keys):
            if self.allow_tensor_fallback:
                return self.get_or_compute(
                    pixel_values=pixel_values,
                    grid_thw=grid_thw,
                    compute_fn=compute_fn,
                    namespace=namespace,
                )
            with self._lock:
                self._metrics["skipped_unkeyed_calls"] += 1
            trace_vllm_mm_hidden_event(
                "qwen3vl_cache_skip_unkeyed",
                stable_keys=keys,
                num_items=num_items,
                reason="missing_or_mismatched_stable_keys",
            )
            self._flush_metrics()
            return compute_fn()

        try:
            merge_size = int(getattr(visual, "spatial_merge_size", 1) or 1)
            input_sizes = [int(x) for x in grid_thw.prod(-1).detach().cpu().tolist()]
            output_sizes = [max(0, int(x) // merge_size // merge_size) for x in input_sizes]
            if sum(input_sizes) != int(pixel_values.shape[0]) or any(s <= 0 for s in output_sizes):
                # Unexpected layout; preserve correctness.  Avoid tensor hashing
                # unless explicitly enabled because vLLM profiling can pass very
                # large synthetic images before real request keys exist.
                if self.allow_tensor_fallback:
                    return self.get_or_compute(
                        pixel_values=pixel_values,
                        grid_thw=grid_thw,
                        compute_fn=compute_fn,
                        namespace=namespace,
                    )
                with self._lock:
                    self._metrics["skipped_unkeyed_calls"] += 1
                trace_vllm_mm_hidden_event(
                    "qwen3vl_cache_skip_layout",
                    stable_keys=keys,
                    num_items=num_items,
                    input_sizes=input_sizes,
                    pixel_shape=list(pixel_values.shape),
                    output_sizes=output_sizes,
                )
                self._flush_metrics()
                return compute_fn()
        except Exception as exc:
            self._record_error(exc)
            return compute_fn()

        cache_keys = [
            self._stable_item_key(
                stable_key=keys[i],
                grid_row=grid_thw[i],
                namespace=namespace,
                output_tokens=output_sizes[i],
            )
            for i in range(num_items)
        ]
        l2_keys = [
            self._l2_key_for_qwen3vl_item(
                stable_key=keys[i],
                grid_row=grid_thw[i],
                visual=visual,
                namespace=namespace,
                output_tokens=output_sizes[i],
            )
            for i in range(num_items)
        ]

        outputs: list[Optional[torch.Tensor]] = [None] * num_items
        missing: list[int] = []
        with self._lock:
            self._metrics["lookups"] += num_items
            self._metrics["stable_key_lookups"] += num_items
        for i, key in enumerate(cache_keys):
            self._remember_key_debug(mode="stable", key=key, source=keys[i], grid=grid_thw[i])
            cached = self._lookup(key, target_device=pixel_values.device, target_dtype=None)
            if cached is None:
                cached = self._lookup_l2(
                    l2_keys[i],
                    target_device=pixel_values.device,
                )
                if cached is not None:
                    # L2 output is exact and immutable; retain an independent
                    # L1 copy for low-latency repeat hits in this worker.
                    self._store(key, cached)
            if cached is None:
                missing.append(i)
            else:
                outputs[i] = cached

        trace_vllm_mm_hidden_event(
            "qwen3vl_cache_lookup_result",
            stable_keys=keys,
            hits=[i for i in range(num_items) if i not in missing],
            missing=missing,
        )
        if missing:
            pixel_chunks = list(torch.split(pixel_values, input_sizes, dim=0))
            miss_pixels = torch.cat([pixel_chunks[i] for i in missing], dim=0)
            miss_grid = grid_thw[missing]
            compute_started = time.perf_counter()
            miss_embeds = visual(miss_pixels, grid_thw=miss_grid)
            compute_ms = (time.perf_counter() - compute_started) * 1000.0
            miss_output_sizes = [output_sizes[i] for i in missing]
            miss_chunks = list(torch.split(miss_embeds, miss_output_sizes, dim=0))
            if len(miss_chunks) != len(missing):
                raise RuntimeError(
                    "Qwen3-VL hidden cache split mismatch: "
                    f"missing={len(missing)} chunks={len(miss_chunks)}"
                )
            for item_idx, tensor in zip(missing, miss_chunks):
                outputs[item_idx] = tensor
                self._store(cache_keys[item_idx], tensor)
                self._store_l2(l2_keys[item_idx], tensor)
            trace_vllm_mm_hidden_event(
                "qwen3vl_cache_compute_missing",
                missing=missing,
                compute_ms=compute_ms,
                miss_pixel_shape=list(miss_pixels.shape),
                miss_grid_shape=list(miss_grid.shape),
            )
            with self._lock:
                self._metrics["vision_compute_ms_total"] += float(compute_ms)
                if len(missing) == num_items:
                    self._metrics["full_miss_batches"] += 1
                else:
                    self._metrics["partial_hit_batches"] += 1
        else:
            with self._lock:
                self._metrics["full_hit_batches"] += 1

        self._flush_metrics()
        # mypy cannot infer all entries have been filled.
        return torch.cat([t for t in outputs if t is not None], dim=0)



    def record_precomputed_image_embeds_hit(
        self,
        *,
        count: int,
        stable_keys: Optional[Sequence[str]] = None,
    ) -> None:
        """Record Qwen/vLLM ``image_embeds`` inputs as vision-encoder skips.

        Mooncake MM Store prefetch can inject precomputed image hidden states into
        vLLM.  Qwen3-VL then takes the native ``image_embeds`` branch and never
        calls the vision tower.  Count those as hidden-state hits so RFC metrics
        reflect the actual hot path, not only model-side LRU lookups.
        """

        if not self.enabled:
            return
        n = max(0, int(count))
        if n <= 0:
            return
        keys = tuple(stable_keys or ())
        trace_vllm_mm_hidden_event("precomputed_image_embeds_hit", count=n, stable_keys=keys)
        with self._lock:
            self._metrics["lookups"] += n
            self._metrics["hits"] += n
            self._metrics["precomputed_image_embeds_hits"] += n
            if len(keys) == n and all(keys):
                self._metrics["stable_key_lookups"] += n
        if self.debug:
            for i in range(n):
                source = keys[i] if i < len(keys) else "image_embeds"
                self._remember_key_debug(
                    mode="precomputed-image-embeds",
                    key=str(source),
                    source=str(source),
                    grid=torch.empty(0),
                )
        self._flush_metrics()

    def record_native_encoder_cache_hit(self, stable_key: str) -> None:
        """Record a vLLM native encoder-cache hit as hidden-state reuse.

        In vLLM V1 the scheduler can skip ``embed_multimodal`` entirely when
        an encoder output for the same feature identifier is still resident in
        the native encoder cache.  That is a real vision-encoder skip but it
        bypasses the Qwen model insertion point, so we account it here.
        """

        if not self.enabled or not stable_key:
            return
        trace_vllm_mm_hidden_event("native_encoder_cache_hit", stable_key=stable_key)
        with self._lock:
            self._metrics["lookups"] += 1
            self._metrics["hits"] += 1
            self._metrics["stable_key_lookups"] += 1
            self._metrics["native_encoder_cache_hits"] += 1
        self._remember_key_debug(
            mode="vllm-native-hit",
            key=str(stable_key),
            source=str(stable_key),
            grid=torch.empty(0),
        )
        self._flush_metrics()

    def _l2_key_for_qwen3vl_item(
        self,
        *,
        stable_key: str,
        grid_row: torch.Tensor,
        visual: Any,
        namespace: str,
        output_tokens: int,
    ) -> Optional[HiddenStateCacheKeyV2]:
        """Build an exact L2 key from propagated identity metadata only.

        ``grid_row`` is small layout metadata (normally three integers), not
        image/hidden tensor content.  A pathological large metadata tensor is
        rejected rather than copied from a CUDA device for cache keying.
        """

        if self.l2_store is None or not self.l2_policy.has_l2_identity:
            return None
        try:
            if int(grid_row.numel()) <= 0 or int(grid_row.numel()) > 16:
                return None
            grid = tuple(
                int(value)
                for value in grid_row.detach().to(device="cpu").reshape(-1).tolist()
            )
            dtype = str(getattr(visual, "dtype", "") or "")
            if not dtype or dtype == "unknown":
                return None
            return HiddenStateCacheKeyV2.from_vllm_stable_identity(
                stable_asset_hash=str(stable_key),
                modality="image",
                model_id=str(self.l2_policy.model_id),
                model_revision=str(self.l2_policy.model_revision),
                processor_revision=str(self.l2_policy.processor_revision),
                dtype=dtype,
                output_schema=str(self.l2_policy.output_schema),
                grid_thw=grid,
                relevant_kwargs={
                    "namespace": str(namespace),
                    "output_tokens": int(output_tokens),
                    "spatial_merge_size": int(
                        getattr(visual, "spatial_merge_size", 1) or 1
                    ),
                },
            )
        except (HiddenCacheKeyError, TypeError, ValueError, RuntimeError) as exc:
            # L2 identity being unavailable is a cache miss, never a request
            # failure.  Keep a bounded error metric for rollout diagnosis.
            with self._lock:
                self._metrics["l2_errors"] += 1
                self._metrics["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
            return None

    def _lookup_l2(
        self,
        key: Optional[HiddenStateCacheKeyV2],
        *,
        target_device: torch.device,
    ) -> Optional[torch.Tensor]:
        if key is None or self.l2_store is None:
            return None
        with self._lock:
            self._metrics["l2_lookups"] += 1
        try:
            tensor = self.l2_store.get(key, target_device=target_device)
        except Exception as exc:
            with self._lock:
                self._metrics["l2_errors"] += 1
                self._metrics["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
            return None
        with self._lock:
            if tensor is None:
                self._metrics["l2_misses"] += 1
            else:
                self._metrics["l2_hits"] += 1
        return tensor

    def _store_l2(
        self,
        key: Optional[HiddenStateCacheKeyV2],
        tensor: torch.Tensor,
    ) -> None:
        if key is None or self.l2_store is None:
            return
        try:
            self.l2_store.put(
                key,
                tensor,
                lease_seconds=float(self.l2_policy.lease_seconds),
            )
        except Exception as exc:
            with self._lock:
                self._metrics["l2_errors"] += 1
                self._metrics["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
            return
        with self._lock:
            self._metrics["l2_stores"] += 1

    def _tensor_key(self, *, pixel_values: torch.Tensor, grid_thw: torch.Tensor, namespace: str) -> str:
        h = hashlib.sha256()
        h.update(b"mooncake-vllm-mm-hidden-cache-tensor-v1\0")
        h.update(str(namespace).encode("utf-8", errors="replace"))
        h.update(b"\0pv-shape\0")
        h.update(json.dumps(list(pixel_values.shape), separators=(",", ":")).encode())
        h.update(b"\0pv-dtype\0")
        h.update(str(pixel_values.dtype).encode())
        h.update(b"\0grid-shape\0")
        h.update(json.dumps(list(grid_thw.shape), separators=(",", ":")).encode())
        h.update(b"\0grid-dtype\0")
        h.update(str(grid_thw.dtype).encode())
        h.update(b"\0grid\0")
        h.update(self._tensor_raw_bytes(grid_thw))
        h.update(b"\0pixel-values\0")
        raw = self._tensor_raw_bytes(pixel_values)
        if self.hash_sample_bytes and len(raw) > self.hash_sample_bytes * 2:
            n = int(self.hash_sample_bytes)
            h.update(len(raw).to_bytes(8, "little", signed=False))
            h.update(raw[:n])
            h.update(raw[-n:])
        else:
            h.update(raw)
        key = h.hexdigest()
        self._remember_key_debug(mode="tensor", key=key, source="tensor", grid=grid_thw)
        return key

    def _stable_item_key(
        self,
        *,
        stable_key: str,
        grid_row: torch.Tensor,
        namespace: str,
        output_tokens: int,
    ) -> str:
        h = hashlib.sha256()
        h.update(b"mooncake-vllm-mm-hidden-cache-stable-item-v1\0")
        h.update(str(namespace).encode("utf-8", errors="replace"))
        h.update(b"\0stable\0")
        h.update(str(stable_key).encode("utf-8", errors="replace"))
        h.update(b"\0grid\0")
        h.update(self._tensor_raw_bytes(grid_row))
        h.update(b"\0out\0")
        h.update(str(int(output_tokens)).encode())
        return h.hexdigest()

    @staticmethod
    def _tensor_raw_bytes(tensor: torch.Tensor) -> bytes:
        """Return dtype-preserving raw bytes for all torch dtypes, including BF16."""
        cpu = tensor.detach().to(device="cpu", copy=True).contiguous()
        return cpu.view(torch.uint8).numpy().tobytes()

    def _lookup(
        self,
        key: str,
        *,
        target_device: torch.device,
        target_dtype: Optional[torch.dtype],
    ) -> Optional[torch.Tensor]:
        load_started = time.perf_counter()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._metrics["misses"] += 1
                return None
            self._entries.move_to_end(key)
            entry.hits += 1
            entry.last_access_at = time.time()
            tensor = entry.tensor
            self._metrics["hits"] += 1
        out = (
            tensor.to(device=target_device, dtype=target_dtype, non_blocking=True)
            if target_dtype
            else tensor.to(device=target_device, non_blocking=True)
        )
        # CPU->CPU without a dtype change returns the cache's internal tensor.
        # Never expose that mutable storage to model code or callers.
        if out.data_ptr() == tensor.data_ptr():
            out = out.clone()
        with self._lock:
            self._metrics["cache_load_ms_total"] += (time.perf_counter() - load_started) * 1000.0
        return out

    def _store(self, key: str, tensor: torch.Tensor) -> None:
        detached = tensor.detach()
        storage = detached.clone() if self.store_on_gpu else detached.to(device="cpu", copy=True)
        storage = storage.contiguous()
        num_bytes = int(storage.numel() * storage.element_size())
        if num_bytes > self.max_bytes:
            return
        with self._lock:
            existing = self._entries.pop(key, None)
            if existing is not None:
                self._bytes -= int(existing.num_bytes)
            now = time.time()
            self._entries[key] = MMHiddenCacheEntry(
                tensor=storage,
                shape=tuple(int(x) for x in storage.shape),
                dtype=str(storage.dtype),
                device_type=str(storage.device.type),
                num_bytes=num_bytes,
                created_at=now,
                last_access_at=now,
            )
            self._bytes += num_bytes
            self._metrics["stores"] += 1
            self._evict_locked()
            self._metrics["bytes"] = int(self._bytes)
            self._metrics["entries"] = int(len(self._entries))

    def _evict_locked(self) -> None:
        while len(self._entries) > self.max_entries or self._bytes > self.max_bytes:
            _, entry = self._entries.popitem(last=False)
            self._bytes -= int(entry.num_bytes)
            self._metrics["evictions"] += 1

    def _record_error(self, exc: Exception) -> None:
        with self._lock:
            self._metrics["errors"] += 1
            self._metrics["last_error"] = f"{type(exc).__name__}: {exc}"[:500]
        self._flush_metrics(force=True)

    def _remember_key_debug(self, *, mode: str, key: str, source: str, grid: torch.Tensor) -> None:
        if not self.debug:
            return
        try:
            grid_shape = list(grid.shape)
            grid_sum = int(grid.detach().to(device="cpu").sum().item())
        except Exception:
            grid_shape = []
            grid_sum = -1
        with self._lock:
            self._recent_keys.append(
                {
                    "mode": mode,
                    "key_prefix": key[:16],
                    "source_prefix": str(source)[:32],
                    "grid_shape": grid_shape,
                    "grid_sum": grid_sum,
                    "ts": time.time(),
                }
            )

    def _build_metrics_path(self) -> Optional[Path]:
        metrics_dir = os.getenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", "").strip()
        if not metrics_dir:
            return None
        path = Path(metrics_dir).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        engine_id = _sanitize_component(os.getenv("MOONCAKE_EPD_ENGINE_ID", "vllm"))
        role = _sanitize_component(os.getenv("MOONCAKE_EPD_KV_ROLE", "unknown"))
        return path / f"mm-hidden-cache-{engine_id}-{role}-pid{os.getpid()}.mm_hidden.json"

    def _flush_metrics(self, *, force: bool = False) -> None:
        path = self._metrics_path
        if path is None:
            return
        now = time.time()
        if not force:
            remaining_s = self.metrics_flush_interval_s - (now - self._last_flush)
            if remaining_s > 0.0:
                # The runner waits for proxy metrics after the measured phase,
                # but a cache that only sees one precomputed image would
                # otherwise never write its post-hit snapshot before teardown.
                # Coalesce to one delayed flush; do not add filesystem I/O to
                # the FeatureHandle/model hot path.
                self._schedule_metrics_flush(remaining_s)
                return
        self._cancel_scheduled_metrics_flush()
        with self._lock:
            self._last_flush = now
            payload = {
                "version": 1,
                "kind": "mm_hidden_cache",
                "identity": {
                    "pid": os.getpid(),
                    "engine_id": os.getenv("MOONCAKE_EPD_ENGINE_ID", ""),
                    "kv_role": os.getenv("MOONCAKE_EPD_KV_ROLE", ""),
                },
                "updated_at": now,
                "metrics": self.stats,
            }
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            os.replace(tmp, path)
        except Exception:
            # Metrics must never affect serving.
            pass

    def _schedule_metrics_flush(self, delay_s: float) -> None:
        """Persist the latest snapshot after the rate-limit window.

        This runs only when a normal flush was rate-limited.  A daemon timer is
        intentionally used because vLLM workers are short-lived child
        processes; it must not delay process shutdown or inference completion.
        """

        if self._metrics_path is None:
            return
        delay_s = max(0.001, float(delay_s))
        with self._flush_timer_lock:
            existing = self._flush_timer
            if existing is not None and existing.is_alive():
                return
            timer = threading.Timer(delay_s, self._run_scheduled_metrics_flush)
            timer.daemon = True
            self._flush_timer = timer
            timer.start()

    def _run_scheduled_metrics_flush(self) -> None:
        with self._flush_timer_lock:
            self._flush_timer = None
        self._flush_metrics(force=True)

    def _cancel_scheduled_metrics_flush(self) -> None:
        with self._flush_timer_lock:
            timer = self._flush_timer
            self._flush_timer = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


_GLOBAL_CACHE: Optional[VLLMMMHiddenStateCache] = None
_GLOBAL_LOCK = threading.Lock()
_GLOBAL_L2_STORE: Optional[MooncakeHiddenStateStore] = None
_GLOBAL_L2_POLICY: Optional[HiddenCachePolicy] = None


def get_global_mm_hidden_cache() -> VLLMMMHiddenStateCache:
    global _GLOBAL_CACHE
    if _GLOBAL_CACHE is None:
        with _GLOBAL_LOCK:
            if _GLOBAL_CACHE is None:
                _GLOBAL_CACHE = VLLMMMHiddenStateCache(
                    l2_store=_GLOBAL_L2_STORE,
                    l2_policy=_GLOBAL_L2_POLICY,
                )
    return _GLOBAL_CACHE


def configure_vllm_mm_hidden_l2_store(
    store: Optional[MooncakeHiddenStateStore],
    *,
    policy: Optional[HiddenCachePolicy] = None,
) -> None:
    """Inject an exact L2 Store adapter into this vLLM worker process.

    Bootstrap code owns client construction and credentials.  This function is
    intentionally explicit so importing the cache cannot accidentally create a
    networked Store connection or enable L2 with an underspecified identity.
    """

    global _GLOBAL_L2_STORE, _GLOBAL_L2_POLICY
    with _GLOBAL_LOCK:
        _GLOBAL_L2_STORE = store
        _GLOBAL_L2_POLICY = policy or HiddenCachePolicy.from_env()
        if _GLOBAL_CACHE is not None:
            _GLOBAL_CACHE.configure_l2_store(
                _GLOBAL_L2_STORE,
                policy=_GLOBAL_L2_POLICY,
            )


def record_vllm_precomputed_image_embeds_hit(
    count: int,
    stable_keys: Optional[Sequence[str]] = None,
) -> None:
    try:
        get_global_mm_hidden_cache().record_precomputed_image_embeds_hit(
            count=count,
            stable_keys=stable_keys,
        )
    except Exception:
        # Metrics must never affect serving.
        pass


def record_vllm_native_encoder_cache_hit(stable_key: str) -> None:
    try:
        get_global_mm_hidden_cache().record_native_encoder_cache_hit(stable_key)
    except Exception:
        # Metrics must never affect serving.
        pass


def get_or_compute_qwen3vl_image_embeds(
    *,
    pixel_values: torch.Tensor,
    grid_thw: torch.Tensor,
    visual: Any,
    compute_fn: Callable[[], torch.Tensor],
    cache_keys: Optional[Sequence[str]] = None,
) -> torch.Tensor:
    """Return Qwen3-VL image embeds, using hidden-state cache when safe."""

    namespace = "qwen3vl"
    try:
        namespace = ":".join(
            [
                "qwen3vl",
                f"merge={getattr(visual, 'spatial_merge_size', 'unknown')}",
                f"hidden={getattr(visual, 'out_hidden_size', 'unknown')}",
                f"dtype={getattr(visual, 'dtype', 'unknown')}",
            ]
        )
    except Exception:
        pass
    keys = tuple(cache_keys) if cache_keys is not None else get_current_mm_hidden_cache_keys()
    return get_global_mm_hidden_cache().get_or_compute_qwen3vl_items(
        pixel_values=pixel_values,
        grid_thw=grid_thw,
        visual=visual,
        compute_fn=compute_fn,
        namespace=namespace,
        stable_keys=keys,
    )
