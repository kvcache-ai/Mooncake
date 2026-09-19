"""Hidden-state prefix cache for multimodal Omni models.

This module targets Qwen2.5-Omni's real encoder boundary without modifying
Transformers source files.  It wraps ``get_image_features`` and
``get_audio_features`` on a loaded thinker/model instance and reuses the
contiguous multimodal prefix that has already been encoded.  Only the suffix
that misses the cache is sent through the vision/audio tower, which is the
critical optimization for multi-round Agent workflows where the same image or
audio prefix is reused while later turns append new content.
"""

from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import hashlib
import json
import os
import threading
import time
from collections import OrderedDict
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import torch

try:  # Transformers is present in production/vLLM envs; keep import soft for tests.
    from transformers.modeling_outputs import BaseModelOutput, BaseModelOutputWithPooling
except Exception:  # pragma: no cover - only for extremely small envs.
    BaseModelOutput = None  # type: ignore[assignment]
    BaseModelOutputWithPooling = None  # type: ignore[assignment]


_CURRENT_OMNI_HIDDEN_CACHE_KEYS: contextvars.ContextVar[Optional[Tuple[str, ...]]] = (
    contextvars.ContextVar("mooncake_epd_omni_hidden_cache_keys", default=None)
)


def _normalize_stable_keys(keys: Optional[Sequence[str]]) -> Optional[Tuple[str, ...]]:
    if keys is None:
        return None
    normalized = tuple(str(key).strip() for key in keys)
    if not normalized or any(not key for key in normalized):
        return None
    return normalized


@contextlib.contextmanager
def use_omni_hidden_cache_keys(keys: Optional[Sequence[str]]):
    """Expose authoritative source-content IDs to the Omni cache wrapper.

    The IDs must identify the loaded source bytes, not a mutable URL or a
    sampled tensor digest. Invalid or absent IDs intentionally select the
    existing full-tensor hashing path.
    """

    token = _CURRENT_OMNI_HIDDEN_CACHE_KEYS.set(_normalize_stable_keys(keys))
    try:
        yield
    finally:
        _CURRENT_OMNI_HIDDEN_CACHE_KEYS.reset(token)


def get_current_omni_hidden_cache_keys() -> Optional[Tuple[str, ...]]:
    return _CURRENT_OMNI_HIDDEN_CACHE_KEYS.get()


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


@dataclass(frozen=True)
class OmniHiddenPrefixCacheConfig:
    enabled: bool = True
    max_entries: int = 256
    max_bytes: int = 2 * 1024 * 1024 * 1024
    store_on_gpu: bool = False
    namespace: str = "qwen2.5-omni"
    metrics_path: Optional[str] = None
    metrics_flush_interval_s: float = 0.25
    allow_partial_prefix_reuse: bool = False

    @classmethod
    def from_env(cls) -> "OmniHiddenPrefixCacheConfig":
        return cls(
            enabled=_env_bool("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE", True),
            max_entries=_env_int("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_MAX_ENTRIES", 256, minimum=1),
            max_bytes=_env_int(
                "MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_MAX_BYTES",
                2 * 1024 * 1024 * 1024,
                minimum=1,
            ),
            store_on_gpu=_env_bool("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_GPU", False),
            namespace=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_NAMESPACE", "qwen2.5-omni"),
            metrics_path=(os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_METRICS") or None),
            metrics_flush_interval_s=_env_float(
                "MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_METRICS_INTERVAL_S",
                0.25,
                minimum=0.0,
            ),
            allow_partial_prefix_reuse=_env_bool(
                "MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_ALLOW_PARTIAL",
                False,
            ),
        )


@dataclass
class _CacheEntry:
    tensor: torch.Tensor
    num_bytes: int
    shape: Tuple[int, ...]
    dtype: str
    device_type: str
    created_at: float = field(default_factory=time.monotonic)
    last_access_at: float = field(default_factory=time.monotonic)
    hits: int = 0


class OmniHiddenPrefixCache:
    """Thread-safe LRU cache for Omni image/audio hidden-state prefixes."""

    def __init__(self, config: Optional[OmniHiddenPrefixCacheConfig] = None) -> None:
        self.config = config or OmniHiddenPrefixCacheConfig.from_env()
        self._lock = threading.RLock()
        self._metrics_write_lock = threading.Lock()
        self._entries: "OrderedDict[str, _CacheEntry]" = OrderedDict()
        self._inflight: Dict[str, threading.Event] = {}
        self._bytes = 0
        self._last_flush = 0.0
        self._metrics_dirty = True
        self._metrics_force_flush = False
        self._lookup_lock_wait_samples_ms: "deque[float]" = deque(maxlen=2048)
        self._lookup_lock_hold_samples_ms: "deque[float]" = deque(maxlen=2048)
        self._cache_load_samples_ms: "deque[float]" = deque(maxlen=2048)
        self._metrics: Dict[str, Any] = {
            "enabled": bool(self.config.enabled),
            "lookups": 0,
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "evictions": 0,
            "bytes": 0,
            "entries": 0,
            "image_batches": 0,
            "audio_batches": 0,
            "full_hit_batches": 0,
            "partial_hit_batches": 0,
            "full_miss_batches": 0,
            "prefix_hit_items": 0,
            "prefix_miss_items": 0,
            "image_encoder_calls": 0,
            "audio_encoder_calls": 0,
            "encoder_compute_ms_total": 0.0,
            "cache_load_ms_total": 0.0,
            "hash_ms_total": 0.0,
            "stable_key_lookups": 0,
            "stable_key_batches": 0,
            "tensor_key_lookups": 0,
            "stable_key_fallbacks": 0,
            "coalesced_waits": 0,
            "coalesced_hits": 0,
            "errors": 0,
            "last_error": "",
        }
        self._flush_metrics(force=True)

    @property
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            out = dict(self._metrics)
            out["bytes"] = int(self._bytes)
            out["entries"] = int(len(self._entries))
            out["max_entries"] = int(self.config.max_entries)
            out["max_bytes"] = int(self.config.max_bytes)
            out["store_on_gpu"] = bool(self.config.store_on_gpu)
            total = int(out.get("lookups", 0) or 0)
            out["hit_rate"] = float(out.get("hits", 0) or 0) / total if total else 0.0
            batches = int(out.get("image_batches", 0) or 0) + int(out.get("audio_batches", 0) or 0)
            out["batch_full_hit_rate"] = float(out.get("full_hit_batches", 0) or 0) / batches if batches else 0.0
            out["lookup_lock_wait_p95_ms"] = self._percentile(
                self._lookup_lock_wait_samples_ms, 0.95
            )
            out["lookup_lock_hold_p95_ms"] = self._percentile(
                self._lookup_lock_hold_samples_ms, 0.95
            )
            out["cache_load_p95_ms"] = self._percentile(self._cache_load_samples_ms, 0.95)
            return out

    def get_or_compute_image(
        self,
        *,
        model: Any,
        original_fn: Callable[..., Any],
        pixel_values: torch.Tensor,
        image_grid_thw: Optional[torch.Tensor],
        kwargs: Dict[str, Any],
    ) -> Any:
        if not self.config.enabled or image_grid_thw is None:
            return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
        if kwargs.get("return_dict") is False:
            return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
        if not self.config.allow_partial_prefix_reuse:
            return self._get_or_compute_image_exact_batch(
                model=model,
                original_fn=original_fn,
                pixel_values=pixel_values,
                image_grid_thw=image_grid_thw,
                kwargs=kwargs,
            )
        try:
            grid = image_grid_thw
            if grid.ndim != 2 or grid.shape[-1] != 3:
                return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
            item_count = int(grid.shape[0])
            merge = int(getattr(getattr(model, "visual", None), "spatial_merge_size", 1) or 1)
            input_sizes = [int(x) for x in grid.prod(-1).detach().cpu().tolist()]
            output_sizes = [max(1, int(size) // max(1, merge * merge)) for size in input_sizes]
            if item_count <= 0 or sum(input_sizes) != int(pixel_values.shape[0]):
                return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
            chunks = list(torch.split(pixel_values, input_sizes, dim=0))
            started_hash = time.perf_counter()
            stable_keys = self._stable_keys_for_count(item_count)
            if stable_keys is not None:
                keys = [
                    self._stable_content_key(
                        modality="image",
                        stable_ids=(stable_keys[i],),
                        tensor=chunks[i],
                        aux=grid[i],
                        namespace=self._model_namespace(model),
                        output_tokens=output_sizes[i],
                    )
                    for i in range(item_count)
                ]
                self._record_key_mode(stable=True, lookups=item_count)
            else:
                keys = [
                    self._tensor_key(
                        modality="image",
                        tensor=chunks[i],
                        aux=grid[i],
                        namespace=self._model_namespace(model),
                        output_tokens=output_sizes[i],
                    )
                    for i in range(item_count)
                ]
                self._record_key_mode(stable=False, lookups=item_count)
            self._add_metric("hash_ms_total", (time.perf_counter() - started_hash) * 1000.0)
            prefix_hit, cached = self._lookup_prefix(keys, pixel_values.device, None)
            miss_count = item_count - prefix_hit
            with self._lock:
                self._metrics["image_batches"] += 1
            if miss_count == 0:
                self._record_batch(item_count, 0, prefix_hit)
                return self._image_output(torch.cat(cached, dim=0))
            suffix_pixels = torch.cat(chunks[prefix_hit:], dim=0)
            suffix_grid = grid[prefix_hit:]
            t0 = time.perf_counter()
            result = original_fn(suffix_pixels, image_grid_thw=suffix_grid, **kwargs)
            compute_ms = (time.perf_counter() - t0) * 1000.0
            suffix_hidden = getattr(result, "pooler_output", None)
            if suffix_hidden is None:
                return result if prefix_hit == 0 else original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
            suffix_chunks = list(torch.split(suffix_hidden, output_sizes[prefix_hit:], dim=0))
            if len(suffix_chunks) != miss_count:
                return result if prefix_hit == 0 else original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
            for key, tensor in zip(keys[prefix_hit:], suffix_chunks):
                self._store(key, tensor)
            with self._lock:
                self._metrics["image_encoder_calls"] += 1
                self._metrics["encoder_compute_ms_total"] += float(compute_ms)
            self._record_batch(item_count, miss_count, prefix_hit)
            if prefix_hit == 0:
                return result
            return self._image_output(torch.cat([*cached, *suffix_chunks], dim=0))
        except Exception as exc:
            self._record_error(exc)
            return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)

    def get_or_compute_audio(
        self,
        *,
        model: Any,
        original_fn: Callable[..., Any],
        input_features: torch.Tensor,
        feature_attention_mask: Optional[torch.Tensor],
        audio_feature_lengths: Optional[torch.Tensor],
        kwargs: Dict[str, Any],
    ) -> Any:
        if not self.config.enabled or kwargs.get("return_dict") is False:
            return original_fn(
                input_features,
                feature_attention_mask=feature_attention_mask,
                audio_feature_lengths=audio_feature_lengths,
                **kwargs,
            )
        if feature_attention_mask is None and audio_feature_lengths is None:
            return original_fn(
                input_features,
                feature_attention_mask=feature_attention_mask,
                audio_feature_lengths=audio_feature_lengths,
                **kwargs,
            )
        if not self.config.allow_partial_prefix_reuse:
            return self._get_or_compute_audio_exact_batch(
                model=model,
                original_fn=original_fn,
                input_features=input_features,
                feature_attention_mask=feature_attention_mask,
                audio_feature_lengths=audio_feature_lengths,
                kwargs=kwargs,
            )
        try:
            item_count = int(input_features.shape[0])
            if item_count <= 0:
                return original_fn(
                    input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    **kwargs,
                )
            if feature_attention_mask is not None:
                feature_lens = torch.sum(feature_attention_mask, dim=1).to(device=input_features.device)
            else:
                feature_lens = audio_feature_lengths.to(device=input_features.device)  # type: ignore[union-attr]
            _, output_lens = model.audio_tower._get_feat_extract_output_lengths(feature_lens)
            output_sizes = [int(x) for x in output_lens.detach().cpu().tolist()]
            started_hash = time.perf_counter()
            keys = [
                self._tensor_key(
                    modality="audio",
                    tensor=input_features[i],
                    aux=(feature_attention_mask[i] if feature_attention_mask is not None else feature_lens[i]),
                    namespace=self._model_namespace(model),
                    output_tokens=output_sizes[i],
                )
                for i in range(item_count)
            ]
            self._record_key_mode(stable=False, lookups=item_count)
            self._add_metric("hash_ms_total", (time.perf_counter() - started_hash) * 1000.0)
            prefix_hit, cached = self._lookup_prefix(keys, input_features.device, None)
            miss_count = item_count - prefix_hit
            with self._lock:
                self._metrics["audio_batches"] += 1
            if miss_count == 0:
                self._record_batch(item_count, 0, prefix_hit)
                return self._audio_output(torch.cat(cached, dim=0))
            tail_slice = slice(prefix_hit, item_count)
            tail_features = input_features[tail_slice]
            tail_mask = feature_attention_mask[tail_slice] if feature_attention_mask is not None else None
            tail_lengths = audio_feature_lengths[tail_slice] if audio_feature_lengths is not None else None
            t0 = time.perf_counter()
            result = original_fn(
                tail_features,
                feature_attention_mask=tail_mask,
                audio_feature_lengths=tail_lengths,
                **kwargs,
            )
            compute_ms = (time.perf_counter() - t0) * 1000.0
            suffix_hidden = getattr(result, "last_hidden_state", None)
            if suffix_hidden is None:
                return result if prefix_hit == 0 else original_fn(
                    input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    **kwargs,
                )
            suffix_chunks = list(torch.split(suffix_hidden, output_sizes[prefix_hit:], dim=0))
            if len(suffix_chunks) != miss_count:
                return result if prefix_hit == 0 else original_fn(
                    input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    **kwargs,
                )
            for key, tensor in zip(keys[prefix_hit:], suffix_chunks):
                self._store(key, tensor)
            with self._lock:
                self._metrics["audio_encoder_calls"] += 1
                self._metrics["encoder_compute_ms_total"] += float(compute_ms)
            self._record_batch(item_count, miss_count, prefix_hit)
            if prefix_hit == 0:
                return result
            return self._audio_output(torch.cat([*cached, *suffix_chunks], dim=0))
        except Exception as exc:
            self._record_error(exc)
            return original_fn(
                input_features,
                feature_attention_mask=feature_attention_mask,
                audio_feature_lengths=audio_feature_lengths,
                **kwargs,
            )

    def _get_or_compute_image_exact_batch(
        self,
        *,
        model: Any,
        original_fn: Callable[..., Any],
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        kwargs: Dict[str, Any],
    ) -> Any:
        try:
            started_hash = time.perf_counter()
            item_count = int(image_grid_thw.shape[0])
            stable_keys = self._stable_keys_for_count(item_count)
            if stable_keys is not None:
                key = self._stable_content_key(
                    modality="image-batch",
                    stable_ids=stable_keys,
                    tensor=pixel_values,
                    aux=image_grid_thw,
                    namespace=self._model_namespace(model),
                    output_tokens=-1,
                )
                self._record_key_mode(stable=True, lookups=1)
            else:
                key = self._tensor_key(
                    modality="image-batch",
                    tensor=pixel_values,
                    aux=image_grid_thw,
                    namespace=self._model_namespace(model),
                    output_tokens=-1,
                )
                self._record_key_mode(stable=False, lookups=1)
            self._add_metric("hash_ms_total", (time.perf_counter() - started_hash) * 1000.0)
            with self._lock:
                self._metrics["image_batches"] += 1
                self._metrics["lookups"] += 1
            cached = self._lookup(key, target_device=pixel_values.device, target_dtype=None)
            if cached is not None:
                with self._lock:
                    self._metrics["full_hit_batches"] += 1
                    self._metrics["prefix_hit_items"] += int(image_grid_thw.shape[0])
                    self._flush_metrics_locked()
                return self._image_output(cached)
            wait_for = self._begin_or_join_inflight(key)
            if wait_for is not None:
                wait_for.wait()
                cached = self._lookup(key, target_device=pixel_values.device, target_dtype=None)
                if cached is not None:
                    with self._lock:
                        self._metrics["coalesced_hits"] += 1
                        self._metrics["full_hit_batches"] += 1
                        self._metrics["prefix_hit_items"] += int(image_grid_thw.shape[0])
                        self._flush_metrics_locked()
                    return self._image_output(cached)
                wait_for = self._begin_or_join_inflight(key)
                if wait_for is not None:
                    wait_for.wait()
                    cached = self._lookup(key, target_device=pixel_values.device, target_dtype=None)
                    if cached is not None:
                        with self._lock:
                            self._metrics["coalesced_hits"] += 1
                            self._metrics["full_hit_batches"] += 1
                            self._metrics["prefix_hit_items"] += int(image_grid_thw.shape[0])
                            self._flush_metrics_locked()
                        return self._image_output(cached)
            t0 = time.perf_counter()
            try:
                result = original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)
                compute_ms = (time.perf_counter() - t0) * 1000.0
                hidden = getattr(result, "pooler_output", None)
                if hidden is None:
                    return result
                self._store(key, hidden)
                with self._lock:
                    self._metrics["image_encoder_calls"] += 1
                    self._metrics["encoder_compute_ms_total"] += float(compute_ms)
                    self._metrics["full_miss_batches"] += 1
                    self._metrics["prefix_miss_items"] += int(image_grid_thw.shape[0])
                    self._flush_metrics_locked()
                return result
            finally:
                self._finish_inflight(key)
        except Exception as exc:
            self._record_error(exc)
            return original_fn(pixel_values, image_grid_thw=image_grid_thw, **kwargs)

    def _get_or_compute_audio_exact_batch(
        self,
        *,
        model: Any,
        original_fn: Callable[..., Any],
        input_features: torch.Tensor,
        feature_attention_mask: Optional[torch.Tensor],
        audio_feature_lengths: Optional[torch.Tensor],
        kwargs: Dict[str, Any],
    ) -> Any:
        try:
            aux = feature_attention_mask if feature_attention_mask is not None else audio_feature_lengths
            if aux is None:
                return original_fn(
                    input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    **kwargs,
                )
            started_hash = time.perf_counter()
            key = self._tensor_key(
                modality="audio-batch",
                tensor=input_features,
                aux=aux,
                namespace=self._model_namespace(model),
                output_tokens=-1,
            )
            self._record_key_mode(stable=False, lookups=1)
            self._add_metric("hash_ms_total", (time.perf_counter() - started_hash) * 1000.0)
            with self._lock:
                self._metrics["audio_batches"] += 1
                self._metrics["lookups"] += 1
            cached = self._lookup(key, target_device=input_features.device, target_dtype=None)
            if cached is not None:
                with self._lock:
                    self._metrics["full_hit_batches"] += 1
                    self._metrics["prefix_hit_items"] += int(input_features.shape[0])
                    self._flush_metrics_locked()
                return self._audio_output(cached)
            wait_for = self._begin_or_join_inflight(key)
            if wait_for is not None:
                wait_for.wait()
                cached = self._lookup(key, target_device=input_features.device, target_dtype=None)
                if cached is not None:
                    with self._lock:
                        self._metrics["coalesced_hits"] += 1
                        self._metrics["full_hit_batches"] += 1
                        self._metrics["prefix_hit_items"] += int(input_features.shape[0])
                        self._flush_metrics_locked()
                    return self._audio_output(cached)
                wait_for = self._begin_or_join_inflight(key)
                if wait_for is not None:
                    wait_for.wait()
                    cached = self._lookup(key, target_device=input_features.device, target_dtype=None)
                    if cached is not None:
                        with self._lock:
                            self._metrics["coalesced_hits"] += 1
                            self._metrics["full_hit_batches"] += 1
                            self._metrics["prefix_hit_items"] += int(input_features.shape[0])
                            self._flush_metrics_locked()
                        return self._audio_output(cached)
            t0 = time.perf_counter()
            try:
                result = original_fn(
                    input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    **kwargs,
                )
                compute_ms = (time.perf_counter() - t0) * 1000.0
                hidden = getattr(result, "last_hidden_state", None)
                if hidden is None:
                    return result
                self._store(key, hidden)
                with self._lock:
                    self._metrics["audio_encoder_calls"] += 1
                    self._metrics["encoder_compute_ms_total"] += float(compute_ms)
                    self._metrics["full_miss_batches"] += 1
                    self._metrics["prefix_miss_items"] += int(input_features.shape[0])
                    self._flush_metrics_locked()
                return result
            finally:
                self._finish_inflight(key)
        except Exception as exc:
            self._record_error(exc)
            return original_fn(
                input_features,
                feature_attention_mask=feature_attention_mask,
                audio_feature_lengths=audio_feature_lengths,
                **kwargs,
            )

    def _lookup_prefix(
        self,
        keys: Sequence[str],
        target_device: torch.device,
        target_dtype: Optional[torch.dtype],
    ) -> Tuple[int, List[torch.Tensor]]:
        prefix = 0
        tensors: List[torch.Tensor] = []
        for key in keys:
            with self._lock:
                self._metrics["lookups"] += 1
            tensor = self._lookup(key, target_device=target_device, target_dtype=target_dtype)
            if tensor is None:
                break
            prefix += 1
            tensors.append(tensor)
        with self._lock:
            skipped = max(0, len(keys) - prefix - 1)
            if skipped:
                self._metrics["lookups"] += skipped
                self._metrics["misses"] += skipped
        return prefix, tensors

    def _lookup(
        self,
        key: str,
        *,
        target_device: torch.device,
        target_dtype: Optional[torch.dtype],
    ) -> Optional[torch.Tensor]:
        t0 = time.perf_counter()
        lock_wait_started = time.perf_counter()
        with self._lock:
            lock_acquired = time.perf_counter()
            entry = self._entries.get(key)
            if entry is None:
                self._metrics["misses"] += 1
                self._lookup_lock_wait_samples_ms.append(
                    (lock_acquired - lock_wait_started) * 1000.0
                )
                self._lookup_lock_hold_samples_ms.append(
                    (time.perf_counter() - lock_acquired) * 1000.0
                )
                self._flush_metrics_locked()
                return None
            self._entries.move_to_end(key)
            entry.hits += 1
            entry.last_access_at = time.monotonic()
            self._metrics["hits"] += 1
            tensor = entry.tensor
            self._lookup_lock_wait_samples_ms.append(
                (lock_acquired - lock_wait_started) * 1000.0
            )
            self._lookup_lock_hold_samples_ms.append(
                (time.perf_counter() - lock_acquired) * 1000.0
            )
        out = (
            tensor.to(device=target_device, dtype=target_dtype, non_blocking=True)
            if target_dtype
            else tensor.to(target_device, non_blocking=True)
        )
        load_ms = (time.perf_counter() - t0) * 1000.0
        with self._lock:
            self._metrics["cache_load_ms_total"] += load_ms
            self._cache_load_samples_ms.append(load_ms)
            self._metrics_dirty = True
        return out

    def _store(self, key: str, tensor: torch.Tensor) -> None:
        detached = tensor.detach()
        if not self.config.store_on_gpu:
            detached = detached.to("cpu", non_blocking=True)
        else:
            detached = detached.clone()
        detached = detached.contiguous()
        nbytes = int(detached.nelement() * detached.element_size())
        entry = _CacheEntry(
            tensor=detached,
            num_bytes=nbytes,
            shape=tuple(int(x) for x in detached.shape),
            dtype=str(detached.dtype),
            device_type=str(detached.device.type),
        )
        with self._lock:
            old = self._entries.pop(key, None)
            if old is not None:
                self._bytes -= old.num_bytes
            self._entries[key] = entry
            self._bytes += nbytes
            self._metrics["stores"] += 1
            while (
                len(self._entries) > self.config.max_entries
                or self._bytes > self.config.max_bytes
            ) and self._entries:
                _, victim = self._entries.popitem(last=False)
                self._bytes -= victim.num_bytes
                self._metrics["evictions"] += 1
            self._flush_metrics_locked()

    def _record_batch(self, total_items: int, miss_items: int, prefix_hit: int) -> None:
        with self._lock:
            self._metrics["prefix_hit_items"] += int(prefix_hit)
            self._metrics["prefix_miss_items"] += int(miss_items)
            if miss_items == 0:
                self._metrics["full_hit_batches"] += 1
            elif miss_items == total_items:
                self._metrics["full_miss_batches"] += 1
            else:
                self._metrics["partial_hit_batches"] += 1
            self._flush_metrics_locked()

    def _record_error(self, exc: BaseException) -> None:
        with self._lock:
            self._metrics["errors"] += 1
            self._metrics["last_error"] = f"{type(exc).__name__}: {exc}"
            self._flush_metrics_locked(force=True)

    def _add_metric(self, key: str, value: float) -> None:
        with self._lock:
            self._metrics[key] = float(self._metrics.get(key, 0.0) or 0.0) + float(value)
            self._metrics_dirty = True

    def _begin_or_join_inflight(self, key: str) -> Optional[threading.Event]:
        with self._lock:
            event = self._inflight.get(key)
            if event is not None:
                self._metrics["coalesced_waits"] += 1
                self._flush_metrics_locked()
                return event
            self._inflight[key] = threading.Event()
            return None

    def _finish_inflight(self, key: str) -> None:
        with self._lock:
            event = self._inflight.pop(key, None)
            if event is not None:
                event.set()

    def _tensor_key(
        self,
        *,
        modality: str,
        tensor: torch.Tensor,
        aux: torch.Tensor,
        namespace: str,
        output_tokens: int,
    ) -> str:
        h = hashlib.sha256()
        h.update(b"mooncake-epd-omni-hidden-prefix-v1\0")
        h.update(str(self.config.namespace).encode("utf-8", errors="replace"))
        h.update(b"\0model\0")
        h.update(str(namespace).encode("utf-8", errors="replace"))
        h.update(b"\0modality\0")
        h.update(str(modality).encode("ascii", errors="replace"))
        h.update(b"\0output\0")
        h.update(str(int(output_tokens)).encode("ascii"))
        self._hash_tensor_into(h, tensor)
        self._hash_tensor_into(h, aux)
        return h.hexdigest()

    def _stable_keys_for_count(self, expected_count: int) -> Optional[Tuple[str, ...]]:
        keys = get_current_omni_hidden_cache_keys()
        if keys is None:
            return None
        if len(keys) != int(expected_count):
            with self._lock:
                self._metrics["stable_key_fallbacks"] += 1
            return None
        return keys

    def _record_key_mode(self, *, stable: bool, lookups: int) -> None:
        with self._lock:
            if stable:
                self._metrics["stable_key_lookups"] += max(0, int(lookups))
                self._metrics["stable_key_batches"] += 1
            else:
                self._metrics["tensor_key_lookups"] += max(0, int(lookups))

    def _stable_content_key(
        self,
        *,
        modality: str,
        stable_ids: Sequence[str],
        tensor: torch.Tensor,
        aux: torch.Tensor,
        namespace: str,
        output_tokens: int,
    ) -> str:
        """Build a cache key without reading the full preprocessed tensor.

        ``stable_ids`` are authoritative SHA-256-like source identities
        supplied by the encoder boundary. Shape/dtype and the small semantic
        grid tensor remain part of the key so processor-layout changes cannot
        alias an older entry.
        """

        h = hashlib.sha256()
        h.update(b"mooncake-epd-omni-hidden-prefix-stable-v1\0")
        h.update(str(self.config.namespace).encode("utf-8", errors="replace"))
        h.update(b"\0model\0")
        h.update(str(namespace).encode("utf-8", errors="replace"))
        h.update(b"\0modality\0")
        h.update(str(modality).encode("ascii", errors="replace"))
        h.update(b"\0output\0")
        h.update(str(int(output_tokens)).encode("ascii"))
        h.update(b"\0stable-ids\0")
        h.update(
            json.dumps(list(stable_ids), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        self._hash_tensor_metadata_into(h, tensor)
        self._hash_small_tensor_into(h, aux)
        return h.hexdigest()

    @staticmethod
    def _hash_tensor_metadata_into(h: "hashlib._Hash", tensor: torch.Tensor) -> None:
        h.update(b"\0shape\0")
        h.update(json.dumps(list(tensor.shape), separators=(",", ":")).encode())
        h.update(b"\0dtype\0")
        h.update(str(tensor.dtype).encode())

    @staticmethod
    def _hash_small_tensor_into(h: "hashlib._Hash", tensor: torch.Tensor) -> None:
        """Hash semantic metadata tensors such as image_grid_thw.

        This copy is bounded by the number of multimodal items and never reads
        the full pixel tensor.
        """

        t = tensor.detach().contiguous().cpu()
        OmniHiddenPrefixCache._hash_tensor_metadata_into(h, t)
        h.update(b"\0bytes\0")
        h.update(t.view(torch.uint8).numpy().tobytes())

    @staticmethod
    def _hash_tensor_into(h: "hashlib._Hash", tensor: torch.Tensor) -> None:
        t = tensor.detach().contiguous().cpu()
        OmniHiddenPrefixCache._hash_tensor_metadata_into(h, t)
        h.update(b"\0bytes\0")
        h.update(t.view(torch.uint8).numpy().tobytes())

    @staticmethod
    def _model_namespace(model: Any) -> str:
        config = getattr(model, "config", None)
        pieces = [
            model.__class__.__name__,
            getattr(config, "_name_or_path", None) or getattr(config, "name_or_path", None),
            getattr(config, "model_type", None),
            getattr(config, "torch_dtype", None),
        ]
        return "|".join(str(p) for p in pieces if p is not None)

    @staticmethod
    def _image_output(tensor: torch.Tensor) -> Any:
        if BaseModelOutputWithPooling is not None:
            return BaseModelOutputWithPooling(pooler_output=tensor)
        return type("ImageOutput", (), {"pooler_output": tensor})()

    @staticmethod
    def _audio_output(tensor: torch.Tensor) -> Any:
        if BaseModelOutput is not None:
            return BaseModelOutput(last_hidden_state=tensor)
        return type("AudioOutput", (), {"last_hidden_state": tensor})()

    def _flush_metrics(self, *, force: bool = False) -> None:
        path = self.config.metrics_path
        if not path:
            return
        with self._lock:
            now = time.monotonic()
            force = bool(force or self._metrics_force_flush)
            if not force and not self._metrics_dirty:
                return
            if not force and (now - self._last_flush) < self.config.metrics_flush_interval_s:
                return
            self._last_flush = now
            self._metrics_dirty = False
            self._metrics_force_flush = False
            payload = self._metrics_payload_locked()

        try:
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
            with self._metrics_write_lock:
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
                os.replace(tmp, path)
        except Exception:
            with self._lock:
                self._metrics_dirty = True

    def _flush_metrics_locked(self, *, force: bool = False) -> None:
        """Mark metrics dirty; disk I/O is performed outside the cache lock."""

        self._metrics_dirty = True
        self._metrics_force_flush = bool(self._metrics_force_flush or force)

    def _metrics_payload_locked(self) -> Dict[str, Any]:
        payload = dict(self._metrics)
        payload["bytes"] = int(self._bytes)
        payload["entries"] = int(len(self._entries))
        total = int(payload.get("lookups", 0) or 0)
        payload["hit_rate"] = float(payload.get("hits", 0) or 0) / total if total else 0.0
        batches = int(payload.get("image_batches", 0) or 0) + int(payload.get("audio_batches", 0) or 0)
        payload["batch_full_hit_rate"] = (
            float(payload.get("full_hit_batches", 0) or 0) / batches if batches else 0.0
        )
        payload["kind"] = "omni_hidden_prefix_cache"
        payload["ts"] = time.time()
        payload["lookup_lock_wait_p95_ms"] = self._percentile(
            self._lookup_lock_wait_samples_ms, 0.95
        )
        payload["lookup_lock_hold_p95_ms"] = self._percentile(
            self._lookup_lock_hold_samples_ms, 0.95
        )
        payload["cache_load_p95_ms"] = self._percentile(self._cache_load_samples_ms, 0.95)
        return payload

    @staticmethod
    def _percentile(samples: Sequence[float], fraction: float) -> Optional[float]:
        if not samples:
            return None
        ordered = sorted(float(value) for value in samples)
        index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * fraction))))
        return ordered[index]


def install_qwen2_5_omni_hidden_prefix_cache(
    model: Any,
    cache: Optional[OmniHiddenPrefixCache] = None,
) -> OmniHiddenPrefixCache:
    """Install prefix-cache wrappers on a Qwen2.5-Omni thinker/model instance.

    ``model`` may be either ``Qwen2_5OmniThinkerForConditionalGeneration`` or
    the top-level ``Qwen2_5OmniForConditionalGeneration`` that owns a
    ``.thinker`` submodule.  The function is idempotent per target instance.
    """

    target = getattr(model, "thinker", model)
    existing = getattr(target, "_mooncake_epd_omni_hidden_prefix_cache", None)
    if existing is not None:
        return existing
    if not hasattr(target, "get_image_features") and not hasattr(target, "get_audio_features"):
        raise TypeError("target model does not expose Qwen2.5-Omni feature methods")
    cache = cache or OmniHiddenPrefixCache()

    if hasattr(target, "get_image_features"):
        original_image = target.get_image_features

        def _cached_get_image_features(pixel_values, image_grid_thw=None, **kwargs):
            try:
                return cache.get_or_compute_image(
                    model=target,
                    original_fn=original_image,
                    pixel_values=pixel_values,
                    image_grid_thw=image_grid_thw,
                    kwargs=dict(kwargs),
                )
            finally:
                cache._flush_metrics()

        target._mooncake_epd_original_get_image_features = original_image
        target.get_image_features = _cached_get_image_features

    if hasattr(target, "get_audio_features"):
        original_audio = target.get_audio_features

        def _cached_get_audio_features(input_features, feature_attention_mask=None, audio_feature_lengths=None, **kwargs):
            try:
                return cache.get_or_compute_audio(
                    model=target,
                    original_fn=original_audio,
                    input_features=input_features,
                    feature_attention_mask=feature_attention_mask,
                    audio_feature_lengths=audio_feature_lengths,
                    kwargs=dict(kwargs),
                )
            finally:
                cache._flush_metrics()

        target._mooncake_epd_original_get_audio_features = original_audio
        target.get_audio_features = _cached_get_audio_features

    target._mooncake_epd_omni_hidden_prefix_cache = cache
    return cache


__all__ = [
    "OmniHiddenPrefixCache",
    "OmniHiddenPrefixCacheConfig",
    "get_current_omni_hidden_cache_keys",
    "install_qwen2_5_omni_hidden_prefix_cache",
    "use_omni_hidden_cache_keys",
]
