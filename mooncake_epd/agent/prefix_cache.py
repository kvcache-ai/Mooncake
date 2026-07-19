"""
Prefix Cache - Hidden State 缓存

缓存 Vision Encoder 输出，避免重复编码相同图像。
"""

import time
import hashlib
import logging
import os
import threading
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict

import torch

logger = logging.getLogger(__name__)


@dataclass
class CachedHiddenState:
    cache_key: str
    hidden_states: torch.Tensor
    metadata: Dict[str, Any]
    created_at: float
    last_accessed: float
    hit_count: int = 0
    size_bytes: int = 0


def _tensor_cache_hash(tensor: torch.Tensor, *, fast_sample: bool = False) -> str:
    """Collision-resistant tensor fingerprint for hidden-state prefix reuse.

    Production cache keys must not sample tensor bytes: two images differing
    outside the sample window would alias and reuse the wrong hidden state.
    Sampling remains as an explicit opt-in diagnostic mode only.
    """
    meta = f"{tuple(tensor.shape)}|{tensor.dtype}|{tuple(tensor.stride())}".encode()
    flat = tensor.detach().contiguous().view(torch.uint8).cpu()
    if fast_sample and flat.numel() > 4096:
        step = max(1, flat.numel() // 4096)
        payload = flat[::step].numpy().tobytes()
        prefix = b"sampled"
    else:
        payload = flat.numpy().tobytes()
        prefix = b"full"
    h = hashlib.sha256()
    h.update(prefix)
    h.update(meta)
    h.update(payload)
    return h.hexdigest()


def _fast_tensor_hash(tensor: torch.Tensor) -> str:
    return _tensor_cache_hash(
        tensor,
        fast_sample=os.getenv("MOONCAKE_EPD_PREFIX_CACHE_FAST_HASH", "0").lower()
        not in {"", "0", "false", "no", "off"},
    )


class HiddenStatePrefixCache:
    """
    Hidden State 前缀缓存

    缓存 Vision Encoder 输出，相同图像直接返回缓存结果。
    使用快速采样 hash 避免全量数据拷贝开销。
    """

    def __init__(
        self,
        max_cache_size_bytes: int = 4 * 1024**3,
        max_entries: int = 1000,
        ttl_seconds: float = 3600.0,
    ):
        self.max_cache_size_bytes = max_cache_size_bytes
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CachedHiddenState] = OrderedDict()
        self._current_size_bytes = 0
        self._total_hits = 0
        self._total_misses = 0
        self._rejected_entries = 0
        self._lock = threading.RLock()

    def get(
        self, pixel_values: torch.Tensor,
        *,
        stable_key: Optional[str] = None,
    ) -> Optional[Tuple[torch.Tensor, Dict[str, Any]]]:
        cache_key = str(stable_key) if stable_key else _fast_tensor_hash(pixel_values)
        now = time.monotonic()
        with self._lock:
            entry = self._cache.get(cache_key)
            if entry is None:
                self._total_misses += 1
                return None

            if now - entry.created_at > self.ttl_seconds:
                self._evict(cache_key)
                self._total_misses += 1
                return None

            entry.last_accessed = now
            entry.hit_count += 1
            self._cache.move_to_end(cache_key)
            self._total_hits += 1
            # A cached hidden state is shared internally.  Returning its raw
            # mutable tensor lets an accidental downstream in-place operation
            # corrupt every later request, so expose an isolated snapshot.
            return entry.hidden_states.clone(), dict(entry.metadata)

    def put(
        self,
        pixel_values: torch.Tensor,
        hidden_states: torch.Tensor,
        metadata: Dict[str, Any],
        *,
        stable_key: Optional[str] = None,
    ):
        cache_key = str(stable_key) if stable_key else _fast_tensor_hash(pixel_values)

        size_bytes = hidden_states.nelement() * hidden_states.element_size()
        with self._lock:
            if cache_key in self._cache:
                self._cache.move_to_end(cache_key)
                return False
            # An entry larger than the complete cache can never fit.  Reject it
            # rather than looping forever after the final LRU item is evicted.
            if (
                size_bytes > self.max_cache_size_bytes
                or self.max_entries <= 0
                or self.max_cache_size_bytes <= 0
            ):
                self._rejected_entries += 1
                return False
            while self._should_evict(size_bytes):
                if not self._cache:
                    self._rejected_entries += 1
                    return False
                self._evict_lru()

            now = time.monotonic()
            self._cache[cache_key] = CachedHiddenState(
                cache_key=cache_key,
                hidden_states=hidden_states.detach().clone(),
                metadata={**metadata.copy(), "cache_key_mode": "stable" if stable_key else "tensor_sha256"},
                created_at=now,
                last_accessed=now,
                size_bytes=size_bytes,
            )
            self._current_size_bytes += size_bytes
            return True

    def _should_evict(self, additional_bytes: int) -> bool:
        return (
            self._current_size_bytes + additional_bytes > self.max_cache_size_bytes
            or len(self._cache) >= self.max_entries
        )

    def _evict_lru(self):
        if self._cache:
            key, entry = self._cache.popitem(last=False)
            self._current_size_bytes -= entry.size_bytes

    def _evict(self, key: str):
        entry = self._cache.pop(key, None)
        if entry:
            self._current_size_bytes -= entry.size_bytes

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._total_hits + self._total_misses
            return {
                "total_entries": len(self._cache),
                "cache_size_mb": self._current_size_bytes / (1024 * 1024),
                "max_size_mb": self.max_cache_size_bytes / (1024 * 1024),
                "total_hits": self._total_hits,
                "total_misses": self._total_misses,
                "rejected_entries": self._rejected_entries,
                "hit_rate": self._total_hits / max(total, 1),
            }

    def clear(self):
        with self._lock:
            self._cache.clear()
            self._current_size_bytes = 0
