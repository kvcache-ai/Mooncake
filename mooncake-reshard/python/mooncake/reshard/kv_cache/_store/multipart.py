"""Upload and restore KV pages using caller-supplied source manifests."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from ..placement import KVCachePlacementManifest
from ..resolved import DEFAULT_TRANSFER_LIMITS
from ..types import (
    require_integer,
    require_manifest_items,
    require_nonempty_string,
    require_sha256,
)
from .backend import MultipartBackend
from .binding import KVCacheStoreRuntimeBinding
from .manifest import KVCacheStoreManifest
from .page_reader import MultipartPageReader
from .planner import DEFAULT_STORE_PLANNING_LIMITS, KVCacheStoreUploadPlan
from .runtime import StoreByteRange, lower_store_ranges


@dataclass(frozen=True)
class MultipartReadContext:
    operation_id: str
    model_domain: str
    page_keys: tuple[str, ...]
    layout_ids: tuple[str, ...]
    manifests: tuple[KVCacheStoreManifest, ...]

    def __post_init__(self) -> None:
        require_nonempty_string(self.operation_id, "operation_id")
        require_sha256(self.model_domain, "model_domain")
        page_keys = require_manifest_items(self.page_keys, "page_keys", str)
        layout_ids = require_manifest_items(self.layout_ids, "layout_ids", str)
        manifests = require_manifest_items(
            self.manifests, "manifests", KVCacheStoreManifest
        )
        for key in page_keys:
            require_nonempty_string(key, "page_key")
        by_id = {m.layout.layout_id: m for m in manifests}
        if len(page_keys) != len(layout_ids):
            raise ValueError("invalid multipart read context")
        if len(by_id) != len(manifests) or set(by_id) != set(layout_ids):
            raise ValueError("multipart source manifests differ")
        if any(m.model_domain != self.model_domain for m in manifests):
            raise ValueError("multipart model domain differs")
        object.__setattr__(self, "page_keys", page_keys)
        object.__setattr__(self, "layout_ids", layout_ids)
        object.__setattr__(self, "manifests", manifests)


class MultipartKVCacheStore:
    """Execute KV transfers using a compatible Part backend and explicit manifests.

    The caller owns registration and keeps source/destination buffers and page
    reservations valid until the synchronous upload or load finishes. This
    layer does not publish, discover, or choose a manifest representation.
    """

    def __init__(
        self, native: Any, manifest: KVCacheStoreManifest, *, cache_capacity: int = 64
    ) -> None:
        require_integer(cache_capacity, "cache_capacity", minimum=1)
        self.backend = (
            native if isinstance(native, MultipartBackend) else MultipartBackend(native)
        )
        self.manifest = manifest
        self.planning_limits = DEFAULT_STORE_PLANNING_LIMITS
        self.transfer_limits = DEFAULT_TRANSFER_LIMITS
        self.cache_capacity = cache_capacity

    def prepare_page_reader(
        self,
        placement: KVCachePlacementManifest,
        binding: KVCacheStoreRuntimeBinding,
    ) -> MultipartPageReader:
        return MultipartPageReader(self, placement, binding)

    def upload(
        self,
        plan: KVCacheStoreUploadPlan,
        page_keys: Sequence[str],
        binding: KVCacheStoreRuntimeBinding,
        *,
        operation_id: str,
        cancelled: Callable[[], bool] | None = None,
    ) -> tuple[bool, ...]:
        page_keys = require_manifest_items(page_keys, "page_keys", str)
        for key in page_keys:
            require_nonempty_string(key, "page_key")
        if plan.layout != self.manifest.layout:
            raise ValueError("multipart upload layout differs")
        if not page_keys:
            return ()
        if cancelled is not None and cancelled():
            return (False,) * len(page_keys)
        owned = [
            i
            for i, writer in enumerate(plan.part_writers)
            if writer == binding.participant_id
        ]
        if len(owned) != 1:
            raise ValueError("multipart upload requires one owned K/V shard")
        fragments = tuple(f for f in plan.layout.fragments if f.part_index in owned)
        records = lower_store_ranges(
            plan.source_placement,
            binding,
            (fragments,) * len(page_keys),
            operation_id,
            self.transfer_limits,
        )
        regions = {r.region_id: r for r in binding.regions}
        grouped: list[list[StoreByteRange]] = [[] for _ in page_keys]
        for record in records:
            grouped[record.page_index].append(record)
        pointers: list[list[int]] = []
        lengths: list[list[int]] = []
        for row in grouped:
            row.sort(key=lambda r: (r.part_index, r.part_offset))
            for obj in owned:
                cursor = 0
                for record in row:
                    if record.part_index == obj:
                        if record.part_offset != cursor:
                            raise ValueError("multipart upload byte gap")
                        cursor += record.nbytes
                if cursor != plan.layout.part_sizes[obj]:
                    raise ValueError("multipart upload size differs")
            pointers.append(
                [regions[r.region_id].address + r.region_offset for r in row]
            )
            lengths.append([r.nbytes for r in row])
        return self.backend.put_parts(
            page_keys,
            owned[0],
            len(plan.layout.shards),
            self.manifest.manifest_key,
            pointers,
            lengths,
        )
