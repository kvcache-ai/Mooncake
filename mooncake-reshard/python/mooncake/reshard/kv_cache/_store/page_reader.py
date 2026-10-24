"""Compile address-free Part ranges once; bind fresh page destinations per load."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING

from ...contracts import ParticipantId
from ..placement import KVCachePlacementManifest
from ..resolved import (
    _check_disjoint,
    _head_bytes,
    _physical_spans,
)
from ..types import require_integer
from ._utils import iter_batches
from .backend import KVCacheStoreError
from .binding import KVCacheStoreRuntimeBinding, validate_store_binding
from .manifest import KVCacheStoreManifest
from .planner import plan_kv_cache_store_load
from .runtime import StoreByteRange, lower_store_ranges, split_store_range_batches

if TYPE_CHECKING:
    from .multipart import MultipartKVCacheStore, MultipartReadContext


@dataclass(frozen=True)
class _PartRead:
    part_index: int
    region_id: str
    sizes: tuple[int, ...]
    native_template: object


class MultipartPageReader:
    """Per-rank cache of geometry, never of live replica addresses or leases."""

    def __init__(
        self,
        store: MultipartKVCacheStore,
        placement: KVCachePlacementManifest,
        binding: KVCacheStoreRuntimeBinding,
    ) -> None:
        validate_store_binding(
            placement,
            binding,
            binding.operation_id,
            0,
            placement.descriptor.page_size,
            store.transfer_limits,
        )
        self.store, self.placement, self.binding = store, placement, binding
        self.model_domain = store.manifest.model_domain
        self.regions = {r.region_id: r for r in binding.regions}
        if (
            store.transfer_limits.max_batch_operations > 100_000
            or store.transfer_limits.max_batch_bytes > 1 << 40
            or len(self.regions) > 100_000
        ):
            raise KVCacheStoreError("native multipart template limits exceeded")
        self._templates: OrderedDict[str, tuple[_PartRead, ...]] = OrderedDict()
        self._lock = RLock()
        footprints: dict[str, list[tuple[int, int]]] = {key: [] for key in self.regions}
        for item in binding.ranges:
            region = self.regions[item.region_id]
            footprints[item.region_id].extend(
                (start - region.address, end - region.address)
                for start, end in _physical_spans(
                    item,
                    region,
                    _head_bytes(placement, item.component),
                    store.transfer_limits.max_operations,
                )
            )
        self.footprints: dict[str, tuple[tuple[int, int], ...]] = {}
        for key, spans in footprints.items():
            merged: list[tuple[int, int]] = []
            for start, end in sorted(spans):
                if merged and merged[-1][1] == start:
                    merged[-1] = (merged[-1][0], end)
                else:
                    merged.append((start, end))
            self.footprints[key] = tuple(merged)

    def _compile(self, manifest: KVCacheStoreManifest) -> tuple[_PartRead, ...]:
        participant = self.placement.part(ParticipantId(self.binding.participant_id))
        plan = plan_kv_cache_store_load(
            manifest.layout,
            self.placement,
            target_dp_rank=participant.rank.dp,
            limits=self.store.planning_limits,
        )
        fragments = tuple(
            r.fragment
            for r in plan.ranges
            if r.participant_id == self.binding.participant_id
        )
        records = lower_store_ranges(
            self.placement,
            self.binding,
            (fragments,),
            self.binding.operation_id,
            self.store.transfer_limits,
        )
        grouped: dict[tuple[int, str], list[StoreByteRange]] = {}
        for record in records:
            grouped.setdefault((record.part_index, record.region_id), []).append(record)
        templates: list[_PartRead] = []
        for (part, region), spans in grouped.items():
            for chunk in split_store_range_batches(spans, self.store.transfer_limits):
                dst = tuple(r.region_offset + offset for r, offset, _ in chunk)
                src = tuple(r.part_offset + offset for r, offset, _ in chunk)
                sizes = tuple(size for _, _, size in chunk)
                templates.append(
                    _PartRead(
                        part,
                        region,
                        sizes,
                        self.store.backend.prepare_template(dst, src, sizes),
                    )
                )
        return tuple(templates)

    def _validate_offsets(self, offsets: Sequence[Mapping[str, int]]) -> None:
        spans: list[tuple[int, int]] = []
        limits = self.store.transfer_limits
        for translations in offsets:
            if set(translations) != set(self.regions):
                raise ValueError("page translations differ from registered regions")
            for key, region in self.regions.items():
                delta = translations[key]
                require_integer(delta, "page byte translation")
                if delta % self.placement.descriptor.itemsize:
                    raise ValueError("page translation is not item aligned")
                for start, end in self.footprints[key]:
                    if end + delta > region.nbytes:
                        raise ValueError("page exceeds registered region bounds")
                    if len(spans) >= limits.max_operations:
                        raise ValueError("Store physical operation limit exceeded")
                    spans.append(
                        (region.address + start + delta, region.address + end + delta)
                    )
        _check_disjoint(spans, "page destinations")

    def load(
        self,
        context: MultipartReadContext,
        page_offsets: Sequence[Mapping[str, int]],
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> int:
        if context.model_domain != self.model_domain:
            raise ValueError("read context model domain differs")
        offsets = tuple(page_offsets)
        if len(offsets) != len(context.page_keys):
            raise ValueError("page translations and read context differ")
        if not offsets or (cancelled is not None and cancelled()):
            return 0
        self._validate_offsets(offsets)
        templates: dict[str, tuple[_PartRead, ...]] = {}
        with self._lock:
            for manifest in context.manifests:
                layout_id = manifest.layout.layout_id
                if layout_id not in self._templates:
                    self._templates[layout_id] = self._compile(manifest)
                self._templates.move_to_end(layout_id)
                templates[layout_id] = self._templates[layout_id]
                while len(self._templates) > self.store.cache_capacity:
                    self._templates.popitem(last=False)
        count = sum(len(r.sizes) for i in context.layout_ids for r in templates[i])
        nbytes = sum(sum(r.sizes) for i in context.layout_ids for r in templates[i])
        limits = self.store.transfer_limits
        if count > limits.max_operations or nbytes > limits.max_bytes:
            raise ValueError("Store transfer limit exceeded")
        read = self.store.backend.ranged_reader(context)
        buffers = [r.address for r in self.regions.values()]
        region_indices = {name: i for i, name in enumerate(self.regions)}
        success = [True] * len(offsets)
        remaining = [len(templates[i]) for i in context.layout_ids]
        bound = (
            (page, key, translations[t.region_id], t)
            for page, (key, layout_id, translations) in enumerate(
                zip(context.page_keys, context.layout_ids, offsets)
            )
            for t in templates[layout_id]
        )
        for batch in iter_batches(
            bound,
            cost=lambda row: (len(row[3].sizes), sum(row[3].sizes)),
            max_operations=limits.max_batch_operations,
            max_bytes=limits.max_batch_bytes,
        ):
            if cancelled is not None and cancelled():
                return 0
            # Aliases address Parts inside this read snapshot; no hidden Store keys.
            results = read(
                [t.native_template for _, _, _, t in batch],
                buffers,
                [region_indices[t.region_id] for _, _, _, t in batch],
                [key + f"\x1fp{t.part_index}" for _, key, _, t in batch],
                [delta for _, _, delta, _ in batch],
            )
            if len(results) != len(batch) or any(type(v) is not bool for v in results):
                raise KVCacheStoreError("native template read result shape differs")
            for (page, _, _, _), complete in zip(batch, results):
                success[page] &= complete
                remaining[page] -= 1
        if cancelled is not None and cancelled():
            return 0
        return next(
            (i for i, pending in enumerate(remaining) if pending or not success[i]),
            len(success),
        )
