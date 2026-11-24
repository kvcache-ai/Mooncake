"""Lower fixed-page Store geometry into checked local memory byte ranges."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace

from ..placement import KVCachePlacementManifest
from ..resolved import (
    KVCacheTransferLimits,
    _head_bytes,
)
from ._utils import KVCacheRangeGeometry, index_runtime_ranges, plan_range_copy
from .binding import KVCacheStoreRuntimeBinding, validate_store_binding
from .manifest import KVCacheStoreFragment


@dataclass(frozen=True)
class StoreByteRange:
    page_index: int
    part_index: int
    part_offset: int
    region_id: str
    region_offset: int
    nbytes: int


def split_store_range_batches(
    records: Sequence[StoreByteRange], limits: KVCacheTransferLimits
) -> Iterator[list[tuple[StoreByteRange, int, int]]]:
    """Split ranges and fill byte/count budgets for direct and prepared reads."""
    batch: list[tuple[StoreByteRange, int, int]] = []
    used = 0
    for record in records:
        offset = 0
        while offset < record.nbytes:
            if (
                len(batch) == limits.max_batch_operations
                or used == limits.max_batch_bytes
            ):
                yield batch
                batch, used = [], 0
            size = min(record.nbytes - offset, limits.max_batch_bytes - used)
            batch.append((record, offset, size))
            used += size
            offset += size
    if batch:
        yield batch


def lower_store_ranges(
    placement: KVCachePlacementManifest,
    binding: KVCacheStoreRuntimeBinding,
    fragments: tuple[tuple[KVCacheStoreFragment, ...], ...],
    operation_id: str,
    limits: KVCacheTransferLimits,
) -> tuple[StoreByteRange, ...]:
    """Validate all runtime pages, then join each page's selected Store layout."""
    page_size = placement.descriptor.page_size
    validate_store_binding(
        placement, binding, operation_id, 0, len(fragments) * page_size, limits
    )
    by_layer = index_runtime_ranges(binding.ranges)
    records: list[StoreByteRange] = []
    useful_bytes = 0
    for page_index, page_fragments in enumerate(fragments):
        page_start = page_index * page_size
        for fragment in page_fragments:
            source = KVCacheRangeGeometry(
                page_start,
                page_size,
                fragment.head_start,
                fragment.head_count,
                fragment.part_offset,
                fragment.token_stride_bytes,
                fragment.head_stride_bytes,
            )
            head_bytes = _head_bytes(placement, fragment.component)
            for target in by_layer.get(
                (fragment.global_layer_id, fragment.component), []
            ):
                copy = plan_range_copy(source, target, head_bytes)
                if copy is None:
                    continue
                if len(records) + copy.segment_count > limits.max_operations:
                    raise ValueError("Store physical operation limit exceeded")
                useful_bytes += copy.nbytes
                if useful_bytes > limits.max_bytes:
                    raise ValueError("Store transfer byte limit exceeded")
                for source_offset, target_offset, nbytes in copy.segments():
                    records.append(
                        StoreByteRange(
                            page_index,
                            fragment.part_index,
                            source_offset,
                            target.region_id,
                            target_offset,
                            nbytes,
                        )
                    )
    records.sort(
        key=lambda r: (
            r.page_index,
            r.part_index,
            r.part_offset,
            r.region_id,
            r.region_offset,
        )
    )
    merged: list[StoreByteRange] = []
    for record in records:
        if merged:
            last = merged[-1]
            if (
                (last.page_index, last.part_index, last.region_id)
                == (record.page_index, record.part_index, record.region_id)
                and last.part_offset + last.nbytes == record.part_offset
                and last.region_offset + last.nbytes == record.region_offset
            ):
                merged[-1] = replace(last, nbytes=last.nbytes + record.nbytes)
                continue
        merged.append(record)
    return tuple(merged)
