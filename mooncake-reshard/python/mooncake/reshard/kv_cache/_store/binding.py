"""Store-specific local binding, independent of the R2R snapshot lifecycle."""

from __future__ import annotations

from dataclasses import dataclass

from ...contracts import ParticipantId
from ..placement import KVCachePlacementManifest
from ..resolved import (
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheTransferLimits,
    _check_disjoint,
    _head_bytes,
    _physical_spans,
    _range_key,
)
from ..types import (
    KVCacheComponent,
    require_integer,
    require_manifest_items,
    require_nonempty_string,
    require_sha256,
)


@dataclass(frozen=True)
class KVCacheStoreRuntimeBinding:
    """Operation-scoped local memory for Store I/O; no R2R snapshot identity."""

    operation_id: str
    resource_id: str
    placement_id: str
    placement_digest: str
    instance_id: str
    revision: str
    participant_id: str
    regions: tuple[KVCacheRegisteredRegion, ...]
    ranges: tuple[KVCacheResolvedRange, ...]

    def __post_init__(self) -> None:
        for name in (
            "operation_id",
            "resource_id",
            "placement_id",
            "instance_id",
            "revision",
            "participant_id",
        ):
            require_nonempty_string(getattr(self, name), name)
        require_sha256(self.placement_digest, "placement_digest")
        regions = require_manifest_items(
            self.regions, "registered regions", KVCacheRegisteredRegion
        )
        ranges = require_manifest_items(
            self.ranges, "resolved ranges", KVCacheResolvedRange
        )
        if not regions or not ranges:
            raise ValueError("resolved binding must contain regions and ranges")
        if len({r.region_id for r in regions}) != len(regions):
            raise ValueError("duplicate registered region_id")
        if len({r.endpoint for r in regions}) != 1:
            raise ValueError("one runtime binding must use a single endpoint")
        object.__setattr__(
            self, "regions", tuple(sorted(regions, key=lambda r: r.region_id))
        )
        object.__setattr__(self, "ranges", tuple(sorted(ranges, key=_range_key)))


def validate_store_binding(
    placement: KVCachePlacementManifest,
    binding: KVCacheStoreRuntimeBinding,
    operation_id: str,
    token_start: int,
    token_count: int,
    limits: KVCacheTransferLimits,
) -> list[tuple[int, int]]:
    """Validate Store page coverage, ownership, strides and registered bounds."""
    if not isinstance(binding, KVCacheStoreRuntimeBinding):
        raise TypeError("binding must be a KVCacheStoreRuntimeBinding")
    if not isinstance(placement, KVCachePlacementManifest):
        raise TypeError("placement must be a KVCachePlacementManifest")
    if not isinstance(limits, KVCacheTransferLimits):
        raise TypeError("limits must be KVCacheTransferLimits")
    require_nonempty_string(operation_id, "operation_id")
    require_integer(token_start, "token_start")
    require_integer(token_count, "token_count", minimum=1)
    token_end = require_integer(token_start + token_count, "token_end")
    expected = {
        "operation_id": operation_id,
        "resource_id": placement.resource_id,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "revision": placement.revision,
    }
    for name, value in expected.items():
        if getattr(binding, name) != value:
            raise ValueError(f"resolved binding {name} differs")
    if len(binding.ranges) > limits.max_ranges:
        raise ValueError("resolved range count limit exceeded")
    part = placement.part(ParticipantId(binding.participant_id))
    regions = {r.region_id: r for r in binding.regions}
    groups: dict[tuple[int, KVCacheComponent], list[KVCacheResolvedRange]] = {}
    spans: list[tuple[int, int]] = []
    for item in binding.ranges:
        if item.global_layer_id not in part.layer_ids:
            raise ValueError("resolved range contains an unowned layer")
        if not token_start <= item.token_start < item.token_end <= token_end:
            raise ValueError("resolved range is outside operation token interval")
        if (
            not part.head_start
            <= item.head_start
            < item.head_end
            <= part.head_start + part.head_count
        ):
            raise ValueError("resolved range contains unowned heads")
        if item.region_id not in regions:
            raise ValueError("resolved range references an unknown registered region")
        region = regions[item.region_id]
        head_bytes = _head_bytes(placement, item.component)
        row_span = (item.head_count - 1) * item.head_stride_bytes + head_bytes
        if item.head_stride_bytes < head_bytes or item.token_stride_bytes < row_span:
            raise ValueError("resolved range strides overlap logical values")
        end = (
            item.offset_bytes
            + (item.token_count - 1) * item.token_stride_bytes
            + row_span
        )
        require_integer(end, "resolved byte end")
        if end > region.nbytes:
            raise ValueError("resolved range exceeds registered region bounds")
        if any(
            v % placement.descriptor.itemsize
            for v in (
                region.address + item.offset_bytes,
                item.token_stride_bytes,
                item.head_stride_bytes,
            )
        ):
            raise ValueError(
                "resolved range addresses and strides must be item aligned"
            )
        spans.extend(
            _physical_spans(
                item, region, head_bytes, limits.max_operations - len(spans)
            )
        )
        groups.setdefault((item.global_layer_id, item.component), []).append(item)
    _check_disjoint(spans, "binding")
    expected_keys = {
        (layer, component) for layer in part.layer_ids for component in KVCacheComponent
    }
    if set(groups) != expected_keys:
        raise ValueError("resolved binding misses layer/component coverage")
    work = 0
    for items in groups.values():
        boundaries = sorted(
            {token_start, token_end}
            | {t for r in items for t in (r.token_start, r.token_end)}
        )
        work += len(boundaries) * len(items)
        if work > limits.max_validation_work:
            raise ValueError("resolved coverage validation work limit exceeded")
        for token in boundaries[:-1]:
            intervals = sorted(
                (r.head_start, r.head_end)
                for r in items
                if r.token_start <= token < r.token_end
            )
            head = part.head_start
            for start, stop in intervals:
                if start != head:
                    raise ValueError("resolved logical coverage has a gap or overlap")
                head = stop
            if head != part.head_start + part.head_count:
                raise ValueError("resolved logical coverage is incomplete")
    return spans
