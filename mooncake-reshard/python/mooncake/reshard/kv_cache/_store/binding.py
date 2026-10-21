"""Store-specific local binding, independent of the R2R snapshot lifecycle."""

from __future__ import annotations

from dataclasses import dataclass

from ..placement import KVCachePlacementManifest
from ..resolved import (
    KVCacheRegisteredRegion,
    KVCacheResolvedRange,
    KVCacheTransferLimits,
    _range_key,
    _validated_range_spans,
)
from ..types import (
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
    return _validated_range_spans(
        placement,
        binding.participant_id,
        binding.regions,
        binding.ranges,
        token_start=token_start,
        token_end=token_end,
        limits=limits,
    )
