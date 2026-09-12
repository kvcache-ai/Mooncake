"""Operation-scoped, framework-neutral logical ranges in registered memory."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from ..contracts import ParticipantId
from .binding import validate_runtime_binding
from .placement import KVCachePlacementManifest
from .runtime import KVCacheRuntimeBindingManifest
from .snapshot import KVCacheSnapshotDescriptor, _canonical_digest
from .types import (
    KVCacheComponent,
    require_integer,
    require_manifest_items,
    require_nonempty_string,
    require_sha256,
)


@dataclass(frozen=True)
class KVCacheTransferLimits:
    max_ranges: int = 100_000
    max_operations: int = 1_000_000
    max_bytes: int = 1 << 40
    max_batch_operations: int = 4096
    max_batch_bytes: int = 64 << 20
    max_validation_work: int = 10_000_000

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            require_integer(getattr(self, name), name, minimum=1)


DEFAULT_TRANSFER_LIMITS = KVCacheTransferLimits()


@dataclass(frozen=True)
class KVCacheRegisteredRegion:
    """A runtime attestation of memory registered with the endpoint's TE.

    Registration, pinning and reservation must remain valid until all submitted
    work is quiescent. This descriptor does not own or register the allocation.
    """

    region_id: str
    endpoint: str
    address: int
    nbytes: int

    def __post_init__(self) -> None:
        require_nonempty_string(self.region_id, "region_id")
        require_nonempty_string(self.endpoint, "endpoint")
        require_integer(self.address, "region address", minimum=1)
        require_integer(self.nbytes, "region nbytes", minimum=1)
        require_integer(self.address + self.nbytes, "region address end")


@dataclass(frozen=True)
class KVCacheResolvedRange:
    global_layer_id: int
    component: KVCacheComponent
    token_start: int
    token_count: int
    head_start: int
    head_count: int
    region_id: str
    offset_bytes: int
    token_stride_bytes: int
    head_stride_bytes: int

    def __post_init__(self) -> None:
        for name in ("global_layer_id", "token_start", "head_start", "offset_bytes"):
            require_integer(getattr(self, name), name)
        for name in (
            "token_count",
            "head_count",
            "token_stride_bytes",
            "head_stride_bytes",
        ):
            require_integer(getattr(self, name), name, minimum=1)
        require_integer(self.token_end, "resolved token end")
        require_integer(self.head_end, "resolved head end")
        require_nonempty_string(self.region_id, "region_id")
        if not isinstance(self.component, KVCacheComponent):
            raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004

    @property
    def token_end(self) -> int:
        return self.token_start + self.token_count

    @property
    def head_end(self) -> int:
        return self.head_start + self.head_count

    def address(self, region: KVCacheRegisteredRegion, token: int, head: int) -> int:
        return (
            region.address
            + self.offset_bytes
            + (token - self.token_start) * self.token_stride_bytes
            + (head - self.head_start) * self.head_stride_bytes
        )


def _range_key(item: KVCacheResolvedRange) -> tuple[int, str, int, int, str, int]:
    return (
        item.global_layer_id,
        item.component.value,
        item.token_start,
        item.head_start,
        item.region_id,
        item.offset_bytes,
    )


@dataclass(frozen=True)
class KVCacheResolvedRuntimeBinding:
    operation_id: str
    resource_id: str
    placement_id: str
    placement_digest: str
    instance_id: str
    revision: str
    participant_id: str
    snapshot_id: str
    snapshot_digest: str
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
            "snapshot_id",
        ):
            require_nonempty_string(getattr(self, name), name)
        for name in ("placement_digest", "snapshot_digest"):
            require_sha256(getattr(self, name), name)
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

    @property
    def digest(self) -> str:
        return _canonical_digest(
            {"schema": "kv-cache-resolved-binding", **asdict(self)}
        )


def _head_bytes(
    placement: KVCachePlacementManifest, component: KVCacheComponent
) -> int:
    d = placement.descriptor
    return d.itemsize * (
        d.key_head_dim if component is KVCacheComponent.KEY else d.value_head_dim
    )


def _physical_spans(
    item: KVCacheResolvedRange,
    region: KVCacheRegisteredRegion,
    head_bytes: int,
    limit: int,
) -> list[tuple[int, int]]:
    if limit < 1:
        raise ValueError("resolved range physical expansion limit exceeded")
    base = region.address + item.offset_bytes
    if item.head_stride_bytes == head_bytes:
        row_bytes = item.head_count * head_bytes
        if item.token_stride_bytes == row_bytes:
            return [(base, base + item.token_count * row_bytes)]
        count = item.token_count
        width = row_bytes
    else:
        count = item.token_count * item.head_count
        width = head_bytes
    if count > limit:
        raise ValueError("resolved range physical expansion limit exceeded")
    if item.head_stride_bytes == head_bytes:
        return [
            (
                base + t * item.token_stride_bytes,
                base + t * item.token_stride_bytes + width,
            )
            for t in range(item.token_count)
        ]
    return [
        (item.address(region, t, h), item.address(region, t, h) + width)
        for t in range(item.token_start, item.token_end)
        for h in range(item.head_start, item.head_end)
    ]


def _check_disjoint(spans: list[tuple[int, int]], label: str) -> None:
    spans.sort()
    end = 0
    for start, stop in spans:
        if start < end:
            raise ValueError(f"{label} physical ranges overlap")
        end = stop


def validate_resolved_runtime_binding(
    placement: KVCachePlacementManifest,
    snapshot: KVCacheSnapshotDescriptor,
    binding: KVCacheResolvedRuntimeBinding,
    *,
    operation_id: str,
    limits: KVCacheTransferLimits = DEFAULT_TRANSFER_LIMITS,
) -> None:
    """Check identity, exact token/head coverage, strides and registered bounds."""
    _validated_spans(placement, snapshot, binding, operation_id, limits)


def _validated_spans(
    placement: KVCachePlacementManifest,
    snapshot: KVCacheSnapshotDescriptor,
    binding: KVCacheResolvedRuntimeBinding,
    operation_id: str,
    limits: KVCacheTransferLimits,
) -> list[tuple[int, int]]:
    if not isinstance(binding, KVCacheResolvedRuntimeBinding):
        raise TypeError("binding must be a KVCacheResolvedRuntimeBinding")
    if not isinstance(placement, KVCachePlacementManifest):
        raise TypeError("placement must be a KVCachePlacementManifest")
    if not isinstance(snapshot, KVCacheSnapshotDescriptor):
        raise TypeError("snapshot must be a KVCacheSnapshotDescriptor")
    if not isinstance(limits, KVCacheTransferLimits):
        raise TypeError("limits must be KVCacheTransferLimits")
    require_nonempty_string(operation_id, "operation_id")
    expected = {
        "operation_id": operation_id,
        "resource_id": placement.resource_id,
        "placement_id": placement.placement_id,
        "placement_digest": placement.digest,
        "revision": placement.revision,
        "snapshot_id": snapshot.snapshot_id,
        "snapshot_digest": snapshot.digest,
    }
    for name, value in expected.items():
        if getattr(binding, name) != value:
            raise ValueError(f"resolved binding {name} differs")
    if snapshot.resource_id != placement.resource_id:
        raise ValueError("snapshot resource_id differs from placement")
    if len(binding.ranges) > limits.max_ranges:
        raise ValueError("resolved range count limit exceeded")
    part = placement.part(ParticipantId(binding.participant_id))
    regions = {r.region_id: r for r in binding.regions}
    groups: dict[tuple[int, KVCacheComponent], list[KVCacheResolvedRange]] = {}
    spans: list[tuple[int, int]] = []
    for item in binding.ranges:
        if item.global_layer_id not in part.layer_ids:
            raise ValueError("resolved range contains an unowned layer")
        if (
            not snapshot.token_start
            <= item.token_start
            < item.token_end
            <= snapshot.token_end
        ):
            raise ValueError("resolved range is outside snapshot token interval")
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
            {snapshot.token_start, snapshot.token_end}
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


def resolve_contiguous_runtime_binding(
    placement: KVCachePlacementManifest,
    snapshot: KVCacheSnapshotDescriptor,
    binding: KVCacheRuntimeBindingManifest,
    *,
    operation_id: str,
    first_token_offset: int = 0,
) -> KVCacheResolvedRuntimeBinding:
    """Adapt legacy contiguous buffers; offsets are supplied by the runtime.

    Paged/noncontiguous allocations must export resolved ranges themselves.
    No framework page IDs or allocation metadata enter Mooncake.
    """
    validate_runtime_binding(placement, binding, snapshot=snapshot)
    require_integer(first_token_offset, "first_token_offset")
    part = placement.part(ParticipantId(binding.participant_id))
    regions: list[KVCacheRegisteredRegion] = []
    ranges: list[KVCacheResolvedRange] = []
    for item in binding.buffers:
        f = item.fragment
        if first_token_offset + snapshot.token_count > f.local_shape[0]:
            raise ValueError("snapshot exceeds legacy runtime buffer token capacity")
        regions.append(
            KVCacheRegisteredRegion(f.fragment_id, f.endpoint, f.address, f.nbytes)
        )
        ranges.append(
            KVCacheResolvedRange(
                item.global_layer_id,
                item.component,
                snapshot.token_start,
                snapshot.token_count,
                part.head_start,
                part.head_count,
                f.fragment_id,
                first_token_offset * f.strides_bytes[0],
                f.strides_bytes[0],
                f.strides_bytes[1],
            )
        )
    result = KVCacheResolvedRuntimeBinding(
        operation_id,
        binding.resource_id,
        binding.placement_id,
        binding.placement_digest,
        binding.instance_id,
        binding.revision,
        binding.participant_id,
        snapshot.snapshot_id,
        snapshot.digest,
        tuple(regions),
        tuple(ranges),
    )
    validate_resolved_runtime_binding(
        placement, snapshot, result, operation_id=operation_id
    )
    return result
