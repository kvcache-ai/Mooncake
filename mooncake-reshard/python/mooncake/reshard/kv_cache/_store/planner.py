"""Fixed-page Store geometry planning; no snapshots, runtime addresses or I/O."""

from __future__ import annotations

from dataclasses import dataclass, replace

from ...contracts import ParticipantId
from ..part import KVCachePlacementPart
from ..placement import KVCachePlacementManifest
from ..types import KVCacheComponent, require_integer, require_nonempty_string
from ._utils import HeadIntervalIndex
from ._utils import canonical_digest as _canonical_digest
from .manifest import (
    KVCacheStoreFormat,
    KVCacheStoreFragment,
    KVCacheStoreLayout,
    KVCacheStoreShard,
)


@dataclass(frozen=True)
class KVCacheStorePlanningLimits:
    """Bound metadata and useful bytes before materializing a transfer plan."""

    max_parts: int = 100_000
    max_fragments: int = 100_000
    max_bytes: int = 1 << 40

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            require_integer(getattr(self, name), name, minimum=1)
        if max(self.max_parts, self.max_fragments) > 100_000:
            raise ValueError("planning limits exceed the contract collection limit")
        if self.max_bytes > 1 << 40:
            raise ValueError("planning byte limit exceeds the 1 TiB contract limit")


DEFAULT_STORE_PLANNING_LIMITS = KVCacheStorePlanningLimits()


def _selected_parts(
    placement: KVCachePlacementManifest,
    dp_rank: int | None,
    *,
    replicas: bool,
) -> tuple[int, tuple[KVCachePlacementPart, ...]]:
    """Upload selects one head replica; load fills every declared target replica."""
    if not isinstance(placement, KVCachePlacementManifest):
        raise ValueError("placement must be a KVCachePlacementManifest")  # noqa: TRY004
    selected = min(placement.dp_ranks) if dp_rank is None else dp_rank
    require_integer(selected, "dp_rank")
    if selected not in placement.dp_ranks:
        raise ValueError("DP rank is absent from placement")
    return selected, tuple(
        part
        for part in placement.parts
        if part.rank.dp == selected
        and part.layer_ids
        and (replicas or part.replica_ordinal == 0)
    )


def _check_limits(
    layout: KVCacheStoreLayout, limits: KVCacheStorePlanningLimits
) -> None:
    if not isinstance(limits, KVCacheStorePlanningLimits):
        raise ValueError("limits must be KVCacheStorePlanningLimits")  # noqa: TRY004
    if len(layout.shards) > limits.max_parts:
        raise ValueError("Store part limit exceeded")
    if sum(len(s.layer_ids) * 2 for s in layout.shards) > limits.max_fragments:
        raise ValueError("Store fragment limit exceeded")
    if sum(layout.part_sizes) > limits.max_bytes:
        raise ValueError("Store byte limit exceeded")


@dataclass(frozen=True, init=False)
class KVCacheStoreUploadPlan:
    """Source-native layout plus one complete writer per Part; no reshard target."""

    source_placement: KVCachePlacementManifest
    source_dp_rank: int
    layout: KVCacheStoreLayout
    part_writers: tuple[ParticipantId, ...]

    def __init__(
        self,
        source_placement: KVCachePlacementManifest,
        *,
        object_format: KVCacheStoreFormat,
        source_dp_rank: int | None = None,
        limits: KVCacheStorePlanningLimits = DEFAULT_STORE_PLANNING_LIMITS,
    ) -> None:
        selected, parts = _selected_parts(
            source_placement, source_dp_rank, replicas=False
        )
        parts = tuple(sorted(parts, key=lambda p: (p.layer_ids, p.head_start)))
        layout = KVCacheStoreLayout(
            source_placement.descriptor,
            object_format,
            tuple(
                KVCacheStoreShard(
                    p.layer_ids,
                    p.head_start,
                    p.head_count,
                    f"{p.rank.tp}_{p.rank.pp}"
                    if source_placement.topology.pp_size > 1
                    else str(p.rank.tp),
                )
                for p in parts
            ),
        )
        _check_limits(layout, limits)
        object.__setattr__(self, "source_placement", source_placement)
        object.__setattr__(self, "source_dp_rank", selected)
        object.__setattr__(self, "layout", layout)
        object.__setattr__(
            self,
            "part_writers",
            tuple(p.participant_id for p in parts),
        )


@dataclass(frozen=True)
class KVCacheStoreLoadRange:
    """One target participant's logical range mapped to relative Store bytes."""

    participant_id: ParticipantId
    fragment: KVCacheStoreFragment

    def __post_init__(self) -> None:
        require_nonempty_string(self.participant_id, "participant_id")
        if not isinstance(self.fragment, KVCacheStoreFragment):
            raise ValueError("fragment must be a KVCacheStoreFragment")  # noqa: TRY004


@dataclass(frozen=True, init=False)
class KVCacheStoreLoadPlan:
    """Derived read geometry for one complete target DP replica; no page or address."""

    target_placement: KVCachePlacementManifest
    layout: KVCacheStoreLayout
    target_dp_rank: int
    ranges: tuple[KVCacheStoreLoadRange, ...]

    def __init__(
        self,
        layout: KVCacheStoreLayout,
        target_placement: KVCachePlacementManifest,
        *,
        target_dp_rank: int | None = None,
        limits: KVCacheStorePlanningLimits = DEFAULT_STORE_PLANNING_LIMITS,
    ) -> None:
        if not isinstance(layout, KVCacheStoreLayout):
            raise ValueError("layout must be a KVCacheStoreLayout")  # noqa: TRY004
        # Source fragments retain their stored strides. Destination strides
        # come from the validated runtime binding during range lowering.
        selected, parts = _selected_parts(
            target_placement, target_dp_rank, replicas=True
        )
        if target_placement.descriptor != layout.descriptor:
            raise ValueError(
                "runtime and Store descriptors differ; conversion is unsupported"
            )
        _check_limits(layout, limits)
        ranges = _load_ranges(parts, layout, limits)
        object.__setattr__(self, "target_placement", target_placement)
        object.__setattr__(self, "layout", layout)
        object.__setattr__(self, "target_dp_rank", selected)
        object.__setattr__(self, "ranges", ranges)

    @property
    def digest(self) -> str:
        """Identify a reusable layout-to-target template, not a read operation."""
        return _canonical_digest(
            {
                "placement_id": self.target_placement.placement_id,
                "placement_digest": self.target_placement.digest,
                "layout_id": self.layout.layout_id,
                "target_dp_rank": self.target_dp_rank,
            }
        )


def _load_ranges(
    parts: tuple[KVCachePlacementPart, ...],
    layout: KVCacheStoreLayout,
    limits: KVCacheStorePlanningLimits,
) -> tuple[KVCacheStoreLoadRange, ...]:
    """Join sorted head intervals per layer/component, without a Cartesian scan."""
    if sum(len(p.layer_ids) * 2 for p in parts) > limits.max_fragments:
        raise ValueError("runtime fragment limit exceeded")
    inventory: dict[tuple[int, KVCacheComponent], list[KVCacheStoreFragment]] = {}
    for fragment in layout.fragments:
        inventory.setdefault((fragment.global_layer_id, fragment.component), []).append(
            fragment
        )
    indexes = {
        key: HeadIntervalIndex(
            (fragment.head_start, fragment.head_start + fragment.head_count, fragment)
            for fragment in fragments
        )
        for key, fragments in inventory.items()
    }
    result: list[KVCacheStoreLoadRange] = []
    useful_bytes = 0
    for part in parts:
        end = part.head_start + part.head_count
        for layer in part.layer_ids:
            for component in KVCacheComponent:
                key = (layer, component)
                for start, stop, candidates in indexes[key].cover(part.head_start, end):
                    (fragment,) = candidates
                    if len(result) >= limits.max_fragments:
                        raise ValueError("transfer range limit exceeded")
                    dim = (
                        layout.descriptor.key_head_dim
                        if component is KVCacheComponent.KEY
                        else layout.descriptor.value_head_dim
                    )
                    useful_bytes += (
                        (stop - start)
                        * layout.token_count
                        * dim
                        * layout.descriptor.itemsize
                    )
                    if useful_bytes > limits.max_bytes:
                        raise ValueError("transfer byte limit exceeded")
                    result.append(
                        KVCacheStoreLoadRange(
                            part.participant_id,
                            replace(
                                fragment,
                                head_start=start,
                                head_count=stop - start,
                                part_offset=fragment.part_offset
                                + (start - fragment.head_start)
                                * fragment.head_stride_bytes,
                            ),
                        )
                    )
    return tuple(result)


def plan_kv_cache_store_upload(
    source_placement: KVCachePlacementManifest,
    *,
    object_format: KVCacheStoreFormat,
    source_dp_rank: int | None = None,
    limits: KVCacheStorePlanningLimits = DEFAULT_STORE_PLANNING_LIMITS,
) -> KVCacheStoreUploadPlan:
    """Build this server's own layout and complete object writers, without Store I/O."""
    return KVCacheStoreUploadPlan(
        source_placement,
        object_format=object_format,
        source_dp_rank=source_dp_rank,
        limits=limits,
    )


def plan_kv_cache_store_load(
    layout: KVCacheStoreLayout,
    target_placement: KVCachePlacementManifest,
    *,
    target_dp_rank: int | None = None,
    limits: KVCacheStorePlanningLimits = DEFAULT_STORE_PLANNING_LIMITS,
) -> KVCacheStoreLoadPlan:
    """Plan reads from one selected source layout into the target placement."""
    return KVCacheStoreLoadPlan(
        layout,
        target_placement,
        target_dp_rank=target_dp_rank,
        limits=limits,
    )


__all__ = [
    "KVCacheStoreLoadPlan",
    "KVCacheStoreLoadRange",
    "KVCacheStorePlanningLimits",
    "KVCacheStoreUploadPlan",
    "plan_kv_cache_store_load",
    "plan_kv_cache_store_upload",
]
