"""Per-participant address-free KV-cache placement contribution."""

from __future__ import annotations

from dataclasses import dataclass

from ..contracts import (
    ParticipantId,
    PlacementSetId,
    ResourceId,
    RevisionId,
    TopologyId,
)
from .types import (
    KVCacheDescriptor,
    KVCacheRank,
    require_integer,
    require_integer_tuple,
    require_nonempty_string,
)


@dataclass(frozen=True)
class KVCachePlacementPart:
    """One selected worker's logical layer/head ownership."""

    resource_id: ResourceId
    revision: RevisionId
    placement_set_id: PlacementSetId
    topology_id: TopologyId
    participant_id: ParticipantId
    rank: KVCacheRank
    descriptor: KVCacheDescriptor
    layer_ids: tuple[int, ...]
    head_start: int
    head_count: int
    replica_ordinal: int = 0
    replica_count: int = 1

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "revision",
            "placement_set_id",
            "topology_id",
            "participant_id",
        ):
            require_nonempty_string(getattr(self, name), name)
        if not isinstance(self.rank, KVCacheRank):
            raise ValueError("placement part rank must be a KVCacheRank")  # noqa: TRY004
        if not isinstance(self.descriptor, KVCacheDescriptor):
            raise ValueError(  # noqa: TRY004
                "placement part descriptor must be a KVCacheDescriptor"
            )
        layers = require_integer_tuple(self.layer_ids, "layer_ids")
        if len(layers) != len(set(layers)):
            raise ValueError("layer_ids must be unique")
        if not set(layers).issubset(self.descriptor.global_layer_ids):
            raise ValueError("placement part contains an unknown global layer")
        object.__setattr__(self, "layer_ids", tuple(sorted(layers)))
        require_integer(self.head_start, "head_start")
        require_integer(self.head_count, "head_count", minimum=1)
        require_integer(self.replica_ordinal, "replica_ordinal")
        require_integer(self.replica_count, "replica_count", minimum=1)
        if self.head_start + self.head_count > self.descriptor.total_kv_heads:
            raise ValueError("head interval exceeds total_kv_heads")
        if self.replica_ordinal >= self.replica_count:
            raise ValueError("replica_ordinal must be smaller than replica_count")


__all__ = ["KVCachePlacementPart"]
