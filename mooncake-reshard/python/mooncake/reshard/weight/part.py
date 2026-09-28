"""Per-participant logical weight placement contracts."""

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
    ParallelRank,
    PlacementFragment,
    TensorDescriptor,
    _require_nonempty_string,
    _require_u64,
    require_manifest_items,
)
from .validation import _validate_fragments


@dataclass(frozen=True)
class WeightPlacementPart:
    """One participant's address-free contribution to a global placement."""

    resource_id: ResourceId
    revision: RevisionId
    weight_generation: int
    placement_set_id: PlacementSetId
    topology_id: TopologyId
    participant_id: ParticipantId
    rank: ParallelRank
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[PlacementFragment, ...]

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "revision",
            "placement_set_id",
            "topology_id",
            "participant_id",
        ):
            _require_nonempty_string(getattr(self, name), name)
        _require_u64(self.weight_generation, "weight_generation")
        if not isinstance(self.rank, ParallelRank):
            raise ValueError("placement part rank must be a ParallelRank")  # noqa: TRY004

        tensors = require_manifest_items(
            self.tensors,
            "WeightPlacementPart tensors",
            TensorDescriptor,
        )
        fragments = require_manifest_items(
            self.fragments,
            "WeightPlacementPart fragments",
            PlacementFragment,
        )
        if any(fragment.rank != self.rank for fragment in fragments):
            raise ValueError("placement part fragment rank differs from part rank")
        referenced_tensor_ids = {fragment.tensor_id for fragment in fragments}
        unreferenced_tensor_ids = sorted(
            {tensor.tensor_id for tensor in tensors} - referenced_tensor_ids
        )
        if unreferenced_tensor_ids:
            raise ValueError(
                "placement part contains an unreferenced tensor: "
                f"{unreferenced_tensor_ids[0]}"
            )
        object.__setattr__(
            self,
            "tensors",
            tuple(sorted(tensors, key=lambda item: item.tensor_id)),
        )
        object.__setattr__(
            self,
            "fragments",
            tuple(sorted(fragments, key=lambda item: item.placement_fragment_id)),
        )
        _validate_fragments(self.tensors, self.fragments)


__all__ = ["WeightPlacementPart"]
