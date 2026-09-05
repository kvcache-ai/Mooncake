"""Source contracts for one immutable model-weight snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, Sequence

from ...contracts import PlacementFragmentId, ResourceId, RevisionId
from ..manifest import WeightPlacementManifest, WeightRuntimeBindingManifest

if TYPE_CHECKING:
    from ..lifetime import WeightAllocationGuardProviders


def _require_nonempty_string(value: object, name: str) -> None:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _require_u64(value: object, name: str) -> None:
    if type(value) is not int or value < 0 or value >= (1 << 64):
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")


@dataclass(frozen=True)
class WeightSnapshotDescriptor:
    """Immutable identity supplied before a complete model snapshot is written."""

    resource_id: ResourceId
    revision: RevisionId
    weight_generation: int
    namespace: str = "default"

    def __post_init__(self) -> None:
        _require_nonempty_string(self.resource_id, "resource_id")
        _require_nonempty_string(self.revision, "revision")
        _require_nonempty_string(self.namespace, "namespace")
        _require_u64(self.weight_generation, "weight_generation")


class WeightSnapshotSource(Protocol):
    """Complete source inventory used to prepare one Store upload plan."""

    placement: WeightPlacementManifest
    bindings: Sequence[WeightRuntimeBindingManifest]


class WeightSnapshotAdapter(Protocol):
    """Framework-owned bridge from a tensor to canonical source fragments."""

    def export_source(self, snapshot: WeightSnapshotDescriptor) -> WeightSnapshotSource:
        """Return the complete source placement and live bindings for this snapshot."""
        ...

    def resolve_fragment_ids(
        self,
        *,
        tensor_id: str,
        tensor: object,
        source: WeightSnapshotSource,
    ) -> Sequence[PlacementFragmentId]:
        """Validate one tensor and return its canonical placement fragments."""
        ...

    def source_allocation_guards(
        self,
        binding: WeightRuntimeBindingManifest,
    ) -> WeightAllocationGuardProviders | None:
        """Return framework lifetime guards for a source binding."""
        ...


__all__ = ["WeightSnapshotAdapter", "WeightSnapshotDescriptor"]
