"""Stable public facade for model-weight manifest contracts."""

from .binding import validate_runtime_binding, validate_runtime_bindings
from .part import WeightPlacementPart
from .placement import WeightPlacementManifest
from .runtime import WeightRuntimeBindingManifest
from .topology import ParallelTopology, TopologyParticipant
from .types import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
)


__all__ = [
    "ParallelRank",
    "ParallelTopology",
    "PlacementFragment",
    "TopologyParticipant",
    "WeightPlacementManifest",
    "WeightPlacementPart",
    "RuntimeBindingFragment",
    "WeightRuntimeBindingManifest",
    "SplitAxis",
    "ReplicatedAxis",
    "OwnershipAxis",
    "TensorDescriptor",
    "validate_runtime_binding",
    "validate_runtime_bindings",
]
