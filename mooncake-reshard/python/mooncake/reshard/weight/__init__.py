"""Public contracts for framework-neutral model-weight resharding."""

from .manifest import (
    OwnershipAxis,
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    ReplicatedAxis,
    RuntimeBindingFragment,
    SplitAxis,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
    validate_runtime_binding,
    validate_runtime_bindings,
)
from .serde import weight_placement_from_json, weight_placement_to_json

__all__ = [
    "ParallelRank",
    "ParallelTopology",
    "PlacementFragment",
    "WeightPlacementManifest",
    "WeightPlacementPart",
    "RuntimeBindingFragment",
    "WeightRuntimeBindingManifest",
    "SplitAxis",
    "ReplicatedAxis",
    "OwnershipAxis",
    "TensorDescriptor",
    "TopologyParticipant",
    "validate_runtime_binding",
    "validate_runtime_bindings",
    "weight_placement_from_json",
    "weight_placement_to_json",
]
