"""Address-free planning for COO sparse structured objects.

The sparse planner operates on logical tensor boxes and compact COO tile
indexes.  It deliberately has no dependency on Mooncake Store, transports,
RPC clients, or framework objects.  Store integrations can consume the
returned plans and decide how to execute the requested member ranges.
"""

from .planner import (
    Box,
    Placement,
    placement_matches,
    SparseObjectIndex,
    SparseObjectPlan,
    SparseObjectRegion,
    SparseObjectStorePlanner,
    canonical_source_placement,
    is_canonical_source,
    normalize_placement,
    normalize_shape,
    object_ref_key,
    plan_sparse_object_target,
)

__all__ = [
    "Box",
    "Placement",
    "placement_matches",
    "SparseObjectIndex",
    "SparseObjectPlan",
    "SparseObjectRegion",
    "SparseObjectStorePlanner",
    "canonical_source_placement",
    "is_canonical_source",
    "normalize_placement",
    "normalize_shape",
    "object_ref_key",
    "plan_sparse_object_target",
]
