from __future__ import annotations

from dataclasses import dataclass

import pytest

from mooncake.reshard.sparse import (
    SparseObjectIndex,
    SparseObjectStorePlanner,
)
from mooncake.reshard.weight import ReplicatedAxis, SplitAxis, TensorDescriptor


@dataclass(frozen=True)
class _Source:
    tensor_id: str
    global_shape: tuple[int, ...]
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    source_placement: tuple[tuple[str, int], ...]
    object_ref: str


@dataclass(frozen=True)
class _Target:
    tensor_id: str
    global_shape: tuple[int, ...]
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    target_placement: tuple[tuple[str, int], ...]


def _tensor() -> TensorDescriptor:
    return TensorDescriptor(
        tensor_id="layer.weight",
        global_shape=(8, 8),
        dtype="float32",
        itemsize=4,
        shard_dims=(1,),
        layout_fingerprint="test:layer.weight",
        parallel_axes=(ReplicatedAxis("ep"), SplitAxis("tp", 1)),
    )


def _index(
    object_ref: str,
    columns: tuple[int, ...],
    *,
    global_offset: tuple[int, ...] = (0, 0),
    local_shape: tuple[int, ...] = (8, 8),
) -> SparseObjectIndex:
    return SparseObjectIndex(
        object_ref=object_ref,
        tensor_id="layer.weight",
        global_shape=(8, 8),
        global_offset=global_offset,
        local_shape=local_shape,
        tile_shape=(4, 4),
        tile_coords=tuple((row, column) for row, column in columns),
        tile_ptr=tuple(range(len(columns) + 1)),
        nnz=len(columns),
        base_generation=1,
        delta_generation=2,
    )


def test_multiple_source_shards_are_kept_and_same_geometry_is_represented_once() -> (
    None
):
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("layer.weight", (8, 8), (0, 0), (8, 8), (("tp", 0),))
    left = _Source("layer.weight", (8, 8), (0, 0), (8, 4), (), "left")
    left_replica = _Source("layer.weight", (8, 8), (0, 0), (8, 4), (), "left-replica")
    right = _Source("layer.weight", (8, 8), (0, 4), (8, 4), (), "right")
    sources = (left_replica, right, left)
    selected = planner.select_source_fragments(
        tensor_id="layer.weight",
        target=target,
        source_fragments=sources,
        tensor=tensor,
    )
    assert {(item.global_offset, item.local_shape) for item in selected} == {
        ((0, 0), (8, 4)),
        ((0, 4), (8, 4)),
    }

    plan = planner.plan_target(
        tensor_id="layer.weight",
        tensor=tensor,
        target=target,
        source_fragments=sources,
        source_indexes={
            "left": _index("left", ((0, 0), (1, 0)), local_shape=(8, 4)),
            "left-replica": _index(
                "left-replica", ((0, 0), (1, 0)), local_shape=(8, 4)
            ),
            "right": _index(
                "right", ((0, 1), (1, 1)), global_offset=(0, 4), local_shape=(8, 4)
            ),
        },
        base_generation=1,
        delta_generation=2,
    )
    assert {region.source_object_ref for region in plan.regions} == {
        "left",
        "right",
    }


def test_partial_source_geometry_is_valid_for_a_target_shard() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("layer.weight", (8, 8), (0, 4), (8, 4), (("tp", 1),))
    source = _Source("layer.weight", (8, 8), (0, 4), (8, 4), (), "right")
    plan = planner.plan_target(
        tensor_id="layer.weight",
        tensor=tensor,
        target=target,
        source_fragments=(source,),
        source_indexes={
            "right": _index(
                "right", ((0, 1), (1, 1)), global_offset=(0, 4), local_shape=(8, 4)
            )
        },
        base_generation=1,
        delta_generation=2,
    )
    assert plan.range_request_count == 1


def test_target_coverage_rejects_gaps_and_overlaps() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("layer.weight", (8, 8), (0, 0), (8, 8), ())
    left = _Source("layer.weight", (8, 8), (0, 0), (8, 3), (), "left")
    right = _Source("layer.weight", (8, 8), (0, 4), (8, 4), (), "right")
    indexes = {
        "left": _index("left", ((0, 0),), local_shape=(8, 3)),
        "right": _index("right", ((0, 1),), global_offset=(0, 4), local_shape=(8, 4)),
    }
    with pytest.raises(ValueError, match="not fully covered"):
        planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(left, right),
            source_indexes=indexes,
            base_generation=1,
            delta_generation=2,
        )

    overlapping_right = _Source(
        "layer.weight", (8, 8), (0, 4), (8, 4), (), "overlapping-right"
    )
    overlapping_left = _Source(
        "layer.weight", (8, 8), (0, 0), (8, 5), (), "overlapping-left"
    )
    overlapping_indexes = {
        "overlapping-left": _index("overlapping-left", ((0, 0),), local_shape=(8, 5)),
        "overlapping-right": _index(
            "overlapping-right",
            ((0, 1),),
            global_offset=(0, 4),
            local_shape=(8, 4),
        ),
    }
    with pytest.raises(ValueError, match="not fully covered"):
        planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(overlapping_left, overlapping_right),
            source_indexes=overlapping_indexes,
            base_generation=1,
            delta_generation=2,
        )


def test_index_metadata_rejects_geometry_rank_mismatch() -> None:
    metadata = {
        "schema": "mooncake.sparse_object",
        "version": 1,
        "coordinate_space": "global",
        "tensor_id": "layer.weight",
        "global_shape": [8, 8],
        "global_offset": [0],
        "local_shape": [8, 8],
        "tile_shape": [4, 4],
        "base_generation": 1,
        "delta_generation": 2,
        "members": {
            "indices": {"dtype": "uint32", "shape": [1, 2]},
            "values": {"dtype": "float32", "shape": [1]},
            "tile_coords": {"dtype": "uint32", "shape": [1, 2]},
            "tile_ptr": {"dtype": "uint64", "shape": [2]},
        },
        "index_dtype": "uint32",
        "value_dtype": "float32",
    }
    with pytest.raises(ValueError, match="geometry rank"):
        SparseObjectIndex.from_metadata(
            object_ref="object",
            metadata=metadata,
            tile_coords=((0, 0),),
            tile_ptr=(0, 1),
        )


def test_index_metadata_rejects_inconsistent_index_dtype() -> None:
    metadata = {
        "schema": "mooncake.sparse_object",
        "version": 1,
        "coordinate_space": "global",
        "tensor_id": "layer.weight",
        "global_shape": [8, 8],
        "global_offset": [0, 0],
        "local_shape": [8, 8],
        "tile_shape": [4, 4],
        "base_generation": 1,
        "delta_generation": 2,
        "index_dtype": "float32",
        "value_dtype": "float32",
        "members": {
            "indices": {"dtype": "float32", "shape": [1, 2]},
            "values": {"dtype": "float32", "shape": [1]},
            "tile_coords": {"dtype": "uint32", "shape": [1, 2]},
            "tile_ptr": {"dtype": "uint64", "shape": [2]},
        },
    }
    with pytest.raises(ValueError, match="tile_coords member dtype"):
        SparseObjectIndex.from_metadata(
            object_ref="object",
            metadata=metadata,
            tile_coords=((0, 0),),
            tile_ptr=(0, 1),
        )


def test_generation_filter_selects_current_replica_before_object_order() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("layer.weight", (8, 8), (0, 0), (8, 8), ())
    stale = _Source("layer.weight", (8, 8), (0, 0), (8, 8), (), "aaa-stale")
    current = _Source("layer.weight", (8, 8), (0, 0), (8, 8), (), "zzz-current")
    stale_index = SparseObjectIndex(
        object_ref="aaa-stale",
        tensor_id="layer.weight",
        global_shape=(8, 8),
        tile_shape=(4, 4),
        tile_coords=((0, 0),),
        tile_ptr=(0, 1),
        nnz=1,
        base_generation=0,
        delta_generation=1,
    )
    current_index = _index("zzz-current", ((0, 0),))
    selected = planner.select_source_fragments(
        tensor_id="layer.weight",
        target=target,
        source_fragments=(stale, current),
        tensor=tensor,
        source_indexes={
            "aaa-stale": stale_index,
            "zzz-current": current_index,
        },
        base_generation=1,
        delta_generation=2,
    )
    assert selected == (current,)


def test_index_geometry_and_object_identity_are_checked() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("layer.weight", (8, 8), (0, 0), (8, 8), ())
    source = _Source("layer.weight", (8, 8), (0, 0), (8, 4), (), "source")
    mismatched_geometry = SparseObjectIndex(
        object_ref="source",
        tensor_id="layer.weight",
        global_shape=(8, 8),
        tile_shape=(4, 4),
        tile_coords=((0, 0),),
        tile_ptr=(0, 1),
        nnz=1,
        base_generation=1,
        delta_generation=2,
        global_offset=(0, 4),
        local_shape=(8, 4),
    )
    with pytest.raises(ValueError, match="global_offset"):
        planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(source,),
            source_indexes={"source": mismatched_geometry},
            base_generation=1,
            delta_generation=2,
        )

    wrong_object = _index("other", ((0, 0),), local_shape=(8, 4))
    with pytest.raises(ValueError, match="object_ref"):
        planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(source,),
            source_indexes={"source": wrong_object},
            base_generation=1,
            delta_generation=2,
        )


def test_target_tensor_id_must_match_explicit_tensor_id() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    target = _Target("other.weight", (8, 8), (0, 0), (8, 8), ())
    with pytest.raises(ValueError, match="target tensor_id"):
        planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(),
            source_indexes={},
            base_generation=1,
            delta_generation=2,
        )


def test_plan_targets_filters_stale_replicas_per_target_geometry() -> None:
    planner = SparseObjectStorePlanner()
    tensor = _tensor()
    left = _Source("layer.weight", (8, 8), (0, 0), (8, 4), (), "left")
    right = _Source("layer.weight", (8, 8), (0, 4), (8, 4), (), "right")
    stale_left = _Source("layer.weight", (8, 8), (0, 0), (8, 4), (), "stale-left")
    targets = (
        _Target("layer.weight", (8, 8), (0, 0), (8, 4), ()),
        _Target("layer.weight", (8, 8), (0, 4), (8, 4), ()),
    )
    indexes = {
        "left": _index("left", ((0, 0),), local_shape=(8, 4)),
        "right": _index("right", ((0, 1),), global_offset=(0, 4), local_shape=(8, 4)),
        "stale-left": SparseObjectIndex(
            object_ref="stale-left",
            tensor_id="layer.weight",
            global_shape=(8, 8),
            global_offset=(0, 0),
            local_shape=(8, 4),
            tile_shape=(4, 4),
            tile_coords=((0, 0),),
            tile_ptr=(0, 1),
            nnz=1,
            base_generation=0,
            delta_generation=1,
        ),
    }
    plans = planner.plan_targets(
        targets=targets,
        source_fragments=(stale_left, left, right),
        tensors={"layer.weight": tensor},
        source_indexes=indexes,
        base_generation=1,
        delta_generation=2,
    )
    assert tuple(plan.source_ranges for plan in plans) == (
        (("left", (0, 1)),),
        (("right", (0, 1)),),
    )
