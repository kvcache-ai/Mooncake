from __future__ import annotations

import pytest

from mooncake.reshard.weight.manifest import ParallelRank, SplitAxis
from mooncake.reshard.weight.planner import (
    PlanningLimits,
    TransferRegion,
    plan_placement_transfer,
)

from .helpers import (
    assert_plan_copies_logical_contents,
    build_manifests,
    ep_tp_placements,
    plan_transfer,
    tensor_descriptor,
)


def test_four_axis_reshard_has_complete_content_and_routes() -> None:
    source_tensors = tuple(
        tensor_descriptor(
            f"layers.{layer}.experts.w1",
            global_shape=(8, 8, 2),
            shard_dims=(0, 1),
            layer_id=layer,
        )
        for layer in range(4)
    )
    target_tensors = tuple(
        tensor_descriptor(
            tensor.tensor_id,
            global_shape=tensor.global_shape,
            shard_dims=(0, 1),
            layer_id=tensor.layer_id,
        )
        for tensor in source_tensors
    )
    source_owners = {
        tensor.tensor_id: tensor.layer_id // 2 for tensor in source_tensors
    }
    target_owners = {tensor.tensor_id: tensor.layer_id for tensor in target_tensors}
    sources = build_manifests(
        "source",
        ep_tp_placements(
            source_tensors,
            dp=2,
            pp_owner=source_owners,
            ep=8,
            tp=4,
            tp_dim=1,
        ),
        address_base=0x10000000,
    )
    targets = build_manifests(
        "target",
        ep_tp_placements(
            target_tensors,
            dp=4,
            pp_owner=target_owners,
            ep=2,
            tp=8,
            tp_dim=1,
        ),
        address_base=0x20000000,
    )

    plan = plan_transfer(sources, targets)

    assert plan.total_bytes == 4 * 8 * 8 * 2 * 2 * 4
    assert {operation.source.rank.dp for operation in plan.operations} == {0, 1}
    assert {operation.target.rank.dp for operation in plan.operations} == {
        0,
        1,
        2,
        3,
    }
    assert {operation.source.rank.tp for operation in plan.operations} == set(range(4))
    assert {operation.target.rank.tp for operation in plan.operations} == set(range(8))
    assert {operation.source.rank.ep for operation in plan.operations} == set(range(8))
    assert {operation.target.rank.ep for operation in plan.operations} == {0, 1}
    assert {(route.source_pp, route.target_pp) for route in plan.pipeline_routes} == {
        (0, 0),
        (0, 1),
        (1, 2),
        (1, 3),
    }
    assert_plan_copies_logical_contents(plan, sources, targets)


def test_cross_dim_planner_keeps_operation_count_at_region_granularity() -> None:
    source_tensor = tensor_descriptor(
        "layers.0.experts.w1",
        global_shape=(8, 8192, 8192),
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    target_tensor = tensor_descriptor(
        source_tensor.tensor_id,
        global_shape=source_tensor.global_shape,
        shard_dims=(2,),
    )
    source_placements = [
        (
            source_tensor,
            ParallelRank(ep=rank),
            (rank, 0, 0),
            (1, 8192, 8192),
        )
        for rank in range(8)
    ]
    target_placements = [
        (
            target_tensor,
            ParallelRank(tp=rank),
            (0, 0, rank * 1024),
            (8, 8192, 1024),
        )
        for rank in range(8)
    ]
    sources = build_manifests("source", source_placements, address_base=0x100000000)
    targets = build_manifests("target", target_placements, address_base=0x300000000)

    plan = plan_transfer(sources, targets)

    assert len(plan.operations) == 64
    assert {operation.segment_count for operation in plan.operations} == {8192}
    assert all(isinstance(operation, TransferRegion) for operation in plan.operations)


def test_cross_dim_planner_rejects_total_region_budget_before_materialization() -> None:
    source_tensor = tensor_descriptor(
        "layers.0.experts.w1",
        global_shape=(8, 8, 8),
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    target_tensor = tensor_descriptor(
        source_tensor.tensor_id,
        global_shape=source_tensor.global_shape,
        shard_dims=(2,),
    )
    sources = build_manifests(
        "source-budget",
        [
            (source_tensor, ParallelRank(ep=rank), (rank, 0, 0), (1, 8, 8))
            for rank in range(8)
        ],
        address_base=0x100000000,
    )
    targets = build_manifests(
        "target-budget",
        [
            (target_tensor, ParallelRank(tp=rank), (0, 0, rank), (8, 8, 1))
            for rank in range(8)
        ],
        address_base=0x300000000,
    )

    with pytest.raises(ValueError, match="max_transfer_regions"):
        plan_placement_transfer(
            sources.placement,
            targets.placement,
            planning_limits=PlanningLimits(max_transfer_regions=63),
        )
