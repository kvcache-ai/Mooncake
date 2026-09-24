from __future__ import annotations

import pytest

from mooncake.reshard.weight.manifest import ParallelRank, SplitAxis
from mooncake.reshard.weight.planner import TransferRegion

from .helpers import (
    assert_plan_copies_logical_contents,
    build_manifests,
    ep_tp_placements,
    plan_transfer,
    pp_placements,
    tensor_descriptor,
)


@pytest.mark.parametrize(
    ("source_owners", "target_owners", "expected_routes"),
    [
        (
            {f"layers.{layer}.weight": layer // 2 for layer in range(4)},
            {f"layers.{layer}.weight": layer for layer in range(4)},
            {(0, 0), (0, 1), (1, 2), (1, 3)},
        ),
        (
            {f"layers.{layer}.weight": layer for layer in range(4)},
            {f"layers.{layer}.weight": layer // 2 for layer in range(4)},
            {(0, 0), (1, 0), (2, 1), (3, 1)},
        ),
    ],
)
def test_pp_ownership_routes_are_manifest_derived(
    source_owners: dict[str, int],
    target_owners: dict[str, int],
    expected_routes: set[tuple[int, int]],
) -> None:
    tensors = tuple(
        tensor_descriptor(
            f"layers.{layer}.weight",
            global_shape=(8,),
            shard_dims=(),
            layer_id=layer,
        )
        for layer in range(4)
    )
    sources = build_manifests(
        "source",
        pp_placements(tensors, source_owners),
        address_base=0x100000,
    )
    targets = build_manifests(
        "target",
        pp_placements(tensors, target_owners),
        address_base=0x200000,
    )

    plan = plan_transfer(sources, targets)

    assert {
        (route.source_pp, route.target_pp) for route in plan.pipeline_routes
    } == expected_routes
    assert sorted(
        index for route in plan.pipeline_routes for index in route.operation_indices
    ) == list(range(len(plan.operations)))
    assert_plan_copies_logical_contents(plan, sources, targets)


@pytest.mark.parametrize(("source_ep", "target_ep"), [(8, 2), (2, 8)])
def test_ep_reshard_uses_leading_expert_coordinate(
    source_ep: int, target_ep: int
) -> None:
    source_tensor = tensor_descriptor(
        "layers.0.experts.w1",
        global_shape=(8, 4, 2),
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    target_tensor = tensor_descriptor(
        source_tensor.tensor_id,
        global_shape=source_tensor.global_shape,
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    sources = build_manifests(
        "source",
        ep_tp_placements(
            (source_tensor,),
            dp=1,
            pp_owner={source_tensor.tensor_id: 0},
            ep=source_ep,
            tp=1,
            tp_dim=1,
        ),
        address_base=0x100000,
    )
    targets = build_manifests(
        "target",
        ep_tp_placements(
            (target_tensor,),
            dp=1,
            pp_owner={target_tensor.tensor_id: 0},
            ep=target_ep,
            tp=1,
            tp_dim=1,
        ),
        address_base=0x200000,
    )

    plan = plan_transfer(sources, targets)

    assert all(operation.overlap_shape[0] > 0 for operation in plan.operations)
    assert {operation.source.rank.ep for operation in plan.operations} == set(
        range(source_ep)
    )
    assert {operation.target.rank.ep for operation in plan.operations} == set(
        range(target_ep)
    )
    assert_plan_copies_logical_contents(plan, sources, targets)


@pytest.mark.parametrize("target_dim", [1, 2])
def test_ep_tp_cross_dim_reshard(target_dim: int) -> None:
    source_tensor = tensor_descriptor(
        "layers.0.experts.w1",
        global_shape=(4, 6, 8),
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    target_tensor = tensor_descriptor(
        source_tensor.tensor_id,
        global_shape=source_tensor.global_shape,
        shard_dims=(target_dim,),
    )
    source_placements = []
    for ep_rank in range(2):
        source_placements.append(
            (
                source_tensor,
                ParallelRank(ep=ep_rank),
                (ep_rank * 2, 0, 0),
                (2, 6, 8),
            )
        )
    target_placements = []
    for tp_rank in range(2):
        shape = list(target_tensor.global_shape)
        offset = [0, 0, 0]
        shape[target_dim] //= 2
        offset[target_dim] = tp_rank * shape[target_dim]
        target_placements.append(
            (
                target_tensor,
                ParallelRank(tp=tp_rank),
                tuple(offset),
                tuple(shape),
            )
        )
    sources = build_manifests("source", source_placements, address_base=0x100000)
    targets = build_manifests("target", target_placements, address_base=0x200000)

    plan = plan_transfer(sources, targets)

    assert len(plan.operations) == 4
    assert all(isinstance(operation, TransferRegion) for operation in plan.operations)
    selected = next(
        operation
        for operation in plan.operations
        if operation.source.rank.ep == 0 and operation.target.rank.tp == 1
    )
    if target_dim == 1:
        assert selected.overlap_offset == (0, 3, 0)
        assert selected.overlap_shape == (2, 3, 8)
        assert selected.source_base_offset == 48
        assert selected.target_base_offset == 0
        assert selected.inner_bytes == 48
        assert selected.outer_loop_counts == (2,)
        assert selected.source_strides == (96,)
        assert selected.target_strides == (48,)
    else:
        assert selected.overlap_offset == (0, 0, 4)
        assert selected.overlap_shape == (2, 6, 4)
        assert selected.source_base_offset == 8
        assert selected.target_base_offset == 0
        assert selected.inner_bytes == 8
        assert selected.outer_loop_counts == (2, 6)
        assert selected.source_strides == (96, 16)
        assert selected.target_strides == (48, 8)
    assert_plan_copies_logical_contents(plan, sources, targets)
