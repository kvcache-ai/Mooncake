from __future__ import annotations

from dataclasses import replace
from itertools import product

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    OwnershipAxis,
    ReplicatedAxis,
    SplitAxis,
    plan_placement_transfer,
)
from mooncake.reshard.weight._planner.ownership import (
    complete_parallel_source_replicas,
    parallel_tensor_owner,
)

from .helpers import (
    _canonical_strides_bytes,
    CountingFragment,
    combine_runtime_inputs,
    descriptor,
    distribute_tp_shards_across_ep_ranks,
    global_placement_from_fragments,
    operation_for_target,
    plan_transfer,
    plan_transfer_to_local_target,
    replace_placement_fragment,
    runtime_inputs_from_groups,
    tp_manifests,
)


def test_target_dp_replicas_on_distinct_devices_are_not_deduplicated() -> None:
    source = tp_manifests(
        tp=1,
        dp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    tensor = source.placement.tensors[0]
    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"target-dp{dp}-placement",
            tensor_id=tensor.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor.global_shape,
            nbytes=source.placement.fragments[0].nbytes,
            rank=ParallelRank(dp=dp),
        )
        for dp in range(2)
    )
    target = runtime_inputs_from_groups(
        resource_id=source.placement.resource_id,
        revision=source.placement.revision,
        placement_set_id="target",
        tensors=(tensor,),
        groups=tuple(
            (
                f"target-dp{dp}",
                (fragment,),
                (
                    RuntimeBindingFragment(
                        placement_fragment_id=fragment.placement_fragment_id,
                        fragment_id=f"target-dp{dp}-runtime",
                        address=0x40000,
                        nbytes=fragment.nbytes,
                        worker_id="target-worker",
                        endpoint="target-worker:12345",
                        device=f"cuda:{dp}",
                        itemsize=tensor.itemsize,
                        local_shape=fragment.local_shape,
                        strides_bytes=_canonical_strides_bytes(
                            fragment.local_shape, tensor.itemsize
                        ),
                        storage_address=0x40000,
                        storage_nbytes=fragment.nbytes,
                        storage_offset_bytes=0,
                    ),
                ),
            )
            for dp, fragment in enumerate(fragments)
        ),
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 2
    assert {operation.target.device for operation in plan.operations} == {
        "cuda:0",
        "cuda:1",
    }
    assert all(
        plan.operation_indices_for_executor(executor, "target")
        for executor in plan.target_executors
    )


def test_ep_ownership_is_derived_from_parallel_axis_semantics() -> None:
    ownership_tensor = replace(
        descriptor(),
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="ep"),),
    )
    split_tensor = replace(
        descriptor(),
        expert_id=None,
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    fragment = PlacementFragment(
        tensor_id=ownership_tensor.tensor_id,
        global_offset=(0, 0),
        local_shape=ownership_tensor.global_shape,
        nbytes=64,
        rank=ParallelRank(ep=3),
    )

    assert parallel_tensor_owner(ownership_tensor, fragment) == (("ep", 3),)
    assert parallel_tensor_owner(split_tensor, fragment) == ()


def test_dp_ownership_routes_each_tensor_through_its_declared_owner() -> None:
    tensor_a = replace(
        descriptor(),
        tensor_id="layers.0.weight",
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="dp"),),
    )
    tensor_b = replace(
        descriptor(),
        tensor_id="layers.1.weight",
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="dp"),),
    )

    def fragment(tensor, dp_rank: int, prefix: str) -> PlacementFragment:
        return PlacementFragment(
            placement_fragment_id=f"{prefix}-{tensor.tensor_id}-dp{dp_rank}",
            tensor_id=tensor.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor.global_shape,
            nbytes=64,
            rank=ParallelRank(dp=dp_rank),
        )

    source_fragments = (
        fragment(tensor_a, 0, "source"),
        fragment(tensor_b, 1, "source"),
    )
    target_fragments = (
        fragment(tensor_a, 0, "target"),
        fragment(tensor_b, 1, "target"),
    )
    source = global_placement_from_fragments(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="source-dp-owners",
        tensors=(tensor_a, tensor_b),
        fragments=source_fragments,
    )
    target = global_placement_from_fragments(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="target-dp-owners",
        tensors=(tensor_a, tensor_b),
        fragments=target_fragments,
    )

    plan = plan_placement_transfer(source, target)

    assert len(plan.operations) == 2
    assert {
        (operation.source.tensor_id, operation.source.rank.dp)
        for operation in plan.operations
    } == {
        (tensor_a.tensor_id, 0),
        (tensor_b.tensor_id, 1),
    }
    assert {
        (operation.target.tensor_id, operation.target.rank.dp)
        for operation in plan.operations
    } == {
        (tensor_a.tensor_id, 0),
        (tensor_b.tensor_id, 1),
    }


def test_dp_ownership_rejects_ambiguous_complete_source_owners() -> None:
    tensor = replace(
        descriptor(),
        tensor_id="layers.0.weight",
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="dp"),),
    )

    def fragment(dp_rank: int, prefix: str) -> PlacementFragment:
        return PlacementFragment(
            placement_fragment_id=f"{prefix}-dp{dp_rank}",
            tensor_id=tensor.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor.global_shape,
            nbytes=64,
            rank=ParallelRank(dp=dp_rank),
        )

    source = global_placement_from_fragments(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="source-ambiguous-dp-owners",
        tensors=(tensor,),
        fragments=(fragment(0, "source"), fragment(1, "source")),
    )
    target = global_placement_from_fragments(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="target-dp-owner",
        tensors=(tensor,),
        fragments=(fragment(0, "target"),),
    )

    with pytest.raises(ValueError, match="ambiguous declared owners"):
        plan_placement_transfer(source, target)


def test_pipeline_routes_keep_distinct_virtual_stages_on_one_pp_rank() -> None:
    tensor_a = replace(
        descriptor(),
        tensor_id="layers.0.weight",
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="pp"),),
    )
    tensor_b = replace(
        descriptor(),
        tensor_id="layers.1.weight",
        shard_dims=(),
        expert_id=None,
        parallel_axes=(OwnershipAxis(kind="pp"),),
    )
    source_fragments = (
        PlacementFragment(
            placement_fragment_id="source-stage-0",
            tensor_id=tensor_a.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor_a.global_shape,
            nbytes=64,
            rank=ParallelRank(pp=0),
            pipeline_stage_id=0,
        ),
        PlacementFragment(
            placement_fragment_id="source-stage-1",
            tensor_id=tensor_b.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor_b.global_shape,
            nbytes=64,
            rank=ParallelRank(pp=0),
            pipeline_stage_id=1,
        ),
    )
    target_fragments = (
        PlacementFragment(
            placement_fragment_id="target-stage-2",
            tensor_id=tensor_a.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor_a.global_shape,
            nbytes=64,
            rank=ParallelRank(pp=0),
            pipeline_stage_id=2,
        ),
        PlacementFragment(
            placement_fragment_id="target-stage-3",
            tensor_id=tensor_b.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor_b.global_shape,
            nbytes=64,
            rank=ParallelRank(pp=0),
            pipeline_stage_id=3,
        ),
    )
    source = runtime_inputs_from_groups(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="source-pipeline-stages",
        tensors=(tensor_a, tensor_b),
        groups=(("source", source_fragments, ()),),
    )
    target = runtime_inputs_from_groups(
        resource_id="qwen3.5-0.8b",
        revision="step-42",
        placement_set_id="target-pipeline-stages",
        tensors=(tensor_a, tensor_b),
        groups=(("target", target_fragments, ()),),
    )

    logical = plan_placement_transfer(source.placement, target.placement)

    assert {
        (
            route.source_pp,
            route.source_pipeline_stage_id,
            route.target_pp,
            route.target_pipeline_stage_id,
        )
        for route in logical.pipeline_routes
    } == {
        (0, 0, 0, 2),
        (0, 1, 0, 3),
    }


def test_all_parallel_axes_change_in_one_plan() -> None:
    source = tp_manifests(
        tp=2,
        dp=2,
        pp_rank=1,
        ep_rank=1,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=4,
        dp=3,
        pp_rank=2,
        ep_rank=3,
        address_base=0x40000,
        worker_prefix="target",
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 12
    assert {op.target.rank.dp for op in plan.operations} == {0, 1, 2}
    assert {op.target.rank.pp for op in plan.operations} == {2}
    assert {op.target.rank.ep for op in plan.operations} == {3}
    assert {op.source.rank.pp for op in plan.operations} == {1}
    assert {op.source.rank.ep for op in plan.operations} == {1}
    assert {op.source.rank.dp for op in operation_for_target(plan, 0, 0)} == {0}
    assert {op.source.rank.dp for op in operation_for_target(plan, 0, 1)} == {1}
    assert {op.source.rank.dp for op in operation_for_target(plan, 0, 2)} == {0}


def test_dense_source_tp_coverage_cannot_span_ep_replicas() -> None:
    tensor = replace(
        descriptor(),
        tensor_id="layers.2.self_attn.q_proj.weight",
        expert_id=None,
        parallel_axes=(
            ReplicatedAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            ReplicatedAxis(kind="ep"),
            SplitAxis(kind="tp", dim=0),
        ),
    )
    with pytest.raises(
        ValueError, match="global placement tensor is not fully covered"
    ):
        distribute_tp_shards_across_ep_ranks(
            tp_manifests(
                tp=4,
                pp_rank=0,
                ep_rank=0,
                address_base=0x10000,
                worker_prefix="source",
                tensor=tensor,
            )
        )


def test_source_replica_validation_indexes_fragments_by_tensor_once() -> None:
    tensor_count = 64
    tensors = tuple(
        replace(
            descriptor(),
            tensor_id=f"layers.{index}.self_attn.q_proj.weight",
            layer_id=index,
            expert_id=None,
            parallel_axes=(SplitAxis(kind="tp", dim=0),),
        )
        for index in range(tensor_count)
    )
    fragments = tuple(
        tp_manifests(
            tp=1,
            pp_rank=0,
            ep_rank=0,
            address_base=0x10000 + index * 0x1000,
            worker_prefix=f"source-{index}",
            tensor=tensor,
        )[0].fragments[0]
        for index, tensor in enumerate(tensors)
    )
    accesses = [0]

    replicas = complete_parallel_source_replicas(
        {tensor.tensor_id: tensor for tensor in tensors},
        tuple(CountingFragment(fragment, accesses) for fragment in fragments),
    )

    assert set(replicas) == {0}
    assert accesses[0] <= tensor_count * 3


def test_dense_target_tp_coverage_cannot_span_ep_replicas() -> None:
    tensor = replace(
        descriptor(),
        tensor_id="layers.2.self_attn.q_proj.weight",
        expert_id=None,
        parallel_axes=(
            ReplicatedAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            ReplicatedAxis(kind="ep"),
            SplitAxis(kind="tp", dim=0),
        ),
    )
    with pytest.raises(
        ValueError, match="global placement tensor is not fully covered"
    ):
        distribute_tp_shards_across_ep_ranks(
            tp_manifests(
                tp=2,
                pp_rank=0,
                ep_rank=0,
                address_base=0x40000,
                worker_prefix="target",
                tensor=tensor,
            )
        )


def test_global_manifest_barrier_rejects_expert_tp_spanning_ep_owners() -> None:
    with pytest.raises(
        ValueError,
        match="global placement tensor is not fully covered",
    ):
        distribute_tp_shards_across_ep_ranks(
            tp_manifests(
                tp=2,
                pp_rank=0,
                ep_rank=0,
                address_base=0x10000,
                worker_prefix="source",
            )
        )


def test_local_plan_uses_one_complete_source_dp_replica() -> None:
    sources = tp_manifests(
        tp=2,
        dp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=1,
        dp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x40000,
        worker_prefix="target",
    )

    plan = plan_transfer_to_local_target(sources, target, target_index=1)

    assert {operation.source.rank.dp for operation in plan.operations} == {1}


def test_global_manifest_barrier_rejects_tp_coverage_split_across_pp_owners() -> None:
    targets = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x40000,
        worker_prefix="target",
    )
    moved = replace(
        targets[1].fragments[0],
        rank=replace(targets[1].fragments[0].rank, pp=1),
    )
    with pytest.raises(
        ValueError, match="global placement tensor is not fully covered"
    ):
        replace_placement_fragment(targets, 1, moved)


def test_complete_tensor_replicas_across_pp_owners_are_each_transferred() -> None:
    tensor = replace(
        descriptor(),
        tensor_id="lm_head.weight",
        layer_id=None,
        expert_id=None,
        parallel_axes=(
            OwnershipAxis(kind="pp"),
            SplitAxis(kind="tp", dim=0),
        ),
    )
    source = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
        tensor=tensor,
    )
    target = combine_runtime_inputs(
        tp_manifests(
            tp=1,
            pp_rank=0,
            ep_rank=0,
            address_base=0x40000,
            worker_prefix="target-pp0",
            tensor=tensor,
        ),
        tp_manifests(
            tp=1,
            pp_rank=1,
            ep_rank=0,
            address_base=0x50000,
            worker_prefix="target-pp1",
            tensor=tensor,
        ),
    )

    plan = plan_transfer(source, target)

    assert {operation.target.rank.pp for operation in plan.operations} == {0, 1}
    assert len(plan.operations) == 4


def test_multiple_layers_and_experts_move_pp_ep_tp_dp_ownership_together() -> None:
    source_inputs = []
    target_inputs = []
    for layer_id, expert_id in product(range(2), range(2)):
        tensor = replace(
            descriptor(),
            tensor_id=f"layers.{layer_id}.experts.{expert_id}.w1",
            layer_id=layer_id,
            expert_id=expert_id,
        )
        source_inputs.append(
            tp_manifests(
                tp=2,
                dp=2,
                pp_rank=layer_id,
                ep_rank=expert_id,
                address_base=0x100000 + (layer_id * 2 + expert_id) * 0x10000,
                worker_prefix=f"source-l{layer_id}-e{expert_id}",
                tensor=tensor,
            )
        )
        target_inputs.append(
            tp_manifests(
                tp=4,
                dp=3,
                pp_rank=1 - layer_id,
                ep_rank=1 - expert_id,
                address_base=0x400000 + (layer_id * 2 + expert_id) * 0x10000,
                worker_prefix=f"target-l{layer_id}-e{expert_id}",
                tensor=tensor,
            )
        )

    plan = plan_transfer(
        combine_runtime_inputs(*source_inputs),
        combine_runtime_inputs(*target_inputs),
    )

    assert plan.resource_id == "qwen3.5-0.8b"
    assert plan.revision == "step-42"
    assert plan.total_bytes == 4 * 3 * 8 * 4 * 2
    assert len(plan.operations) == 4 * 3 * 4
    for operation in plan.operations:
        layer_id = operation.target.tensor_id.split(".")[1]
        expert_id = operation.target.tensor_id.split(".")[3]
        assert operation.source.tensor_id == operation.target.tensor_id
        assert operation.source.rank.pp == int(layer_id)
        assert operation.target.rank.pp == 1 - int(layer_id)
        assert operation.source.rank.ep == int(expert_id)
        assert operation.target.rank.ep == 1 - int(expert_id)
