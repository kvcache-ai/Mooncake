from __future__ import annotations

from itertools import product
from math import prod

import pytest

from mooncake.reshard.weight.manifest import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    SplitAxis,
)

from .helpers import (
    MODEL_ID,
    REVISION,
    assert_plan_copies_logical_contents,
    build_manifests,
    ep_tp_placements,
    plan_transfer,
    tensor_descriptor,
)
from model_weight_planner.helpers import (
    RuntimeInputs,
    _canonical_strides_bytes,
    runtime_inputs_from_groups,
)


@pytest.mark.parametrize(("source_tp", "target_tp"), [(4, 8), (8, 4)])
def test_tp_split_and_merge_preserve_logical_bytes(
    source_tp: int,
    target_tp: int,
) -> None:
    tensor = tensor_descriptor(
        "layers.0.mlp.down_proj.weight",
        global_shape=(64, 16),
        shard_dims=(0,),
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )
    owner = {tensor.tensor_id: 0}
    sources = build_manifests(
        "source",
        ep_tp_placements(
            (tensor,),
            dp=1,
            pp_owner=owner,
            ep=1,
            tp=source_tp,
            tp_dim=0,
        ),
        address_base=0x100000,
    )
    targets = build_manifests(
        "target",
        ep_tp_placements(
            (tensor,),
            dp=1,
            pp_owner=owner,
            ep=1,
            tp=target_tp,
            tp_dim=0,
        ),
        address_base=0x200000,
    )

    plan = plan_transfer(sources, targets)

    assert plan.total_bytes == prod(tensor.global_shape) * tensor.itemsize
    assert_plan_copies_logical_contents(plan, sources, targets)


def test_serialized_block_fp8_weight_and_scale_reshard_together() -> None:
    weight = TensorDescriptor(
        tensor_id="layers.1.mlp.gate_proj.weight",
        global_shape=(1024, 128),
        dtype="float8_e4m3fn",
        itemsize=1,
        shard_dims=(0,),
        layer_id=1,
        expert_id=None,
        layout_fingerprint=(
            "sglang:qwen3.5:gate-up:v1|serialized-block-fp8:e4m3fn:128x128:weight:v1"
        ),
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )
    scale = TensorDescriptor(
        tensor_id="layers.1.mlp.gate_proj.weight_scale_inv",
        global_shape=(8, 1),
        dtype="float32",
        itemsize=4,
        shard_dims=(0,),
        layer_id=1,
        expert_id=None,
        layout_fingerprint=(
            "sglang:qwen3.5:gate-up:v1|serialized-block-fp8:"
            "fp32-inverse-scale:128x128:v1"
        ),
        parallel_axes=(SplitAxis(kind="tp", dim=0),),
    )
    tensors = (weight, scale)
    owners = {tensor.tensor_id: 0 for tensor in tensors}
    sources = build_manifests(
        "source",
        ep_tp_placements(
            tensors,
            dp=1,
            pp_owner=owners,
            ep=1,
            tp=4,
            tp_dim=0,
        ),
        address_base=0x100000,
    )
    targets = build_manifests(
        "target",
        ep_tp_placements(
            tensors,
            dp=1,
            pp_owner=owners,
            ep=1,
            tp=8,
            tp_dim=0,
        ),
        address_base=0x200000,
    )

    plan = plan_transfer(sources, targets)

    assert {operation.tensor_id for operation in plan.operations} == {
        weight.tensor_id,
        scale.tensor_id,
    }
    assert plan.total_bytes == sum(
        fragment.nbytes for manifest in targets for fragment in manifest.fragments
    )
    assert_plan_copies_logical_contents(plan, sources, targets)


def fused_moe_fp8_manifests(
    side: str,
    *,
    ep: int,
    tp: int,
    pp: int,
    up_first: bool,
    address_base: int,
) -> RuntimeInputs:
    weight_shape = (4, 256, 128)
    scale_shape = (4, 2, 1)
    descriptors = {}
    for component in ("gate_proj", "up_proj"):
        descriptors[f"{component}.weight"] = TensorDescriptor(
            tensor_id=f"layers.0.mlp.experts.{component}.weight",
            global_shape=weight_shape,
            dtype="float8_e4m3fn",
            itemsize=1,
            layer_id=0,
            layout_fingerprint=(
                "sglang:qwen3.5:moe-w13:v1|serialized-block-fp8:"
                "e4m3fn:128x128:weight:v1"
            ),
            shard_dims=(0, 1),
            parallel_axes=(
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="ep", dim=0),
                SplitAxis(kind="tp", dim=1),
            ),
        )
        descriptors[f"{component}.scale"] = TensorDescriptor(
            tensor_id=f"layers.0.mlp.experts.{component}.weight_scale_inv",
            global_shape=scale_shape,
            dtype="float32",
            itemsize=4,
            layer_id=0,
            layout_fingerprint=(
                "sglang:qwen3.5:moe-w13:v1|serialized-block-fp8:"
                "fp32-inverse-scale:128x128:v1"
            ),
            shard_dims=(0, 1),
            parallel_axes=(
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="ep", dim=0),
                SplitAxis(kind="tp", dim=1),
            ),
        )

    local_experts = weight_shape[0] // ep
    local_intermediate = weight_shape[1] // tp
    local_scale_intermediate = scale_shape[1] // tp
    component_order = (
        ("up_proj", "gate_proj")
        if up_first
        else (
            "gate_proj",
            "up_proj",
        )
    )
    groups = []
    for ep_rank, tp_rank in product(range(ep), range(tp)):
        rank = ParallelRank(pp=pp, ep=ep_rank, tp=tp_rank)
        worker_id = f"{side}-p{pp}-e{ep_rank}-t{tp_rank}"
        rank_index = ep_rank * tp + tp_rank
        weight_base = address_base + rank_index * 0x1000000
        scale_base = weight_base + 0x800000
        weight_component_bytes = local_intermediate * weight_shape[2]
        scale_component_bytes = local_scale_intermediate * scale_shape[2] * 4
        placement_fragments = []
        binding_fragments = []
        for local_expert in range(local_experts):
            expert_id = ep_rank * local_experts + local_expert
            for component_index, component in enumerate(component_order):
                weight = descriptors[f"{component}.weight"]
                scale = descriptors[f"{component}.scale"]
                physical_component = (
                    local_expert * len(component_order) + component_index
                )
                weight_fragment_id = f"{worker_id}-expert{expert_id}-{component}-weight"
                scale_fragment_id = f"{worker_id}-expert{expert_id}-{component}-scale"
                placement_fragments.extend(
                    (
                        PlacementFragment(
                            placement_fragment_id=f"{weight_fragment_id}-placement",
                            tensor_id=weight.tensor_id,
                            global_offset=(
                                expert_id,
                                tp_rank * local_intermediate,
                                0,
                            ),
                            local_shape=(1, local_intermediate, weight_shape[2]),
                            nbytes=weight_component_bytes,
                            rank=rank,
                        ),
                        PlacementFragment(
                            placement_fragment_id=f"{scale_fragment_id}-placement",
                            tensor_id=scale.tensor_id,
                            global_offset=(
                                expert_id,
                                tp_rank * local_scale_intermediate,
                                0,
                            ),
                            local_shape=(
                                1,
                                local_scale_intermediate,
                                scale_shape[2],
                            ),
                            nbytes=scale_component_bytes,
                            rank=rank,
                        ),
                    )
                )
                binding_fragments.extend(
                    (
                        RuntimeBindingFragment(
                            placement_fragment_id=f"{weight_fragment_id}-placement",
                            fragment_id=weight_fragment_id,
                            address=(
                                weight_base
                                + physical_component * weight_component_bytes
                            ),
                            nbytes=weight_component_bytes,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=weight.itemsize,
                            local_shape=(
                                1,
                                local_intermediate,
                                weight_shape[2],
                            ),
                            strides_bytes=_canonical_strides_bytes(
                                (1, local_intermediate, weight_shape[2]),
                                weight.itemsize,
                            ),
                            storage_address=(
                                weight_base
                                + physical_component * weight_component_bytes
                            ),
                            storage_nbytes=weight_component_bytes,
                            storage_offset_bytes=0,
                        ),
                        RuntimeBindingFragment(
                            placement_fragment_id=f"{scale_fragment_id}-placement",
                            fragment_id=scale_fragment_id,
                            address=(
                                scale_base + physical_component * scale_component_bytes
                            ),
                            nbytes=scale_component_bytes,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=scale.itemsize,
                            local_shape=(
                                1,
                                local_scale_intermediate,
                                scale_shape[2],
                            ),
                            strides_bytes=_canonical_strides_bytes(
                                (1, local_scale_intermediate, scale_shape[2]),
                                scale.itemsize,
                            ),
                            storage_address=(
                                scale_base + physical_component * scale_component_bytes
                            ),
                            storage_nbytes=scale_component_bytes,
                            storage_offset_bytes=0,
                        ),
                    )
                )
        groups.append((worker_id, tuple(placement_fragments), tuple(binding_fragments)))
    return runtime_inputs_from_groups(
        resource_id=MODEL_ID,
        revision=REVISION,
        placement_set_id=side,
        tensors=tuple(descriptors.values()),
        groups=groups,
    )


def test_fp8_w31_to_w13_reshard_preserves_component_identity_and_bytes() -> None:
    sources = fused_moe_fp8_manifests(
        "source",
        ep=2,
        tp=2,
        pp=0,
        up_first=True,
        address_base=0x10000000,
    )
    targets = fused_moe_fp8_manifests(
        "target",
        ep=4,
        tp=1,
        pp=1,
        up_first=False,
        address_base=0x40000000,
    )

    plan = plan_transfer(sources, targets)

    source_fragments = {
        fragment.fragment_id: fragment
        for binding in sources.bindings
        for fragment in binding.fragments
    }
    target_fragments = {
        fragment.fragment_id: fragment
        for binding in targets.bindings
        for fragment in binding.fragments
    }
    assert (
        source_fragments["source-p0-e0-t0-expert0-up_proj-weight"].address
        < source_fragments["source-p0-e0-t0-expert0-gate_proj-weight"].address
    )
    assert (
        target_fragments["target-p1-e0-t0-expert0-gate_proj-weight"].address
        < target_fragments["target-p1-e0-t0-expert0-up_proj-weight"].address
    )
    assert (
        source_fragments["source-p0-e0-t0-expert0-up_proj-scale"].address
        < source_fragments["source-p0-e0-t0-expert0-gate_proj-scale"].address
    )
    assert (
        target_fragments["target-p1-e0-t0-expert0-gate_proj-scale"].address
        < target_fragments["target-p1-e0-t0-expert0-up_proj-scale"].address
    )
    assert {operation.tensor_id for operation in plan.operations} == {
        "layers.0.mlp.experts.gate_proj.weight",
        "layers.0.mlp.experts.gate_proj.weight_scale_inv",
        "layers.0.mlp.experts.up_proj.weight",
        "layers.0.mlp.experts.up_proj.weight_scale_inv",
    }
    assert {(route.source_pp, route.target_pp) for route in plan.pipeline_routes} == {
        (0, 1)
    }
    assert plan.total_bytes == sum(
        fragment.nbytes for manifest in targets for fragment in manifest.fragments
    )
    assert len(plan.operations) == 32
    assert (
        sum(
            1
            for operation in plan.operations
            for _ in operation.iter_segments(max_segments=operation.segment_count)
        )
        == 32
    )
    assert_plan_copies_logical_contents(plan, sources, targets)
