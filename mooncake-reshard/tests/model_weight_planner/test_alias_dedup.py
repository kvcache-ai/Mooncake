from __future__ import annotations

from dataclasses import replace
from math import prod

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    OwnershipAxis,
    SplitAxis,
)
from mooncake.reshard.weight._planner.bound_contracts import RuntimeLeaseSnapshot

from .helpers import (
    RuntimeInputs,
    _canonical_strides_bytes,
    descriptor,
    plan_transfer,
    runtime_inputs_from_groups,
)


def _runtime_inputs(
    *,
    resource_id: str,
    tensors: tuple[TensorDescriptor, ...],
    groups: tuple[tuple[str, tuple[tuple[PlacementFragment, int], ...]], ...],
) -> RuntimeInputs:
    return runtime_inputs_from_groups(
        resource_id=resource_id,
        revision="step-42",
        placement_set_id=groups[0][0],
        tensors=tensors,
        groups=tuple(
            (
                worker_id,
                tuple(fragment for fragment, _ in fragments),
                tuple(
                    RuntimeBindingFragment(
                        placement_fragment_id=fragment.placement_fragment_id,
                        fragment_id=(
                            f"{worker_id}-{fragment.placement_fragment_id}-runtime"
                        ),
                        address=address,
                        nbytes=fragment.nbytes,
                        worker_id=worker_id,
                        endpoint=f"{worker_id}:12345",
                        device="cuda:0",
                        itemsize=(fragment.nbytes // prod(fragment.local_shape)),
                        local_shape=fragment.local_shape,
                        strides_bytes=_canonical_strides_bytes(
                            fragment.local_shape,
                            fragment.nbytes // prod(fragment.local_shape),
                        ),
                        storage_address=address,
                        storage_nbytes=fragment.nbytes,
                        storage_offset_bytes=0,
                    )
                    for fragment, address in fragments
                ),
            )
            for worker_id, fragments in groups
        ),
    )


def _alias_tensors() -> tuple[TensorDescriptor, ...]:
    return tuple(
        replace(
            descriptor(),
            tensor_id=tensor_id,
            global_shape=(32,),
            shard_dims=(),
            layer_id=None,
            expert_id=None,
            layout_fingerprint="sglang:qwen3.5:vocab-parallel:v1",
            parallel_axes=(OwnershipAxis(kind="pp"),),
        )
        for tensor_id in ("embed_tokens.weight", "lm_head.weight")
    )


def _flat_fragment(
    tensor_id: str,
    *,
    prefix: str,
    aliases: tuple[str, ...],
    pp: int = 0,
) -> PlacementFragment:
    return PlacementFragment(
        placement_fragment_id=f"{prefix}-{tensor_id}-placement",
        tensor_id=tensor_id,
        global_offset=(0,),
        local_shape=(32,),
        nbytes=64,
        rank=ParallelRank(pp=pp),
        aliases=aliases,
    )


def test_plan_preserves_identical_physical_alias_copies() -> None:
    aliases = ("embed_tokens.weight", "lm_head.weight")
    tensors = _alias_tensors()
    source = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "source",
                tuple(
                    (
                        _flat_fragment(t.tensor_id, prefix="source", aliases=aliases),
                        0x10000,
                    )
                    for t in tensors
                ),
            ),
        ),
    )
    target = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "target",
                tuple(
                    (
                        _flat_fragment(t.tensor_id, prefix="target", aliases=aliases),
                        0x20000,
                    )
                    for t in tensors
                ),
            ),
        ),
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 2
    assert plan.total_bytes == 128


def test_plan_rejects_target_aliases_with_distinct_source_storage() -> None:
    aliases = ("embed_tokens.weight", "lm_head.weight")
    tensors = _alias_tensors()
    source = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "source",
                tuple(
                    (
                        _flat_fragment(t.tensor_id, prefix="source", aliases=()),
                        0x10000 + index * 0x1000,
                    )
                    for index, t in enumerate(tensors)
                ),
            ),
        ),
    )
    target = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "target",
                tuple(
                    (
                        _flat_fragment(t.tensor_id, prefix="target", aliases=aliases),
                        0x20000,
                    )
                    for t in tensors
                ),
            ),
        ),
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        plan_transfer(source, target)


def test_plan_preserves_declared_target_aliases_across_pp_sources() -> None:
    aliases = ("embed_tokens.weight", "lm_head.weight")
    tensors = _alias_tensors()
    source = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=tuple(
            (
                f"source-pp{pp}",
                (
                    (
                        _flat_fragment(
                            tensor.tensor_id,
                            prefix=f"source-pp{pp}",
                            aliases=aliases,
                            pp=pp,
                        ),
                        0x10000 + pp * 0x1000,
                    ),
                ),
            )
            for pp, tensor in enumerate(tensors)
        ),
    )
    target = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "target",
                tuple(
                    (
                        _flat_fragment(t.tensor_id, prefix="target", aliases=aliases),
                        0x20000,
                    )
                    for t in tensors
                ),
            ),
        ),
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 2
    assert {operation.tensor_id for operation in plan.operations} == {
        "embed_tokens.weight",
        "lm_head.weight",
    }
    assert {operation.source.worker_id for operation in plan.operations} == {
        "source-pp0",
        "source-pp1",
    }


def test_plan_preserves_cross_dim_declared_aliases() -> None:
    tensor_ids = ("alias.a", "alias.b")
    aliases = tensor_ids
    source_tensors = tuple(
        TensorDescriptor(
            tensor_id=tensor_id,
            global_shape=(2, 2),
            dtype="bfloat16",
            itemsize=2,
            layer_id=None,
            expert_id=None,
            layout_fingerprint="framework:tied-weight:v2",
            shard_dims=(0,),
            parallel_axes=(
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="tp", dim=0),
            ),
        )
        for tensor_id in tensor_ids
    )
    target_tensors = tuple(
        replace(
            tensor,
            shard_dims=(1,),
            parallel_axes=(
                OwnershipAxis(kind="pp"),
                SplitAxis(kind="tp", dim=1),
            ),
        )
        for tensor in source_tensors
    )
    source_fragments = tuple(
        (
            PlacementFragment(
                placement_fragment_id=f"source-{tensor.tensor_id}-row{row}",
                tensor_id=tensor.tensor_id,
                global_offset=(row, 0),
                local_shape=(1, 2),
                nbytes=4,
                rank=ParallelRank(pp=0),
                aliases=aliases,
            ),
            0x10000 + tensor_index * 0x100 + row * 4,
        )
        for tensor_index, tensor in enumerate(source_tensors)
        for row in range(2)
    )
    target_fragments = tuple(
        (
            PlacementFragment(
                placement_fragment_id=f"target-{tensor.tensor_id}-column{column}",
                tensor_id=tensor.tensor_id,
                global_offset=(0, column),
                local_shape=(2, 1),
                nbytes=4,
                rank=ParallelRank(pp=1),
                aliases=aliases,
            ),
            0x20000 + column * 0x100,
        )
        for tensor in target_tensors
        for column in range(2)
    )
    source = _runtime_inputs(
        resource_id="qwen-family",
        tensors=source_tensors,
        groups=(("source", source_fragments),),
    )
    target = _runtime_inputs(
        resource_id="qwen-family",
        tensors=target_tensors,
        groups=(("target", target_fragments),),
    )

    plan = plan_transfer(source, target)

    assert len(plan.operations) == 8
    assert {operation.tensor_id for operation in plan.operations} == {
        "alias.a",
        "alias.b",
    }
    assert plan.pipeline_routes[0].source_pp == 0
    assert plan.pipeline_routes[0].target_pp == 1


def test_plan_rejects_alias_targets_with_distinct_runtime_lease_scopes() -> None:
    """A physical target range cannot span independently fenced bindings."""

    aliases = ("embed_tokens.weight", "lm_head.weight")
    tensors = _alias_tensors()
    source = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=tuple(
            (
                f"source-pp{pp}",
                (
                    (
                        _flat_fragment(
                            tensor.tensor_id,
                            prefix=f"source-pp{pp}",
                            aliases=aliases,
                            pp=pp,
                        ),
                        0x10000 + pp * 0x1000,
                    ),
                ),
            )
            for pp, tensor in enumerate(tensors)
        ),
    )
    target = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=tuple(
            (
                f"target-pp{pp}",
                (
                    (
                        _flat_fragment(
                            tensor.tensor_id,
                            prefix=f"target-pp{pp}",
                            aliases=aliases,
                            pp=pp,
                        ),
                        0x20000,
                    ),
                ),
            )
            for pp, tensor in enumerate(tensors)
        ),
    )
    target = RuntimeInputs(
        target.placement,
        tuple(
            replace(
                binding,
                instance_id="shared-target-instance",
                lease_id=f"target-lease-{index}",
                fragments=(
                    replace(
                        binding.fragments[0],
                        worker_id="shared-target-worker",
                        endpoint="shared-target-worker:12345",
                        address=0x20000,
                        storage_address=0x20000,
                        owner=f"target-owner-{index}",
                    ),
                ),
            )
            for index, binding in enumerate(target.bindings)
        ),
    )

    with pytest.raises(ValueError, match="conflicting target physical range"):
        plan_transfer(source, target)


def test_bound_participant_lease_collection_is_linear(monkeypatch) -> None:
    """Executor provenance must not use repeated list-membership scans."""

    fragment_count = 32
    tensors = tuple(
        replace(
            descriptor(),
            tensor_id=f"layers.{index}.mlp.down_proj.weight",
            global_shape=(32,),
            shard_dims=(),
            layer_id=None,
            expert_id=None,
            layout_fingerprint="sglang:qwen3.5:linear-lease-scan:v1",
            parallel_axes=(OwnershipAxis(kind="pp"),),
        )
        for index in range(fragment_count)
    )
    source = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "source",
                tuple(
                    (
                        _flat_fragment(tensor.tensor_id, prefix="source", aliases=()),
                        0x10000 + index * 0x1000,
                    )
                    for index, tensor in enumerate(tensors)
                ),
            ),
        ),
    )
    target = _runtime_inputs(
        resource_id="qwen3.5-0.8b",
        tensors=tensors,
        groups=(
            (
                "target",
                tuple(
                    (
                        _flat_fragment(tensor.tensor_id, prefix="target", aliases=()),
                        0x40000 + index * 0x1000,
                    )
                    for index, tensor in enumerate(tensors)
                ),
            ),
        ),
    )

    comparisons = 0
    original_eq = RuntimeLeaseSnapshot.__eq__

    def counting_eq(self, other):
        nonlocal comparisons
        comparisons += 1
        return original_eq(self, other)

    monkeypatch.setattr(RuntimeLeaseSnapshot, "__eq__", counting_eq)

    plan = plan_transfer(source, target)

    assert len(plan.operations) == fragment_count
    assert comparisons < fragment_count * 8
