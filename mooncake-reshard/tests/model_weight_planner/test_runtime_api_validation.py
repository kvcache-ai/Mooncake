from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight._planner.api import plan_placement_transfer_to_local_target

from .helpers import (
    combine_runtime_inputs,
    descriptor,
    plan_transfer,
    plan_transfer_to_local_target,
    tp_manifests,
)


def test_planner_rejects_incompatible_layout() -> None:
    source = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target",
        tensor=descriptor(fingerprint="sglang:qwen3.5:fp8:v1"),
    )

    with pytest.raises(ValueError, match="layout mismatch"):
        plan_transfer(source, target)


@pytest.mark.parametrize("field", ["layer_id", "expert_id"])
def test_planner_rejects_incompatible_tensor_semantics(field: str) -> None:
    source = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    target_tensor = replace(descriptor(), **{field: 99})
    target = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="target",
        tensor=target_tensor,
    )

    with pytest.raises(ValueError, match="tensor descriptor mismatch"):
        plan_transfer(source, target)


def test_local_target_plan_supports_independent_tp_rank_startup() -> None:
    sources = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    targets = tp_manifests(
        tp=4,
        pp_rank=2,
        ep_rank=3,
        address_base=0x40000,
        worker_prefix="target",
    )

    plan = plan_transfer_to_local_target(sources, targets, target_index=1)

    assert len(plan.target_executors) == 1
    assert plan.target_executors[0].rank == targets[1].fragments[0].rank
    assert len(plan.operations) == 1
    operation = plan.operations[0]
    assert operation.source.rank.tp == 0
    assert operation.target.rank.tp == 1
    assert operation.source_offset == 2 * 4 * 2
    assert operation.target_offset == 0
    assert operation.nbytes == 2 * 4 * 2
    assert len(plan.source_executors) == 1
    assert plan.source_executors[0].rank.tp == 0


def test_local_target_plan_allows_explicit_pp_ep_tensor_subset() -> None:
    first = descriptor()
    second = replace(
        first,
        tensor_id="layers.7.experts.5.w1",
        layer_id=7,
        expert_id=5,
    )
    sources = combine_runtime_inputs(
        tp_manifests(
            tp=2,
            pp_rank=0,
            ep_rank=0,
            address_base=0x10000,
            worker_prefix="source-first",
            tensor=first,
        ),
        tp_manifests(
            tp=2,
            pp_rank=1,
            ep_rank=1,
            address_base=0x30000,
            worker_prefix="source-second",
            tensor=second,
        ),
    )
    local_targets = tp_manifests(
        tp=4,
        pp_rank=3,
        ep_rank=2,
        address_base=0x50000,
        worker_prefix="target",
        tensor=second,
    )

    plan = plan_transfer_to_local_target(sources, local_targets, target_index=2)

    assert {operation.tensor_id for operation in plan.operations} == {second.tensor_id}
    assert {operation.source.rank.pp for operation in plan.operations} == {1}
    assert {operation.source.rank.ep for operation in plan.operations} == {1}
    assert {operation.target.rank.pp for operation in plan.operations} == {3}
    assert {operation.target.rank.ep for operation in plan.operations} == {2}


def test_local_target_plan_rejects_unknown_target_tensor() -> None:
    sources = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    targets = tp_manifests(
        tp=4,
        pp_rank=0,
        ep_rank=0,
        address_base=0x40000,
        worker_prefix="target",
        tensor=replace(descriptor(), tensor_id="unknown.weight"),
    )

    with pytest.raises(ValueError, match="unknown tensors"):
        plan_transfer_to_local_target(sources, targets)


def test_local_target_plan_rejects_tensor_without_local_fragment() -> None:
    sources = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    targets = tp_manifests(
        tp=4,
        pp_rank=1,
        ep_rank=0,
        address_base=0x40000,
        worker_prefix="target",
    )
    empty_part = next(part for part in targets.placement.parts if not part.fragments)

    with pytest.raises(ValueError, match="no fragments"):
        plan_placement_transfer_to_local_target(
            sources.placement,
            targets.placement,
            empty_part.participant_id,
        )
