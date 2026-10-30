from __future__ import annotations

from copy import copy
from dataclasses import replace

import pytest

from model_weight_planner.helpers import (
    bound_fragment,
    descriptor,
    plan_transfer,
    runtime_inputs_from_groups,
)
from mooncake.reshard.weight._te.execution import (
    executor_requirements_for_operation_indices,
)
from mooncake.reshard.weight import TransferEngineError, TransferPlan


class _CountedFragmentId(str):
    comparisons = 0
    __hash__ = str.__hash__

    def __eq__(self, other):
        type(self).comparisons += 1
        return str.__eq__(self, other)


def _plan(size: int) -> TransferPlan:
    tensors = tuple(
        replace(descriptor(), tensor_id=f"tensor-{index:04d}") for index in range(size)
    )
    inputs = []
    for side, base in (("source", 0x10000), ("target", 0x1000000)):
        fragments = tuple(
            bound_fragment(
                fragment_id=f"{side}-{index:04d}",
                tensor_id=tensor.tensor_id,
                global_offset=(0, 0),
                local_shape=tensor.global_shape,
                address=base + index * 256,
                nbytes=64,
                worker_id=side,
                endpoint=f"{side}:12345",
            )
            for index, tensor in enumerate(tensors)
        )
        inputs.append(
            runtime_inputs_from_groups(
                resource_id="scaling",
                revision="revision",
                placement_set_id=side,
                tensors=tensors,
                groups=(
                    (
                        side,
                        tuple(item.placement for item in fragments),
                        tuple(item.binding for item in fragments),
                    ),
                ),
            )
        )
    return plan_transfer(*inputs)


@pytest.mark.parametrize("side", ["source", "target"])
def test_executor_scope_fragment_membership_scales_linearly(side: str) -> None:
    counts = []
    for size in (64, 128):
        plan = _plan(size)
        executor = getattr(plan, f"{side}_executors")[0]
        object.__setattr__(
            executor,
            "fragment_ids",
            tuple(_CountedFragmentId(item) for item in executor.fragment_ids),
        )
        _CountedFragmentId.comparisons = 0
        requirements = executor_requirements_for_operation_indices(
            plan, tuple(range(size)), side
        )
        counts.append(_CountedFragmentId.comparisons)
        assert requirements == {
            (executor.instance_id, executor.participant_id): tuple(
                sorted(executor.fragment_ids)
            )
        }
    assert counts[1] <= counts[0] * 2.5, counts


@pytest.mark.parametrize("side", ["source", "target"])
def test_executor_scope_rejects_unowned_fragment(side: str) -> None:
    plan = _plan(2)
    operation = copy(plan.operations[0])
    fragment = copy(getattr(operation, side))
    binding = copy(fragment.binding)
    object.__setattr__(binding, "fragment_id", "unowned-fragment")
    object.__setattr__(fragment, "binding", binding)
    object.__setattr__(operation, side, fragment)
    object.__setattr__(plan, "operations", (operation, *plan.operations[1:]))
    with pytest.raises(TransferEngineError, match="does not own planned fragment"):
        executor_requirements_for_operation_indices(plan, (0, 1), side)


@pytest.mark.parametrize("side", ["source", "target"])
def test_executor_scope_rejects_incomplete_operations(side: str) -> None:
    plan = _plan(2)
    object.__setattr__(plan, f"{side}_executors", ())
    with pytest.raises(TransferEngineError, match="executor scope is incomplete"):
        executor_requirements_for_operation_indices(plan, (0, 1), side)
