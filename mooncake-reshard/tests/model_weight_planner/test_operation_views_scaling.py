from __future__ import annotations

from copy import copy
from dataclasses import replace

import pytest

from mooncake.reshard.weight import (
    bind_logical_transfer_plan,
    plan_placement_transfer,
)
from mooncake.reshard.weight._planner import contracts

from .helpers import (
    RuntimeInputs,
    bound_fragment,
    descriptor,
    runtime_inputs_from_groups,
)


class _CountedTuple(tuple):
    hashed_items = 0

    def __hash__(self):
        type(self).hashed_items += len(self)
        return tuple.__hash__(self)


def _inputs(size: int, side: str) -> RuntimeInputs:
    tensors = tuple(
        replace(descriptor(), tensor_id=f"tensor-{index:04d}") for index in range(size)
    )
    fragments = tuple(
        bound_fragment(
            fragment_id=f"{side}-{index:04d}",
            tensor_id=tensor.tensor_id,
            global_offset=(0, 0),
            local_shape=tensor.global_shape,
            address=(0x10000 if side == "source" else 0x1000000) + index * 256,
            nbytes=64,
            worker_id=side,
            endpoint=f"{side}:12345",
        )
        for index, tensor in enumerate(tensors)
    )
    return runtime_inputs_from_groups(
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


@pytest.mark.parametrize("phase", ["planner", "binder"])
def test_public_operation_views_hash_fragment_ids_linearly(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    original = contracts._executor_projection_key

    def counted_projection(executor):
        key = original(executor)
        # Instrument derived keys while keeping public inputs canonical.
        return (*key[:-1], _CountedTuple(key[-1]))

    monkeypatch.setattr(contracts, "_executor_projection_key", counted_projection)
    counts = []
    for size in (64, 128):
        source, target = _inputs(size, "source"), _inputs(size, "target")
        _CountedTuple.hashed_items = 0
        logical = plan_placement_transfer(source.placement, target.placement)
        if phase == "binder":
            _CountedTuple.hashed_items = 0
            bind_logical_transfer_plan(
                logical, target.bindings, source_bindings=source.bindings
            )
        counts.append(_CountedTuple.hashed_items)
    assert counts[1] <= counts[0] * 2.5, counts


@pytest.mark.parametrize("bound", [False, True])
def test_operation_views_lookup_accepts_equal_executor_copy(bound: bool) -> None:
    source, target = _inputs(3, "source"), _inputs(3, "target")
    plan = plan_placement_transfer(source.placement, target.placement)
    if bound:
        plan = bind_logical_transfer_plan(
            plan, target.bindings, source_bindings=source.bindings
        )
    for side in ("source", "target"):
        executor = getattr(plan, f"{side}_executors")[0]
        equal_executor = copy(executor)
        assert equal_executor is not executor
        assert plan.operation_indices_for_executor(equal_executor, side) == (0, 1, 2)


@pytest.mark.parametrize("side", ["source", "target"])
@pytest.mark.parametrize("bound", [False, True])
def test_operation_views_reject_duplicate_executor_projection(
    side: str, bound: bool
) -> None:
    source, target = _inputs(2, "source"), _inputs(2, "target")
    plan = plan_placement_transfer(source.placement, target.placement)
    if bound:
        plan = bind_logical_transfer_plan(
            plan, target.bindings, source_bindings=source.bindings
        )
    executor = getattr(plan, f"{side}_executors")[0]
    with pytest.raises(ValueError, match="duplicate executor projection key"):
        replace(plan, **{f"{side}_executors": (executor, copy(executor))})
