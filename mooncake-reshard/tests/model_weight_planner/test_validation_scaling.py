from __future__ import annotations

from copy import copy
from dataclasses import replace

import pytest

from mooncake.reshard.weight import (
    PlacementFragment,
    bind_logical_transfer_plan,
    plan_placement_transfer,
)

from .helpers import (
    RuntimeInputs,
    bound_fragment,
    descriptor,
    runtime_inputs_from_groups,
)


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
def test_public_validation_fragment_equality_scales_linearly(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    original_equal = PlacementFragment.__eq__
    comparisons = 0

    def counted_equal(left, right):
        nonlocal comparisons
        comparisons += 1
        return original_equal(left, right)

    monkeypatch.setattr(PlacementFragment, "__eq__", counted_equal)
    counts = []
    for size in (64, 128):
        source, target = _inputs(size, "source"), _inputs(size, "target")
        comparisons = 0
        logical = plan_placement_transfer(source.placement, target.placement)
        if phase == "binder":
            comparisons = 0
            bind_logical_transfer_plan(
                logical, target.bindings, source_bindings=source.bindings
            )
        counts.append(comparisons)
    assert counts[1] <= counts[0] * 2.5, counts


@pytest.mark.parametrize("side", ["source", "target"])
def test_public_planner_rejects_same_id_forged_fragment(side: str) -> None:
    source, target = _inputs(2, "source"), _inputs(2, "target")
    logical = plan_placement_transfer(source.placement, target.placement)
    operation = logical.operations[0]
    forged_fragment = copy(getattr(operation, side))
    object.__setattr__(forged_fragment, "local_shape", (4, 8))
    forged_operation = copy(operation)
    object.__setattr__(forged_operation, side, forged_fragment)
    with pytest.raises(ValueError, match="fragment snapshots differ"):
        replace(logical, operations=(forged_operation, *logical.operations[1:]))


def test_public_binder_rejects_same_id_forged_target_fragment() -> None:
    source, target = _inputs(2, "source"), _inputs(2, "target")
    logical = plan_placement_transfer(source.placement, target.placement)
    forged_fragment = copy(logical.operations[0].target)
    object.__setattr__(forged_fragment, "local_shape", (4, 8))
    forged_operation = copy(logical.operations[0])
    object.__setattr__(forged_operation, "target", forged_fragment)
    forged = copy(logical)
    object.__setattr__(
        forged, "operations", (forged_operation, *logical.operations[1:])
    )
    with pytest.raises(ValueError, match="fragment snapshots differ"):
        bind_logical_transfer_plan(
            forged, target.bindings, source_bindings=source.bindings
        )
