from __future__ import annotations

from itertools import islice, product
from dataclasses import replace
from math import prod

import pytest

from mooncake.reshard.weight._planner.contracts import ExecutableTransferOperation

from .helpers import bound_fragment


def make_operation(counts: tuple[int, ...]) -> ExecutableTransferOperation:
    def fragment(name: str, width: int, last_offset: int, address: int):
        shape = (*counts, width)
        return bound_fragment(
            fragment_id=name,
            tensor_id="tensor",
            global_offset=(*((0,) * len(counts)), last_offset),
            local_shape=shape,
            address=address,
            nbytes=prod(shape) * 2,
            worker_id=name,
            endpoint=f"{name}:12345",
        )

    def strides(width: int) -> tuple[int, ...]:
        return tuple(
            prod(counts[index + 1 :]) * width * 2 for index in range(len(counts))
        )

    return ExecutableTransferOperation(
        tensor_id="tensor",
        source=fragment("source", 8, 0, 0x10000),
        target=fragment("target", 6, 1, 0x20000),
        overlap_offset=(*((0,) * len(counts)), 2),
        overlap_shape=(*counts, 2),
        source_base_offset=4,
        target_base_offset=2,
        inner_bytes=4,
        outer_loop_counts=counts,
        source_strides=strides(8),
        target_strides=strides(6),
    )


def old_algorithm(operation: ExecutableTransferOperation):
    for indices in product(*(range(count) for count in operation.outer_loop_counts)):
        yield (
            operation.source_base_offset
            + sum(
                index * stride
                for index, stride in zip(indices, operation.source_strides, strict=True)
            ),
            operation.target_base_offset
            + sum(
                index * stride
                for index, stride in zip(indices, operation.target_strides, strict=True)
            ),
            operation.inner_bytes,
        )


@pytest.mark.parametrize(
    "counts",
    [(), (1,), (9,), (2, 3), (2, 1, 3), (3, 4, 5), (1, 1, 1, 1), (3, 33, 100)],
)
def test_segment_iteration_matches_independent_old_algorithm(counts):
    operation = make_operation(counts)
    actual = operation.iter_segments(max_segments=operation.segment_count)
    expected = old_algorithm(operation)
    while True:
        actual_batch = tuple(islice(actual, 8192))
        expected_batch = tuple(islice(expected, 8192))
        assert actual_batch == expected_batch
        if not actual_batch:
            break


@pytest.mark.parametrize("budget", [0, -1, True, 1.0, "2", None])
def test_segment_iteration_rejects_invalid_budget_before_yield(budget):
    with pytest.raises(ValueError, match="positive integer"):
        next(iter(make_operation((2, 3)).iter_segments(max_segments=budget)))


def test_segment_iteration_rejects_insufficient_budget_before_yield():
    operation = make_operation((2, 3))
    with pytest.raises(ValueError, match="exceeds max_segments"):
        next(iter(operation.iter_segments(max_segments=5)))


def test_singleton_zero_stride_is_rejected_as_noncanonical():
    with pytest.raises(ValueError, match="loop geometry is not canonical"):
        replace(make_operation((1,)), source_strides=(0,), target_strides=(0,))
