from __future__ import annotations

import sys

import numpy as np
import pytest
from mooncake._partial_read import plan_matrix_read


def _execute_plan(source: np.ndarray, plan) -> np.ndarray:
    assert plan.ranges is not None
    source_bytes = source.tobytes(order="C")
    output = bytearray(plan.nbytes)
    for source_offset, destination_offset, size in plan.ranges:
        output[destination_offset : destination_offset + size] = source_bytes[
            source_offset : source_offset + size
        ]
    return np.frombuffer(output, dtype=source.dtype).reshape(plan.shape)


@pytest.mark.parametrize(
    ("shape", "dtype", "rows", "columns", "expected_shape", "expected_ranges"),
    [
        (
            (4, 5),
            np.int16,
            (3, 0, 3),
            slice(1, 4),
            (3, 3),
            ((32, 0, 6), (2, 6, 6), (32, 12, 6)),
        ),
        ((4, 6), np.int32, (2,), slice(1, 5), (1, 4), None),
        ((4, 6), np.int32, (3, 0, 1), slice(4, 5), (3, 1), None),
        (
            (6, 3),
            np.uint8,
            (2, 3, 4, 1, 1, 2),
            slice(None),
            (6, 3),
            ((6, 0, 9), (3, 9, 3), (3, 12, 6)),
        ),
    ],
)
def test_matrix_plan_matches_numpy(
    shape, dtype, rows, columns, expected_shape, expected_ranges
) -> None:
    source = np.arange(np.prod(shape), dtype=dtype).reshape(shape)
    plan = plan_matrix_read(shape, source.itemsize, rows, columns, max_ranges=len(rows))

    assert plan.shape == expected_shape
    if expected_ranges is not None:
        assert plan.ranges == expected_ranges
    np.testing.assert_array_equal(
        _execute_plan(source, plan), source[list(rows), columns]
    )


@pytest.mark.parametrize(
    ("rows", "columns", "expected_shape"),
    [
        ((), slice(1, 3), (0, 2)),
        ((0, 1), slice(2, 2), (2, 0)),
    ],
)
def test_empty_selection_has_shape_but_no_ranges(rows, columns, expected_shape) -> None:
    plan = plan_matrix_read(
        shape=(3, 4),
        itemsize=4,
        rows=rows,
        columns=columns,
        max_ranges=0,
    )

    assert plan.shape == expected_shape
    assert plan.nbytes == 0
    assert plan.ranges == ()


def test_slice_bounds_are_normalized_like_numpy() -> None:
    source = np.arange(20, dtype=np.int16).reshape(4, 5)

    plan = plan_matrix_read(
        source.shape,
        source.itemsize,
        rows=(2, 0),
        columns=slice(-3, 99),
        max_ranges=16,
    )

    np.testing.assert_array_equal(_execute_plan(source, plan), source[[2, 0], -3:99])


def test_range_cap_returns_fallback_without_partial_ranges() -> None:
    plan = plan_matrix_read(
        shape=(5, 4),
        itemsize=4,
        rows=(0, 1),
        columns=slice(1, 3),
        max_ranges=1,
    )

    assert plan.shape == (2, 2)
    assert plan.ranges is None


def test_contiguous_full_rows_use_one_range() -> None:
    plan = plan_matrix_read(
        shape=(8, 4),
        itemsize=2,
        rows=range(2, 6),
        columns=slice(None),
        max_ranges=1,
    )

    assert plan.ranges == ((16, 0, 32),)
    assert (
        plan_matrix_read((8, 4), 2, range(2, 6), slice(None), max_ranges=0).ranges
        is None
    )


@pytest.mark.parametrize(
    ("shape", "rows", "columns", "error"),
    [
        ((3,), (), slice(None), ValueError),
        ((3, 4, 5), (), slice(None), ValueError),
        ((3, 4), (), slice(None, None, 2), ValueError),
        ((3, 4), range(2, -1, -1), slice(None), ValueError),
        ((3, 4), (3,), slice(None), IndexError),
        ((sys.maxsize, 2), (), slice(None), OverflowError),
    ],
)
def test_invalid_shape_step_and_rows_raise(shape, rows, columns, error) -> None:
    with pytest.raises(error):
        plan_matrix_read(
            shape=shape,
            itemsize=4,
            rows=rows,
            columns=columns,
            max_ranges=16,
        )
