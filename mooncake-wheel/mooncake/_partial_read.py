from __future__ import annotations

import operator
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class MatrixReadPlan:
    shape: tuple[int, int]
    nbytes: int
    ranges: tuple[tuple[int, int, int], ...] | None


def plan_matrix_read(
    shape: tuple[int, int],
    itemsize: int,
    rows: range | tuple[int, ...],
    columns: slice,
    max_ranges: int,
) -> MatrixReadPlan:
    if len(shape) != 2:
        raise ValueError("matrix partial reads require a two-dimensional shape")
    row_count, column_count = (operator.index(value) for value in shape)
    if row_count < 0 or column_count < 0 or itemsize <= 0:
        raise ValueError("matrix shape and itemsize must be non-negative")
    if not isinstance(rows, (range, tuple)) or not isinstance(columns, slice):
        raise TypeError("matrix rows and columns have invalid types")
    if isinstance(rows, range) and rows.step <= 0:
        raise ValueError("rows range step must be positive")
    if rows:
        bounds = (rows[0], rows[-1]) if isinstance(rows, range) else rows
        if min(bounds) < 0 or max(bounds) >= row_count:
            raise IndexError("row index out of range")
    start, stop, step = columns.indices(column_count)
    if step != 1:
        raise ValueError("column slice step must be None or 1")

    output_shape = (len(rows), max(0, stop - start))
    row_bytes = column_count * itemsize
    selected_bytes = output_shape[1] * itemsize
    output_bytes = output_shape[0] * selected_bytes
    if max(row_count * row_bytes, output_bytes) > sys.maxsize:
        raise OverflowError("matrix byte size exceeds platform limits")
    if output_bytes == 0:
        return MatrixReadPlan(output_shape, 0, ())
    if selected_bytes == row_bytes:
        ranges = []
        previous = None
        run_start = run_length = 0
        for output, row in enumerate(map(operator.index, rows)):
            if previous is None or row != previous + 1:
                if previous is not None:
                    ranges.append(
                        (
                            run_start * row_bytes,
                            (output - run_length) * row_bytes,
                            run_length * row_bytes,
                        )
                    )
                    if len(ranges) > max_ranges:
                        return MatrixReadPlan(output_shape, output_bytes, None)
                run_start, run_length = row, 1
            else:
                run_length += 1
            previous = row
        ranges.append(
            (
                run_start * row_bytes,
                (len(rows) - run_length) * row_bytes,
                run_length * row_bytes,
            )
        )
        if len(ranges) > max_ranges:
            return MatrixReadPlan(output_shape, output_bytes, None)
        return MatrixReadPlan(output_shape, output_bytes, tuple(ranges))
    if len(rows) > max_ranges:
        return MatrixReadPlan(output_shape, output_bytes, None)
    return MatrixReadPlan(
        output_shape,
        output_bytes,
        tuple(
            (
                row * row_bytes + start * itemsize,
                output * selected_bytes,
                selected_bytes,
            )
            for output, row in enumerate(map(operator.index, rows))
        ),
    )
