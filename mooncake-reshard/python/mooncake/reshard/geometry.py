"""Resource-neutral N-D logical-box helpers for reshard planners."""

from __future__ import annotations

import heapq
from array import array
from math import prod
from typing import Iterable, Protocol, Sequence

from ._compat import _strict_zip


_MAX_HIGH_DIMENSIONAL_PAIRWISE_COMPARISONS = 1_000_000


class LogicalBox(Protocol):
    """An address-free logical box supplied by a resource-specific adapter."""

    @property
    def global_offset(self) -> tuple[int, ...]: ...

    @property
    def local_shape(self) -> tuple[int, ...]: ...


class OverlapRegion(Protocol):
    """The logical overlap portion of a resource-specific transfer region."""

    @property
    def overlap_offset(self) -> tuple[int, ...]: ...

    @property
    def overlap_shape(self) -> tuple[int, ...]: ...


def box_contains(
    outer_offset: tuple[int, ...],
    outer_shape: tuple[int, ...],
    inner_offset: tuple[int, ...],
    inner_shape: tuple[int, ...],
) -> bool:
    """Return whether one N-D logical box is wholly inside another."""

    return all(
        outer_begin <= inner_begin
        and inner_begin + inner_extent <= outer_begin + outer_extent
        for outer_begin, outer_extent, inner_begin, inner_extent in _strict_zip(
            outer_offset,
            outer_shape,
            inner_offset,
            inner_shape,
        )
    )


def boxes_overlap(
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    """Return whether any two N-D boxes overlap.

    One-dimensional intervals and two-dimensional rectangles use bounded sweep
    algorithms. Higher-dimensional boxes retain full intersection semantics but
    fail closed after an explicit pairwise-comparison budget rather than making
    unbounded validation work part of the public manifest contract.
    """

    if len(boxes) < 2:
        return False
    ndim = len(boxes[0][0])
    if ndim == 1:
        return _one_dimensional_boxes_overlap(boxes)
    if ndim == 2:
        return _two_dimensional_boxes_overlap(boxes)
    return _high_dimensional_boxes_overlap(boxes)


def _one_dimensional_boxes_overlap(
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    ordered = sorted((offset[0], offset[0] + shape[0]) for offset, shape in boxes)
    previous_end = ordered[0][1]
    for begin, end in ordered[1:]:
        if begin < previous_end:
            return True
        previous_end = max(previous_end, end)
    return False


class _RangeMaxTree:
    """Coordinate-compressed range-add/range-maximum tree for 2-D sweeps."""

    def __init__(self, size: int) -> None:
        if size <= 0:
            raise ValueError("range tree size must be positive")
        capacity = size * 4
        self._size = size
        self._maximum = array("i", [0]) * capacity
        self._lazy = array("i", [0]) * capacity

    def add(self, left: int, right: int, value: int) -> None:
        self._add(1, 0, self._size - 1, left, right, value)

    def maximum(self, left: int, right: int) -> int:
        return self._maximum_in(1, 0, self._size - 1, left, right)

    def _add(
        self,
        node: int,
        begin: int,
        end: int,
        left: int,
        right: int,
        value: int,
    ) -> None:
        if left <= begin and end <= right:
            self._maximum[node] += value
            self._lazy[node] += value
            return
        middle = (begin + end) // 2
        if left <= middle:
            self._add(node * 2, begin, middle, left, right, value)
        if right > middle:
            self._add(node * 2 + 1, middle + 1, end, left, right, value)
        self._maximum[node] = self._lazy[node] + max(
            self._maximum[node * 2], self._maximum[node * 2 + 1]
        )

    def _maximum_in(
        self,
        node: int,
        begin: int,
        end: int,
        left: int,
        right: int,
    ) -> int:
        if left <= begin and end <= right:
            return self._maximum[node]
        middle = (begin + end) // 2
        result = 0
        if left <= middle:
            result = self._maximum_in(node * 2, begin, middle, left, right)
        if right > middle:
            result = max(
                result,
                self._maximum_in(node * 2 + 1, middle + 1, end, left, right),
            )
        return self._lazy[node] + result


def _two_dimensional_boxes_overlap(
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    coordinates = sorted(
        {
            coordinate
            for offset, shape in boxes
            for coordinate in (offset[1], offset[1] + shape[1])
        }
    )
    coordinate_index = {
        coordinate: index for index, coordinate in enumerate(coordinates)
    }
    tree = _RangeMaxTree(len(coordinates) - 1)
    events: list[tuple[int, int, int, int]] = []
    for offset, shape in boxes:
        x_begin = offset[0]
        x_end = offset[0] + shape[0]
        y_begin = coordinate_index[offset[1]]
        y_end = coordinate_index[offset[1] + shape[1]] - 1
        events.append((x_end, 0, y_begin, y_end))
        events.append((x_begin, 1, y_begin, y_end))

    for _, is_start, y_begin, y_end in sorted(events):
        if not is_start:
            tree.add(y_begin, y_end, -1)
            continue
        if tree.maximum(y_begin, y_end) > 0:
            return True
        tree.add(y_begin, y_end, 1)
    return False


def _high_dimensional_boxes_overlap(
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    sweep_dim = max(
        range(len(boxes[0][0])),
        key=lambda dim: len(
            {(offset[dim], offset[dim] + shape[dim]) for offset, shape in boxes}
        ),
    )
    ordered = sorted(
        enumerate(boxes),
        key=lambda item: item[1][0][sweep_dim],
    )
    active: dict[int, tuple[tuple[int, ...], tuple[int, ...]]] = {}
    expirations: list[tuple[int, int]] = []
    comparisons = 0
    for index, (offset, shape) in ordered:
        begin = offset[sweep_dim]
        while expirations and expirations[0][0] <= begin:
            _, expired_index = heapq.heappop(expirations)
            active.pop(expired_index, None)
        for candidate_offset, candidate_shape in active.values():
            if comparisons >= _MAX_HIGH_DIMENSIONAL_PAIRWISE_COMPARISONS:
                raise ValueError(
                    "high-dimensional overlap budget exceeded before validation"
                )
            comparisons += 1
            if all(
                left_begin < right_begin + right_extent
                and right_begin < left_begin + left_extent
                for left_begin, left_extent, right_begin, right_extent in _strict_zip(
                    candidate_offset,
                    candidate_shape,
                    offset,
                    shape,
                )
            ):
                return True
        active[index] = (offset, shape)
        heapq.heappush(expirations, (offset[sweep_dim] + shape[sweep_dim], index))
    return False


def boxes_exactly_cover(
    container_offset: tuple[int, ...],
    container_shape: tuple[int, ...],
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    """Require in-bounds, non-overlapping boxes with exact logical volume."""

    if not boxes:
        return False
    if any(
        not box_contains(container_offset, container_shape, offset, shape)
        for offset, shape in boxes
    ):
        return False
    if sum(prod(shape) for _, shape in boxes) != prod(container_shape):
        return False
    return not boxes_overlap(boxes)


def regions_exactly_cover(
    target: LogicalBox,
    regions: Iterable[OverlapRegion],
) -> bool:
    """Require resource-neutral N-D regions to completely cover one target box."""

    return boxes_exactly_cover(
        target.global_offset,
        target.local_shape,
        tuple((region.overlap_offset, region.overlap_shape) for region in regions),
    )


__all__ = [
    "LogicalBox",
    "OverlapRegion",
    "box_contains",
    "boxes_exactly_cover",
    "boxes_overlap",
    "regions_exactly_cover",
]
