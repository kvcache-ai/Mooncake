from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from hypothesis import given, settings, strategies as st
import pytest

import mooncake.reshard.geometry as geometry
from mooncake.reshard.geometry import (
    LogicalBox,
    OverlapRegion,
    boxes_exactly_cover,
    regions_exactly_cover,
)


@dataclass(frozen=True)
class _Box:
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]


@dataclass(frozen=True)
class _Region:
    overlap_offset: tuple[int, ...]
    overlap_shape: tuple[int, ...]


def test_geometry_contract_is_structural_and_resource_agnostic() -> None:
    target: LogicalBox = _Box(global_offset=(8, 4), local_shape=(2, 6))
    regions: tuple[OverlapRegion, ...] = (
        _Region(overlap_offset=(8, 4), overlap_shape=(1, 6)),
        _Region(overlap_offset=(9, 4), overlap_shape=(1, 6)),
    )

    assert regions_exactly_cover(target, regions)
    assert boxes_exactly_cover(
        target.global_offset,
        target.local_shape,
        tuple((region.overlap_offset, region.overlap_shape) for region in regions),
    )
    assert not regions_exactly_cover(target, (regions[0], regions[0], regions[1]))


def _row_split_exact_cover(
    row_count: int,
    *,
    ndim: int = 2,
) -> tuple[
    tuple[int, ...],
    tuple[int, ...],
    tuple[tuple[tuple[int, ...], tuple[int, ...]], ...],
]:
    if ndim < 2:
        raise ValueError("row split requires at least two dimensions")
    container_offset = (0,) * ndim
    container_shape = (row_count + 1, row_count, *((1,) * (ndim - 2)))
    boxes = []
    for row in range(row_count):
        split = row + 1
        left_offset = (0, row, *((0,) * (ndim - 2)))
        right_offset = (split, row, *((0,) * (ndim - 2)))
        left_shape = (split, 1, *((1,) * (ndim - 2)))
        right_shape = (row_count + 1 - split, 1, *((1,) * (ndim - 2)))
        boxes.extend(((left_offset, left_shape), (right_offset, right_shape)))
    return container_offset, container_shape, tuple(boxes)


def test_boxes_exactly_cover_row_split_layout_within_two_dimensional_budget() -> None:
    container_offset, container_shape, boxes = _row_split_exact_cover(4096)

    started_at = perf_counter()
    assert boxes_exactly_cover(container_offset, container_shape, boxes)
    assert perf_counter() - started_at < 2.0


def test_high_dimensional_overlap_fails_closed_after_comparison_budget(
    monkeypatch,
) -> None:
    _, _, boxes = _row_split_exact_cover(3, ndim=3)
    monkeypatch.setattr(
        geometry,
        "_MAX_HIGH_DIMENSIONAL_PAIRWISE_COMPARISONS",
        0,
        raising=False,
    )

    with pytest.raises(ValueError, match="high-dimensional overlap budget"):
        geometry.boxes_overlap(boxes)


@st.composite
def _small_boxes(draw) -> tuple[tuple[tuple[int, ...], tuple[int, ...]], ...]:
    ndim = draw(st.integers(min_value=1, max_value=3))
    count = draw(st.integers(min_value=0, max_value=8))
    boxes = []
    for _ in range(count):
        boxes.append(
            (
                tuple(draw(st.integers(min_value=0, max_value=6)) for _ in range(ndim)),
                tuple(draw(st.integers(min_value=1, max_value=4)) for _ in range(ndim)),
            )
        )
    return tuple(boxes)


def _brute_boxes_overlap(
    boxes: tuple[tuple[tuple[int, ...], tuple[int, ...]], ...],
) -> bool:
    return any(
        all(
            left_begin < right_begin + right_extent
            and right_begin < left_begin + left_extent
            for left_begin, left_extent, right_begin, right_extent in zip(
                left_offset,
                left_shape,
                right_offset,
                right_shape,
            )
        )
        for index, (left_offset, left_shape) in enumerate(boxes)
        for right_offset, right_shape in boxes[index + 1 :]
    )


@settings(max_examples=200, deadline=None)
@given(_small_boxes())
def test_boxes_overlap_matches_brute_force_for_small_inputs(boxes) -> None:
    assert geometry.boxes_overlap(boxes) is _brute_boxes_overlap(boxes)


@st.composite
def _tiled_target_and_regions(draw) -> tuple[_Box, tuple[_Region, ...]]:
    ndim = draw(st.integers(min_value=1, max_value=3))
    split_dim = draw(st.integers(min_value=0, max_value=ndim - 1))
    origin = tuple(draw(st.integers(min_value=0, max_value=8)) for _ in range(ndim))
    shape = tuple(
        draw(st.integers(min_value=2 if dim == split_dim else 1, max_value=6))
        for dim in range(ndim)
    )
    split = draw(st.integers(min_value=1, max_value=shape[split_dim] - 1))
    left_shape = tuple(
        split if dim == split_dim else extent for dim, extent in enumerate(shape)
    )
    right_offset = tuple(
        origin[dim] + split if dim == split_dim else origin[dim] for dim in range(ndim)
    )
    right_shape = tuple(
        extent - split if dim == split_dim else extent
        for dim, extent in enumerate(shape)
    )
    return _Box(origin, shape), (
        _Region(origin, left_shape),
        _Region(right_offset, right_shape),
    )


@settings(max_examples=80, deadline=None)
@given(_tiled_target_and_regions())
def test_regions_exactly_cover_random_n_dim_tilings(
    tiled: tuple[_Box, tuple[_Region, ...]],
) -> None:
    target, regions = tiled

    assert regions_exactly_cover(target, regions)
    assert not regions_exactly_cover(target, regions[:1])
