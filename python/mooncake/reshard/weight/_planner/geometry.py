from __future__ import annotations

from dataclasses import dataclass
from math import prod
from typing import TYPE_CHECKING, Optional, Sequence, Union

from ..._typing import TypeAlias

from ..._compat import _strict_zip
from ...contracts import TensorId
from ..types import TensorDescriptor
from ..storage_manifest import StoredFragment
from .fragments import (
    GeometryFragment,
    LogicalSourceFragment,
    LogicalTargetFragment,
)

if TYPE_CHECKING:
    from .contracts import TransferRegion


def _fragment_itemsize(fragment: GeometryFragment) -> int:
    elements = prod(fragment.local_shape)
    if elements <= 0 or fragment.nbytes % elements != 0:
        raise ValueError("transfer region fragment byte size is invalid")
    itemsize = fragment.nbytes // elements
    if itemsize <= 0:
        raise ValueError("transfer region fragment itemsize is invalid")
    return itemsize


def _logical_byte_offset(
    fragment: GeometryFragment,
    overlap_offset: tuple[int, ...],
    itemsize: int,
) -> int:
    linear_offset = 0
    stride = 1
    for global_begin, local_begin, extent in _strict_zip(
        reversed(overlap_offset),
        reversed(fragment.global_offset),
        reversed(fragment.local_shape),
    ):
        linear_offset += (global_begin - local_begin) * stride
        stride *= extent
    return linear_offset * itemsize


def _validate_outer_strides(
    counts: tuple[int, ...],
    strides: tuple[int, ...],
    inner_bytes: int,
    side: str,
) -> None:
    span = inner_bytes
    for count, stride in _strict_zip(reversed(counts), reversed(strides)):
        if count > 1 and stride < span:
            raise ValueError(f"transfer region {side} strides overlap")
        span += (count - 1) * stride


GeometryKey: TypeAlias = tuple[TensorId, tuple[int, ...], tuple[int, ...]]
SourceSortKey: TypeAlias = Union[
    tuple[int, int, int, int, str],
    tuple[
        int,
        int,
        int,
        int,
        str,
        str,
    ],
]


def _geometry_key(fragment: LogicalSourceFragment) -> GeometryKey:
    return fragment.tensor_id, fragment.global_offset, fragment.local_shape


def _source_sort_key(fragment: LogicalSourceFragment) -> SourceSortKey:
    if isinstance(fragment, StoredFragment):
        return (0, 0, 0, 0, fragment.object_key, fragment.fragment_id)
    return (
        fragment.rank.dp,
        fragment.rank.pp,
        fragment.rank.ep,
        fragment.rank.tp,
        fragment.fragment_id,
    )


@dataclass(frozen=True)
class _CandidateInterval:
    begin: int
    end: int
    group: tuple[LogicalSourceFragment, ...]


@dataclass(frozen=True)
class _CandidateIntervalNode:
    center: int
    crossing_by_begin: tuple[_CandidateInterval, ...]
    crossing_by_end: tuple[_CandidateInterval, ...]
    left: Optional[_CandidateIntervalNode] = None
    right: Optional[_CandidateIntervalNode] = None

    @classmethod
    def build(
        cls, intervals: Sequence[_CandidateInterval]
    ) -> Optional[_CandidateIntervalNode]:
        if not intervals:
            return None
        center = sorted((interval.begin + interval.end) // 2 for interval in intervals)[
            len(intervals) // 2
        ]
        left: list[_CandidateInterval] = []
        right: list[_CandidateInterval] = []
        crossing: list[_CandidateInterval] = []
        for interval in intervals:
            if interval.end <= center:
                left.append(interval)
            elif interval.begin > center:
                right.append(interval)
            else:
                crossing.append(interval)
        return cls(
            center=center,
            crossing_by_begin=tuple(
                sorted(crossing, key=lambda item: (item.begin, item.end))
            ),
            crossing_by_end=tuple(
                sorted(crossing, key=lambda item: (item.end, item.begin))
            ),
            left=cls.build(left),
            right=cls.build(right),
        )

    def query(
        self,
        begin: int,
        end: int,
        result: list[tuple[LogicalSourceFragment, ...]],
    ) -> None:
        if end <= self.center:
            for interval in self.crossing_by_begin:
                if interval.begin >= end:
                    break
                result.append(interval.group)
            if self.left is not None:
                self.left.query(begin, end, result)
            return
        if begin > self.center:
            for interval in reversed(self.crossing_by_end):
                if interval.end <= begin:
                    break
                result.append(interval.group)
            if self.right is not None:
                self.right.query(begin, end, result)
            return

        result.extend(interval.group for interval in self.crossing_by_begin)
        if begin < self.center and self.left is not None:
            self.left.query(begin, end, result)
        if self.right is not None:
            self.right.query(begin, end, result)


@dataclass(frozen=True)
class _CandidateBoxIndex:
    dimension: int
    root: _CandidateIntervalNode

    @classmethod
    def build(
        cls, groups: Sequence[tuple[LogicalSourceFragment, ...]]
    ) -> _CandidateBoxIndex:
        if not groups:
            raise ValueError("candidate box index requires source fragments")
        ndim = len(groups[0][0].global_offset)
        dimension = max(
            range(ndim),
            key=lambda dim: (
                len(
                    {
                        (
                            group[0].global_offset[dim],
                            group[0].global_offset[dim] + group[0].local_shape[dim],
                        )
                        for group in groups
                    }
                ),
                -dim,
            ),
        )
        root = _CandidateIntervalNode.build(
            tuple(
                _CandidateInterval(
                    begin=group[0].global_offset[dimension],
                    end=(
                        group[0].global_offset[dimension]
                        + group[0].local_shape[dimension]
                    ),
                    group=group,
                )
                for group in groups
            )
        )
        assert root is not None
        return cls(dimension=dimension, root=root)

    def query(
        self, target: LogicalTargetFragment
    ) -> tuple[tuple[LogicalSourceFragment, ...], ...]:
        begin = target.global_offset[self.dimension]
        result: list[tuple[LogicalSourceFragment, ...]] = []
        self.root.query(
            begin,
            begin + target.local_shape[self.dimension],
            result,
        )
        return tuple(result)


def _overlap_box(
    source: GeometryFragment,
    target: GeometryFragment,
) -> Optional[tuple[tuple[int, ...], tuple[int, ...]]]:
    overlap_offset = tuple(
        max(source_begin, target_begin)
        for source_begin, target_begin in _strict_zip(
            source.global_offset, target.global_offset
        )
    )
    overlap_end = tuple(
        min(
            source_begin + source_extent,
            target_begin + target_extent,
        )
        for source_begin, source_extent, target_begin, target_extent in _strict_zip(
            source.global_offset,
            source.local_shape,
            target.global_offset,
            target.local_shape,
        )
    )
    overlap_shape = tuple(
        end - begin for begin, end in _strict_zip(overlap_offset, overlap_end)
    )
    if any(extent <= 0 for extent in overlap_shape):
        return None
    return overlap_offset, overlap_shape


def _canonical_byte_strides(shape: tuple[int, ...], itemsize: int) -> tuple[int, ...]:
    result: list[int] = []
    running = itemsize
    for extent in reversed(shape):
        result.append(running)
        running *= extent
    return tuple(reversed(result))


def _derive_region_geometry(
    source: GeometryFragment,
    target: GeometryFragment,
    overlap_offset: tuple[int, ...],
    overlap_shape: tuple[int, ...],
) -> tuple[
    int,
    int,
    int,
    tuple[int, ...],
    tuple[int, ...],
    tuple[int, ...],
]:
    source_itemsize = _fragment_itemsize(source)
    target_itemsize = _fragment_itemsize(target)
    if source_itemsize != target_itemsize:
        raise ValueError("transfer region source and target itemsize differ")

    source_byte_strides = _canonical_byte_strides(source.local_shape, source_itemsize)
    target_byte_strides = _canonical_byte_strides(target.local_shape, target_itemsize)
    source_base_offset = sum(
        (overlap_begin - fragment_begin) * stride
        for overlap_begin, fragment_begin, stride in _strict_zip(
            overlap_offset,
            source.global_offset,
            source_byte_strides,
        )
    )
    target_base_offset = sum(
        (overlap_begin - fragment_begin) * stride
        for overlap_begin, fragment_begin, stride in _strict_zip(
            overlap_offset,
            target.global_offset,
            target_byte_strides,
        )
    )

    suffix_begin = len(overlap_shape) - 1
    inner_bytes = overlap_shape[-1] * source_itemsize
    for dim in range(len(overlap_shape) - 2, -1, -1):
        if (
            source_byte_strides[dim] != inner_bytes
            or target_byte_strides[dim] != inner_bytes
        ):
            break
        inner_bytes *= overlap_shape[dim]
        suffix_begin = dim

    return (
        source_base_offset,
        target_base_offset,
        inner_bytes,
        overlap_shape[:suffix_begin],
        source_byte_strides[:suffix_begin],
        target_byte_strides[:suffix_begin],
    )


def _transfer_region(
    tensor: TensorDescriptor,
    source: LogicalSourceFragment,
    target: LogicalTargetFragment,
    overlap_offset: tuple[int, ...],
    overlap_shape: tuple[int, ...],
) -> "TransferRegion[LogicalSourceFragment, LogicalTargetFragment]":
    from .contracts import TransferRegion as transfer_region

    if (
        _fragment_itemsize(source) != tensor.itemsize
        or _fragment_itemsize(target) != tensor.itemsize
    ):
        raise ValueError("transfer region fragment itemsize differs from descriptor")
    (
        source_base_offset,
        target_base_offset,
        inner_bytes,
        outer_loop_counts,
        source_strides,
        target_strides,
    ) = _derive_region_geometry(source, target, overlap_offset, overlap_shape)

    return transfer_region(
        tensor_id=tensor.tensor_id,
        source=source,
        target=target,
        overlap_offset=overlap_offset,
        overlap_shape=overlap_shape,
        source_base_offset=source_base_offset,
        target_base_offset=target_base_offset,
        inner_bytes=inner_bytes,
        outer_loop_counts=outer_loop_counts,
        source_strides=source_strides,
        target_strides=target_strides,
    )
