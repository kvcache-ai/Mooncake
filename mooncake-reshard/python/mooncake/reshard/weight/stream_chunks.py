"""Bounded canonical source views; transport owns registration and lease fences.

Planning retains views of the actual model storage. No encoder, baseline or
full-model arena is constructed. A chunk is a tuple of canonical shard specs.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import is_dataclass, replace
from typing import Any

from ..geometry import box_intersection

DEFAULT_BUCKET_BYTES = 256 * 1024 * 1024


def partition_source_views(shard: Any, axis: int, partitions: int) -> tuple[Any, ...]:
    """Intersect a source shard with equal global-axis partitions without copying.

    Bounds use ``extent * index // partitions``, allowing uneven partitions.
    Empty intersections are omitted; adapter-specific dataclass fields survive.
    """
    if not is_dataclass(shard) or isinstance(shard, type):
        raise TypeError("source shard must be a dataclass instance")
    shape = tuple(shard.tensor.shape)
    offset, global_shape = tuple(shard.global_offset), tuple(shard.global_shape)
    if not shape or len(shape) != len(offset) or len(shape) != len(global_shape):
        raise ValueError("source shard geometry rank mismatch")
    if type(axis) is not int or not 0 <= axis < len(shape):
        raise ValueError("axis outside source shard rank")
    if (
        any(type(n) is not int or n <= 0 for n in shape + global_shape)
        or any(type(o) is not int or o < 0 for o in offset)
        or any(o + n > g for o, n, g in zip(offset, shape, global_shape))
    ):
        raise ValueError("source shard exceeds global geometry")
    extent = global_shape[axis]
    if type(partitions) is not int or not 0 < partitions <= extent:
        raise ValueError("partition count must fit positive global axis extent")
    if partitions == 1:
        return (shard,)
    result = []
    for index in range(partitions):
        lower, upper = extent * index // partitions, extent * (index + 1) // partitions
        partition_offset = [0] * len(shape)
        partition_shape = list(global_shape)
        partition_offset[axis], partition_shape[axis] = lower, upper - lower
        overlap = box_intersection(
            offset, shape, tuple(partition_offset), tuple(partition_shape)
        )
        if overlap is None:
            continue
        begin, end = overlap
        result.append(
            replace(
                shard,
                tensor=shard.tensor.narrow(
                    axis, begin[axis] - offset[axis], end[axis] - begin[axis]
                ),
                global_offset=begin,
            )
        )
    return tuple(result)


def chunk_nbytes(chunk: Iterable[Any]) -> int:
    return int(sum(spec.tensor.numel() * spec.tensor.element_size() for spec in chunk))


def _split(spec: Any, max_bytes: int) -> Iterator[Any]:
    tensor = spec.tensor
    shape = tuple(tensor.shape)
    elements = max_bytes // tensor.element_size()
    if tensor.numel() <= elements:
        yield spec
    elif len(shape) == 1:
        for start in range(0, shape[0], elements):
            yield replace(
                spec,
                tensor=tensor[start : start + elements],
                global_offset=(spec.global_offset[0] + start,),
            )
    elif shape[1] <= elements:
        rows = elements // shape[1]
        for start in range(0, shape[0], rows):
            yield replace(
                spec,
                tensor=tensor[start : start + rows, :],
                global_offset=(spec.global_offset[0] + start, spec.global_offset[1]),
            )
    else:
        for row in range(shape[0]):
            for column in range(0, shape[1], elements):
                yield replace(
                    spec,
                    tensor=tensor[row : row + 1, column : column + elements],
                    global_offset=(
                        spec.global_offset[0] + row,
                        spec.global_offset[1] + column,
                    ),
                )


def plan_chunks(
    shards: Iterable[Any],
    max_bytes: int = DEFAULT_BUCKET_BYTES,
    *,
    group_key: Callable[[Any], object] | None = None,
) -> tuple[tuple[Any, ...], ...]:
    """Preserve source order and optional groups; split oversized rows safely."""
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("bucket capacity must be a positive integer")
    shards = tuple(shards)
    if not shards:
        return ()
    first = shards[0].tensor
    for spec in shards:
        tensor = spec.tensor
        shape = tuple(tensor.shape)
        if len(shape) not in (1, 2) or len(spec.global_offset) != len(shape):
            raise ValueError("canonical source rank must be one or two")
        if any(type(n) is not int or n <= 0 for n in shape):
            raise ValueError("canonical source dimensions must be positive integers")
        if (
            len(spec.global_shape) != len(shape)
            or any(type(n) is not int or n <= 0 for n in spec.global_shape)
            or any(
                o + n > g
                for o, n, g in zip(spec.global_offset, shape, spec.global_shape)
            )
        ):
            raise ValueError("canonical source exceeds global shape")
        if any(type(n) is not int or n < 0 for n in spec.global_offset):
            raise ValueError("canonical source offsets must be nonnegative integers")
        if tensor.device != first.device:
            raise ValueError("bounded source requires a uniform device")
        if tensor.stride()[-1] != 1:
            raise ValueError(
                "canonical source innermost dimension must have unit stride"
            )
        if max_bytes < tensor.element_size():
            raise ValueError("bucket cannot fit one source element")
    groups: list[tuple[object, list[Any]]] = []
    for spec in shards:
        key = group_key(spec) if group_key is not None else spec.tensor_id
        if not groups or groups[-1][0] != key:
            groups.append((key, []))
        groups[-1][1].append(spec)
    chunks: list[tuple[Any, ...]] = []
    pending: list[Any] = []
    used = 0
    for _key, group in groups:
        group_bytes = chunk_nbytes(group)
        if pending and (
            used + group_bytes > max_bytes
            or pending[0].tensor.dtype != group[0].tensor.dtype
        ):
            chunks.append(tuple(pending))
            pending, used = [], 0
        for spec in group:
            for piece in _split(spec, max_bytes):
                size = chunk_nbytes((piece,))
                if pending and (
                    used + size > max_bytes
                    or pending[0].tensor.dtype != piece.tensor.dtype
                ):
                    chunks.append(tuple(pending))
                    pending, used = [], 0
                pending.append(piece)
                used += size
    if pending:
        chunks.append(tuple(pending))
    return tuple(chunks)


def _pad2(values: Sequence[int]) -> tuple[int, ...]:
    # Match the existing encoder's published vector geometry, including offsets.
    return tuple(values) if len(values) == 2 else (values[0], 1)


def chunk_table(chunk: Sequence[Any]) -> dict[str, Any]:
    """Construct a bucket-local directory with the original public planner."""
    from mooncake.reshard.weight import TiledCOOLayout

    return TiledCOOLayout.build(
        tuple(s.tensor_id for s in chunk),
        tuple(_pad2(tuple(s.tensor.shape)) for s in chunk),
        tuple(
            tuple(s.global_offset)
            if len(s.global_offset) == 2
            else (s.global_offset[0], 0)
            for s in chunk
        ),
    ).to_dict()


def pack_bucket(chunk: Sequence[Any], arena: Any, torch: Any) -> Any:
    """Enqueue exact strided source copies; caller supplies producer-ready fence.

    The returned byte view aliases the registered slot. The caller must own a
    reusable slot lease before calling and keep it until every consumer ACK.
    """
    if not chunk:
        raise ValueError("cannot pack an empty source bucket")
    size = chunk_nbytes(chunk)
    if (
        arena.dtype != torch.uint8
        or arena.dim() != 1
        or not arena.is_contiguous()
        or size > arena.numel()
    ):
        raise ValueError("source slot must be a sufficient contiguous byte arena")
    dtype = chunk[0].tensor.dtype
    destinations: list[Any] = []
    sources: list[Any] = []
    cursor = 0
    for spec in chunk:
        tensor = spec.tensor
        if tensor.dtype != dtype or tensor.device != arena.device:
            raise ValueError("source bucket dtype or device changed")
        nbytes = tensor.numel() * tensor.element_size()
        destinations.append(
            arena[cursor : cursor + nbytes]
            .view(dtype)
            .reshape(tuple(tensor.shape))
            .view(torch.uint8)
        )
        sources.append(tensor.view(torch.uint8))
        cursor += nbytes
    torch._foreach_copy_(destinations, sources)
    return arena[:size]


def iter_packed_chunks(
    chunks: Sequence[Sequence[Any]],
    slots: Sequence[Any],
    *,
    acquire: Callable[[int], object],
    ready: Callable[[], object],
    torch_module: Any,
) -> Iterator[tuple[int, Sequence[Any], Any]]:
    """Pack leased slots and drain consumption before returning.

    ``acquire(ordinal)`` must wait until that ring slot is safe to overwrite.
    ``ready()`` fences producer copies before the yielded chunk is published.
    The caller owns publication, error propagation, and distributed control.
    Closing or failing the iterator never acknowledges unfinished consumption.
    Existing live source storage can bypass packing and use Reader directly.
    """
    if not chunks:
        return
    if not slots:
        raise ValueError("At least one source slot is required")
    count = len(slots)
    for ordinal, chunk in enumerate(chunks):
        acquire(ordinal)
        packed = pack_bucket(chunk, slots[ordinal % count], torch_module)
        ready()
        yield ordinal, chunk, packed
    for ordinal in range(max(0, len(chunks) - count), len(chunks)):
        acquire(ordinal + count)
