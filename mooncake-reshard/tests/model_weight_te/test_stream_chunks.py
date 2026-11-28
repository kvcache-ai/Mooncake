from dataclasses import dataclass

import pytest
import torch
from mooncake.reshard.weight.stream_chunks import (
    chunk_nbytes,
    chunk_table,
    iter_packed_chunks,
    pack_bucket,
    partition_source_views,
    plan_chunks,
)


@dataclass(frozen=True)
class Shard:
    tensor_id: str
    tensor: object
    global_shape: tuple
    global_offset: tuple


@dataclass(frozen=True)
class OwnedShard(Shard):
    owner: object


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("partial", [False, True])
def test_partition_source_views_preserves_coverage_and_alias(axis, partial):
    source = torch.arange(77, dtype=torch.float64).reshape(7, 11)
    tensor = source[1:6, 2:10] if partial else source
    offset = (1, 2) if partial else (0, 0)
    owner = object()
    shard = OwnedShard("weight", tensor, (7, 11), offset, owner)
    pieces = partition_source_views(shard, axis, 4)
    assert chunk_nbytes(pieces) == chunk_nbytes((shard,))
    coverage = torch.zeros_like(source, dtype=torch.int32)
    for piece in pieces:
        assert piece.owner is owner
        assert piece.global_shape == shard.global_shape
        assert piece.tensor_id == shard.tensor_id
        assert piece.tensor.dtype == tensor.dtype
        assert (
            piece.tensor.untyped_storage().data_ptr()
            == tensor.untyped_storage().data_ptr()
        )
        index = tuple(
            slice(o, o + n) for o, n in zip(piece.global_offset, piece.tensor.shape)
        )
        assert torch.equal(piece.tensor, source[index])
        coverage[index] += 1
    expected = torch.zeros_like(coverage)
    expected[tuple(slice(o, o + n) for o, n in zip(offset, tensor.shape))] = 1
    assert torch.equal(coverage, expected)
    pieces[0].tensor.fill_(-1)
    assert tensor.flatten()[0].item() == -1


def test_partition_source_views_vector_empty_intersections_and_identity():
    shard = Shard("vector", torch.arange(3), (13,), (5,))
    pieces = partition_source_views(shard, 0, 5)
    assert [piece.global_offset for piece in pieces] == [(5,), (7,)]
    assert [piece.tensor.numel() for piece in pieces] == [2, 1]
    assert torch.equal(torch.cat([piece.tensor for piece in pieces]), shard.tensor)
    assert partition_source_views(shard, 0, 1)[0] is shard


@pytest.mark.parametrize(
    "axis, partitions",
    [(-1, 2), (2, 2), (True, 2), (0, 0), (0, 8), (0, True), (0, 1.5)],
)
def test_partition_source_views_rejects_invalid_partition(axis, partitions):
    with pytest.raises(ValueError):
        partition_source_views(
            Shard("weight", torch.ones(3, 4), (7, 9), (1, 2)), axis, partitions
        )


@pytest.mark.parametrize(
    "shape, offset",
    [
        ((3,), (0, 0)),
        ((3, 4), (-1, 0)),
        ((3, 4), (1, 0)),
        ((3, 4), (0, 0.5)),
        ((3, False), (0, 0)),
    ],
)
def test_partition_source_views_rejects_invalid_geometry(shape, offset):
    with pytest.raises(ValueError):
        partition_source_views(Shard("weight", torch.ones(3, 4), shape, offset), 0, 1)


def test_partition_source_views_rejects_empty_tensor_and_non_dataclass():
    with pytest.raises(ValueError):
        partition_source_views(Shard("weight", torch.empty(0), (3,), (0,)), 0, 1)
    with pytest.raises(TypeError):
        partition_source_views(object(), 0, 1)


def test_framework_neutral_chunk_roundtrip():
    values = [
        torch.arange(21, dtype=torch.float64).view(3, 7),
        torch.arange(13, dtype=torch.int16),
    ]
    chunks = plan_chunks(
        [Shard(str(i), t, tuple(t.shape), (0,) * t.ndim) for i, t in enumerate(values)],
        16,
    )
    restored = [torch.empty_like(t) for t in values]
    for chunk in chunks:
        assert chunk_nbytes(chunk) <= 16
        packed = pack_bucket(chunk, torch.empty(16, dtype=torch.uint8), torch)
        cursor = 0
        for spec in chunk:
            size = spec.tensor.numel() * spec.tensor.element_size()
            index = tuple(
                slice(o, o + n) for o, n in zip(spec.global_offset, spec.tensor.shape)
            )
            restored[int(spec.tensor_id)][index] = (
                packed[cursor : cursor + size]
                .view(spec.tensor.dtype)
                .view(spec.tensor.shape)
            )
            cursor += size
    assert all(torch.equal(a, b) for a, b in zip(values, restored))
    from mooncake.reshard.weight import TiledCOOLayout

    assert TiledCOOLayout.from_dict(chunk_table(chunks[0])).records


@pytest.mark.parametrize("slots_count", [1, 2, 4])
def test_packed_stream_waits_before_copy_and_drains(slots_count):
    source = torch.arange(18, dtype=torch.float32)
    chunks = plan_chunks([Shard("weight", source, (18,), (0,))], 16)
    slots = tuple(torch.empty(16, dtype=torch.uint8) for _ in range(slots_count))
    events, received = [], []

    def acquire(ordinal):
        events.append(("wait", ordinal))

    def ready():
        events.append(("ready",))

    for ordinal, chunk, used in iter_packed_chunks(
        chunks, slots, acquire=acquire, ready=ready, torch_module=torch
    ):
        assert events[-2:] == [("wait", ordinal), ("ready",)]
        assert used.data_ptr() == slots[ordinal % slots_count].data_ptr()
        received.append(used.clone().view(torch.float32))
    assert torch.equal(torch.cat(received), source)
    assert events[-min(len(chunks), slots_count) :] == [
        ("wait", i + slots_count)
        for i in range(max(0, len(chunks) - slots_count), len(chunks))
    ]


def test_stream_failed_publication_does_not_drain_or_advance():
    source = torch.arange(12, dtype=torch.float32)
    chunks = plan_chunks([Shard("weight", source, (12,), (0,))], 16)
    waits = []
    iterator = iter_packed_chunks(
        chunks,
        (torch.empty(16, dtype=torch.uint8),),
        acquire=waits.append,
        ready=lambda: None,
        torch_module=torch,
    )
    next(iterator)
    iterator.close()
    assert waits == [0]
    with pytest.raises(ValueError, match="slot"):
        next(
            iter_packed_chunks(
                chunks, (), acquire=waits.append, ready=lambda: None, torch_module=torch
            )
        )


def test_empty_writer_needs_no_slots_or_callbacks():
    def unexpected(*args):
        raise AssertionError("Empty stream must not wait or copy")

    assert (
        list(
            iter_packed_chunks(
                (), (), acquire=unexpected, ready=unexpected, torch_module=None
            )
        )
        == []
    )
