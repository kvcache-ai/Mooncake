"""Stored sources are logical boxes, independent of training rank semantics."""

from __future__ import annotations

from dataclasses import replace
from itertools import product
from math import prod

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    SplitAxis,
    TensorDescriptor,
    plan_stored_transfer_to_target_placement,
)
from mooncake.reshard.weight.storage_manifest import (
    StoredFragmentSnapshot,
    StoredWeightManifest,
)

from .helpers import global_placement_from_fragments


def _boxes(count: int, dim: int):
    shape = (19, 23)
    boundaries = [shape[dim] * index // count for index in range(count + 1)]
    for begin, end in zip(boundaries, boundaries[1:]):
        offset = [0, 0]
        extent = list(shape)
        offset[dim] = begin
        extent[dim] = end - begin
        yield tuple(offset), tuple(extent)


def _reference_bytes(offset: tuple[int, ...], shape: tuple[int, ...]) -> bytes:
    # The oracle uses global coordinates, independently of planner lowering.
    return b"".join(
        ((offset[0] + row) * 23 + offset[1] + col).to_bytes(4, "little")
        for row, col in product(range(shape[0]), range(shape[1]))
    )


def _layouts(source_count: int, target_count: int, source_dim: int, target_dim: int):
    tensor = TensorDescriptor(
        tensor_id="tensor-a",
        global_shape=(19, 23),
        dtype="uint32",
        itemsize=4,
        shard_dims=(),
        layout_fingerprint="contiguous-u32",
        parallel_axes=(),
    )
    group = "weights/example/revision-1/generation-1"
    object_key = f"{group}/payload/packed"
    payload = bytearray(b"\xff" * 16)
    source_fragments = []
    for index, (offset, shape) in enumerate(_boxes(source_count, source_dim)):
        value = _reference_bytes(offset, shape)
        source_fragments.append(
            StoredFragmentSnapshot(
                fragment_id=f"fragment-{index}",
                tensor_id=tensor.tensor_id,
                global_offset=offset,
                local_shape=shape,
                object_key=object_key,
                object_offset=len(payload),
                nbytes=len(value),
            )
        )
        payload.extend(value)
        payload.extend(b"\xee" * 8)
    source = StoredWeightManifest(
        namespace="example",
        resource_id="model-a",
        revision="revision-1",
        weight_generation=1,
        group_id=group,
        manifest_key=f"{group}/manifest",
        tensors=(tensor,),
        fragments=tuple(source_fragments),
        created_at="2026-10-21T00:00:00Z",
    )
    target_tensor = replace(
        tensor,
        shard_dims=(target_dim,),
        parallel_axes=(SplitAxis(kind="tp", dim=target_dim),),
    )
    target = global_placement_from_fragments(
        resource_id=source.resource_id,
        revision=source.revision,
        weight_generation=source.weight_generation,
        placement_set_id="target-layout",
        tensors=(target_tensor,),
        fragments=tuple(
            PlacementFragment(
                placement_fragment_id=f"target-{rank}",
                tensor_id=tensor.tensor_id,
                global_offset=offset,
                local_shape=shape,
                nbytes=prod(shape) * tensor.itemsize,
                rank=ParallelRank(tp=rank),
            )
            for rank, (offset, shape) in enumerate(_boxes(target_count, target_dim))
        ),
    )
    return source, target, {object_key: bytes(payload)}


@pytest.mark.parametrize("source_count,target_count", [(4, 8), (8, 4)])
@pytest.mark.parametrize("source_dim,target_dim", list(product(range(2), repeat=2)))
def test_stored_geometry_reconstructs_different_target_layouts(
    source_count: int, target_count: int, source_dim: int, target_dim: int
) -> None:
    source, target, objects = _layouts(
        source_count, target_count, source_dim, target_dim
    )
    assert source.tensors[0].parallel_axes == ()
    assert all(not hasattr(fragment, "rank") for fragment in source.fragments)
    # Persisted identities, fragment order and padding must not become implicit
    # source-rank or partition assumptions.
    round_tripped = StoredWeightManifest.from_json(source.to_json())
    for manifest in (round_tripped, replace(source, fragments=source.fragments[::-1])):
        plan = plan_stored_transfer_to_target_placement(manifest, target)
        assert plan.source_placement is None
        assert plan.source_executors == ()
        assert plan.total_bytes == 19 * 23 * 4
        assert {operation.source.fragment_id for operation in plan.operations} == {
            fragment.fragment_id for fragment in source.fragments
        }
        output = {
            fragment.fragment_id: bytearray(fragment.nbytes)
            for fragment in target.fragments
        }
        writes = {
            fragment.fragment_id: [0] * fragment.nbytes for fragment in target.fragments
        }
        for operation in plan.operations:
            fragment = operation.source
            assert isinstance(fragment, StoredFragmentSnapshot)
            for src, dst, length in operation.iter_segments(max_segments=1000):
                assert 0 <= src < src + length <= fragment.nbytes
                assert 0 <= dst < dst + length <= operation.target.nbytes
                begin = fragment.object_offset + src
                output[operation.target.fragment_id][dst : dst + length] = objects[
                    fragment.object_key
                ][begin : begin + length]
                for index in range(dst, dst + length):
                    writes[operation.target.fragment_id][index] += 1
        for fragment in target.fragments:
            assert output[fragment.fragment_id] == _reference_bytes(
                fragment.global_offset, fragment.local_shape
            )
            assert writes[fragment.fragment_id] == [1] * fragment.nbytes


def test_stored_geometry_rejects_missing_source_box() -> None:
    source, _, _ = _layouts(4, 8, 0, 1)
    with pytest.raises(ValueError, match="not fully covered"):
        replace(source, fragments=source.fragments[:-1])


def test_stored_geometry_rejects_overlap_and_matching_gap() -> None:
    source, _, _ = _layouts(4, 8, 0, 1)
    second = source.fragments[1]
    overlapping = replace(second, global_offset=(second.global_offset[0] - 1, 0))
    with pytest.raises(ValueError, match="not fully covered"):
        replace(
            source, fragments=(source.fragments[0], overlapping, *source.fragments[2:])
        )


def test_stored_geometry_rejects_generation_mismatch() -> None:
    source, target, _ = _layouts(4, 8, 0, 1)
    with pytest.raises(ValueError, match="weight_generation differ"):
        plan_stored_transfer_to_target_placement(
            replace(source, weight_generation=2), target
        )
