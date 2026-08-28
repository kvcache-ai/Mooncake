from __future__ import annotations

from dataclasses import replace
from math import prod

import pytest

import mooncake.reshard.weight._planner.contracts as planner_contracts
from mooncake.reshard.weight._planner.contracts import (
    BoundWeightFragment,
    TransferRegion,
)
from mooncake.reshard.weight.manifest import ParallelRank

from .helpers import bound_fragment


def test_canonical_contract_exposes_only_n_dim_transfer_regions() -> None:
    assert not hasattr(planner_contracts, "CopyRange")


def n_dim_fragment(
    *,
    fragment_id: str,
    global_offset: tuple[int, ...],
    local_shape: tuple[int, ...],
    address: int,
) -> BoundWeightFragment:
    return bound_fragment(
        fragment_id=fragment_id,
        tensor_id="layers.2.experts.w1",
        global_offset=global_offset,
        local_shape=local_shape,
        address=address,
        nbytes=2 * prod(local_shape),
        worker_id=fragment_id,
        endpoint=f"{fragment_id}:12345",
        device="cuda:0",
        rank=ParallelRank(),
    )


@pytest.mark.parametrize(
    (
        "target_offset",
        "target_shape",
        "overlap_offset",
        "overlap_shape",
        "source_base_offset",
        "target_base_offset",
        "outer_loop_counts",
        "source_strides",
        "target_strides",
        "inner_bytes",
    ),
    [
        (
            (0, 2, 0),
            (4, 3, 8),
            (1, 2, 0),
            (2, 3, 8),
            32,
            48,
            (2,),
            (96,),
            (48,),
            48,
        ),
        (
            (0, 0, 4),
            (4, 6, 4),
            (1, 0, 4),
            (2, 6, 4),
            8,
            48,
            (2, 6),
            (96, 16),
            (48, 8),
            8,
        ),
    ],
)
def test_transfer_region_describes_cross_dim_logical_overlap(
    target_offset: tuple[int, ...],
    target_shape: tuple[int, ...],
    overlap_offset: tuple[int, ...],
    overlap_shape: tuple[int, ...],
    source_base_offset: int,
    target_base_offset: int,
    outer_loop_counts: tuple[int, ...],
    source_strides: tuple[int, ...],
    target_strides: tuple[int, ...],
    inner_bytes: int,
) -> None:
    source = n_dim_fragment(
        fragment_id="source",
        global_offset=(1, 0, 0),
        local_shape=(2, 6, 8),
        address=0x10000,
    )
    target = n_dim_fragment(
        fragment_id="target",
        global_offset=target_offset,
        local_shape=target_shape,
        address=0x20000,
    )

    region = TransferRegion(
        tensor_id=source.tensor_id,
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

    assert region.overlap_offset == overlap_offset
    assert region.overlap_shape == overlap_shape
    assert region.source_base_offset == source_base_offset
    assert region.target_base_offset == target_base_offset
    assert region.inner_bytes == inner_bytes
    assert region.outer_loop_counts == outer_loop_counts
    assert region.source_strides == source_strides
    assert region.target_strides == target_strides
    assert region.segment_count == prod(outer_loop_counts)
    assert region.total_bytes == prod(overlap_shape) * 2
    assert (
        len(tuple(region.iter_segments(max_segments=region.segment_count)))
        == region.segment_count
    )
    with pytest.raises(ValueError, match="exceeds max_segments"):
        tuple(region.iter_segments(max_segments=region.segment_count - 1))

    # TransferPlan is the live, runtime-attested boundary. This test only
    # exercises address-free N-D region geometry.


def test_transfer_region_mixed_radix_iteration_and_n_dim_bounds() -> None:
    source = n_dim_fragment(
        fragment_id="source",
        global_offset=(1, 0, 0),
        local_shape=(2, 6, 8),
        address=0x10000,
    )
    target = n_dim_fragment(
        fragment_id="target",
        global_offset=(0, 2, 0),
        local_shape=(4, 3, 8),
        address=0x20000,
    )
    region = TransferRegion(
        tensor_id=source.tensor_id,
        source=source,
        target=target,
        overlap_offset=(1, 2, 0),
        overlap_shape=(2, 3, 8),
        source_base_offset=32,
        target_base_offset=48,
        inner_bytes=48,
        outer_loop_counts=(2,),
        source_strides=(96,),
        target_strides=(48,),
    )

    assert tuple(region.iter_segments(max_segments=region.segment_count)) == (
        (32, 48, 48),
        (128, 96, 48),
    )

    with pytest.raises(ValueError, match="canonical"):
        replace(region, source_strides=(193,))


def test_transfer_region_rejects_noncanonical_same_volume_geometry() -> None:
    source = n_dim_fragment(
        fragment_id="source",
        global_offset=(1, 0, 0),
        local_shape=(2, 6, 8),
        address=0x10000,
    )
    target = n_dim_fragment(
        fragment_id="target",
        global_offset=(0, 2, 0),
        local_shape=(4, 3, 8),
        address=0x20000,
    )

    with pytest.raises(ValueError, match="canonical"):
        TransferRegion(
            tensor_id=source.tensor_id,
            source=source,
            target=target,
            overlap_offset=(1, 2, 0),
            overlap_shape=(2, 3, 8),
            source_base_offset=32,
            target_base_offset=48,
            inner_bytes=16,
            outer_loop_counts=(2, 3),
            source_strides=(96, 16),
            target_strides=(80, 16),
        )
