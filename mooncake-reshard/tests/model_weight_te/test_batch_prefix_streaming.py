from dataclasses import replace
from math import prod
import tracemalloc

import pytest

from mooncake.reshard.weight._te.batching import iter_transfer_batches
from mooncake.reshard.weight.manifest import ParallelRank
from mooncake.reshard.weight.planner import TransferRegion

from model_weight_planner.helpers import bound_fragment


def _region(counts):
    shape = counts + (2,)
    fragments = []
    for name, address, offset in (("source", 0x10000, 16), ("target", 0x40000, 32)):
        fragment = bound_fragment(
            fragment_id=name,
            tensor_id="weight",
            global_offset=(0,) * len(shape),
            local_shape=shape,
            address=address,
            nbytes=prod(shape) * 2,
            worker_id=name,
            endpoint=f"{name}:12345",
            device="cuda:0",
            rank=ParallelRank(),
        )
        fragments.append(
            replace(
                fragment,
                binding=replace(
                    fragment.binding,
                    storage_address=address - offset,
                    storage_offset_bytes=offset,
                    storage_nbytes=fragment.nbytes + offset,
                ),
            )
        )
    strides = tuple(prod(shape[dim + 1 :]) * 2 for dim in range(len(counts)))
    return TransferRegion(
        tensor_id="weight",
        source=fragments[0],
        target=fragments[1],
        overlap_offset=(0,) * len(counts) + (1,),
        overlap_shape=counts + (1,),
        source_base_offset=2,
        target_base_offset=2,
        inner_bytes=2,
        outer_loop_counts=counts,
        source_strides=strides,
        target_strides=strides,
    )


def _batches(region, *, budget=None):
    return iter_transfer_batches(
        region.target.endpoint,
        ((region, region.source, region.target),),
        max_batch_operations=64,
        max_region_segments=region.segment_count if budget is None else budget,
    )


def test_first_batch_long_prefix_has_bounded_memory():
    region = _region((500000, 2))
    tracemalloc.start()
    try:
        batch = next(_batches(region))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert batch.operation_count == 64
    assert batch.source_addresses == tuple(
        region.source.address + 2 + i * 4 for i in range(64)
    )
    assert peak < 1024 * 1024


@pytest.mark.parametrize("counts", [(2,), (2, 3, 4), (2, 1, 3, 2)])
def test_prefix_carry_preserves_order_and_storage_offsets(counts):
    region = _region(counts)
    expected = tuple(region.iter_segments(max_segments=region.segment_count))
    batches = tuple(_batches(region))
    actual = tuple(
        (source - region.source.address, target - region.target.address, size)
        for batch in batches
        for source, target, size in zip(
            batch.source_addresses, batch.target_addresses, batch.sizes
        )
    )
    assert actual == expected
    assert all(batch.operation_count <= 64 for batch in batches)
    assert batches[0].ranges[0].source_offsets[0] == 18
    assert batches[0].ranges[0].target_offsets[0] == 34


def test_prefix_budget_rejected_before_first_batch():
    with pytest.raises(ValueError, match="max_segments"):
        next(_batches(_region((500000, 2)), budget=999999))


def test_prefix_subclass_retains_custom_segment_iterator():
    class CustomRegion(TransferRegion):
        def iter_segments(self, *, max_segments):
            assert max_segments == self.segment_count
            yield 7, 9, 2

    region = _region((2, 3))
    custom = CustomRegion(
        **{name: getattr(region, name) for name in region.__dataclass_fields__}
    )
    (batch,) = tuple(_batches(custom))
    assert batch.source_addresses == (region.source.address + 7,)
    assert batch.target_addresses == (region.target.address + 9,)


def test_short_prefix_runs_accumulate_once_per_batch():
    region = _region((10000, 2))
    batches = tuple(
        iter_transfer_batches(
            region.target.endpoint,
            ((region, region.source, region.target),),
            max_batch_operations=8192,
            max_region_segments=region.segment_count,
        )
    )
    assert [batch.operation_count for batch in batches] == [8192, 8192, 3616]
    assert sum(len(batch.ranges) for batch in batches) == 3
    assert tuple(
        address for batch in batches for address in batch.source_addresses
    ) == tuple(region.source.address + 2 + i * 4 for i in range(20000))


@pytest.mark.parametrize("limit", [5, 64])
def test_partial_prefix_batches_preserve_operation_allocation_boundaries(limit):
    first = _region((2, 3))
    owner = object()
    second_source = replace(
        first.source,
        owner=owner,
        binding=replace(
            first.source.binding,
            owner=owner,
            storage_nbytes=first.source.storage_nbytes + 8,
        ),
    )
    second_target = replace(
        first.target,
        owner=owner,
        binding=replace(
            first.target.binding,
            owner=owner,
            storage_nbytes=first.target.storage_nbytes + 8,
        ),
    )
    second = replace(first, source=second_source, target=second_target)
    batches = tuple(
        iter_transfer_batches(
            first.target.endpoint,
            (
                (first, first.source, first.target),
                (second, second.source, second.target),
            ),
            max_batch_operations=limit,
            max_region_segments=first.segment_count,
        )
    )
    expected = tuple(first.iter_segments(max_segments=first.segment_count)) * 2
    assert (
        tuple(
            (source - first.source.address, target - first.target.address, size)
            for batch in batches
            for source, target, size in zip(
                batch.source_addresses, batch.target_addresses, batch.sizes
            )
        )
        == expected
    )
    ranges = tuple(r for batch in batches for r in batch.ranges)
    expected_ranges = 4 if limit == 5 else 2
    assert len(ranges) == expected_ranges
    assert all(r.source_base_address == first.source.storage_address for r in ranges)
    assert all(r.target_base_address == first.target.storage_address for r in ranges)
    assert {r.source_capacity for r in ranges} == {
        first.source.storage_nbytes,
        second.source.storage_nbytes,
    }
    assert {r.target_capacity for r in ranges} == {
        first.target.storage_nbytes,
        second.target.storage_nbytes,
    }
    assert ranges[0].source_offsets[0] == 18
    assert ranges[0].target_offsets[0] == 34
