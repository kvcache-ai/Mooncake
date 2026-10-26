"""Verify byte lowering without a Store service or discovery protocol."""

import ctypes

import pytest
from _store_test_utils import FORMATS, _reference, memory_binding
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheTransferLimits,
    plan_kv_cache_store_load,
    plan_kv_cache_store_upload,
)
from mooncake.reshard.kv_cache._store.runtime import (
    lower_store_ranges,
    split_store_range_batches,
)
from test_kv_cache_reshard import _placement


@pytest.mark.parametrize("source_format", FORMATS)
@pytest.mark.parametrize("target_format", FORMATS)
def test_lowered_ranges_restore_tp1pp3_to_tp2pp2(source_format, target_format):
    source = _placement("source", ((0,), (1, 2), (3,)), 1)
    target = _placement("target", ((0, 1), (2, 3)), 2)
    layout = plan_kv_cache_store_upload(source, object_format=source_format).layout
    parts = [
        b"".join(
            _reference(
                layout, shard.layer_ids, shard.head_start, shard.head_count, component
            )
            for component in KVCacheComponent
        )
        for shard in layout.shards
    ]
    plan = plan_kv_cache_store_load(layout, target)
    limits = KVCacheTransferLimits(max_batch_operations=3, max_batch_bytes=137)
    for part in target.parts:
        binding, buffers, expected = memory_binding(
            target, part.participant_id, target_format, 1, "read", fill=False
        )
        fragments = tuple(
            r.fragment for r in plan.ranges if r.participant_id == part.participant_id
        )
        ranges = lower_store_ranges(target, binding, (fragments,), "read", limits)
        regions = {r.region_id: r for r in binding.regions}
        copied = 0
        for batch in split_store_range_batches(ranges, limits):
            assert len(batch) <= 3 and sum(n for _, _, n in batch) <= 137
            for r, offset, size in batch:
                data = parts[r.part_index][
                    r.part_offset + offset : r.part_offset + offset + size
                ]
                assert len(data) == size
                ctypes.memmove(
                    regions[r.region_id].address + r.region_offset + offset, data, size
                )
                copied += size
        assert copied == sum(len(b) for b in expected)
        assert [bytes(b) for b in buffers] == expected
