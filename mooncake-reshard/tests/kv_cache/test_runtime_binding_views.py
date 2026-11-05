"""Shared-allocation strided views must be checked by their live payload."""

from dataclasses import replace
from itertools import product

import mooncake.reshard.kv_cache.binding as binding_checks
import pytest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheTransferEngineExecutor,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    resolve_contiguous_runtime_binding,
    validate_runtime_binding,
)
from test_cache_components import resource
from test_kv_cache_reshard import _binding, _snapshot
from test_runtime_transfer import MemoryEngine


def interleaved_binding(prefix, base, snapshot, kind, second_offset=48):
    placement = resource(
        prefix,
        1,
        KVCacheComponent.SSM if kind == "ssm" else None,
        strided=True,
        layer_ids=(0, 1) if kind == "ssm" else (0,),
    )
    binding = _binding(
        placement,
        placement.parts[0].participant_id,
        base_address=base,
        snapshot=snapshot,
        capacity_tokens=3,
    )
    return placement, replace(
        binding,
        buffers=tuple(
            replace(
                item,
                fragment=replace(
                    item.fragment,
                    address=base + offset,
                    storage_address=base,
                    storage_nbytes=max(288, second_offset + 240),
                    storage_offset_bytes=offset,
                    nbytes=240,
                    strides_bytes=(96, 12, 2),
                ),
            )
            for item, offset in zip(binding.buffers, (0, second_offset))
        ),
    )


@pytest.mark.parametrize("kind", ["ssm", "kv"])
@pytest.mark.parametrize("first_row", [0, 1])
def test_interleaved_payloads_resolve_prepare_and_register(kind, first_row):
    snapshot = _snapshot(token_count=2)
    source, binding = interleaved_binding("source", 1_000_000, snapshot, kind)
    target, target_binding = interleaved_binding("target", 2_000_000, snapshot, kind)
    resolved = resolve_contiguous_runtime_binding(
        source,
        snapshot,
        binding,
        operation_id="interleaved",
        first_token_offset=first_row,
    )
    assert len(resolved.regions) == 1
    region = resolved.regions[0]
    assert (region.address, region.nbytes) == (1_000_000, 288)
    assert {item.region_id for item in resolved.ranges} == {region.region_id}
    payloads = sorted(
        item.address(region, token, item.head_start)
        for item in resolved.ranges
        for token in range(item.token_start, item.token_end)
    )
    assert payloads == [
        1_000_000 + row * 48 for row in range(2 * first_row, 2 * first_row + 4)
    ]
    executor = KVCacheTransferEngineExecutor(
        MemoryEngine(), instance_id=resolved.instance_id, endpoint=region.endpoint
    )
    executor.register_region(region)
    executor.validate_local_binding(resolved)
    executor.unregister_region(region.region_id)
    prepared = prepare_kv_cache_transfer(
        plan_kv_cache_transfer_to_local_target(
            source, target, target_binding.participant_id, snapshot=snapshot
        ),
        binding,
        target_binding,
    )
    assert len(prepared.edges) == 2
    assert {e.source_head_offset_bytes for e in prepared.edges} == {0, 48}
    assert {e.target_head_offset_bytes for e in prepared.edges} == {0, 48}
    assert all(
        e.nbytes == 48
        and e.source_row_stride == e.target_row_stride == 96
        and not e.is_full_row
        for e in prepared.edges
    )


@pytest.mark.parametrize("kind", ["ssm", "kv"])
@pytest.mark.parametrize("second_offset", [0, 46, 50, 96])
def test_strided_payload_overlap_is_rejected(kind, second_offset):
    placement, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=3), kind, second_offset
    )
    with pytest.raises(ValueError, match="buffers overlap"):
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize("kind", ["ssm", "kv"])
def test_interleaved_views_require_consistent_backing_allocations(kind):
    placement, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=3), kind
    )
    first, second = binding.buffers
    binding = replace(
        binding,
        buffers=(
            first,
            replace(second, fragment=replace(second.fragment, storage_nbytes=384)),
        ),
    )
    with pytest.raises(ValueError, match="backing allocations overlap"):
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize(
    "change,match",
    [
        ({"nbytes": 1}, "nbytes"),
        *[
            ({"strides_bytes": strides}, "disjoint contiguous NHD rows")
            for strides in ((47, 12, 2), (145, 12, 2), (144, 14, 2), (144, 12, 4))
        ],
    ],
)
def test_invalid_strided_geometry_is_rejected(change, match):
    placement, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=3), "ssm"
    )
    first, second = binding.buffers
    broken = replace(
        binding,
        buffers=(replace(first, fragment=replace(first.fragment, **change)), second),
    )
    with pytest.raises(ValueError, match=match):
        validate_runtime_binding(placement, broken)


@pytest.mark.parametrize("second_offset,overlap", [(48, False), (46, True), (50, True)])
def test_large_interleaved_capacity_never_expands_rows(
    monkeypatch, second_offset, overlap
):
    placement, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=1), "ssm", second_offset
    )
    rows = 10**12
    binding = replace(
        binding,
        buffers=tuple(
            replace(
                item,
                fragment=replace(
                    item.fragment,
                    local_shape=(rows, 4, 6),
                    nbytes=(rows - 1) * 96 + 48,
                    storage_nbytes=(rows + 1) * 96,
                ),
            )
            for item in binding.buffers
        ),
    )

    def no_expansion(_fragment):
        raise AssertionError("capacity-sized row expansion")

    monkeypatch.setattr(binding_checks, "_payload_ranges", no_expansion)
    if overlap:
        with pytest.raises(ValueError, match="buffers overlap"):
            validate_runtime_binding(placement, binding)
    else:
        validate_runtime_binding(placement, binding)


@pytest.mark.parametrize("rows", [3, 10**12])
def test_mixed_stride_overlap_work_is_bounded(monkeypatch, rows):
    placement, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=1), "ssm"
    )
    binding = replace(
        binding,
        buffers=tuple(
            replace(
                item,
                fragment=replace(
                    item.fragment,
                    local_shape=(rows, 4, 6),
                    strides_bytes=(stride, 12, 2),
                    nbytes=(rows - 1) * stride + 48,
                    storage_nbytes=(rows + 1) * 192,
                ),
            )
            for item, stride in zip(binding.buffers, (96, 192))
        ),
    )
    if rows == 3:
        validate_runtime_binding(placement, binding)
    else:

        def no_expansion(_fragment):
            raise AssertionError("expanded before checking the work budget")

        monkeypatch.setattr(binding_checks, "_payload_ranges", no_expansion)
        with pytest.raises(ValueError, match="validation work limit"):
            validate_runtime_binding(placement, binding)


def test_equal_stride_arithmetic_matches_small_byte_oracle():
    _, binding = interleaved_binding(
        "source", 1_000_000, _snapshot(token_count=1), "ssm"
    )
    template = binding.buffers[0].fragment
    for left_rows, right_rows, left_width, right_width, offset in product(
        (2, 4), (2, 3), (2, 8, 16), (2, 8, 16), range(0, 80, 2)
    ):
        left, right = (
            replace(
                template,
                address=template.address + start,
                storage_offset_bytes=start,
                storage_nbytes=256,
                local_shape=(rows, 1, width // 2),
                strides_bytes=(16, width, 2),
                nbytes=(rows - 1) * 16 + width,
            )
            for rows, width, start in (
                (left_rows, left_width, 0),
                (right_rows, right_width, offset),
            )
        )
        expected = any(
            max(i * 16, offset + j * 16)
            < min(i * 16 + left_width, offset + j * 16 + right_width)
            for i in range(left_rows)
            for j in range(right_rows)
        )
        assert binding_checks._view_payloads_overlap(left, right) == expected
