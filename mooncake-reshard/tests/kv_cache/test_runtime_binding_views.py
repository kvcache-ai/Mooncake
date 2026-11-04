"""Shared-allocation strided views must be checked by their live payload."""

from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCachePlacementManifest,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    resolve_contiguous_runtime_binding,
    validate_runtime_binding,
    validate_runtime_bindings,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot


def interleaved_binding(prefix, base, snapshot, kind, second_offset=48):
    layers = (0, 1) if kind == "ssm" else (0,)
    placement = _placement(prefix, (layers,), 1, page_size=1)
    descriptor = replace(
        placement.descriptor,
        key_head_dim=6,
        value_head_dim=6,
        components=(KVCacheComponent.SSM,)
        if kind == "ssm"
        else (KVCacheComponent.KEY, KVCacheComponent.VALUE),
        layout=KVCacheLayout.NHD_STRIDED,
    )
    placement = KVCachePlacementManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_set_id=placement.placement_set_id,
        topology=placement.topology,
        descriptor=descriptor,
        parts=tuple(replace(p, descriptor=descriptor) for p in placement.parts),
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
def test_interleaved_payloads_pass_binding_resolution_and_preparation(kind):
    snapshot = _snapshot(token_count=3)
    source, binding = interleaved_binding("source", 1_000_000, snapshot, kind)
    target, target_binding = interleaved_binding("target", 2_000_000, snapshot, kind)
    validate_runtime_binding(source, binding, snapshot=snapshot)
    validate_runtime_bindings(source, (binding,), snapshot=snapshot)
    resolved = resolve_contiguous_runtime_binding(
        source, snapshot, binding, operation_id="interleaved"
    )
    regions = {r.region_id: r for r in resolved.regions}
    payloads = sorted(
        (item.address(regions[item.region_id], token, item.head_start), 48)
        for item in resolved.ranges
        for token in range(item.token_start, item.token_end)
    )
    assert payloads == [(1_000_000 + row * 48, 48) for row in range(6)]
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
    snapshot = _snapshot(token_count=3)
    source, binding = interleaved_binding(
        "source", 1_000_000, snapshot, kind, second_offset
    )
    target, target_binding = interleaved_binding("target", 2_000_000, snapshot, kind)
    logical = plan_kv_cache_transfer_to_local_target(
        source, target, target_binding.participant_id, snapshot=snapshot
    )
    for check in (
        lambda: validate_runtime_binding(source, binding, snapshot=snapshot),
        lambda: validate_runtime_bindings(source, (binding,), snapshot=snapshot),
        lambda: resolve_contiguous_runtime_binding(
            source, snapshot, binding, operation_id="overlap"
        ),
        lambda: prepare_kv_cache_transfer(logical, binding, target_binding),
    ):
        with pytest.raises(ValueError, match="buffers overlap"):
            check()


@pytest.mark.parametrize("kind", ["ssm", "kv"])
def test_interleaved_views_require_consistent_backing_allocations(kind):
    snapshot = _snapshot(token_count=3)
    placement, binding = interleaved_binding("source", 1_000_000, snapshot, kind)
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
