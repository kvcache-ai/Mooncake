"""Shared-allocation strided views must be checked by their live payload."""

import ctypes
from dataclasses import replace
from unittest.mock import Mock

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCachePlacementManifest,
    KVCacheRuntimeTransferPlan,
    KVCacheTransferEngineExecutor,
    plan_kv_cache_transfer_to_local_target,
    prepare_kv_cache_transfer,
    resolve_contiguous_runtime_binding,
    validate_runtime_binding,
    validate_runtime_bindings,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot
from test_runtime_transfer import MemoryEngine


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


@pytest.mark.parametrize("kind", ["ssm", "kv"])
@pytest.mark.parametrize("source_row,target_row", [(0, 0), (0, 1), (1, 0)])
def test_interleaved_views_register_and_copy_selected_rows(
    kind, source_row, target_row, monkeypatch
):
    snapshot = _snapshot(token_start=13, token_count=2)
    source_memory = ctypes.create_string_buffer(
        bytes(range(256)) + bytes(range(64)), 320
    )
    target_memory = ctypes.create_string_buffer(b"\xa5" * 320, 320)
    source_before = source_memory.raw
    expected = bytearray(target_memory.raw)
    for view_offset in (0, 48):
        for row in range(snapshot.token_count):
            source_offset = 16 + view_offset + (source_row + row) * 96
            target_offset = 16 + view_offset + (target_row + row) * 96
            expected[target_offset : target_offset + 48] = source_before[
                source_offset : source_offset + 48
            ]

    placements, bindings, executors = [], [], []
    for prefix, allocation, first_row in (
        ("source", source_memory, source_row),
        ("target", target_memory, target_row),
    ):
        base = ctypes.addressof(allocation)
        placement, binding = interleaved_binding(prefix, base + 16, snapshot, kind)
        binding = replace(
            binding,
            buffers=tuple(
                replace(
                    item,
                    fragment=replace(
                        item.fragment,
                        storage_address=base,
                        storage_nbytes=len(allocation),
                        storage_offset_bytes=item.fragment.storage_offset_bytes + 16,
                    ),
                )
                for item in binding.buffers
            ),
        )
        resolved = resolve_contiguous_runtime_binding(
            placement,
            snapshot,
            binding,
            operation_id="interleaved-copy",
            first_token_offset=first_row,
        )
        engine = MemoryEngine()
        register = Mock(return_value=0)
        monkeypatch.setattr(engine, "register_memory", register)
        executor = KVCacheTransferEngineExecutor(
            engine,
            instance_id=resolved.instance_id,
            endpoint=resolved.regions[0].endpoint,
        )
        for region in resolved.regions:
            executor.register_region(region)
        executor.validate_local_binding(resolved)
        register.assert_called_once_with(base, len(allocation))
        assert len(resolved.regions) == 1
        assert {r.region_id for r in resolved.ranges} == {resolved.regions[0].region_id}
        assert {r.offset_bytes for r in resolved.ranges} == {
            16 + first_row * 96,
            64 + first_row * 96,
        }
        # A caller cannot bypass overlap protection by using another region ID.
        with pytest.raises(ValueError, match="registered regions overlap"):
            executor.register_region(replace(resolved.regions[0], region_id="overlap"))
        register.assert_called_once_with(base, len(allocation))
        placements.append(placement)
        bindings.append(resolved)
        executors.append(executor)

    source, target = placements
    source_binding, target_binding = bindings
    plan = KVCacheRuntimeTransferPlan(
        "interleaved-copy",
        (
            plan_kv_cache_transfer_to_local_target(
                source, target, target_binding.participant_id, snapshot=snapshot
            ),
        ),
        (source_binding,),
        (target_binding,),
    )
    receipts = executors[0].execute(plan, source_binding.participant_id)
    assert receipts and all(r.success and r.quiesced for r in receipts)
    assert source_memory.raw == source_before
    assert target_memory.raw == bytes(expected)
    for executor, binding in zip(executors, bindings):
        executor.unregister_region(binding.regions[0].region_id)
