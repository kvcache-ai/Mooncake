"""Participants on one runtime share allocation registration and lifetime."""

import ctypes
from dataclasses import replace
from unittest.mock import Mock

import pytest
from mooncake.reshard.contracts import RuntimeFragmentId, RuntimeInstanceId
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCachePlacementManifest,
    KVCacheRuntimeTransferPlan,
    KVCacheTransferEngineExecutor,
    plan_kv_cache_transfer_to_local_target,
    resolve_contiguous_runtime_binding,
    validate_runtime_bindings,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot
from test_runtime_transfer import MemoryEngine


def shared_bindings(prefix, snapshot, base, *, ssm=False):
    placement = _placement(prefix, ((0,),), 2, page_size=1)
    if ssm:
        descriptor = replace(placement.descriptor, components=(KVCacheComponent.SSM,))
        placement = KVCachePlacementManifest(
            resource_id=placement.resource_id,
            revision=placement.revision,
            placement_set_id=placement.placement_set_id,
            topology=placement.topology,
            descriptor=descriptor,
            parts=tuple(replace(p, descriptor=descriptor) for p in placement.parts),
        )
    bindings = []
    for rank, part in enumerate(placement.parts):
        binding = _binding(
            placement,
            part.participant_id,
            base_address=base,
            capacity_tokens=1,
            snapshot=snapshot,
        )
        bindings.append(
            replace(
                binding,
                instance_id=RuntimeInstanceId(f"{prefix}-runtime"),
                buffers=tuple(
                    replace(
                        item,
                        fragment=replace(
                            item.fragment,
                            address=base + rank * 128 + view * 64,
                            worker_id=f"{prefix}-worker",
                            endpoint=f"{prefix}:12345",
                            storage_address=base,
                            storage_nbytes=512,
                            storage_offset_bytes=rank * 128 + view * 64,
                        ),
                    )
                    for view, item in enumerate(binding.buffers)
                ),
            )
        )
    validate_runtime_bindings(placement, tuple(bindings), snapshot=snapshot)
    return placement, tuple(bindings)


def resolve_all(placement, snapshot, bindings, operation_id="shared-copy"):
    return tuple(
        resolve_contiguous_runtime_binding(
            placement, snapshot, binding, operation_id=operation_id
        )
        for binding in bindings
    )


@pytest.mark.parametrize("ssm", [False, True])
def test_participants_resolve_and_register_one_allocation(ssm, monkeypatch):
    snapshot = _snapshot(token_count=1)
    allocation = ctypes.create_string_buffer(512)
    base = ctypes.addressof(allocation)
    placement, bindings = shared_bindings("source", snapshot, base, ssm=ssm)
    resolved = resolve_all(placement, snapshot, bindings)
    engine = MemoryEngine()
    register, unregister = Mock(return_value=0), Mock(return_value=0)
    monkeypatch.setattr(engine, "register_memory", register)
    monkeypatch.setattr(engine, "unregister_memory", unregister)
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id=resolved[0].instance_id, endpoint="source:12345"
    )
    for binding in resolved:
        for region in binding.regions:
            executor.register_region(region)
        executor.validate_local_binding(binding)
    assert resolved[0].regions == resolved[1].regions
    assert len(resolved[0].regions) == 1
    register.assert_called_once_with(base, 512)
    renamed = tuple(
        replace(
            b,
            buffers=tuple(
                replace(
                    item,
                    fragment=replace(
                        item.fragment,
                        fragment_id=RuntimeFragmentId(
                            f"new:{item.fragment.fragment_id}"
                        ),
                    ),
                )
                for item in b.buffers
            ),
        )
        for b in bindings
    )
    # A new operation and new view IDs still refer to the same allocation.
    another = resolve_all(placement, snapshot, renamed, operation_id="next-copy")
    assert another[0].regions == resolved[0].regions
    for binding in resolved:
        with pytest.raises(ValueError, match="registered regions overlap"):
            executor.register_region(replace(binding.regions[0], region_id="alias"))
    # The runtime releases the unique allocation once, after all participants.
    for region in {r for b in resolved for r in b.regions}:
        executor.unregister_region(region.region_id)
    unregister.assert_called_once_with(base)


@pytest.mark.parametrize("ssm", [False, True])
@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_shared_allocation_lives_until_last_participant_finishes(
    ssm, order, monkeypatch
):
    snapshot = _snapshot(token_count=1)
    source_memory = ctypes.create_string_buffer(bytes(range(256)) * 2, 512)
    target_memory = ctypes.create_string_buffer(b"\xa5" * 512, 512)
    source, sb = shared_bindings(
        "source", snapshot, ctypes.addressof(source_memory), ssm=ssm
    )
    target, tb = shared_bindings(
        "target", snapshot, ctypes.addressof(target_memory), ssm=ssm
    )
    sources = resolve_all(source, snapshot, sb)
    targets = resolve_all(target, snapshot, tb)
    plan = KVCacheRuntimeTransferPlan(
        "shared-copy",
        tuple(
            plan_kv_cache_transfer_to_local_target(
                source, target, p.participant_id, snapshot=snapshot
            )
            for p in target.parts
        ),
        sources,
        targets,
    )
    executors, unregisters = [], []
    for bindings in (sources, targets):
        engine = MemoryEngine()
        register, unregister = Mock(return_value=0), Mock(return_value=0)
        monkeypatch.setattr(engine, "register_memory", register)
        monkeypatch.setattr(engine, "unregister_memory", unregister)
        executor = KVCacheTransferEngineExecutor(
            engine,
            instance_id=bindings[0].instance_id,
            endpoint=bindings[0].regions[0].endpoint,
        )
        for binding in bindings:
            for region in binding.regions:
                executor.register_region(region, owner_id=binding.digest)
                executor.register_region(region, owner_id=binding.digest)
        register.assert_called_once_with(bindings[0].regions[0].address, 512)
        with pytest.raises(ValueError, match="not registered by this owner"):
            executor.unregister_region(bindings[0].regions[0].region_id)
        executors.append(executor)
        unregisters.append(unregister)

    expected = bytearray(target_memory.raw)
    for index in order:
        for executor, bindings in zip(executors, (sources, targets)):
            executor.validate_local_binding(bindings[index])
        receipts = executors[0].execute(plan, sources[index].participant_id)
        assert receipts and all(r.success and r.quiesced for r in receipts)
        for item in sb[index].buffers:
            offset = item.fragment.storage_offset_bytes
            expected[offset : offset + 32] = source_memory.raw[offset : offset + 32]
        assert target_memory.raw == bytes(expected)
        for executor, bindings in zip(executors, (sources, targets)):
            binding = bindings[index]
            executor.unregister_region(
                binding.regions[0].region_id, owner_id=binding.digest
            )
        if index == order[0]:
            for unregister in unregisters:
                unregister.assert_not_called()
    for unregister, allocation in zip(unregisters, (source_memory, target_memory)):
        unregister.assert_called_once_with(ctypes.addressof(allocation))
    assert source_memory.raw == bytes(range(256)) * 2


def test_owner_claims_preserve_manual_registration_and_native_failure(monkeypatch):
    snapshot = _snapshot(token_count=1)
    placement, bindings = shared_bindings("source", snapshot, 1_000_000)
    region = resolve_all(placement, snapshot, bindings)[0].regions[0]
    engine = MemoryEngine()
    register, unregister = Mock(return_value=0), Mock(return_value=0)
    monkeypatch.setattr(engine, "register_memory", register)
    monkeypatch.setattr(engine, "unregister_memory", unregister)
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id="source-runtime", endpoint=region.endpoint
    )
    executor.register_region(region)
    executor.register_region(region, owner_id="participant")
    executor.unregister_region(region.region_id, owner_id="participant")
    unregister.assert_not_called()
    with pytest.raises(ValueError, match="not registered by this owner"):
        executor.unregister_region(region.region_id, owner_id="participant")
    unregister.return_value = -1
    with pytest.raises(RuntimeError, match="unregister_memory failed"):
        executor.unregister_region(region.region_id)
    with pytest.raises(RuntimeError, match="recovery"):
        executor.register_region(region, owner_id="another")
    with pytest.raises(RuntimeError, match="recovery"):
        executor.unregister_region(region.region_id)
    register.assert_called_once_with(region.address, region.nbytes)
    unregister.assert_called_once_with(region.address)
