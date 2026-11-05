"""Participants on one runtime share allocation registration and lifetime."""

import ctypes
from dataclasses import replace

import pytest
from mooncake.reshard.contracts import RuntimeFragmentId, RuntimeInstanceId
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCachePlacementManifest,
    KVCacheRuntimeTransferPlan,
    KVCacheTransferEngineExecutor,
    plan_kv_cache_transfer_to_local_target,
    resolve_contiguous_runtime_binding,
    validate_runtime_bindings,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot
from test_runtime_transfer import MemoryEngine


class CountingEngine:
    """Count real calls while delegating to either memcpy or native RDMA."""

    def __init__(self, engine):
        self.engine = engine
        self.registers = self.unregisters = 0

    def register_memory(self, address, nbytes):
        self.registers += 1
        return self.engine.register_memory(address, nbytes)

    def unregister_memory(self, address):
        self.unregisters += 1
        return self.engine.unregister_memory(address)

    def __getattr__(self, name):
        return getattr(self.engine, name)


def shared_bindings(
    prefix, snapshot, base, *, ssm=False, strided=False, endpoint=None, device="cpu"
):
    placement = _placement(prefix, ((0,),), 2, page_size=1)
    descriptor = replace(
        placement.descriptor,
        components=(KVCacheComponent.SSM,)
        if ssm
        else (KVCacheComponent.KEY, KVCacheComponent.VALUE),
        dtype="float32" if ssm else "bfloat16",
        itemsize=4 if ssm else 2,
        key_head_dim=4 if ssm else 8,
        value_head_dim=4 if ssm else 8,
        layout=KVCacheLayout.NHD_STRIDED if strided else KVCacheLayout.NHD,
    )
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
            capacity_tokens=3 if strided else 1,
            snapshot=snapshot,
        )
        buffers = []
        for view, item in enumerate(binding.buffers):
            offset = rank * (64 if strided else 128) + view * (32 if strided else 64)
            buffers.append(
                replace(
                    item,
                    fragment=replace(
                        item.fragment,
                        address=base + offset,
                        worker_id=prefix,
                        endpoint=endpoint or f"{prefix}:12345",
                        device=device,
                        storage_address=base,
                        storage_nbytes=512,
                        storage_offset_bytes=offset,
                        nbytes=288 if strided else 32,
                        strides_bytes=(
                            128 if strided else 32,
                            *item.fragment.strides_bytes[1:],
                        ),
                    ),
                )
            )
        bindings.append(
            replace(
                binding,
                instance_id=RuntimeInstanceId(f"{prefix}-runtime"),
                buffers=tuple(buffers),
            )
        )
    validate_runtime_bindings(placement, tuple(bindings), snapshot=snapshot)
    return placement, tuple(bindings)


def resolve_all(
    placement, snapshot, bindings, operation_id="shared-copy", first_token_offset=0
):
    return tuple(
        resolve_contiguous_runtime_binding(
            placement,
            snapshot,
            binding,
            operation_id=operation_id,
            first_token_offset=first_token_offset,
        )
        for binding in bindings
    )


def shared_plan(source, target, snapshot, sources, targets):
    return KVCacheRuntimeTransferPlan(
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


@pytest.mark.parametrize("ssm", [False, True])
def test_participants_resolve_and_register_one_allocation(ssm):
    snapshot = _snapshot(token_count=1)
    base = 1_000_000
    placement, bindings = shared_bindings("source", snapshot, base, ssm=ssm)
    resolved = resolve_all(placement, snapshot, bindings)
    engine = CountingEngine(MemoryEngine())
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id=resolved[0].instance_id, endpoint="source:12345"
    )
    for binding in resolved:
        for region in binding.regions:
            executor.register_region(region)
        executor.validate_local_binding(binding)
    assert resolved[0].regions == resolved[1].regions
    assert len(resolved[0].regions) == 1
    assert (resolved[0].regions[0].address, resolved[0].regions[0].nbytes) == (
        base,
        512,
    )
    assert engine.registers == 1
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
    with pytest.raises(ValueError, match="registered regions overlap"):
        executor.register_region(replace(resolved[0].regions[0], region_id="alias"))
    # The runtime releases the unique allocation once, after all participants.
    for region in {r for b in resolved for r in b.regions}:
        executor.unregister_region(region.region_id)
    assert engine.unregisters == 1


@pytest.mark.parametrize("ssm", [False, True])
@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_shared_allocation_lives_until_last_participant_finishes(ssm, order):
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
    plan = shared_plan(source, target, snapshot, sources, targets)
    executors = []
    for bindings in (sources, targets):
        engine = CountingEngine(MemoryEngine())
        executor = KVCacheTransferEngineExecutor(
            engine,
            instance_id=bindings[0].instance_id,
            endpoint=bindings[0].regions[0].endpoint,
        )
        for binding in bindings:
            for region in binding.regions:
                executor.register_region(region, owner_id=binding.digest)
                executor.register_region(region, owner_id=binding.digest)
        assert engine.registers == 1
        with pytest.raises(ValueError, match="not registered by this owner"):
            executor.unregister_region(bindings[0].regions[0].region_id)
        executors.append(executor)

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
            assert executor.engine.unregisters == int(index == order[-1])
    assert source_memory.raw == bytes(range(256)) * 2


def test_owner_claims_preserve_manual_registration():
    snapshot = _snapshot(token_count=1)
    placement, bindings = shared_bindings("source", snapshot, 1_000_000)
    region = resolve_all(placement, snapshot, bindings)[0].regions[0]
    engine = CountingEngine(MemoryEngine())
    executor = KVCacheTransferEngineExecutor(
        engine, instance_id="source-runtime", endpoint=region.endpoint
    )
    executor.register_region(region)
    executor.register_region(region, owner_id="participant")
    executor.unregister_region(region.region_id, owner_id="participant")
    assert engine.unregisters == 0
    with pytest.raises(ValueError, match="not registered by this owner"):
        executor.unregister_region(region.region_id, owner_id="participant")
    executor.unregister_region(region.region_id)
    assert engine.registers == engine.unregisters == 1
