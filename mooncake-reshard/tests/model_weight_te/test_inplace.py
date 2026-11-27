from __future__ import annotations

import ctypes
from dataclasses import replace

import pytest

from mooncake.reshard.weight.inplace import InplaceWeightResharder
from mooncake.reshard.weight.plan_cache import WeightTransferPlanCache
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    TransferCompletionUnknownError,
    TransferEngineError,
)

from .helpers import (
    FakeTransferEngine,
    RuntimeInputs,
    allocation_guards,
    execute_reader,
    manifests,
    plan_transfer_to_local_target,
    registration_leases,
)


class CopyEngine(FakeTransferEngine):
    def batch_transfer_sync_read(self, endpoint, targets, sources, sizes):
        result = super().batch_transfer_sync_read(endpoint, targets, sources, sizes)
        if result == 0:
            for target, source, size in zip(targets, sources, sizes, strict=True):
                ctypes.memmove(target, source, size)
        return result


def runtime(buffer, *, generation=1, lease_id="lease"):
    inputs = manifests(tp=1, prefix="fleet", address_base=ctypes.addressof(buffer))
    binding = inputs.bindings[0]
    return RuntimeInputs(
        inputs.placement,
        (
            replace(
                binding,
                generation=generation,
                instance_id=f"instance-{lease_id}",
                lease_id=lease_id,
                fragments=(replace(binding.fragments[0], device="cpu"),),
            ),
        ),
    )


def execute(resharder, source, target, **kwargs):
    kwargs.setdefault("source_allocation_guards", allocation_guards(source))
    kwargs.setdefault("target_allocation_guards", allocation_guards(target))
    return resharder.execute(
        source.placement,
        source.bindings,
        target.placement,
        target.bindings[0],
        source_registrations=registration_leases(source),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
        **kwargs,
    )


@pytest.mark.parametrize("enabled", [False, True])
def test_inplace_reads_bytes_with_fresh_bindings_on_repeated_plan(enabled):
    cache = WeightTransferPlanCache(enabled=enabled)
    engine = CopyEngine()
    resharder = InplaceWeightResharder(
        MooncakeTransferEngineReader(engine), plan_cache=cache
    )
    buffers = []
    for generation, data in enumerate((b"abcdefgh", b"12345678"), start=1):
        source_buffer = ctypes.create_string_buffer(data, 8)
        target_buffer = ctypes.create_string_buffer(8)
        buffers.extend((source_buffer, target_buffer))
        source = runtime(
            source_buffer, generation=generation, lease_id=f"s-{generation}"
        )
        target = runtime(
            target_buffer, generation=generation, lease_id=f"t-{generation}"
        )
        receipts = execute(resharder, source, target)
        assert target_buffer.raw == data
        assert sum(receipt.nbytes for receipt in receipts) == 8
        assert engine.calls[-1][1:3] == (
            [ctypes.addressof(target_buffer)],
            [ctypes.addressof(source_buffer)],
        )
    assert cache.cache_info().hits == int(enabled)


def test_inplace_rejects_different_participant_fleet_before_io():
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    with pytest.raises(ValueError, match="same participant set"):
        execute(
            InplaceWeightResharder(
                MooncakeTransferEngineReader(engine),
                plan_cache=WeightTransferPlanCache(),
            ),
            source,
            target,
        )
    assert engine.calls == []


def test_inplace_preserves_unknown_completion_quarantine_and_blocks_reentry():
    source_buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    target_buffer = ctypes.create_string_buffer(8)
    source, target = (
        runtime(source_buffer, lease_id="source"),
        runtime(target_buffer, lease_id="target"),
    )
    engine = FakeTransferEngine()
    engine.read_result = 5
    reader = MooncakeTransferEngineReader(engine)
    resharder = InplaceWeightResharder(reader, plan_cache=WeightTransferPlanCache(True))
    source_guards, target_guards = allocation_guards(source), allocation_guards(target)
    with pytest.raises(TransferCompletionUnknownError) as raised:
        execute(
            resharder,
            source,
            target,
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )
    assert all(
        not token.released_states
        for guard in (*source_guards.values(), *target_guards.values())
        for token in guard.tokens
    )
    pending_id = raised.value.pending_transfer_id
    assert reader.pending_transfer_ids() == (pending_id,)
    assert (
        reader.pending_transfer_status(pending_id)
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    with pytest.raises(TransferEngineError):
        execute(resharder, source, target)
    assert len(engine.calls) == 1
    assert reader.pending_transfer_ids() == (pending_id,)


def test_only_new_constructor_reads_cache_environment(monkeypatch):
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", "invalid")
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    reader = MooncakeTransferEngineReader(FakeTransferEngine())
    assert execute_reader(
        reader,
        plan_transfer_to_local_target(source, target),
        source,
        target,
        source_registrations=registration_leases(source),
    )
    with pytest.raises(ValueError, match="MC_RESHARD_PLAN_CACHE_ENABLED"):
        InplaceWeightResharder(reader)
    InplaceWeightResharder(reader, plan_cache=WeightTransferPlanCache())


def test_inplace_rejects_known_same_runtime_overlap_before_io():
    buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    source = runtime(buffer)
    target = runtime(buffer)
    engine = FakeTransferEngine()
    resharder = InplaceWeightResharder(
        MooncakeTransferEngineReader(engine), plan_cache=WeightTransferPlanCache()
    )
    with pytest.raises(ValueError, match="runtime storage allocations overlap"):
        execute(resharder, source, target)
    assert engine.calls == []


def test_inplace_cache_hit_still_rejects_stale_registration_generation():
    source_buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    target_buffer = ctypes.create_string_buffer(8)
    source = runtime(source_buffer, lease_id="source")
    target = runtime(target_buffer, lease_id="target")
    engine = CopyEngine()
    cache = WeightTransferPlanCache(True)
    resharder = InplaceWeightResharder(
        MooncakeTransferEngineReader(engine), plan_cache=cache
    )
    execute(resharder, source, target)
    current_source = runtime(source_buffer, generation=2, lease_id="source-new")
    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        resharder.execute(
            current_source.placement,
            current_source.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(source),
            source_allocation_guards=allocation_guards(current_source),
            target_allocation_guards=allocation_guards(target),
        )
    assert cache.cache_info().hits == 1
    assert len(engine.calls) == 1


def test_cache_environment_is_read_once_at_new_constructor(monkeypatch):
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", "1")
    resharder = InplaceWeightResharder(MooncakeTransferEngineReader(CopyEngine()))
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", "invalid")
    source_buffer = ctypes.create_string_buffer(b"abcdefgh", 8)
    target_buffer = ctypes.create_string_buffer(8)
    source = runtime(source_buffer, lease_id="source")
    target = runtime(target_buffer, lease_id="target")
    execute(resharder, source, target)
    execute(resharder, source, target)
    assert target_buffer.raw == b"abcdefgh"
    assert resharder.plan_cache.cache_info().hits == 1
