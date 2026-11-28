"""The existing public Reader composes with bounded streaming in both fleets."""

from __future__ import annotations

import asyncio
import ctypes
from dataclasses import replace

import pytest

from mooncake.reshard.weight.inplace import InplaceWeightResharder
from mooncake.reshard.weight.plan_cache import WeightTransferPlanCache
from mooncake.reshard.weight.planner import bind_logical_transfer_plan
from mooncake.reshard.weight.streaming import WeightStreamCoordinator
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    TransferCompletionUnknownError,
)

from .helpers import (
    FakeTransferEngine,
    RuntimeInputs,
    allocation_guards,
    manifests,
    registration_leases,
)


class CopyEngine(FakeTransferEngine):
    def batch_transfer_sync_read(self, endpoint, targets, sources, sizes):
        result = super().batch_transfer_sync_read(endpoint, targets, sources, sizes)
        if result == 0:
            for target, source, size in zip(targets, sources, sizes, strict=True):
                ctypes.memmove(target, source, size)
        return result


def runtime(buffer, *, prefix, role, epoch):
    inputs = manifests(tp=1, prefix=prefix, address_base=ctypes.addressof(buffer))
    binding = inputs.bindings[0]
    binding = replace(
        binding,
        generation=epoch,
        instance_id=f"{prefix}-{role}",
        lease_id=f"{role}-lease-{epoch}",
        fragments=(replace(binding.fragments[0], device="cpu", owner=buffer),),
    )
    return RuntimeInputs(inputs.placement, (binding,))


def execute(reader, cache, source, target, *, same_fleet, **guards):
    kwargs = dict(
        source_registrations=registration_leases(source),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
        source_allocation_guards=guards.get("source_guards", allocation_guards(source)),
        target_allocation_guards=guards.get("target_guards", allocation_guards(target)),
    )
    if same_fleet:
        return InplaceWeightResharder(reader, plan_cache=cache).execute(
            source.placement,
            source.bindings,
            target.placement,
            target.bindings[0],
            **kwargs,
        )
    logical = cache.plan(
        source.placement, target.placement, target.bindings[0].participant_id
    )
    plan = bind_logical_transfer_plan(
        logical, target.bindings, source_bindings=source.bindings
    )
    return reader.execute(
        plan,
        source.placement,
        source.bindings,
        target.placement,
        target.bindings[0],
        **kwargs,
    )


def coordinator(chunks=3):
    stream = WeightStreamCoordinator(slot_count=1)
    stream.configure(["consumer"], writer_world_size=1)
    stream.begin_source(1, 0)
    stream.stream_header(
        1,
        0,
        dict(
            chunks=chunks, payload_bytes=8 * chunks, writer_rank=0, writer_world_size=1
        ),
    )
    return stream


def publish(stream, ordinal):
    key = f"chunk-{ordinal}"
    stream.publish(key, dict(metadata=dict(generation=1, writer_rank=0)))
    stream.publish_chunk(1, 0, ordinal, key, key)
    return key


@pytest.mark.parametrize("same_fleet", [False, True])
def test_streamed_public_reader_waits_for_consumption_and_rebinds(same_fleet):
    async def scenario():
        stream = coordinator()
        source_buffer = ctypes.create_string_buffer(8)
        targets = []
        reader = MooncakeTransferEngineReader(CopyEngine())
        cache = WeightTransferPlanCache(enabled=True)
        for ordinal, value in enumerate((b"abcdefgh", b"12345678", b"ABCDEFGH")):
            source_buffer.raw = value
            target_buffer = ctypes.create_string_buffer(8)
            targets.append((target_buffer, value))
            source = runtime(
                source_buffer, prefix="fleet", role="source", epoch=ordinal + 1
            )
            target = runtime(
                target_buffer,
                prefix="fleet" if same_fleet else "inference",
                role="target",
                epoch=ordinal + 1,
            )
            token = publish(stream, ordinal)
            consumption_complete = asyncio.Event()

            async def consume():
                receipts = execute(reader, cache, source, target, same_fleet=same_fleet)
                assert sum(receipt.nbytes for receipt in receipts) == 8
                assert target_buffer.raw == value
                # A terminal transfer alone does not finish framework use of a slot.
                await consumption_complete.wait()
                stream.ack_chunk(1, 0, ordinal, token, "consumer")

            consuming = asyncio.create_task(consume())
            blocked_producer = asyncio.create_task(
                stream.wait_reusable(1, 0, ordinal + 1)
            )
            await asyncio.sleep(0)
            assert not consuming.done() and not blocked_producer.done()
            assert not stream.reusable(1, 0, ordinal + 1)
            consumption_complete.set()
            await consuming
            assert await blocked_producer
        # Cache entries are logical: each real execution acquired fresh bindings.
        assert cache.cache_info().hits == 2
        assert len(reader.engine.calls) == 3
        assert len({call[1][0] for call in reader.engine.calls}) == 3
        assert all(buffer.raw == value for buffer, value in targets)
        assert stream.acknowledge(1, "consumer") == 1
        assert stream.state()["bounded_chunks_acked"] == 3

    asyncio.run(scenario())


@pytest.mark.parametrize("same_fleet", [False, True])
def test_streamed_public_reader_unknown_keeps_tokens_and_slot_leased(same_fleet):
    stream = coordinator(chunks=1)
    token = publish(stream, 0)
    source = runtime(
        ctypes.create_string_buffer(b"abcdefgh", 8),
        prefix="fleet",
        role="source",
        epoch=1,
    )
    target = runtime(
        ctypes.create_string_buffer(8),
        prefix="fleet" if same_fleet else "inference",
        role="target",
        epoch=1,
    )
    engine = CopyEngine()
    engine.read_result = 5
    reader = MooncakeTransferEngineReader(engine)
    source_guards, target_guards = allocation_guards(source), allocation_guards(target)
    with pytest.raises(TransferCompletionUnknownError):
        execute(
            reader,
            WeightTransferPlanCache(),
            source,
            target,
            same_fleet=same_fleet,
            source_guards=source_guards,
            target_guards=target_guards,
        )
        stream.ack_chunk(1, 0, 0, token, "consumer")
    assert reader.pending_transfer_ids()
    assert not stream.reusable(1, 0, 1)
    assert all(
        not token.released_states
        for guard in (*source_guards.values(), *target_guards.values())
        for token in guard.tokens
    )
    with pytest.raises(ValueError, match="precedes chunk"):
        stream.acknowledge(1, "consumer")
    assert engine.unregister_calls == []
