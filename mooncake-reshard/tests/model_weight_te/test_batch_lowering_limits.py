from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.manifest import SplitAxis, TensorDescriptor
from mooncake.reshard.weight.planner import (
    TransferRegion,
)
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    TransferEngineError,
)
from mooncake.reshard.weight._te.batching import iter_transfer_batches

from global_placement_helpers import global_placement

from .helpers import (
    FakeTransferEngine,
    RuntimeInputs,
    execute_reader,
    execute_sink,
    manifests,
    nd_te_manifests,
    participant_inputs,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
    runtime_fragment,
)


def _dp_sources() -> RuntimeInputs:
    source_dp0 = manifests(tp=1, prefix="source-d0", address_base=0x10000)
    dp0_fragment = source_dp0.placement.fragments[0]
    dp1_rank = replace(dp0_fragment.rank, dp=1)
    placement_fragment = replace(
        dp0_fragment,
        placement_fragment_id="source-d1-placement",
        rank=dp1_rank,
    )
    placement = global_placement(
        resource_id=source_dp0.placement.resource_id,
        revision=source_dp0.placement.revision,
        weight_generation=source_dp0.placement.weight_generation,
        placement_set_id="source-dp",
        tensors=source_dp0.placement.tensors,
        fragments=(dp0_fragment, placement_fragment),
        ranks=(dp0_fragment.rank, dp1_rank),
    )
    participant_by_rank = {
        participant.rank: participant.participant_id
        for participant in placement.topology.participants
    }
    dp0_binding = replace(
        source_dp0.bindings[0],
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        participant_id=participant_by_rank[dp0_fragment.rank],
    )
    binding_fragment = replace(
        dp0_binding.fragments[0],
        placement_fragment_id=placement_fragment.placement_fragment_id,
        fragment_id="source-d1-fragment",
        address=0x20000,
        storage_address=0x20000,
        worker_id="source-d1-t0",
        endpoint="source-d1-t0:12345",
    )
    binding = replace(
        dp0_binding,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        participant_id=participant_by_rank[dp1_rank],
        instance_id="source-d1-t0",
        lease_id="source-d1-t0-runtime-lease",
        fragments=(binding_fragment,),
    )
    return RuntimeInputs(placement, (dp0_binding, binding))


def test_te_sink_lowers_nd_regions_in_bounded_batches() -> None:
    sources = nd_te_manifests("source", 0x10000, source=True)
    targets = nd_te_manifests("target", 0x40000, source=False)
    plan = plan_transfer(sources, targets)
    engine = FakeTransferEngine()

    receipts = execute_sink(
        MooncakeTransferEngineSink(
            engine,
            max_batch_operations=5,
            max_region_segments=12,
        ),
        plan,
        sources,
        targets,
        target_registrations=registration_leases(targets),
    )

    assert [receipt.operation_count for receipt in receipts] == [12, 12]
    assert [receipt.nbytes for receipt in receipts] == [48, 48]
    assert [len(call[3]) for call in engine.calls] == [5, 5, 2, 5, 5, 2]
    assert max(len(call[3]) for call in engine.calls) == 5


def test_te_reader_lowers_nd_regions_in_bounded_batches() -> None:
    sources = nd_te_manifests("source", 0x10000, source=True)
    target = participant_inputs(nd_te_manifests("target", 0x40000, source=False), 1)
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    receipts = execute_reader(
        MooncakeTransferEngineReader(
            engine,
            max_batch_operations=5,
            max_region_segments=12,
        ),
        plan,
        sources,
        target,
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
    )

    assert [receipt.operation_count for receipt in receipts] == [12, 12]
    assert [receipt.nbytes for receipt in receipts] == [48, 48]
    assert [len(call[3]) for call in engine.calls] == [5, 5, 2, 5, 5, 2]
    assert max(len(call[3]) for call in engine.calls) == 5


@pytest.mark.parametrize("executor", ["sink", "reader"])
def test_te_rejects_nd_region_above_lowering_limit(executor: str) -> None:
    sources = nd_te_manifests("source", 0x10000, source=True)
    targets = nd_te_manifests("target", 0x40000, source=False)
    engine = FakeTransferEngine()

    with pytest.raises(TransferEngineError, match="max_region_segments"):
        if executor == "sink":
            plan = plan_transfer(sources, targets)
            execute_sink(
                MooncakeTransferEngineSink(engine, max_region_segments=11),
                plan,
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )
        else:
            target = participant_inputs(targets, 1)
            plan = plan_transfer_to_local_target(sources, target)
            execute_reader(
                MooncakeTransferEngineReader(engine, max_region_segments=11),
                plan,
                sources,
                target,
                source_registrations=registration_leases(sources),
                target_pre_registered=True,
                target_registrations=registration_leases(target),
            )

    assert engine.calls == []
    assert engine.register_calls == []


def test_te_sink_lowers_canonical_transfer_region() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    planned = plan_transfer(sources, targets)
    region = planned.operations[0]
    assert isinstance(region, TransferRegion)
    engine = FakeTransferEngine()

    receipts = execute_sink(
        MooncakeTransferEngineSink(engine),
        planned,
        sources,
        targets,
        target_registrations=registration_leases(targets),
    )

    assert receipts[0].nbytes == sources.placement.fragments[0].nbytes
    assert len(engine.calls) == 1


def test_te_sink_expands_compact_ranges_in_bounded_batches() -> None:
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(5, 8),
        dtype="uint8",
        itemsize=1,
        shard_dims=(1,),
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
        parallel_axes=(SplitAxis("tp", dim=1),),
    )
    sources = manifests(
        tp=2,
        prefix="source",
        address_base=0x10000,
        tensor=tensor,
    )
    targets = manifests(
        tp=4,
        prefix="target",
        address_base=0x40000,
        tensor=tensor,
    )
    plan = plan_transfer(sources, targets)
    engine = FakeTransferEngine()

    receipts = execute_sink(
        MooncakeTransferEngineSink(engine, max_batch_operations=2),
        plan,
        sources,
        targets,
        target_registrations=registration_leases(targets),
    )

    assert [len(call[1]) for call in engine.calls] == [2, 2, 1, 2, 2, 1]
    assert sum(receipt.operation_count for receipt in receipts) == 10
    assert sum(receipt.nbytes for receipt in receipts) == 20


def test_te_sink_skips_an_unselected_source_without_an_executor() -> None:
    sources = _dp_sources()
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    assert {executor.rank.dp for executor in plan.source_executors} == {0}
    engine = FakeTransferEngine()

    receipts = execute_sink(
        MooncakeTransferEngineSink(engine),
        plan,
        sources,
        targets,
        source_binding_index=1,
        target_registrations=registration_leases(targets),
    )

    assert receipts == ()
    assert engine.calls == []
    assert engine.register_calls == []


def test_te_sink_skips_a_stale_unselected_source_without_acquiring_a_guard() -> None:
    sources = _dp_sources()
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    stale = RuntimeInputs(
        sources.placement,
        (
            sources.bindings[0],
            replace(
                sources.bindings[1],
                generation=2,
                fragments=(
                    replace(
                        sources.bindings[1].fragments[0],
                        address=0x90000,
                        storage_address=0x90000,
                        endpoint="source-d1-t0:54321",
                    ),
                ),
            ),
        ),
    )

    engine = FakeTransferEngine()
    receipts = execute_sink(
        MooncakeTransferEngineSink(engine),
        plan,
        stale,
        targets,
        source_binding_index=1,
        target_registrations=registration_leases(targets),
    )

    assert receipts == ()
    assert engine.calls == []
    assert engine.register_calls == []


def test_te_reader_batches_large_repeats_without_segment_tuple_expansion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repeat = 8192
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(repeat, 8),
        dtype="uint8",
        itemsize=1,
        shard_dims=(1,),
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
        parallel_axes=(SplitAxis("tp", dim=1),),
    )
    sources = manifests(
        tp=2,
        prefix="source",
        address_base=0x10000,
        tensor=tensor,
    )
    target = participant_inputs(
        manifests(
            tp=1,
            prefix="target",
            address_base=0x40000,
            tensor=tensor,
        ),
        0,
    )
    plan = plan_transfer_to_local_target(sources, target)
    state = {"yielded": 0, "exhausted": False, "first_batch": None}

    class StreamingProbeTransferEngine(FakeTransferEngine):
        def batch_transfer_sync_read(
            self,
            endpoint,
            target_addresses,
            source_addresses,
            sizes,
        ):
            if state["first_batch"] is None:
                state["first_batch"] = (state["yielded"], state["exhausted"])
            return super().batch_transfer_sync_read(
                endpoint,
                target_addresses,
                source_addresses,
                sizes,
            )

    engine = StreamingProbeTransferEngine()

    assert [operation.repeat for operation in plan.operations] == [repeat, repeat]
    assert all(isinstance(operation, TransferRegion) for operation in plan.operations)

    original_iter_segments = TransferRegion.iter_segments

    def observe_streaming_segments(
        self: TransferRegion,
        *,
        max_segments: int,
    ):
        try:
            for segment in original_iter_segments(self, max_segments=max_segments):
                state["yielded"] += 1
                yield segment
        finally:
            state["exhausted"] = True

    monkeypatch.setattr(TransferRegion, "iter_segments", observe_streaming_segments)

    receipts = execute_reader(
        MooncakeTransferEngineReader(engine, max_batch_operations=1024),
        plan,
        sources,
        target,
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
    )

    endpoints = [call[0] for call in engine.calls]
    assert endpoints == ["source-t0:12345"] * 8 + ["source-t1:12345"] * 8
    assert all(len(call[3]) == 1024 for call in engine.calls)
    assert [receipt.operation_count for receipt in receipts] == [repeat, repeat]
    assert [receipt.nbytes for receipt in receipts] == [repeat * 4, repeat * 4]
    assert state["first_batch"] == (1024, False)
    assert engine.calls[0][1][0] == 0x40000
    assert engine.calls[7][1][-1] == 0x40000 + (repeat - 1) * 8
    assert engine.calls[8][1][0] == 0x40004
    assert engine.calls[15][1][-1] == 0x40004 + (repeat - 1) * 8


def test_te_ranges_use_allocation_bases_and_view_relative_offsets() -> None:
    class Operation:
        @staticmethod
        def iter_segments(*, max_segments: int):
            assert max_segments >= 2
            yield 1, 2, 3
            yield 4, 8, 2

    source = runtime_fragment(
        placement_fragment_id="source-placement",
        fragment_id="source-fragment",
        address=0x1100,
        nbytes=16,
        worker_id="source",
        endpoint="source:12345",
        local_shape=(16,),
        itemsize=1,
        storage_address=0x1000,
        storage_nbytes=0x1000,
    )
    target = runtime_fragment(
        placement_fragment_id="target-placement",
        fragment_id="target-fragment",
        address=0x2280,
        nbytes=16,
        worker_id="target",
        endpoint="target:12345",
        local_shape=(16,),
        itemsize=1,
        storage_address=0x2000,
        storage_nbytes=0x1000,
    )

    (batch,) = tuple(
        iter_transfer_batches(
            target.endpoint,
            ((Operation(), source, target),),
            max_batch_operations=8,
            max_region_segments=2,
        )
    )
    (transfer_range,) = batch.ranges

    assert transfer_range.source_base_address == source.storage_address
    assert transfer_range.source_capacity == source.storage_nbytes
    assert transfer_range.target_base_address == target.storage_address
    assert transfer_range.target_capacity == target.storage_nbytes
    assert transfer_range.source_offsets == (0x101, 0x104)
    assert transfer_range.target_offsets == (0x282, 0x288)
    assert batch.source_addresses == (source.address + 1, source.address + 4)
    assert batch.target_addresses == (target.address + 2, target.address + 8)
