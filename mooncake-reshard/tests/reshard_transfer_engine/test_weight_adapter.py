from __future__ import annotations

from global_placement_helpers import global_placement
from model_weight_te.helpers import allocation_guards_for_bindings

from mooncake.reshard.transfer_engine import MooncakeTransferEngineExecutor
from mooncake.reshard.weight import (
    MemoryRegistrationLease,
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    SplitAxis,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    bind_logical_transfer_plan,
    plan_placement_transfer,
)


class FakeEngine:
    def get_engine_ptr(self) -> int:
        return id(self)


class CompletedTicket:
    status = "COMPLETED"


class RecordingEngine(FakeEngine):
    def __init__(self) -> None:
        self.calls = []

    def batch_transfer_sync_read_with_ticket(self, *arguments):
        self.calls.append(("read", arguments))
        return CompletedTicket()

    def batch_transfer_sync_write_with_ticket(self, *arguments):
        self.calls.append(("write", arguments))
        return CompletedTicket()


def placement_and_bindings(
    placement_set_id: str,
    address_base: int,
) -> tuple[
    WeightPlacementManifest,
    tuple[WeightRuntimeBindingManifest, ...],
]:
    ranks = (ParallelRank(tp=0), ParallelRank(tp=1))
    tensor = TensorDescriptor(
        tensor_id="weight",
        global_shape=(4,),
        dtype="float32",
        itemsize=4,
        shard_dims=(0,),
        layout_fingerprint="contiguous",
        parallel_axes=(SplitAxis("tp", dim=0),),
    )
    placement = global_placement(
        resource_id="model",
        revision="revision",
        placement_set_id=placement_set_id,
        tensors=(tensor,),
        fragments=tuple(
            PlacementFragment(
                placement_fragment_id=f"{placement_set_id}-{index}",
                tensor_id=tensor.tensor_id,
                global_offset=(index * 2,),
                local_shape=(2,),
                nbytes=8,
                rank=rank,
            )
            for index, rank in enumerate(ranks)
        ),
        ranks=ranks,
    )
    participant_by_rank = {
        participant.rank: participant.participant_id
        for participant in placement.topology.participants
    }
    bindings = tuple(
        WeightRuntimeBindingManifest(
            resource_id=placement.resource_id,
            revision=placement.revision,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            participant_id=participant_by_rank[fragment.rank],
            instance_id=f"{placement_set_id}-{index}",
            generation=1,
            lease_id=f"lease-{placement_set_id}-{index}",
            fragments=(
                RuntimeBindingFragment(
                    placement_fragment_id=fragment.placement_fragment_id,
                    fragment_id=f"runtime-{fragment.placement_fragment_id}",
                    address=address_base + index * 0x1000,
                    nbytes=fragment.nbytes,
                    worker_id=participant_by_rank[fragment.rank],
                    endpoint=f"{participant_by_rank[fragment.rank]}:12345",
                    device="cuda:0",
                    itemsize=tensor.itemsize,
                    local_shape=fragment.local_shape,
                    strides_bytes=(tensor.itemsize,),
                    storage_address=address_base + index * 0x1000,
                    storage_nbytes=fragment.nbytes,
                    storage_offset_bytes=0,
                ),
            ),
        )
        for index, fragment in enumerate(placement.fragments)
    )
    return placement, bindings


def registration_leases(
    bindings: tuple[WeightRuntimeBindingManifest, ...],
) -> tuple[MemoryRegistrationLease, ...]:
    return tuple(
        MemoryRegistrationLease.from_fragment(
            fragment,
            lease_generation=binding.generation,
            runtime_lease_id=binding.lease_id,
        )
        for binding in bindings
        for fragment in binding.fragments
    )


def test_weight_reader_and_sink_share_resource_neutral_executor() -> None:
    reader = MooncakeTransferEngineReader(FakeEngine())
    sink = MooncakeTransferEngineSink(FakeEngine())

    assert isinstance(reader.transfer_executor, MooncakeTransferEngineExecutor)
    assert isinstance(sink.transfer_executor, MooncakeTransferEngineExecutor)


def test_weight_reader_and_sink_execute_each_participant_from_global_placements() -> (
    None
):
    source_placement, source_bindings = placement_and_bindings("source", 0x1000)
    target_placement, target_bindings = placement_and_bindings("target", 0x3000)
    logical_plan = plan_placement_transfer(source_placement, target_placement)
    plan = bind_logical_transfer_plan(
        logical_plan,
        target_bindings,
        source_bindings=source_bindings,
    )

    read_engine = RecordingEngine()
    reader = MooncakeTransferEngineReader(read_engine)
    source_registrations = registration_leases(source_bindings)
    read_receipts = tuple(
        receipt
        for target_binding in target_bindings
        for receipt in reader.execute(
            plan,
            source_placement,
            source_bindings,
            target_placement,
            target_binding,
            source_registrations=source_registrations,
            target_pre_registered=True,
            target_registrations=registration_leases((target_binding,)),
            source_allocation_guards=allocation_guards_for_bindings(source_bindings),
            target_allocation_guards=allocation_guards_for_bindings((target_binding,)),
        )
    )

    write_engine = RecordingEngine()
    sink = MooncakeTransferEngineSink(write_engine)
    write_receipts = tuple(
        receipt
        for source_binding in source_bindings
        for receipt in sink.execute(
            plan,
            source_placement,
            source_binding,
            target_placement,
            target_bindings,
            target_registrations=registration_leases(target_bindings),
            source_pre_registered=True,
            source_registrations=registration_leases((source_binding,)),
            source_allocation_guards=allocation_guards_for_bindings((source_binding,)),
            target_allocation_guards=allocation_guards_for_bindings(target_bindings),
        )
    )

    assert [
        (
            receipt.source_endpoint,
            receipt.target_worker_id,
            receipt.operation_count,
            receipt.nbytes,
        )
        for receipt in read_receipts
    ] == [
        ("source-d0-p0-e0-t0:12345", "target-d0-p0-e0-t0", 1, 8),
        ("source-d0-p0-e0-t1:12345", "target-d0-p0-e0-t1", 1, 8),
    ]
    assert read_engine.calls == [
        (
            "read",
            ("source-d0-p0-e0-t0:12345", [0x3000], [0x1000], [8]),
        ),
        (
            "read",
            ("source-d0-p0-e0-t1:12345", [0x4000], [0x2000], [8]),
        ),
    ]

    assert [
        (
            receipt.source_worker_id,
            receipt.target_endpoint,
            receipt.operation_count,
            receipt.nbytes,
        )
        for receipt in write_receipts
    ] == [
        ("source-d0-p0-e0-t0", "target-d0-p0-e0-t0:12345", 1, 8),
        ("source-d0-p0-e0-t1", "target-d0-p0-e0-t1:12345", 1, 8),
    ]
    assert write_engine.calls == [
        (
            "write",
            ("target-d0-p0-e0-t0:12345", [0x1000], [0x3000], [8]),
        ),
        (
            "write",
            ("target-d0-p0-e0-t1:12345", [0x2000], [0x4000], [8]),
        ),
    ]
