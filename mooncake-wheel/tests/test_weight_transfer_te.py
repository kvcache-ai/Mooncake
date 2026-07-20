from __future__ import annotations

from dataclasses import replace
from math import prod

import pytest

from mooncake.weight_transfer.manifest import (
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    TensorDescriptor,
)
from mooncake.weight_transfer.planner import (
    CopyRange,
    plan_runtime_transfer,
    plan_runtime_transfer_to_local_target,
)
from mooncake.weight_transfer.te import (
    MemoryRegistrationLease,
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    TransferEngineError,
)


class FakeTransferEngine:
    def __init__(self) -> None:
        self.calls = []
        self.fail_endpoint: str | None = None
        self.read_result: int | None = None
        self.register_calls: list[tuple[int, int]] = []
        self.unregister_calls: list[int] = []

    def register_memory(self, address: int, nbytes: int) -> int:
        self.register_calls.append((address, nbytes))
        return 0

    def unregister_memory(self, address: int) -> int:
        self.unregister_calls.append(address)
        return 0

    def batch_transfer_sync_write(
        self,
        endpoint: str,
        source_addresses: list[int],
        target_addresses: list[int],
        sizes: list[int],
    ) -> int:
        self.calls.append((endpoint, source_addresses, target_addresses, sizes))
        return -5 if endpoint == self.fail_endpoint else 0

    def batch_transfer_sync_read(
        self,
        endpoint: str,
        target_addresses: list[int],
        source_addresses: list[int],
        sizes: list[int],
    ) -> int:
        self.calls.append((endpoint, target_addresses, source_addresses, sizes))
        if self.read_result is not None:
            return self.read_result
        return -5 if endpoint == self.fail_endpoint else 0


def manifests(
    tp: int,
    prefix: str,
    address_base: int,
    *,
    tensor: TensorDescriptor | None = None,
) -> tuple[RuntimeManifest, ...]:
    tensor = tensor or TensorDescriptor(
        tensor_id="layers.0.mlp.gate_up",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        partition_dim=0,
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
    )
    dim = tensor.partition_dim
    assert dim is not None
    extent = tensor.global_shape[dim] // tp
    result = []
    for tp_rank in range(tp):
        worker_id = f"{prefix}-t{tp_rank}"
        local_shape = list(tensor.global_shape)
        local_shape[dim] = extent
        global_offset = [0] * len(tensor.global_shape)
        global_offset[dim] = tp_rank * extent
        fragment = RuntimeFragment(
            fragment_id=f"{worker_id}-fragment",
            tensor_id=tensor.tensor_id,
            global_offset=tuple(global_offset),
            local_shape=tuple(local_shape),
            address=address_base + tp_rank * 0x1000,
            nbytes=prod(local_shape) * tensor.itemsize,
            worker_id=worker_id,
            endpoint=f"{worker_id}:12345",
            rank=ParallelRank(tp=tp_rank),
            lease_generation=1,
        )
        result.append(
            RuntimeManifest(
                model_id="qwen3.5-0.8b",
                revision="step-42",
                instance_id=worker_id,
                tensors=(tensor,),
                fragments=(fragment,),
                lease_id=f"{worker_id}-runtime-lease",
            )
        )
    return tuple(result)


def registration_leases(
    manifests: tuple[RuntimeManifest, ...],
) -> tuple[MemoryRegistrationLease, ...]:
    return tuple(
        MemoryRegistrationLease.from_fragment(
            fragment,
            runtime_lease_id=manifest.lease_id,
        )
        for manifest in manifests
        for fragment in manifest.fragments
    )


def test_te_sink_executes_local_source_ranges_without_staging_buffer() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    engine = FakeTransferEngine()
    sink = MooncakeTransferEngineSink(engine)

    receipts = sink.execute(
        plan,
        sources[0],
        targets,
        target_registrations=registration_leases(targets),
    )

    assert receipts[0].source_worker_id == "source-t0"
    assert sum(receipt.nbytes for receipt in receipts) == 4
    assert engine.calls == [
        ("target-t0:12345", [0x10000], [0x40000], [2]),
        ("target-t1:12345", [0x10002], [0x41000], [2]),
    ]
    assert engine.register_calls == [(0x10000, 4)]
    assert engine.unregister_calls == [0x10000]


def test_te_sink_requires_generation_bound_target_registration_leases() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    sink = MooncakeTransferEngineSink(FakeTransferEngine())

    with pytest.raises(TransferEngineError, match="target registration"):
        sink.execute(plan, sources[0], targets)

    stale_leases = list(registration_leases(targets))
    stale_leases[0] = replace(stale_leases[0], lease_generation=2)
    with pytest.raises(TransferEngineError, match="target registration"):
        sink.execute(
            plan,
            sources[0],
            targets,
            target_registrations=tuple(stale_leases),
        )


def test_te_sink_surfaces_endpoint_failure() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    engine = FakeTransferEngine()
    engine.fail_endpoint = "target-t1:12345"

    with pytest.raises(TransferEngineError, match="target-t1:12345"):
        MooncakeTransferEngineSink(engine).execute(
            plan,
            sources[0],
            targets,
            target_registrations=registration_leases(targets),
        )


def test_te_sink_rejects_stale_source_generation() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    stale_fragment = replace(sources[0].fragments[0], lease_generation=2)
    stale_manifest = replace(sources[0], fragments=(stale_fragment,))

    with pytest.raises(TransferEngineError, match="source executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            stale_manifest,
            targets,
            target_registrations=registration_leases(targets),
        )


def test_te_sink_rejects_generation_scoped_source_id_rollover() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    replacement = replace(
        sources[0].fragments[0],
        fragment_id="replacement-source-fragment",
        worker_id="replacement-source-worker",
        lease_generation=2,
    )
    current = replace(
        sources[0],
        instance_id="replacement-source-instance",
        fragments=(replacement,),
    )

    with pytest.raises(TransferEngineError, match="source executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            current,
            targets,
            target_registrations=registration_leases(targets),
        )


def test_te_sink_rejects_stale_target_address_and_generation() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    replacement = replace(targets[0].fragments[0], address=0x90000, lease_generation=2)
    current_targets = (replace(targets[0], fragments=(replacement,)), *targets[1:])

    with pytest.raises(TransferEngineError, match="target executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources[0],
            current_targets,
            target_registrations=registration_leases(targets),
        )


def test_te_sink_rejects_generation_scoped_target_id_rollover() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    replacement = replace(
        targets[0].fragments[0],
        fragment_id="replacement-target-fragment",
        lease_generation=2,
    )
    current_targets = (replace(targets[0], fragments=(replacement,)), *targets[1:])

    with pytest.raises(TransferEngineError, match="target executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources[0],
            current_targets,
            target_registrations=registration_leases(targets),
        )


@pytest.mark.parametrize("side", ["source", "target"])
def test_te_sink_rejects_revision_mismatch(side: str) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    if side == "source":
        sources = (replace(sources[0], revision="step-43"), *sources[1:])
    else:
        targets = (replace(targets[0], revision="step-43"), *targets[1:])

    with pytest.raises(TransferEngineError, match="revision mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources[0],
            targets,
            target_registrations=registration_leases(targets),
        )


def test_te_sink_expands_compact_ranges_in_bounded_batches() -> None:
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(5, 8),
        dtype="uint8",
        itemsize=1,
        partition_dim=1,
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
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
    plan = plan_runtime_transfer(sources, targets)
    engine = FakeTransferEngine()

    receipts = MooncakeTransferEngineSink(engine, max_batch_operations=2).execute(
        plan,
        sources[0],
        targets,
        target_registrations=registration_leases(targets),
    )

    assert [len(call[1]) for call in engine.calls] == [2, 2, 1, 2, 2, 1]
    assert sum(receipt.operation_count for receipt in receipts) == 10
    assert sum(receipt.nbytes for receipt in receipts) == 20


def test_te_sink_allows_only_an_explicitly_planned_noop_source_executor() -> None:
    source_dp0 = manifests(tp=1, prefix="source-d0", address_base=0x10000)[0]
    source_dp1_fragment = replace(
        source_dp0.fragments[0],
        fragment_id="source-d1-fragment",
        address=0x20000,
        worker_id="source-d1-t0",
        endpoint="source-d1-t0:12345",
        rank=replace(source_dp0.fragments[0].rank, dp=1),
    )
    source_dp1 = replace(
        source_dp0,
        instance_id="source-d1-t0",
        fragments=(source_dp1_fragment,),
    )
    sources = (source_dp0, source_dp1)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer(sources, targets)
    engine = FakeTransferEngine()

    receipts = MooncakeTransferEngineSink(engine).execute(
        plan,
        source_dp1,
        targets,
        target_registrations=registration_leases(targets),
    )

    assert receipts == ()
    assert engine.calls == []
    assert engine.register_calls == []


def test_te_sink_fences_runtime_lease_for_explicit_noop_executor() -> None:
    source_dp0 = manifests(tp=1, prefix="source-d0", address_base=0x10000)[0]
    source_dp1_fragment = replace(
        source_dp0.fragments[0],
        fragment_id="source-d1-fragment",
        address=0x20000,
        worker_id="source-d1-t0",
        endpoint="source-d1-t0:12345",
        rank=replace(source_dp0.fragments[0].rank, dp=1),
    )
    source_dp1 = replace(
        source_dp0,
        instance_id="source-d1-t0",
        fragments=(source_dp1_fragment,),
    )
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer((source_dp0, source_dp1), targets)
    stale = replace(
        source_dp1,
        fragments=(
            replace(
                source_dp1_fragment,
                address=0x90000,
                endpoint="source-d1-t0:54321",
                lease_generation=2,
            ),
        ),
    )

    with pytest.raises(TransferEngineError, match="source executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            stale,
            targets,
            target_registrations=registration_leases(targets),
        )


def test_te_receipt_identifies_worker_instead_of_serving_instance() -> None:
    source = replace(
        manifests(tp=1, prefix="source", address_base=0x10000)[0],
        instance_id="serving-instance",
    )
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_runtime_transfer((source,), targets)

    receipts = MooncakeTransferEngineSink(FakeTransferEngine()).execute(
        plan,
        source,
        targets,
        target_registrations=registration_leases(targets),
    )

    assert receipts[0].source_worker_id == "source-t0"


def test_te_reader_pulls_local_target_ranges_without_source_rpc() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    target = targets[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    receipts = MooncakeTransferEngineReader(engine).execute(
        plan,
        sources,
        target,
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases((target,)),
    )

    assert receipts[0].source_endpoint == "source-t0:12345"
    assert receipts[0].target_worker_id == "target-t1"
    assert receipts[0].nbytes == 2
    assert engine.calls == [("source-t0:12345", [0x41000], [0x10002], [2])]
    assert engine.register_calls == []
    assert engine.unregister_calls == []


def test_te_reader_requires_generation_bound_source_registration_leases() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    reader = MooncakeTransferEngineReader(FakeTransferEngine())
    target_leases = registration_leases((target,))

    with pytest.raises(TransferEngineError, match="source registration leases"):
        reader.execute(
            plan,
            sources,
            target,
            target_pre_registered=True,
            target_registrations=target_leases,
        )

    stale_generation = list(registration_leases(sources))
    stale_generation[0] = replace(stale_generation[0], lease_generation=2)
    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        reader.execute(
            plan,
            sources,
            target,
            source_registrations=stale_generation,
            target_pre_registered=True,
            target_registrations=target_leases,
        )

    stale_runtime_lease = list(registration_leases(sources))
    stale_runtime_lease[0] = replace(
        stale_runtime_lease[0], runtime_lease_id="stale-runtime-lease"
    )
    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        reader.execute(
            plan,
            sources,
            target,
            source_registrations=stale_runtime_lease,
            target_pre_registered=True,
            target_registrations=target_leases,
        )


@pytest.mark.parametrize(
    "field, value",
    [
        ("address", 0x90000),
        ("nbytes", 1),
        ("lease_generation", 2),
        ("runtime_lease_id", "stale-runtime-lease"),
    ],
)
def test_te_reader_rejects_source_registration_snapshot_mismatch(
    field: str, value: int | str
) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    leases = list(registration_leases(sources))
    leases[0] = replace(leases[0], **{field: value})

    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan,
            sources,
            target,
            source_registrations=leases,
            target_pre_registered=True,
            target_registrations=registration_leases((target,)),
        )


def test_te_reader_requires_runtime_lease_id_for_used_source_fragment() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    sources = (replace(sources[0], lease_id=None), sources[1])
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)

    with pytest.raises(TransferEngineError, match="source runtime lease_id"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan,
            sources,
            target,
            source_registrations=registration_leases(sources),
            target_pre_registered=True,
            target_registrations=registration_leases((target,)),
        )


def test_te_reader_requires_registrations_only_for_used_source_fragments() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    used_fragment_ids = {operation.source.fragment_id for operation in plan.operations}
    source_registrations = tuple(
        registration
        for registration in registration_leases(sources)
        if registration.fragment_id in used_fragment_ids
    )

    receipts = MooncakeTransferEngineReader(FakeTransferEngine()).execute(
        plan,
        sources,
        target,
        source_registrations=source_registrations,
        target_pre_registered=True,
        target_registrations=registration_leases((target,)),
    )

    assert sum(receipt.nbytes for receipt in receipts) == 2


def test_te_reader_batches_large_repeats_without_segment_tuple_expansion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repeat = 8192
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(repeat, 8),
        dtype="uint8",
        itemsize=1,
        partition_dim=1,
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
    )
    sources = manifests(
        tp=2,
        prefix="source",
        address_base=0x10000,
        tensor=tensor,
    )
    target = manifests(
        tp=1,
        prefix="target",
        address_base=0x40000,
        tensor=tensor,
    )[0]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    assert [operation.repeat for operation in plan.operations] == [repeat, repeat]

    def reject_segment_tuple_expansion(
        self: CopyRange,
    ) -> None:
        raise AssertionError(
            f"reader expanded {self.repeat} segments through iter_segments"
        )

    monkeypatch.setattr(CopyRange, "iter_segments", reject_segment_tuple_expansion)

    receipts = MooncakeTransferEngineReader(engine, max_batch_operations=1024).execute(
        plan,
        sources,
        target,
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases((target,)),
    )

    endpoints = [call[0] for call in engine.calls]
    assert endpoints == ["source-t0:12345"] * 8 + ["source-t1:12345"] * 8
    assert all(len(call[3]) == 1024 for call in engine.calls)
    assert [receipt.operation_count for receipt in receipts] == [repeat, repeat]
    assert [receipt.nbytes for receipt in receipts] == [repeat * 4, repeat * 4]
    assert engine.calls[0][1][0] == 0x40000
    assert engine.calls[7][1][-1] == 0x40000 + (repeat - 1) * 8
    assert engine.calls[8][1][0] == 0x40004
    assert engine.calls[15][1][-1] == 0x40004 + (repeat - 1) * 8


def test_te_reader_requires_complete_planned_source_executor_set() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)

    with pytest.raises(TransferEngineError, match="source executor set is incomplete"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan,
            sources[:1],
            target,
            source_registrations=registration_leases(sources[:1]),
            target_pre_registered=True,
            target_registrations=registration_leases((target,)),
        )


def test_te_reader_surfaces_source_endpoint_failure() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    engine.fail_endpoint = "source-t0:12345"

    with pytest.raises(TransferEngineError, match="source-t0:12345"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources,
            target,
            source_registrations=registration_leases(sources),
            target_pre_registered=True,
            target_registrations=registration_leases((target,)),
        )


def test_te_reader_rejects_positive_nonzero_transfer_status() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    engine.read_result = 5

    with pytest.raises(TransferEngineError, match="failed: 5"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources,
            target,
            source_registrations=registration_leases(sources),
            target_pre_registered=True,
            target_registrations=registration_leases((target,)),
        )


def test_te_reader_rejects_leases_without_pre_registered_mode() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = manifests(tp=4, prefix="target", address_base=0x40000)[1]
    plan = plan_runtime_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    with pytest.raises(TransferEngineError, match="target_pre_registered"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources,
            target,
            source_registrations=registration_leases(sources),
            target_registrations=registration_leases((target,)),
        )

    assert engine.register_calls == []
    assert engine.unregister_calls == []
