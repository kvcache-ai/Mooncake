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
from mooncake.weight_transfer.planner import plan_runtime_transfer
from mooncake.weight_transfer.te import (
    MemoryRegistrationLease,
    MooncakeTransferEngineSink,
    TransferEngineError,
)


class FakeTransferEngine:
    def __init__(self) -> None:
        self.calls = []
        self.fail_endpoint: str | None = None
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
            )
        )
    return tuple(result)


def registration_leases(
    manifests: tuple[RuntimeManifest, ...],
) -> tuple[MemoryRegistrationLease, ...]:
    return tuple(
        MemoryRegistrationLease.from_fragment(fragment)
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
