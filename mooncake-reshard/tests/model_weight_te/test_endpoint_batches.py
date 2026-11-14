from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.transfer_engine import (
    MooncakeTransferEngineExecutor,
    TransferDirection,
)
from mooncake.reshard.weight._te.execution import (
    endpoint_receipt_totals,
    iter_endpoint_batches,
)
from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
)

from .helpers import (
    FakeAsyncScatterTransferEngine,
    FakeTransferEngine,
    execute_reader,
    execute_sink,
    manifests,
    multi_dim_te_manifests,
    participant_inputs,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
)


@pytest.mark.parametrize("direction", ["read", "write"])
def test_endpoint_receipts_count_physical_segments(direction: str) -> None:
    sources = multi_dim_te_manifests("source", 0x10000, source=True)
    targets = multi_dim_te_manifests("target", 0x40000, source=False)
    engine = FakeTransferEngine()
    if direction == "read":
        target = participant_inputs(targets, 1)
        receipts = execute_reader(
            MooncakeTransferEngineReader(engine, max_batch_operations=5),
            plan_transfer_to_local_target(sources, target),
            sources,
            target,
            source_registrations=registration_leases(sources),
        )
        endpoints = [receipt.source_endpoint for receipt in receipts]
    else:
        receipts = execute_sink(
            MooncakeTransferEngineSink(engine, max_batch_operations=5),
            plan_transfer(sources, targets),
            sources,
            targets,
            target_registrations=registration_leases(targets),
        )
        endpoints = [receipt.target_endpoint for receipt in receipts]

    assert len(endpoints) == 2
    assert endpoints == sorted(endpoints)
    assert [call[0] for call in engine.calls] == [
        endpoint for endpoint in endpoints for _ in range(3)
    ]
    assert [len(call[3]) for call in engine.calls] == [5, 5, 2, 5, 5, 2]
    assert [(receipt.operation_count, receipt.nbytes) for receipt in receipts] == [
        (12, 48),
        (12, 48),
    ]


@pytest.mark.parametrize("direction", ["read", "write"])
def test_unselected_endpoint_has_no_receipt_or_io(direction: str) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=2, prefix="target", address_base=0x40000)
    local_target = participant_inputs(targets, 0)
    plan = plan_transfer_to_local_target(sources, local_target)
    engine = FakeTransferEngine()
    if direction == "read":
        receipts = execute_reader(
            MooncakeTransferEngineReader(engine),
            plan,
            sources,
            targets,
            target_binding_index=1,
        )
    else:
        receipts = execute_sink(
            MooncakeTransferEngineSink(engine),
            plan,
            sources,
            local_target,
            source_binding_index=1,
        )
    assert receipts == ()
    assert engine.calls == []
    assert engine.register_calls == []


def test_canonical_region_rejects_zero_size_before_batching() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    (operation,) = plan_transfer(sources, targets).operations
    with pytest.raises(ValueError, match="positive"):
        replace(operation, inner_bytes=0)
    with pytest.raises(ValueError, match="positive"):
        replace(
            operation,
            outer_loop_counts=(0,),
            source_strides=(0,),
            target_strides=(0,),
        )


@pytest.mark.parametrize("direction", [TransferDirection.READ, TransferDirection.WRITE])
def test_endpoint_batches_sort_and_total_actual_receipts(direction) -> None:
    operations = []
    for source_address, target_address in (
        (0x30000, 0x50000),
        (0x20000, 0x40000),
        (0x10000, 0x40000),
    ):
        sources = manifests(tp=1, prefix="source", address_base=source_address)
        targets = manifests(tp=1, prefix="target", address_base=target_address)
        (operation,) = plan_transfer(sources, targets).operations
        operations.append((operation, operation.source, operation.target))
    batches = tuple(
        iter_endpoint_batches(
            {"z:12345": operations[:1], "empty:12345": [], "a:12345": operations},
            max_batch_operations=2,
            max_region_segments=1,
        )
    )
    assert [batch.endpoint for batch in batches] == ["a:12345", "a:12345", "z:12345"]
    assert [batch.source_addresses for batch in batches] == [
        (0x10000, 0x20000),
        (0x30000,),
        (0x30000,),
    ]
    assert [batch.target_addresses for batch in batches] == [
        (0x40000, 0x40000),
        (0x50000,),
        (0x50000,),
    ]
    engine = FakeAsyncScatterTransferEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    with executor.submission() as submission:
        receipts = submission.execute_batches(
            batches, direction, max_inflight_batches=2
        )
    assert endpoint_receipt_totals(receipts) == {
        "a:12345": (3, 24),
        "z:12345": (1, 8),
    }


@pytest.mark.parametrize("groups", [{}, {"empty:12345": []}])
def test_empty_endpoint_groups_produce_no_batches_or_totals(groups) -> None:
    batches = iter_endpoint_batches(
        groups, max_batch_operations=2, max_region_segments=1
    )
    engine = FakeTransferEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    with executor.submission() as submission:
        receipts = submission.execute_batches(batches, TransferDirection.READ)
    assert endpoint_receipt_totals(receipts) == {}
    assert engine.calls == []
