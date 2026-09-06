from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.te import (
    TransferCompletionUnknownError,
    MooncakeTransferEngineSink,
    TransferEngineError,
)

from .helpers import (
    allocation_guards,
    allocation_guards_for_bindings,
    FakeTransferEngine,
    RuntimeInputs,
    manifests,
    plan_transfer,
    registration_leases,
    with_revision,
)


def test_te_sink_executes_local_source_ranges_without_staging_buffer() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    engine = FakeTransferEngine()
    sink = MooncakeTransferEngineSink(engine)

    receipts = sink.execute(
        plan,
        sources.placement,
        sources.bindings[0],
        targets.placement,
        targets.bindings,
        target_registrations=registration_leases(targets),
        source_allocation_guards=allocation_guards(sources),
        target_allocation_guards=allocation_guards(targets),
    )

    assert receipts[0].source_worker_id == "source-t0"
    assert sum(receipt.nbytes for receipt in receipts) == 4
    assert engine.calls == [
        ("target-t0:12345", [0x10000], [0x40000], [2]),
        ("target-t1:12345", [0x10002], [0x41000], [2]),
    ]
    assert engine.register_calls == [(0x10000, 4)]
    assert engine.unregister_calls == [0x10000]


def test_te_sink_lowers_nonzero_views_to_flat_transfer_addresses() -> None:
    source_inputs = manifests(tp=2, prefix="source", address_base=0x10000)
    source_fragment = replace(
        source_inputs.bindings[0].fragments[0],
        storage_address=0xF000,
        storage_nbytes=0x2000,
        storage_offset_bytes=0x1000,
    )
    sources = RuntimeInputs(
        source_inputs.placement,
        (
            replace(source_inputs.bindings[0], fragments=(source_fragment,)),
            *source_inputs.bindings[1:],
        ),
    )
    target_inputs = manifests(tp=4, prefix="target", address_base=0x40000)
    target_fragments = (
        replace(
            target_inputs.bindings[0].fragments[0],
            storage_address=0x3F000,
            storage_nbytes=0x2000,
            storage_offset_bytes=0x1000,
        ),
        replace(
            target_inputs.bindings[1].fragments[0],
            storage_address=0x40800,
            storage_nbytes=0x1000,
            storage_offset_bytes=0x800,
        ),
    )
    targets = RuntimeInputs(
        target_inputs.placement,
        (
            replace(target_inputs.bindings[0], fragments=(target_fragments[0],)),
            replace(target_inputs.bindings[1], fragments=(target_fragments[1],)),
            *target_inputs.bindings[2:],
        ),
    )
    engine = FakeTransferEngine()

    MooncakeTransferEngineSink(engine).execute(
        plan_transfer(sources, targets),
        sources.placement,
        sources.bindings[0],
        targets.placement,
        targets.bindings,
        target_registrations=registration_leases(targets),
        source_allocation_guards=allocation_guards(sources),
        target_allocation_guards=allocation_guards(targets),
    )

    assert engine.register_calls == [(0xF000, 0x2000)]
    assert engine.unregister_calls == [0xF000]
    assert engine.calls == [
        ("target-t0:12345", [0x10000], [0x40000], [2]),
        ("target-t1:12345", [0x10002], [0x41000], [2]),
    ]


def test_te_sink_requires_generation_bound_target_registration_leases() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    sink = MooncakeTransferEngineSink(FakeTransferEngine())

    with pytest.raises(TransferEngineError, match="target registration"):
        sink.execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            targets.bindings,
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(targets),
        )

    stale_leases = list(registration_leases(targets))
    stale_leases[0] = replace(stale_leases[0], lease_generation=2)
    with pytest.raises(TransferEngineError, match="target registration"):
        sink.execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            targets.bindings,
            target_registrations=tuple(stale_leases),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(targets),
        )


def test_te_sink_surfaces_endpoint_failure() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    engine = FakeTransferEngine()
    engine.fail_endpoint = "target-t1:12345"

    with pytest.raises(TransferEngineError, match="target-t1:12345"):
        MooncakeTransferEngineSink(engine).execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            targets.bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(targets),
        )


def test_te_sink_quarantines_write_exception_without_completion_ticket() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    engine = FakeTransferEngine()

    def fail_write(*args, **kwargs):
        raise RuntimeError("write exploded")

    engine.batch_transfer_sync_write = fail_write

    sink = MooncakeTransferEngineSink(engine)
    with pytest.raises(TransferCompletionUnknownError) as raised:
        sink.execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            targets.bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(targets),
        )

    assert engine.unregister_calls == []
    assert (
        sink.pending_transfer_status(raised.value.pending_transfer_id)
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )


def test_te_sink_rejects_stale_source_generation() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    stale_binding = replace(sources.bindings[0], generation=2)

    with pytest.raises(TransferEngineError, match="source executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            stale_binding,
            targets.placement,
            targets.bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards_for_bindings((stale_binding,)),
            target_allocation_guards=allocation_guards(targets),
        )


def test_te_sink_rejects_generation_scoped_source_id_rollover() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    replacement = replace(
        sources.bindings[0].fragments[0],
        fragment_id="replacement-source-fragment",
        worker_id="replacement-source-worker",
    )
    current_binding = replace(
        sources.bindings[0],
        instance_id="replacement-source-instance",
        generation=2,
        fragments=(replacement,),
    )

    with pytest.raises(TransferEngineError, match="source executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            current_binding,
            targets.placement,
            targets.bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards_for_bindings((current_binding,)),
            target_allocation_guards=allocation_guards(targets),
        )


def test_te_sink_rejects_stale_target_address_and_generation() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    replacement = replace(
        targets.bindings[0].fragments[0],
        address=0x90000,
        storage_address=0x90000,
    )
    current_bindings = (
        replace(targets.bindings[0], fragments=(replacement,), generation=2),
        *targets.bindings[1:],
    )

    with pytest.raises(TransferEngineError, match="target executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            current_bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards_for_bindings(current_bindings),
        )


def test_te_sink_rejects_generation_scoped_target_id_rollover() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    replacement = replace(
        targets.bindings[0].fragments[0],
        fragment_id="replacement-target-fragment",
    )
    current_bindings = (
        replace(targets.bindings[0], fragments=(replacement,), generation=2),
        *targets.bindings[1:],
    )

    with pytest.raises(TransferEngineError, match="target executor snapshot mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            current_bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards_for_bindings(current_bindings),
        )


@pytest.mark.parametrize("side", ["source", "target"])
def test_te_sink_rejects_revision_mismatch(side: str) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    plan = plan_transfer(sources, targets)
    if side == "source":
        sources = with_revision(sources, "step-43")
    else:
        targets = with_revision(targets, "step-43")

    with pytest.raises(TransferEngineError, match="revision mismatch"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            sources.bindings[0],
            targets.placement,
            targets.bindings,
            target_registrations=registration_leases(targets),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(targets),
        )


def test_te_receipt_identifies_worker_instead_of_serving_instance() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    source = RuntimeInputs(
        source.placement,
        (
            replace(
                source.bindings[0],
                instance_id="serving-instance",
            ),
        ),
    )
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_transfer(source, targets)

    receipts = MooncakeTransferEngineSink(FakeTransferEngine()).execute(
        plan,
        source.placement,
        source.bindings[0],
        targets.placement,
        targets.bindings,
        target_registrations=registration_leases(targets),
        source_allocation_guards=allocation_guards(source),
        target_allocation_guards=allocation_guards(targets),
    )

    assert receipts[0].source_worker_id == "source-t0"
