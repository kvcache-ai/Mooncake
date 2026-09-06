from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    TransferEngineError,
)

from .helpers import (
    allocation_guards,
    FakeTransferEngine,
    RuntimeInputs,
    manifests,
    participant_inputs,
    plan_transfer_to_local_target,
    registration_leases,
)


def test_te_reader_pulls_local_target_ranges_without_source_rpc() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    targets = manifests(tp=4, prefix="target", address_base=0x40000)
    target = participant_inputs(targets, 1)
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    receipts = MooncakeTransferEngineReader(engine).execute(
        plan,
        sources.placement,
        sources.bindings,
        target.placement,
        target.bindings[0],
        source_registrations=registration_leases(sources),
        target_pre_registered=True,
        target_registrations=registration_leases(target),
        source_allocation_guards=allocation_guards(sources),
        target_allocation_guards=allocation_guards(target),
    )

    assert receipts[0].source_endpoint == "source-t0:12345"
    assert receipts[0].target_worker_id == "target-t1"
    assert receipts[0].nbytes == 2
    assert engine.calls == [("source-t0:12345", [0x41000], [0x10002], [2])]
    assert engine.register_calls == []
    assert engine.unregister_calls == []


def test_te_reader_wraps_target_registration_exception() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    def fail_register(*args, **kwargs):
        raise RuntimeError("register exploded")

    engine.register_memory = fail_register

    with pytest.raises(TransferEngineError, match="register exploded"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(sources),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )


def test_te_reader_requires_generation_bound_source_registration_leases() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    reader = MooncakeTransferEngineReader(FakeTransferEngine())
    target_leases = registration_leases(target)

    with pytest.raises(TransferEngineError, match="source registration leases"):
        reader.execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            target_pre_registered=True,
            target_registrations=target_leases,
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )

    stale_generation = list(registration_leases(sources))
    stale_generation[0] = replace(stale_generation[0], lease_generation=2)
    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        reader.execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=stale_generation,
            target_pre_registered=True,
            target_registrations=target_leases,
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )

    stale_runtime_lease = list(registration_leases(sources))
    stale_runtime_lease[0] = replace(
        stale_runtime_lease[0],
        runtime_lease_id="stale-runtime-lease",
    )
    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        reader.execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=stale_runtime_lease,
            target_pre_registered=True,
            target_registrations=target_leases,
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"address": 0x90000, "storage_address": 0x90000},
        {"nbytes": 1},
        {"lease_generation": 2},
        {"runtime_lease_id": "stale-runtime-lease"},
    ],
)
def test_te_reader_rejects_source_registration_snapshot_mismatch(
    changes: dict[str, object],
) -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    leases = list(registration_leases(sources))
    leases[0] = replace(leases[0], **changes)

    with pytest.raises(TransferEngineError, match="source registration lease mismatch"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=leases,
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )


def test_te_reader_requires_registrations_only_for_used_source_fragments() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    used_fragment_ids = {operation.source.fragment_id for operation in plan.operations}
    source_registrations = tuple(
        registration
        for registration in registration_leases(sources)
        if registration.fragment_id in used_fragment_ids
    )

    receipts = MooncakeTransferEngineReader(FakeTransferEngine()).execute(
        plan,
        sources.placement,
        sources.bindings,
        target.placement,
        target.bindings[0],
        source_registrations=source_registrations,
        target_pre_registered=True,
        target_registrations=registration_leases(target),
        source_allocation_guards=allocation_guards(sources),
        target_allocation_guards=allocation_guards(target),
    )

    assert sum(receipt.nbytes for receipt in receipts) == 2


def test_te_reader_requires_complete_planned_source_executor_set() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=1, prefix="target", address_base=0x40000), 0
    )
    plan = plan_transfer_to_local_target(sources, target)
    partial_sources = RuntimeInputs(sources.placement, sources.bindings[:1])

    with pytest.raises(TransferEngineError, match="source executor set is incomplete"):
        MooncakeTransferEngineReader(FakeTransferEngine()).execute(
            plan,
            partial_sources.placement,
            partial_sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(partial_sources),
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=allocation_guards(partial_sources),
            target_allocation_guards=allocation_guards(target),
        )


def test_te_reader_surfaces_source_endpoint_failure() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    engine.fail_endpoint = "source-t0:12345"

    with pytest.raises(TransferEngineError, match="source-t0:12345"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(sources),
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )


def test_te_reader_rejects_positive_nonzero_transfer_status() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()
    engine.read_result = 5

    with pytest.raises(TransferEngineError, match="failed: 5"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(sources),
            target_pre_registered=True,
            target_registrations=registration_leases(target),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )


def test_te_reader_rejects_leases_without_pre_registered_mode() -> None:
    sources = manifests(tp=2, prefix="source", address_base=0x10000)
    target = participant_inputs(
        manifests(tp=4, prefix="target", address_base=0x40000), 1
    )
    plan = plan_transfer_to_local_target(sources, target)
    engine = FakeTransferEngine()

    with pytest.raises(TransferEngineError, match="target_pre_registered"):
        MooncakeTransferEngineReader(engine).execute(
            plan,
            sources.placement,
            sources.bindings,
            target.placement,
            target.bindings[0],
            source_registrations=registration_leases(sources),
            target_registrations=registration_leases(target),
            source_allocation_guards=allocation_guards(sources),
            target_allocation_guards=allocation_guards(target),
        )

    assert engine.register_calls == []
    assert engine.unregister_calls == []
