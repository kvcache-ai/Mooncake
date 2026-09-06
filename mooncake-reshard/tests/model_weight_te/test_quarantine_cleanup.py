from __future__ import annotations

import gc
import threading
import weakref
from dataclasses import replace

import pytest

from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    TransferCompletionUnknownError,
    TransferEngineError,
)

from .helpers import (
    FakeBatchTransferTicket,
    FakeTransferEngine,
    RuntimeInputs,
    execute_reader,
    execute_sink,
    manifests,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
)


@pytest.mark.parametrize("executor_name", ("sink", "reader"))
def test_te_pending_transfer_cannot_drain_before_resource_handoff(
    executor_name: str,
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 2)
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(
            engine,
            max_completion_drain_attempts=1,
        )

        def execute() -> None:
            execute_sink(
                executor,
                plan_transfer(sources, targets),
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )

        pending_owner = executor
        owned_address = sources.bindings[0].fragments[0].address
    else:
        executor = MooncakeTransferEngineReader(
            engine,
            max_completion_drain_attempts=1,
        )

        def execute() -> None:
            execute_reader(
                executor,
                plan_transfer_to_local_target(sources, targets),
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

        pending_owner = executor._pending
        owned_address = targets.bindings[0].fragments[0].address

    original_retain = pending_owner._retain_pending_resources
    handoff_entered = threading.Event()
    allow_handoff = threading.Event()
    pending_ids = []

    def block_resource_handoff(
        pending_transfer_id: str,
        *,
        registrations,
        resources,
        allocation_tokens=(),
    ) -> None:
        pending_ids.append(pending_transfer_id)
        handoff_entered.set()
        assert allow_handoff.wait(timeout=2)
        original_retain(
            pending_transfer_id,
            registrations=registrations,
            resources=resources,
            allocation_tokens=allocation_tokens,
        )

    pending_owner._retain_pending_resources = block_resource_handoff
    execution_errors = []

    def run_execute() -> None:
        try:
            execute()
        except Exception as error:
            execution_errors.append(error)

    worker = threading.Thread(target=run_execute)
    worker.start()
    assert handoff_entered.wait(timeout=2)
    pending_id = pending_ids[0]
    ticket._statuses = ["COMPLETED"]
    drain_calls_before_handoff = tuple(ticket.drain_calls)
    try:
        with pytest.raises(TransferEngineError, match="resource handoff"):
            executor.drain_pending_transfer(pending_id)
        assert tuple(ticket.drain_calls) == drain_calls_before_handoff
        assert engine.unregister_calls == []
    finally:
        allow_handoff.set()
        worker.join(timeout=2)

    assert not worker.is_alive()
    assert len(execution_errors) == 1
    assert isinstance(execution_errors[0], TransferCompletionUnknownError)
    assert executor.pending_transfer_status(pending_id) == "COMPLETED"
    assert executor.drain_pending_transfer(pending_id) == "COMPLETED"
    assert engine.unregister_calls == [owned_address]


def test_te_pending_transfer_retains_fragment_owner_until_terminal() -> None:
    class Owner:
        pass

    def create_pending():
        sources = manifests(tp=1, prefix="source", address_base=0x10000)
        targets = manifests(tp=1, prefix="target", address_base=0x40000)
        owner = Owner()
        owned_fragment = replace(targets.bindings[0].fragments[0], owner=owner)
        owned_target = RuntimeInputs(
            targets.placement,
            (replace(targets.bindings[0], fragments=(owned_fragment,)),),
        )
        engine = FakeTransferEngine()
        ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 4)
        engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket
        reader = MooncakeTransferEngineReader(
            engine,
            max_completion_drain_attempts=1,
        )
        try:
            execute_reader(
                reader,
                plan_transfer_to_local_target(sources, owned_target),
                sources,
                owned_target,
                source_registrations=registration_leases(sources),
            )
        except TransferCompletionUnknownError as error:
            pending_id = error.pending_transfer_id
        else:
            raise AssertionError("transfer must remain pending")
        return reader, ticket, pending_id, weakref.ref(owner)

    reader, ticket, pending_id, owner_ref = create_pending()
    gc.collect()
    assert owner_ref() is not None

    ticket._statuses = ["COMPLETED"]
    assert reader.drain_pending_transfer(pending_id) == "COMPLETED"
    gc.collect()
    assert owner_ref() is None


def test_te_pending_transfer_survives_executor_destruction() -> None:
    class Owner:
        pass

    def create_pending():
        sources = manifests(tp=1, prefix="source", address_base=0x10000)
        targets = manifests(tp=1, prefix="target", address_base=0x40000)
        owner = Owner()
        owned_fragment = replace(targets.bindings[0].fragments[0], owner=owner)
        owned_target = RuntimeInputs(
            targets.placement,
            (replace(targets.bindings[0], fragments=(owned_fragment,)),),
        )
        engine = FakeTransferEngine()
        ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 4)
        engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket
        reader = MooncakeTransferEngineReader(
            engine,
            max_completion_drain_attempts=1,
        )
        with pytest.raises(TransferCompletionUnknownError) as raised:
            execute_reader(
                reader,
                plan_transfer_to_local_target(sources, owned_target),
                sources,
                owned_target,
                source_registrations=registration_leases(sources),
            )
        return engine, ticket, raised.value.pending_transfer_id, weakref.ref(owner)

    engine, ticket, pending_id, owner_ref = create_pending()
    gc.collect()
    assert owner_ref() is not None

    recovery_reader = MooncakeTransferEngineReader(engine)
    assert recovery_reader.pending_transfer_status(pending_id) == "COMPLETION_UNKNOWN"
    ticket._statuses = ["COMPLETED"]
    assert recovery_reader.drain_pending_transfer(pending_id) == "COMPLETED"
    assert engine.unregister_calls == [0x40000]
    gc.collect()
    assert owner_ref() is None


@pytest.mark.parametrize("executor_name", ("sink", "reader"))
def test_te_pre_registered_unknown_retains_external_owner_until_terminal(
    executor_name: str,
) -> None:
    class ExternalOwner:
        pass

    def create_pending():
        sources = manifests(tp=1, prefix="source", address_base=0x10000)
        targets = manifests(tp=1, prefix="target", address_base=0x40000)
        engine = FakeTransferEngine()
        ticket = FakeBatchTransferTicket(
            ["COMPLETION_UNKNOWN", "COMPLETION_UNKNOWN"],
            on_drain=lambda: (
                pytest.fail("external registration released before transfer drained")
                if engine.unregister_calls
                else None
            ),
        )
        owner = ExternalOwner()
        if executor_name == "sink":
            owned_fragment = replace(sources.bindings[0].fragments[0], owner=owner)
            sources = RuntimeInputs(
                sources.placement,
                (replace(sources.bindings[0], fragments=(owned_fragment,)),),
            )
            executor = MooncakeTransferEngineSink(
                engine,
                max_completion_drain_attempts=1,
            )
            engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
        else:
            owned_fragment = replace(targets.bindings[0].fragments[0], owner=owner)
            targets = RuntimeInputs(
                targets.placement,
                (replace(targets.bindings[0], fragments=(owned_fragment,)),),
            )
            executor = MooncakeTransferEngineReader(
                engine,
                max_completion_drain_attempts=1,
            )
            engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket

        external_address = owned_fragment.address
        assert engine.register_memory(external_address, owned_fragment.nbytes) == 0
        with pytest.raises(TransferCompletionUnknownError) as raised:
            if executor_name == "sink":
                execute_sink(
                    executor,
                    plan_transfer(sources, targets),
                    sources,
                    targets,
                    target_registrations=registration_leases(targets),
                    source_pre_registered=True,
                    source_registrations=registration_leases(sources),
                )
            else:
                execute_reader(
                    executor,
                    plan_transfer_to_local_target(sources, targets),
                    sources,
                    targets,
                    source_registrations=registration_leases(sources),
                    target_pre_registered=True,
                    target_registrations=registration_leases(targets),
                )
        return (
            engine,
            ticket,
            raised.value.pending_transfer_id,
            type(executor),
            external_address,
            weakref.ref(owner),
        )

    engine, ticket, pending_id, executor_type, external_address, owner_ref = (
        create_pending()
    )
    gc.collect()
    assert engine.unregister_calls == []
    assert owner_ref() is not None

    recovery = executor_type(engine)
    assert recovery.pending_transfer_status(pending_id) == "COMPLETION_UNKNOWN"
    ticket._statuses = ["COMPLETED"]
    assert recovery.drain_pending_transfer(pending_id) == "COMPLETED"
    assert engine.unregister_calls == []
    gc.collect()
    assert owner_ref() is None

    assert engine.unregister_memory(external_address) == 0
    assert engine.unregister_calls == [external_address]
