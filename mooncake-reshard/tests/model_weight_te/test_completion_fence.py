from __future__ import annotations

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
    execute_reader,
    execute_sink,
    manifests,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
)


@pytest.mark.parametrize(
    ("executor", "terminal_status", "expect_error"),
    [
        ("sink", "COMPLETED", False),
        ("sink", "FAILED_DRAINED", True),
        ("reader", "COMPLETED", False),
        ("reader", "FAILED_DRAINED", True),
    ],
)
def test_te_ticket_path_keeps_registration_until_completion_is_known(
    executor: str,
    terminal_status: str,
    expect_error: bool,
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(
        ["COMPLETION_UNKNOWN", "COMPLETION_UNKNOWN", terminal_status],
        on_drain=lambda: (
            pytest.fail("registration released before transfer drained")
            if engine.unregister_calls
            else None
        ),
    )

    def write_with_ticket(endpoint, source_addresses, target_addresses, sizes):
        engine.calls.append((endpoint, source_addresses, target_addresses, sizes))
        return ticket

    def read_with_ticket(endpoint, target_addresses, source_addresses, sizes):
        engine.calls.append((endpoint, target_addresses, source_addresses, sizes))
        return ticket

    engine.batch_transfer_sync_write_with_ticket = write_with_ticket
    engine.batch_transfer_sync_read_with_ticket = read_with_ticket

    if executor == "sink":
        plan = plan_transfer(sources, targets)

        def execute():
            return execute_sink(
                MooncakeTransferEngineSink(engine),
                plan,
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )

        expected_unregister = [0x10000]
    else:
        plan = plan_transfer_to_local_target(sources, targets)

        def execute():
            return execute_reader(
                MooncakeTransferEngineReader(engine),
                plan,
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

        expected_unregister = [0x40000]

    if expect_error:
        with pytest.raises(TransferEngineError, match="FAILED_DRAINED"):
            execute()
    else:
        execute()

    assert ticket.drain_calls == [1000, 1000]
    assert engine.unregister_calls == expected_unregister


@pytest.mark.parametrize("executor_name", ["sink", "reader"])
def test_te_unknown_completion_moves_registration_to_queryable_quarantine(
    executor_name: str,
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 4)

    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(
            engine,
            max_completion_drain_attempts=2,
        )

        def execute():
            return execute_sink(
                executor,
                plan_transfer(sources, targets),
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )

        expected_unregister = [0x10000]
    else:
        executor = MooncakeTransferEngineReader(
            engine,
            max_completion_drain_attempts=2,
        )

        def execute():
            return execute_reader(
                executor,
                plan_transfer_to_local_target(sources, targets),
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

        expected_unregister = [0x40000]

    with pytest.raises(TransferCompletionUnknownError) as raised:
        execute()

    pending_id = raised.value.pending_transfer_id
    assert ticket.drain_calls == [1000, 1000]
    assert engine.unregister_calls == []
    assert executor.pending_transfer_ids() == (pending_id,)
    assert executor.pending_transfer_status(pending_id) == "COMPLETION_UNKNOWN"

    ticket._statuses = ["COMPLETED"]
    assert executor.drain_pending_transfer(pending_id) == "COMPLETED"
    assert engine.unregister_calls == expected_unregister
    assert executor.pending_transfer_ids() == ()


@pytest.mark.parametrize("executor_name", ["sink", "reader"])
@pytest.mark.parametrize("interruption_type", [KeyboardInterrupt, SystemExit])
def test_te_interrupted_unknown_completion_quarantines_before_reraising(
    executor_name: str,
    interruption_type: type[BaseException],
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()

    def interrupt_drain() -> None:
        raise interruption_type("stop transfer wait")

    ticket = FakeBatchTransferTicket(
        ["COMPLETION_UNKNOWN"],
        on_drain=interrupt_drain,
    )
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    engine.batch_transfer_sync_read_with_ticket = lambda *args: ticket

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(
            engine,
            max_completion_drain_attempts=1,
        )

        def execute():
            execute_sink(
                executor,
                plan_transfer(sources, targets),
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )

        expected_unregister = [0x10000]
    else:
        executor = MooncakeTransferEngineReader(
            engine,
            max_completion_drain_attempts=1,
        )

        def execute():
            execute_reader(
                executor,
                plan_transfer_to_local_target(sources, targets),
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

        expected_unregister = [0x40000]

    with pytest.raises(interruption_type, match="stop transfer wait"):
        execute()

    pending_ids = executor.pending_transfer_ids()
    assert len(pending_ids) == 1
    pending_id = pending_ids[0]
    assert engine.unregister_calls == []
    assert executor.pending_transfer_status(pending_id) == "COMPLETION_UNKNOWN"

    ticket._on_drain = None
    ticket._statuses = ["COMPLETED"]
    assert executor.drain_pending_transfer(pending_id) == "COMPLETED"
    assert engine.unregister_calls == expected_unregister
    assert executor.pending_transfer_ids() == ()


@pytest.mark.parametrize("executor_name", ["sink", "reader"])
@pytest.mark.parametrize("interruption_type", [KeyboardInterrupt, SystemExit])
def test_te_interrupted_native_submission_requires_restart_before_unregister(
    executor_name: str,
    interruption_type: type[BaseException],
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()

    def interrupt_submission(*args):
        raise interruption_type("stop native submission")

    engine.batch_transfer_sync_write_with_ticket = interrupt_submission
    engine.batch_transfer_sync_read_with_ticket = interrupt_submission

    if executor_name == "sink":
        executor = MooncakeTransferEngineSink(engine)

        def execute():
            execute_sink(
                executor,
                plan_transfer(sources, targets),
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )

    else:
        executor = MooncakeTransferEngineReader(engine)

        def execute():
            execute_reader(
                executor,
                plan_transfer_to_local_target(sources, targets),
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

    with pytest.raises(interruption_type, match="stop native submission"):
        execute()

    pending_ids = executor.pending_transfer_ids()
    assert len(pending_ids) == 1
    assert engine.unregister_calls == []
    assert (
        executor.pending_transfer_status(pending_ids[0])
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    with pytest.raises(TransferEngineError, match="restart-required"):
        execute()


def test_te_continuous_drain_errors_remain_quarantined_until_terminal() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()

    def fail_drain() -> None:
        raise RuntimeError("backend unavailable")

    ticket = FakeBatchTransferTicket(
        ["COMPLETION_UNKNOWN"],
        on_drain=fail_drain,
    )
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    sink = MooncakeTransferEngineSink(
        engine,
        max_completion_drain_attempts=2,
    )

    with pytest.raises(TransferCompletionUnknownError) as raised:
        execute_sink(
            sink,
            plan_transfer(sources, targets),
            sources,
            targets,
            target_registrations=registration_leases(targets),
        )

    pending_id = raised.value.pending_transfer_id
    assert ticket.drain_calls == [1000, 1000]
    assert engine.unregister_calls == []
    assert sink.drain_pending_transfer(pending_id) == "COMPLETION_UNKNOWN"
    assert engine.unregister_calls == []

    ticket._on_drain = None
    ticket._statuses = ["FAILED_DRAINED"]
    assert sink.drain_pending_transfer(pending_id) == "FAILED_DRAINED"
    assert engine.unregister_calls == [0x10000]
