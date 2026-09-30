from __future__ import annotations

import threading

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


@pytest.mark.parametrize("healthy_executor", ("sink", "reader"))
def test_te_pending_transfer_backpressure_is_engine_scoped(
    healthy_executor: str,
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    blocked_engine = FakeTransferEngine()
    blocked_engine.read_result = -2

    with pytest.raises(TransferCompletionUnknownError):
        execute_reader(
            MooncakeTransferEngineReader(blocked_engine),
            plan_transfer_to_local_target(sources, targets),
            sources,
            targets,
            source_registrations=registration_leases(sources),
        )

    healthy_engine = FakeTransferEngine()
    if healthy_executor == "sink":
        receipts = execute_sink(
            MooncakeTransferEngineSink(healthy_engine),
            plan_transfer(sources, targets),
            sources,
            targets,
            target_registrations=registration_leases(targets),
        )
    else:
        receipts = execute_reader(
            MooncakeTransferEngineReader(healthy_engine),
            plan_transfer_to_local_target(sources, targets),
            sources,
            targets,
            source_registrations=registration_leases(sources),
        )

    assert len(receipts) == 1
    assert len(healthy_engine.calls) == 1


@pytest.mark.parametrize("recovery_executor", ("sink", "reader"))
def test_te_pending_transfer_allows_new_transfer_after_drain_cleanup(
    recovery_executor: str,
) -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    pending_ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 2)
    engine.batch_transfer_sync_write_with_ticket = lambda *args: pending_ticket
    sink = MooncakeTransferEngineSink(engine, max_completion_drain_attempts=1)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        execute_sink(
            sink,
            plan_transfer(sources, targets),
            sources,
            targets,
            target_registrations=registration_leases(targets),
        )

    completed_ticket = FakeBatchTransferTicket(["COMPLETED"])
    engine.batch_transfer_sync_write_with_ticket = lambda *args: completed_ticket
    engine.batch_transfer_sync_read_with_ticket = lambda *args: completed_ticket

    def recover() -> None:
        if recovery_executor == "sink":
            execute_sink(
                MooncakeTransferEngineSink(engine),
                plan_transfer(sources, targets),
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )
        else:
            execute_reader(
                MooncakeTransferEngineReader(engine),
                plan_transfer_to_local_target(sources, targets),
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

    with pytest.raises(TransferEngineError, match="drain_pending_transfer"):
        recover()

    pending_ticket._statuses = ["COMPLETED"]
    assert sink.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    assert sink.pending_transfer_ids() == ()
    recover()


@pytest.mark.parametrize(
    ("first_executor_name", "second_executor_name", "use_wrapper_aliases"),
    [
        pytest.param("sink", "sink", False, id="sink-sink"),
        pytest.param("reader", "reader", False, id="reader-reader"),
        pytest.param("sink", "reader", False, id="sink-reader"),
        pytest.param("sink", "sink", True, id="wrapper-alias"),
    ],
)
def test_te_submission_reservation_is_atomic(
    first_executor_name: str,
    second_executor_name: str,
    use_wrapper_aliases: bool,
) -> None:
    class EngineAlias:
        def __init__(self, wrapped_engine) -> None:
            self._wrapped_engine = wrapped_engine

        def __getattr__(self, name: str):
            return getattr(self._wrapped_engine, name)

    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    sink_plan = plan_transfer(sources, targets)
    reader_plan = plan_transfer_to_local_target(sources, targets)
    engine = FakeTransferEngine()
    first_engine = engine
    second_engine = engine
    if use_wrapper_aliases:
        engine.get_engine_ptr = lambda: 0xCA110CA1
        first_engine = EngineAlias(engine)
        second_engine = EngineAlias(engine)

    first_entered = threading.Event()
    allow_first_to_finish = threading.Event()
    submission_lock = threading.Lock()
    submission_count = 0
    pending_ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 2)
    completed_ticket = FakeBatchTransferTicket(["COMPLETED"])

    def transfer_with_ticket(*args):
        nonlocal submission_count
        with submission_lock:
            submission_count += 1
            call_number = submission_count
        if call_number == 1:
            first_entered.set()
            assert allow_first_to_finish.wait(timeout=2)
            return pending_ticket
        return completed_ticket

    engine.batch_transfer_sync_write_with_ticket = transfer_with_ticket
    engine.batch_transfer_sync_read_with_ticket = transfer_with_ticket

    def make_executor(executor_name: str, transfer_engine):
        executor_type = (
            MooncakeTransferEngineSink
            if executor_name == "sink"
            else MooncakeTransferEngineReader
        )
        return executor_type(
            transfer_engine,
            max_completion_drain_attempts=1,
        )

    def execute(executor_name: str, executor) -> None:
        if executor_name == "sink":
            execute_sink(
                executor,
                sink_plan,
                sources,
                targets,
                target_registrations=registration_leases(targets),
            )
        else:
            execute_reader(
                executor,
                reader_plan,
                sources,
                targets,
                source_registrations=registration_leases(sources),
            )

    first_executor = make_executor(first_executor_name, first_engine)
    second_executor = make_executor(second_executor_name, second_engine)
    first_errors = []
    second_errors = []

    def capture_error(executor_name: str, executor, errors: list) -> None:
        try:
            execute(executor_name, executor)
        except Exception as error:
            errors.append(error)

    first = threading.Thread(
        target=capture_error,
        args=(first_executor_name, first_executor, first_errors),
    )
    second = threading.Thread(
        target=capture_error,
        args=(second_executor_name, second_executor, second_errors),
    )
    first.start()
    try:
        assert first_entered.wait(timeout=2)
        second.start()
        second.join(timeout=2)
        assert not second.is_alive()
        register_calls_while_active = tuple(engine.register_calls)
        unregister_calls_while_active = tuple(engine.unregister_calls)
    finally:
        allow_first_to_finish.set()
        first.join(timeout=2)
        if second.ident is not None:
            second.join(timeout=2)

    assert not first.is_alive()
    assert len(first_errors) == 1
    assert isinstance(first_errors[0], TransferCompletionUnknownError)
    pending_id = first_errors[0].pending_transfer_id
    visible_pending_ids = second_executor.pending_transfer_ids()
    pending_ticket._statuses = ["COMPLETED"]
    assert second_executor.drain_pending_transfer(pending_id) == "COMPLETED"

    first_fragment = (
        sources.bindings[0].fragments[0]
        if first_executor_name == "sink"
        else targets.bindings[0].fragments[0]
    )
    assert visible_pending_ids == (pending_id,)
    assert submission_count == 1
    assert register_calls_while_active == (
        (first_fragment.address, first_fragment.nbytes),
    )
    assert unregister_calls_while_active == ()
    assert engine.unregister_calls == [first_fragment.address]
    assert len(second_errors) == 1
    assert isinstance(second_errors[0], TransferEngineError)
    assert "active resource transfer submission" in str(second_errors[0])


def test_te_rejects_concurrent_drain_for_same_pending_transfer() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 2)
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    sink = MooncakeTransferEngineSink(
        engine,
        max_completion_drain_attempts=1,
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
    first_entered = threading.Event()
    allow_first_to_finish = threading.Event()

    def block_first_drain() -> None:
        first_entered.set()
        assert allow_first_to_finish.wait(timeout=2)

    ticket._statuses = ["COMPLETED"]
    ticket._on_drain = block_first_drain
    statuses = []
    errors = []

    def drain() -> None:
        try:
            statuses.append(sink.drain_pending_transfer(pending_id))
        except Exception as error:
            errors.append(error)

    first = threading.Thread(target=drain)
    second = threading.Thread(target=drain)
    first.start()
    assert first_entered.wait(timeout=2)
    second.start()
    second.join(timeout=0.2)
    allow_first_to_finish.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert statuses == ["COMPLETED"]
    assert len(errors) == 1
    assert isinstance(errors[0], TransferEngineError)
    assert "already being drained" in str(errors[0])
    assert engine.unregister_calls == [0x10000]
