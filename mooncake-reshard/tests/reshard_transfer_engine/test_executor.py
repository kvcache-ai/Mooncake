from __future__ import annotations

import gc
from dataclasses import replace

import pytest
from mooncake.reshard.transfer_engine import (
    AllocationFence,
    MooncakeTransferEngineExecutor,
    TerminalTransferState,
    TransferBatch,
    TransferBatchRange,
    TransferCompletionFailedError,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferDirection,
    TransferEngineError,
    TransferSubmission,
)


class CompletedTicket:
    status = "COMPLETED"


class TicketEngine:
    def __init__(self) -> None:
        self.calls = []

    def get_engine_ptr(self) -> int:
        return id(self)

    def batch_transfer_sync_read_with_ticket(self, *arguments):
        self.calls.append(("read", arguments))
        return CompletedTicket()

    def batch_transfer_sync_write_with_ticket(self, *arguments):
        self.calls.append(("write", arguments))
        return CompletedTicket()


class ScatterTicketEngine(TicketEngine):
    def scatter_transfer_sync_read_with_ticket(self, *arguments):
        self.calls.append(("scatter_read", arguments))
        return CompletedTicket()

    def scatter_transfer_sync_write_with_ticket(self, *arguments):
        self.calls.append(("scatter_write", arguments))
        return CompletedTicket()


class AsyncScatterTicket:
    def __init__(self, engine, name: str, result: str = "COMPLETED") -> None:
        self.engine = engine
        self.name = name
        self.result = result
        self.status = "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms: int) -> str:
        self.engine.events.append(("drain", self.name))
        if self.status == "COMPLETION_UNKNOWN" and self.result != "COMPLETION_UNKNOWN":
            self.status = self.result
            self.engine.inflight -= 1
        return self.status


class AsyncScatterEngine(ScatterTicketEngine):
    def __init__(self, results=()) -> None:
        super().__init__()
        self.results = list(results)
        self.events = []
        self.inflight = 0
        self.peak_inflight = 0
        self.tickets = []

    def submit_scatter_write(self, *arguments):
        name = arguments[0]
        result = self.results.pop(0) if self.results else "COMPLETED"
        ticket = AsyncScatterTicket(self, name, result)
        self.tickets.append(ticket)
        self.events.append(("submit", name))
        self.inflight += 1
        self.peak_inflight = max(self.peak_inflight, self.inflight)
        return ticket

    submit_scatter_read = submit_scatter_write


class LegacyEngine:
    """Match the flat synchronous batch API exposed by the current binding."""

    def __init__(self) -> None:
        self.calls = []

    def get_engine_ptr(self) -> int:
        return id(self)

    def batch_transfer_sync_read(self, *arguments) -> int:
        self.calls.append(("read", arguments))
        return 0

    def batch_transfer_sync_write(self, *arguments) -> int:
        self.calls.append(("write", arguments))
        return 0


def batch() -> TransferBatch:
    return TransferBatch(
        endpoint="worker-1:12345",
        source_addresses=(0x1000, 0x2000),
        target_addresses=(0x3000, 0x4000),
        sizes=(64, 128),
    )


def range_batch() -> TransferBatch:
    return TransferBatch.from_ranges(
        endpoint="worker-1:12345",
        ranges=(
            TransferBatchRange(
                source_base_address=0x1000,
                source_capacity=0x400,
                target_base_address=0x3000,
                target_capacity=0x800,
                source_offsets=(0x20, 0x100),
                target_offsets=(0x40, 0x200),
                sizes=(64, 128),
            ),
            TransferBatchRange(
                source_base_address=0x5000,
                source_capacity=0x100,
                target_base_address=0x7000,
                target_capacity=0x100,
                source_offsets=(0,),
                target_offsets=(0x20,),
                sizes=(32,),
            ),
        ),
    )


def range_batch_for(endpoint: str) -> TransferBatch:
    return replace(range_batch(), endpoint=endpoint)


def test_execute_batches_max_inflight_one_keeps_synchronous_path() -> None:
    engine = AsyncScatterEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    batches = (range_batch_for("worker-0"), range_batch_for("worker-1"))

    with executor.submission() as submission:
        receipts = submission.execute_batches(
            batches,
            TransferDirection.WRITE,
            max_inflight_batches=1,
        )

    assert [receipt.endpoint for receipt in receipts] == ["worker-0", "worker-1"]
    assert engine.events == []
    assert [call[0] for call in engine.calls] == ["scatter_write", "scatter_write"]


def test_execute_batches_bounds_window_and_preserves_receipt_order() -> None:
    engine = AsyncScatterEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    batches = tuple(range_batch_for(f"worker-{index}") for index in range(4))

    with executor.submission() as submission:
        receipts = submission.execute_batches(
            batches,
            TransferDirection.WRITE,
            max_inflight_batches=2,
        )

    assert engine.peak_inflight == 2
    assert engine.events == [
        ("submit", "worker-0"),
        ("submit", "worker-1"),
        ("drain", "worker-0"),
        ("submit", "worker-2"),
        ("drain", "worker-1"),
        ("submit", "worker-3"),
        ("drain", "worker-2"),
        ("drain", "worker-3"),
    ]
    assert [receipt.endpoint for receipt in receipts] == [
        "worker-0",
        "worker-1",
        "worker-2",
        "worker-3",
    ]


def test_execute_batches_streams_input_within_the_inflight_window() -> None:
    engine = AsyncScatterEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    def batches():
        yield range_batch_for("worker-0")
        assert engine.events == [("submit", "worker-0")]
        yield range_batch_for("worker-1")
        assert ("drain", "worker-0") in engine.events
        yield range_batch_for("worker-2")

    with executor.submission() as submission:
        receipts = submission.execute_batches(
            batches(),
            TransferDirection.WRITE,
            max_inflight_batches=2,
        )

    assert [receipt.endpoint for receipt in receipts] == [
        "worker-0",
        "worker-1",
        "worker-2",
    ]
    assert engine.peak_inflight == 2


def test_execute_batches_drains_published_work_before_prepare_error() -> None:
    engine = AsyncScatterEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferEngineError, match="Scatter range batches"):
            submission.execute_batches(
                (range_batch_for("worker-0"), batch()),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert engine.events == [
        ("submit", "worker-0"),
        ("drain", "worker-0"),
    ]
    assert engine.inflight == 0


def test_execute_batches_preserves_stream_failure_in_pending_terminal_state() -> None:
    engine = AsyncScatterEngine(("COMPLETION_UNKNOWN",))
    executor = MooncakeTransferEngineExecutor(engine)

    def batches():
        yield range_batch_for("worker-0")
        raise RuntimeError("batch stream failed")

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                batches(),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    engine.tickets[0].result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "FAILED_DRAINED"
    )


def test_execute_batches_preserves_stream_interruption_with_pending_ticket() -> None:
    engine = AsyncScatterEngine(("COMPLETION_UNKNOWN",))
    executor = MooncakeTransferEngineExecutor(engine)

    def batches():
        yield range_batch_for("worker-0")
        raise KeyboardInterrupt("batch stream interrupted")

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionInterrupted) as raised:
            submission.execute_batches(
                batches(),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert isinstance(raised.value.interruption, KeyboardInterrupt)
    engine.tickets[0].result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "FAILED_DRAINED"
    )


def test_execute_batches_drains_all_tickets_after_receipt_interruption() -> None:
    engine = AsyncScatterEngine(("COMPLETED", "COMPLETION_UNKNOWN"))
    executor = MooncakeTransferEngineExecutor(engine)
    original_receipt = executor._receipt
    interrupted = False

    def interrupt_first_receipt(prepared):
        nonlocal interrupted
        receipt = original_receipt(prepared)
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt("receipt interrupted")
        return receipt

    executor._receipt = interrupt_first_receipt

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionInterrupted) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"), range_batch_for("worker-1")),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert engine.events[-1] == ("drain", "worker-1")
    engine.tickets[1].result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "FAILED_DRAINED"
    )


def test_execute_batches_known_failure_stops_submission_and_drains_published() -> None:
    engine = AsyncScatterEngine(("COMPLETED", "FAILED_DRAINED", "COMPLETED"))
    executor = MooncakeTransferEngineExecutor(engine)
    batches = tuple(range_batch_for(f"worker-{index}") for index in range(4))

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionFailedError, match="worker-1"):
            submission.execute_batches(
                batches,
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert [event for event in engine.events if event[0] == "submit"] == [
        ("submit", "worker-0"),
        ("submit", "worker-1"),
        ("submit", "worker-2"),
    ]
    assert ("drain", "worker-2") in engine.events
    assert engine.inflight == 0


def test_execute_batches_retains_unknown_tickets_as_one_pending_transfer() -> None:
    engine = AsyncScatterEngine(("COMPLETION_UNKNOWN", "COMPLETION_UNKNOWN"))
    executor = MooncakeTransferEngineExecutor(engine)
    batches = (range_batch_for("worker-0"), range_batch_for("worker-1"))

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                batches,
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert executor.pending_transfer_ids() == (raised.value.pending_transfer_id,)
    for ticket in engine.tickets:
        ticket.result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "COMPLETED"
    )


def test_execute_batches_submit_without_ticket_requires_restart() -> None:
    failure = RuntimeError("submit failed after crossing native boundary")

    class FailingSubmitEngine(AsyncScatterEngine):
        def submit_scatter_write(self, *arguments):
            if arguments[0] == "worker-1":
                raise failure
            return super().submit_scatter_write(*arguments)

    engine = FailingSubmitEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"), range_batch_for("worker-1")),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert executor.pending_transfer_status(raised.value.pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert raised.value.__cause__ is failure


def test_execute_batches_preserves_ticket_probe_failure() -> None:
    failure = RuntimeError("ticket status lookup failed")

    class BrokenTicket:
        @property
        def status(self):
            raise failure

        def drain(self, timeout_ms):
            return "COMPLETION_UNKNOWN"

    class BrokenEngine(AsyncScatterEngine):
        def submit_scatter_write(self, *arguments):
            return BrokenTicket()

    executor = MooncakeTransferEngineExecutor(BrokenEngine())
    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                (range_batch(),), TransferDirection.WRITE, max_inflight_batches=2
            )
    assert raised.value.__cause__ is failure


def test_execute_batches_rejects_missing_receipt(monkeypatch) -> None:
    engine = AsyncScatterEngine()
    executor = MooncakeTransferEngineExecutor(engine)
    monkeypatch.setattr(executor, "_receipt", lambda prepared: None)
    with executor.submission() as submission:
        with pytest.raises(TransferEngineError, match="missing its receipt"):
            submission.execute_batches(
                (range_batch(),), TransferDirection.WRITE, max_inflight_batches=2
            )
    assert engine.inflight == 0


def test_execute_batches_borrows_iterator_until_caller_closes_after_drain() -> None:
    engine = AsyncScatterEngine(results=("COMPLETION_UNKNOWN",))
    executor = MooncakeTransferEngineExecutor(engine, max_completion_drain_attempts=1)
    cleanup = []

    def batches():
        try:
            yield range_batch()
            yield batch()
        finally:
            cleanup.append("closed")

    stream = batches()
    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                stream, TransferDirection.WRITE, max_inflight_batches=2
            )
    assert cleanup == []
    engine.tickets[0].result = "COMPLETED"
    assert (
        executor.drain_pending_transfer(raised.value.pending_transfer_id)
        == "FAILED_DRAINED"
    )
    stream.close()
    assert cleanup == ["closed"]


def test_execute_batches_invalid_ticket_requires_restart() -> None:
    class InvalidTicketEngine(AsyncScatterEngine):
        def submit_scatter_write(self, *arguments):
            self.events.append(("submit", arguments[0]))
            pass

    engine = InvalidTicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"),),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert executor.pending_transfer_status(raised.value.pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )


def test_execute_batches_ticket_without_status_requires_restart() -> None:
    finalized = []

    class TicketWithoutStatus:
        def drain(self, timeout_ms: int) -> str:
            return "COMPLETED"

        def __del__(self) -> None:
            finalized.append("native-operation-owner-released")

    class InvalidTicketEngine(AsyncScatterEngine):
        def submit_scatter_write(self, *arguments):
            self.events.append(("submit", arguments[0]))
            return TicketWithoutStatus()

    engine = InvalidTicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"),),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    pending_transfer_id = raised.value.pending_transfer_id
    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    del raised
    gc.collect()
    assert finalized == []


def test_execute_batches_requires_async_scatter_before_submitting() -> None:
    engine = ScatterTicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferEngineError, match="submit_scatter_write"):
            submission.execute_batches(
                (range_batch_for("worker-0"), range_batch_for("worker-1")),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert engine.calls == []


def test_execute_batches_interruption_retains_all_unresolved_tickets() -> None:
    class InterruptingAsyncTicket(AsyncScatterTicket):
        def drain(self, timeout_ms: int) -> str:
            if self.result == "INTERRUPT":
                self.result = "COMPLETION_UNKNOWN"
                raise KeyboardInterrupt("window drain interrupted")
            return super().drain(timeout_ms)

    class InterruptingAsyncEngine(AsyncScatterEngine):
        def submit_scatter_write(self, *arguments):
            name = arguments[0]
            result = self.results.pop(0)
            ticket = InterruptingAsyncTicket(self, name, result)
            self.tickets.append(ticket)
            self.events.append(("submit", name))
            self.inflight += 1
            self.peak_inflight = max(self.peak_inflight, self.inflight)
            return ticket

    engine = InterruptingAsyncEngine(("INTERRUPT", "COMPLETION_UNKNOWN"))
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionInterrupted) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"), range_batch_for("worker-1")),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert executor.pending_transfer_ids() == (raised.value.pending_transfer_id,)
    for ticket in engine.tickets:
        ticket.result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "COMPLETED"
    )


def test_execute_batches_unknown_outweighs_known_failure() -> None:
    engine = AsyncScatterEngine(("FAILED_DRAINED", "COMPLETION_UNKNOWN"))
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batches(
                (range_batch_for("worker-0"), range_batch_for("worker-1")),
                TransferDirection.WRITE,
                max_inflight_batches=2,
            )

    assert executor.pending_transfer_ids() == (raised.value.pending_transfer_id,)
    engine.tickets[1].result = "COMPLETED"
    assert executor.drain_pending_transfer(raised.value.pending_transfer_id) == (
        "FAILED_DRAINED"
    )


def test_resource_neutral_executor_supports_ticket_capable_binding() -> None:
    engine = TicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    read = executor.execute_batch(batch(), TransferDirection.READ)
    write = executor.execute_batch(batch(), TransferDirection.WRITE)

    assert [call[0] for call in engine.calls] == ["read", "write"]
    assert read.operation_count == write.operation_count == 2
    assert read.nbytes == write.nbytes == 192
    assert read.endpoint == write.endpoint == "worker-1:12345"


def test_submission_executes_multiple_batches_and_closes_its_handle() -> None:
    engine = TicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        read = submission.execute_batch(batch(), TransferDirection.READ)
        write = submission.execute_batch(batch(), TransferDirection.WRITE)

    assert read.operation_count == write.operation_count == 2
    assert [call[0] for call in engine.calls] == ["read", "write"]
    with pytest.raises(TransferEngineError, match="no longer active"):
        submission.execute_batch(batch(), TransferDirection.READ)


def test_transfer_submission_cannot_be_constructed_outside_executor_reservation() -> (
    None
):
    engine = TicketEngine()
    owner = MooncakeTransferEngineExecutor(engine)
    peer = MooncakeTransferEngineExecutor(engine)

    with owner.submission():
        with pytest.raises(TransferEngineError, match="must be created"):
            TransferSubmission(peer)
        with pytest.raises(TypeError):
            TransferSubmission(peer, object())

    assert engine.calls == []


def test_transfer_batch_rejects_mismatched_or_invalid_ranges() -> None:
    for values in (
        {"source_addresses": (0x1000,), "target_addresses": (), "sizes": (1,)},
        {
            "source_addresses": (0x1000,),
            "target_addresses": (0x2000,),
            "sizes": (0,),
        },
    ):
        try:
            TransferBatch(endpoint="worker-1:12345", **values)
        except ValueError:
            continue
        raise AssertionError("invalid transfer batch was accepted")


@pytest.mark.parametrize("address_field", ["source_addresses", "target_addresses"])
def test_transfer_batch_rejects_flat_address_range_overflow(
    address_field: str,
) -> None:
    values = {
        "source_addresses": (0x1000,),
        "target_addresses": (0x2000,),
        "sizes": (2,),
    }
    values[address_field] = ((1 << 64) - 1,)

    with pytest.raises(ValueError, match="address range overflows"):
        TransferBatch(endpoint="worker-1:12345", **values)


def test_transfer_batch_ranges_preserve_allocation_bounds_and_flattening() -> None:
    value = range_batch()

    assert value.source_addresses == (0x1020, 0x1100, 0x5000)
    assert value.target_addresses == (0x3040, 0x3200, 0x7020)
    assert value.sizes == (64, 128, 32)
    assert value.operation_count == 3

    with pytest.raises(ValueError, match="source allocation bounds"):
        TransferBatchRange(
            source_base_address=0x1000,
            source_capacity=64,
            target_base_address=0x2000,
            target_capacity=128,
            source_offsets=(32,),
            target_offsets=(0,),
            sizes=(64,),
        )


def test_executor_flattens_range_batches_for_the_current_python_binding() -> None:
    engine = LegacyEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    executor.execute_batch(range_batch(), TransferDirection.READ)

    assert engine.calls == [
        (
            "read",
            (
                "worker-1:12345",
                [0x3040, 0x3200, 0x7020],
                [0x1020, 0x1100, 0x5000],
                [64, 128, 32],
            ),
        )
    ]


def test_executor_uses_scatter_ticket_for_range_read() -> None:
    engine = ScatterTicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    receipt = executor.execute_batch(range_batch(), TransferDirection.READ)

    assert engine.calls == [
        (
            "scatter_read",
            (
                "worker-1:12345",
                [0x3000, 0x7000],
                [0x800, 0x100],
                [0x1000, 0x5000],
                [0x400, 0x100],
                [[0x40, 0x200], [0x20]],
                [[0x20, 0x100], [0]],
                [[64, 128], [32]],
            ),
        )
    ]
    assert receipt.operation_count == 3
    assert receipt.nbytes == 224


def test_executor_uses_scatter_ticket_for_range_write() -> None:
    engine = ScatterTicketEngine()
    executor = MooncakeTransferEngineExecutor(engine)

    receipt = executor.execute_batch(range_batch(), TransferDirection.WRITE)

    assert engine.calls == [
        (
            "scatter_write",
            (
                "worker-1:12345",
                [0x1000, 0x5000],
                [0x400, 0x100],
                [0x3000, 0x7000],
                [0x800, 0x100],
                [[0x20, 0x100], [0]],
                [[0x40, 0x200], [0x20]],
                [[64, 128], [32]],
            ),
        )
    ]
    assert receipt.operation_count == 3
    assert receipt.nbytes == 224


class UnknownTicket:
    status = "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms: int) -> str:
        return self.status


def test_submission_rejects_reuse_after_completion_becomes_unknown() -> None:
    ticket = UnknownTicket()
    engine = TicketEngine()
    calls = 0

    def return_unknown_ticket(*arguments):
        nonlocal calls
        calls += 1
        return ticket

    engine.batch_transfer_sync_write_with_ticket = return_unknown_ticket
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionUnknownError) as raised:
            submission.execute_batch(batch(), TransferDirection.WRITE)
        with pytest.raises(TransferEngineError, match="completion is unresolved"):
            submission.execute_batch(batch(), TransferDirection.WRITE)

    assert calls == 1
    ticket.status = "COMPLETED"
    assert (
        executor.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    )
    executor.execute_batch(batch(), TransferDirection.WRITE)
    assert calls == 2


class InterruptedTicket:
    status = "COMPLETION_UNKNOWN"

    def __init__(self, interruption: BaseException) -> None:
        self._interrupt = True
        self._interruption = interruption

    def drain(self, timeout_ms: int) -> str:
        if self._interrupt:
            self._interrupt = False
            raise self._interruption
        self.status = "COMPLETED"
        return self.status


@pytest.mark.parametrize(
    "interruption",
    (KeyboardInterrupt("completion wait interrupted"), SystemExit(2)),
)
def test_public_execute_batch_exposes_public_interruption(
    interruption: BaseException,
) -> None:
    ticket = InterruptedTicket(interruption)
    engine = TicketEngine()
    engine.batch_transfer_sync_write_with_ticket = lambda *arguments: ticket
    executor = MooncakeTransferEngineExecutor(engine)

    with pytest.raises(TransferCompletionInterrupted) as raised:
        executor.execute_batch(batch(), TransferDirection.WRITE)

    pending_transfer_id = raised.value.pending_transfer_id
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"
    assert raised.value.interruption is interruption


def test_submission_rejects_reuse_after_completion_wait_is_interrupted() -> None:
    ticket = InterruptedTicket(KeyboardInterrupt("completion wait interrupted"))
    engine = TicketEngine()
    calls = 0

    def return_interrupted_ticket(*arguments):
        nonlocal calls
        calls += 1
        return ticket

    engine.batch_transfer_sync_write_with_ticket = return_interrupted_ticket
    executor = MooncakeTransferEngineExecutor(engine)

    with executor.submission() as submission:
        with pytest.raises(TransferCompletionInterrupted) as raised:
            submission.execute_batch(batch(), TransferDirection.WRITE)
        with pytest.raises(TransferEngineError, match="completion is unresolved"):
            submission.execute_batch(batch(), TransferDirection.WRITE)

    assert calls == 1
    assert (
        executor.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    )


def test_scatter_unknown_ticket_is_retained_until_later_drain() -> None:
    ticket = UnknownTicket()
    engine = ScatterTicketEngine()
    engine.scatter_transfer_sync_write_with_ticket = lambda *arguments: ticket
    executor = MooncakeTransferEngineExecutor(engine)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        executor.execute_batch(range_batch(), TransferDirection.WRITE)

    pending_transfer_id = raised.value.pending_transfer_id
    ticket.status = "COMPLETED"
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"


class StatusReadFailsTicket:
    def __init__(self) -> None:
        self._first_read = True

    @property
    def status(self) -> str:
        if self._first_read:
            self._first_read = False
            raise RuntimeError("native ticket status is unavailable")
        return "COMPLETED"

    def drain(self, timeout_ms: int) -> str:
        return "COMPLETED"


def test_ticket_status_failure_quarantines_returned_ticket() -> None:
    ticket = StatusReadFailsTicket()
    engine = TicketEngine()
    engine.batch_transfer_sync_write_with_ticket = lambda *arguments: ticket
    executor = MooncakeTransferEngineExecutor(engine)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        executor.execute_batch(batch(), TransferDirection.WRITE)

    pending_transfer_id = raised.value.pending_transfer_id
    assert executor.drain_pending_transfer(pending_transfer_id) == "COMPLETED"


class SharedEngine(TicketEngine):
    def __init__(self, engine_ptr: int, ticket: UnknownTicket) -> None:
        super().__init__()
        self.engine_ptr = engine_ptr
        self.ticket = ticket

    def get_engine_ptr(self) -> int:
        return self.engine_ptr

    def batch_transfer_sync_write_with_ticket(self, *arguments):
        self.calls.append(("write", arguments))
        return self.ticket


def test_pending_engine_fence_is_shared_across_resource_executors() -> None:
    ticket = UnknownTicket()
    weight = MooncakeTransferEngineExecutor(SharedEngine(0xCAFE, ticket))
    kv = MooncakeTransferEngineExecutor(SharedEngine(0xCAFE, CompletedTicket()))

    with pytest.raises(TransferCompletionUnknownError) as raised:
        weight.execute_batch(batch(), TransferDirection.WRITE)

    with pytest.raises(TransferEngineError, match="pending transfer"):
        kv.execute_batch(batch(), TransferDirection.WRITE)

    ticket.status = "COMPLETED"
    assert (
        weight.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    )
    assert kv.execute_batch(batch(), TransferDirection.WRITE).operation_count == 2


class ReleaseToken:
    def __init__(
        self,
        token_id: str,
        *,
        fail_first: bool = False,
        reject_second: bool = False,
    ) -> None:
        self.calls = 0
        self.fail_first = fail_first
        self.reject_second = reject_second
        self._fence = AllocationFence(
            resource_id="resource",
            revision="revision",
            placement_id="placement",
            placement_digest="placement-digest",
            instance_id="instance",
            participant_id="participant",
            runtime_lease_id="lease",
            runtime_generation=1,
            binding_digest="binding-digest",
            fragment_ids=("runtime-0",),
            token_id=token_id,
        )

    @property
    def fence(self) -> AllocationFence:
        return self._fence

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        assert terminal_state is TerminalTransferState.COMPLETED
        self.calls += 1
        if self.fail_first and self.calls == 1:
            raise RuntimeError("transient release failure")
        if self.reject_second and self.calls > 1:
            raise RuntimeError("token released twice")


def test_partial_token_release_failure_never_replays_completed_token() -> None:
    executor = MooncakeTransferEngineExecutor(TicketEngine())
    transient = ReleaseToken("transient", fail_first=True)
    completed = ReleaseToken("completed", reject_second=True)
    pending_transfer_id = executor.retain_pending_registration_cleanup(
        terminal_state=TerminalTransferState.COMPLETED,
        registrations=(),
        resources=(),
        allocation_tokens=(transient, completed),
    )

    with pytest.raises(TransferEngineError, match="restart required"):
        executor.drain_pending_transfer(pending_transfer_id)

    assert transient.calls == 1
    assert completed.calls == 1
    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert executor.drain_pending_transfer(pending_transfer_id) == (
        "COMPLETION_UNKNOWN"
    )
    assert transient.calls == 1
    assert completed.calls == 1


def test_pending_unregister_interruption_never_replays_unknown_address() -> None:
    class InterruptAfterUnregisterResult(int):
        def __new__(cls):
            return super().__new__(cls, 0)

        def __ne__(self, other) -> bool:
            raise KeyboardInterrupt("interrupted after unregister returned")

    engine = TicketEngine()
    engine.unregister_calls = []

    def unregister_memory(address: int):
        engine.unregister_calls.append(address)
        return InterruptAfterUnregisterResult()

    engine.unregister_memory = unregister_memory
    executor = MooncakeTransferEngineExecutor(engine)
    pending_transfer_id = executor.retain_pending_registration_cleanup(
        terminal_state=TerminalTransferState.COMPLETED,
        registrations=(0x1000, 0x2000),
        resources=(),
    )

    with pytest.raises(KeyboardInterrupt, match="after unregister returned"):
        executor.drain_pending_transfer(pending_transfer_id, timeout_ms=0)

    assert executor.pending_transfer_status(pending_transfer_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert executor.drain_pending_transfer(pending_transfer_id, timeout_ms=0) == (
        "COMPLETION_UNKNOWN"
    )
    assert engine.unregister_calls == [0x2000]
