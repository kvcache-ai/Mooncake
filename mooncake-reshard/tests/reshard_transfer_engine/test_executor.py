from __future__ import annotations

import pytest

from mooncake.reshard.transfer_engine import (
    AllocationFence,
    MooncakeTransferEngineExecutor,
    TransferBatch,
    TransferBatchRange,
    TransferDirection,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferEngineError,
    TransferSubmission,
    TerminalTransferState,
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
    executor.retain_pending_resources(
        raised.value.pending_transfer_id,
        registrations=(),
        resources=(ticket,),
    )
    ticket.status = "COMPLETED"
    assert (
        executor.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    )


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
    executor.retain_pending_resources(
        pending_transfer_id,
        registrations=(),
        resources=(ticket,),
    )
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
    executor.retain_pending_resources(
        raised.value.pending_transfer_id,
        registrations=(),
        resources=(ticket,),
    )
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
    executor.retain_pending_resources(
        pending_transfer_id,
        registrations=(),
        resources=(ticket,),
    )
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
    executor.retain_pending_resources(
        pending_transfer_id,
        registrations=(),
        resources=(),
    )
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
    weight.retain_pending_resources(
        raised.value.pending_transfer_id,
        registrations=(),
        resources=(ticket,),
    )

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
