from __future__ import annotations

import pytest

from mooncake.reshard.transfer_engine import (
    MooncakeTransferEngineExecutor,
    TransferBatch,
    TransferBatchRange,
    TransferDirection,
    TransferCompletionUnknownError,
    TransferEngineError,
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


class UnknownTicket:
    status = "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms: int) -> str:
        return self.status


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
