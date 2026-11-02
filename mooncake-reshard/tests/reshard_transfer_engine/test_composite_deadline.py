from __future__ import annotations

import time

import pytest
from mooncake.reshard.transfer_engine import (
    MooncakeTransferEngineExecutor,
    TransferBatch,
    TransferBatchRange,
    TransferCompletionUnknownError,
    TransferDirection,
)
from mooncake.reshard.transfer_engine import completion


class Clock:
    now_ns = 0

    def monotonic_ns(self) -> int:
        return self.now_ns


class Ticket:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.status = "COMPLETION_UNKNOWN"
        self.budgets: list[int] = []
        self.wait_ms = 1000
        self.result = "COMPLETION_UNKNOWN"
        self.error: BaseException | None = None

    def drain(self, timeout_ms: int) -> str:
        self.budgets.append(timeout_ms)
        self.clock.now_ns += min(self.wait_ms, timeout_ms) * 1_000_000
        if self.error is not None:
            raise self.error
        self.status = self.result
        return self.status


class Engine:
    def __init__(self, tickets: list[Ticket]) -> None:
        self.tickets = iter(tickets)

    def submit_scatter_write(self, *arguments: object) -> Ticket:
        return next(self.tickets)


def pending(tickets: list[Ticket]) -> tuple[MooncakeTransferEngineExecutor, str]:
    executor = MooncakeTransferEngineExecutor(
        Engine(tickets), max_completion_drain_attempts=0
    )
    batch = TransferBatch.from_ranges(
        endpoint="worker",
        ranges=(TransferBatchRange(0x1000, 64, 0x2000, 64, (0,), (0,), (64,)),),
    )
    with pytest.raises(TransferCompletionUnknownError) as raised:
        with executor.submission() as submission:
            submission.execute_batches(
                (batch,) * len(tickets),
                TransferDirection.WRITE,
                max_inflight_batches=max(2, len(tickets)),
            )
    return executor, raised.value.pending_transfer_id


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock()
    monkeypatch.setattr(time, "monotonic_ns", clock.monotonic_ns)
    monkeypatch.setattr(completion, "_pending_transfers", {})
    return clock


@pytest.mark.parametrize("count", [2, 4, 8])
@pytest.mark.parametrize("timeout_ms", [0, 100])
def test_pending_composite_uses_one_timeout_budget(
    clock: Clock, count: int, timeout_ms: int
) -> None:
    tickets = [Ticket(clock) for _ in range(count)]
    executor, pending_id = pending(tickets)

    assert executor.drain_pending_transfer(pending_id, timeout_ms=timeout_ms) == (
        "COMPLETION_UNKNOWN"
    )
    assert clock.now_ns <= timeout_ms * 1_000_000
    assert [ticket.budgets for ticket in tickets] == [[timeout_ms]] + [[0]] * (
        count - 1
    )
    assert executor.pending_transfer_ids() == (pending_id,)


@pytest.mark.parametrize("result", ["COMPLETED", "FAILED_DRAINED"])
def test_pending_composite_preserves_terminal_result(clock: Clock, result: str) -> None:
    tickets = [Ticket(clock) for _ in range(3)]
    executor, pending_id = pending(tickets)
    tickets[0].status = result
    for ticket in tickets[1:]:
        ticket.wait_ms = 25
        ticket.result = "COMPLETED"

    assert executor.drain_pending_transfer(pending_id, timeout_ms=100) == result
    assert [ticket.budgets for ticket in tickets] == [[], [100], [75]]
    assert executor.pending_transfer_ids() == ()


@pytest.mark.parametrize("interrupt", [False, True])
def test_pending_composite_checks_later_children_after_error(
    clock: Clock, interrupt: bool
) -> None:
    tickets = [Ticket(clock) for _ in range(3)]
    executor, pending_id = pending(tickets)
    tickets[0].error = KeyboardInterrupt("wait") if interrupt else RuntimeError("wait")
    tickets[1].result = "FAILED_DRAINED"
    tickets[2].result = "COMPLETED"

    if interrupt:
        with pytest.raises(KeyboardInterrupt, match="wait"):
            executor.drain_pending_transfer(pending_id, timeout_ms=100)
    else:
        assert executor.drain_pending_transfer(pending_id, timeout_ms=100) == (
            "COMPLETION_UNKNOWN"
        )
    assert [ticket.budgets for ticket in tickets] == [[100], [0], [0]]
    assert executor.pending_transfer_status(pending_id) == "COMPLETION_UNKNOWN"
    tickets[0].error = None
    tickets[0].result = "COMPLETED"
    assert executor.drain_pending_transfer(pending_id, timeout_ms=0) == "FAILED_DRAINED"
    assert executor.pending_transfer_ids() == ()
