from __future__ import annotations

import gc
import weakref

import pytest
from mooncake.reshard.lifetime import AllocationTokenSet
from mooncake.reshard.transfer_engine import (
    AllocationFence,
    MooncakeTransferEngineExecutor,
    TerminalTransferState,
    TransferBatch,
    TransferCompletionUnknownError,
    TransferDirection,
    TransferEngineError,
)
from mooncake.reshard.transfer_engine import completion
from mooncake.reshard.transfer_engine.registration import registered_targets


class Ticket:
    status = "COMPLETION_UNKNOWN"

    def __init__(self) -> None:
        self.drain_calls = 0

    def drain(self, timeout_ms: int) -> str:
        self.drain_calls += 1
        self.status = "COMPLETED"
        return self.status


class Engine:
    ticket_ref: weakref.ReferenceType[Ticket]

    def batch_transfer_sync_write_with_ticket(self, *arguments: object) -> Ticket:
        ticket = Ticket()
        self.ticket_ref = weakref.ref(ticket)
        return ticket


class FailingReleaseToken:
    fence = AllocationFence(
        resource_id="resource",
        revision="revision",
        placement_id="placement",
        placement_digest="digest",
        instance_id="instance",
        participant_id="participant",
        runtime_lease_id="lease",
        runtime_generation=0,
        binding_digest="binding",
        fragment_ids=("fragment",),
        token_id="token",
    )

    def release_after_terminal(self, terminal_state: TerminalTransferState) -> None:
        raise RuntimeError("release outcome unknown")


def quarantine(executor: MooncakeTransferEngineExecutor, engine: Engine) -> str:
    with pytest.raises(TransferCompletionUnknownError) as raised:
        with registered_targets(
            engine,
            executor,
            (),
            pre_registered=True,
            resources=(),
            lifetime_tokens=AllocationTokenSet((FailingReleaseToken(),)),
        ):
            with executor.submission() as submission:
                submission.execute_batch(
                    TransferBatch("worker", (0x1000,), (0x2000,), (64,)),
                    TransferDirection.WRITE,
                )
    return raised.value.pending_transfer_id


def replace_pending_ticket(executor: MooncakeTransferEngineExecutor, pending_id: str):
    with pytest.raises(TransferEngineError, match="release outcome is unknown"):
        executor.drain_pending_transfer(pending_id, timeout_ms=0)


def test_pending_replacement_retains_native_ticket_without_delegating_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(completion, "_pending_transfers", {})
    engine = Engine()
    executor = MooncakeTransferEngineExecutor(engine, max_completion_drain_attempts=0)
    pending_id = quarantine(executor, engine)
    gc.collect()
    assert engine.ticket_ref() is not None

    replace_pending_ticket(executor, pending_id)
    gc.collect()
    ticket = engine.ticket_ref()
    assert ticket is not None
    assert ticket.drain_calls == 1
    assert executor.pending_transfer_status(pending_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert executor.drain_pending_transfer(pending_id, timeout_ms=0) == (
        "COMPLETION_UNKNOWN"
    )
    assert ticket.drain_calls == 1
    assert executor.pending_transfer_ids() == (pending_id,)
