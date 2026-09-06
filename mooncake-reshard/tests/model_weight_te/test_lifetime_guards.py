from __future__ import annotations

import pytest

from mooncake.reshard.weight.te import (
    TransferCompletionFailedError,
    MooncakeTransferEngineSink,
    TransferCompletionUnknownError,
)
from mooncake.reshard.transfer_engine.lifetime import TerminalTransferState

from .helpers import (
    FakeBatchTransferTicket,
    FakeTransferEngine,
    allocation_guards,
    manifests,
    plan_transfer,
    registration_leases,
)


def _all_tokens(*guard_maps):
    return tuple(
        token
        for guard_map in guard_maps
        for guard in guard_map.values()
        for token in guard.tokens
    )


def test_live_te_rejects_raw_bindings_without_allocation_guards() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)

    with pytest.raises(ValueError, match="source allocation guard providers"):
        MooncakeTransferEngineSink(FakeTransferEngine()).execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
        )


def test_live_te_holds_source_and_target_guards_until_known_completion() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)

    MooncakeTransferEngineSink(FakeTransferEngine()).execute(
        plan_transfer(source, target),
        source.placement,
        source.bindings[0],
        target.placement,
        target.bindings,
        target_registrations=registration_leases(target),
        source_allocation_guards=source_guards,
        target_allocation_guards=target_guards,
    )

    assert _all_tokens(source_guards, target_guards)
    assert all(
        token.released_states == [TerminalTransferState.COMPLETED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_pending_transfer_keeps_guards_until_drain_reaches_terminal() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()
    ticket = FakeBatchTransferTicket(["COMPLETION_UNKNOWN"] * 3)
    engine.batch_transfer_sync_write_with_ticket = lambda *args: ticket
    sink = MooncakeTransferEngineSink(engine, max_completion_drain_attempts=1)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        sink.execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert all(
        token.released_states == []
        for token in _all_tokens(source_guards, target_guards)
    )
    ticket._statuses = ["COMPLETED"]
    assert sink.drain_pending_transfer(raised.value.pending_transfer_id) == "COMPLETED"
    assert all(
        token.released_states == [TerminalTransferState.COMPLETED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_known_native_failure_releases_guards_after_failed_drain() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()
    engine.fail_endpoint = "target-t0:12345"

    with pytest.raises(TransferCompletionFailedError, match="target-t0:12345"):
        MooncakeTransferEngineSink(engine).execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert all(
        token.released_states == [TerminalTransferState.FAILED_DRAINED]
        for token in _all_tokens(source_guards, target_guards)
    )


def test_native_exception_without_completion_ticket_keeps_guards_quarantined() -> None:
    source = manifests(tp=1, prefix="source", address_base=0x10000)
    target = manifests(tp=1, prefix="target", address_base=0x40000)
    source_guards = allocation_guards(source)
    target_guards = allocation_guards(target)
    engine = FakeTransferEngine()

    def fail_without_ticket(*args, **kwargs):
        raise RuntimeError("native submission raised")

    engine.batch_transfer_sync_write = fail_without_ticket
    sink = MooncakeTransferEngineSink(engine)
    with pytest.raises(TransferCompletionUnknownError) as raised:
        sink.execute(
            plan_transfer(source, target),
            source.placement,
            source.bindings[0],
            target.placement,
            target.bindings,
            target_registrations=registration_leases(target),
            source_allocation_guards=source_guards,
            target_allocation_guards=target_guards,
        )

    assert (
        sink.pending_transfer_status(raised.value.pending_transfer_id)
        == "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert all(
        token.released_states == []
        for token in _all_tokens(source_guards, target_guards)
    )
