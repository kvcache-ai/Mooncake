from __future__ import annotations

import pytest

from mooncake.reshard.weight.te import (
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    TransferCompletionUnknownError,
    TransferEngineError,
)

from .helpers import (
    FakeTransferEngine,
    execute_reader,
    execute_sink,
    manifests,
    plan_transfer,
    plan_transfer_to_local_target,
    registration_leases,
)


def test_te_reader_quarantines_legacy_completion_unknown_without_ticket() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    engine = FakeTransferEngine()
    engine.read_result = -2
    reader = MooncakeTransferEngineReader(engine)

    with pytest.raises(TransferCompletionUnknownError) as raised:
        execute_reader(
            reader,
            plan_transfer_to_local_target(sources, targets),
            sources,
            targets,
            source_registrations=registration_leases(sources),
        )

    pending_id = raised.value.pending_transfer_id
    assert "restart-required" in str(raised.value)
    assert reader.pending_transfer_ids() == (pending_id,)
    assert reader.pending_transfer_status(pending_id) == (
        "COMPLETION_UNKNOWN_RESTART_REQUIRED"
    )
    assert reader.drain_pending_transfer(pending_id, timeout_ms=0) == (
        "COMPLETION_UNKNOWN"
    )
    assert engine.unregister_calls == []


def test_te_legacy_completion_unknown_blocks_same_engine_until_restart() -> None:
    sources = manifests(tp=1, prefix="source", address_base=0x10000)
    targets = manifests(tp=1, prefix="target", address_base=0x40000)
    plan = plan_transfer_to_local_target(sources, targets)
    engine = FakeTransferEngine()
    engine.read_result = -2
    reader = MooncakeTransferEngineReader(engine)

    with pytest.raises(TransferCompletionUnknownError):
        execute_reader(
            reader,
            plan,
            sources,
            targets,
            source_registrations=registration_leases(sources),
        )

    call_count = len(engine.calls)
    register_count = len(engine.register_calls)
    engine.read_result = 0
    with pytest.raises(TransferEngineError, match="restart-required"):
        execute_sink(
            MooncakeTransferEngineSink(engine),
            plan_transfer(sources, targets),
            sources,
            targets,
            target_registrations=registration_leases(targets),
        )
    with pytest.raises(TransferEngineError, match="restart-required"):
        execute_reader(
            MooncakeTransferEngineReader(engine),
            plan,
            sources,
            targets,
            source_registrations=registration_leases(sources),
        )

    assert len(engine.calls) == call_count
    assert len(engine.register_calls) == register_count
    assert engine.unregister_calls == []


def test_te_canonical_engine_identity_fails_closed_for_broken_getter() -> None:
    engine = FakeTransferEngine()

    def fail_get_engine_ptr() -> int:
        raise RuntimeError("native engine handle unavailable")

    engine.get_engine_ptr = fail_get_engine_ptr
    with pytest.raises(TransferEngineError, match="canonical engine identity"):
        MooncakeTransferEngineSink(engine)
