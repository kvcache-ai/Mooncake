from __future__ import annotations

import ctypes
import gc
import weakref

import pytest

from mooncake.reshard.transfer_engine import (
    RegisteredTransferEngine,
    TransferCompletionInterrupted,
    TransferCompletionUnknownError,
    TransferRegistrationCleanupPendingError,
)


class CopyEngine:
    def __init__(self):
        self.registrations = []
        self.unregistrations = []
        self.calls = []
        self.unregister_result = 0
        self.fail_call = None
        self.unknown_call = None
        self.ticket = None

    def register_memory(self, ptr, size):
        self.registrations.append((ptr, size))
        return 0

    def unregister_memory(self, ptr):
        self.unregistrations.append(ptr)
        return self.unregister_result

    def batch_transfer_sync_read(self, endpoint, targets, sources, sizes):
        self.calls.append((endpoint, targets, sources, sizes))
        if len(self.calls) == self.fail_call:
            return -1
        for target, source, size in zip(targets, sources, sizes):
            ctypes.memmove(target, source, size)
        return 0

    def batch_transfer_sync_read_with_ticket(self, *args):
        result = self.batch_transfer_sync_read(*args)
        status = "COMPLETED" if result == 0 else "FAILED_DRAINED"
        if len(self.calls) == self.unknown_call:
            status = "COMPLETION_UNKNOWN"
        self.ticket = Ticket(status)
        return self.ticket


class Ticket:
    def __init__(self, status):
        self.status = status

    @property
    def drained(self):
        return self.status != "COMPLETION_UNKNOWN"

    def drain(self, timeout_ms):
        return self.status


@pytest.mark.parametrize(
    "ptr,size", [(True, 8), (1, 0), (1 << 64, 8), ((1 << 64) - 1, 8)]
)
def test_invalid_registration_rejected_before_native_call(ptr, size):
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    with pytest.raises(ValueError):
        transport.register_buffer(ptr, size)
    assert engine.registrations == []
    transport.close()


def test_registered_copy_and_owner_identity():
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    source = ctypes.create_string_buffer(b"abcdefgh")
    target = ctypes.create_string_buffer(8)
    ptr = ctypes.addressof(target)
    transport.register_buffer(ptr, 8, target)
    transport.register_buffer(ptr, 8, target)
    assert len(engine.registrations) == 1
    with pytest.raises(ValueError, match="owner changed"):
        transport.register_buffer(ptr, 8, object())
    transport.read_into_targets(
        "remote", ctypes.addressof(source), 8, [(2, ptr + 1, 3, target)]
    )
    assert target.raw == b"\x00cde\x00\x00\x00\x00"
    transport.close()
    assert engine.unregistrations == [ptr]
    with pytest.raises(RuntimeError):
        transport.register_buffer(ptr, 8, target)


def test_all_ranges_validated_before_submit():
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    target = ctypes.create_string_buffer(8)
    ptr = ctypes.addressof(target)
    transport.register_buffer(ptr, 8, target)
    with pytest.raises(ValueError):
        transport.read_into_targets(
            "remote", 1024, 8, [(0, ptr, 1, target), (8, ptr, 1, target)]
        )
    assert engine.calls == []
    transport.close()


def test_unregister_cleanup_retains_owner_until_drain():
    engine = CopyEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    target = ctypes.create_string_buffer(8)
    reference = weakref.ref(target)
    ptr = ctypes.addressof(target)
    transport.register_buffer(ptr, 8, target)
    engine.unregister_result = -1
    with pytest.raises(TransferRegistrationCleanupPendingError):
        transport.close()
    del target
    gc.collect()
    assert reference() is not None
    with pytest.raises(RuntimeError):
        transport.close()
    engine.unregister_result = 0
    assert transport.drain_pending() == ()
    gc.collect()
    assert reference() is None
    transport.close()


def test_unknown_retains_transport_and_owner_until_terminal_drain():
    engine = CopyEngine()
    engine.unknown_call = 1
    transport = RegisteredTransferEngine(engine, endpoint="local")
    source = ctypes.create_string_buffer(b"abcd")
    target = ctypes.create_string_buffer(8)
    reference = weakref.ref(target)
    ptr = ctypes.addressof(target)
    transport.register_buffer(ptr, 8, target)
    with pytest.raises(TransferCompletionUnknownError):
        transport.read_into_targets(
            "remote", ctypes.addressof(source), 4, [(0, ptr, 4, target)]
        )
    assert transport.poisoned
    del target
    gc.collect()
    assert reference() is not None
    with pytest.raises(RuntimeError):
        transport.close()
    assert transport.drain_pending()
    engine.ticket.status = "COMPLETED"
    assert transport.drain_pending() == ()
    assert not transport.poisoned
    transport.close()
    gc.collect()
    assert reference() is None


def test_interrupted_completion_uses_shared_pending_handoff():
    class InterruptTicket(Ticket):
        def drain(self, timeout_ms):
            if self.status == "COMPLETION_UNKNOWN":
                raise KeyboardInterrupt("interrupted completion")
            return self.status

    class InterruptEngine(CopyEngine):
        def batch_transfer_sync_read_with_ticket(self, *args):
            self.batch_transfer_sync_read(*args)
            self.ticket = InterruptTicket("COMPLETION_UNKNOWN")
            return self.ticket

    engine = InterruptEngine()
    transport = RegisteredTransferEngine(engine, endpoint="local")
    source = ctypes.create_string_buffer(b"abcd")
    target = ctypes.create_string_buffer(8)
    ptr = ctypes.addressof(target)
    transport.register_buffer(ptr, 8, target)
    with pytest.raises(TransferCompletionInterrupted):
        transport.read_into_targets(
            "remote", ctypes.addressof(source), 4, [(0, ptr, 4, target)]
        )
    assert transport.poisoned and transport.executor.pending_transfer_ids()
    engine.ticket.status = "COMPLETED"
    assert transport.drain_pending() == ()
    transport.close()
