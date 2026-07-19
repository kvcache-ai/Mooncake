from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import torch

from mooncake_epd.core.transfer import TransferEngine


class _FakeMooncake:
    def __init__(self):
        self.registered = []
        self.unregistered = []
        self.batch_calls = []
        self.single_calls = []

    def register_memory(self, ptr, nbytes):
        self.registered.append((ptr, nbytes))
        return 0

    def unregister_memory(self, ptr):
        self.unregistered.append(ptr)
        return 0

    def batch_transfer_sync_write(self, remote_session, src_ptrs, dst_ptrs, lengths):
        self.batch_calls.append((remote_session, list(src_ptrs), list(dst_ptrs), list(lengths)))
        return 0

    def transfer_sync_write(self, remote_session, src_ptr, dst_ptr, length):
        self.single_calls.append((remote_session, src_ptr, dst_ptr, length))
        return 0


class _ConcurrentUnsafeMooncake(_FakeMooncake):
    """Fake native engine that exposes overlapping write calls deterministically."""

    def __init__(self):
        super().__init__()
        self._active = 0
        self.max_active = 0
        self._active_lock = threading.Lock()

    def batch_transfer_sync_write(self, remote_session, src_ptrs, dst_ptrs, lengths):
        with self._active_lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            # The sleep releases the GIL and makes an unguarded wrapper overlap
            # reliably when several connector sender workers dispatch at once.
            time.sleep(0.02)
            return super().batch_transfer_sync_write(
                remote_session, src_ptrs, dst_ptrs, lengths
            )
        finally:
            with self._active_lock:
                self._active -= 1


class _TransactionUnsafeMooncake(_FakeMooncake):
    """Fake binding that rejects interleaved registration transactions.

    Serializing each individual native call is insufficient for E→P: a second
    request can register its tensor between a first request's registration and
    transfer, replacing the binding's active descriptor state.  This fake
    makes that normally timing-sensitive failure deterministic.
    """

    def __init__(self):
        super().__init__()
        self._owner = None
        self._lock = threading.Lock()
        self.violation = False

    def _step(self, owner, phase):
        owner = int(owner)
        with self._lock:
            if phase == "register":
                if self._owner is not None:
                    self.violation = True
                self._owner = owner
            elif self._owner != owner:
                self.violation = True
            if phase == "unregister" and self._owner == owner:
                self._owner = None
        # Release the GIL so an implementation that locks only individual
        # operations reliably exposes a cross-request interleave.
        time.sleep(0.01)

    def register_memory(self, ptr, nbytes):
        self._step(ptr, "register")
        return super().register_memory(ptr, nbytes)

    def unregister_memory(self, ptr):
        self._step(ptr, "unregister")
        return super().unregister_memory(ptr)

    def batch_transfer_sync_write(self, remote_session, src_ptrs, dst_ptrs, lengths):
        self._step(src_ptrs[0], "write")
        return super().batch_transfer_sync_write(remote_session, src_ptrs, dst_ptrs, lengths)

    def transfer_sync_write(self, remote_session, src_ptr, dst_ptr, length):
        self._step(src_ptr, "write")
        return super().transfer_sync_write(remote_session, src_ptr, dst_ptr, length)

    def batch_transfer_sync_read(self, remote_session, dst_ptrs, src_ptrs, lengths):
        self._step(dst_ptrs[0], "read")
        return 0

    def transfer_sync_read(self, remote_session, dst_ptr, src_ptr, length):
        self._step(dst_ptr, "read")
        return 0


def test_peer_transfer_plan_batches_registered_tensor_descriptors():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = _FakeMooncake()  # noqa: SLF001

    tensors = [
        torch.randn(4, 8, dtype=torch.float32),
        torch.randn(2, 16, dtype=torch.float32),
    ]
    plan = engine.build_peer_transfer_plan(
        tensors=tensors,
        remote_session="peer-0",
        remote_pointers=[0x1000, 0x2000],
        mirror_local_copy=True,
        target_device=torch.device("cpu"),
    )

    result = engine.transfer_peer_buffer_plan(plan)

    assert result.descriptor_count == 2
    assert result.nbytes == sum(t.nelement() * t.element_size() for t in tensors)
    assert len(engine._mooncake.batch_calls) == 1  # noqa: SLF001
    remote_session, src_ptrs, dst_ptrs, lengths = engine._mooncake.batch_calls[0]  # noqa: SLF001
    assert remote_session == "peer-0"
    assert dst_ptrs == [0x1000, 0x2000]
    assert lengths == [t.nelement() * t.element_size() for t in tensors]
    assert src_ptrs == [t.data_ptr() for t in tensors]
    assert engine._mooncake.registered == list(zip(src_ptrs, lengths))  # noqa: SLF001
    assert engine._mooncake.unregistered == src_ptrs  # noqa: SLF001
    assert len(result.mirrored_tensors) == 2
    for original, mirrored in zip(tensors, result.mirrored_tensors):
        assert mirrored is not None
        assert torch.allclose(original, mirrored)


def test_peer_transfer_plan_uses_single_descriptor_fast_path():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = _FakeMooncake()  # noqa: SLF001

    tensor = torch.randn(8, dtype=torch.float32)
    plan = engine.build_peer_transfer_plan(
        tensors=[tensor],
        remote_session="peer-1",
        remote_pointers=[0x3000],
        mirror_local_copy=False,
    )

    result = engine.transfer_peer_buffer_plan(plan)

    assert result.descriptor_count == 1
    assert len(engine._mooncake.single_calls) == 1  # noqa: SLF001
    assert engine._mooncake.batch_calls == []  # noqa: SLF001


def test_pointer_transfer_plan_reuses_registered_pointers_without_extra_register():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    plan = engine.build_pointer_transfer_plan(
        remote_session="peer-2",
        local_pointers=[0x1000, 0x2000],
        remote_pointers=[0x3000, 0x4000],
        lengths=[256, 512],
        registered=True,
    )
    result = engine.transfer_peer_buffer_plan(plan)

    assert result.descriptor_count == 2
    assert result.nbytes == 768
    assert fake.batch_calls == [("peer-2", [0x1000, 0x2000], [0x3000, 0x4000], [256, 512])]
    assert fake.registered == []
    assert fake.unregistered == []


def test_registered_pointer_batch_bypasses_descriptor_and_registration_work():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    result = engine.transfer_registered_pointer_batch(
        remote_session="peer-fast",
        local_pointers=[0x1000, 0x2000],
        remote_pointers=[0x3000, 0x4000],
        lengths=[256, 512],
    )

    assert result.descriptor_count == 2
    assert result.nbytes == 768
    assert fake.batch_calls == [
        ("peer-fast", [0x1000, 0x2000], [0x3000, 0x4000], [256, 512])
    ]
    assert fake.registered == []
    assert fake.unregistered == []
    assert result.timings_ms["registered_pointer_fast_path"] == 1.0


def test_registered_pointer_batch_uses_correct_single_descriptor_signature():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    result = engine.transfer_registered_pointer_batch(
        remote_session="peer-fast-single",
        local_pointers=[0x1000],
        remote_pointers=[0x3000],
        lengths=[256],
    )

    assert result.descriptor_count == 1
    assert fake.single_calls == [("peer-fast-single", 0x1000, 0x3000, 256)]
    assert fake.batch_calls == []


def test_registered_pointer_batch_serializes_shared_native_engine_calls():
    """Concurrent sender workers must not overlap one Mooncake engine call.

    The EPD producer intentionally keeps CUDA-event waits and descriptor
    preparation parallel, but its one native TransferEngine instance is shared
    by all sender workers.  TCP's single I/O context does not make concurrent
    Python binding entry safe, so the fast path must hold its engine-local
    lock only around the native call.
    """

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _ConcurrentUnsafeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    def _send(index: int):
        return engine.transfer_registered_pointer_batch(
            remote_session="peer-concurrent",
            local_pointers=[0x1000 + index * 0x100, 0x2000 + index * 0x100],
            remote_pointers=[0x3000 + index * 0x100, 0x4000 + index * 0x100],
            lengths=[256, 512],
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(_send, range(4)))

    assert all(result.descriptor_count == 2 for result in results)
    assert len(fake.batch_calls) == 4
    assert fake.max_active == 1


def test_registered_descriptor_transaction_is_atomic_across_concurrent_feature_writes():
    """E→P source registration cannot interleave with another publish."""

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _TransactionUnsafeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    def _send(index: int):
        tensor = torch.full((16,), float(index), dtype=torch.float32)
        plan = engine.build_peer_transfer_plan(
            tensors=[tensor],
            remote_session="prefill-feature-session",
            remote_pointers=[0x9000 + index * 0x100],
            mirror_local_copy=False,
        )
        return engine.transfer_peer_buffer_plan(plan)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(_send, range(4)))

    assert all(result.descriptor_count == 1 for result in results)
    assert fake.violation is False
    assert len(fake.registered) == len(fake.unregistered) == 4


def test_registered_descriptor_plan_re_registers_tensor_on_reuse():
    """A caller may reuse a plan after its first temporary registration ends."""

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones((8,), dtype=torch.float32)
    plan = engine.build_peer_transfer_plan(
        tensors=[tensor],
        remote_session="prefill-feature-session",
        remote_pointers=[0xABCD],
        mirror_local_copy=False,
    )

    engine.transfer_peer_buffer_plan(plan)
    engine.transfer_peer_buffer_plan(plan)

    assert len(fake.registered) == 2
    assert fake.unregistered == [tensor.data_ptr(), tensor.data_ptr()]


def test_registered_tensor_read_transaction_is_atomic_across_concurrent_feature_reads():
    """EngineCore E→P reads keep registration, read and cleanup together."""

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _TransactionUnsafeMooncake()
    engine._mooncake = fake  # noqa: SLF001

    def _read(index: int):
        tensor = torch.empty((32,), dtype=torch.float32)
        return engine.read_remote_peer_buffers_into_tensors(
            remote_session="prefill-api-session",
            remote_pointers=[0xB000 + index * 0x100],
            tensors=[tensor],
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(_read, range(4)))

    assert all(result["descriptor_count"] == 1.0 for result in results)
    assert fake.violation is False
    assert len(fake.registered) == len(fake.unregistered) == 4


def test_registered_pointer_batch_rejects_shape_mismatch():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = _FakeMooncake()  # noqa: SLF001

    with pytest.raises(ValueError, match="identical lengths"):
        engine.transfer_registered_pointer_batch(
            remote_session="peer-fast",
            local_pointers=[0x1000],
            remote_pointers=[0x2000, 0x3000],
            lengths=[256],
        )


def test_bind_mooncake_backend_does_not_shutdown_borrowed_engine():
    class _BorrowedMooncake(_FakeMooncake):
        def __init__(self):
            super().__init__()
            self.shutdown_calls = 0

        def shutdown(self):
            self.shutdown_calls += 1

    borrowed = _BorrowedMooncake()
    engine = TransferEngine(protocol="tcp")
    engine.bind_mooncake_backend(borrowed, initialized=True, owns_backend=False)

    engine.shutdown()

    assert borrowed.shutdown_calls == 0


def test_pointer_transfer_plan_rejects_unregistered_pointer_only_descriptors():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = _FakeMooncake()  # noqa: SLF001

    plan = engine.build_pointer_transfer_plan(
        remote_session="peer-3",
        local_pointers=[0x1000],
        remote_pointers=[0x2000],
        lengths=[128],
        registered=False,
    )

    with pytest.raises(ValueError, match="registered memory"):
        engine.transfer_peer_buffer_plan(plan)
