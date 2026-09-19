from __future__ import annotations

import threading

import pytest
import torch

from mooncake_epd.core.transfer import (
    PeerTransferDescriptor,
    PeerTransferPlan,
    TransferEngine,
)


class _FakeMooncake:
    def __init__(self, register_rc=0):
        self.registered = []
        self.unregistered = []
        self.batch_calls = []
        self.single_calls = []
        self.register_rc = register_rc

    def register_memory(self, ptr, nbytes):
        self.registered.append((ptr, nbytes))
        return self.register_rc

    def unregister_memory(self, ptr):
        self.unregistered.append(ptr)
        return 0

    def batch_transfer_sync_write(self, remote_session, src_ptrs, dst_ptrs, lengths):
        self.batch_calls.append((remote_session, list(src_ptrs), list(dst_ptrs), list(lengths)))
        return 0

    def transfer_sync_write(self, remote_session, src_ptr, dst_ptr, length):
        self.single_calls.append((remote_session, src_ptr, dst_ptr, length))
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


def test_borrowed_native_registration_is_never_unregistered_by_borrower():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake(register_rc=-600)
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)

    handle = engine.register_tensor_memory(tensor)
    engine.unregister_tensor_memory(handle)

    assert handle.borrowed is True
    assert handle.owns_registration is False
    assert fake.unregistered == []
    stats = engine.registration_stats()
    assert stats["borrowed_registrations"] == 1
    assert stats["active_registrations"] == 0
    assert stats["illegal_unregisters"] == 0


def test_duplicate_local_registration_defers_owner_unregister_until_last_release():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)

    owner = engine.register_tensor_memory(tensor)
    borrower = engine.register_tensor_memory(tensor)
    assert len(fake.registered) == 1
    assert owner.owns_registration is True
    assert borrower.borrowed is True

    engine.unregister_tensor_memory(owner)
    assert fake.unregistered == []
    assert engine.registration_stats()["active_registration_refs"] == 1
    engine.unregister_tensor_memory(borrower)

    assert fake.unregistered == [tensor.data_ptr()]
    assert engine.registration_stats()["active_registrations"] == 0


def test_owner_release_during_inflight_transfer_defers_unregister():
    started = threading.Event()
    finish = threading.Event()

    class _BlockingMooncake(_FakeMooncake):
        def transfer_sync_write(self, remote_session, src_ptr, dst_ptr, length):
            self.single_calls.append((remote_session, src_ptr, dst_ptr, length))
            started.set()
            assert finish.wait(timeout=5)
            return 0

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _BlockingMooncake()
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)
    handle = engine.register_tensor_memory(tensor)
    plan = PeerTransferPlan(
        remote_session="peer-inflight",
        descriptors=[
            PeerTransferDescriptor(
                local_pointer=tensor.data_ptr(),
                remote_pointer=0x4000,
                size_bytes=tensor.nelement() * tensor.element_size(),
                local_buffer=handle,
            )
        ],
        mirror_local_copy=False,
    )
    errors = []

    def _transfer():
        try:
            engine.transfer_peer_buffer_plan(plan)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=_transfer)
    thread.start()
    assert started.wait(timeout=5)
    engine.unregister_tensor_memory(handle)
    assert fake.unregistered == []
    assert engine.registration_stats()["inflight_registrations"] == 1
    finish.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    assert fake.unregistered == [tensor.data_ptr()]
    assert engine.registration_stats()["active_registrations"] == 0


def test_transfer_cleanup_does_not_unregister_native_borrowed_registration():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake(register_rc=-600)
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)
    plan = engine.build_peer_transfer_plan(
        tensors=[tensor],
        remote_session="peer-borrowed",
        remote_pointers=[0x5000],
        mirror_local_copy=False,
    )

    result = engine.transfer_peer_buffer_plan(plan)

    assert result.descriptor_count == 1
    assert fake.unregistered == []
    assert engine.registration_stats()["active_registrations"] == 0


def test_descriptor_validation_failure_releases_prior_owned_registration():
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake()
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)
    plan = PeerTransferPlan(
        remote_session="peer-invalid",
        descriptors=[
            PeerTransferDescriptor(
                local_pointer=tensor.data_ptr(),
                remote_pointer=0x6000,
                size_bytes=tensor.nelement() * tensor.element_size(),
                tensor=tensor,
            ),
            PeerTransferDescriptor(
                local_pointer=0,
                remote_pointer=0x7000,
                size_bytes=64,
            ),
        ],
    )

    with pytest.raises(ValueError, match="descriptor must provide"):
        engine.transfer_peer_buffer_plan(plan)

    assert fake.unregistered == [tensor.data_ptr()]
    assert engine.registration_stats()["active_registrations"] == 0


@pytest.mark.parametrize("register_rc, expected_unregisters", [(0, 1), (-600, 0)])
def test_shutdown_only_unregisters_engine_owned_regions(register_rc, expected_unregisters):
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    fake = _FakeMooncake(register_rc=register_rc)
    engine._mooncake = fake  # noqa: SLF001
    tensor = torch.ones(16, dtype=torch.float32)
    engine.register_tensor_memory(tensor)

    engine.shutdown()

    assert len(fake.unregistered) == expected_unregisters
    assert engine.registration_stats()["active_registrations"] == 0
