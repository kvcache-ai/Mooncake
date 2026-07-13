from __future__ import annotations

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
