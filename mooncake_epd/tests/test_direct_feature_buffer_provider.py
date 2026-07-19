from __future__ import annotations

import threading
import time

import torch

from mooncake_epd.core.state import (
    DirectFeatureBufferRegistry,
    FeatureBundle,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    register_direct_feature_buffer_registry,
    unregister_direct_feature_buffer_registry,
)
from mooncake_epd.core.transfer import TransferEngine


class _CopyingMooncakeEngine:
    """In-process stand-in for Mooncake C++ direct writes.

    PyTorch wheels do not consistently expose from_address(), so the test
    registers pointer->tensor mappings and copies through those tensors while
    still exercising the production pointer plan contract.
    """

    def __init__(self, tensors_by_ptr):
        self.tensors_by_ptr = dict(tensors_by_ptr)
        self.registered = []
        self.unregistered = []

    def register_memory(self, ptr, nbytes):
        self.registered.append((int(ptr), int(nbytes)))
        return 0

    def unregister_memory(self, ptr):
        self.unregistered.append(int(ptr))
        return 0

    def transfer_sync_write(self, remote_session, local_ptr, remote_ptr, length):
        self._copy_one(local_ptr, remote_ptr, length)
        return 0

    def batch_transfer_sync_write(self, remote_session, local_ptrs, remote_ptrs, lengths):
        for src, dst, nbytes in zip(local_ptrs, remote_ptrs, lengths):
            self._copy_one(src, dst, nbytes)
        return 0

    def _copy_one(self, src: int, dst: int, nbytes: int) -> None:
        src_tensor = self.tensors_by_ptr[int(src)]
        dst_tensor = self.tensors_by_ptr[int(dst)]
        assert src_tensor.nelement() * src_tensor.element_size() == int(nbytes)
        assert dst_tensor.nelement() * dst_tensor.element_size() == int(nbytes)
        dst_tensor.copy_(src_tensor)


def _source_bundle() -> FeatureBundle:
    return FeatureBundle(
        image_hash="img-direct-provider",
        last_hidden=torch.arange(8, dtype=torch.float32).reshape(2, 4),
        intermediates=[(3, torch.arange(8, 16, dtype=torch.float32).reshape(2, 4))],
        grid_thw=torch.tensor([[1, 2, 1]], dtype=torch.int64),
        metadata={"model_fingerprint": "m", "processor_fingerprint": "p"},
    )


def test_epd_direct_handle_resolves_from_prefill_buffer_registry():
    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(worker_id="prefill-0", device="cpu")
    allocation = registry.allocate_for_descriptor(bundle.descriptor())
    register_direct_feature_buffer_registry(registry)
    try:
        tensors_by_ptr = {}
        engine = TransferEngine(protocol="tcp")
        for _, tensor in engine.feature_bundle_tensor_items(bundle):
            tensors_by_ptr[int(tensor.data_ptr())] = tensor
        for tensor in allocation.tensors.values():
            tensors_by_ptr[int(tensor.data_ptr())] = tensor
        engine.bind_mooncake_backend(_CopyingMooncakeEngine(tensors_by_ptr), initialized=True, owns_backend=False)
        plan = engine.build_feature_bundle_peer_buffer_plan(
            bundle,
            remote_session="prefill-session",
            remote_pointers=allocation.remote_pointers,
        )
        result = engine.transfer_feature_bundle_peer_buffer_plan(bundle, plan)
        assert result.nbytes == bundle.nbytes()

        handle = __import__("mooncake_epd.core.state", fromlist=["FeatureHandle"]).FeatureHandle(
            handle_id="direct-test",
            feature_id=bundle.image_hash,
            store_id="direct-store",
            uri=f"epd-direct://direct-store/{bundle.image_hash}",
            descriptor=bundle.descriptor(),
            metadata={
                "backend": "direct_engine",
                "direct_plan": {
                    "feature_id": plan.feature_id,
                    "targets": [
                        {"name": t.name, "remote_pointer": t.remote_pointer, "nbytes": t.nbytes}
                        for t in plan.targets
                    ],
                },
            },
        )
        provider = FeatureHandleProvider(FeatureHandleProviderConfig(worker_id="prefill-0", device="cpu", strict=True))
        resolved = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]}, device="cpu", dtype=torch.float32)

        assert resolved is not None
        # Provider packs main hidden + DeepStack hidden on the last dimension.
        assert torch.equal(resolved.image_embeds, torch.cat([bundle.last_hidden, bundle.intermediates[0][1]], dim=-1))
        assert torch.equal(resolved.image_grid_thw, bundle.grid_thw)
        assert registry.stats()["allocations"] == 1
    finally:
        unregister_direct_feature_buffer_registry("prefill-0")


def test_direct_feature_registry_singleflights_concurrent_allocation(monkeypatch):
    import mooncake_epd.core.state.direct_feature_buffer as direct_buffer

    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    original_allocate = direct_buffer._allocate_tensors_for_descriptor
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def slow_allocate(*args, **kwargs):
        calls.append(1)
        entered.set()
        release.wait(timeout=1.0)
        return original_allocate(*args, **kwargs)

    monkeypatch.setattr(direct_buffer, "_allocate_tensors_for_descriptor", slow_allocate)
    results = []
    errors = []

    def allocate() -> None:
        try:
            results.append(registry.allocate_for_descriptor(bundle.descriptor()))
        except Exception as exc:  # pragma: no cover - assertion below reports it
            errors.append(exc)

    first = threading.Thread(target=allocate)
    second = threading.Thread(target=allocate)
    first.start()
    assert entered.wait(timeout=1.0)
    second.start()
    time.sleep(0.05)
    release.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)

    assert not errors
    assert len(calls) == 1
    assert len(results) == 2
    assert results[0] is results[1]
    assert results[0].ref_count == 2


def test_direct_feature_registry_nonpersistent_release_honors_shared_refs():
    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
        persistent_cache=False,
    )

    first = registry.allocate_for_descriptor(bundle.descriptor())
    second = registry.allocate_for_descriptor(bundle.descriptor())
    assert first is second
    assert registry.stats()["ref_count"] == 2

    registry.mark_ready(bundle.image_hash)
    registry.release(bundle.image_hash)
    assert registry.stats()["allocations"] == 1
    assert registry.stats()["ref_count"] == 1

    registry.release(bundle.image_hash)
    assert registry.stats()["allocations"] == 0
    assert registry.stats()["bytes"] == 0


def test_direct_feature_target_reallocation_gets_a_new_allocation_incarnation():
    """A content-stable feature id must not identify a recycled GPU buffer."""

    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
        persistent_cache=False,
    )

    first = registry.allocate_for_descriptor(bundle.descriptor())
    first_target = first.as_direct_target()
    registry.mark_ready(bundle.image_hash)
    registry.release(bundle.image_hash)
    assert registry.get(bundle.image_hash) is None

    second = registry.allocate_for_descriptor(bundle.descriptor())
    second_target = second.as_direct_target()

    assert first.feature_id == second.feature_id
    assert first_target["allocation_id"] == first.allocation_id
    assert second_target["allocation_id"] == second.allocation_id
    assert second.allocation_id != first.allocation_id


def test_direct_feature_registry_lookup_can_atomically_reserve_bounded_leases():
    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
        persistent_cache=False,
    )
    registry.allocate_for_descriptor(bundle.descriptor())
    registry.mark_ready(bundle.image_hash)

    allocation = registry.lookup_ready(bundle.image_hash, lease_count=3)

    assert allocation is not None
    # One allocation owner plus three atomically reserved request references.
    assert registry.stats()["ref_count"] == 4
    for _ in range(4):
        registry.release(bundle.image_hash)
    assert registry.stats()["allocations"] == 0
