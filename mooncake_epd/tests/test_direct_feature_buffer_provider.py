from __future__ import annotations

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


def test_direct_target_exports_prefill_process_incarnation():
    bundle = _source_bundle()
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        remote_incarnation="prefill-process-a",
    )

    target = registry.allocate_for_descriptor(bundle.descriptor()).as_direct_target()

    assert target["remote_session"] == "prefill-session"
    assert target["remote_incarnation"] == "prefill-process-a"
    assert registry.stats()["remote_incarnation"] == "prefill-process-a"
    assert registry.stats()["remote_incarnation_present"] is True


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
