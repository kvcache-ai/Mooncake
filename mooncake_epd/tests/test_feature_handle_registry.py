from __future__ import annotations

import json
import time

import pytest
import torch

from mooncake_epd.core.state import (
    FeatureBundle,
    FeatureHandle,
    FeatureHandleError,
    FeatureHandleExpired,
    FeatureHandleRegistry,
    MMStore,
)
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy


def _bundle(feature_id: str = "img-handle") -> FeatureBundle:
    return FeatureBundle(
        image_hash=feature_id,
        last_hidden=torch.randn(2, 4, dtype=torch.float32),
        intermediates=[(8, torch.randn(2, 4, dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
        metadata={"model_fingerprint": "model-x", "processor_fingerprint": "proc-x"},
    )


def test_feature_handle_registry_publish_prefetch_and_validate():
    mm_store = MMStore(transfer_engine=TransferEngine(protocol="local"))
    try:
        registry = FeatureHandleRegistry(mm_store, store_id="store-a")
        handle = registry.publish_bundle(_bundle(), ttl_seconds=30.0, checksum=True)
        assert handle.uri == f"mmstore://store-a/{handle.feature_id}"
        assert handle.descriptor.feature_id == handle.feature_id
        payload = handle.as_control_payload()
        json.dumps(payload)
        handle = FeatureHandle.from_control_payload(payload)
        assert handle.descriptor.feature_id == handle.feature_id

        prefetch = registry.prefetch(
            handle,
            target_worker_id="prefill-0",
            target_device=torch.device("cpu"),
            policy=TransferPolicy(mode=Mode.PULL, channel=Channel.ENCODER_TO_PREFILL),
        )
        resolved = registry.wait_and_validate(
            handle,
            prefetch,
            timeout=5.0,
            expected_model_fingerprint="model-x",
            expected_processor_fingerprint="proc-x",
            require_checksum=True,
        )
        assert torch.equal(resolved.last_hidden, mm_store.lookup("prefill-0", handle.feature_id).last_hidden)
        assert registry.resolve_local(handle, worker_id="prefill-0") is not None
    finally:
        mm_store.stop()


def test_feature_handle_registry_rejects_wrong_store_and_expired_handle():
    mm_store = MMStore(transfer_engine=TransferEngine(protocol="local"))
    try:
        registry = FeatureHandleRegistry(mm_store, store_id="store-a")
        handle = registry.publish_bundle(_bundle(), ttl_seconds=0.001)
        wrong_store = FeatureHandle(
            handle_id=handle.handle_id,
            feature_id=handle.feature_id,
            store_id="store-b",
            uri=f"mmstore://store-b/{handle.feature_id}",
            descriptor=handle.descriptor,
        )
        with pytest.raises(FeatureHandleError, match="targets store"):
            registry.prefetch(wrong_store, target_worker_id="p", target_device=torch.device("cpu"))

        time.sleep(0.01)
        with pytest.raises(FeatureHandleExpired):
            registry.prefetch(handle, target_worker_id="p", target_device=torch.device("cpu"))
    finally:
        mm_store.stop()


def test_feature_handle_registry_rejects_descriptor_feature_mismatch():
    mm_store = MMStore(transfer_engine=TransferEngine(protocol="local"))
    try:
        registry = FeatureHandleRegistry(mm_store, store_id="store-a")
        handle = registry.publish_bundle(_bundle("img-a"))
        bad = FeatureHandle(
            handle_id=handle.handle_id,
            feature_id="img-b",
            store_id=handle.store_id,
            uri="mmstore://store-a/img-b",
            descriptor=handle.descriptor,
        )
        with pytest.raises(FeatureHandleError, match="does not match descriptor"):
            registry.prefetch(bad, target_worker_id="p", target_device=torch.device("cpu"))
    finally:
        mm_store.stop()


def test_feature_handle_registry_surfaces_descriptor_validation_failure():
    mm_store = MMStore(transfer_engine=TransferEngine(protocol="local"))
    try:
        registry = FeatureHandleRegistry(mm_store, store_id="store-a")
        handle = registry.publish_bundle(_bundle("img-a"), checksum=True)
        # Replace shared-store payload under the same key with incompatible tensor shape.
        mm_store.shared_store.put(
            "img-a",
            FeatureBundle(
                image_hash="img-a",
                last_hidden=torch.randn(3, 4, dtype=torch.float32),
                intermediates=[(8, torch.randn(2, 4, dtype=torch.float32))],
                grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                metadata={"model_fingerprint": "model-x", "processor_fingerprint": "proc-x"},
            ),
        )
        prefetch = registry.prefetch(handle, target_worker_id="prefill-1", target_device=torch.device("cpu"))
        with pytest.raises(ValueError, match="shape mismatch"):
            registry.wait_and_validate(handle, prefetch, timeout=5.0, require_checksum=True)
    finally:
        mm_store.stop()
