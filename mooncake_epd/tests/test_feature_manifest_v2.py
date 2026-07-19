from __future__ import annotations

import pytest
import torch

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.core.state.feature_manifest_v2 import (
    FeatureManifestError,
    FeatureState,
    FeatureStateConflictError,
    FeatureStateManifestV2,
    FeatureStateRegistry,
)


def _descriptor(feature_id: str = "image-a"):
    return FeatureBundle(
        image_hash=feature_id,
        last_hidden=torch.ones((2, 4), dtype=torch.float32),
        grid_thw=torch.tensor([[1, 1, 2]], dtype=torch.long),
        metadata={"model_fingerprint": "model-a", "processor_fingerprint": "processor-a"},
    ).descriptor(checksum=True)


def _manifest(*, now: float = 10.0, lease_seconds: float = 30.0) -> FeatureStateManifestV2:
    return FeatureStateManifestV2.from_descriptor(
        _descriptor(),
        request_id="request-1",
        workflow_id="workflow-1",
        ordinal=0,
        attempt=0,
        producer_id="encoder-0",
        producer_generation="encoder-g1",
        consumer_id="prefill-0",
        consumer_generation="prefill-g2",
        vllm_adapter_version="vllm-0.23.0-adapter-1",
        lease_seconds=lease_seconds,
        now=now,
    )


def test_feature_state_manifest_round_trip_checksum_and_ready_validation():
    manifest = _manifest()
    restored = FeatureStateManifestV2.from_control_payload(manifest.as_control_payload())
    assert restored.checksum == manifest.checksum
    assert restored.state is FeatureState.DESCRIBED

    registry = FeatureStateRegistry()
    registry.register(restored)
    allocated = registry.transition(
        restored.manifest_id,
        FeatureState.ALLOCATED,
        expected_revision=0,
        transport_patch={"target_worker_id": "prefill-0", "remote_session": "session-1"},
    )
    transferring = registry.transition(
        restored.manifest_id,
        FeatureState.TRANSFERRING,
        expected_revision=allocated.revision,
    )
    ready = registry.transition(
        restored.manifest_id,
        FeatureState.READY,
        expected_revision=transferring.revision,
    )
    ready.validate_handle(
        feature_id="image-a",
        descriptor=_descriptor(),
        consumer_id="prefill-0",
        consumer_generation="prefill-g2",
        require_compatibility=True,
        now=11.0,
    )
    assert [entry.to_state for entry in registry.history(ready.manifest_id)] == [
        FeatureState.DESCRIBED,
        FeatureState.ALLOCATED,
        FeatureState.TRANSFERRING,
        FeatureState.READY,
    ]


def test_feature_state_manifest_rejects_tampering_and_wrong_consumer():
    manifest = _manifest()
    payload = manifest.as_control_payload()
    payload["transport"] = {"target_worker_id": "tampered"}
    with pytest.raises(FeatureManifestError, match="checksum"):
        FeatureStateManifestV2.from_control_payload(payload)

    registry = FeatureStateRegistry()
    registry.register(manifest)
    allocated = registry.transition(manifest.manifest_id, FeatureState.ALLOCATED)
    transferring = registry.transition(
        manifest.manifest_id,
        FeatureState.TRANSFERRING,
        expected_revision=allocated.revision,
    )
    ready = registry.transition(
        manifest.manifest_id,
        FeatureState.READY,
        expected_revision=transferring.revision,
    )
    with pytest.raises(FeatureManifestError, match="consumer mismatch"):
        ready.validate_handle(
            feature_id="image-a",
            descriptor=_descriptor(),
            consumer_id="prefill-wrong",
            now=11.0,
        )


def test_feature_state_registry_is_idempotent_and_cas_fenced():
    first = _manifest()
    same_identity = FeatureStateManifestV2.from_descriptor(
        _descriptor(),
        request_id="request-1",
        workflow_id="workflow-1",
        ordinal=0,
        attempt=0,
        producer_id="encoder-0",
        producer_generation="encoder-g1",
        consumer_id="prefill-0",
        consumer_generation="prefill-g2",
        vllm_adapter_version="vllm-0.23.0-adapter-1",
        now=10.0,
    )
    registry = FeatureStateRegistry()
    registered = registry.register(first)
    assert registry.register(same_identity) is registered
    with pytest.raises(FeatureStateConflictError, match="revision mismatch"):
        registry.transition(first.manifest_id, FeatureState.ALLOCATED, expected_revision=9)

    altered = FeatureStateManifestV2.from_descriptor(
        _descriptor(),
        request_id="request-1",
        workflow_id="workflow-1",
        ordinal=0,
        attempt=0,
        producer_id="encoder-0",
        producer_generation="encoder-g3",
        consumer_id="prefill-0",
        consumer_generation="prefill-g2",
        vllm_adapter_version="vllm-0.23.0-adapter-1",
        now=10.0,
    )
    with pytest.raises(FeatureStateConflictError, match="idempotency key conflicts"):
        registry.register(altered)


def test_feature_state_registry_expiry_aborts_then_releases():
    manifest = _manifest(now=5.0, lease_seconds=1.0)
    registry = FeatureStateRegistry()
    registry.register(manifest)
    assert registry.sweep_expired(now=5.5) == []
    assert registry.sweep_expired(now=6.0) == [manifest.manifest_id]
    released = registry.get(manifest.manifest_id)
    assert released.state is FeatureState.RELEASED
    assert [entry.to_state for entry in registry.history(manifest.manifest_id)] == [
        FeatureState.DESCRIBED,
        FeatureState.ABORTING,
        FeatureState.RELEASED,
    ]
