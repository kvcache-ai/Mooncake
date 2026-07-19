from __future__ import annotations

import pytest

from mooncake_epd.core.state.kv_transfer_manifest_v2 import (
    KVTransferLayoutV2,
    KVTransferManifestError,
    KVTransferManifestV2,
)


def _layout() -> KVTransferLayoutV2:
    return KVTransferLayoutV2(
        model_id="Qwen3-VL-8B-Instruct",
        model_revision="revision-a",
        vllm_adapter_version="mooncake-epd-adapter-1",
        dtype="float16",
        block_size=16,
        num_layers=36,
        tp_size=1,
        tp_rank=0,
    )


def _manifest(*, now: float = 100.0) -> KVTransferManifestV2:
    return KVTransferManifestV2.create(
        transfer_id="transfer-1",
        workflow_id="workflow-1",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        layout=_layout(),
        remote_block_ids=[[10, 11], [12]],
        token_count=48,
        layer_groups=[[0, 18], [18, 36]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
        lease_seconds=30.0,
        now=now,
    )


def test_kv_transfer_manifest_round_trip_binds_consumer_and_layout():
    manifest = _manifest()
    restored = KVTransferManifestV2.from_control_payload(manifest.as_control_payload())

    restored.validate_for_consumer(
        transfer_id="transfer-1",
        workflow_id="workflow-1",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        require_compatibility=True,
        now=101.0,
    )
    assert restored.layout_checksum == _layout().checksum
    assert restored.remote_block_ids == ((10, 11), (12,))


def test_kv_transfer_manifest_rejects_tampering_expiry_and_wrong_target():
    manifest = _manifest()
    payload = manifest.as_control_payload()
    payload["remote_block_ids"] = [[999]]
    with pytest.raises(KVTransferManifestError, match="checksum"):
        KVTransferManifestV2.from_control_payload(payload)

    with pytest.raises(KVTransferManifestError, match="lease expired"):
        manifest.validate_for_consumer(now=130.0)
    with pytest.raises(KVTransferManifestError, match="destination_engine_id mismatch"):
        manifest.validate_for_consumer(destination_engine_id="decode-1", now=101.0)


def test_kv_transfer_manifest_rebind_preserves_source_lease_and_changes_target_fence():
    manifest = _manifest()
    rebound = manifest.rebind_destination(
        destination_engine_id="decode-1",
        destination_generation="decode-generation-2",
        now=101.0,
    )

    assert rebound.manifest_id != manifest.manifest_id
    assert rebound.lease_id == manifest.lease_id
    assert rebound.expires_at_unix_s == manifest.expires_at_unix_s
    assert rebound.attempt == manifest.attempt + 1
    rebound.validate_for_consumer(
        destination_engine_id="decode-1",
        destination_generation="decode-generation-2",
        now=101.0,
    )


def test_kv_transfer_manifest_can_require_non_unknown_generation_fences():
    unfenced = KVTransferManifestV2.create(
        transfer_id="transfer-unfenced",
        workflow_id="workflow-1",
        source_engine_id="epd-prefill",
        destination_engine_id="decode-0",
        layout=_layout(),
        remote_block_ids=[[10]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
        now=100.0,
    )

    with pytest.raises(KVTransferManifestError, match="generations are missing"):
        unfenced.validate_for_consumer(
            require_generation_fences=True,
            now=101.0,
        )


def test_kv_transfer_manifest_binds_the_current_producer_generation():
    manifest = _manifest()

    manifest.validate_for_producer(
        transfer_id="transfer-1",
        workflow_id="workflow-1",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        require_generation_fences=True,
        now=101.0,
    )

    with pytest.raises(KVTransferManifestError, match="source_generation mismatch"):
        manifest.validate_for_producer(
            transfer_id="transfer-1",
            source_engine_id="epd-prefill",
            source_generation="prefill-generation-restarted",
            require_generation_fences=True,
            now=101.0,
        )
