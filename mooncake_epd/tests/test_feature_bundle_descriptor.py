from __future__ import annotations

import json

import pytest
import torch

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy


def test_feature_bundle_descriptor_validates_shape_dtype_and_checksum():
    bundle = FeatureBundle(
        image_hash="img-desc",
        last_hidden=torch.arange(12, dtype=torch.float32).reshape(3, 4),
        intermediates=[(2, torch.ones(3, 2, dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 2, 3]], dtype=torch.long),
        metadata={
            "model_fingerprint": "model-a",
            "processor_fingerprint": "processor-a",
            "non_json": object(),
        },
    )

    descriptor = bundle.descriptor(checksum=True)
    descriptor.validate_bundle(
        bundle,
        expected_model_fingerprint="model-a",
        expected_processor_fingerprint="processor-a",
        require_checksum=True,
    )
    assert descriptor.feature_id == "img-desc"
    assert descriptor.last_hidden.shape == (3, 4)
    assert descriptor.nbytes == bundle.nbytes()
    assert "non_json" not in descriptor.metadata
    payload = descriptor.to_dict()
    json.dumps(payload)
    restored = type(descriptor).from_dict(payload)
    restored.validate_bundle(bundle, require_checksum=True)

    corrupted = FeatureBundle(
        image_hash="img-desc",
        last_hidden=bundle.last_hidden.clone(),
        intermediates=[(2, torch.zeros(3, 2, dtype=torch.float32))],
        grid_thw=bundle.grid_thw.clone(),
        metadata=dict(bundle.metadata),
    )
    with pytest.raises(ValueError, match="checksum mismatch"):
        descriptor.validate_bundle(corrupted, require_checksum=True)


def test_feature_bundle_descriptor_survives_local_transfer_metadata_contract():
    src = FeatureBundle(
        image_hash="img-transfer",
        last_hidden=torch.randn(2, 8, dtype=torch.float32),
        intermediates=[(4, torch.randn(2, 8, dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 2, 4]], dtype=torch.long),
        metadata={"model_fingerprint": "m", "processor_fingerprint": "p"},
    )
    engine = TransferEngine(protocol="local")
    moved = engine.transfer_feature_bundle(
        src,
        torch.device("cpu"),
        TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
    )

    descriptor = moved.descriptor(checksum=False)
    descriptor.validate_bundle(
        moved,
        expected_model_fingerprint="m",
        expected_processor_fingerprint="p",
    )
    assert moved.metadata["model_fingerprint"] == "m"
    assert torch.equal(moved.last_hidden, src.last_hidden)
