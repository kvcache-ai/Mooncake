from __future__ import annotations

from mooncake_epd.core.control.serving_controller import ServingControlPlane
from mooncake_epd.scripts.epd_encoder_service import _stable_mm_hash as _encoder_mm_hash
from mooncake_epd.scripts.build_qwen_vl_feature_handle_request import _stable_mm_hash


def test_feature_handle_request_builder_hash_matches_control_plane():
    item = {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}, "detail": "low"}
    req = {"messages": [{"role": "user", "content": [item, {"type": "text", "text": "x"}]}]}
    _, hashes = ServingControlPlane().classify_request(req)
    assert hashes == [_stable_mm_hash(item)]


def test_uuid_hash_matches_encoder_and_survives_payload_omission():
    full = {
        "type": "image_url",
        "image_url": {"url": "data:image/png;base64,AAAA"},
        "uuid": "asset-123",
    }
    compact = {
        "type": "image_url",
        "image_url": None,
        "uuid": "asset-123",
    }

    expected = ServingControlPlane._stable_mm_hash(full)
    assert expected == ServingControlPlane._stable_mm_hash(compact)
    assert expected == _stable_mm_hash(full)
    assert expected == _encoder_mm_hash(full)
    assert expected == _encoder_mm_hash(compact)
