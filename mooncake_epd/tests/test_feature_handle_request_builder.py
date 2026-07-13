from __future__ import annotations

from mooncake_epd.core.control.serving_controller import ServingControlPlane
from mooncake_epd.scripts.build_qwen_vl_feature_handle_request import _stable_mm_hash


def test_feature_handle_request_builder_hash_matches_control_plane():
    item = {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}, "detail": "low"}
    req = {"messages": [{"role": "user", "content": [item, {"type": "text", "text": "x"}]}]}
    _, hashes = ServingControlPlane().classify_request(req)
    assert hashes == [_stable_mm_hash(item)]
