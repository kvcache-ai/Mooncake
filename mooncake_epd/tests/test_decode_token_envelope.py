from __future__ import annotations

import pytest

from mooncake_epd.core.control.decode_token_envelope import (
    DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION,
    DecodeTokenEnvelopeError,
    build_decode_token_envelope,
    validate_decode_token_envelope,
)


def _kv() -> dict:
    return {
        "transfer_id": "transfer-1",
        "remote_engine_id": "prefill-engine",
        "remote_bootstrap_addr": "http://prefill:8998",
        "remote_block_ids": [[11, 12], [21, 22]],
        "handoff_id": "handoff-1",
        "a2a_source_node": "prefill-0",
        "a2a_target_node": "decode-0",
        "source_generation": "prefill-generation-1",
        "destination_generation": "decode-generation-1",
    }


def _build():
    request = {
        "model": "/models/Qwen3-VL-8B-Instruct",
        "messages": [{"role": "user", "content": "describe"}],
        "max_tokens": 16,
        "temperature": 0.0,
        "stop": ["<stop>"],
    }
    prefill = {
        "prompt_token_ids": [1, 7, 9, 11],
        "mooncake_epd_prompt_envelope": {
            "version": 1,
            "prompt_token_ids": [1, 7, 9, 11],
            "mm_placeholders": {
                "image": [{"offset": 1, "length": 2}],
            },
            "mm_hashes": {"image": ["renderer-image-a"]},
            "mm_metadata": {
                "image": [{"image_grid_thw": [1, 2, 2]}],
            },
        },
        "usage": {
            "prompt_tokens": 4,
            "completion_tokens": 0,
            "total_tokens": 4,
        },
    }
    envelope = build_decode_token_envelope(
        api="/v1/chat/completions",
        request_body=request,
        prefill_response=prefill,
        kv_transfer_params=_kv(),
        prefill_worker_id="prefill-0",
        decode_worker_id="decode-0",
        multimodal_feature_hashes=["image-b", "image-a"],
    )
    return request, prefill, envelope


def test_decode_token_envelope_binds_prompt_controls_kv_and_workers():
    request, _, envelope = _build()

    assert envelope.payload["version"] == DECODE_TOKEN_ENVELOPE_PROTOCOL_VERSION
    assert envelope.prompt_token_ids == (1, 7, 9, 11)
    assert envelope.payload["multimodal_feature_hashes"] == [
        "image-a",
        "image-b",
    ]
    assert envelope.payload["multimodal_placeholder_spans"] == {
        "image": [{"offset": 1, "length": 2}]
    }
    assert envelope.payload["render_multimodal_hashes"] == {
        "image": ["renderer-image-a"]
    }
    assert envelope.payload["render_multimodal_metadata"] == {
        "image": [{"image_grid_thw": [1, 2, 2]}]
    }

    validated = validate_decode_token_envelope(
        envelope.to_dict(),
        request_body=request,
        kv_transfer_params=_kv(),
        expected_api="/v1/chat/completions",
        expected_prefill_worker_id="prefill-0",
        expected_decode_worker_id="decode-0",
        expected_multimodal_feature_hashes=["image-a", "image-b"],
    )
    assert validated.envelope_sha256 == envelope.envelope_sha256


@pytest.mark.parametrize(
    "mutation, expected",
    [
        (lambda raw: raw["prompt_token_ids"].append(12), "token count"),
        (
            lambda raw: raw.update({"decode_worker_id": "decode-stale"}),
            "checksum",
        ),
        (
            lambda raw: raw.update({"version": 999}),
            "protocol mismatch",
        ),
    ],
)
def test_decode_token_envelope_rejects_tampering(mutation, expected):
    request, _, envelope = _build()
    raw = envelope.to_dict()
    mutation(raw)

    with pytest.raises(DecodeTokenEnvelopeError, match=expected):
        validate_decode_token_envelope(
            raw,
            request_body=request,
            kv_transfer_params=_kv(),
            expected_api="/v1/chat/completions",
            expected_prefill_worker_id="prefill-0",
            expected_decode_worker_id="decode-0",
            expected_multimodal_feature_hashes=["image-a", "image-b"],
        )


def test_decode_token_envelope_rejects_stale_kv_binding():
    request, _, envelope = _build()
    stale_kv = _kv()
    stale_kv["handoff_id"] = "handoff-2"

    with pytest.raises(DecodeTokenEnvelopeError, match="KV binding"):
        validate_decode_token_envelope(
            envelope.to_dict(),
            request_body=request,
            kv_transfer_params=stale_kv,
            expected_api="/v1/chat/completions",
            expected_prefill_worker_id="prefill-0",
            expected_decode_worker_id="decode-0",
            expected_multimodal_feature_hashes=["image-a", "image-b"],
        )


def test_decode_token_envelope_rejects_prefill_usage_token_mismatch():
    request, prefill, _ = _build()
    prefill["usage"]["prompt_tokens"] = 5

    with pytest.raises(DecodeTokenEnvelopeError, match="token count"):
        build_decode_token_envelope(
            api="/v1/chat/completions",
            request_body=request,
            prefill_response=prefill,
            kv_transfer_params=_kv(),
            prefill_worker_id="prefill-0",
            decode_worker_id="decode-0",
        )


@pytest.mark.parametrize(
    "spans, expected",
    [
        ({"image": [{"offset": 3, "length": 2}]}, "exceeds"),
        (
            {
                "image": [
                    {"offset": 0, "length": 2},
                    {"offset": 1, "length": 2},
                ]
            },
            "overlap",
        ),
        ({"image": [{"offset": 0, "length": 0}]}, "length > 0"),
    ],
)
def test_decode_token_envelope_rejects_invalid_placeholder_spans(
    spans, expected
):
    request, prefill, _ = _build()
    prefill["mooncake_epd_prompt_envelope"]["mm_placeholders"] = spans

    with pytest.raises(DecodeTokenEnvelopeError, match=expected):
        build_decode_token_envelope(
            api="/v1/chat/completions",
            request_body=request,
            prefill_response=prefill,
            kv_transfer_params=_kv(),
            prefill_worker_id="prefill-0",
            decode_worker_id="decode-0",
            multimodal_feature_hashes=["image-a"],
        )


def test_decode_token_envelope_requires_mm_spans_for_multimodal_request():
    request, prefill, _ = _build()
    prefill.pop("mooncake_epd_prompt_envelope")

    with pytest.raises(
        DecodeTokenEnvelopeError,
        match="requires captured placeholder spans",
    ):
        build_decode_token_envelope(
            api="/v1/chat/completions",
            request_body=request,
            prefill_response=prefill,
            kv_transfer_params=_kv(),
            prefill_worker_id="prefill-0",
            decode_worker_id="decode-0",
            multimodal_feature_hashes=["image-a"],
        )
