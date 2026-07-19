from __future__ import annotations

import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("vllm")

import sitecustomize  # noqa: E402
from vllm.entrypoints.openai.completion.serving import (  # noqa: E402
    OpenAIServingCompletion,
)


def _sha256(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def test_decode_token_patch_restores_mm_identity_without_media():
    original = OpenAIServingCompletion.render_completion_request
    token_ids = [1, 2, 3, 4]
    spans = {"image": [{"offset": 1, "length": 2}]}
    mm_hashes = {"image": ["render-hash-0"]}
    mm_metadata = {"image": [{"image_grid_thw": [1, 2, 2]}]}

    async def fake_render(_self, _request):
        return [{"type": "token", "prompt_token_ids": list(token_ids)}]

    OpenAIServingCompletion.render_completion_request = fake_render
    try:
        sitecustomize._patch_vllm_decode_token_multimodal_metadata()
        request = SimpleNamespace(
            metadata={
                "mooncake_epd_decode_media_stripped": True,
                "mooncake_epd_decode_token_envelope": {
                    "kind": "mooncake_epd.decode_token_envelope",
                    "version": 1,
                    "prompt_token_count": len(token_ids),
                    "prompt_token_sha256": _sha256(token_ids),
                    "multimodal_placeholder_spans": spans,
                    "multimodal_placeholder_spans_sha256": _sha256(spans),
                    "render_multimodal_hashes": mm_hashes,
                    "render_multimodal_hashes_sha256": _sha256(mm_hashes),
                    "render_multimodal_metadata": mm_metadata,
                    "render_multimodal_metadata_sha256": _sha256(
                        mm_metadata
                    ),
                },
            }
        )

        result = asyncio.run(
            OpenAIServingCompletion.render_completion_request(None, request)
        )

        assert result[0]["type"] == "multimodal"
        assert result[0]["prompt_token_ids"] == token_ids
        assert result[0]["mm_hashes"] == mm_hashes
        assert result[0]["mm_placeholders"]["image"][0].offset == 1
        assert result[0]["mm_placeholders"]["image"][0].length == 2
        structural_data = result[0]["mm_kwargs"]["image"][0].get_data()
        assert set(structural_data) == {"image_grid_thw"}
        assert structural_data["image_grid_thw"].tolist() == [1, 2, 2]
        assert "messages" not in result[0]
        assert "media" not in json.dumps(
            {
                "tokens": result[0]["prompt_token_ids"],
                "hashes": result[0]["mm_hashes"],
            }
        )
    finally:
        OpenAIServingCompletion.render_completion_request = original
