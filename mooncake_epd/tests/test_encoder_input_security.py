from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, HTTPException

from mooncake_epd.scripts.epd_encoder_service import (
    EncoderServiceConfig,
    _load_url_bytes,
)


def _app(config: EncoderServiceConfig) -> FastAPI:
    app = FastAPI()
    app.state.config = config
    return app


def test_encoder_rejects_local_files_and_http_by_default(tmp_path):
    image = tmp_path / "secret.png"
    image.write_bytes(b"secret")
    app = _app(EncoderServiceConfig(device="cpu"))

    with pytest.raises(HTTPException, match="file image URLs are disabled"):
        asyncio.run(_load_url_bytes(app, image.as_uri(), max_bytes=1024))
    with pytest.raises(HTTPException, match="HTTP image URLs are disabled"):
        asyncio.run(_load_url_bytes(app, "https://example.test/image.png", max_bytes=1024))


def test_encoder_local_input_is_root_bounded_and_rejects_path_escape(tmp_path):
    root = tmp_path / "images"
    root.mkdir()
    allowed = root / "ok.png"
    allowed.write_bytes(b"ok")
    secret = tmp_path / "secret.png"
    secret.write_bytes(b"secret")
    config = EncoderServiceConfig(
        device="cpu",
        allow_file_images=True,
        allowed_image_root=str(root),
    )
    app = _app(config)

    payload, _ = asyncio.run(_load_url_bytes(app, allowed.as_uri(), max_bytes=1024))
    assert payload == b"ok"
    with pytest.raises(HTTPException, match="outside the allowed image root"):
        asyncio.run(_load_url_bytes(app, secret.as_uri(), max_bytes=1024))


def test_encoder_http_allowlist_rejects_unapproved_and_private_ip_hosts():
    config = EncoderServiceConfig(
        device="cpu",
        allow_http_images=True,
        allowed_http_hosts=("images.example.test", "127.0.0.1"),
    )
    app = _app(config)
    with pytest.raises(HTTPException, match="not allowlisted"):
        asyncio.run(_load_url_bytes(app, "https://other.example.test/image.png", max_bytes=1024))
    with pytest.raises(HTTPException, match="not publicly routable"):
        asyncio.run(_load_url_bytes(app, "http://127.0.0.1/image.png", max_bytes=1024))
