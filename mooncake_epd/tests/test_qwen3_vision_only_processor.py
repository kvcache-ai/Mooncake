from __future__ import annotations

import base64
import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import torch
from fastapi.testclient import TestClient
from PIL import Image

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.scripts.epd_encoder_service import EncoderServiceConfig, create_app


def _png_data_url(color=(32, 64, 128)) -> str:
    image = Image.new("RGB", (4, 4), color=color)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _request_body(color=(32, 64, 128), prompt="describe this") -> dict:
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url(color)}},
                    {"type": "text", "text": prompt},
                ],
            }
        ]
    }


class _CountingImageProcessor:
    def __init__(self, owner):
        self.owner = owner

    def __call__(self, *, images, return_tensors):
        assert len(images) == 1
        assert isinstance(images[0], Image.Image)
        assert return_tensors == "pt"
        self.owner.image_processor_calls += 1
        return {
            "pixel_values": torch.arange(12, dtype=torch.float32).reshape(3, 4),
            "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
        }


class _VisionOnlyProcessor:
    def __init__(self):
        self.image_processor_calls = 0
        self.apply_chat_template_calls = 0
        self.image_processor = _CountingImageProcessor(self)

    def apply_chat_template(self, *args, **kwargs):
        del args, kwargs
        self.apply_chat_template_calls += 1
        raise AssertionError("vision-only processor path must not tokenize or render chat templates")


class _LegacyProcessor:
    def __init__(self):
        self.apply_chat_template_calls = 0

    def apply_chat_template(self, messages, **kwargs):
        assert messages[0]["content"][0]["type"] == "image"
        assert kwargs["tokenize"] is True
        assert kwargs["return_tensors"] == "pt"
        self.apply_chat_template_calls += 1
        return {
            "pixel_values": torch.arange(12, dtype=torch.float32).reshape(3, 4),
            "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
        }


class _MissingFieldsImageProcessor:
    def __init__(self, owner):
        self.owner = owner

    def __call__(self, *, images, return_tensors):
        del images, return_tensors
        self.owner.image_processor_calls += 1
        return {"pixel_values": torch.ones((3, 4), dtype=torch.float32)}


class _MissingFieldsProcessor:
    def __init__(self):
        self.image_processor_calls = 0
        self.apply_chat_template_calls = 0
        self.image_processor = _MissingFieldsImageProcessor(self)

    def apply_chat_template(self, *args, **kwargs):
        del args, kwargs
        self.apply_chat_template_calls += 1
        raise AssertionError("missing vision-only fields must fail closed, not fall back silently")


class _NonReentrantVisionOnlyProcessor:
    def __init__(self, registry):
        self.registry = registry
        self.image_processor = self
        self.active = False
        self.max_active = 0
        self.image_processor_calls = 0
        self.apply_chat_template_calls = 0
        self.lock = threading.Lock()
        registry.append(self)

    def __deepcopy__(self, memo):
        del memo
        return _NonReentrantVisionOnlyProcessor(self.registry)

    def __call__(self, *, images, return_tensors):
        assert len(images) == 1
        assert return_tensors == "pt"
        with self.lock:
            if self.active:
                raise RuntimeError("processor replica was re-entered")
            self.active = True
            self.max_active = max(self.max_active, 1)
            self.image_processor_calls += 1
        try:
            time.sleep(0.04)
            return {
                "pixel_values": torch.arange(12, dtype=torch.float32).reshape(3, 4),
                "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
            }
        finally:
            with self.lock:
                self.active = False

    def apply_chat_template(self, *args, **kwargs):
        del args, kwargs
        self.apply_chat_template_calls += 1
        raise AssertionError("replica pool vision-only path must not call chat templates")


class _FakeQwen3Encoder:
    def __init__(self, processor):
        self.processor = processor
        self.encode_calls = 0
        self.encode_inputs = []

    def encode(self, *, pixel_values, image_grid_thw, image_id=None, cache_key=None):
        self.encode_calls += 1
        self.encode_inputs.append(
            {
                "pixel_values": pixel_values.detach().clone(),
                "image_grid_thw": image_grid_thw.detach().clone(),
                "image_id": image_id,
                "cache_key": cache_key,
            }
        )

        class Out:
            pass

        out = Out()
        out.encode_time_ms = 1.0
        out.bundle = FeatureBundle(
            image_hash=image_id or "img",
            last_hidden=torch.ones((4, 8), dtype=torch.float32),
            intermediates=[(1, torch.full((4, 8), 2.0, dtype=torch.float32))],
            grid_thw=image_grid_thw.detach().cpu(),
            metadata={"kind": "qwen3_vision_only_fake"},
        )
        return out


def _config(**overrides) -> EncoderServiceConfig:
    values = {
        "publish_backend": "direct_engine",
        "device": "cpu",
        "enable_feature_bundle_cache": False,
        "qwen3_content_first_decode": True,
    }
    values.update(overrides)
    return EncoderServiceConfig(**values)


def test_enabled_vision_only_processor_uses_image_processor_without_chat_template():
    processor = _VisionOnlyProcessor()
    encoder = _FakeQwen3Encoder(processor)
    app = create_app(
        _config(enable_qwen3_vision_only_processor=True),
        encoder=encoder,
    )

    with TestClient(app) as client:
        response = client.post("/describe", json=_request_body())
        health = client.get("/health").json()
        cleanup = client.post("/discard_direct", json={"tickets": [response.json()["ticket"]]})

    assert response.status_code == 200
    assert cleanup.status_code == 200
    assert processor.image_processor_calls == 1
    assert processor.apply_chat_template_calls == 0
    assert encoder.encode_calls == 1
    assert torch.equal(encoder.encode_inputs[0]["image_grid_thw"], torch.tensor([[1, 2, 2]]))
    vision_stats = health["qwen3_preprocess_pipeline"]["vision_only_processor"]
    assert vision_stats == {"enabled": True, "calls": 1, "fallbacks": 0}


def test_disabled_vision_only_processor_keeps_legacy_chat_template_path():
    processor = _LegacyProcessor()
    encoder = _FakeQwen3Encoder(processor)
    app = create_app(
        _config(enable_qwen3_vision_only_processor=False),
        encoder=encoder,
    )

    with TestClient(app) as client:
        response = client.post("/describe", json=_request_body())
        health = client.get("/health").json()
        cleanup = client.post("/discard_direct", json={"tickets": [response.json()["ticket"]]})

    assert response.status_code == 200
    assert cleanup.status_code == 200
    assert processor.apply_chat_template_calls == 1
    vision_stats = health["qwen3_preprocess_pipeline"]["vision_only_processor"]
    assert vision_stats == {"enabled": False, "calls": 0, "fallbacks": 0}


def test_processor_replica_pool_uses_vision_only_path_without_reentry_and_returns_replicas():
    replicas = []
    processor = _NonReentrantVisionOnlyProcessor(replicas)
    encoder = _FakeQwen3Encoder(processor)
    app = create_app(
        _config(
            enable_qwen3_vision_only_processor=True,
            enable_qwen3_preprocess_executors=True,
            qwen3_media_workers=4,
            qwen3_media_max_pending=8,
            qwen3_processor_workers=4,
            qwen3_processor_max_pending=8,
        ),
        encoder=encoder,
    )
    bodies = [_request_body((index, 40, 80), "replica stress") for index in range(4)]

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=4) as pool:
        responses = [
            future.result(timeout=3.0)
            for future in [pool.submit(client.post, "/describe", json=body) for body in bodies]
        ]
        health = client.get("/health").json()
        cleanup = client.post(
            "/discard_direct",
            json={"tickets": [response.json()["ticket"] for response in responses]},
        )

    assert all(response.status_code == 200 for response in responses)
    assert cleanup.status_code == 200
    assert len(replicas) == 4
    assert all(replica.max_active == 1 for replica in replicas)
    assert sum(replica.image_processor_calls for replica in replicas) == 4
    assert sum(replica.apply_chat_template_calls for replica in replicas) == 0
    pool_stats = health["qwen3_preprocess_pipeline"]["processor_pool"]
    assert pool_stats["replicas"] == 4
    assert pool_stats["checkouts"] == 4
    assert pool_stats["failures"] == 0
    assert pool_stats["in_use"] == 0
    assert pool_stats["available"] == 4
    vision_stats = health["qwen3_preprocess_pipeline"]["vision_only_processor"]
    assert vision_stats == {"enabled": True, "calls": 4, "fallbacks": 0}


def test_enabled_vision_only_processor_falls_back_to_legacy_path_when_image_processor_api_missing():
    processor = _LegacyProcessor()
    encoder = _FakeQwen3Encoder(processor)
    app = create_app(
        _config(enable_qwen3_vision_only_processor=True),
        encoder=encoder,
    )

    with TestClient(app) as client:
        response = client.post("/describe", json=_request_body())
        health = client.get("/health").json()
        cleanup = client.post("/discard_direct", json={"tickets": [response.json()["ticket"]]})

    assert response.status_code == 200
    assert cleanup.status_code == 200
    assert processor.apply_chat_template_calls == 1
    vision_stats = health["qwen3_preprocess_pipeline"]["vision_only_processor"]
    assert vision_stats == {"enabled": True, "calls": 0, "fallbacks": 1}


def test_enabled_vision_only_processor_fails_closed_when_required_output_fields_are_missing():
    processor = _MissingFieldsProcessor()
    encoder = _FakeQwen3Encoder(processor)
    app = create_app(
        _config(enable_qwen3_vision_only_processor=True),
        encoder=encoder,
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/describe", json=_request_body())
        health = client.get("/health").json()

    assert response.status_code == 500
    assert "pixel_values/image_grid_thw" in response.text
    assert processor.image_processor_calls == 1
    assert processor.apply_chat_template_calls == 0
    assert encoder.encode_calls == 0
    vision_stats = health["qwen3_preprocess_pipeline"]["vision_only_processor"]
    assert vision_stats == {"enabled": True, "calls": 1, "fallbacks": 0}
