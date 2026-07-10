from __future__ import annotations

import base64
import io
from types import SimpleNamespace

import torch
from fastapi.testclient import TestClient
from PIL import Image

from mooncake_epd.core.omni_encoder_worker import Qwen25OmniImageEncoderWorker
from mooncake_epd.core.state import FeatureHandleProvider, FeatureHandleProviderConfig
from mooncake_epd.scripts.epd_encoder_service import EncoderServiceConfig, create_app


class _FakeProcessor:
    name_or_path = "fake-omni-processor"

    def apply_chat_template(self, messages, add_generation_prompt=True, tokenize=False, **kwargs):
        if tokenize:
            return self(text=["fake"], images=[Image.new("RGB", (4, 4))], return_tensors="pt", padding=True)
        return "fake chat"

    def __call__(self, *, text, images, return_tensors="pt", padding=True):
        chunks = []
        grids = []
        for idx, image in enumerate(images):
            if idx == 0:
                n = 4
                grid = [1, 2, 2]
            else:
                n = 2
                grid = [1, 1, 2]
            base = float(sum(image.convert("RGB").getpixel((0, 0))))
            chunks.append(torch.full((n, 4), base + idx, dtype=torch.float32))
            grids.append(grid)
        return {
            "pixel_values": torch.cat(chunks, dim=0),
            "image_grid_thw": torch.tensor(grids, dtype=torch.long),
        }


class _FakeOmniModel:
    def __init__(self):
        self.config = SimpleNamespace(model_type="qwen2_5_omni", _name_or_path="fake-omni")
        self.visual = SimpleNamespace(spatial_merge_size=1)
        self.calls = 0

    def get_image_features(self, pixel_values, image_grid_thw=None, return_dict=True, **kwargs):
        self.calls += 1
        return SimpleNamespace(pooler_output=pixel_values[:, :2].contiguous())


def _image(color):
    return Image.new("RGB", (8, 8), color=color)


def _data_url(color) -> str:
    buf = io.BytesIO()
    _image(color).save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _request() -> dict:
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _data_url((10, 20, 30))}},
                    {"type": "image_url", "image_url": {"url": _data_url((40, 50, 60))}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }


def test_qwen25_omni_worker_exact_hidden_segment_cache_hits_on_repeat():
    model = _FakeOmniModel()
    worker = Qwen25OmniImageEncoderWorker(model, _FakeProcessor(), torch.device("cpu"))
    images = [_image((10, 20, 30)), _image((40, 50, 60))]

    first = worker.encode_images(images, image_ids=["a", "b"], prompt="describe")
    second = worker.encode_images(images, image_ids=["a", "b"], prompt="describe")

    assert model.calls == 1
    assert len(first.outputs) == 2
    assert len(second.outputs) == 2
    assert second.cache_stats["full_hit_batches"] >= 1
    assert second.cache_stats["image_encoder_calls"] == 1
    assert torch.equal(first.outputs[0].bundle.last_hidden, second.outputs[0].bundle.last_hidden)
    assert first.outputs[0].bundle.metadata["kind"] == "qwen2_5_omni_image_hidden_state"


def test_encoder_service_uses_omni_batch_cache_and_publishes_handles(tmp_path):
    model = _FakeOmniModel()
    worker = Qwen25OmniImageEncoderWorker(model, _FakeProcessor(), torch.device("cpu"))
    app = create_app(
        EncoderServiceConfig(
            model="fake-omni",
            device="cpu",
            encoder_family="qwen2_5_omni",
            store_dir=str(tmp_path / "store"),
        ),
        encoder=worker,
    )

    with TestClient(app) as client:
        r1 = client.post("/encode", json=_request())
        assert r1.status_code == 200, r1.text
        p1 = r1.json()
        r2 = client.post("/encode", json=_request())
        assert r2.status_code == 200, r2.text
        p2 = r2.json()

    assert model.calls == 1
    assert p1["count"] == 2
    assert p2["count"] == 2
    assert p2["encoder_family"] == "qwen2_5_omni"
    assert p2["omni_hidden_prefix_cache"]["feature_bundle_cache"]["hits"] >= 2
    provider = FeatureHandleProvider(FeatureHandleProviderConfig(store_dirs=(tmp_path / "store",)))
    resolved = provider.resolve_from_sources({"mm_feature_handles": [p2["handles"][0]]}, device="cpu", dtype=torch.float32)
    assert resolved is not None
    assert tuple(resolved.image_embeds.shape) == (4, 2)
    assert p2["handles"][0]["descriptor"]["metadata"]["kind"] == "qwen2_5_omni_image_hidden_state"
