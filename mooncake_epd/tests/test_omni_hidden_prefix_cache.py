from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import torch

from mooncake_epd.core.state.omni_hidden_prefix_cache import (
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
    install_qwen2_5_omni_hidden_prefix_cache,
)


class _FakeVisual:
    spatial_merge_size = 1


class _FakeAudioTower:
    def _get_feat_extract_output_lengths(self, feature_lens):
        return feature_lens, torch.clamp(feature_lens // 2, min=1)


class _FakeOmniThinker:
    def __init__(self):
        self.visual = _FakeVisual()
        self.audio_tower = _FakeAudioTower()
        self.config = SimpleNamespace(model_type="fake-qwen2_5_omni", _name_or_path="fake")
        self.image_calls = []
        self.audio_calls = []

    def get_image_features(self, pixel_values, image_grid_thw=None, **kwargs):
        self.image_calls.append(int(pixel_values.shape[0]))
        # Deterministic hidden states: preserve input identity in output.
        return SimpleNamespace(pooler_output=pixel_values.to(torch.float32)[:, :2].contiguous())

    def get_audio_features(self, input_features, feature_attention_mask=None, audio_feature_lengths=None, **kwargs):
        self.audio_calls.append(int(input_features.shape[0]))
        if feature_attention_mask is not None:
            lens = feature_attention_mask.sum(dim=1)
        else:
            lens = audio_feature_lengths
        out_lens = torch.clamp(lens // 2, min=1).tolist()
        chunks = []
        for idx, out_len in enumerate(out_lens):
            base = input_features[idx, :, : int(lens[idx])].mean().to(torch.float32)
            chunks.append(torch.full((int(out_len), 3), base.item(), dtype=torch.float32))
        return SimpleNamespace(last_hidden_state=torch.cat(chunks, dim=0))


def _cache() -> OmniHiddenPrefixCache:
    return OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(enabled=True, max_entries=16, max_bytes=1024 * 1024, store_on_gpu=False, allow_partial_prefix_reuse=True)
    )


def test_image_hidden_prefix_cache_reuses_prefix_and_computes_suffix_only():
    model = _FakeOmniThinker()
    cache = _cache()
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    grid = torch.tensor([[1, 2, 2], [1, 1, 2]], dtype=torch.long)
    first = torch.arange(24, dtype=torch.float32).reshape(6, 4)
    out1 = model.get_image_features(first, image_grid_thw=grid, return_dict=True).pooler_output
    assert model.image_calls == [6]

    second = first.clone()
    second[4:] += 1000  # same first image prefix, changed suffix image.
    out2 = model.get_image_features(second, image_grid_thw=grid, return_dict=True).pooler_output
    assert model.image_calls == [6, 2]
    assert torch.equal(out2[:4], out1[:4])
    assert torch.equal(out2[4:], second[4:, :2])

    stats = cache.stats
    assert stats["image_batches"] == 2
    assert stats["partial_hit_batches"] == 1
    assert stats["prefix_hit_items"] == 1
    assert stats["image_encoder_calls"] == 2

    out3 = model.get_image_features(second, image_grid_thw=grid, return_dict=True).pooler_output
    assert model.image_calls == [6, 2]
    assert torch.equal(out3, out2)
    assert cache.stats["full_hit_batches"] == 1


def test_audio_hidden_prefix_cache_reuses_prefix_and_subsets_masks():
    model = _FakeOmniThinker()
    cache = _cache()
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    feats = torch.zeros((2, 4, 8), dtype=torch.float32)
    feats[0] = 1.0
    feats[1] = 2.0
    mask = torch.tensor([[1, 1, 1, 1, 1, 1, 0, 0], [1, 1, 1, 1, 0, 0, 0, 0]], dtype=torch.long)
    out1 = model.get_audio_features(feats, feature_attention_mask=mask, return_dict=True).last_hidden_state
    assert model.audio_calls == [2]

    feats2 = feats.clone()
    feats2[1] = 5.0
    out2 = model.get_audio_features(feats2, feature_attention_mask=mask, return_dict=True).last_hidden_state
    assert model.audio_calls == [2, 1]
    assert torch.equal(out2[:3], out1[:3])  # first sample length floor(6/2)=3 cached.
    assert torch.all(out2[3:] == 5.0)
    assert cache.stats["audio_batches"] == 2
    assert cache.stats["partial_hit_batches"] == 1


def test_default_image_cache_uses_exact_batch_reuse_for_qwen25_safety():
    model = _FakeOmniThinker()
    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(enabled=True, max_entries=16, max_bytes=1024 * 1024, store_on_gpu=False)
    )
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    grid = torch.tensor([[1, 2, 2], [1, 1, 2]], dtype=torch.long)
    first = torch.arange(24, dtype=torch.float32).reshape(6, 4)
    second = first.clone()
    second[4:] += 1000

    out1 = model.get_image_features(first, image_grid_thw=grid, return_dict=True).pooler_output
    out2 = model.get_image_features(second, image_grid_thw=grid, return_dict=True).pooler_output
    out3 = model.get_image_features(second, image_grid_thw=grid, return_dict=True).pooler_output

    assert model.image_calls == [6, 6]
    assert torch.equal(out1, first[:, :2])
    assert torch.equal(out2, second[:, :2])
    assert torch.equal(out3, out2)
    stats = cache.stats
    assert stats["full_miss_batches"] == 2
    assert stats["full_hit_batches"] == 1
    assert stats["partial_hit_batches"] == 0


def test_install_prefix_cache_is_idempotent():
    model = _FakeOmniThinker()
    cache = _cache()
    first = install_qwen2_5_omni_hidden_prefix_cache(model, cache)
    second = install_qwen2_5_omni_hidden_prefix_cache(model, _cache())
    assert first is second


def test_exact_batch_cache_coalesces_concurrent_image_misses():
    class _SlowFakeOmniThinker(_FakeOmniThinker):
        def get_image_features(self, pixel_values, image_grid_thw=None, **kwargs):
            time.sleep(0.05)
            return super().get_image_features(pixel_values, image_grid_thw=image_grid_thw, **kwargs)

    model = _SlowFakeOmniThinker()
    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(enabled=True, max_entries=16, max_bytes=1024 * 1024, store_on_gpu=False)
    )
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    grid = torch.tensor([[1, 2, 2], [1, 1, 2]], dtype=torch.long)
    pixels = torch.arange(24, dtype=torch.float32).reshape(6, 4)
    barrier = threading.Barrier(4)
    outputs = []

    def _call():
        barrier.wait()
        outputs.append(model.get_image_features(pixels, image_grid_thw=grid, return_dict=True).pooler_output)

    threads = [threading.Thread(target=_call) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert len(outputs) == 4
    assert model.image_calls == [6]
    assert all(torch.equal(out, outputs[0]) for out in outputs)
    stats = cache.stats
    assert stats["image_encoder_calls"] == 1
    assert stats["coalesced_waits"] >= 1
    assert stats["coalesced_hits"] >= 1


def test_metrics_path_can_be_relative_filename(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = "omni_metrics.json"
    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=16,
            max_bytes=1024 * 1024,
            store_on_gpu=False,
            metrics_path=path,
            metrics_flush_interval_s=0.0,
        )
    )
    model = _FakeOmniThinker()
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)

    grid = torch.tensor([[1, 1, 2]], dtype=torch.long)
    pixels = torch.arange(8, dtype=torch.float32).reshape(2, 4)
    model.get_image_features(pixels, image_grid_thw=grid, return_dict=True)

    payload = json.loads((tmp_path / path).read_text(encoding="utf-8"))
    assert payload["kind"] == "omni_hidden_prefix_cache"
    assert payload["stores"] == 1
