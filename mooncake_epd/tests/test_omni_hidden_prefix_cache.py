from __future__ import annotations

import json
import ctypes
import threading
import time
from types import SimpleNamespace

import torch

from mooncake_epd.core.state.omni_hidden_prefix_cache import (
    OmniHiddenPrefixCache,
    OmniHiddenPrefixCacheConfig,
    install_qwen2_5_omni_hidden_prefix_cache,
    use_omni_hidden_cache_keys,
)
from mooncake_epd.core.state.hidden_cache_policy import HiddenCachePolicy
from mooncake_epd.core.state.mooncake_hidden_state_store import MooncakeHiddenStateStore


class _FakeVisual:
    spatial_merge_size = 1
    dtype = torch.float32


class _RegisteredBufferStore:
    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}
        self.registered: set[int] = set()

    def register_buffer(self, pointer: int, _size: int) -> int:
        self.registered.add(int(pointer))
        return 0

    def unregister_buffer(self, pointer: int) -> int:
        self.registered.discard(int(pointer))
        return 0

    def batch_put_from(self, keys, pointers, sizes):
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            self.payloads[str(key)] = ctypes.string_at(int(pointer), int(size))
        return [0] * len(keys)

    def batch_get_into(self, keys, pointers, sizes):
        results = []
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            ctypes.memmove(int(pointer), self.payloads[str(key)], int(size))
            results.append(int(size))
        return results

    def remove(self, key, _force=False):
        self.payloads.pop(str(key), None)
        return 0


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
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=16,
            max_bytes=1024 * 1024,
            store_on_gpu=False,
            allow_partial_prefix_reuse=True,
            partial_oracle_validated=True,
            allow_tensor_hash_fallback=True,
        )
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
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=16,
            max_bytes=1024 * 1024,
            store_on_gpu=False,
            allow_tensor_hash_fallback=True,
        )
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


def test_default_omni_cache_requires_stable_identity_instead_of_hashing_full_tensor():
    model = _FakeOmniThinker()
    cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=16,
            max_bytes=1024 * 1024,
            store_on_gpu=False,
            # Default false is the production-safe behavior under test.
            allow_tensor_hash_fallback=False,
        )
    )
    install_qwen2_5_omni_hidden_prefix_cache(model, cache)
    grid = torch.tensor([[1, 1, 2]], dtype=torch.long)
    pixels = torch.arange(8, dtype=torch.float32).reshape(2, 4)

    # No content identity: exact caching is intentionally bypassed.
    model.get_image_features(pixels, image_grid_thw=grid, return_dict=True)
    model.get_image_features(pixels, image_grid_thw=grid, return_dict=True)
    assert model.image_calls == [2, 2]
    assert cache.stats["hash_ms_total"] == 0.0
    assert cache.stats["skipped_unkeyed_calls"] == 2

    # Stable ingest-time identity enables exact L1 reuse without tensor hashing.
    with use_omni_hidden_cache_keys(["sha256:asset-a"]):
        first = model.get_image_features(pixels, image_grid_thw=grid, return_dict=True).pooler_output
    with use_omni_hidden_cache_keys(["sha256:asset-a"]):
        second = model.get_image_features(pixels, image_grid_thw=grid, return_dict=True).pooler_output
    second[0, 0] = -1
    with use_omni_hidden_cache_keys(["sha256:asset-a"]):
        third = model.get_image_features(pixels, image_grid_thw=grid, return_dict=True).pooler_output
    assert model.image_calls == [2, 2, 2]
    assert torch.equal(first, pixels[:, :2])
    assert third[0, 0] == pixels[0, 0]
    assert cache.stats["stable_key_lookups"] >= 3


def test_omni_exact_l2_reuses_stable_batch_across_workers():
    policy = HiddenCachePolicy(
        enable_l1=True,
        enable_l2=True,
        model_id="qwen2.5-omni",
        model_revision="model-r1",
        processor_revision="processor-r1",
        output_schema="qwen2.5-omni-image-hidden-v1",
        lease_seconds=30.0,
    )
    backing = _RegisteredBufferStore()
    first_cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(enabled=True, max_entries=16, max_bytes=1024 * 1024),
        l2_store=MooncakeHiddenStateStore(backing),
        l2_policy=policy,
    )
    first_model = _FakeOmniThinker()
    install_qwen2_5_omni_hidden_prefix_cache(first_model, first_cache)
    grid = torch.tensor([[1, 1, 2]], dtype=torch.long)
    pixels = torch.arange(8, dtype=torch.float32).reshape(2, 4)
    with use_omni_hidden_cache_keys(["sha256:asset-a"]):
        first = first_model.get_image_features(
            pixels,
            image_grid_thw=grid,
            return_dict=True,
        ).pooler_output
    assert first_model.image_calls == [2]
    assert first_cache.stats["l2_stores"] == 1

    second_cache = OmniHiddenPrefixCache(
        OmniHiddenPrefixCacheConfig(enabled=True, max_entries=16, max_bytes=1024 * 1024),
        l2_store=MooncakeHiddenStateStore(backing),
        l2_policy=policy,
    )
    second_model = _FakeOmniThinker()
    install_qwen2_5_omni_hidden_prefix_cache(second_model, second_cache)
    with use_omni_hidden_cache_keys(["sha256:asset-a"]):
        second = second_model.get_image_features(
            pixels + 999,
            image_grid_thw=grid,
            return_dict=True,
        ).pooler_output
    assert second_model.image_calls == []
    assert torch.equal(second, first)
    assert second_cache.stats["l2_hits"] == 1


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
        OmniHiddenPrefixCacheConfig(
            enabled=True,
            max_entries=16,
            max_bytes=1024 * 1024,
            store_on_gpu=False,
            allow_tensor_hash_fallback=True,
        )
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
            allow_tensor_hash_fallback=True,
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
