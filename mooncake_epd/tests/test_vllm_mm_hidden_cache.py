from __future__ import annotations

import json
from pathlib import Path

import torch

from mooncake_epd.core.control.connector_metrics import ConnectorMetricsReader, ConnectorMetricsSink
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta
from mooncake_epd.core.state.vllm_mm_hidden_cache import VLLMMMHiddenStateCache


class _Visual:
    spatial_merge_size = 2
    out_hidden_size = 8
    dtype = torch.float32


def test_vllm_mm_hidden_cache_skips_second_vision_compute(monkeypatch, tmp_path):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES", "4")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES", str(1024 * 1024))
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S", "0")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))
    monkeypatch.setenv("MOONCAKE_EPD_ENGINE_ID", "test-prefill")
    monkeypatch.setenv("MOONCAKE_EPD_KV_ROLE", "kv_producer")

    cache = VLLMMMHiddenStateCache()
    pixel_values = torch.arange(3 * 4, dtype=torch.float32).reshape(3, 4)
    grid_thw = torch.tensor([[1, 2, 2], [1, 2, 2], [1, 2, 2]], dtype=torch.long)
    calls = {"count": 0}

    def compute():
        calls["count"] += 1
        return torch.full((3, 8), 7.0, dtype=torch.float32)

    first = cache.get_or_compute(
        pixel_values=pixel_values,
        grid_thw=grid_thw,
        compute_fn=compute,
        namespace="qwen3vl-test",
    )
    second = cache.get_or_compute(
        pixel_values=pixel_values.clone(),
        grid_thw=grid_thw.clone(),
        compute_fn=compute,
        namespace="qwen3vl-test",
    )

    assert calls["count"] == 1
    assert torch.equal(first, second)
    stats = cache.stats
    assert stats["lookups"] == 2
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["stores"] == 1

    payloads = [json.loads(path.read_text()) for path in tmp_path.glob("*.mm_hidden.json")]
    assert len(payloads) == 1
    assert payloads[0]["kind"] == "mm_hidden_cache"
    assert payloads[0]["metrics"]["hits"] == 1


def test_connector_metrics_reader_aggregates_hidden_cache_without_polluting_kv_workers(tmp_path):
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="epd-prefill",
        role="producer",
        pid=123,
    )
    sink.record(LayeredTransferWorkerMeta(grouped_batches=1, grouped_bytes=64))
    hidden_payload = {
        "version": 1,
        "kind": "mm_hidden_cache",
        "identity": {"pid": 456, "engine_id": "epd-prefill", "kv_role": "kv_producer"},
        "updated_at": 10.0,
        "metrics": {
            "enabled": True,
            "lookups": 3,
            "hits": 2,
            "misses": 1,
            "stores": 1,
            "evictions": 0,
            "bytes": 128,
            "entries": 1,
            "vision_compute_ms_total": 20.0,
            "cache_load_ms_total": 2.0,
            "hash_ms_total": 1.0,
            "errors": 0,
        },
    }
    (tmp_path / "mm-hidden-cache-epd-prefill-kv_producer-pid456.mm_hidden.json").write_text(
        json.dumps(hidden_payload), encoding="utf-8"
    )

    reader = ConnectorMetricsReader(tmp_path)
    kv = reader.aggregate()
    hidden = reader.aggregate_mm_hidden_cache()

    assert kv.workers == 1
    assert kv.totals.grouped_batches == 1
    assert hidden["workers"] == 1
    assert hidden["enabled_workers"] == 1
    assert hidden["lookups"] == 3
    assert hidden["hits"] == 2
    assert hidden["misses"] == 1
    assert hidden["hit_rate"] == 2 / 3
    assert hidden["vision_compute_ms_avg"] == 20.0
    assert hidden["cache_load_ms_avg"] == 1.0


def test_qwen3vl_hidden_cache_uses_stable_vllm_keys_per_image(monkeypatch, tmp_path):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_ENTRIES", "8")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_MAX_BYTES", str(1024 * 1024))
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S", "0")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))

    class Visual(_Visual):
        def __init__(self):
            self.calls: list[int] = []

        def __call__(self, pixel_values, *, grid_thw):
            self.calls.append(int(pixel_values.shape[0]))
            # merge=2 and each grid row has 4 input patches -> 1 output token.
            rows = int(grid_thw.shape[0])
            base = float(len(self.calls) * 10)
            return torch.stack(
                [torch.tensor([base + i, base + i + 0.5]) for i in range(rows)]
            )

    from mooncake_epd.core.state.vllm_mm_hidden_cache import (
        get_or_compute_qwen3vl_image_embeds,
        use_mm_hidden_cache_keys,
    )

    cache = VLLMMMHiddenStateCache()
    # Isolate the global cache used by the public wrapper for this test.
    import mooncake_epd.core.state.vllm_mm_hidden_cache as module

    module._GLOBAL_CACHE = cache
    visual = Visual()
    grid = torch.tensor([[1, 2, 2], [1, 2, 2]], dtype=torch.long)
    pixels = torch.arange(8 * 3, dtype=torch.float32).reshape(8, 3)

    with use_mm_hidden_cache_keys(["image-a", "image-b"]):
        first = get_or_compute_qwen3vl_image_embeds(
            pixel_values=pixels,
            grid_thw=grid,
            visual=visual,
            compute_fn=lambda: visual(pixels, grid_thw=grid),
        )
    with use_mm_hidden_cache_keys(["image-a", "image-b"]):
        second = get_or_compute_qwen3vl_image_embeds(
            pixel_values=pixels + 1,  # stable vLLM key, not tensor bytes, drives reuse
            grid_thw=grid,
            visual=visual,
            compute_fn=lambda: visual(pixels + 1, grid_thw=grid),
        )
    with use_mm_hidden_cache_keys(["image-a", "image-c"]):
        third = get_or_compute_qwen3vl_image_embeds(
            pixel_values=pixels + 2,
            grid_thw=grid,
            visual=visual,
            compute_fn=lambda: visual(pixels + 2, grid_thw=grid),
        )

    assert visual.calls == [8, 4]
    assert torch.equal(first, second)
    assert torch.equal(third[0], first[0])
    assert not torch.equal(third[1], first[1])
    stats = cache.stats
    assert stats["stable_key_lookups"] == 6
    assert stats["hits"] == 3
    assert stats["misses"] == 3
    assert stats["stores"] == 3
    assert stats["full_hit_batches"] == 1
    assert stats["partial_hit_batches"] == 1


def test_native_vllm_encoder_cache_hit_is_counted(monkeypatch, tmp_path):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S", "0")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))

    cache = VLLMMMHiddenStateCache()
    cache.record_native_encoder_cache_hit("stable-mm-hash")
    stats = cache.stats
    assert stats["lookups"] == 1
    assert stats["hits"] == 1
    assert stats["stable_key_lookups"] == 1
    assert stats["native_encoder_cache_hits"] == 1


def test_precomputed_image_embeds_hit_is_counted(monkeypatch, tmp_path):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_METRICS_INTERVAL_S", "0")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))

    cache = VLLMMMHiddenStateCache()
    cache.record_precomputed_image_embeds_hit(count=2, stable_keys=["a", "b"])
    stats = cache.stats
    assert stats["lookups"] == 2
    assert stats["hits"] == 2
    assert stats["stable_key_lookups"] == 2
    assert stats["precomputed_image_embeds_hits"] == 2
