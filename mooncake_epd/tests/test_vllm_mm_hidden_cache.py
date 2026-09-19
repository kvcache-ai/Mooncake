from __future__ import annotations

import json
import threading
import time
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
    assert stats["lookup_lock_hold_p95_ms"] is not None
    assert stats["cache_load_p95_ms"] is not None

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


def test_qwen3vl_stable_key_path_does_not_read_full_pixel_bytes(monkeypatch, tmp_path):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))
    cache = VLLMMMHiddenStateCache()

    class Visual(_Visual):
        def __call__(self, pixel_values, *, grid_thw):
            return torch.ones((int(grid_thw.shape[0]), 2), device=pixel_values.device)

    visual = Visual()
    grid = torch.tensor([[1, 2, 2]], dtype=torch.long)
    pixels = torch.arange(12, dtype=torch.float32).reshape(4, 3)
    original = cache._tensor_raw_bytes

    def _guard(tensor):
        if tensor.data_ptr() == pixels.data_ptr():
            raise AssertionError("stable key path read full pixel bytes")
        return original(tensor)

    monkeypatch.setattr(cache, "_tensor_raw_bytes", _guard)
    output = cache.get_or_compute_qwen3vl_items(
        pixel_values=pixels,
        grid_thw=grid,
        visual=visual,
        compute_fn=lambda: torch.ones((1, 2)),
        namespace="qwen3vl-test",
        stable_keys=["source-sha256"],
    )

    assert tuple(output.shape) == (1, 2)
    assert cache.stats["stable_key_lookups"] == 1
    assert cache.stats["tensor_key_lookups"] == 0


def test_sampled_tensor_candidate_collision_is_full_verified(monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE_HASH_SAMPLE_BYTES", "16")
    cache = VLLMMMHiddenStateCache()
    grid = torch.tensor([[1, 10, 10]], dtype=torch.long)
    first = torch.zeros(100, dtype=torch.float32)
    second = first.clone()
    second[50] = 1.0

    key_a = cache._tensor_key(pixel_values=first, grid_thw=grid, namespace="test")
    key_b = cache._tensor_key(pixel_values=second, grid_thw=grid, namespace="test")

    assert key_a != key_b
    assert cache.stats["collision_full_verifies"] == 2
    assert cache.stats["sampled_candidate_collisions"] == 1


def test_qwen3vl_hidden_cache_coalesces_concurrent_stable_key_misses(monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    cache = VLLMMMHiddenStateCache()
    barrier = threading.Barrier(4)
    call_lock = threading.Lock()

    class SlowVisual(_Visual):
        def __init__(self):
            self.calls = 0

        def __call__(self, pixel_values, *, grid_thw):
            with call_lock:
                self.calls += 1
            time.sleep(0.05)
            return torch.ones((int(grid_thw.shape[0]), 2))

    visual = SlowVisual()
    grid = torch.tensor([[1, 2, 2]], dtype=torch.long)
    pixels = torch.arange(12, dtype=torch.float32).reshape(4, 3)
    outputs = []
    errors = []

    def _call():
        try:
            barrier.wait()
            outputs.append(
                cache.get_or_compute_qwen3vl_items(
                    pixel_values=pixels,
                    grid_thw=grid,
                    visual=visual,
                    compute_fn=lambda: visual(pixels, grid_thw=grid),
                    namespace="qwen3vl-test",
                    stable_keys=["same-source"],
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_call) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert errors == []
    assert len(outputs) == 4
    assert visual.calls == 1
    assert all(torch.equal(output, outputs[0]) for output in outputs)
    stats = cache.stats
    assert stats["vision_encoder_calls"] == 1
    assert stats["coalesced_waits"] >= 1
    assert stats["coalesced_hits"] >= 1


def test_qwen3vl_singleflight_releases_waiters_when_owner_compute_fails(monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    cache = VLLMMMHiddenStateCache()
    barrier = threading.Barrier(2)
    call_lock = threading.Lock()

    class FailOnceVisual(_Visual):
        def __init__(self):
            self.calls = 0

        def __call__(self, pixel_values, *, grid_thw):
            with call_lock:
                self.calls += 1
                call_number = self.calls
            time.sleep(0.03)
            if call_number == 1:
                raise RuntimeError("injected vision failure")
            return torch.ones((int(grid_thw.shape[0]), 2))

    visual = FailOnceVisual()
    grid = torch.tensor([[1, 2, 2]], dtype=torch.long)
    pixels = torch.arange(12, dtype=torch.float32).reshape(4, 3)
    outputs = []
    errors = []

    def _call():
        try:
            barrier.wait()
            outputs.append(
                cache.get_or_compute_qwen3vl_items(
                    pixel_values=pixels,
                    grid_thw=grid,
                    visual=visual,
                    compute_fn=lambda: visual(pixels, grid_thw=grid),
                    namespace="qwen3vl-test",
                    stable_keys=["same-source"],
                )
            )
        except RuntimeError as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=_call) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert len(errors) == 1
    assert len(outputs) == 1
    assert visual.calls == 2
    assert cache._inflight == {}
    retry = cache.get_or_compute_qwen3vl_items(
        pixel_values=pixels,
        grid_thw=grid,
        visual=visual,
        compute_fn=lambda: visual(pixels, grid_thw=grid),
        namespace="qwen3vl-test",
        stable_keys=["same-source"],
    )
    assert torch.equal(retry, outputs[0])
    assert visual.calls == 2


def test_qwen3vl_singleflight_opposite_item_order_does_not_deadlock(monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    cache = VLLMMMHiddenStateCache()
    barrier = threading.Barrier(2)
    call_lock = threading.Lock()

    class SlowVisual(_Visual):
        def __init__(self):
            self.calls = 0

        def __call__(self, pixel_values, *, grid_thw):
            with call_lock:
                self.calls += 1
            time.sleep(0.04)
            return torch.arange(int(grid_thw.shape[0]) * 2, dtype=torch.float32).reshape(-1, 2)

    visual = SlowVisual()
    grid = torch.tensor([[1, 2, 2], [1, 2, 2]], dtype=torch.long)
    pixels = torch.arange(24, dtype=torch.float32).reshape(8, 3)
    outputs = []
    errors = []

    def _call(keys):
        try:
            barrier.wait()
            outputs.append(
                cache.get_or_compute_qwen3vl_items(
                    pixel_values=pixels,
                    grid_thw=grid,
                    visual=visual,
                    compute_fn=lambda: visual(pixels, grid_thw=grid),
                    namespace="qwen3vl-test",
                    stable_keys=keys,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=_call, args=(["a", "b"],)),
        threading.Thread(target=_call, args=(["b", "a"],)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert errors == []
    assert len(outputs) == 2
    assert visual.calls == 1
    assert cache._inflight == {}


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
