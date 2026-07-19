from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import torch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mooncake_epd.core.state import FeatureBundle, unregister_direct_feature_buffer_registry


def test_decode_timing_flush_is_batched_off_the_first_token_path(monkeypatch, tmp_path):
    patch_path = Path(__file__).resolve().parents[2] / "sitecustomize.py"
    module_name = "mooncake_epd_sitecustomize_timing_test"
    spec = importlib.util.spec_from_file_location(module_name, patch_path)
    assert spec is not None and spec.loader is not None
    runtime_patch = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = runtime_patch
    spec.loader.exec_module(runtime_patch)
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path))
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_ROLE", "decode")
    monkeypatch.setenv("MOONCAKE_EPD_KV_ROLE", "kv_consumer")
    monkeypatch.setenv("MOONCAKE_EPD_DECODE_TIMING_FLUSH_INTERVAL_S", "0.1")

    old_stop = runtime_patch._DECODE_ENGINE_TIMING_FLUSH_STOP
    if old_stop is not None:
        old_stop.set()
    old_thread = runtime_patch._DECODE_ENGINE_TIMING_FLUSH_THREAD
    if old_thread is not None:
        old_thread.join(timeout=1.0)
    with runtime_patch._DECODE_ENGINE_TIMING_LOCK:
        for key in runtime_patch._DECODE_ENGINE_TIMING_METRICS:
            runtime_patch._DECODE_ENGINE_TIMING_METRICS[key] = 0.0 if key.endswith("_total") else 0
        runtime_patch._DECODE_ENGINE_TIMING_SEEN.clear()
        runtime_patch._DECODE_ENGINE_TIMING_LAST_FLUSH = 0.0
        runtime_patch._DECODE_ENGINE_TIMING_DIRTY_SEQUENCE = 0
        runtime_patch._DECODE_ENGINE_TIMING_FLUSHED_SEQUENCE = 0
        runtime_patch._DECODE_ENGINE_TIMING_FLUSH_THREAD = None
        runtime_patch._DECODE_ENGINE_TIMING_FLUSH_STOP = None

    runtime_patch._record_decode_engine_first_token(
        request_id="req-1",
        arrival_time_s=time.time() - 0.01,
        kv_transfer_params={"transfer_id": "kv-1"},
        output_tokens=1,
    )
    # The request path only updates in-memory counters. The periodic sink owns
    # the atomic JSON write after the configured flush interval.
    assert not list(tmp_path.glob("decode-engine-*.json"))

    deadline = time.time() + 2.0
    metrics_paths = []
    while time.time() < deadline:
        metrics_paths = list(tmp_path.glob("decode-engine-*.json"))
        if metrics_paths:
            break
        time.sleep(0.02)
    assert metrics_paths
    payload = json.loads(metrics_paths[0].read_text(encoding="utf-8"))
    assert payload["metrics"]["first_token_requests"] == 1
    assert payload["metrics"]["kv_first_token_requests"] == 1

    stop = runtime_patch._DECODE_ENGINE_TIMING_FLUSH_STOP
    if stop is not None:
        stop.set()
    thread = runtime_patch._DECODE_ENGINE_TIMING_FLUSH_THREAD
    if thread is not None:
        thread.join(timeout=1.0)


def test_embedded_direct_feature_lookup_offloads_registry_work(monkeypatch):
    """The production Prefill hook must not block its ASGI loop on lookup."""

    import mooncake_epd.core.transfer as transfer_module

    class FakeTransferEngine:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def initialize(self):
            return None

    monkeypatch.setattr(transfer_module, "TransferEngine", FakeTransferEngine)
    monkeypatch.setenv("MOONCAKE_EPD_ENABLE_DIRECT_FEATURE_BUFFER", "1")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_BUFFER_AUTH_TOKEN", "test-token")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID", "prefill-site-test")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_BUFFER_DEVICE", "cpu")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_REMOTE_SESSION", "prefill-test-session")
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_REGISTER_MEMORY", "0")

    patch_path = Path(__file__).resolve().parents[2] / "sitecustomize.py"
    module_name = "mooncake_epd_sitecustomize_direct_lookup_test"
    spec = importlib.util.spec_from_file_location(module_name, patch_path)
    assert spec is not None and spec.loader is not None
    runtime_patch = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = runtime_patch
    spec.loader.exec_module(runtime_patch)

    app = FastAPI()
    runtime_patch._install_direct_feature_buffer_routes(app)
    registry = app._mooncake_epd_direct_feature_buffer_registry
    bundle = FeatureBundle(
        image_hash="site-direct-lookup",
        last_hidden=torch.ones((1, 2), dtype=torch.float32),
    )
    registry.allocate_for_descriptor(bundle.descriptor(), zero_fill=False)
    registry.mark_ready(bundle.image_hash)

    original_to_thread = runtime_patch.asyncio.to_thread
    offloaded_calls: list[str] = []

    async def tracking_to_thread(func, /, *args, **kwargs):
        offloaded_calls.append(str(getattr(func, "__name__", "")))
        return await original_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(runtime_patch.asyncio, "to_thread", tracking_to_thread)
    try:
        with TestClient(app) as client:
            response = client.post(
                "/mooncake_epd/direct_feature_buffer/lookup",
                json={"feature_ids": [bundle.image_hash], "lease_count": 1},
                headers={"X-Mooncake-EPD-Token": "test-token"},
            )
        assert response.status_code == 200, response.text
        assert response.json()["server_lookup_ms"] >= 0.0
        assert "_lookup_ready_targets" in offloaded_calls
    finally:
        registry.release(bundle.image_hash)
        registry.release(bundle.image_hash)
        unregister_direct_feature_buffer_registry(registry.worker_id)
