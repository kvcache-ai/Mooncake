from __future__ import annotations

import threading

import torch

from mooncake_epd.core.state import FeatureBundle, FeatureStore, MMStore
from mooncake_epd.core.transfer import TransferEngine


def test_mm_store_deduplicates_same_worker_prefetch():
    shared = FeatureStore(max_bytes=16 * 1024 * 1024)
    bundle = FeatureBundle(
        image_hash="img-dedup",
        last_hidden=torch.randn(2, 8),
        intermediates=[],
    )
    shared.put(bundle.image_hash, bundle)
    store = MMStore(shared, TransferEngine(protocol="local"))
    try:
        h1 = store.prefetch(
            "img-dedup",
            target_worker_id="worker-a",
            target_device=torch.device("cpu"),
        )
        h2 = store.prefetch(
            "img-dedup",
            target_worker_id="worker-a",
            target_device=torch.device("cpu"),
        )
        r1 = store.wait(h1, timeout=5.0)
        r2 = store.wait(h2, timeout=5.0)
        assert r1 is not None and r2 is not None
        assert h2.deduplicated is True
        stats = store.stats()
        assert stats["deduplicated_events"] >= 1
        assert stats["collapsed_waiters"] >= 1
        assert store.lookup("worker-a", "img-dedup") is not None
    finally:
        store.stop()


def test_mm_store_recompute_once_then_worker_cache_hit():
    shared = FeatureStore(max_bytes=16 * 1024 * 1024)
    store = MMStore(shared, TransferEngine(protocol="local"))
    calls = {"count": 0}
    lock = threading.Lock()

    def hook(image_hash: str) -> FeatureBundle:
        with lock:
            calls["count"] += 1
        return FeatureBundle(
            image_hash=image_hash,
            last_hidden=torch.ones(2, 4),
            intermediates=[],
        )

    store.register_recompute_hook("worker-b", hook)
    try:
        miss = store.prefetch(
            "img-miss",
            target_worker_id="worker-b",
            target_device=torch.device("cpu"),
        )
        result = store.wait(miss, timeout=5.0)
        assert result is not None
        assert miss.recomputed is True
        assert calls["count"] == 1

        # Second lookup should be satisfied by the worker-local cache.
        hit = store.prefetch(
            "img-miss",
            target_worker_id="worker-b",
            target_device=torch.device("cpu"),
        )
        assert hit.worker_cache_hit is True
        assert store.wait(hit, timeout=1.0) is not None
        assert calls["count"] == 1
        stats = store.stats()
        assert stats["recompute_fallbacks"] == 1
        assert stats["worker_cache_hits"] >= 1
    finally:
        store.stop()
