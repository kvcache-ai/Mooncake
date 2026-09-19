import torch

from mooncake_epd.core.state import FeatureBundle, FeatureStore


def _bundle(key: str, *, elements: int = 16) -> FeatureBundle:
    return FeatureBundle(
        image_hash=key,
        last_hidden=torch.arange(elements, dtype=torch.float32),
    )


def _miss_then_put(
    store: FeatureStore,
    key: str,
    *,
    elements: int = 16,
    recompute_cost: float = 16.0,
) -> bool:
    assert store.get(key) is None
    return store.put(
        key,
        _bundle(key, elements=elements),
        recompute_cost=recompute_cost,
    )


def test_reuse_density_admission_keeps_hot_bundle_during_cold_scan():
    store = FeatureStore(
        max_bytes=128,
        max_entries=2,
        admission_policy="reuse_density",
        frequency_sample_size=100,
    )
    assert _miss_then_put(store, "hot") is True
    assert _miss_then_put(store, "cold-0") is True

    # A cache hit raises the hot bundle's expected saved-work density above
    # one-touch scan entries.
    assert store.get("hot") is not None

    assert _miss_then_put(store, "cold-1") is True
    assert store.has("hot") is True
    assert store.has("cold-0") is False

    assert _miss_then_put(store, "cold-2") is True
    assert store.has("hot") is True
    assert store.has("cold-1") is False
    stats = store.stats()
    assert stats["admission_policy"] == "reuse_density"
    assert stats["value_evictions"] == 2
    assert stats["bytes"] <= 128
    assert stats["entries"] <= 2


def test_reuse_density_admission_rejects_candidate_below_all_residents():
    store = FeatureStore(
        max_bytes=128,
        max_entries=2,
        admission_policy="reuse_density",
        frequency_sample_size=100,
    )
    assert _miss_then_put(store, "hot-a") is True
    assert _miss_then_put(store, "hot-b") is True
    assert store.get("hot-a") is not None
    assert store.get("hot-b") is not None

    assert _miss_then_put(store, "scan") is False
    assert store.has("hot-a") is True
    assert store.has("hot-b") is True
    assert store.has("scan") is False
    stats = store.stats()
    assert stats["admission_rejections"] == 1
    assert stats["entries"] == 2
    assert stats["bytes"] == 128


def test_reuse_density_admission_never_exceeds_capacity_when_resident_is_pinned():
    store = FeatureStore(
        max_bytes=64,
        max_entries=1,
        admission_policy="reuse_density",
        frequency_sample_size=100,
    )
    assert _miss_then_put(store, "pinned") is True
    store.incref("pinned")

    assert _miss_then_put(store, "candidate") is False
    assert store.has("pinned") is True
    assert store.has("candidate") is False
    stats = store.stats()
    assert stats["capacity_rejections"] == 1
    assert stats["entries"] == 1
    assert stats["bytes"] == 64


def test_reuse_density_admission_rejects_single_oversize_bundle():
    store = FeatureStore(
        max_bytes=64,
        max_entries=2,
        admission_policy="reuse_density",
    )

    assert _miss_then_put(store, "oversize", elements=32) is False
    stats = store.stats()
    assert stats["oversize_rejections"] == 1
    assert stats["entries"] == 0
    assert stats["bytes"] == 0


def test_lru_admission_rejects_single_oversize_bundle():
    store = FeatureStore(max_bytes=64, max_entries=2, admission_policy="lru")

    assert _miss_then_put(store, "oversize", elements=32) is False
    stats = store.stats()
    assert stats["oversize_rejections"] == 1
    assert stats["entries"] == 0
    assert stats["bytes"] == 0


def test_lru_admission_never_exceeds_capacity_when_resident_is_pinned():
    store = FeatureStore(max_bytes=64, max_entries=1, admission_policy="lru")
    assert _miss_then_put(store, "pinned") is True
    store.incref("pinned")

    assert _miss_then_put(store, "candidate") is False
    stats = store.stats()
    assert stats["capacity_rejections"] == 1
    assert stats["admissions"] == 1
    assert stats["entries"] == 1
    assert stats["bytes"] == 64
