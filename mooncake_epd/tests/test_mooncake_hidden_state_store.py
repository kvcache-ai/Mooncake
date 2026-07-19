from __future__ import annotations

import ctypes
import time

import torch

from mooncake_epd.core.state.hidden_cache_key import (
    HiddenStateCacheKeyV2,
    stable_multimodal_asset_hash,
    stable_multimodal_feature_id,
)
from mooncake_epd.core.state.mooncake_hidden_state_store import MooncakeHiddenStateStore


class _RegisteredBufferStore:
    """Pointer-only fake Store; catches accidental bytes/get API use."""

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
        result = []
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            payload = self.payloads[str(key)]
            if len(payload) != int(size):
                result.append(-1)
                continue
            ctypes.memmove(int(pointer), payload, int(size))
            result.append(int(size))
        return result

    def remove(self, key, _force=False):
        self.payloads.pop(str(key), None)
        return 0


def _key(**overrides) -> HiddenStateCacheKeyV2:
    values = {
        "asset_hash": "sha256:asset-a",
        "modality": "image",
        "model_id": "qwen-vl",
        "model_revision": "model-r1",
        "processor_revision": "processor-r1",
        "dtype": "torch.float32",
        "output_schema": "qwen3vl-image-embeds-v1",
        "grid_thw": (1, 2, 2),
        "relevant_kwargs": {"merge_size": 2, "output_tokens": 1},
    }
    values.update(overrides)
    return HiddenStateCacheKeyV2(**values)


def test_multimodal_feature_identity_is_preprocess_bound_but_preserves_legacy_asset_id():
    item = {
        "type": "image_url",
        "image_url": {"url": "data:image/png;base64,abc"},
    }
    legacy = stable_multimodal_asset_hash(item)

    assert stable_multimodal_feature_id(item) == legacy
    assert stable_multimodal_feature_id(item, {"max_pixels": 1_003_520}) != legacy
    assert stable_multimodal_feature_id(
        item,
        {"max_pixels": 1_003_520, "do_resize": True},
    ) == stable_multimodal_feature_id(
        item,
        {"do_resize": True, "max_pixels": 1_003_520},
    )


def test_exact_l2_reuses_across_workers_and_never_leaks_mutable_storage():
    backing = _RegisteredBufferStore()
    producer = MooncakeHiddenStateStore(backing)
    consumer = MooncakeHiddenStateStore(backing)
    key = _key()
    expected = torch.arange(12, dtype=torch.float32).reshape(3, 4)

    producer.put(key, expected, lease_seconds=30.0)
    first = consumer.get(key, target_device="cpu")
    assert first is not None
    first[0, 0] = -999.0
    second = consumer.get(key, target_device="cpu")
    assert second is not None
    assert torch.equal(second, expected)
    assert consumer.stats()["hits"] == 2
    assert not backing.registered


def test_l2_invalidates_model_processor_dtype_grid_and_schema_changes():
    base = _key()
    variants = [
        _key(model_revision="model-r2"),
        _key(processor_revision="processor-r2"),
        _key(dtype="torch.float16"),
        _key(grid_thw=(1, 1, 4)),
        _key(output_schema="qwen3vl-image-embeds-v2"),
    ]
    assert len({base.digest, *(item.digest for item in variants)}) == len(variants) + 1

    backing = _RegisteredBufferStore()
    store = MooncakeHiddenStateStore(backing)
    store.put(base, torch.ones((2, 2), dtype=torch.float32), lease_seconds=30.0)
    for item in variants:
        assert store.get(item, target_device="cpu") is None


def test_l2_rejects_checksum_corruption_and_expiry_without_returning_stale_data():
    backing = _RegisteredBufferStore()
    store = MooncakeHiddenStateStore(backing)
    key = _key()
    store.put(key, torch.ones((2, 2), dtype=torch.float32), lease_seconds=30.0)
    payload_key, _manifest_key = store._keys(key)
    backing.payloads[payload_key] = b"x" * len(backing.payloads[payload_key])
    assert store.get(key, target_device="cpu") is None
    assert store.stats()["checksum_failures"] == 1

    expiring = _key(asset_hash="sha256:asset-expiring")
    store.put(expiring, torch.ones((2, 2), dtype=torch.float32), lease_seconds=0.001)
    time.sleep(0.01)
    assert store.get(expiring, target_device="cpu") is None
    assert store.stats()["expired"] == 1
