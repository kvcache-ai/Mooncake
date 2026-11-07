"""Contract tests for the per-sequence state adapter (#4803).

These tests pin the adapter *contract* agreed in the issue discussion:

1. hit                  - put then get/is_exist round-trips byte-exactly
2. missing              - unknown keys report miss, no exceptions
3. expired (race)       - object vanishing between exists and get is a miss
4. layout-mismatch      - a different fingerprint is a clean miss
5. boundary-misaligned  - non-aligned lengths are rejected at the interface
6. namespace-collision  - adapter keys never collide with block KV keys
7. write visibility     - a key is observable only after put succeeds

They run against an in-memory backend so no Mooncake build is required;
the backend follows the same ``put/get/remove/is_exist`` surface that
``mooncake.store.MooncakeDistributedStore`` exposes.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Import the state adapter module directly from its file. The package
# __init__ selects a compiled backend (mooncake._store / mooncake.store.rs)
# which is not required by the adapter contract itself, and must not be
# required to run these contract tests.
_MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "mooncake" / "store" / "state_store.py"
)
_spec = importlib.util.spec_from_file_location("mooncake_store_state_store", _MODULE_PATH)
_state_store = importlib.util.module_from_spec(_spec)
sys.modules["mooncake_store_state_store"] = _state_store
_spec.loader.exec_module(_state_store)

from mooncake_store_state_store import (  # noqa: E402
    STATE_NAMESPACE,
    HitReport,
    StateAlignmentError,
    StateFingerprintError,
    StateLayout,
    StateSizeError,
    StateStoreAdapter,
)


class InMemoryStore:
    """Backend stub matching the MooncakeDistributedStore surface."""

    def __init__(self, *, evict_after_exists: bool = False) -> None:
        self.objects: dict[str, bytes] = {}
        self.evict_after_exists = False
        self._evict_once = evict_after_exists

    def put(self, key: str, value, config=None) -> int:
        self.objects[key] = bytes(value)
        return 0

    def get(self, key: str) -> bytes:
        value = self.objects.get(key)
        if value is None:
            # Mooncake surfaces a missing object as -704 / an error; the
            # adapter must treat anything it cannot return as a miss.
            raise RuntimeError(f"object missing ({-704})")
        return value

    def remove(self, key: str, force: bool = False) -> int:
        if key in self.objects:
            del self.objects[key]
            return 0
        return -704

    def is_exist(self, key: str) -> int:
        if key in self.objects:
            if self._evict_once:
                # Race under test: present at exists-time, gone at get-time.
                self.objects.pop(key)
                self._evict_once = False
                return 1
            return 1
        return 0


def make_layout() -> StateLayout:
    # Scaled-down DeepSeek V4.1-Flash shape: 2 layers x 64 B = 128 B slots.
    return StateLayout(
        layer_count=2,
        bytes_per_layer=64,
        boundary=128,
        scheme_version="v1",
        model_id="test-swa-model",
    )


def make_adapter(store=None, layout=None, fingerprint=None) -> StateStoreAdapter:
    layout = layout or make_layout()
    fingerprint = fingerprint or layout.fingerprint()
    return StateStoreAdapter(store or InMemoryStore(), layout, fingerprint)


def payload(layout: StateLayout, fill: int = 0xAB) -> bytes:
    return bytes([fill]) * layout.state_size


CHAIN = "deadbeef" * 8
ALIGNED = 256  # 2 x 128-token boundary


# -- 1. hit ---------------------------------------------------------------


def test_hit_round_trip():
    store = InMemoryStore()
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())
    data = payload(layout)

    assert adapter.put_state(CHAIN, ALIGNED, data) == 0
    assert adapter.is_available(CHAIN, ALIGNED) is True
    assert adapter.get_state(CHAIN, ALIGNED) == data


# -- 2. missing -----------------------------------------------------------


def test_missing_key_is_a_quiet_miss():
    adapter = make_adapter()
    assert adapter.is_available(CHAIN, ALIGNED) is False
    assert adapter.get_state(CHAIN, ALIGNED) is None


# -- 3. expired (race between exists and get) -----------------------------


def test_expired_race_is_a_miss():
    store = InMemoryStore(evict_after_exists=True)
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())
    adapter.put_state(CHAIN, ALIGNED, payload(layout))

    # exists says present, then the object is evicted before get.
    assert adapter.is_available(CHAIN, ALIGNED) is True
    assert adapter.get_state(CHAIN, ALIGNED) is None
    assert adapter.is_available(CHAIN, ALIGNED) is False


def test_size_mismatch_is_a_miss():
    store = InMemoryStore()

    class LiarStore(InMemoryStore):
        def get(self, key: str) -> bytes:
            return b"wrong-size"  # not state_size bytes

    liar = LiarStore()
    layout = make_layout()
    adapter = StateStoreAdapter(liar, layout, layout.fingerprint())
    liar.objects[adapter.state_key(CHAIN, ALIGNED)] = b"wrong-size"
    assert adapter.get_state(CHAIN, ALIGNED) is None


# -- 4. layout-mismatch ---------------------------------------------------


def test_layout_mismatch_is_a_clean_miss():
    layout = make_layout()
    fp_v1 = layout.fingerprint()

    changed = StateLayout(
        layer_count=4,  # layout change: more layers -> new fingerprint
        bytes_per_layer=64,
        boundary=128,
        scheme_version="v1",
        model_id="test-swa-model",
    )
    fp_changed = changed.fingerprint()
    assert fp_v1 != fp_changed, "layout change must change the fingerprint"

    store = InMemoryStore()
    old = StateStoreAdapter(store, layout, fp_v1)
    new = StateStoreAdapter(store, layout, fp_changed)

    old.put_state(CHAIN, ALIGNED, payload(layout))
    # The v1 object exists but is invisible under the changed layout:
    # clean miss, never a wrong-layout payload.
    assert new.get_state(CHAIN, ALIGNED) is None
    assert new.is_available(CHAIN, ALIGNED) is False


def test_fingerprint_must_be_versioned_and_known():
    layout = make_layout()
    store = InMemoryStore()
    with pytest.raises(StateFingerprintError):
        StateStoreAdapter(store, layout, "0123456789abcdef")  # no scheme
    with pytest.raises(StateFingerprintError):
        StateStoreAdapter(store, layout, "v9-0123456789abcdef")  # wrong scheme


# -- 5. boundary-misaligned -------------------------------------------------


def test_boundary_misaligned_is_rejected():
    adapter = make_adapter()
    layout = make_layout()

    for bad_length in (0, -128, 127, 129, 255):
        with pytest.raises(StateAlignmentError):
            adapter.state_key(CHAIN, bad_length)
        with pytest.raises(StateAlignmentError):
            adapter.put_state(CHAIN, bad_length, payload(layout))
        with pytest.raises(StateAlignmentError):
            adapter.get_state(CHAIN, bad_length)


def test_payload_size_is_validated_on_put():
    adapter = make_adapter()
    with pytest.raises(StateSizeError):
        adapter.put_state(CHAIN, ALIGNED, b"\x00" * 7)


# -- 6. namespace-collision -------------------------------------------------


def test_namespace_is_reserved_and_disjoint_from_block_kv():
    layout = make_layout()
    adapter = make_adapter(layout=layout, fingerprint=layout.fingerprint())
    key = adapter.state_key(CHAIN, ALIGNED)

    assert key.startswith(f"{STATE_NAMESPACE}/")
    # The reserved prefix cannot be produced by the engine's block KV
    # namespace, and block KV keys never leak into the state namespace.
    block_key = f"dsv4kv/{layout.fingerprint()}/g4/{CHAIN}"
    assert not block_key.startswith(f"{STATE_NAMESPACE}/")
    assert not key.startswith("dsv4kv/")


def test_adapter_and_block_kv_coexist_without_interference():
    store = InMemoryStore()
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())

    block_key = f"dsv4kv/{layout.fingerprint()}/g4/{CHAIN}"
    store.put(block_key, b"block-kv-bytes" * 8)

    adapter.put_state(CHAIN, ALIGNED, payload(layout))

    assert store.get(block_key) == b"block-kv-bytes" * 8
    assert adapter.get_state(CHAIN, ALIGNED) == payload(layout)
    assert adapter.is_available(CHAIN, ALIGNED) is True


# -- 7. write visibility ------------------------------------------------------


def test_availability_published_only_after_write():
    store = InMemoryStore()
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())

    key = adapter.state_key(CHAIN, ALIGNED)
    assert key not in store.objects
    assert adapter.is_available(CHAIN, ALIGNED) is False

    adapter.put_state(CHAIN, ALIGNED, payload(layout))
    assert key in store.objects
    assert adapter.is_available(CHAIN, ALIGNED) is True


# -- resolve_hit reports facts only ------------------------------------------


def test_resolve_hit_reports_independent_facts():
    store = InMemoryStore()
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())

    # In a real prefix chain the hash at boundary 128 differs from the hash
    # at boundary 256, so each candidate boundary has one chain hash.
    chain_at_128 = "aa" * 16
    chain_at_256 = "bb" * 16
    chain_at_384 = "cc" * 16
    adapter.put_state(chain_at_128, 128, payload(layout))
    adapter.put_state(chain_at_256, 256, payload(layout))

    report = adapter.resolve_hit(
        block_hit_length=512,
        state_candidates=[
            (chain_at_128, 128),
            (chain_at_256, 256),
            (chain_at_384, 384),
        ],
    )

    assert isinstance(report, HitReport)
    assert report.block_hit_length == 512
    assert report.state_available == {128: True, 256: True, 384: False}


def test_resolve_hit_rejects_misaligned_candidates():
    adapter = make_adapter()
    with pytest.raises(StateAlignmentError):
        adapter.resolve_hit(
            block_hit_length=512,
            state_candidates=[(CHAIN, 130)],
        )


def test_remove_state():
    store = InMemoryStore()
    layout = make_layout()
    adapter = StateStoreAdapter(store, layout, layout.fingerprint())
    adapter.put_state(CHAIN, ALIGNED, payload(layout))
    adapter.remove_state(CHAIN, ALIGNED)
    assert adapter.is_available(CHAIN, ALIGNED) is False
