"""State-machine tests for async offload / restore lifecycle."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.agent.coordination.offload import OffloadManager  # noqa: E402
from mooncake_epd.core.state import FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta  # noqa: E402


DEVICE = torch.device("cpu")


def _make_state_layer():
    pm = PagedKVManager(
        page_size=8,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.bfloat16,
        device=DEVICE,
    )
    tree = RadixTree(pm, max_entries=64)
    fs = FeatureStore(max_bytes=8 * 1024 * 1024)
    return StateLayer(pm, tree, fs, default_ttl_seconds=10.0)


def _build_state(sl: StateLayer, token_count: int = 16):
    refs = []
    for start in range(0, token_count, sl.pm.page_size):
        fill = min(sl.pm.page_size, token_count - start)
        ref = sl.pm.allocate_page(filled=fill)
        k = torch.randn(
            sl.pm.num_layers,
            sl.pm.num_kv_heads,
            fill,
            sl.pm.head_dim,
            dtype=sl.pm.dtype,
            device=sl.pm.device,
        )
        v = torch.randn_like(k)
        sl.pm.write_page_slots(ref, k, v, offset=0)
        refs.append(ref)
    state = sl.register(
        kv_refs=refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=list(range(token_count)),
            workflow_id="wf-offload-sm",
            agent_id="agent-sm",
        ),
    )
    return state


def test_async_offload_transitions_to_offloaded():
    sl = _make_state_layer()
    state = _build_state(sl)
    om = OffloadManager(sl, DEVICE)

    handle = om.start_offload(state, expected_return_seconds=5.0, copy_delay_seconds=0.1)
    assert handle == state.state_id
    assert state.status == "OFFLOADING"
    assert len(state.kv_refs) > 0

    assert om.await_offload(handle, timeout=5.0)
    assert state.status == "OFFLOADED"
    assert state.kv_refs == []
    assert om.stats()["offloaded"] == 1

    restored = om.restore(handle)
    assert restored is state
    assert restored.status == "ACTIVE"
    assert len(restored.kv_refs) > 0

    sl.release(restored)
    sl.radix.clear()


def test_restore_during_offloading_aborts_inflight_dma():
    sl = _make_state_layer()
    state = _build_state(sl)
    original_refs = list(state.kv_refs)
    om = OffloadManager(sl, DEVICE)

    handle = om.start_offload(state, expected_return_seconds=5.0, copy_delay_seconds=0.2)
    assert state.status == "OFFLOADING"

    restored = om.restore(handle)
    assert restored is state
    assert state.status == "ACTIVE"
    assert state.kv_refs == original_refs
    assert om.stats()["aborted"] == 1
    assert om.stats()["offloaded"] == 0

    sl.release(state)
    sl.radix.clear()


def test_offload_holds_directory_leases_during_copy_window():
    sl = _make_state_layer()
    state = _build_state(sl)
    om = OffloadManager(sl, DEVICE)

    handle = om.start_offload(state, expected_return_seconds=5.0, copy_delay_seconds=0.2)
    time.sleep(0.05)
    assert sl.pm.kv_directory.stats()["leased_blocks"] > 0

    assert om.await_offload(handle, timeout=5.0)
    assert sl.pm.kv_directory.stats()["leased_blocks"] == 0

    restored = om.restore(handle)
    assert restored is state
    sl.release(state)
    sl.radix.clear()
