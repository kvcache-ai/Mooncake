from __future__ import annotations

import time

import torch

from mooncake_epd.core.state import FeatureBundle, FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta


def _make_state_layer() -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-version",
    )
    return StateLayer(
        pm,
        RadixTree(pm, max_entries=64),
        FeatureStore(max_bytes=8 * 1024 * 1024, ttl_seconds=0.05),
        default_ttl_seconds=10.0,
    )


def _build_refs(sl: StateLayer, token_ids) -> list:
    refs = []
    for start in range(0, len(token_ids), sl.pm.page_size):
        chunk = token_ids[start : start + sl.pm.page_size]
        ref = sl.pm.allocate_page(filled=len(chunk))
        key = torch.randn(2, 2, len(chunk), 8, dtype=torch.float32)
        value = torch.randn_like(key)
        sl.pm.write_page_slots(ref, key, value, offset=0)
        refs.append(ref)
    return refs


def test_state_layer_version_lookup_and_snapshot_fork():
    sl = _make_state_layer()
    state0 = sl.register(
        kv_refs=_build_refs(sl, [1, 2, 3, 4]),
        feature_hash=None,
        meta=StateMeta(
            token_ids=[1, 2, 3, 4],
            workflow_id="wf-version",
            agent_id="agent-a",
        ),
    )
    matched, delta, _ = sl.advance_step_prepare(state0, [5, 6, 7, 8], [])
    assert delta == [5, 6, 7, 8]
    state1 = sl.advance_step_commit(
        state0,
        [5, 6, 7, 8],
        [],
        new_kv_refs=list(matched) + _build_refs(sl, [5, 6, 7, 8]),
        matched_pages=matched,
    )

    latest = sl.latest_workflow_version("wf-version")
    assert latest is state1
    snap0 = sl.get_state_version("wf-version", snapshot_epoch=0)
    assert snap0 is state0
    by_version = sl.get_state_version("wf-version", version_id=state1.version_id)
    assert by_version is state1

    forked = sl.fork_at_snapshot("wf-version", 0, child_agent_id="agent-b")
    assert forked.snapshot_epoch == 0
    assert forked.parent_version_id == state0.version_id
    assert forked.meta.agent_id == "agent-b"
    assert forked.meta.token_ids == state0.meta.token_ids

    sl.release(forked)
    sl.release(state1)
    sl.release(state0)
    sl.radix.clear()


def test_feature_store_refcount_lease_and_ttl_cleanup():
    store = FeatureStore(max_bytes=1024 * 1024, max_entries=4, ttl_seconds=0.05)
    bundle = FeatureBundle("img-x", torch.randn(2, 8))
    store.put("img-x", bundle)
    assert store.has("img-x")

    store.incref("img-x")
    store.lease("img-x")
    record = store.get_record("img-x")
    assert record is not None
    assert record.refcount == 1
    assert record.lease_count == 1

    time.sleep(0.08)
    # Pinned + leased entry must survive TTL expiry.
    assert store.has("img-x")

    store.release("img-x")
    assert store.refcount("img-x") == 0
    assert store.has("img-x")

    store.release_lease("img-x")
    swept = store.sweep_expired()
    assert swept in {0, 1}
    assert store.has("img-x") is False
