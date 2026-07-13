from __future__ import annotations

import torch

from mooncake_epd.agent.coordination.offload import OffloadManager
from mooncake_epd.core.state import FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta


def _make_state_layer(*, handoff_wal_path: str | None = None) -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-wal",
    )
    return StateLayer(
        pm,
        RadixTree(pm, max_entries=64),
        FeatureStore(max_bytes=8 * 1024 * 1024),
        default_ttl_seconds=10.0,
        handoff_wal_path=handoff_wal_path,
    )


def _build_state(sl: StateLayer, workflow_id: str = "wf-wal"):
    ref = sl.pm.allocate_page(filled=4)
    key = torch.randn(2, 2, 4, 8, dtype=torch.float32)
    value = torch.randn_like(key)
    sl.pm.write_page_slots(ref, key, value, offset=0)
    return sl.register(
        kv_refs=[ref],
        feature_hash=None,
        meta=StateMeta(
            token_ids=[1, 2, 3, 4],
            workflow_id=workflow_id,
            agent_id="agent-a",
        ),
    )


def test_handoff_wal_recovery_rolls_back_prepared_transaction(tmp_path):
    wal_path = tmp_path / "handoff.jsonl"
    sl = _make_state_layer(handoff_wal_path=str(wal_path))
    state = _build_state(sl)

    sl.handoff_prepare(state, "agent-b", target_node_id="node-b")
    handoff = sl.pm.kv_directory.handoff_states()
    assert handoff
    assert state.status == "HANDING_OVER"

    sl_recovered = StateLayer(
        sl.pm,
        sl.radix,
        sl.features,
        default_ttl_seconds=10.0,
        handoff_wal_path=str(wal_path),
    )
    sl_recovered._states[state.state_id] = state  # reattach state object for recovery semantics
    result = sl_recovered.recover_handoffs()
    assert result
    assert state.status == "ACTIVE"
    assert sl.pm.kv_directory.handoff_states() == {}


def test_offload_wal_recovery_restores_from_checkpoint(tmp_path):
    wal_path = tmp_path / "offload.jsonl"
    spill_dir = tmp_path / "spill"
    sl = _make_state_layer()
    state = _build_state(sl, workflow_id="wf-offload-wal")

    manager = OffloadManager(
        sl,
        torch.device("cpu"),
        wal_path=str(wal_path),
        spill_dir=str(spill_dir),
    )
    handle = manager.start_offload(state, expected_return_seconds=5.0)
    assert manager.await_offload(handle, timeout=5.0)
    assert state.status == "OFFLOADED"
    assert state.kv_refs == []

    recovered = OffloadManager(
        sl,
        torch.device("cpu"),
        wal_path=str(wal_path),
        spill_dir=str(spill_dir),
    )
    result = recovered.recover()
    assert result[handle] == "RECOVERED_OFFLOADED"

    restored = recovered.restore(handle)
    assert restored is state
    assert restored.status == "ACTIVE"
    assert len(restored.kv_refs) == 1
