"""Local 2PC-style handoff tests for MultimodalState."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta  # noqa: E402


def _make_state_layer():
    pm = PagedKVManager(
        page_size=8,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.bfloat16,
        device=torch.device("cpu"),
    )
    tree = RadixTree(pm, max_entries=64)
    fs = FeatureStore(max_bytes=8 * 1024 * 1024)
    return StateLayer(pm, tree, fs, default_ttl_seconds=10.0)


def _build_state(sl: StateLayer):
    ref = sl.pm.allocate_page(filled=8)
    k = torch.randn(2, 2, 8, 8, dtype=torch.bfloat16)
    v = torch.randn_like(k)
    sl.pm.write_page_slots(ref, k, v, offset=0)
    return sl.register(
        kv_refs=[ref],
        feature_hash=None,
        meta=StateMeta(
            token_ids=list(range(8)),
            workflow_id="wf-handoff-sm",
            agent_id="agent-A",
        ),
    )


def test_handoff_prepare_commit():
    sl = _make_state_layer()
    state = _build_state(sl)

    sl.handoff_prepare(state, "agent-B")
    assert state.meta.agent_id == "agent-A"
    assert state.status == "HANDING_OVER"

    sl.handoff_commit(state)
    assert state.meta.agent_id == "agent-B"
    assert state.status == "ACTIVE"


def test_handoff_prepare_rollback():
    sl = _make_state_layer()
    state = _build_state(sl)

    sl.handoff_prepare(state, "agent-C")
    assert state.status == "HANDING_OVER"
    sl.handoff_rollback(state)
    assert state.meta.agent_id == "agent-A"
    assert state.status == "ACTIVE"


def test_handoff_disallowed_during_offloading():
    sl = _make_state_layer()
    state = _build_state(sl)
    state.status = "OFFLOADING"

    with pytest.raises(RuntimeError):
        sl.handoff_prepare(state, "agent-Z")
