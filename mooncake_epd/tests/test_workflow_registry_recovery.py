from __future__ import annotations

import torch

from mooncake_epd.agent.coordination import Workflow
from mooncake_epd.agent.coordination.offload import OffloadManager
from mooncake_epd.core.state import (
    FeatureStore,
    PagedKVManager,
    RadixTree,
    StateLayer,
    StateMeta,
    WorkflowStateRegistry,
)


def _make_state_layer(registry: WorkflowStateRegistry, *, handoff_wal_path: str | None = None) -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-registry",
    )
    return StateLayer(
        pm,
        RadixTree(pm, max_entries=64),
        FeatureStore(max_bytes=8 * 1024 * 1024),
        default_ttl_seconds=10.0,
        handoff_wal_path=handoff_wal_path,
        workflow_registry=registry,
    )


def _build_state(sl: StateLayer, workflow_id: str = "wf-registry"):
    ref = sl.pm.allocate_page(filled=4)
    key = torch.randn(2, 2, 4, 8, dtype=torch.float32)
    value = torch.randn_like(key)
    sl.pm.write_page_slots(ref, key, value, offset=0)
    return sl.register(
        kv_refs=[ref],
        feature_hash=None,
        meta=StateMeta(
            token_ids=[10, 11, 12, 13],
            workflow_id=workflow_id,
            agent_id="agent-a",
        ),
    )


def _build_refs(sl: StateLayer, token_ids) -> list:
    refs = []
    for start in range(0, len(token_ids), sl.pm.page_size):
        chunk = token_ids[start : start + sl.pm.page_size]
        ref = sl.pm.allocate_page(filled=len(chunk))
        key = torch.randn(
            sl.pm.num_layers,
            sl.pm.num_kv_heads,
            len(chunk),
            sl.pm.head_dim,
            dtype=sl.pm.dtype,
        )
        value = torch.randn_like(key)
        sl.pm.write_page_slots(ref, key, value, offset=0)
        refs.append(ref)
    return refs


def test_workflow_registry_tracks_state_transitions(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "registry.jsonl"))
    sl = _make_state_layer(registry)
    state = _build_state(sl)

    record = registry.get_record(state.state_id)
    assert record is not None
    assert record.status == "ACTIVE"
    assert record.workflow_id == "wf-registry"

    sl.handoff_prepare(state, "agent-b", target_node_id="node-b")
    record = registry.get_record(state.state_id)
    assert record is not None
    assert record.status == "HANDING_OVER"
    assert record.handoff_id is not None
    assert record.target_agent_id == "agent-b"

    sl.handoff_commit(state)
    record = registry.get_record(state.state_id)
    assert record is not None
    assert record.status == "ACTIVE"
    assert record.agent_id == "agent-b"
    assert record.handoff_id is None

    sl.release(state)
    record = registry.get_record(state.state_id)
    assert record is not None
    assert record.status == "RELEASED"
    assert record.released_at is not None


def test_offload_recovery_rehydrates_state_from_registry(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "registry.jsonl"))
    offload_wal = tmp_path / "offload.jsonl"
    spill_dir = tmp_path / "spill"

    sl = _make_state_layer(registry)
    state = _build_state(sl, workflow_id="wf-registry-offload")
    manager = OffloadManager(
        sl,
        torch.device("cpu"),
        wal_path=str(offload_wal),
        spill_dir=str(spill_dir),
    )
    handle = manager.start_offload(state, expected_return_seconds=5.0)
    assert manager.await_offload(handle, timeout=5.0)
    assert state.status == "OFFLOADED"

    # Simulate process restart: only keep persisted registry/WAL, drop live state memory.
    registry2 = WorkflowStateRegistry(str(tmp_path / "registry.jsonl"))
    sl2 = _make_state_layer(registry2)
    manager2 = OffloadManager(
        sl2,
        torch.device("cpu"),
        wal_path=str(offload_wal),
        spill_dir=str(spill_dir),
    )
    recovered = manager2.recover()
    assert recovered[handle] == "RECOVERED_OFFLOADED"

    restored = manager2.restore(handle)
    assert restored is not None
    assert restored.state_id == state.state_id
    assert restored.workflow_id == "wf-registry-offload"
    assert restored.status == "ACTIVE"
    assert len(restored.kv_refs) == 1


def test_workflow_registry_persists_reuse_telemetry_across_recovery(tmp_path):
    registry_path = tmp_path / "registry.jsonl"
    registry = WorkflowStateRegistry(str(registry_path))
    sl = _make_state_layer(registry)

    def delta_only_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    wf = Workflow(
        workflow_id="wf-registry-reuse",
        agent_id="agent-a",
        state_layer=sl,
        prefill_fn=delta_only_prefill,
    )

    wf.advance([1, 2, 3, 4])
    step1 = wf.advance([5, 6, 7, 8])

    record = registry.get_record(step1.state.state_id)
    assert record is not None
    assert record.reuse_telemetry["cross_step"] is True
    assert record.reuse_telemetry["reuse_ratio"] == 0.5
    assert record.reuse_telemetry["reused_tokens"] == 4
    assert record.reuse_telemetry["total_tokens"] == 8
    assert record.reuse_telemetry["delta_tokens"] == 4
    assert record.reuse_telemetry["tier1_matched_tokens"] == 4
    assert record.reuse_telemetry["delta_prefill_calls"] == 1
    assert record.reuse_telemetry["delta_prefill_tokens"] == 4

    registry_recovered = WorkflowStateRegistry(str(registry_path))
    recovered = registry_recovered.get_record(step1.state.state_id)
    assert recovered is not None
    assert recovered.reuse_telemetry == record.reuse_telemetry


def test_workflow_registry_summary_aggregates_tier3_fields(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "registry-summary.jsonl"))
    sl = _make_state_layer(registry)

    def delta_only_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    wf = Workflow(
        workflow_id="wf-summary-tier3",
        agent_id="agent-a",
        state_layer=sl,
        prefill_fn=delta_only_prefill,
    )

    wf.advance([1, 2, 3, 4])
    step1 = wf.advance([5, 6, 7, 8])
    record = registry.get_record(step1.state.state_id)
    assert record is not None
    record.reuse_telemetry.update(
        {
            "approximate": True,
            "tier3_accepted_pages": 1,
            "tier3_reused_tokens": 4,
            "tier3_mean_similarity": 0.93,
        }
    )
    registry.upsert_record(record, event="TEST_TIER3", preserve_existing_reuse=False)

    summary = registry.reuse_telemetry_summary()
    assert summary["total_tier3_accepted_pages"] == 1
    assert summary["total_tier3_reused_tokens"] == 4
    assert summary["avg_tier3_mean_similarity"] == 0.93
    latest = summary["latest_reuse_by_workflow"]["wf-summary-tier3"]
    assert latest["tier3_accepted_pages"] == 1
    assert latest["tier3_reused_tokens"] == 4
