from __future__ import annotations

import torch

from mooncake_epd.agent.coordination import (
    AdmissionAction,
    AgentRequest,
    AgentScheduler,
    AgentType,
    DegradeLevel,
    WorkerLoad,
    Workflow,
)
from mooncake_epd.agent.coordination.workflow import WorkflowStep
from mooncake_epd.agent.coordination.reuse_pipeline import ReusePipeline
from mooncake_epd.core.state import (
    AttentionSimilarityReuse,
    FeatureBundle,
    FeatureStore,
    MMStore,
    PagedKVManager,
    RadixTree,
    RelayRecompute,
    StateLayer,
    StateMeta,
)
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy


def _make_state_layer(*, node_id: str = "node-a") -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=6,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id=node_id,
    )
    return StateLayer(
        pm,
        RadixTree(pm, max_entries=128),
        FeatureStore(max_bytes=16 * 1024 * 1024),
        default_ttl_seconds=30.0,
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


def test_workflow_commit_retains_prefix_when_prefill_returns_delta_only():
    sl = _make_state_layer()

    def delta_only_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs
        return _build_refs(sl, delta_tokens), None

    wf = Workflow(
        workflow_id="wf-commit-fix",
        agent_id="agent-x",
        state_layer=sl,
        prefill_fn=delta_only_prefill,
    )

    step0 = wf.advance([1, 2, 3, 4])
    step1 = wf.advance([5, 6])

    assert step0.state.num_kv_tokens == 4
    assert step1.state.num_kv_tokens == 6
    assert len(step1.state.kv_refs) == 2

    wf.release()
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_workflow_relay_commit_registers_full_path_for_future_exact_prefix_match():
    sl = _make_state_layer()

    def prefill_fn(delta_tokens, prefix_kv_refs=None, **kwargs):
        new_refs = _build_refs(sl, list(delta_tokens))
        return list(prefix_kv_refs or []) + new_refs, None

    wf = Workflow(
        workflow_id="wf-relay-register",
        agent_id="agent-x",
        state_layer=sl,
        prefill_fn=prefill_fn,
        enable_relay=True,
        relay_min_match_run=4,
    )

    base_tokens = list(range(10, 18))
    step0_refs = _build_refs(sl, base_tokens)
    step0_state = sl.register(
        kv_refs=step0_refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=base_tokens,
            workflow_id=wf.workflow_id,
            agent_id="agent-x",
            step=0,
        ),
    )
    wf.steps.append(
        WorkflowStep(
            step_index=0,
            state=step0_state,
            added_tokens=base_tokens,
            total_tokens=len(base_tokens),
        )
    )

    step1 = wf.advance(
        [14, 15, 16, 17, 50, 51, 52, 53],
        divergence_threshold=0.6,
    )

    assert step1.approximate is True
    assert step1.state.approximate is True
    assert (
        step1.relay_stats.get("reused_tokens", 0)
        + step1.relay_stats.get("tier3_reused_tokens", 0)
    ) >= 4
    assert step1.prefill_time_ms >= 0.0

    probe_tokens = step1.state.meta.token_ids + [60, 61, 62, 63]
    matched, delta = sl.borrow_prefix_refs(probe_tokens, workflow_id=wf.workflow_id)
    assert sum(ref.filled for ref in matched) == len(step1.state.meta.token_ids)
    assert delta == [60, 61, 62, 63]

    sl.pm.release_refs(matched)
    wf.release()
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_workflow_uses_tier3_reuse_pipeline_for_approximate_reuse():
    sl = _make_state_layer()

    wf = Workflow(
        workflow_id="wf-tier3-workflow",
        agent_id="agent-x",
        state_layer=sl,
        prefill_fn=None,
        enable_relay=True,
        enable_tier3=True,
        relay_min_match_run=4,
    )

    base_tokens = list(range(1, 9))
    base_refs = _build_refs(sl, base_tokens)
    base_state = sl.register(
        kv_refs=base_refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=base_tokens,
            workflow_id=wf.workflow_id,
            agent_id="agent-x",
            step=0,
        ),
    )
    wf.steps.append(
        WorkflowStep(
            step_index=0,
            state=base_state,
            added_tokens=base_tokens,
            total_tokens=len(base_tokens),
        )
    )

    prev_key0, prev_val0 = sl.pm.get_page(base_refs[0])
    prev_key1, prev_val1 = sl.pm.get_page(base_refs[1])
    candidate_key = torch.cat(
        [prev_key0[:, :, 2:4, :], prev_key1[:, :, 0:2, :]],
        dim=-2,
    )
    candidate_val = torch.cat(
        [prev_val0[:, :, 2:4, :], prev_val1[:, :, 0:2, :]],
        dim=-2,
    )

    def prefill_fn(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        if list(delta_tokens) == [3, 4, 5, 6]:
            ref = sl.pm.allocate_page(filled=4)
            sl.pm.write_page_slots(
                ref,
                candidate_key + 0.0001 * torch.randn_like(candidate_key),
                candidate_val,
                offset=0,
            )
            return [ref], None
        return _build_refs(sl, list(delta_tokens)), None

    wf.prefill_fn = prefill_fn

    step1 = wf.advance(
        [3, 4, 5, 6, 99, 100],
        divergence_threshold=0.95,
    )

    assert step1.approximate is True
    assert step1.relay_stats["tier3_accepted_pages"] == 1
    assert step1.relay_stats["tier3_reused_tokens"] == 4
    assert step1.relay_stats["tier3_mean_similarity"] >= 0.9
    assert step1.state.reuse_telemetry["tier3_accepted_pages"] == 1
    assert step1.state.reuse_telemetry["tier3_reused_tokens"] == 4
    assert step1.reuse_ratio > 0.8

    wf.release()
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_workflow_reuse_stats_use_absolute_reused_token_counts():
    sl = _make_state_layer()

    def delta_only_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    wf = Workflow(
        workflow_id="wf-reuse-stats",
        agent_id="agent-x",
        state_layer=sl,
        prefill_fn=delta_only_prefill,
    )

    step0 = wf.advance([1, 2, 3, 4])
    step1 = wf.advance([5, 6, 7, 8])
    step2 = wf.advance([9, 10, 11, 12])
    stats = wf.reuse_stats()

    assert step1.reused_tokens == 4
    assert step2.reused_tokens == 8
    assert stats["total_tokens_reused_from_prefix"] == 12
    assert stats["total_context_tokens"] == step1.total_tokens + step2.total_tokens
    assert stats["approximate_steps"] == 0

    wf.release()
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_relay_recompute_does_not_reuse_partial_pages_for_substring_hits():
    sl = _make_state_layer()

    def delta_only_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    prev_tokens = [10, 11, 12, 13, 14, 15, 16, 17]
    prev_refs = _build_refs(sl, prev_tokens)
    sl.register(
        kv_refs=prev_refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=prev_tokens,
            workflow_id="wf-relay-partial-page",
            agent_id="agent-x",
        ),
    )

    relay = RelayRecompute(sl.pm, sl.radix, delta_only_prefill, min_match_run=4)
    new_refs, stats = relay.run(
        new_tokens=[11, 12, 13, 14],
        prev_tokens=prev_tokens,
        scope="wf-relay-partial-page",
    )

    assert stats["reused_tokens"] == 0
    assert stats["substring_hits"] == 0
    assert stats["substring_misses"] == 1
    assert len(new_refs) == 1
    assert all(ref.physical_id not in {prev.physical_id for prev in prev_refs} for ref in new_refs)

    sl.pm.release_refs(new_refs)
    sl.release_chain("wf-relay-partial-page")
    sl.radix.clear()


def test_layered_kv_transfer_groups_layers_and_preserves_content():
    sl = _make_state_layer()
    refs = _build_refs(sl, list(range(8)))
    originals = [tuple(t.clone() for t in sl.pm.get_page(ref)) for ref in refs]

    engine = TransferEngine(protocol="local")
    target_pm, target_refs = engine.transfer_pages(
        sl.pm,
        refs,
        torch.device("cpu"),
        TransferPolicy(
            mode=Mode.STREAM,
            channel=Channel.PREFILL_TO_DECODE,
            extra={
                "layered": True,
                "layers_per_group": 2,
                "force_new_manager": True,
                "force_copy": True,
            },
        ),
    )
    try:
        assert target_pm is not sl.pm
        assert len(target_refs) == len(refs)
        batches = getattr(target_pm, "_last_layer_batches")
        assert len(batches) == len(refs) * 3  # 6 layers / groups of 2
        for (ok, ov), ref in zip(originals, target_refs):
            nk, nv = target_pm.get_page(ref)
            assert torch.allclose(ok, nk)
            assert torch.allclose(ov, nv)
    finally:
        target_pm.release_refs(target_refs)
        sl.pm.release_refs(refs)


def test_mm_store_event_prefetch_and_recompute_fallback():
    shared = FeatureStore(max_bytes=16 * 1024 * 1024)
    transfer = TransferEngine(protocol="local")
    mm_store = MMStore(shared, transfer)
    try:
        bundle = FeatureBundle(
            image_hash="img-1",
            last_hidden=torch.randn(2, 8, dtype=torch.float32),
            intermediates=[(4, torch.randn(2, 8, dtype=torch.float32))],
        )
        mm_store.publish(bundle)
        handle = mm_store.prefetch(
            "img-1",
            target_worker_id="prefill-0",
            target_device=torch.device("cpu"),
            policy=TransferPolicy(
                mode=Mode.PULL,
                channel=Channel.ENCODER_TO_PREFILL,
                extra={"force_copy": True},
            ),
        )
        pulled = mm_store.wait(handle, timeout=5.0)
        assert pulled is not None
        assert mm_store.lookup("prefill-0", "img-1") is not None
        assert pulled.last_hidden.data_ptr() != bundle.last_hidden.data_ptr()

        mm_store.register_recompute_hook(
            "prefill-1",
            lambda image_hash: FeatureBundle(
                image_hash=image_hash,
                last_hidden=torch.ones(2, 4, dtype=torch.float32),
                intermediates=[],
            ),
        )
        miss_handle = mm_store.prefetch(
            "img-recompute",
            target_worker_id="prefill-1",
            target_device=torch.device("cpu"),
        )
        miss_bundle = mm_store.wait(miss_handle, timeout=5.0)
        assert miss_bundle is not None
        assert miss_handle.recomputed is True
        assert mm_store.lookup("prefill-1", "img-recompute") is not None
    finally:
        mm_store.stop()


def test_a2a_2pc_handoff_updates_directory_owner_on_commit():
    sl = _make_state_layer(node_id="node-src")
    refs = _build_refs(sl, list(range(8)))
    state = sl.register(
        kv_refs=refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=list(range(8)),
            workflow_id="wf-a2a-2pc",
            agent_id="agent-a",
        ),
    )

    sl.handoff_prepare(state, "agent-b", target_node_id="node-dst")
    txn = sl.get_handoff(state.state_id)
    assert txn is not None
    assert txn.target_node_id == "node-dst"
    assert state.meta.agent_id == "agent-a"
    assert state.status == "HANDING_OVER"

    sl.handoff_commit(state)
    assert state.meta.agent_id == "agent-b"
    assert state.status == "ACTIVE"
    for ref in state.kv_refs:
        record = sl.pm.kv_directory.get_record(ref.global_block_id)
        assert record is not None
        assert record.owner_shard == "node-dst"
        assert record.physical_node_id == "node-dst"

    sl.release(state)
    sl.radix.clear()


def test_scheduler_applies_backpressure_before_rejecting():
    sched = AgentScheduler(
        workers=[
            WorkerLoad(
                worker_id="decode-hot",
                worker_type="decode",
                current_load=76,
                max_capacity=80,
                queue_size=45,
                queue_capacity=64,
                service_rate=100.0,
                arrival_rate=96.0,
                avg_latency_ms=35.0,
                critical_rho=0.90,
            )
        ]
    )
    req = AgentRequest(
        request_id="req-hot",
        agent_type=AgentType.INTERACTIVE,
        input_tokens=128,
        priority=5,
    )
    decision = sched.admission_decision(req)
    assert decision.action is AdmissionAction.BACKPRESSURE
    assert decision.degrade_level in {
        DegradeLevel.DISABLE_APPROX_REUSE,
        DegradeLevel.RAISE_COMPRESSION,
        DegradeLevel.OFFLOAD_AGGRESSIVELY,
    }

    hot_worker = sched.workers[0]
    hot_worker.queue_size = hot_worker.queue_capacity
    reject = sched.admission_decision(req)
    assert reject.action is AdmissionAction.REJECT


def test_paged_kv_manager_partial_ingest_keeps_only_delta_tokens():
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=3,
        dtype=torch.float32,
        device=torch.device("cpu"),
    )
    seq_len = 10
    layers = []
    for layer_idx in range(pm.num_layers):
        key = torch.arange(
            layer_idx * 1000,
            layer_idx * 1000 + pm.num_kv_heads * seq_len * pm.head_dim,
            dtype=pm.dtype,
        ).reshape(1, pm.num_kv_heads, seq_len, pm.head_dim)
        value = key + 0.5
        layers.append((key, value))

    refs = pm.ingest_kv_cache(tuple(layers), token_start=6)

    assert len(refs) == 1
    assert refs[0].filled == 4
    rebuilt = pm.materialize_kv_cache(refs)
    assert rebuilt[0][0].shape[-2] == 4
    assert torch.allclose(rebuilt[0][0], layers[0][0][:, :, 6:, :])
    assert torch.allclose(rebuilt[1][1], layers[1][1][:, :, 6:, :])

    pm.release_refs(refs)
    assert pm.stats()["total_pages"] == 0


def test_page_manager_materialize_and_reference_chunk_support_virtual_offsets():
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=4,
        dtype=torch.float32,
        device=torch.device("cpu"),
    )
    refs = []
    for page_idx in range(2):
        ref = pm.allocate_page(filled=4)
        base = page_idx * 1000
        key = torch.arange(
            base,
            base + pm.num_layers * pm.num_kv_heads * 4 * pm.head_dim,
            dtype=pm.dtype,
        ).reshape(pm.num_layers, pm.num_kv_heads, 4, pm.head_dim)
        value = key + 0.5
        pm.write_page_slots(ref, key, value, offset=0)
        refs.append(ref)

    chunk_refs = pm.reference_token_chunks(
        refs,
        token_start=2,
        chunk_sizes=[2, 4],
    )
    assert len(chunk_refs) == 2
    assert chunk_refs[0].physical_id == refs[0].physical_id
    assert chunk_refs[0].virtual_offset == 2
    assert chunk_refs[0].filled == 2
    assert chunk_refs[1].physical_id != refs[0].physical_id

    rebuilt = pm.materialize_kv_cache(chunk_refs)
    assert rebuilt[0][0].shape[-2] == 6

    expected_parts = []
    for ref in refs:
        k, _ = pm.get_page(ref)
        expected_parts.append(k[:, :, : ref.filled, :])
    expected = torch.cat(expected_parts, dim=-2)[:, :, 2:8, :]
    assert torch.allclose(rebuilt[0][0].squeeze(0), expected[0])

    pm.release_refs(chunk_refs)
    pm.release_refs(refs)
    assert pm.stats()["total_pages"] == 0


def test_reuse_pipeline_full_refs_release_does_not_free_parent_state():
    sl = _make_state_layer()
    state0 = sl.register(
        kv_refs=_build_refs(sl, [1, 2, 3, 4, 5]),
        feature_hash=None,
        meta=StateMeta(
            token_ids=[1, 2, 3, 4, 5],
            workflow_id="wf-b8-release",
            agent_id="agent-a",
        ),
    )

    def delta_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    reuse = ReusePipeline(
        sl,
        delta_prefill,
        relay_threshold=0.1,
        enable_tier2=True,
        enable_tier3=False,
    )
    refs, _, _ = reuse.run(
        new_tokens=[6, 7],
        prev_tokens=state0.meta.token_ids,
        prev_refs=state0.kv_refs,
        workflow_id=state0.workflow_id,
    )

    sl.pm.release_refs(refs)
    sl.release(state0)
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_reuse_pipeline_ratio_is_normalized_by_full_prompt_and_tracks_delta_costs():
    sl = _make_state_layer()
    state0 = sl.register(
        kv_refs=_build_refs(sl, [1, 2, 3, 4, 5]),
        feature_hash=None,
        meta=StateMeta(
            token_ids=[1, 2, 3, 4, 5],
            workflow_id="wf-b8-metrics",
            agent_id="agent-a",
        ),
    )

    def delta_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    reuse = ReusePipeline(
        sl,
        delta_prefill,
        relay_threshold=0.95,
        enable_tier2=True,
        enable_tier3=True,
    )
    refs, _, stats = reuse.run(
        new_tokens=[6, 7],
        prev_tokens=state0.meta.token_ids,
        prev_refs=state0.kv_refs,
        workflow_id=state0.workflow_id,
    )

    assert stats.total_tokens == 7
    assert stats.delta_tokens == 2
    # Partial tail pages are now inserted into the radix as exact-prefix
    # candidates, so Tier1 can reuse the full previous prompt rather than
    # only the last full page boundary.
    assert stats.tier1_matched_tokens == 5
    assert abs(stats.reuse_ratio - (5.0 / 7.0)) < 1e-6
    assert 0.0 <= stats.reuse_ratio <= 1.0
    assert stats.delta_prefill_calls == 1
    assert stats.delta_prefill_tokens == 2
    assert stats.tier3_accepted_pages == 0
    assert stats.tier3_candidate_pages == 0

    sl.pm.release_refs(refs)
    sl.release(state0)
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_reuse_pipeline_reports_relay_breakdown_for_approximate_reuse():
    sl = _make_state_layer()
    prev_tokens = list(range(1, 9))
    state0 = sl.register(
        kv_refs=_build_refs(sl, prev_tokens),
        feature_hash=None,
        meta=StateMeta(
            token_ids=prev_tokens,
            workflow_id="wf-b8-relay",
            agent_id="agent-a",
        ),
    )

    def delta_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        return _build_refs(sl, list(delta_tokens)), None

    reuse = ReusePipeline(
        sl,
        delta_prefill,
        relay_threshold=0.95,
        enable_tier2=True,
        enable_tier3=False,
    )
    refs, _, stats = reuse.run(
        new_tokens=[5, 6, 7, 8, 99, 100],
        prev_tokens=state0.meta.token_ids,
        prev_refs=state0.kv_refs,
        workflow_id=state0.workflow_id,
    )

    assert stats.total_tokens == 14
    assert stats.delta_tokens == 6
    assert stats.tier2_reused_tokens == 4
    assert stats.tier2_recomputed_tokens == 2
    assert abs(stats.delta_reuse_ratio - (4.0 / 6.0)) < 1e-6
    assert stats.relay_segments >= 2
    assert stats.relay_substring_hits >= 1
    assert stats.delta_prefill_calls >= 1
    assert stats.delta_prefill_tokens == 2
    assert stats.delta_prefill_ms >= 0.0

    sl.pm.release_refs(refs)
    sl.release(state0)
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_reuse_pipeline_tier3_reuses_recomputed_partial_page_when_similarity_passes():
    sl = _make_state_layer()
    prev_tokens = list(range(1, 9))
    prev_refs = _build_refs(sl, prev_tokens)
    state0 = sl.register(
        kv_refs=prev_refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=prev_tokens,
            workflow_id="wf-b8-tier3",
            agent_id="agent-a",
        ),
    )

    prev_key0, prev_val0 = sl.pm.get_page(prev_refs[0])
    prev_key1, prev_val1 = sl.pm.get_page(prev_refs[1])
    candidate_key = torch.cat(
        [
            prev_key0[:, :, 2:4, :],
            prev_key1[:, :, 0:2, :],
        ],
        dim=-2,
    )
    candidate_val = torch.cat(
        [
            prev_val0[:, :, 2:4, :],
            prev_val1[:, :, 0:2, :],
        ],
        dim=-2,
    )

    def delta_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
        del prefix_kv_refs, kwargs
        if list(delta_tokens) == [3, 4, 5, 6]:
            ref = sl.pm.allocate_page(filled=4)
            sl.pm.write_page_slots(
                ref,
                candidate_key + 0.0001 * torch.randn_like(candidate_key),
                candidate_val,
                offset=0,
            )
            return [ref], None
        return _build_refs(sl, list(delta_tokens)), None

    reuse = ReusePipeline(
        sl,
        delta_prefill,
        relay_threshold=0.95,
        attention_threshold=0.90,
        enable_tier2=True,
        enable_tier3=True,
    )
    refs, _, stats = reuse.run(
        new_tokens=[3, 4, 5, 6, 99, 100],
        prev_tokens=state0.meta.token_ids,
        prev_refs=state0.kv_refs,
        workflow_id=state0.workflow_id,
    )

    assert stats.tier2_reused_tokens == 0
    assert stats.tier3_candidate_pages == 1
    assert stats.tier3_accepted_pages == 1
    assert stats.tier3_reused_tokens == 4
    assert stats.tier3_mean_similarity >= 0.9
    assert abs(stats.delta_reuse_ratio - (4.0 / 6.0)) < 1e-6
    assert refs[2].physical_id != refs[3].physical_id

    sl.pm.release_refs(refs)
    sl.release(state0)
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_attention_similarity_reuse_compares_key_norm_profiles_for_subpages():
    sl = _make_state_layer()
    ref = sl.pm.allocate_page(filled=4)
    key = torch.randn(
        sl.pm.num_layers,
        sl.pm.num_kv_heads,
        4,
        sl.pm.head_dim,
        dtype=sl.pm.dtype,
    )
    value = torch.randn_like(key)
    sl.pm.write_page_slots(ref, key, value, offset=0)

    attn = AttentionSimilarityReuse(sl.pm, threshold=0.99)
    # Keep per-token norms identical while changing the raw tensor direction.
    new_key = key.roll(shifts=1, dims=-1)
    accepted, sim = attn.evaluate_page(ref, new_key)

    assert accepted is not None
    assert sim >= 0.99

    sl.pm.release_refs([accepted])
    sl.pm.release_refs([ref])
    assert sl.pm.stats()["total_pages"] == 0


def test_encoder_worker_accepts_structured_qwen_vision_output():
    class _VisionOutput:
        def __init__(self):
            self.pooler_output = [torch.randn(2, 8), torch.randn(3, 8)]
            self.deepstack_hidden_states = [torch.randn(2, 8), torch.randn(3, 8)]

    class _VisionModel:
        class config:
            class vision_config:
                deepstack_visual_indexes = [4, 8]

        class model:
            @staticmethod
            def get_image_features(pixel_values, image_grid_thw=None):
                del pixel_values, image_grid_thw
                return _VisionOutput()

    from mooncake_epd.core.epd_workers import EncoderWorker

    worker = EncoderWorker(_VisionModel(), processor=None, device=torch.device("cpu"))
    out = worker.encode(
        pixel_values=torch.randn(1, 3, 4, 4),
        image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
        image_id="img-struct",
    )

    assert out.image_id == "img-struct"
    assert out.bundle.last_hidden.shape == (5, 8)
    assert len(out.bundle.intermediates) == 2
    assert out.bundle.intermediates[0][0] == 4


def test_encoder_worker_reads_upstream_qwen3vl_deepstack_features():
    class _VisionOutput:
        def __init__(self):
            self.pooler_output = [torch.randn(2, 8)]
            self.deepstack_features = [torch.randn(2, 8), torch.randn(2, 8), torch.randn(2, 8)]

    class _VisionModel:
        class config:
            class vision_config:
                deepstack_visual_indexes = [8, 16, 24]

        class model:
            @staticmethod
            def get_image_features(pixel_values, image_grid_thw=None):
                del pixel_values, image_grid_thw
                return _VisionOutput()

    from mooncake_epd.core.epd_workers import EncoderWorker

    worker = EncoderWorker(_VisionModel(), processor=None, device=torch.device("cpu"))
    out = worker.encode(
        pixel_values=torch.randn(1, 3, 4, 4),
        image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
        image_id="img-upstream-deepstack",
    )

    assert out.bundle.last_hidden.shape == (2, 8)
    assert [layer for layer, _ in out.bundle.intermediates] == [8, 16, 24]
