from __future__ import annotations

import pytest

import json
import time

import torch

from mooncake_epd.agent.coordination import Workflow, WorkflowStep
from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.control.connector_metrics import ConnectorMetricsReader, ConnectorMetricsSink
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta
from mooncake_epd.core.state import (
    FeatureStore,
    KVTransferManifestV2,
    PagedKVManager,
    RadixTree,
    StateLayer,
    StateMeta,
    WorkflowStateRegistry,
)



def _mm_request():
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": "https://example.com/demo.png"}},
                    {"type": "text", "text": "Describe the image and summarize the trend."},
                ],
            }
        ],
        "metadata": {"workflow_id": "wf-mm-1"},
    }


def _make_state_layer(registry: WorkflowStateRegistry) -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=6,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-registry",
    )
    return StateLayer(
        pm,
        RadixTree(pm, max_entries=128),
        FeatureStore(max_bytes=16 * 1024 * 1024),
        workflow_registry=registry,
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


def test_stage_lifecycle_separates_queue_running_and_first_token():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-lifecycle")
    )
    cp.register_stage_workers("decode", ["decode-0"])
    ctx = cp.start_request(
        {
            "messages": [{"role": "user", "content": "hello"}],
            "metadata": {"workflow_id": "wf-lifecycle"},
        },
        "req-lifecycle",
    )

    decision = cp.admit_stage("decode", ctx)
    worker = cp.stage_workers("decode")[0]
    assert decision.worker_id == "decode-0"
    assert worker.queue_size == 1
    assert worker.queued_requests == 1
    assert worker.current_load == 0
    assert worker.running_requests == 0

    assert cp.mark_stage_started("decode", "decode-0", ctx=ctx) is True
    assert cp.mark_stage_started("decode", "decode-0", ctx=ctx) is False
    assert worker.queue_size == 0
    assert worker.current_load == 1
    assert worker.first_token_pending == 1
    assert worker.active_decode_sequences == 1

    assert (
        cp.mark_stage_first_token(
            "decode", "decode-0", ctx=ctx, first_token_ms=12.5
        )
        is True
    )
    assert (
        cp.mark_stage_first_token(
            "decode", "decode-0", ctx=ctx, first_token_ms=99.0
        )
        is False
    )
    assert worker.first_token_pending == 0
    assert worker.avg_first_token_ms == pytest.approx(12.5)

    cp.mark_stage_complete(
        "decode",
        "decode-0",
        latency_ms=40.0,
        success=True,
        ctx=ctx,
    )
    cp.mark_stage_complete(
        "decode",
        "decode-0",
        latency_ms=80.0,
        success=True,
        ctx=ctx,
    )
    assert worker.current_load == 0
    assert worker.active_decode_sequences == 0
    assert worker.avg_completion_ms == pytest.approx(40.0)
    assert ctx.stage_lifecycle["decode"]["state"] == "completed"



def test_serving_control_plane_classifies_multimodal_and_builds_handoff():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-test", layers_per_group=8))
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-1")
    assert ctx.modality == "multimodal"
    assert ctx.routing_path == "EPD"
    assert ctx.workflow_id == "wf-mm-1"
    assert len(ctx.mm_hashes) == 1

    prefill = cp.admit_stage("prefill", ctx)
    prefill_kv = cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    assert prefill_kv["layered_kv_transfer"] is True
    assert prefill_kv["layers_per_group"] == 8
    assert prefill_kv["mm_prefetch_image_hashes"] == ctx.mm_hashes

    upstream_kv = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-1",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[101, 102], [103]],
        },
        decode_worker_id="decode-0",
    )
    assert upstream_kv["handoff_id"] == ctx.handoff_id
    assert cp.kv_directory.get_handoff(ctx.handoff_id) is not None

    decode = cp.admit_stage("decode", ctx)
    decode_kv = cp.build_decode_kv_params(ctx, decode, upstream_kv)
    assert decode_kv["handoff_id"] == ctx.handoff_id
    assert decode_kv["do_remote_prefill"] is True
    assert decode_kv["do_remote_decode"] is False
    assert decode_kv["a2a_source_node"] == "prefill-0"
    assert decode_kv["a2a_target_node"] == "decode-0"
    assert ctx.block_ids == [
        "prefill-engine:101",
        "prefill-engine:102",
        "prefill-engine:103",
    ]

    cp.commit_handoff(ctx)
    record = cp.kv_directory.get_handoff(ctx.handoff_id)
    assert record is not None
    assert record.status == "COMMITTED"
    owner = cp.kv_directory.get_record("prefill-engine:101")
    assert owner is not None
    assert owner.owner_shard == "decode-0"

    cp.finish_request(ctx.request_id)
    assert cp.kv_directory.get_handoff(ctx.handoff_id) is None
    assert cp.kv_directory.get_record("prefill-engine:101") is None

    snapshot = cp.snapshot()
    epd_stats = snapshot["metrics"]["path_stats"]["EPD"]
    assert epd_stats["requests_total"] == 1
    assert epd_stats["requests_finished"] == 1
    assert epd_stats["requests_active"] == 0
    assert epd_stats["requests_multimodal"] == 1
    assert epd_stats["mm_prefetch_announced"] == 1
    assert epd_stats["handoff_prepared"] == 1
    assert epd_stats["handoff_committed"] == 1
    assert epd_stats["handoff_rolled_back"] == 0
    assert epd_stats["stage_dispatches"] == {"prefill": 1, "decode": 1}
    assert snapshot["metrics"]["path_stats"]["PD"]["requests_total"] == 0


def test_serving_control_plane_binds_checksums_kv_transfer_manifest_before_decode(tmp_path):
    generation_dir = tmp_path / "worker-generations"
    generation_dir.mkdir()
    (generation_dir / "prefill-0").write_text("prefill-generation-1\n", encoding="utf-8")
    (generation_dir / "decode-0").write_text("decode-generation-runtime\n", encoding="utf-8")
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-manifest",
            require_kv_transfer_manifest_v2=True,
            require_kv_transfer_manifest_compatibility=True,
            worker_generation_dir=str(generation_dir),
        )
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-manifest")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    handoff = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-manifest",
            "remote_engine_id": "epd-prefill",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[101, 102], [103]],
            "source_generation": "prefill-generation-1",
            "destination_generation": "ignored-legacy-generation",
            "mooncake_protocol": "tcp",
            "transport_backend": "mooncake_engine_direct",
            "kv_layout_v2": {
                "model_id": "Qwen3-VL-8B-Instruct",
                "model_revision": "revision-a",
                "vllm_adapter_version": "mooncake-epd-adapter-1",
                "dtype": "float16",
                "block_size": 16,
                "num_layers": 36,
                "tp_size": 1,
                "tp_rank": 0,
            },
        },
        decode_worker_id="decode-0",
    )
    pre_dispatch = KVTransferManifestV2.from_control_payload(
        handoff["kv_transfer_manifest_v2"]
    )
    assert pre_dispatch.destination_engine_id == "decode-0"
    assert pre_dispatch.source_generation == "prefill-generation-1"

    decode = cp.admit_stage("decode", ctx)
    decode_kv = cp.build_decode_kv_params(ctx, decode, handoff)
    manifest = KVTransferManifestV2.from_control_payload(
        decode_kv["kv_transfer_manifest_v2"]
    )
    manifest.validate_for_consumer(
        transfer_id="xfer-manifest",
        workflow_id="wf-mm-1",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-runtime",
        require_compatibility=True,
    )
    assert "kv_transfer_manifest_v2" not in ctx.reuse_telemetry["kv_handoff"]
    assert ctx.reuse_telemetry["kv_handoff"]["source_kv_transfer_manifest_v2"] == manifest.as_control_payload()
    metrics = cp.snapshot()["metrics"]
    assert metrics["kv_transfer_manifests_created"] == 1
    assert metrics["kv_transfer_manifests_validated"] == 1


def test_prefill_decode_transport_affinity_preserves_pair_and_bypasses_saturation():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-affinity",
            prefill_decode_affinity=(
                ("prefill-0", "decode-0"),
                ("prefill-1", "decode-1"),
            ),
        )
    )
    cp.register_stage_workers("prefill", ["prefill-0", "prefill-1"])
    cp.register_stage_workers("decode", ["decode-0", "decode-1"])

    ctx = cp.start_request(
        {"messages": [{"role": "user", "content": "summarize this request"}]},
        "req-affinity-0",
    )
    prefill = cp.admit_stage("prefill", ctx)
    assert prefill.worker_id == "prefill-0"
    prefill_kv = cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-1")
    assert prefill_kv["a2a_target_node"] == "decode-0"
    decode = cp.admit_stage("decode", ctx)
    assert decode.worker_id == "decode-0"

    # The preference is not a hard pin: once the paired Decode is full, a
    # healthy peer is selected so locality cannot create an admission outage.
    cp.update_worker_load("decode", "decode-0", current_load=64, queue_size=0)
    cp.update_worker_load("prefill", "prefill-1", current_load=32, queue_size=0)
    ctx_overloaded = cp.start_request(
        {"messages": [{"role": "user", "content": "summarize this request"}]},
        "req-affinity-1",
    )
    prefill_overloaded = cp.admit_stage("prefill", ctx_overloaded)
    assert prefill_overloaded.worker_id == "prefill-0"
    cp.build_prefill_kv_params(ctx_overloaded, prefill_overloaded)
    fallback_decode = cp.admit_stage("decode", ctx_overloaded)
    assert fallback_decode.worker_id == "decode-1"

    metrics = cp.snapshot()["metrics"]
    assert metrics["pd_transport_affinity_candidates"] == 2
    assert metrics["pd_transport_affinity_hits"] == 1
    assert metrics["pd_transport_affinity_fallbacks"] == 1


def test_serving_control_plane_syncs_workflow_registry(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-registry.jsonl"))
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-registry", layers_per_group=4),
        workflow_registry=registry,
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-registry")
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "ACTIVE"
    assert record.workflow_id == "wf-mm-1"
    assert record.feature_hashes == ctx.mm_hashes

    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "PREFILL_DISPATCHED"
    assert record.agent_id == "prefill-0"

    upstream_kv = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-registry",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[201, 202]],
        },
        decode_worker_id="decode-0",
    )
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "HANDING_OVER"
    assert record.handoff_id == ctx.handoff_id
    assert record.kv_block_ids == ["prefill-engine:201", "prefill-engine:202"]

    decode = cp.admit_stage("decode", ctx)
    cp.build_decode_kv_params(ctx, decode, upstream_kv)
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "DECODE_DISPATCHED"
    assert record.agent_id == "decode-0"
    assert record.target_agent_id == "decode-0"

    cp.commit_handoff(ctx)
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "ACTIVE"
    assert record.agent_id == "decode-0"
    assert record.handoff_id is None

    cp.finish_request(ctx.request_id)
    record = registry.get_record("req-registry")
    assert record is not None
    assert record.status == "RELEASED"
    assert record.released_at is not None

    snapshot = cp.snapshot()
    reg_snapshot = snapshot["workflow_registry"]
    assert reg_snapshot["enabled"] is True
    assert reg_snapshot["tracked_states"] == 1
    assert reg_snapshot["status_counts"]["RELEASED"] == 1
    assert reg_snapshot["active_state_ids"] == []


def test_serving_control_plane_registry_tracks_rollback(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-rollback.jsonl"))
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-rollback"),
        workflow_registry=registry,
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request({"messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]}, "req-rb")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-rb",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [301],
        },
        decode_worker_id="decode-0",
    )

    cp.rollback_handoff(ctx)
    record = registry.get_record("req-rb")
    assert record is not None
    assert record.status == "ROLLED_BACK"
    assert record.handoff_id == ctx.handoff_id

    cp.finish_request(ctx.request_id)
    record = registry.get_record("req-rb")
    assert record is not None
    assert record.status == "RELEASED"

    snapshot = cp.snapshot()
    pd_stats = snapshot["metrics"]["path_stats"]["PD"]
    assert pd_stats["requests_total"] == 1
    assert pd_stats["requests_text"] == 1
    assert pd_stats["requests_finished"] == 1
    assert pd_stats["handoff_prepared"] == 1
    assert pd_stats["handoff_committed"] == 0
    assert pd_stats["handoff_rolled_back"] == 1
    assert pd_stats["stage_dispatches"] == {"prefill": 1, "decode": 0}


def test_serving_control_plane_snapshot_reports_registry_reuse_summary(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-reuse-registry.jsonl"))
    sl = _make_state_layer(registry)

    def prefill_fn(delta_tokens, prefix_kv_refs=None, **kwargs):
        new_refs = _build_refs(sl, list(delta_tokens))
        return list(prefix_kv_refs or []) + new_refs, None

    wf = Workflow(
        workflow_id="wf-registry-reuse",
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

    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-reuse-summary"),
        workflow_registry=registry,
    )
    reg_snapshot = cp.snapshot()["workflow_registry"]
    reuse_summary = reg_snapshot["reuse_summary"]

    assert reuse_summary["states_with_reuse_telemetry"] >= 1
    assert reuse_summary["cross_step_records"] == 1
    assert reuse_summary["approximate_states"] == 1
    assert step1.state.state_id in reuse_summary["active_approximate_state_ids"]
    assert reuse_summary["total_reused_tokens"] >= step1.relay_stats.get("reused_tokens", 0)
    latest = reuse_summary["latest_reuse_by_workflow"]["wf-registry-reuse"]
    assert latest["state_id"] == step1.state.state_id
    assert latest["reuse_ratio"] == step1.reuse_ratio


def test_serving_control_plane_owner_shards_accepts_worker_ids_for_directory_placeholders():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-dist",
            owner_shards=4,
        )
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-dist")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    upstream_kv = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-dist",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[401, 402]],
        },
        decode_worker_id="decode-0",
    )

    assert ctx.handoff_id is not None
    assert cp.kv_directory.get_handoff(ctx.handoff_id) is not None
    placeholder = cp.kv_directory.get_record("prefill-engine:401")
    assert placeholder is not None
    assert placeholder.physical_node_id == "prefill-0"
    assert placeholder.owner_shard.startswith("shard-")

    decode = cp.admit_stage("decode", ctx)
    decode_kv = cp.build_decode_kv_params(ctx, decode, upstream_kv)
    assert decode_kv["a2a_source_node"] == "prefill-0"
    assert decode_kv["a2a_target_node"] == "decode-0"

    cp.commit_handoff(ctx)
    cp.finish_request(ctx.request_id)


def test_register_stage_workers_merges_incrementally():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-test"))
    cp.register_stage_workers("decode", ["decode-0"])
    cp.register_stage_workers("decode", ["decode-0", "decode-1"])
    worker_ids = [worker.worker_id for worker in cp.stage_workers("decode")]
    assert worker_ids == ["decode-0", "decode-1"]


def test_stage_lifecycle_tracks_tokens_and_idempotent_failure():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-tokens"))
    cp.register_stage_workers("prefill", ["prefill-0"])
    ctx = cp.start_request(
        {
            "messages": [{"role": "user", "content": "one two three"}],
            "max_tokens": 8,
        },
        "req-token-lifecycle",
    )
    decision = cp.admit_stage("prefill", ctx)
    worker = cp.stage_workers("prefill")[0]

    assert worker.queued_tokens == 3
    assert cp.mark_stage_started(
        "prefill", decision.worker_id, ctx=ctx
    )
    assert worker.queued_tokens == 0
    assert worker.running_tokens == 3

    cp.mark_stage_failed_or_cancelled(
        "prefill",
        decision.worker_id,
        ctx=ctx,
        latency_ms=2.0,
    )
    cp.mark_stage_failed_or_cancelled(
        "prefill",
        decision.worker_id,
        ctx=ctx,
        latency_ms=3.0,
    )

    assert worker.running_tokens == 0
    assert worker.running_requests == 0
    assert ctx.stage_lifecycle["prefill"]["state"] == "failed"


def test_worker_snapshot_exposes_unknown_telemetry_as_null():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-unknown-telemetry")
    )
    cp.register_stage_workers("decode", ["decode-0"])

    worker = cp.snapshot()["workers"]["decode"][0]

    assert worker["telemetry"]["gpu_utilization"] is None
    assert worker["telemetry_known"]["gpu_utilization"] is False



def test_serving_control_plane_backpressure_and_reject():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            warn_rho=0.80,
            critical_rho=0.95,
            max_backpressure_delay_ms=200.0,
        )
    )
    cp.register_stage_workers("decode", ["decode-0"])
    cp.update_worker_load(
        "decode",
        "decode-0",
        current_load=50,
        max_capacity=64,
        queue_size=40,
        queue_capacity=48,
        service_rate=10.0,
        arrival_rate=9.0,
    )
    ctx = cp.start_request({"messages": [{"role": "user", "content": "hello"}]}, "req-text")
    decision = cp.admit_stage("decode", ctx)
    assert decision.decision.action.value == "BACKPRESSURE"
    assert decision.wait_ms > 0

    cp.update_worker_load(
        "decode",
        "decode-0",
        current_load=64,
        max_capacity=64,
        queue_size=48,
        queue_capacity=48,
        service_rate=10.0,
        arrival_rate=20.0,
    )
    ctx2 = cp.start_request({"messages": [{"role": "user", "content": "again"}]}, "req-reject")
    try:
        cp.admit_stage("decode", ctx2)
    except RuntimeError as exc:
        assert "rejected" in str(exc) or "failed" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected reject")


def test_serving_control_plane_does_not_reject_idle_worker_on_predictive_arrival_only():
    cp = ServingControlPlane(ServingControlPlaneConfig())
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.update_worker_load(
        "prefill",
        "prefill-0",
        current_load=0,
        max_capacity=32,
        queue_size=0,
        queue_capacity=64,
        service_rate=5.0,
        arrival_rate=100.0,
    )

    ctx = cp.start_request({"messages": [{"role": "user", "content": "hello"}]}, "req-idle")
    decision = cp.admit_stage("prefill", ctx)

    assert decision.decision.action.value == "ADMIT"
    assert decision.wait_ms == 0.0


def test_serving_control_plane_snapshot_uses_bounded_scheduler_rho():
    cp = ServingControlPlane(ServingControlPlaneConfig())
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.update_worker_load(
        "prefill",
        "prefill-0",
        current_load=0,
        max_capacity=32,
        queue_size=0,
        queue_capacity=64,
        service_rate=5.0,
        arrival_rate=100.0,
    )

    snap = cp.snapshot()

    rho = snap["workers"]["prefill"][0]["rho"]
    assert rho < 1.0


def test_serving_control_plane_snapshot_is_read_only_for_arrival_rate():
    cp = ServingControlPlane(ServingControlPlaneConfig())
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.update_worker_load(
        "prefill",
        "prefill-0",
        current_load=0,
        max_capacity=32,
        queue_size=0,
        queue_capacity=64,
        service_rate=5.0,
        arrival_rate=10.0,
    )
    before = cp.stage_workers("prefill")[0].arrival_rate

    cp.snapshot()

    after = cp.stage_workers("prefill")[0].arrival_rate
    assert after == before


def test_serving_control_plane_snapshot_merges_real_connector_metrics(tmp_path):
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="engine-0",
        role="producer",
        hostname="host-0",
        rpc_port=8999,
        tp_rank=0,
    )
    sink.record(
        LayeredTransferWorkerMeta(
            grouped_batches=3,
            grouped_bytes=192,
            grouped_descriptors=6,
            peer_buffer_batches=2,
            peer_buffer_bytes=128,
            fallback_batches=1,
            fallback_bytes=64,
            received_group_batches=5,
            received_finished_reqs=2,
            layer_wait_calls=7,
            layer_wait_ms=12.5,
            receive_failures=1,
            receive_kv_requests=2,
            receive_kv_worker_roundtrips=2,
            receive_kv_worker_ms=30.0,
            receive_kv_response_messages=5,
            receive_kv_first_response_count=2,
            receive_kv_first_response_ms=10.0,
            receive_kv_last_response_count=2,
            receive_kv_last_response_ms=28.0,
            receive_kv_response_process_count=5,
            receive_kv_response_process_ms=1.25,
            receive_kv_first_group_count=2,
            receive_kv_first_group_ms=12.0,
            receive_kv_finished_count=2,
            receive_kv_finished_ms=26.0,
            backend_counts={
                "peer_buffer_direct": 2,
                "batch_transfer_fallback": 1,
            },
        ),
        path_totals={
            "PD": LayeredTransferWorkerMeta(
                grouped_batches=1,
                grouped_bytes=64,
                grouped_descriptors=2,
            ),
            "EPD": LayeredTransferWorkerMeta(
                grouped_batches=2,
                grouped_bytes=128,
                grouped_descriptors=4,
            ),
        },
    )
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-test",
            connector_metrics_dir=str(tmp_path),
        )
    )

    snapshot = cp.snapshot()
    metrics = snapshot["metrics"]

    assert metrics["connector_metric_workers"] == 1
    assert metrics["layered_transfer_grouped_batches"] == 3
    assert metrics["peer_buffer_batches"] == 2
    assert metrics["layered_receive_group_batches"] == 5
    assert metrics["layered_receive_finished_reqs"] == 2
    assert metrics["layer_load_wait_calls"] == 7
    assert metrics["layer_load_wait_ms"] == 12.5
    assert metrics["layered_receive_failures"] == 1
    assert metrics["layered_receive_kv_requests"] == 2
    assert metrics["layered_receive_kv_worker_roundtrips"] == 2
    assert metrics["layered_receive_kv_worker_ms_avg"] == 15.0
    assert metrics["layered_receive_kv_response_messages"] == 5
    assert metrics["layered_receive_kv_first_response_count"] == 2
    assert metrics["layered_receive_kv_first_response_ms_avg"] == 5.0
    assert metrics["layered_receive_kv_last_response_count"] == 2
    assert metrics["layered_receive_kv_last_response_ms_avg"] == 14.0
    assert metrics["layered_receive_kv_response_process_count"] == 5
    assert metrics["layered_receive_kv_response_process_ms_avg"] == 0.25
    assert metrics["layered_receive_kv_first_group_count"] == 2
    assert metrics["layered_receive_kv_first_group_ms_avg"] == 6.0
    assert metrics["layered_receive_kv_finished_count"] == 2
    assert metrics["layered_receive_kv_finished_ms_avg"] == 13.0
    assert metrics["fallback_batches"] == 1
    assert metrics["path_stats"]["PD"]["requests_total"] == 0
    assert metrics["path_stats"]["EPD"]["requests_total"] == 0
    assert metrics["connector_path_stats"]["PD"]["grouped_bytes"] == 64
    assert metrics["connector_path_stats"]["EPD"]["grouped_descriptors"] == 4
    assert metrics["remote_transfer_backend_counts"] == {
        "peer_buffer_direct": 2,
        "batch_transfer_fallback": 1,
    }


def test_connector_metrics_sink_cleans_stale_same_worker_files(tmp_path):
    stale = tmp_path / "engine-0-producer-rank0-pid111.json"
    stale.write_text(
        '{"version":1,"identity":{"engine_id":"engine-0","role":"producer","tp_rank":0,"pid":111},"updated_at":1.0,"totals":{"grouped_batches":99}}',
        encoding="utf-8",
    )

    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="engine-0",
        role="producer",
        hostname="host-0",
        rpc_port=8999,
        tp_rank=0,
        pid=222,
    )

    assert not stale.exists()
    sink.record(LayeredTransferWorkerMeta(grouped_batches=1, backend_counts={"peer_buffer_direct": 1}))
    files = sorted(tmp_path.glob("*.json"))
    assert len(files) == 1
    assert "pid222" in files[0].name


def test_connector_metrics_sink_defers_hot_path_flush_until_forced(tmp_path):
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="engine-deferred",
        role="producer",
        hostname="host-0",
        rpc_port=8999,
        tp_rank=0,
        flush_interval_s=3600.0,
    )

    sink.record(LayeredTransferWorkerMeta(grouped_batches=1))
    assert ConnectorMetricsReader(tmp_path).aggregate().totals.grouped_batches == 1

    sink.record(LayeredTransferWorkerMeta(grouped_batches=2))
    # The second delta stays in process memory until an interval tick or a
    # request-terminal forced flush. This is the sender hot-path contract.
    assert ConnectorMetricsReader(tmp_path).aggregate().totals.grouped_batches == 1

    assert sink.flush(force=True) is True
    assert ConnectorMetricsReader(tmp_path).aggregate().totals.grouped_batches == 3


def test_serving_control_plane_records_cross_step_reuse_in_hot_path_registry(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-hot-reuse.jsonl"))
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-hot-reuse"),
        workflow_registry=registry,
    )
    cp.register_stage_workers("prefill", ["prefill-0", "prefill-1"])
    cp.register_stage_workers("decode", ["decode-0"])

    req0 = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": "shared prefix question"}]}],
        "metadata": {"workflow_id": "wf-hot-reuse"},
    }
    ctx0 = cp.start_request(req0, "req-hot-0")
    prefill0 = cp.admit_stage("prefill", ctx0)
    cp.build_prefill_kv_params(ctx0, prefill0, decode_worker_id="decode-0")
    kv0 = cp.note_prefill_response(
        ctx0,
        {
            "transfer_id": "xfer-hot-0",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[501, 502]],
        },
        decode_worker_id="decode-0",
    )
    cp.mark_stage_started("prefill", prefill0.worker_id, ctx=ctx0)
    cp.mark_stage_complete(
        "prefill",
        prefill0.worker_id,
        latency_ms=10.0,
        success=True,
        ctx=ctx0,
    )
    decode0 = cp.admit_stage("decode", ctx0)
    cp.build_decode_kv_params(ctx0, decode0, kv0)
    cp.commit_handoff(ctx0)
    cp.finish_request(ctx0.request_id)

    req1 = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": "shared prefix question followup"}]}],
        "metadata": {"workflow_id": "wf-hot-reuse"},
    }
    ctx1 = cp.start_request(req1, "req-hot-1")
    assert ctx1.reuse_telemetry["cross_step"] is True
    assert ctx1.reuse_telemetry["reused_tokens"] >= 3
    assert ctx1.reuse_candidate_block_ids == ["prefill-engine:501", "prefill-engine:502"]

    prefill1 = cp.admit_stage("prefill", ctx1)
    # Workflow affinity should prefer the same prefill worker used by req0.
    assert prefill1.worker_id == "prefill-0"
    prefill_kv1 = cp.build_prefill_kv_params(ctx1, prefill1, decode_worker_id="decode-0")
    assert prefill_kv1["serving_reuse_telemetry"]["cross_step"] is True
    assert prefill_kv1["serving_reuse_candidate_block_ids"] == ["prefill-engine:501", "prefill-engine:502"]

    snapshot = cp.snapshot()
    assert snapshot["metrics"]["serving_cross_step_reuse_candidates"] == 1
    assert snapshot["metrics"]["serving_cross_step_reused_tokens"] >= 3
    assert snapshot["metrics"]["serving_workflow_state_commits"] == 1
    assert snapshot["metrics"]["serving_workflow_affinity_hits"] == 1
    assert snapshot["metrics"]["path_stats"]["PD"]["serving_workflow_state_commits"] == 1
    assert snapshot["metrics"]["path_stats"]["PD"]["serving_workflow_affinity_hits"] == 1
    reuse_summary = snapshot["workflow_registry"]["reuse_summary"]
    assert reuse_summary["cross_step_records"] >= 1
    assert reuse_summary["total_reused_tokens"] >= 3


def test_serving_control_plane_forks_workflow_state_zero_copy(tmp_path):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-agent-fork.jsonl"))
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-agent-fork",
            enable_agent_state_clone=True,
        ),
        workflow_registry=registry,
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-fork-parent")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    kv = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-fork",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[701, 702]],
        },
        decode_worker_id="decode-0",
    )
    decode = cp.admit_stage("decode", ctx)
    cp.build_decode_kv_params(ctx, decode, kv)
    cp.commit_handoff(ctx)
    cp.finish_request(ctx.request_id)

    # Agent cloning keeps the concrete directory records pinned after request
    # completion so later branches can share real owner-shard KV descriptors.
    assert cp.kv_directory.get_record("prefill-engine:701") is not None
    result = cp.fork_workflow_state(
        workflow_id="wf-mm-1",
        parent_request_id="req-fork-parent",
        branch_count=3,
        target_node_id="decode-0",
    )

    assert result["zero_copy_branches"] == 3
    assert result["copied_bytes"] == 0
    assert result["kv_block_ids"] == ["prefill-engine:701", "prefill-engine:702"]
    assert result["refcounts"]["prefill-engine:701"] == 4
    records = [registry.get_record(branch["branch_id"]) for branch in result["branches"]]
    assert all(record is not None and record.status == "AGENT_BRANCH_ACTIVE" for record in records)

    snapshot = cp.snapshot()
    assert snapshot["metrics"]["agent_state_clone_requests"] == 1
    assert snapshot["metrics"]["agent_state_clone_branches"] == 3
    assert snapshot["metrics"]["agent_state_clone_zero_copy_branches"] == 3
    assert snapshot["metrics"]["agent_state_clone_copied_bytes"] == 0


def test_serving_control_plane_rolls_back_partial_agent_fork(tmp_path, monkeypatch):
    registry = WorkflowStateRegistry(str(tmp_path / "serving-agent-fork-rollback.jsonl"))
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-agent-fork-rollback",
            enable_agent_state_clone=True,
        ),
        workflow_registry=registry,
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    ctx = cp.start_request(_mm_request(), "req-fork-rollback-parent")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    kv = cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer-fork-rollback",
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap",
            "remote_block_ids": [[801, 802]],
        },
        decode_worker_id="decode-0",
    )
    decode = cp.admit_stage("decode", ctx)
    cp.build_decode_kv_params(ctx, decode, kv)
    cp.commit_handoff(ctx)
    cp.finish_request(ctx.request_id)

    original_upsert = registry.upsert_record
    calls = 0

    def fail_second_upsert(record, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated workflow registry failure")
        return original_upsert(record, **kwargs)

    monkeypatch.setattr(registry, "upsert_record", fail_second_upsert)
    with pytest.raises(RuntimeError, match="simulated workflow registry failure"):
        cp.fork_workflow_state(
            workflow_id="wf-mm-1",
            parent_request_id="req-fork-rollback-parent",
            branch_count=2,
            target_node_id="decode-0",
            for_write=True,
        )

    for gid in ("prefill-engine:801", "prefill-engine:802"):
        record = cp.kv_directory.get_record(gid)
        assert record is not None
        assert record.refcount == 1
        assert record.lease_count == 0
    records = [
        row
        for row in registry.workflow_records("wf-mm-1")
        if row.state_id != "req-fork-rollback-parent"
    ]
    assert records
    assert all(row.status == "RELEASED" for row in records)



def test_connector_reader_aggregates_decode_engine_timing(tmp_path):
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    payloads = [
        {
            "version": 1,
            "kind": "decode_engine_timing",
            "updated_at": 10.0,
            "identity": {"pid": 1, "role": "decode"},
            "metrics": {
                "first_token_requests": 2,
                "first_token_latency_ms_total": 120.0,
                "kv_first_token_requests": 2,
                "kv_first_token_latency_ms_total": 120.0,
                "kv_first_token_output_tokens": 2,
                "scheduler_update_calls": 7,
            },
        },
        {
            "version": 1,
            "kind": "decode_engine_timing",
            "updated_at": 11.0,
            "identity": {"pid": 2, "role": "decode"},
            "metrics": {
                "first_token_requests": 1,
                "first_token_latency_ms_total": 30.0,
                "kv_first_token_requests": 0,
                "kv_first_token_latency_ms_total": 0.0,
                "kv_first_token_output_tokens": 0,
                "scheduler_update_calls": 3,
            },
        },
        {
            "version": 1,
            "identity": {"pid": 3, "role": "decode"},
            "updated_at": 12.0,
            "totals": LayeredTransferWorkerMeta(receive_kv_requests=99).to_dict(),
        },
    ]
    for idx, payload in enumerate(payloads):
        (metrics_dir / f"worker-{idx}.json").write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )

    from mooncake_epd.core.control.connector_metrics import ConnectorMetricsReader

    agg = ConnectorMetricsReader(str(metrics_dir)).aggregate_decode_engine_timing()
    assert agg["workers"] == 2
    assert agg["updated_at_max"] == 11.0
    assert agg["first_token_requests"] == 3
    assert agg["first_token_latency_ms_total"] == 150.0
    assert agg["first_token_latency_ms_avg"] == 50.0
    assert agg["kv_first_token_requests"] == 2
    assert agg["kv_first_token_latency_ms_avg"] == 60.0
    assert agg["kv_first_token_output_tokens"] == 2
    assert agg["scheduler_update_calls"] == 10


def test_serving_snapshot_exposes_decode_engine_timing(tmp_path):
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    (metrics_dir / "decode.json").write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "decode_engine_timing",
                "updated_at": time.time(),
                "metrics": {
                    "first_token_requests": 4,
                    "first_token_latency_ms_total": 200.0,
                    "kv_first_token_requests": 3,
                    "kv_first_token_latency_ms_total": 180.0,
                    "kv_first_token_output_tokens": 3,
                    "scheduler_update_calls": 12,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-decode-timing",
            connector_metrics_dir=str(metrics_dir),
        )
    )
    metrics = cp.snapshot()["metrics"]
    assert metrics["decode_engine_timing_workers"] == 1
    assert metrics["decode_engine_first_token_requests"] == 4
    assert metrics["decode_engine_first_token_latency_ms_total"] == 200.0
    assert metrics["decode_engine_first_token_latency_ms_avg"] == 50.0
    assert metrics["decode_engine_kv_first_token_requests"] == 3
    assert metrics["decode_engine_kv_first_token_latency_ms_total"] == 180.0
    assert metrics["decode_engine_kv_first_token_latency_ms_avg"] == 60.0
    assert metrics["decode_engine_kv_first_token_output_tokens"] == 3
    assert metrics["decode_engine_scheduler_update_calls"] == 12
