from __future__ import annotations

import pytest
import torch

from mooncake_epd.agent.coordination import Workflow, WorkflowStep
from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.control.connector_metrics import ConnectorMetricsSink
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta
from mooncake_epd.core.state import (
    FeatureStore,
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


def test_multimodal_uuid_identity_survives_payload_omission():
    full = {
        "type": "image_url",
        "image_url": {"url": "data:image/png;base64,ZmFrZQ=="},
        "uuid": "asset-123",
    }
    compact = {
        "type": "image_url",
        "image_url": None,
        "uuid": "asset-123",
    }

    assert ServingControlPlane._stable_mm_hash(full) == (
        ServingControlPlane._stable_mm_hash(compact)
    )
    assert ServingControlPlane._stable_mm_hash(compact) != (
        ServingControlPlane._stable_mm_hash(compact | {"uuid": "asset-456"})
    )
    assert ServingControlPlane._stable_mm_hash(compact) != (
        ServingControlPlane._stable_mm_hash(compact | {"type": "audio_url"})
    )


def test_decode_worker_hint_targets_registered_worker():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-targeted"))
    cp.register_stage_workers("decode", ["decode-0", "decode-1"])
    request = _mm_request()
    request["metadata"]["mooncake_epd_decode_worker_id"] = "decode-1"

    ctx = cp.start_request(request, "req-targeted")
    decision = cp.admit_stage("decode", ctx)

    assert ctx.decode_worker_hint == "decode-1"
    assert decision.worker_id == "decode-1"


def test_decode_worker_hint_rejects_unknown_worker():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-targeted"))
    cp.register_stage_workers("decode", ["decode-0"])
    request = _mm_request()
    request["metadata"]["mooncake_epd_decode_worker_id"] = "decode-missing"
    ctx = cp.start_request(request, "req-targeted-missing")

    with pytest.raises(RuntimeError, match="not registered"):
        cp.admit_stage("decode", ctx)


def test_unavailable_decode_worker_is_hard_excluded_and_recovers():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-health"))
    cp.register_stage_workers("decode", ["decode-0", "decode-1"])
    cp.set_stage_worker_available("decode", "decode-1", available=False)

    automatic = cp.start_request(_mm_request(), "req-health-auto")
    automatic_decision = cp.admit_stage("decode", automatic)
    assert automatic_decision.worker_id == "decode-0"
    cp.mark_stage_complete("decode", "decode-0", latency_ms=1.0)
    assert cp.snapshot()["worker_health"]["decode"] == {
        "decode-0": True,
        "decode-1": False,
    }

    targeted_request = _mm_request()
    targeted_request["metadata"]["mooncake_epd_decode_worker_id"] = "decode-1"
    targeted = cp.start_request(targeted_request, "req-health-targeted")
    with pytest.raises(RuntimeError, match="worker is unavailable"):
        cp.admit_stage("decode", targeted)

    cp.set_stage_worker_available("decode", "decode-1", available=True)
    recovered = cp.start_request(targeted_request, "req-health-recovered")
    assert cp.admit_stage("decode", recovered).worker_id == "decode-1"


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


def test_serving_control_plane_records_stage_conservation_with_overlap():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-timing"))
    ctx = cp.start_request(
        {"messages": [{"role": "user", "content": "hello"}]},
        "req-timing",
        created_at=100.0,
    )
    cp.record_stage_span(ctx, "proxy_parse", started_at=100.0, ended_at=102.0)
    cp.record_stage_span(ctx, "prefill_dispatch", started_at=101.0, ended_at=104.0)
    cp.record_stage_span(ctx, "handoff_commit", started_at=104.0, ended_at=105.0)

    summary = cp.mark_first_token(ctx, emitted_at=105.0)
    duplicate = cp.mark_first_token(ctx, emitted_at=106.0)
    cp.finish_request(ctx.request_id)

    assert summary == duplicate
    assert summary["critical_path_ms"] == 5000.0
    assert summary["raw_sum_ms"] == 6000.0
    assert summary["accounted_union_ms"] == 5000.0
    assert summary["overlap_ms"] == 1000.0
    assert summary["unattributed_ms"] == 0.0
    assert summary["stage_conservation_ok"] is True

    metrics = cp.snapshot()["metrics"]
    assert metrics["first_token_ms"]["count"] == 1
    assert metrics["first_token_ms"]["p95"] == 5000.0
    assert metrics["request_stage_timing_ms"]["proxy_parse"]["count"] == 1
    assert metrics["request_stage_timing_ms"]["prefill_dispatch"]["avg"] == 3000.0
    assert metrics["stage_conservation"]["count"] == 1
    assert metrics["stage_conservation"]["ok_count"] == 1
    assert metrics["stage_conservation"]["ok_rate"] == 1.0


def test_serving_control_plane_exposes_unavailable_timing_stats_without_zero_samples():
    metrics = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-no-timing")
    ).snapshot()["metrics"]

    assert metrics["first_token_ms"] == {
        "count": 0,
        "avg": None,
        "p50": None,
        "p95": None,
        "p99": None,
        "max": None,
    }
    assert metrics["request_stage_timing_ms"] == {}
    assert metrics["stage_conservation"]["count"] == 0
    assert metrics["stage_conservation"]["ok_rate"] is None


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
        force_relay=True,
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
            transfer_attempts=3,
            transfer_successes=3,
            transfer_bytes=192,
            transfer_elapsed_ms=3.0,
            transfer_attempt_elapsed_ms=3.0,
            backend_counts={
                "peer_buffer_direct": 2,
                "batch_transfer_fallback": 1,
            },
            backend_bytes={
                "peer_buffer_direct": 128,
                "batch_transfer_fallback": 64,
            },
            backend_elapsed_ms={
                "peer_buffer_direct": 2.0,
                "batch_transfer_fallback": 1.0,
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
    assert metrics["fallback_batches"] == 1
    assert metrics["kv_transfer_attempts"] == 3
    assert metrics["kv_transfer_successes"] == 3
    assert metrics["kv_transfer_bytes"] == 192
    assert metrics["kv_transfer_elapsed_ms"] == 3.0
    assert metrics["kv_transfer_elapsed_ms_avg"] == 1.0
    assert metrics["kv_transfer_gbps"] == 0.000512
    assert metrics["remote_transfer_backend_gbps"] == {
        "batch_transfer_fallback": 0.000512,
        "peer_buffer_direct": 0.000512,
    }
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
