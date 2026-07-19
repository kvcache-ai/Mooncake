from __future__ import annotations

import json
from pathlib import Path

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.scripts.build_agent_pd_dataset import build_rows, write_jsonl
from mooncake_epd.scripts.run_agent_pd_scheduler_eval import run as run_agent_pd_eval


def _cp() -> ServingControlPlane:
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            high_prefill_worker_ids=("prefill-high-0",),
            standard_prefill_worker_ids=("prefill-standard-0",),
            low_latency_decode_worker_ids=("decode-low-0",),
            standard_decode_worker_ids=("decode-standard-0",),
        )
    )
    cp.register_stage_workers("prefill", ["prefill-high-0", "prefill-standard-0"])
    cp.register_stage_workers("decode", ["decode-low-0", "decode-standard-0"])
    cp.update_worker_load("prefill", "prefill-high-0", max_capacity=96, avg_latency_ms=180.0)
    cp.update_worker_load("prefill", "prefill-standard-0", max_capacity=48, avg_latency_ms=40.0)
    cp.update_worker_load("decode", "decode-low-0", max_capacity=128, avg_latency_ms=10.0)
    cp.update_worker_load("decode", "decode-standard-0", max_capacity=96, avg_latency_ms=60.0)
    return cp


def _request(agent_type: str, routing_target: str, *, task_id: str = "req"):
    return {
        "messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}],
        "metadata": {
            "workflow_id": f"wf-{task_id}",
            "agent_type": agent_type,
            "routing_target": routing_target,
            "input_tokens_bucket": "8k-16k" if agent_type == "thinking" else "0-512",
            "expected_output_tokens_bucket": "1k-2k" if agent_type == "thinking" else "0-128",
            "latency_sensitivity": "high" if agent_type != "thinking" else "medium",
            "quality_priority": "accuracy" if agent_type == "thinking" else "latency",
        },
    }


def test_agent_pd_routes_thinking_to_high_prefill_and_standard_decode():
    cp = _cp()
    ctx = cp.start_request(_request("thinking", "high_prefill_pool", task_id="think"), "think")

    prefill = cp.admit_stage("prefill", ctx)
    decode = cp.admit_stage("decode", ctx)

    assert prefill.worker_id == "prefill-high-0"
    assert decode.worker_id == "decode-standard-0"
    prefill_kv = cp.build_prefill_kv_params(ctx, prefill, decode_worker_id=decode.worker_id)
    assert prefill_kv["agent_pd_prefill_pool"] == "high_prefill_pool"
    assert prefill_kv["agent_pd_decode_pool"] == "standard_decode_pool"
    assert prefill_kv["agent_pd_routing_target"] == "high_prefill_pool"


def test_agent_pd_routes_interactive_to_low_latency_decode_and_standard_prefill():
    cp = _cp()
    ctx = cp.start_request(_request("interactive", "low_latency_decode_pool", task_id="interactive"), "interactive")

    prefill = cp.admit_stage("prefill", ctx)
    decode = cp.admit_stage("decode", ctx)

    assert prefill.worker_id == "prefill-standard-0"
    assert decode.worker_id == "decode-low-0"
    snapshot = cp.snapshot()
    assert snapshot["metrics"]["agent_pd_route_correct"] == 2
    assert snapshot["metrics"]["agent_pd_stage_pool_counts"]["decode"]["low_latency_decode_pool"] == 1


def test_agent_pd_routes_hybrid_to_high_prefill_and_low_latency_decode():
    cp = _cp()
    ctx = cp.start_request(_request("hybrid", "mixed", task_id="hybrid"), "hybrid")

    prefill = cp.admit_stage("prefill", ctx)
    decode = cp.admit_stage("decode", ctx)

    assert prefill.worker_id == "prefill-high-0"
    assert decode.worker_id == "decode-low-0"
    snapshot = cp.snapshot()
    assert snapshot["metrics"]["agent_pd_hybrid_fast_path"] == 1


def test_single_worker_stage_is_a_member_of_both_logical_agent_pools():
    """1P1D cannot be scored as an exclusive high/low physical partition."""

    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            high_prefill_worker_ids=("prefill-0",),
            low_latency_decode_worker_ids=("decode-0",),
        )
    )
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])

    for index, (agent_type, target) in enumerate(
        (
            ("interactive", "low_latency_decode_pool"),
            ("thinking", "high_prefill_pool"),
            ("hybrid", "mixed"),
        )
    ):
        ctx = cp.start_request(_request(agent_type, target, task_id=f"single-{index}"), f"single-{index}")
        assert cp.admit_stage("prefill", ctx).worker_id == "prefill-0"
        assert cp.admit_stage("decode", ctx).worker_id == "decode-0"

    snapshot = cp.snapshot()
    assert snapshot["metrics"]["agent_pd_route_total"] == 6
    assert snapshot["metrics"]["agent_pd_route_correct"] == 6
    assert snapshot["metrics"]["agent_pd_route_incorrect"] == 0


def test_agent_pd_pool_preference_spills_when_preferred_prefill_is_congested():
    cp = _cp()
    cp.update_worker_load(
        "prefill",
        "prefill-high-0",
        current_load=90,
        queue_size=40,
        queue_capacity=128,
    )
    ctx = cp.start_request(_request("thinking", "high_prefill_pool", task_id="spill"), "spill")

    prefill = cp.admit_stage("prefill", ctx)

    # Pool tags are a locality/role hint. They must not pin a request to a
    # congested worker when a healthy compatible Prefill worker is available.
    assert prefill.worker_id == "prefill-standard-0"


def test_cross_step_affinity_overrides_interactive_prefill_pool_when_reuse_is_available():
    cp = _cp()

    first = _request("thinking", "high_prefill_pool", task_id="agent-reuse")
    first["messages"][0]["content"][0]["text"] = "shared agent prefix plan"
    first["metadata"]["workflow_id"] = "wf-agent-reuse"
    ctx0 = cp.start_request(first, "agent-reuse-0")
    prefill0 = cp.admit_stage("prefill", ctx0)
    assert prefill0.worker_id == "prefill-high-0"
    cp.build_prefill_kv_params(ctx0, prefill0, decode_worker_id="decode-standard-0")
    cp.note_prefill_response(
        ctx0,
        {
            "transfer_id": "xfer-agent-reuse-0",
            "remote_engine_id": "prefill-high-engine",
            "remote_bootstrap_addr": "http://prefill-high-bootstrap",
            "remote_block_ids": [101, 102],
        },
        decode_worker_id="decode-standard-0",
    )
    cp.mark_stage_started(
        "prefill", prefill0.worker_id, ctx=ctx0
    )
    cp.mark_stage_complete(
        "prefill",
        prefill0.worker_id,
        latency_ms=10.0,
        success=True,
        ctx=ctx0,
    )

    second = _request("interactive", "low_latency_decode_pool", task_id="agent-reuse")
    second["messages"][0]["content"][0]["text"] = "shared agent prefix plan continue"
    second["metadata"]["workflow_id"] = "wf-agent-reuse"
    ctx1 = cp.start_request(second, "agent-reuse-1")
    assert ctx1.reuse_candidate_block_ids == [
        "prefill-high-engine:101",
        "prefill-high-engine:102",
    ]

    prefill1 = cp.admit_stage("prefill", ctx1)
    assert prefill1.worker_id == "prefill-high-0"
    snapshot = cp.snapshot()
    recent = snapshot["metrics"]["agent_pd_route_decisions_recent"]
    assert recent[-1]["stage"] == "prefill"
    assert recent[-1]["affinity_override"] is True
    assert recent[-1]["route_correct"] is True
    assert snapshot["metrics"]["serving_workflow_affinity_hits"] == 1


def test_agent_pd_dataset_and_traces_have_required_fields(tmp_path):
    rows = build_rows(20, seed=7)
    tasks = tmp_path / "tasks.jsonl"
    write_jsonl(tasks, rows)

    class Args:
        tasks_jsonl = str(tasks)
        output_dir = str(tmp_path)
        traces_jsonl = None
        model = "Qwen3.5-9B"

    summary = run_agent_pd_eval(Args())
    assert summary["count"] == 20
    assert summary["route_correct_rate"] >= 0.98
    traces_path = Path(summary["traces_jsonl"])
    first = json.loads(traces_path.read_text(encoding="utf-8").splitlines()[0])
    required = {
        "task_id",
        "input_tokens",
        "output_tokens",
        "prefill_time_ms",
        "decode_time_ms",
        "ttft_ms",
        "tpot_ms",
        "itl_p50_ms",
        "itl_p95_ms",
        "kv_cache_mb",
        "gpu_memory_peak_mb",
        "route_decision",
        "route_correct",
        "quality_score",
        "sla_satisfied",
    }
    assert required.issubset(first)
