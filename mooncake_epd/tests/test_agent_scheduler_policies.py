from __future__ import annotations

from mooncake_epd.agent.coordination import (
    AgentRequest,
    AgentScheduler,
    AgentType,
    SchedulingPolicy,
    WorkerLoad,
)


def _workers():
    return [
        WorkerLoad(
            worker_id="prefill-a",
            worker_type="prefill",
            current_load=2,
            max_capacity=8,
            queue_size=2,
            avg_latency_ms=30,
            pool_tags=["high-prefill"],
        ),
        WorkerLoad(
            worker_id="prefill-b",
            worker_type="prefill",
            current_load=0,
            max_capacity=8,
            queue_size=0,
            avg_latency_ms=80,
            pool_tags=["standard-prefill"],
        ),
    ]


def test_least_loaded_and_round_robin_policies_are_explicit_and_deterministic():
    request = AgentRequest("r", AgentType.THINKING, input_tokens=1024, metadata={"stage": "prefill"})
    least_loaded = AgentScheduler(_workers(), policy=SchedulingPolicy.LEAST_LOADED)
    assert least_loaded.route(request).worker_id == "prefill-b"  # type: ignore[union-attr]

    round_robin = AgentScheduler(_workers(), policy=SchedulingPolicy.ROUND_ROBIN)
    assert round_robin.route(request).worker_id == "prefill-a"  # type: ignore[union-attr]
    assert round_robin.route(request).worker_id == "prefill-b"  # type: ignore[union-attr]


def test_static_type_policy_prefers_pool_but_never_selects_unhealthy_worker():
    request = AgentRequest("r", AgentType.THINKING, input_tokens=4096, metadata={"stage": "prefill"})
    scheduler = AgentScheduler(_workers(), policy=SchedulingPolicy.STATIC_TYPE_ROUTE)
    assert scheduler.route(request).worker_id == "prefill-a"  # type: ignore[union-attr]
    scheduler.update_load("prefill-a", healthy=False)
    assert scheduler.route(request).worker_id == "prefill-b"  # type: ignore[union-attr]


def test_agent_aware_trace_contains_explainable_scores():
    request = AgentRequest("r", AgentType.THINKING, input_tokens=4096, metadata={"stage": "prefill"})
    scheduler = AgentScheduler(_workers(), policy=SchedulingPolicy.AGENT_AWARE)
    worker = scheduler.route(request)
    trace = scheduler.last_route_trace()

    assert worker is not None
    assert trace["chosen_worker_id"] == worker.worker_id
    assert set(trace["scores"]) == {"prefill-a", "prefill-b"}
    assert "capacity" in trace["scores"][worker.worker_id]
    assert "predicted_time_cost_ms" in trace["scores"][worker.worker_id]
    assert "telemetry_confidence" in trace["scores"][worker.worker_id]


def test_agent_aware_affinity_escapes_when_queue_cost_exceeds_threshold():
    workers = [
        WorkerLoad(
            worker_id="decode-local",
            worker_type="decode",
            queue_size=8,
            current_load=8,
            max_capacity=16,
            queue_capacity=16,
            service_rate=20.0,
            avg_first_token_ms=120.0,
        ),
        WorkerLoad(
            worker_id="decode-free",
            worker_type="decode",
            queue_size=0,
            current_load=0,
            max_capacity=16,
            queue_capacity=16,
            service_rate=20.0,
            avg_first_token_ms=30.0,
        ),
    ]
    scheduler = AgentScheduler(
        workers, policy=SchedulingPolicy.AGENT_AWARE
    )
    request = AgentRequest(
        "interactive",
        AgentType.INTERACTIVE,
        input_tokens=64,
        output_tokens=32,
        metadata={
            "stage": "decode",
            "preferred_worker_id": "decode-local",
            "affinity_benefit_ms": 3.0,
            "affinity_congestion_escape_ms": 25.0,
        },
    )

    selected = scheduler.route(request)

    assert selected is not None
    assert selected.worker_id == "decode-free"
    assert scheduler.stats()["congestion_escapes"] == 1

    # A small improvement below the hysteresis re-entry threshold must not
    # immediately pin the next request back to the preferred peer.
    workers[0].queue_size = 0
    workers[0].current_load = 0
    workers[0].avg_first_token_ms = 50.0
    workers[1].avg_first_token_ms = 30.0
    selected_again = scheduler.route(request)
    assert selected_again is not None
    assert selected_again.worker_id == "decode-free"
    assert scheduler.stats()["congestion_escapes"] == 1

    workers[0].avg_first_token_ms = 30.0
    selected_reentry = scheduler.route(request)
    assert selected_reentry is not None
    assert selected_reentry.worker_id == "decode-local"
    assert scheduler.stats()["congestion_reentries"] == 1


def test_unknown_telemetry_is_neutral_and_reduces_confidence():
    unknown = WorkerLoad(
        worker_id="decode-unknown",
        worker_type="decode",
        max_capacity=16,
    )
    known = WorkerLoad(
        worker_id="decode-known",
        worker_type="decode",
        max_capacity=16,
        telemetry_known={
            "gpu_utilization": True,
            "avg_latency_ms": True,
            "kv_free_blocks": True,
            "transfer_backlog_bytes": True,
            "transfer_bandwidth_bytes_per_s": True,
        },
        gpu_utilization=0.0,
        avg_latency_ms=20.0,
        kv_free_blocks=16,
        transfer_backlog_bytes=0,
        transfer_bandwidth_bytes_per_s=1_000_000_000.0,
    )
    scheduler = AgentScheduler([unknown, known])
    request = AgentRequest(
        "interactive",
        AgentType.INTERACTIVE,
        input_tokens=32,
        output_tokens=16,
        metadata={"stage": "decode"},
    )

    unknown_score = scheduler.score_components(request, unknown)
    known_score = scheduler.score_components(request, known)

    assert unknown.telemetry_is_known("gpu_utilization") is False
    assert known.telemetry_is_known("gpu_utilization") is True
    assert unknown_score["telemetry_confidence"] < known_score[
        "telemetry_confidence"
    ]
    assert unknown_score["telemetry_uncertainty_penalty"] < 0


def test_telemetry_none_marks_signal_unknown_without_faking_zero():
    worker = WorkerLoad(
        worker_id="decode-0",
        worker_type="decode",
        gpu_utilization=0.75,
    )
    scheduler = AgentScheduler([worker])

    scheduler.update_load("decode-0", gpu_utilization=None)

    assert worker.gpu_utilization == 0.75
    assert worker.telemetry_is_known("gpu_utilization") is False
