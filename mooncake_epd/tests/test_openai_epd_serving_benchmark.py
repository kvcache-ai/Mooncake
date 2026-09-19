from __future__ import annotations

import copy

from mooncake_epd.scripts.benchmark_openai_epd_serving import (
    _assign_stable_mm_uuids,
    _compact_mm_uuid_entry,
    _decode_worker_ids_from_metrics,
    _measured_proxy_stage_timing,
    _request_entry,
    _strict_epd_metric_gate,
    _target_decode_worker_entry,
    summarize_run,
)


def _row(index: int, *, ttft: float, tpot: float, elapsed: float, route: str = "EPD"):
    return {
        "index": index,
        "status_code": 200,
        "response_content_len": 4,
        "completion_tokens": 4,
        "prompt_tokens": 10,
        "ttft_ms": ttft,
        "tpot_ms": tpot,
        "elapsed_ms": elapsed,
        "routing_path": route,
        "admission": "ADMIT",
        "degrade_level": "NONE",
        "workload_family": "W0",
    }


def test_summarize_real_endpoint_run_reports_latency_throughput_and_goodput():
    summary = summarize_run(
        [
            _row(0, ttft=100.0, tpot=10.0, elapsed=130.0),
            _row(1, ttft=200.0, tpot=20.0, elapsed=260.0),
        ],
        concurrency=2,
        elapsed_s=1.0,
        goodput_ttft_ms=150.0,
        goodput_tpot_ms=15.0,
        expected_routing_path="EPD",
    )

    assert summary["successful"] == 2
    assert summary["request_throughput_rps"] == 2.0
    assert summary["output_token_throughput_tps"] == 8.0
    assert summary["ttft_ms"]["p50"] == 150.0
    assert summary["goodput_count"] == 1
    assert summary["route_mismatch_count"] == 0
    assert summary["admission_mismatch_count"] == 0
    assert summary["degrade_mismatch_count"] == 0


def test_summarize_real_endpoint_run_rejects_empty_or_wrong_route_samples():
    empty = _row(0, ttft=100.0, tpot=10.0, elapsed=130.0)
    empty["response_content_len"] = 0
    wrong_route = _row(1, ttft=100.0, tpot=10.0, elapsed=130.0, route="PD")

    summary = summarize_run(
        [empty, wrong_route],
        concurrency=1,
        elapsed_s=1.0,
        goodput_ttft_ms=0.0,
        goodput_tpot_ms=0.0,
        expected_routing_path="EPD",
    )

    assert summary["successful"] == 1
    assert summary["failed"] == 1
    assert summary["route_mismatch_count"] == 1


def test_measured_proxy_stage_timing_excludes_warmup_and_process_history():
    measured_rows = [
        _row(0, ttft=100.0, tpot=10.0, elapsed=130.0),
        _row(1, ttft=200.0, tpot=20.0, elapsed=260.0),
    ]
    measured_rows[0]["workflow_id"] = "wf-run0-seq0"
    measured_rows[1]["workflow_id"] = "wf-run0-seq1"
    metrics_after = {
        "metrics": {
            "request_timing_recent": [
                {
                    "workflow_id": "wf-run-1-seq0",
                    "critical_path_ms": 9_999.0,
                    "accounted_union_ms": 9_900.0,
                    "unattributed_ms": 99.0,
                    "overlap_ms": 1.0,
                    "stage_conservation_ok": False,
                    "stage_ms": {"prefill_dispatch": 8_888.0},
                },
                {
                    "workflow_id": "wf-run0-seq0",
                    "critical_path_ms": 100.0,
                    "accounted_union_ms": 95.0,
                    "unattributed_ms": 5.0,
                    "overlap_ms": 20.0,
                    "stage_conservation_ok": True,
                    "stage_ms": {
                        "prefill_dispatch": 60.0,
                        "decode_first_chunk_wait": 30.0,
                    },
                },
                {
                    "workflow_id": "wf-run0-seq1",
                    "critical_path_ms": 200.0,
                    "accounted_union_ms": 180.0,
                    "unattributed_ms": 20.0,
                    "overlap_ms": 40.0,
                    "stage_conservation_ok": False,
                    "stage_ms": {
                        "prefill_dispatch": 100.0,
                        "decode_first_chunk_wait": 70.0,
                    },
                },
            ]
        }
    }

    evidence = _measured_proxy_stage_timing(
        metrics_after=metrics_after,
        runs=[{"concurrency": 4, "successful": 2, "raw_results": measured_rows}],
        workflow_mode="unique",
    )

    assert evidence["supported"] is True
    assert evidence["matched_requests"] == 2
    assert evidence["coverage"] == 1.0
    run = evidence["runs"][0]
    assert run["critical_path_ms"]["p50"] == 150.0
    assert run["stages_ms"]["prefill_dispatch"]["p50"] == 80.0
    assert run["stage_conservation"] == {
        "ok_count": 1,
        "failed_count": 1,
        "ok_rate": 0.5,
    }


def test_measured_proxy_stage_timing_rejects_stable_workflow_ids():
    evidence = _measured_proxy_stage_timing(
        metrics_after={"metrics": {"request_timing_recent": []}},
        runs=[],
        workflow_mode="stable",
    )

    assert evidence["supported"] is False
    assert "cannot distinguish warmup" in evidence["reason"]


def test_unique_workflow_ids_are_namespaced_per_benchmark_invocation():
    source = {
        "request": {"metadata": {"workflow_id": "wf"}},
        "sample": {"sample_id": "sample"},
    }

    first = _request_entry(
        source,
        run_index=0,
        sequence=0,
        workflow_mode="unique",
        benchmark_id="first",
    )
    second = _request_entry(
        source,
        run_index=0,
        sequence=0,
        workflow_mode="unique",
        benchmark_id="second",
    )

    assert first["request"]["metadata"]["workflow_id"] == "wf-benchfirst-run0-seq0"
    assert second["request"]["metadata"]["workflow_id"] == "wf-benchsecond-run0-seq0"
    assert first["sample"]["workflow_id"] == "wf-benchfirst-run0-seq0"
    assert second["sample"]["workflow_id"] == "wf-benchsecond-run0-seq0"


def test_client_mm_uuid_benchmark_warms_full_then_compacts_media():
    entries = [
        {
            "request": {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": "data:image/png;base64,ZmFrZQ=="},
                            },
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ]
            },
            "sample": {"sample_id": "sample-0"},
            "family": "W0",
        }
    ]

    assert _assign_stable_mm_uuids(entries) == 1
    full_item = entries[0]["request"]["messages"][0]["content"][0]
    assert full_item["image_url"]["url"].startswith("data:image")
    assert full_item["uuid"].startswith("epd-mm-v1-")

    repeated = copy.deepcopy(entries)
    repeated[0]["request"]["messages"][0]["content"][0].pop("uuid")
    assert _assign_stable_mm_uuids(repeated) == 1
    assert repeated[0]["request"]["messages"][0]["content"][0]["uuid"] == (
        full_item["uuid"]
    )

    compact, count = _compact_mm_uuid_entry(entries[0])
    compact_item = compact["request"]["messages"][0]["content"][0]
    assert count == 1
    assert compact_item["image_url"] is None
    assert compact_item["uuid"] == full_item["uuid"]
    assert full_item["image_url"]["url"].startswith("data:image")


def test_decode_worker_targeting_does_not_mutate_dataset_entry():
    entry = {
        "request": {
            "messages": [{"role": "user", "content": "hello"}],
            "metadata": {"workflow_id": "wf"},
        },
        "sample": {"sample_id": "sample"},
        "family": "W0",
    }

    targeted = _target_decode_worker_entry(entry, "decode-1")

    assert targeted["request"]["metadata"]["mooncake_epd_decode_worker_id"] == (
        "decode-1"
    )
    assert "mooncake_epd_decode_worker_id" not in entry["request"]["metadata"]


def test_decode_worker_ids_from_object_and_legacy_metrics():
    assert _decode_worker_ids_from_metrics(
        {
            "workers": {
                "decode": [
                    {"worker_id": "decode-0", "current_load": 0},
                    {"worker_id": "decode-1", "current_load": 1},
                    {"worker_id": "decode-1", "current_load": 2},
                    "decode-legacy",
                    {"current_load": 3},
                ]
            }
        }
    ) == ["decode-0", "decode-1", "decode-legacy"]
    assert _decode_worker_ids_from_metrics(None) == []


def test_strict_gate_requires_all_measured_client_media_to_be_compact():
    base_metrics = {
        "requests_total": 4,
        "handoff_prepared": 4,
        "handoff_committed": 4,
        "handoff_rolled_back": 0,
        "fallback_batches": 0,
        "fallback_bytes": 0,
        "layered_receive_failures": 0,
        "layered_transfer_failed_batches": 0,
        "remote_transfer_backend_failures": 0,
        "kv_transfer_successes": 20,
        "degrade_level_counts": {"NONE": 8},
    }
    before = {
        "config": {"strict_no_fallback": True},
        "metrics": base_metrics,
        "client_mm_uuid_references": {
            "enabled": True,
            "legacy_full_requests": 0,
            "uuid_full_requests": 4,
            "compact_requests": 0,
            "mixed_requests": 0,
            "compact_items": 0,
            "compact_request_body_bytes": 0,
            "cold_misses": 0,
        },
    }
    after = {
        "config": {"strict_no_fallback": True},
        "metrics": {
            **base_metrics,
            "requests_total": 7,
            "handoff_prepared": 7,
            "handoff_committed": 7,
            "kv_transfer_successes": 35,
            "degrade_level_counts": {"NONE": 14},
        },
        "direct_feature_handle_cache": {"inflight": 0},
        "prefill_render_cache": {"inflight": 0},
        "client_mm_uuid_references": {
            "enabled": True,
            "legacy_full_requests": 0,
            "uuid_full_requests": 4,
            "compact_requests": 3,
            "mixed_requests": 0,
            "compact_items": 3,
            "compact_request_body_bytes": 2400,
            "cold_misses": 0,
        },
    }

    failures, evidence = _strict_epd_metric_gate(
        metrics_before=before,
        metrics_after=after,
        expected_successful=3,
        require_client_mm_uuid_cache=True,
    )

    assert failures == []
    assert evidence["client_mm_uuid_reference_deltas"]["compact_requests"] == 3
    after["client_mm_uuid_references"]["cold_misses"] = 1
    failures, _ = _strict_epd_metric_gate(
        metrics_before=before,
        metrics_after=after,
        expected_successful=3,
        require_client_mm_uuid_cache=True,
    )
    assert any("cold_misses" in failure for failure in failures)


def test_strict_epd_metric_gate_checks_handoff_transfer_and_fallback_deltas():
    before = {
        "config": {"strict_no_fallback": True},
        "metrics": {
            "requests_total": 10,
            "handoff_prepared": 10,
            "handoff_committed": 10,
            "handoff_rolled_back": 0,
            "fallback_batches": 0,
            "fallback_bytes": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "remote_transfer_backend_failures": 0,
            "kv_transfer_successes": 100,
            "degrade_level_counts": {"NONE": 20},
        },
    }
    after = {
        "config": {"strict_no_fallback": True},
        "direct_feature_handle_cache": {"inflight": 0, "entries": 4},
        "metrics": {
            **before["metrics"],
            "requests_total": 14,
            "handoff_prepared": 14,
            "handoff_committed": 14,
            "kv_transfer_successes": 140,
            "degrade_level_counts": {"NONE": 28},
        },
    }

    failures, evidence = _strict_epd_metric_gate(
        metrics_before=before,
        metrics_after=after,
        expected_successful=4,
    )

    assert failures == []
    assert evidence["counter_deltas"]["handoff_committed"] == 4
    assert evidence["counter_deltas"]["kv_transfer_successes"] == 40


def test_strict_epd_metric_gate_rejects_fallback_and_inflight_leak():
    before = {
        "config": {"strict_no_fallback": True},
        "metrics": {
            "requests_total": 0,
            "handoff_prepared": 0,
            "handoff_committed": 0,
            "handoff_rolled_back": 0,
            "fallback_batches": 0,
            "kv_transfer_successes": 0,
        },
    }
    after = {
        "config": {"strict_no_fallback": True},
        "direct_feature_handle_cache": {"inflight": 1},
        "metrics": {
            **before["metrics"],
            "requests_total": 1,
            "handoff_prepared": 1,
            "handoff_committed": 1,
            "fallback_batches": 1,
            "kv_transfer_successes": 1,
        },
    }

    failures, _ = _strict_epd_metric_gate(
        metrics_before=before,
        metrics_after=after,
        expected_successful=1,
    )

    assert any("fallback_batches" in failure for failure in failures)
    assert any("inflight=1" in failure for failure in failures)


def test_strict_epd_metric_gate_proves_hot_hash_only_decode_path():
    base_metrics = {
        "requests_total": 4,
        "handoff_prepared": 4,
        "handoff_committed": 4,
        "handoff_rolled_back": 0,
        "fallback_batches": 0,
        "fallback_bytes": 0,
        "layered_receive_failures": 0,
        "layered_transfer_failed_batches": 0,
        "remote_transfer_backend_failures": 0,
        "kv_transfer_successes": 20,
        "degrade_level_counts": {"NONE": 8},
    }
    before = {
        "config": {"strict_no_fallback": True},
        "metrics": base_metrics,
        "decode_mm_hash_cache": {
            "enabled": True,
            "epoch_monitor_enabled": True,
            "lookups": 4,
            "hash_only_requests": 0,
            "full_requests": 4,
            "invalidations": 0,
            "rejected_cold_metadata_only": 0,
            "avoided_serialized_bytes": 0,
            "epoch_changes": 0,
            "epoch_probe_failures": 0,
            "epoch_probe_responses": 10,
            "epoch_probe_response_bytes": 600,
            "worker_unavailable_events": 0,
        },
    }
    after = {
        "config": {"strict_no_fallback": True},
        "workers": {
            "decode": [
                {"worker_id": "decode-0", "current_load": 0},
                {"worker_id": "decode-1", "current_load": 0},
            ]
        },
        "metrics": {
            **base_metrics,
            "requests_total": 7,
            "handoff_prepared": 7,
            "handoff_committed": 7,
            "kv_transfer_successes": 35,
            "degrade_level_counts": {"NONE": 14},
        },
        "direct_feature_handle_cache": {"inflight": 0},
        "prefill_render_cache": {"inflight": 0},
        "decode_mm_hash_cache": {
            "enabled": True,
            "epoch_monitor_enabled": True,
            "entries_by_worker": {"decode-0": 9, "decode-1": 9},
            "lookups": 7,
            "hash_only_requests": 3,
            "full_requests": 4,
            "invalidations": 0,
            "rejected_cold_metadata_only": 0,
            "avoided_serialized_bytes": 45_000_000,
            "epoch_changes": 0,
            "epoch_probe_failures": 0,
            "epoch_probe_responses": 13,
            "epoch_probe_response_bytes": 780,
            "worker_unavailable_events": 0,
        },
    }

    failures, evidence = _strict_epd_metric_gate(
        metrics_before=before,
        metrics_after=after,
        expected_successful=3,
        require_decode_mm_hash_cache=True,
    )

    assert failures == []
    assert evidence["decode_mm_hash_cache_deltas"]["hash_only_requests"] == 3
    assert evidence["decode_mm_hash_cache_deltas"]["full_requests"] == 0
    assert evidence["decode_mm_hash_cache_deltas"]["avoided_serialized_bytes"] == 45_000_000
    assert evidence["decode_mm_hash_cache_deltas"]["epoch_probe_responses"] == 3
    assert (
        evidence["decode_mm_hash_cache_deltas"][
            "epoch_probe_response_bytes_per_response"
        ]
        == 60.0
    )
