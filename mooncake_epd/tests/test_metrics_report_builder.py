from __future__ import annotations

import os
from pathlib import Path

import pytest

from mooncake_epd.benchmarks.metrics_suite import (
    MetricsCollector,
    build_rfc_eval_report,
    render_rfc_eval_report_markdown,
)


def test_build_rfc_eval_report():
    phase6 = {
        "summary": {
            "baseline_avg_ms": 100.0,
            "baseline_avg_tps": 10.0,
            "epd_avg_ttft_ms": 20.0,
            "epd_avg_decode_tps": 30.0,
            "cross_step_avg_reuse_ratio": 0.75,
        },
        "epd_step0": [
            {"encode_ms": 5.0, "prefill_ms": 6.0, "ttft_ms": 20.0, "decode_ms": 7.0, "total_ms": 40.0},
            {"encode_ms": 7.0, "prefill_ms": 8.0, "ttft_ms": 22.0, "decode_ms": 9.0, "total_ms": 44.0},
        ],
        "cross_step_reuse": [
            {"scenario": "multi_turn", "reuse_ratio": 0.8, "matched_tokens": 80, "delta_tokens": 20},
            {"scenario": "multi_turn", "reuse_ratio": 0.7, "matched_tokens": 70, "delta_tokens": 30},
            {"scenario": "tool_use_vqa", "reuse_ratio": 0.6, "matched_tokens": 60, "delta_tokens": 40},
        ],
        "ablations": {
            "B1": {"ttft_ms": 100.0},
            "B2": {"ttft_ms": 60.0},
            "B3": {"ttft_ms": 80.0},
            "B7": {
                "ttft_ms": 50.0,
                "rows": [
                    {"scenario": "multi_turn", "image": "room", "ttft_ms": 50.0},
                ],
            },
            "B8": {
                "ttft_ms": 140.0,
                "rows": [
                    {
                        "scenario": "multi_turn",
                        "image": "room",
                        "ttft_ms": 140.0,
                        "reuse_ratio": 0.875,
                        "delta_reuse_ratio": 0.5,
                        "tier1_matched_tokens": 7,
                        "tier2_reused_tokens": 6,
                        "tier2_recomputed_tokens": 6,
                        "tier3_accepted_pages": 4,
                        "delta_prefill_ms": 18.0,
                        "pipeline_overhead_ms": 12.0,
                        "relay_segments": 3,
                        "relay_recompute_segments": 2,
                    }
                ],
            },
        },
    }
    soak = {
        "summary": {
            "epd_ttft_avg_ms": 18.0,
            "epd_total_avg_ms": 35.0,
            "epd_decode_tps_avg": 31.0,
            "failures": 0,
            "gpu_peak_gb": {"enc": 1.0, "pre": 2.0, "dec": 3.0, "base": 4.0},
        },
        "dataset_summary": {
            "total_examples": 8,
            "by_scenario": {"multi_turn": 4, "tool_use_vqa": 4},
            "unique_images": ["a", "b"],
        },
        "results": [{}, {}, {}],
    }
    traces = [{"workflow_id": "wf-1"}, {"workflow_id": "wf-2"}]
    serving_e2e = {
        "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
        "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
        "workflow_registry_events": 14,
        "metrics": {
            "metrics": {
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 18,
                "layered_transfer_grouped_bytes": 8388608,
                "layered_transfer_grouped_descriptors": 1152,
                "layered_receive_group_batches": 18,
                "layered_receive_finished_reqs": 2,
                "requests_total": 2,
                "peer_buffer_batches": 18,
                "peer_buffer_bytes": 8388608,
                "path_stats": {
                    "PD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                        "stage_dispatches": {"prefill": 1, "decode": 1},
                    },
                    "EPD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                        "mm_prefetch_announced": 1,
                        "stage_dispatches": {"prefill": 1, "decode": 1},
                    },
                },
                "connector_path_stats": {
                    "PD": {
                        "grouped_batches": 4,
                        "grouped_descriptors": 256,
                        "grouped_bytes": 1048576,
                        "peer_buffer_batches": 4,
                        "peer_buffer_bytes": 1048576,
                        "received_group_batches": 9,
                        "received_finished_reqs": 1,
                    },
                    "EPD": {
                        "grouped_batches": 14,
                        "grouped_descriptors": 896,
                        "grouped_bytes": 7340032,
                        "peer_buffer_batches": 14,
                        "peer_buffer_bytes": 7340032,
                        "received_group_batches": 9,
                        "received_finished_reqs": 1,
                    },
                },
                "layer_load_wait_calls": 2304,
                "layered_receive_failures": 0,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 18},
            },
            "workflow_registry": {"enabled": True},
        },
    }
    serving_compare = {
        "summary": {
            "count": 5,
            "exact_match_rate": 0.6,
            "normalized_exact_match_rate": 0.6,
            "avg_token_jaccard": 0.8812,
            "min_token_jaccard": 0.6667,
            "same_finish_reason_rate": 1.0,
            "normalized_exact_or_high_overlap_rate": 1.0,
            "baseline_elapsed_ms_avg": 1153.83,
            "serving_elapsed_ms_avg": 1572.03,
            "pass_recommendation": True,
            "pass_recommendation_label": "pass",
            "gate_failures": [],
            "gate": {
                "high_overlap_threshold": 0.7,
                "min_pass_rate": 0.8,
                "min_same_finish_reason_rate": 0.8,
                "min_avg_token_jaccard": 0.75,
                "min_token_jaccard_threshold": 0.6,
            },
        },
        "rows": [{"index": 0}],
    }

    report = build_rfc_eval_report(
        phase6,
        soak,
        traces,
        serving_e2e_summary=serving_e2e,
        serving_baseline_compare=serving_compare,
    )

    assert report["main_results_table"]["baseline_avg_ms"] == 100.0
    assert report["main_results_table"]["epd_ttft_avg_ms"] == 18.0
    assert report["main_results_table"]["serving_e2e_pass"] is True
    assert report["main_results_table"]["serving_e2e_sanity_pass"] is True
    assert report["main_results_table"]["serving_e2e_path_split_ok"] is True
    assert report["main_results_table"]["serving_e2e_transfer_path_split_ok"] is True
    assert report["main_results_table"]["ablation_ordering_pass"] is True
    assert report["main_results_table"]["serving_e2e_connector_workers"] == 2
    assert report["main_results_table"]["serving_correctness_available"] is True
    assert report["main_results_table"]["serving_correctness_pass"] is True
    assert report["main_results_table"]["serving_correctness_count"] == 5
    assert report["main_results_table"]["serving_correctness_avg_token_jaccard"] == 0.8812
    assert report["main_results_table"]["serving_correctness_same_finish_reason_rate"] == 1.0
    assert report["main_results_table"]["ttft_gain_b2_vs_b1_ms"] == 40.0
    assert report["main_results_table"]["ttft_gain_b7_vs_b3_ms"] == 30.0
    assert report["main_results_table"]["ttft_penalty_b8_vs_b7_ms"] == 90.0
    assert report["cache_reuse_table"]["by_scenario"]["multi_turn"] == 0.75
    assert report["dataset_table"]["workflow_traces"] == 2
    assert report["serving_e2e_table"]["pd_route_ok"] is True
    assert report["serving_e2e_table"]["path_split_ok"] is True
    assert report["serving_e2e_table"]["transfer_path_split_ok"] is True
    assert report["serving_e2e_table"]["epd_mm_prefetch_announced"] == 1
    assert report["serving_e2e_table"]["connector_metric_workers"] == 2
    assert report["serving_e2e_sanity_table"]["sanity_pass"] is True
    assert report["serving_correctness_table"]["available"] is True
    assert report["serving_correctness_table"]["pass_recommendation"] is True
    assert report["serving_correctness_table"]["count"] == 5
    assert report["serving_correctness_table"]["same_finish_reason_rate"] == 1.0
    assert report["serving_e2e_sanity_table"]["group_batches_multiple_of_requests"] is True
    assert report["serving_e2e_sanity_table"]["group_batches_per_request"] == 9.0
    assert report["ablation_gap_table"][0]["id"] == "b2_vs_b1"
    assert report["b8_penalty_table"]["penalty_ms"] == 90.0
    assert report["b8_penalty_table"]["delta_reuse_ratio_avg"] == 0.5
    assert report["b8_penalty_table"]["tier2_recomputed_tokens_avg"] == 6.0
    assert report["b8_penalty_table"]["delta_prefill_ms_avg"] == 18.0
    assert report["b8_penalty_table"]["pipeline_overhead_ms_avg"] == 12.0
    assert report["b8_penalty_table"]["dominant_cost_bucket"] == "delta_prefill_ms"
    assert report["b8_penalty_table"]["tier2_reused_tokens_constant"] is True
    assert report["ablation_ordering_table"]["ordering_pass"] is True
    assert "baseline_matrix_table" in report
    assert "workload_matrix_table" in report
    assert any(row["id"] == "B0" for row in report["baseline_matrix_table"])


def test_build_rfc_eval_report_accepts_mixed_path_serving_asymmetry():
    phase6 = {"ablations": {"B0": {"latency_ms": 1.0}}}
    soak = {}
    traces = {"traces": []}
    serving_e2e = {
        "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
        "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
        "workflow_registry_events": 14,
        "metrics": {
            "metrics": {
                "connector_metric_workers": 2,
                "layered_transfer_grouped_batches": 21,
                "layered_transfer_grouped_bytes": 11534336,
                "layered_transfer_grouped_descriptors": 1488,
                "layered_receive_group_batches": 19,
                "layered_receive_finished_reqs": 2,
                "requests_total": 2,
                "peer_buffer_batches": 21,
                "peer_buffer_bytes": 11534336,
                "path_stats": {
                    "PD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                    },
                    "EPD": {
                        "requests_total": 1,
                        "handoff_committed": 1,
                        "mm_prefetch_announced": 1,
                    },
                },
                "connector_path_stats": {
                    "PD": {
                        "grouped_batches": 6,
                        "grouped_descriptors": 384,
                        "grouped_bytes": 2097152,
                        "peer_buffer_batches": 6,
                        "peer_buffer_bytes": 2097152,
                        "received_group_batches": 9,
                        "received_finished_reqs": 1,
                    },
                    "EPD": {
                        "grouped_batches": 15,
                        "grouped_descriptors": 1104,
                        "grouped_bytes": 9437184,
                        "peer_buffer_batches": 15,
                        "peer_buffer_bytes": 9437184,
                        "received_group_batches": 10,
                        "received_finished_reqs": 1,
                    },
                },
                "layer_load_wait_calls": 2304,
                "layered_receive_failures": 0,
                "remote_transfer_backend_counts": {"peer_buffer_direct": 21},
            },
            "workflow_registry": {"enabled": True},
        },
    }

    report = build_rfc_eval_report(phase6, soak, traces, serving_e2e_summary=serving_e2e)

    assert report["main_results_table"]["serving_e2e_pass"] is True
    assert report["main_results_table"]["serving_e2e_sanity_pass"] is True
    assert report["main_results_table"]["serving_e2e_path_split_ok"] is True
    assert report["main_results_table"]["serving_e2e_transfer_path_split_ok"] is True
    assert report["serving_e2e_sanity_table"]["group_batch_parity_ok"] is True
    assert report["serving_e2e_sanity_table"]["group_batches_multiple_of_requests"] is True


def test_build_rfc_eval_report_backfills_correctness_gate_from_legacy_compare_artifact():
    phase6 = {"ablations": {"B0": {"latency_ms": 1.0}}}
    soak = {}
    traces = []
    compare = {
        "summary": {
            "count": 5,
            "exact_match_rate": 0.6,
            "normalized_exact_match_rate": 0.6,
            "avg_token_jaccard": 0.8812,
            "baseline_elapsed_ms_avg": 1153.83,
            "serving_elapsed_ms_avg": 1572.03,
        },
        "rows": [
            {
                "baseline": {"finish_reason": "stop", "usage": {"completion_tokens": 12}},
                "serving": {"finish_reason": "stop", "usage": {"completion_tokens": 12}},
                "text_metrics": {"normalized_exact_match": True, "token_jaccard": 1.0},
            },
            {
                "baseline": {"finish_reason": "stop", "usage": {"completion_tokens": 18}},
                "serving": {"finish_reason": "stop", "usage": {"completion_tokens": 18}},
                "text_metrics": {"normalized_exact_match": True, "token_jaccard": 1.0},
            },
            {
                "baseline": {"finish_reason": "stop", "usage": {"completion_tokens": 16}},
                "serving": {"finish_reason": "stop", "usage": {"completion_tokens": 16}},
                "text_metrics": {"normalized_exact_match": True, "token_jaccard": 1.0},
            },
            {
                "baseline": {"finish_reason": "length", "usage": {"completion_tokens": 32}},
                "serving": {"finish_reason": "length", "usage": {"completion_tokens": 30}},
                "text_metrics": {"normalized_exact_match": False, "token_jaccard": 0.739},
            },
            {
                "baseline": {"finish_reason": "length", "usage": {"completion_tokens": 32}},
                "serving": {"finish_reason": "length", "usage": {"completion_tokens": 29}},
                "text_metrics": {"normalized_exact_match": False, "token_jaccard": 0.667},
            },
        ],
    }

    report = build_rfc_eval_report(
        phase6,
        soak,
        traces,
        serving_baseline_compare=compare,
    )

    assert report["serving_correctness_table"]["available"] is True
    assert report["serving_correctness_table"]["same_finish_reason_rate"] == 1.0
    assert report["serving_correctness_table"]["normalized_exact_or_high_overlap_rate"] == 0.8
    assert report["serving_correctness_table"]["min_token_jaccard"] == 0.667
    assert report["serving_correctness_table"]["pass_recommendation"] is True
    assert report["main_results_table"]["serving_correctness_pass"] is True


def test_render_rfc_eval_report_markdown():
    report = {
        "main_results_table": {
            "baseline_avg_ms": 1.0,
            "baseline_avg_tps": 2.0,
            "epd_ttft_avg_ms": 3.0,
            "epd_total_avg_ms": 4.0,
            "epd_decode_tps_avg": 5.0,
            "cross_step_reuse_ratio": 0.6,
            "failures": 0,
            "serving_e2e_pass": True,
            "serving_e2e_sanity_pass": True,
            "serving_e2e_path_split_ok": True,
            "serving_e2e_transfer_path_split_ok": True,
            "serving_e2e_connector_path_totals_conserved": True,
            "serving_e2e_connector_workers": 2,
            "serving_e2e_registry_events": 14,
            "serving_correctness_available": True,
            "serving_correctness_pass": True,
            "serving_correctness_count": 5,
            "serving_correctness_avg_token_jaccard": 0.8812,
            "serving_correctness_same_finish_reason_rate": 1.0,
            "ablation_ordering_pass": True,
            "ttft_gain_b2_vs_b1_ms": 40.0,
            "ttft_gain_b7_vs_b3_ms": 30.0,
            "ttft_penalty_b8_vs_b7_ms": 90.0,
        },
        "stage_breakdown_table": {
            "baseline_avg_ms": 1.0,
            "epd_encode_avg_ms": 2.0,
            "epd_prefill_avg_ms": 3.0,
            "epd_ttft_avg_ms": 4.0,
            "epd_decode_avg_ms": 5.0,
            "epd_total_avg_ms": 6.0,
        },
        "cache_reuse_table": {
            "cross_step_avg_reuse_ratio": 0.7,
            "avg_matched_tokens": 10.0,
            "avg_delta_tokens": 2.0,
            "by_scenario": {"multi_turn": 0.8},
        },
        "reliability_table": {
            "failures": 0,
            "samples": 3,
            "gpu_peak_gb": {"enc": 1.0},
        },
        "dataset_table": {
            "workflow_traces": 2,
            "soak_examples": 8,
            "unique_images": 2,
            "scenarios": {"multi_turn": 4},
        },
        "workload_matrix_table": [
            {
                "workload": "W0",
                "samples": 4,
                "avg_steps": 1.0,
                "image_reuse_rate": 0.0,
                "scenarios": ["tool_use_vqa"],
            }
        ],
        "baseline_matrix_table": [
            {
                "id": "B0",
                "name": "Single-node colocated",
                "available": True,
                "latency_ms": 1.0,
                "decode_tps": 2.0,
                "purpose": "baseline",
            }
        ],
        "serving_e2e_table": {
            "available": True,
            "pd_route_ok": True,
            "epd_route_ok": True,
            "workflow_registry_enabled": True,
            "workflow_registry_events": 14,
            "connector_metric_workers": 2,
            "layered_transfer_grouped_batches": 18,
            "layered_receive_group_batches": 18,
            "layered_receive_finished_reqs": 2,
            "peer_buffer_batches": 18,
            "path_stats_available": True,
            "path_split_ok": True,
            "transfer_path_split_ok": True,
            "pd_requests_total": 1,
            "epd_requests_total": 1,
            "pd_handoff_committed": 1,
            "epd_handoff_committed": 1,
            "epd_mm_prefetch_announced": 1,
            "pd_stage_dispatches": {"prefill": 1, "decode": 1},
            "epd_stage_dispatches": {"prefill": 1, "decode": 1},
            "connector_path_stats_available": True,
            "pd_transfer_grouped_descriptors": 256,
            "epd_transfer_grouped_descriptors": 896,
            "pd_transfer_grouped_bytes": 1048576,
            "epd_transfer_grouped_bytes": 7340032,
            "connector_path_stats": {
                "PD": {
                    "grouped_descriptors": 256,
                    "grouped_bytes": 1048576,
                },
                "EPD": {
                    "grouped_descriptors": 896,
                    "grouped_bytes": 7340032,
                },
            },
            "remote_transfer_backend_counts": {"peer_buffer_direct": 18},
        },
        "serving_e2e_sanity_table": {
            "sanity_pass": True,
            "connector_workers_ok": True,
            "group_batch_parity_ok": True,
            "finished_request_parity_ok": True,
            "peer_backend_parity_ok": True,
            "layer_wait_present": True,
            "receive_failures_zero": True,
            "group_batches_multiple_of_requests": True,
            "wait_calls_multiple_of_receive_batches": True,
            "descriptors_multiple_of_group_batches": True,
            "wait_calls_per_receive_batch": 128.0,
            "descriptors_per_group_batch": 64.0,
            "group_batches_per_request": 9.0,
        },
        "serving_correctness_table": {
            "available": True,
            "pass_recommendation": True,
            "pass_recommendation_label": "pass",
            "count": 5,
            "exact_match_rate": 0.6,
            "normalized_exact_match_rate": 0.6,
            "same_finish_reason_rate": 1.0,
            "normalized_exact_or_high_overlap_rate": 1.0,
            "avg_token_jaccard": 0.8812,
            "min_token_jaccard": 0.6667,
            "high_overlap_threshold": 0.7,
            "min_pass_rate": 0.8,
            "min_same_finish_reason_rate": 0.8,
            "min_avg_token_jaccard_threshold": 0.75,
            "min_token_jaccard_threshold": 0.6,
            "baseline_elapsed_ms_avg": 1153.83,
            "serving_elapsed_ms_avg": 1572.03,
            "gate_failures": [],
        },
        "ablation_gap_table": [
            {
                "label": "Native PD → Prefix Cache",
                "effect_type": "gain",
                "base": "B1",
                "target": "B2",
                "base_ttft_ms": 100.0,
                "target_ttft_ms": 60.0,
                "effect_ms": 40.0,
                "effect_pct": 40.0,
            }
        ],
        "b8_penalty_table": {
            "penalty_ms": 90.0,
            "penalty_pct": 180.0,
            "exact_reuse_ratio": 0.8,
            "approx_reuse_ratio": 7.6,
            "tier2_reused_tokens_avg": 6.0,
            "tier3_accepted_pages_avg": 4.5,
            "corr_penalty_reuse_ratio": 0.7,
            "corr_penalty_tier3_pages": -0.4,
            "tier2_reused_tokens_constant": True,
            "max_penalty_scenario": {"scenario": "tool_use_vqa", "image": "objects", "penalty_ms": 97.0},
            "min_penalty_scenario": {"scenario": "tree_of_thought", "image": "map", "penalty_ms": 83.0},
            "per_scenario": [
                {
                    "scenario": "multi_turn",
                    "image": "room",
                    "b7_ttft_ms": 50.0,
                    "b8_ttft_ms": 140.0,
                    "penalty_ms": 90.0,
                    "penalty_pct": 180.0,
                    "reuse_ratio": 7.6,
                    "tier2_reused_tokens": 6.0,
                    "tier3_accepted_pages": 4.0,
                }
            ],
        },
        "ablation_ordering_table": {
            "ordering_pass": True,
            "rows": [
                {
                    "label": "Prefix Cache should beat Native PD",
                    "lhs": "B2",
                    "rhs": "B1",
                    "expect": "lt",
                    "delta_ms": -40.0,
                    "pass": True,
                }
            ],
        },
    }

    md = render_rfc_eval_report_markdown(report)
    assert "# RFC Evaluation Report" in md
    assert "## Main Results" in md
    assert "| multi_turn | 0.8000 |" in md
    assert "| serving_e2e_pass | true |" in md
    assert "| serving_e2e_sanity_pass | true |" in md
    assert "| serving_e2e_transfer_path_split_ok | true |" in md
    assert "| serving_correctness_pass | true |" in md
    assert "| ablation_ordering_pass | true |" in md
    assert "## Serving E2E" in md
    assert "## Serving E2E Sanity" in md
    assert "## Serving Correctness vs Baseline" in md
    assert "## TTFT Gap Decomposition" in md
    assert "## Ablation Ordering Sanity" in md
    assert "## B8 Approx Reuse Penalty" in md
    assert "| connector_metric_workers | 2 |" in md
    assert "## RFC Workloads (dev-small)" in md
    assert "## Baseline Matrix" in md


def test_metrics_collector_ingest_serving_snapshot():
    collector = MetricsCollector()

    collector.ingest_serving_snapshot(
        {
            "metrics": {
                "backpressure_events": 2,
                "reject_events": 1,
                "degrade_level_counts": {"NONE": 1, "PARTIAL": 2},
                "handoff_prepare_ms_avg": 1.0,
                "handoff_commit_ms_avg": 2.0,
                "layered_transfer_grouped_batches": 4,
                "remote_transfer_backend_counts": {
                    "peer_buffer_direct": 3,
                    "batch_transfer_fallback": 1,
                },
            }
        }
    )

    report = collector.report()

    assert report["backpressure_events"] == 2
    assert report["reject_events"] == 1
    assert report["handoff_prepare_ms"] == 1.0
    assert report["handoff_commit_ms"] == 2.0
    assert report["layered_transfer_batches_avg"] == 4.0
    assert report["remote_transfer_backend_counts"] == {
        "peer_buffer_direct": 3,
        "batch_transfer_fallback": 1,
    }


def test_rfc_matrix_loads_real_mooncake_dataset_manifest(tmp_path):
    from mooncake_epd.benchmarks.rfc_eval_matrix import write_rfc_eval_matrix_artifacts

    dataset_root = os.getenv(
        "MOONCAKE_EPD_DATASET_ROOT",
        "datasets/mooncake_test_dataset",
    )
    if not Path(dataset_root).exists():
        pytest.skip("set MOONCAKE_EPD_DATASET_ROOT to run the real dataset test")
    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path="artifacts/phase6_metrics.json",
        soak_path="artifacts/real_soak_report_post_kvdir.json",
        workflow_traces_path="artifacts/workflow_traces.json",
        serving_e2e_summary_path="artifacts/serving_e2e_summary.json",
        matrix_output_path=tmp_path / "matrix.json",
        report_json_path=tmp_path / "report.json",
        report_md_path=tmp_path / "report.md",
        manifest_output_path=tmp_path / "manifest.json",
        workload_manifest_path=tmp_path / "dataset_manifest.json",
        dataset_root=dataset_root,
        dataset_eval_output_path=tmp_path / "dataset_eval.json",
    )

    alignment = matrix["alignment"]
    assert alignment["mooncake_dataset_loaded"] is True
    assert alignment["mooncake_dataset_pass"] is True
    assert alignment["mooncake_workflows_total"] == 160
    assert alignment["mooncake_chat_examples_total"] == 461
    assert alignment["workload_samples"]["W0"] == 80
    assert alignment["workload_samples"]["W5"] > 0
    assert "dataset_eval_table" in matrix["report"]
    assert (tmp_path / "dataset_eval.json").exists()
    assert (tmp_path / "dataset_manifest.json").exists()
    assert "Mooncake Test Dataset" in (tmp_path / "report.md").read_text(encoding="utf-8")
