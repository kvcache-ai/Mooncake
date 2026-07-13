from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from mooncake_epd.benchmarks.rfc_eval_matrix import (
    build_rfc_eval_matrix,
    write_rfc_eval_matrix_artifacts,
)
from mooncake_epd.scripts.run_rfc_eval_matrix import (
    _build_ablation_command,
    _build_regression_gate_command,
    _build_serving_baseline_compare_command,
    _build_feature_handle_e2e_command,
    _build_feature_handle_paired_command,
    _build_online_direct_e2e_command,
)


def _sample_phase6() -> dict:
    return {
        "dataset": {
            "total_examples": 8,
            "by_scenario": {"multi_turn": 4, "tool_use_vqa": 4},
        },
        "summary": {
            "baseline_avg_ms": 100.0,
            "baseline_avg_tps": 10.0,
            "epd_avg_ttft_ms": 20.0,
            "epd_avg_decode_tps": 30.0,
            "cross_step_avg_reuse_ratio": 0.75,
        },
        "epd_step0": [
            {"encode_ms": 5.0, "prefill_ms": 6.0, "ttft_ms": 20.0, "decode_ms": 7.0, "total_ms": 40.0},
        ],
        "cross_step_reuse": [
            {"scenario": "multi_turn", "reuse_ratio": 0.75, "matched_tokens": 75, "delta_tokens": 25},
        ],
        "ablations": {"B1": {"latency_ms": 90.0, "decode_tps": 11.0}},
    }


def _sample_soak() -> dict:
    return {
        "dataset_summary": {
            "total_examples": 8,
            "by_scenario": {"multi_turn": 4, "tool_use_vqa": 4},
            "unique_images": ["a", "b"],
        },
        "results": [{}, {}],
        "summary": {
            "epd_ttft_avg_ms": 18.0,
            "epd_total_avg_ms": 35.0,
            "epd_decode_tps_avg": 31.0,
            "failures": 0,
            "gpu_peak_gb": {"enc": 1.0, "pre": 2.0, "dec": 3.0, "base": 4.0},
        },
    }



def _sample_feature_handle_e2e() -> dict:
    return {
        "response": {
            "status_code": 200,
            "elapsed_ms": 2308.9,
            "headers": {"x-epd-routing-path": "EPD"},
            "response_content_len": 160,
        },
        "feature_handle_metric_summary": {
            "requests_total": 1,
            "requests_multimodal": 1,
            "precomputed_hits": 1,
            "hidden_cache_errors": 0,
            "hidden_cache_full_miss_batches": 0,
            "hidden_cache_vision_compute_ms_avg": 0.0,
            "fallback_batches": 0,
            "fallback_bytes": 0,
            "layered_receive_failures": 0,
            "layered_transfer_failed_batches": 0,
            "peer_buffer_batches": 204,
            "peer_buffer_bytes": 46792704,
            "backend_counts": {"peer_buffer_direct": 204},
        },
    }


def _sample_feature_handle_paired() -> dict:
    return {
        "paired_summary": {
            "pass": True,
            "failed_gates": [],
            "asset_ttft_ms": 2000.0,
            "feature_ttft_ms": 1500.0,
            "ttft_delta_ms": -500.0,
            "ttft_reduction_pct": 25.0,
            "asset_elapsed_ms": 2600.0,
            "feature_elapsed_ms": 2100.0,
            "elapsed_delta_ms": -500.0,
            "elapsed_reduction_pct": 19.2307,
            "asset_goodput_rps": 0.4,
            "feature_goodput_rps": 0.5,
            "goodput_gain_pct": 25.0,
            "asset_metric_summary": {"precomputed_hits": 0, "hidden_cache_vision_compute_ms_avg": 123.0},
            "feature_metric_summary": {
                "precomputed_hits": 1,
                "hidden_cache_vision_compute_ms_avg": 0.0,
                "fallback_batches": 0,
                "layered_receive_failures": 0,
                "backend_counts": {"peer_buffer_direct": 12},
            },
            "text_metrics": {"token_jaccard": 1.0},
            "gates": {"feature_precomputed_hit_ok": True},
        }
    }


def _sample_online_direct_e2e() -> dict:
    payload = _sample_feature_handle_e2e()
    payload["strict_no_fallback"] = True
    payload["direct_buffer_stats_after_release"] = {"allocations": 0, "bytes": 0}
    payload["online_direct_metric_summary"] = dict(payload["feature_handle_metric_summary"])
    payload["online_direct_metric_summary"]["direct_buffer_allocations"] = 0
    return payload

def test_build_rfc_eval_matrix_includes_alignment_and_baseline_coverage():
    matrix = build_rfc_eval_matrix(
        _sample_phase6(),
        _sample_soak(),
        workflow_traces=[{"workflow_id": "wf-1"}, {"workflow_id": "wf-2"}],
    )

    assert matrix["alignment"]["phase6_examples"] == 8
    assert matrix["alignment"]["soak_examples"] == 8
    assert matrix["alignment"]["scenario_counts_match"] is True
    assert "B0" in matrix["baseline_coverage"]["available"]
    assert "B9" in matrix["baseline_coverage"]["available"]
    assert matrix["report"]["main_results_table"]["epd_ttft_avg_ms"] == 18.0


def test_write_rfc_eval_matrix_artifacts_outputs_json_and_markdown(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        report_json_path=tmp_path / "report.json",
        report_md_path=tmp_path / "report.md",
        manifest_output_path=tmp_path / "manifest.json",
        mixed_size=10,
    )

    assert matrix["artifact_status"]["phase6_loaded"] is True
    assert json.loads((tmp_path / "matrix.json").read_text())["alignment"]["workflow_traces"] == 1
    assert json.loads((tmp_path / "manifest.json").read_text())["workloads"]["W5"]
    assert "# RFC Evaluation Report" in (tmp_path / "report.md").read_text()


def test_write_rfc_eval_matrix_artifacts_merges_external_ablation_report(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    ablation_path = tmp_path / "ablations.json"
    sample_phase6 = _sample_phase6()
    sample_phase6.pop("ablations", None)
    phase6_path.write_text(json.dumps(sample_phase6), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    ablation_path.write_text(
        json.dumps(
            {
                "baselines": {
                    "B2": {"latency_ms": 44.0, "decode_tps": 12.5},
                    "B7": {"ttft_ms": 21.0, "decode_tps": 30.0},
                }
            }
        ),
        encoding="utf-8",
    )

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        ablation_report_path=ablation_path,
    )

    rows = {row["id"]: row for row in matrix["report"]["baseline_matrix_table"]}
    assert rows["B2"]["available"] is True
    assert rows["B2"]["latency_ms"] == 44.0
    assert rows["B7"]["available"] is True
    assert rows["B7"]["latency_ms"] == 21.0


def test_write_rfc_eval_matrix_artifacts_merges_serving_e2e_summary(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    serving_e2e_path = tmp_path / "serving_e2e.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    serving_e2e_path.write_text(
        json.dumps(
            {
                "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
                "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
                "workflow_registry_events": 14,
                "metrics": {
                    "metrics": {
                        "requests_total": 2,
                        "connector_metric_workers": 2,
                        "layered_transfer_grouped_batches": 18,
                        "layered_transfer_grouped_bytes": 8388608,
                        "layered_transfer_grouped_descriptors": 1152,
                        "layered_receive_group_batches": 18,
                        "layered_receive_finished_reqs": 2,
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
                        "peer_buffer_batches": 18,
                        "peer_buffer_bytes": 8388608,
                        "remote_transfer_backend_counts": {"peer_buffer_direct": 18},
                    },
                    "workflow_registry": {"enabled": True},
                },
            }
        ),
        encoding="utf-8",
    )

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        serving_e2e_summary_path=serving_e2e_path,
    )

    assert matrix["artifact_status"]["serving_e2e_loaded"] is True
    assert matrix["alignment"]["serving_e2e_requests_total"] == 2
    assert matrix["alignment"]["serving_e2e_pd_requests_total"] == 1
    assert matrix["alignment"]["serving_e2e_epd_requests_total"] == 1
    assert matrix["report"]["main_results_table"]["serving_e2e_pass"] is True
    assert matrix["report"]["main_results_table"]["serving_e2e_path_split_ok"] is True
    assert matrix["report"]["main_results_table"]["serving_e2e_transfer_path_split_ok"] is True
    assert matrix["report"]["serving_e2e_table"]["pd_route_ok"] is True
    assert matrix["report"]["serving_e2e_table"]["path_split_ok"] is True
    assert matrix["report"]["serving_e2e_table"]["transfer_path_split_ok"] is True
    assert matrix["report"]["serving_e2e_table"]["layered_receive_group_batches"] == 18


def test_write_rfc_eval_matrix_artifacts_merges_feature_handle_e2e_summary(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    feature_path = tmp_path / "feature_handle_e2e.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    feature_path.write_text(json.dumps(_sample_feature_handle_e2e()), encoding="utf-8")

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        report_md_path=tmp_path / "report.md",
        feature_handle_e2e_summary_path=feature_path,
    )

    assert matrix["artifact_status"]["feature_handle_e2e_loaded"] is True
    assert matrix["alignment"]["feature_handle_precomputed_hits"] == 1
    assert matrix["alignment"]["feature_handle_vision_compute_ms_avg"] == 0.0
    assert matrix["alignment"]["feature_handle_peer_buffer_batches"] == 204
    assert matrix["alignment"]["feature_handle_peer_buffer_direct_batches"] == 204
    assert matrix["alignment"]["feature_handle_fallback_batches"] == 0
    assert matrix["alignment"]["feature_handle_layered_receive_failures"] == 0
    assert matrix["alignment"]["feature_handle_pass"] is True
    assert matrix["report"]["main_results_table"]["feature_handle_e2e_pass"] is True
    assert matrix["report"]["feature_handle_e2e_table"]["precomputed_hits"] == 1
    assert "## FeatureHandle E2E" in (tmp_path / "report.md").read_text(encoding="utf-8")


def test_write_rfc_eval_matrix_artifacts_merges_online_direct_e2e_summary(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    online_path = tmp_path / "online_direct.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    online_path.write_text(json.dumps(_sample_online_direct_e2e()), encoding="utf-8")

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        online_direct_e2e_summary_path=online_path,
    )

    assert matrix["artifact_status"]["online_direct_e2e_loaded"] is True
    assert matrix["alignment"]["online_direct_pass"] is True
    assert matrix["alignment"]["online_direct_buffer_release_ok"] is True
    assert matrix["report"]["main_results_table"]["online_direct_e2e_pass"] is True
    assert matrix["report"]["online_direct_e2e_table"]["strict_no_fallback"] is True


def test_build_online_direct_e2e_command_uses_real_runner():
    args = type(
        "Args",
        (),
        {
            "online_direct_e2e_workdir": "/tmp/online-direct",
            "online_direct_e2e_local_hostname": "127.0.0.1",
            "online_direct_e2e_timeout": 123.0,
            "online_direct_e2e_request_timeout": 45.0,
            "online_direct_e2e_encoder_device": "cuda:5",
            "online_direct_e2e_encoder_port": 8330,
            "online_direct_e2e_prefill_gpu": 3,
            "online_direct_e2e_decode_gpu": 4,
            "online_direct_e2e_owner_shards": 4,
            "online_direct_e2e_max_group_bytes": 1024,
            "online_direct_e2e_max_transfer_descriptors": 8,
            "online_direct_e2e_max_transfer_bytes": 2048,
            "online_direct_e2e_kv_directory_rpc_url": None,
        },
    )()

    cmd = _build_online_direct_e2e_command(args)

    assert "run_vllm_online_direct_e2e.py" in " ".join(cmd)
    assert "--encoder-device" in cmd
    assert "cuda:5" in cmd
    assert "--owner-shards" in cmd
    assert "4" in cmd

def test_write_rfc_eval_matrix_artifacts_merges_feature_handle_paired_benchmark(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    paired_path = tmp_path / "paired.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    paired_path.write_text(json.dumps(_sample_feature_handle_paired()), encoding="utf-8")

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        report_md_path=tmp_path / "report.md",
        feature_handle_paired_benchmark_path=paired_path,
    )

    assert matrix["artifact_status"]["feature_handle_paired_loaded"] is True
    assert matrix["alignment"]["feature_handle_paired_pass"] is True
    assert matrix["alignment"]["feature_handle_paired_ttft_reduction_pct"] == 25.0
    assert matrix["alignment"]["feature_handle_paired_goodput_gain_pct"] == 25.0
    assert matrix["report"]["feature_handle_paired_table"]["feature_precomputed_hits"] == 1
    assert "## FeatureHandle Paired Benchmark" in (tmp_path / "report.md").read_text(encoding="utf-8")

def test_write_rfc_eval_matrix_artifacts_merges_serving_baseline_compare(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    compare_path = tmp_path / "serving_compare.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    compare_path.write_text(
        json.dumps(
            {
                "summary": {
                    "count": 5,
                    "exact_match_rate": 0.6,
                    "normalized_exact_match_rate": 0.6,
                    "avg_token_jaccard": 0.8812,
                    "min_token_jaccard": 0.6667,
                    "same_finish_reason_rate": 1.0,
                    "normalized_exact_or_high_overlap_rate": 1.0,
                    "pass_recommendation": True,
                    "pass_recommendation_label": "pass",
                    "gate_failures": [],
                    "baseline_elapsed_ms_avg": 1153.83,
                    "serving_elapsed_ms_avg": 1572.03,
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
        ),
        encoding="utf-8",
    )

    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=phase6_path,
        soak_path=soak_path,
        workflow_traces_path=traces_path,
        matrix_output_path=tmp_path / "matrix.json",
        serving_baseline_compare_path=compare_path,
    )

    assert matrix["artifact_status"]["serving_baseline_compare_loaded"] is True
    assert matrix["alignment"]["serving_correctness_available"] is True
    assert matrix["alignment"]["serving_correctness_pass"] is True
    assert matrix["alignment"]["serving_correctness_count"] == 5
    assert matrix["report"]["serving_correctness_table"]["avg_token_jaccard"] == 0.8812
    assert matrix["report"]["main_results_table"]["serving_correctness_pass"] is True


def test_build_ablation_command_passes_baselines_and_token_limit(tmp_path):
    args = type(
        "Args",
        (),
        {
            "ablation_output": str(tmp_path / "ablations.json"),
            "ablation_baselines": ["B2", "B7", "B8"],
            "ablation_max_new_tokens": 16,
        },
    )()

    cmd = _build_ablation_command(args)  # type: ignore[arg-type]

    assert cmd[2:4] == ["--output", str(tmp_path / "ablations.json")]
    assert cmd[4:8] == ["--baselines", "B2", "B7", "B8"]
    assert cmd[-2:] == ["--max-new-tokens", "16"]


def test_build_serving_baseline_compare_command(tmp_path):
    args = type(
        "Args",
        (),
        {
            "serving_baseline_compare_out": str(tmp_path / "serving_compare.json"),
            "serving_baseline_compare_local_hostname": "127.0.0.1",
            "serving_baseline_compare_allow_loopback": True,
            "dataset_root": "/dataset",
            "dataset_chat_split": "dev-small",
            "serving_baseline_compare_max_dataset_requests": 5,
            "serving_baseline_compare_dataset_request_timeout": 180.0,
            "serving_baseline_compare_dataset_max_input_len": 4096,
            "serving_baseline_compare_dataset_request_max_tokens": 32,
            "serving_baseline_compare_dataset_image_max_pixels": 1003520,
            "serving_baseline_compare_dataset_skip_oversized": True,
            "serving_baseline_compare_baseline_gpu": 2,
            "serving_baseline_compare_warmup_requests": 1,
            "serving_baseline_compare_baseline_warmup_requests": 1,
            "serving_baseline_compare_owner_shards": 4,
            "serving_baseline_compare_high_overlap_threshold": 0.7,
            "serving_baseline_compare_min_pass_rate": 0.8,
            "serving_baseline_compare_min_same_finish_reason_rate": 0.8,
            "serving_baseline_compare_min_avg_token_jaccard": 0.75,
            "serving_baseline_compare_min_token_jaccard": 0.6,
            "serving_baseline_compare_dataset_families": ["W0", "W1"],
            "serving_baseline_compare_kv_directory_rpc_url": None,
            "serving_baseline_compare_proxy_url": None,
        },
    )()

    cmd = _build_serving_baseline_compare_command(args)  # type: ignore[arg-type]

    assert cmd[0] == sys.executable
    assert "--dataset-root" in cmd
    assert "--dataset-families" in cmd
    assert "--high-overlap-threshold" in cmd
    assert "--min-token-jaccard" in cmd
    assert str(tmp_path / "serving_compare.json") in cmd


def test_build_feature_handle_e2e_command(tmp_path):
    args = type(
        "Args",
        (),
        {
            "feature_handle_e2e_workdir": str(tmp_path / "fh"),
            "feature_handle_e2e_local_hostname": "127.0.0.1",
            "feature_handle_e2e_timeout": 900.0,
            "feature_handle_e2e_request_timeout": 300.0,
            "feature_handle_e2e_encoder_device": "cuda:5",
            "feature_handle_e2e_prefill_gpu": 3,
            "feature_handle_e2e_decode_gpu": 4,
            "feature_handle_e2e_owner_shards": 2,
            "feature_handle_e2e_max_group_bytes": 16777216,
            "feature_handle_e2e_max_transfer_descriptors": 32,
            "feature_handle_e2e_max_transfer_bytes": 16777216,
            "feature_handle_e2e_request": str(tmp_path / "request.json"),
            "feature_handle_e2e_store_dir": str(tmp_path / "store"),
            "feature_handle_e2e_kv_directory_rpc_url": None,
        },
    )()

    cmd = _build_feature_handle_e2e_command(args)  # type: ignore[arg-type]

    assert cmd[0] == sys.executable
    assert "run_vllm_feature_handle_e2e.py" in cmd[1]
    assert ["--request", str(tmp_path / "request.json")] == cmd[cmd.index("--request"):cmd.index("--request") + 2]
    assert ["--store-dir", str(tmp_path / "store")] == cmd[cmd.index("--store-dir"):cmd.index("--store-dir") + 2]
    assert ["--owner-shards", "2"] == cmd[cmd.index("--owner-shards"):cmd.index("--owner-shards") + 2]

def test_build_feature_handle_paired_command(tmp_path):
    args = type(
        "Args",
        (),
        {
            "feature_handle_paired_workdir": str(tmp_path / "paired"),
            "feature_handle_paired_summary": str(tmp_path / "paired.json"),
            "feature_handle_paired_request": str(tmp_path / "request.json"),
            "feature_handle_e2e_request": None,
            "feature_handle_paired_local_hostname": "127.0.0.1",
            "feature_handle_paired_timeout": 900.0,
            "feature_handle_paired_request_timeout": 300.0,
            "feature_handle_paired_prefill_gpu": 3,
            "feature_handle_paired_decode_gpu": 4,
            "feature_handle_paired_owner_shards": 1,
            "feature_handle_paired_between_mode_sleep_s": 1.0,
            "feature_handle_paired_max_group_bytes": 16777216,
            "feature_handle_paired_max_transfer_descriptors": 32,
            "feature_handle_paired_max_transfer_bytes": 16777216,
            "feature_handle_paired_kv_directory_rpc_url": None,
        },
    )()

    cmd = _build_feature_handle_paired_command(args)  # type: ignore[arg-type]

    assert cmd[0] == sys.executable
    assert "run_feature_handle_paired_benchmark.py" in cmd[1]
    assert ["--request", str(tmp_path / "request.json")] == cmd[cmd.index("--request"):cmd.index("--request") + 2]
    assert ["--output", str(tmp_path / "paired.json")] == cmd[cmd.index("--output"):cmd.index("--output") + 2]

def test_build_regression_gate_command(tmp_path):
    cmd = _build_regression_gate_command(
        report_json_out=str(tmp_path / "report.json"),
        matrix_out=str(tmp_path / "matrix.json"),
    )

    assert cmd[2:4] == ["--report", str(tmp_path / "report.json")]
    assert cmd[-2:] == ["--matrix", str(tmp_path / "matrix.json")]


def test_run_rfc_eval_matrix_script(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
            "--report-json-out",
            str(tmp_path / "report.json"),
            "--report-md-out",
            str(tmp_path / "report.md"),
            "--manifest-out",
            str(tmp_path / "manifest.json"),
            "--mixed-size",
            "10",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"matrix_out"' in proc.stdout
    assert (tmp_path / "matrix.json").exists()
    assert (tmp_path / "report.json").exists()
    assert (tmp_path / "report.md").exists()


def test_run_rfc_eval_matrix_script_supports_execution_flags(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
            "--report-json-out",
            str(tmp_path / "report.json"),
            "--report-md-out",
            str(tmp_path / "report.md"),
            "--manifest-out",
            str(tmp_path / "manifest.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert "executed_steps" in payload


def test_run_rfc_eval_matrix_script_accepts_serving_e2e_summary(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    serving_e2e_path = tmp_path / "serving_e2e.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    serving_e2e_path.write_text(
        json.dumps(
            {
                "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
                "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
                "workflow_registry_events": 4,
                "metrics": {
                    "metrics": {
                        "requests_total": 2,
                        "connector_metric_workers": 2,
                        "layered_transfer_grouped_batches": 2,
                        "layered_transfer_grouped_bytes": 512,
                        "layered_transfer_grouped_descriptors": 8,
                        "layered_receive_group_batches": 2,
                        "layered_receive_finished_reqs": 2,
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
                                "grouped_batches": 1,
                                "grouped_descriptors": 1,
                                "grouped_bytes": 64,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 64,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                            "EPD": {
                                "grouped_batches": 1,
                                "grouped_descriptors": 7,
                                "grouped_bytes": 448,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 448,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                        },
                        "layer_load_wait_calls": 4,
                        "layered_receive_failures": 0,
                        "peer_buffer_batches": 2,
                        "peer_buffer_bytes": 512,
                        "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
                    },
                    "workflow_registry": {"enabled": True},
                },
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--serving-e2e-summary",
            str(serving_e2e_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["alignment"]["serving_e2e_requests_total"] == 2
    assert payload["alignment"]["serving_e2e_pd_requests_total"] == 1
    assert payload["alignment"]["serving_e2e_epd_requests_total"] == 1


def test_run_rfc_eval_matrix_script_accepts_feature_handle_e2e_summary(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    feature_path = tmp_path / "feature_handle_e2e.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    feature_path.write_text(json.dumps(_sample_feature_handle_e2e()), encoding="utf-8")

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--feature-handle-e2e-summary",
            str(feature_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["alignment"]["feature_handle_precomputed_hits"] == 1
    assert payload["alignment"]["feature_handle_pass"] is True
    assert payload["feature_handle_e2e"]["peer_buffer_direct_batches"] == 204

def test_run_rfc_eval_matrix_script_accepts_serving_baseline_compare(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    compare_path = tmp_path / "compare.json"
    phase6_path.write_text(json.dumps(_sample_phase6()), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    compare_path.write_text(
        json.dumps(
            {
                "summary": {
                    "count": 5,
                    "avg_token_jaccard": 0.8812,
                    "same_finish_reason_rate": 1.0,
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
        ),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--serving-baseline-compare-out",
            str(compare_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["alignment"]["serving_correctness_available"] is True
    assert payload["alignment"]["serving_correctness_pass"] is True
    assert payload["serving_correctness"]["avg_token_jaccard"] == 0.8812


def test_run_rfc_eval_matrix_script_runs_regression_gate(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    serving_e2e_path = tmp_path / "serving_e2e.json"
    phase6 = _sample_phase6()
    phase6["ablations"] = {
        "B1": {"ttft_ms": 100.0},
        "B2": {"ttft_ms": 60.0},
        "B3": {"ttft_ms": 80.0},
        "B5": {"ttft_ms": 75.0},
        "B6": {"ttft_ms": 70.0},
        "B7": {"ttft_ms": 50.0},
        "B8": {"ttft_ms": 140.0},
    }
    phase6_path.write_text(json.dumps(phase6), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    serving_e2e_path.write_text(
        json.dumps(
            {
                "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
                "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
                "workflow_registry_events": 4,
                "metrics": {
                    "metrics": {
                        "requests_total": 2,
                        "connector_metric_workers": 2,
                        "layered_transfer_grouped_batches": 2,
                        "layered_transfer_grouped_bytes": 512,
                        "layered_transfer_grouped_descriptors": 8,
                        "layered_receive_group_batches": 2,
                        "layered_receive_finished_reqs": 2,
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
                                "grouped_batches": 1,
                                "grouped_descriptors": 1,
                                "grouped_bytes": 64,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 64,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                            "EPD": {
                                "grouped_batches": 1,
                                "grouped_descriptors": 7,
                                "grouped_bytes": 448,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 448,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                        },
                        "layer_load_wait_calls": 4,
                        "layered_receive_failures": 0,
                        "peer_buffer_batches": 2,
                        "peer_buffer_bytes": 512,
                        "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
                    },
                    "workflow_registry": {"enabled": True},
                },
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_rfc_eval_matrix.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--serving-e2e-summary",
            str(serving_e2e_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
            "--report-json-out",
            str(tmp_path / "report.json"),
            "--run-regression-gate",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["regression_gate"]["pass"] is True
    assert payload["executed_steps"]["regression_gate"] is True


def test_check_rfc_regressions_script(tmp_path):
    report_path = tmp_path / "report.json"
    matrix_path = tmp_path / "matrix.json"
    report_path.write_text(
        json.dumps(
            {
                "main_results_table": {
                    "serving_e2e_pass": True,
                    "serving_e2e_sanity_pass": True,
                    "serving_e2e_path_split_ok": True,
                    "serving_e2e_transfer_path_split_ok": True,
                    "serving_e2e_connector_path_totals_conserved": True,
                    "ablation_ordering_pass": True,
                    "serving_e2e_connector_workers": 2,
                    "serving_e2e_registry_events": 14,
                    "serving_correctness_available": True,
                    "serving_correctness_pass": True,
                    "serving_correctness_count": 5,
                    "serving_correctness_avg_token_jaccard": 0.8812,
                    "ttft_gain_b2_vs_b1_ms": 40.0,
                    "ttft_gain_b7_vs_b3_ms": 30.0,
                    "ttft_penalty_b8_vs_b7_ms": 90.0,
                },
                "serving_e2e_sanity_table": {"sanity_pass": True},
                "ablation_ordering_table": {"ordering_pass": True},
            }
        ),
        encoding="utf-8",
    )
    matrix_path.write_text(
        json.dumps({"baseline_coverage": {"coverage_ratio": 1.0}}),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parent.parent / "scripts" / "check_rfc_regressions.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--report",
            str(report_path),
            "--matrix",
            str(matrix_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["pass"] is True
    assert payload["summary"]["serving_correctness_pass"] is True


def test_build_rfc_eval_report_script_uses_unified_matrix_inputs(tmp_path):
    phase6_path = tmp_path / "phase6.json"
    soak_path = tmp_path / "soak.json"
    traces_path = tmp_path / "traces.json"
    ablation_path = tmp_path / "ablations.json"
    serving_e2e_path = tmp_path / "serving_e2e.json"

    phase6 = _sample_phase6()
    phase6.pop("ablations", None)
    phase6_path.write_text(json.dumps(phase6), encoding="utf-8")
    soak_path.write_text(json.dumps(_sample_soak()), encoding="utf-8")
    traces_path.write_text(json.dumps([{"workflow_id": "wf-1"}]), encoding="utf-8")
    ablation_path.write_text(
        json.dumps(
            {
                "baselines": {
                    "B1": {"ttft_ms": 100.0},
                    "B2": {"ttft_ms": 60.0},
                    "B3": {"ttft_ms": 80.0},
                    "B7": {"ttft_ms": 50.0},
                    "B8": {"ttft_ms": 140.0},
                }
            }
        ),
        encoding="utf-8",
    )
    serving_e2e_path.write_text(
        json.dumps(
            {
                "text_probe_stdout": "headers: {'x-epd-routing-path': 'PD'}",
                "mm_probe_stdout": "headers: {'x-epd-routing-path': 'EPD'}",
                "workflow_registry_events": 4,
                "metrics": {
                    "metrics": {
                        "requests_total": 2,
                        "connector_metric_workers": 2,
                        "layered_transfer_grouped_batches": 2,
                        "layered_transfer_grouped_bytes": 512,
                        "layered_transfer_grouped_descriptors": 8,
                        "layered_receive_group_batches": 2,
                        "layered_receive_finished_reqs": 2,
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
                                "grouped_batches": 1,
                                "grouped_descriptors": 1,
                                "grouped_bytes": 64,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 64,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                            "EPD": {
                                "grouped_batches": 1,
                                "grouped_descriptors": 7,
                                "grouped_bytes": 448,
                                "peer_buffer_batches": 1,
                                "peer_buffer_bytes": 448,
                                "received_group_batches": 1,
                                "received_finished_reqs": 1,
                            },
                        },
                        "layer_load_wait_calls": 4,
                        "layered_receive_failures": 0,
                        "peer_buffer_batches": 2,
                        "peer_buffer_bytes": 512,
                        "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
                    },
                    "workflow_registry": {"enabled": True},
                },
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parent.parent / "scripts" / "build_rfc_eval_report.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--phase6",
            str(phase6_path),
            "--soak",
            str(soak_path),
            "--traces",
            str(traces_path),
            "--ablation-output",
            str(ablation_path),
            "--serving-e2e-summary",
            str(serving_e2e_path),
            "--matrix-out",
            str(tmp_path / "matrix.json"),
            "--report-json-out",
            str(tmp_path / "report.json"),
            "--report-md-out",
            str(tmp_path / "report.md"),
            "--manifest-out",
            str(tmp_path / "manifest.json"),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(proc.stdout)
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert payload["alignment"]["serving_e2e_requests_total"] == 2
    assert payload["alignment"]["serving_e2e_pd_requests_total"] == 1
    assert payload["alignment"]["serving_e2e_epd_requests_total"] == 1
    assert report["main_results_table"]["serving_e2e_pass"] is True
    assert report["main_results_table"]["serving_e2e_sanity_pass"] is True
    assert report["main_results_table"]["serving_e2e_path_split_ok"] is True
    assert report["main_results_table"]["serving_e2e_transfer_path_split_ok"] is True
    assert report["main_results_table"]["ablation_ordering_pass"] is True
    assert report["main_results_table"]["ttft_gain_b2_vs_b1_ms"] == 40.0
    assert report["main_results_table"]["ttft_gain_b7_vs_b3_ms"] == 30.0
    assert report["main_results_table"]["ttft_penalty_b8_vs_b7_ms"] == 90.0
