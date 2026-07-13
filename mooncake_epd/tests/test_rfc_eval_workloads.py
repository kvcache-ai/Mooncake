from __future__ import annotations

import json

from mooncake_epd.benchmarks.metrics_suite import build_rfc_eval_report
from mooncake_epd.benchmarks.rfc_eval_workloads import (
    build_dev_small_workloads,
    export_dev_small_manifest,
    summarize_workloads,
)


def test_build_dev_small_workloads_covers_w0_to_w5():
    workloads = build_dev_small_workloads(mixed_size=12)
    assert set(workloads) == {"W0", "W1", "W2", "W3", "W4", "W5"}
    assert workloads["W0"]
    assert workloads["W1"]
    assert workloads["W2"]
    assert workloads["W3"]
    assert workloads["W4"]
    assert workloads["W5"]

    summary = summarize_workloads(workloads)
    by_workload = {row["workload"]: row for row in summary}
    assert by_workload["W0"]["avg_steps"] == 1.0
    assert by_workload["W1"]["avg_steps"] >= 2.0
    assert by_workload["W5"]["samples"] >= 12


def test_export_dev_small_manifest_and_report_includes_workload_table(tmp_path):
    out = export_dev_small_manifest(tmp_path / "dev_small.json", mixed_size=10)
    payload = json.loads(out.read_text())
    assert "summary" in payload
    assert payload["workloads"]["W5"]

    report = build_rfc_eval_report(
        phase6_metrics={"summary": {}, "epd_step0": [], "cross_step_reuse": []},
        soak_report={"summary": {}, "dataset_summary": {}, "results": []},
        workflow_traces=[],
    )
    assert "workload_matrix_table" in report
    assert any(row["workload"] == "W5" for row in report["workload_matrix_table"])
