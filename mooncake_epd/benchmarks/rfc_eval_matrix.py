"""RFC §8 evaluation-matrix assembly helpers."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from .metrics_suite import build_rfc_eval_report, render_rfc_eval_report_markdown
from .rfc_eval_workloads import DEFAULT_MIX, build_dev_small_workloads, summarize_workloads
from .mooncake_dataset_eval import write_mooncake_dataset_eval_artifacts


def _load_json(path: str | Path, *, default: Any) -> Any:
    p = Path(path)
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def default_workload_manifest(*, mixed_size: int = 20) -> Dict[str, Any]:
    workloads = build_dev_small_workloads(mixed_size=mixed_size)
    return {
        "mix": dict(DEFAULT_MIX),
        "summary": summarize_workloads(workloads),
        "workloads": workloads,
    }


def build_rfc_eval_matrix(
    phase6_metrics: Dict[str, Any],
    soak_report: Dict[str, Any],
    workflow_traces: List[Dict[str, Any]],
    *,
    serving_e2e_summary: Optional[Dict[str, Any]] = None,
    serving_baseline_compare: Optional[Dict[str, Any]] = None,
    feature_handle_e2e_summary: Optional[Dict[str, Any]] = None,
    feature_handle_paired_benchmark: Optional[Dict[str, Any]] = None,
    online_direct_e2e_summary: Optional[Dict[str, Any]] = None,
    workload_manifest: Optional[Dict[str, Any]] = None,
    source_artifacts: Optional[Dict[str, str]] = None,
    workload_manifest_source: str = "in_memory",
) -> Dict[str, Any]:
    manifest = workload_manifest or default_workload_manifest()
    serving_e2e_summary = serving_e2e_summary or {}
    feature_handle_e2e_summary = feature_handle_e2e_summary or {}
    feature_handle_paired_benchmark = feature_handle_paired_benchmark or {}
    online_direct_e2e_summary = online_direct_e2e_summary or {}
    report = build_rfc_eval_report(
        phase6_metrics,
        soak_report,
        workflow_traces,
        serving_e2e_summary=serving_e2e_summary,
        serving_baseline_compare=serving_baseline_compare,
        feature_handle_e2e_summary=feature_handle_e2e_summary,
        feature_handle_paired_benchmark=feature_handle_paired_benchmark,
        online_direct_e2e_summary=online_direct_e2e_summary,
    )
    baseline_rows = list(report.get("baseline_matrix_table") or [])
    workload_rows = list(manifest.get("summary") or report.get("workload_matrix_table") or [])
    dataset_eval = dict(manifest.get("dataset_eval") or {})
    if workload_rows:
        report["workload_matrix_table"] = workload_rows
    if dataset_eval:
        report["dataset_eval_table"] = dataset_eval

    phase6_dataset = dict(phase6_metrics.get("dataset") or {})
    soak_dataset = dict(soak_report.get("dataset_summary") or {})
    phase6_scenarios = dict(phase6_dataset.get("by_scenario") or {})
    soak_scenarios = dict(soak_dataset.get("by_scenario") or {})
    phase6_examples = int(phase6_dataset.get("total_examples", 0) or 0)
    soak_examples = int(soak_dataset.get("total_examples", 0) or 0)

    workload_totals = {
        str(row.get("workload", "unknown")): int(row.get("samples", 0) or 0)
        for row in workload_rows
    }
    baseline_available = [row["id"] for row in baseline_rows if row.get("available")]
    baseline_missing = [row["id"] for row in baseline_rows if not row.get("available")]

    return {
        "generated_at_unix": time.time(),
        "source_artifacts": source_artifacts or {},
        "artifact_status": {
            "phase6_loaded": bool(phase6_metrics),
            "soak_loaded": bool(soak_report),
            "workflow_traces_loaded": bool(workflow_traces),
            "serving_e2e_loaded": bool(serving_e2e_summary),
            "serving_baseline_compare_loaded": bool(serving_baseline_compare),
            "feature_handle_e2e_loaded": bool(feature_handle_e2e_summary),
            "feature_handle_paired_loaded": bool(feature_handle_paired_benchmark),
            "online_direct_e2e_loaded": bool(online_direct_e2e_summary),
            "workload_manifest_loaded": workload_manifest_source == "loaded",
            "workload_manifest_generated": workload_manifest_source == "generated",
            "workload_manifest_source": workload_manifest_source,
        },
        "alignment": {
            "phase6_examples": phase6_examples,
            "soak_examples": soak_examples,
            "workflow_traces": len(workflow_traces),
            "serving_e2e_requests_total": int(
                ((serving_e2e_summary.get("metrics") or {}).get("metrics") or {}).get("requests_total", 0)
                or 0
            ),
            "serving_e2e_pd_requests_total": int(
                ((((serving_e2e_summary.get("metrics") or {}).get("metrics") or {}).get("path_stats") or {}).get("PD") or {}).get("requests_total", 0)
                or 0
            ),
            "serving_e2e_epd_requests_total": int(
                ((((serving_e2e_summary.get("metrics") or {}).get("metrics") or {}).get("path_stats") or {}).get("EPD") or {}).get("requests_total", 0)
                or 0
            ),
            "serving_e2e_registry_events": int(
                serving_e2e_summary.get("workflow_registry_events", 0) or 0
            ),
            "feature_handle_precomputed_hits": int(
                ((report.get("feature_handle_e2e_table") or {}).get("precomputed_hits", 0) or 0)
            ),
            "feature_handle_vision_compute_ms_avg": float(
                ((report.get("feature_handle_e2e_table") or {}).get("hidden_cache_vision_compute_ms_avg", 0.0) or 0.0)
            ),
            "feature_handle_peer_buffer_batches": int(
                ((report.get("feature_handle_e2e_table") or {}).get("peer_buffer_batches", 0) or 0)
            ),
            "feature_handle_peer_buffer_direct_batches": int(
                ((report.get("feature_handle_e2e_table") or {}).get("peer_buffer_direct_batches", 0) or 0)
            ),
            "feature_handle_fallback_batches": int(
                ((report.get("feature_handle_e2e_table") or {}).get("fallback_batches", 0) or 0)
            ),
            "feature_handle_layered_receive_failures": int(
                ((report.get("feature_handle_e2e_table") or {}).get("layered_receive_failures", 0) or 0)
            ),
            "feature_handle_pass": bool(
                ((report.get("feature_handle_e2e_table") or {}).get("pass"))
            ),
            "feature_handle_paired_pass": bool(
                ((report.get("feature_handle_paired_table") or {}).get("pass"))
            ),
            "feature_handle_paired_ttft_reduction_pct": float(
                ((report.get("feature_handle_paired_table") or {}).get("ttft_reduction_pct", 0.0) or 0.0)
            ),
            "feature_handle_paired_goodput_gain_pct": float(
                ((report.get("feature_handle_paired_table") or {}).get("goodput_gain_pct", 0.0) or 0.0)
            ),
            "online_direct_pass": bool(
                ((report.get("online_direct_e2e_table") or {}).get("pass"))
            ),
            "online_direct_peer_buffer_direct_batches": int(
                ((report.get("online_direct_e2e_table") or {}).get("peer_buffer_direct_batches", 0) or 0)
            ),
            "online_direct_buffer_release_ok": bool(
                ((report.get("online_direct_e2e_table") or {}).get("direct_buffer_release_ok"))
            ),
            "serving_correctness_available": bool(
                ((report.get("serving_correctness_table") or {}).get("available"))
            ),
            "serving_correctness_pass": bool(
                ((report.get("serving_correctness_table") or {}).get("pass_recommendation"))
            ),
            "serving_correctness_count": int(
                ((report.get("serving_correctness_table") or {}).get("count", 0) or 0)
            ),
            "serving_correctness_avg_token_jaccard": float(
                ((report.get("serving_correctness_table") or {}).get("avg_token_jaccard", 0.0) or 0.0)
            ),
            "serving_correctness_same_finish_reason_rate": float(
                ((report.get("serving_correctness_table") or {}).get("same_finish_reason_rate", 0.0) or 0.0)
            ),
            "dataset_examples_consistent": (
                phase6_examples == soak_examples if phase6_examples and soak_examples else False
            ),
            "scenario_counts_match": phase6_scenarios == soak_scenarios if phase6_scenarios and soak_scenarios else False,
            "phase6_scenarios": phase6_scenarios,
            "soak_scenarios": soak_scenarios,
            "workload_samples": workload_totals,
            "mooncake_dataset_loaded": bool(dataset_eval),
            "mooncake_dataset_pass": bool(dataset_eval.get("pass")) if dataset_eval else False,
            "mooncake_workflows_total": int((dataset_eval.get("workflow_summary") or {}).get("total_workflows", 0) or 0) if dataset_eval else 0,
            "mooncake_chat_examples_total": int((dataset_eval.get("chat_summary") or {}).get("total_examples", 0) or 0) if dataset_eval else 0,
            "mooncake_w5_schedule_events": int(sum((s.get("event_count", 0) or 0) for s in ((dataset_eval.get("schedule_summary") or {}).get("by_schedule") or {}).values())) if dataset_eval else 0,
        },
        "baseline_coverage": {
            "available": baseline_available,
            "missing": baseline_missing,
            "coverage_ratio": (
                len(baseline_available) / len(baseline_rows)
                if baseline_rows else 0.0
            ),
        },
        "workload_manifest": manifest,
        "report": report,
    }


def write_rfc_eval_matrix_artifacts(
    *,
    phase6_path: str | Path,
    soak_path: str | Path,
    workflow_traces_path: str | Path,
    matrix_output_path: str | Path,
    report_json_path: Optional[str | Path] = None,
    report_md_path: Optional[str | Path] = None,
    manifest_output_path: Optional[str | Path] = None,
    workload_manifest_path: Optional[str | Path] = None,
    ablation_report_path: Optional[str | Path] = None,
    serving_e2e_summary_path: Optional[str | Path] = None,
    serving_baseline_compare_path: Optional[str | Path] = None,
    feature_handle_e2e_summary_path: Optional[str | Path] = None,
    feature_handle_paired_benchmark_path: Optional[str | Path] = None,
    online_direct_e2e_summary_path: Optional[str | Path] = None,
    mixed_size: int = 20,
    dataset_root: Optional[str | Path] = None,
    dataset_eval_output_path: Optional[str | Path] = None,
    dataset_split: str = "dev-small",
    dataset_chat_split: str = "dev-small",
) -> Dict[str, Any]:
    phase6 = _load_json(phase6_path, default={})
    soak = _load_json(soak_path, default={})
    traces = _load_json(workflow_traces_path, default=[])
    serving_e2e_summary = (
        _load_json(serving_e2e_summary_path, default={})
        if serving_e2e_summary_path is not None
        else {}
    )
    serving_baseline_compare = (
        _load_json(serving_baseline_compare_path, default={})
        if serving_baseline_compare_path is not None
        else {}
    )
    feature_handle_e2e_summary = (
        _load_json(feature_handle_e2e_summary_path, default={})
        if feature_handle_e2e_summary_path is not None
        else {}
    )
    feature_handle_paired_benchmark = (
        _load_json(feature_handle_paired_benchmark_path, default={})
        if feature_handle_paired_benchmark_path is not None
        else {}
    )
    online_direct_e2e_summary = (
        _load_json(online_direct_e2e_summary_path, default={})
        if online_direct_e2e_summary_path is not None
        else {}
    )
    ablation_report = _load_json(ablation_report_path, default={}) if ablation_report_path is not None else {}
    if ablation_report and isinstance(ablation_report, dict):
        phase6 = dict(phase6)
        existing_ablations = dict(phase6.get("ablations") or {})
        report_ablations = dict(ablation_report.get("baselines") or {})
        if report_ablations:
            existing_ablations.update(report_ablations)
            phase6["ablations"] = existing_ablations
    manifest: Dict[str, Any]
    generated_dataset_eval: Dict[str, Any] = {}
    if dataset_root is not None:
        eval_out = Path(dataset_eval_output_path) if dataset_eval_output_path is not None else Path(matrix_output_path).parent / "mooncake_dataset_eval.json"
        manifest_out = Path(workload_manifest_path) if workload_manifest_path is not None else Path(matrix_output_path).parent / "mooncake_dataset_manifest.json"
        generated_dataset_eval = write_mooncake_dataset_eval_artifacts(
            dataset_root=dataset_root,
            eval_output_path=eval_out,
            manifest_output_path=manifest_out,
            split=dataset_split,
            chat_split=dataset_chat_split,
        )
        manifest = dict(generated_dataset_eval.get("manifest") or {})
        workload_manifest_path = manifest_out
    else:
        manifest = (
            _load_json(workload_manifest_path, default={})
            if workload_manifest_path is not None
            else default_workload_manifest(mixed_size=mixed_size)
        )
    if not manifest:
        manifest = default_workload_manifest(mixed_size=mixed_size)

    matrix = build_rfc_eval_matrix(
        phase6,
        soak,
        traces,
        serving_e2e_summary=serving_e2e_summary,
        serving_baseline_compare=serving_baseline_compare,
        feature_handle_e2e_summary=feature_handle_e2e_summary,
        feature_handle_paired_benchmark=feature_handle_paired_benchmark,
        online_direct_e2e_summary=online_direct_e2e_summary,
        workload_manifest=manifest,
        source_artifacts={
            "phase6_metrics": str(Path(phase6_path)),
            "soak_report": str(Path(soak_path)),
            "workflow_traces": str(Path(workflow_traces_path)),
            "workload_manifest": str(Path(workload_manifest_path)) if workload_manifest_path else "",
            "mooncake_dataset_eval": str(Path(dataset_eval_output_path)) if dataset_eval_output_path else (str(Path(matrix_output_path).parent / "mooncake_dataset_eval.json") if dataset_root is not None else ""),
            "ablation_report": str(Path(ablation_report_path)) if ablation_report_path else "",
            "serving_e2e_summary": str(Path(serving_e2e_summary_path)) if serving_e2e_summary_path else "",
            "serving_baseline_compare": str(Path(serving_baseline_compare_path)) if serving_baseline_compare_path else "",
            "feature_handle_e2e_summary": str(Path(feature_handle_e2e_summary_path)) if feature_handle_e2e_summary_path else "",
            "feature_handle_paired_benchmark": str(Path(feature_handle_paired_benchmark_path)) if feature_handle_paired_benchmark_path else "",
            "online_direct_e2e_summary": str(Path(online_direct_e2e_summary_path)) if online_direct_e2e_summary_path else "",
        },
        workload_manifest_source="loaded" if workload_manifest_path is not None else "generated",
    )

    matrix_path = Path(matrix_output_path)
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    matrix_path.write_text(json.dumps(matrix, ensure_ascii=False, indent=2), encoding="utf-8")

    if manifest_output_path is not None:
        manifest_path = Path(manifest_output_path)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    report = matrix["report"]
    if report_json_path is not None:
        json_path = Path(report_json_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if report_md_path is not None:
        md_path = Path(report_md_path)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(render_rfc_eval_report_markdown(report), encoding="utf-8")

    return matrix
