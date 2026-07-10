from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent.parent / "artifacts"
    ap = argparse.ArgumentParser(description="Check RFC report/matrix regression gates.")
    ap.add_argument("--report", default=str(root / "rfc_eval_report.json"))
    ap.add_argument("--matrix", default=str(root / "rfc_eval_matrix.json"))
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    report = _load_json(args.report)
    matrix = _load_json(args.matrix)

    main_results = dict(report.get("main_results_table") or {})
    serving_sanity = dict(report.get("serving_e2e_sanity_table") or {})
    ablation_ordering = dict(report.get("ablation_ordering_table") or {})
    coverage = dict(matrix.get("baseline_coverage") or {})
    alignment = dict(matrix.get("alignment") or {})

    failures: list[str] = []
    if float(coverage.get("coverage_ratio", 0.0) or 0.0) < 1.0:
        failures.append("baseline coverage ratio < 1.0")
    if not bool(main_results.get("serving_e2e_pass")):
        failures.append("serving_e2e_pass != true")
    if not bool(main_results.get("serving_e2e_sanity_pass")):
        failures.append("serving_e2e_sanity_pass != true")
    if not bool(main_results.get("serving_e2e_path_split_ok")):
        failures.append("serving_e2e_path_split_ok != true")
    if not bool(main_results.get("serving_e2e_transfer_path_split_ok")):
        failures.append("serving_e2e_transfer_path_split_ok != true")
    if not bool(main_results.get("serving_e2e_connector_path_totals_conserved")):
        failures.append("serving_e2e_connector_path_totals_conserved != true")
    if int(main_results.get("serving_e2e_connector_workers", 0) or 0) < 2:
        failures.append("serving_e2e_connector_workers < 2")
    if int(main_results.get("serving_e2e_registry_events", 0) or 0) <= 0:
        failures.append("serving_e2e_registry_events <= 0")
    if "serving_mm_prefetch_hot_path_ok" in main_results and not bool(
        main_results.get("serving_mm_prefetch_hot_path_ok")
    ):
        failures.append("serving_mm_prefetch_hot_path_ok != true")
    if "serving_cross_step_reuse_hot_path_ok" in main_results and not bool(
        main_results.get("serving_cross_step_reuse_hot_path_ok")
    ):
        failures.append("serving_cross_step_reuse_hot_path_ok != true")
    if bool(main_results.get("serving_correctness_available")) and not bool(
        main_results.get("serving_correctness_pass")
    ):
        failures.append("serving_correctness_pass != true")
    if not bool(serving_sanity.get("sanity_pass")):
        failures.append("serving_e2e_sanity_table.sanity_pass != true")
    if not bool(ablation_ordering.get("ordering_pass")):
        failures.append("ablation_ordering_pass != true")
    if float(main_results.get("ttft_gain_b2_vs_b1_ms", 0.0) or 0.0) <= 0.0:
        failures.append("ttft_gain_b2_vs_b1_ms <= 0")
    if float(main_results.get("ttft_gain_b7_vs_b3_ms", 0.0) or 0.0) <= 0.0:
        failures.append("ttft_gain_b7_vs_b3_ms <= 0")
    if float(main_results.get("ttft_penalty_b8_vs_b7_ms", 0.0) or 0.0) <= 0.0:
        failures.append("ttft_penalty_b8_vs_b7_ms <= 0")
    if bool(alignment.get("mooncake_dataset_loaded")) and not bool(alignment.get("mooncake_dataset_pass")):
        failures.append("mooncake_dataset_pass != true")

    payload = {
        "report": args.report,
        "matrix": args.matrix,
        "pass": not failures,
        "failures": failures,
        "summary": {
            "coverage_ratio": coverage.get("coverage_ratio"),
            "serving_e2e_pass": main_results.get("serving_e2e_pass"),
            "serving_e2e_sanity_pass": main_results.get("serving_e2e_sanity_pass"),
            "serving_e2e_path_split_ok": main_results.get("serving_e2e_path_split_ok"),
            "serving_e2e_transfer_path_split_ok": main_results.get(
                "serving_e2e_transfer_path_split_ok"
            ),
            "serving_e2e_connector_path_totals_conserved": main_results.get(
                "serving_e2e_connector_path_totals_conserved"
            ),
            "serving_mm_prefetch_hot_path_ok": main_results.get("serving_mm_prefetch_hot_path_ok"),
            "serving_mm_prefetch_completed": main_results.get("serving_mm_prefetch_completed"),
            "serving_cross_step_reuse_hot_path_ok": main_results.get("serving_cross_step_reuse_hot_path_ok"),
            "serving_cross_step_reuse_candidates": main_results.get("serving_cross_step_reuse_candidates"),
            "serving_dataset_goodput_rps": main_results.get("serving_dataset_goodput_rps"),
            "serving_correctness_available": main_results.get("serving_correctness_available"),
            "serving_correctness_pass": main_results.get("serving_correctness_pass"),
            "serving_correctness_count": main_results.get("serving_correctness_count"),
            "serving_correctness_avg_token_jaccard": main_results.get(
                "serving_correctness_avg_token_jaccard"
            ),
            "ablation_ordering_pass": main_results.get("ablation_ordering_pass"),
            "ttft_gain_b2_vs_b1_ms": main_results.get("ttft_gain_b2_vs_b1_ms"),
            "ttft_gain_b7_vs_b3_ms": main_results.get("ttft_gain_b7_vs_b3_ms"),
            "ttft_penalty_b8_vs_b7_ms": main_results.get("ttft_penalty_b8_vs_b7_ms"),
            "mooncake_dataset_loaded": alignment.get("mooncake_dataset_loaded"),
            "mooncake_dataset_pass": alignment.get("mooncake_dataset_pass"),
            "mooncake_workflows_total": alignment.get("mooncake_workflows_total"),
            "mooncake_chat_examples_total": alignment.get("mooncake_chat_examples_total"),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
