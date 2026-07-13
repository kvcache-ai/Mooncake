from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.benchmarks.rfc_eval_matrix import (  # noqa: E402
    write_rfc_eval_matrix_artifacts,
)


def parse_args() -> argparse.Namespace:
    artifacts = REPO_ROOT / "artifacts"
    ap = argparse.ArgumentParser(
        description="Build the RFC evaluation report from unified matrix inputs.",
    )
    ap.add_argument("--phase6", default=str(artifacts / "phase6_metrics.json"))
    ap.add_argument("--soak", default=str(artifacts / "real_soak_report_post_kvdir.json"))
    ap.add_argument("--traces", default=str(artifacts / "workflow_traces.json"))
    ap.add_argument("--ablation-output", default=str(artifacts / "real_ablation_report.json"))
    ap.add_argument("--serving-e2e-summary", default=str(artifacts / "serving_e2e_summary.json"))
    ap.add_argument("--serving-baseline-compare", default=str(artifacts / "serving_baseline_compare.json"))
    ap.add_argument("--workload-manifest", default=None)
    ap.add_argument("--manifest-out", default=str(artifacts / "rfc_dev_small_manifest.json"))
    ap.add_argument("--matrix-out", default=str(artifacts / "rfc_eval_matrix.json"))
    ap.add_argument("--report-json-out", default=str(artifacts / "rfc_eval_report.json"))
    ap.add_argument("--report-md-out", default=str(artifacts / "rfc_eval_report.md"))
    ap.add_argument("--mixed-size", type=int, default=20)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    matrix = write_rfc_eval_matrix_artifacts(
        phase6_path=args.phase6,
        soak_path=args.soak,
        workflow_traces_path=args.traces,
        matrix_output_path=args.matrix_out,
        report_json_path=args.report_json_out,
        report_md_path=args.report_md_out,
        manifest_output_path=args.manifest_out,
        workload_manifest_path=args.workload_manifest,
        ablation_report_path=args.ablation_output,
        serving_e2e_summary_path=args.serving_e2e_summary,
        serving_baseline_compare_path=args.serving_baseline_compare,
        mixed_size=args.mixed_size,
    )
    print(
        json.dumps(
            {
                "matrix_out": args.matrix_out,
                "report_json_out": args.report_json_out,
                "report_md_out": args.report_md_out,
                "manifest_out": args.manifest_out,
                "baseline_coverage": matrix.get("baseline_coverage", {}),
                "alignment": matrix.get("alignment", {}),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
