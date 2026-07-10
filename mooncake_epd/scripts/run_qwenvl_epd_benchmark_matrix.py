"""Run/assemble a real-capable Qwen-VL EPD benchmark matrix.

This script can execute caller-supplied real benchmark commands or assemble
already-produced summaries.  It does not fabricate gains: the paired claim gate
is computed from the supplied/produced artifacts and fails closed.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.check_epd_performance_claims import evaluate_claims


def _load(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def _run_command(command: Optional[str]) -> Dict[str, Any]:
    if not command:
        return {"executed": False}
    started = time.time()
    proc = subprocess.run(shlex.split(command), capture_output=True, text=True, check=False)
    return {
        "executed": True,
        "command": command,
        "returncode": proc.returncode,
        "elapsed_s": time.time() - started,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def build_matrix(
    *,
    dataset: str,
    baseline_summary: Optional[str],
    epd_summary: Optional[str],
    output: str,
    protocol: str = "tcp",
    run_baseline_cmd: Optional[str] = None,
    run_epd_cmd: Optional[str] = None,
) -> Dict[str, Any]:
    baseline_run = _run_command(run_baseline_cmd)
    if baseline_run.get("returncode", 0) not in (0, None):
        raise RuntimeError(f"baseline command failed: {baseline_run}")
    epd_run = _run_command(run_epd_cmd)
    if epd_run.get("returncode", 0) not in (0, None):
        raise RuntimeError(f"EPD command failed: {epd_run}")

    dataset_rows = 0
    dataset_path = Path(dataset)
    if dataset_path.exists():
        dataset_rows = sum(1 for line in dataset_path.read_text(encoding="utf-8").splitlines() if line.strip())
    matrix = {
        "generated_at_unix": time.time(),
        "protocol": str(protocol),
        "dataset": {"path": str(dataset), "rows": dataset_rows},
        "baseline": _load(baseline_summary),
        "epd": _load(epd_summary),
        "executed_steps": {"baseline": baseline_run, "epd": epd_run},
    }
    matrix["claim_gate"] = evaluate_claims(matrix)
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(matrix, ensure_ascii=False, indent=2), encoding="utf-8")
    return matrix


def main() -> None:
    ap = argparse.ArgumentParser(description="Run/assemble Qwen-VL EPD benchmark matrix")
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--baseline-summary", default=None)
    ap.add_argument("--epd-summary", default=None)
    ap.add_argument("--output", required=True)
    ap.add_argument("--protocol", default="tcp", choices=["tcp", "rdma", "shm", "local"])
    ap.add_argument("--run-baseline-cmd", default=None)
    ap.add_argument("--run-epd-cmd", default=None)
    ap.add_argument("--enforce-claims", action="store_true")
    args = ap.parse_args()
    matrix = build_matrix(
        dataset=args.dataset,
        baseline_summary=args.baseline_summary,
        epd_summary=args.epd_summary,
        output=args.output,
        protocol=args.protocol,
        run_baseline_cmd=args.run_baseline_cmd,
        run_epd_cmd=args.run_epd_cmd,
    )
    print(json.dumps({"output": args.output, "claim_gate": matrix["claim_gate"]}, ensure_ascii=False, indent=2))
    if args.enforce_claims and not matrix["claim_gate"].get("pass"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
