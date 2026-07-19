"""Run/assemble a real-capable Qwen-VL EPD benchmark matrix.

This script can execute caller-supplied real benchmark commands or assemble
already-produced summaries.  It does not fabricate gains: the paired claim gate
is computed from the supplied/produced artifacts and fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.scripts.check_epd_performance_claims import evaluate_claims


def _load(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def _summary_path_from_stdout(stdout: str) -> Optional[str]:
    """Extract the summary path printed by a real runner."""

    for line in reversed(str(stdout or "").splitlines()):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        candidate = payload.get("summary") if isinstance(payload, dict) else None
        if candidate:
            path = Path(str(candidate)).expanduser()
            if path.exists():
                return str(path)
    # Repository runners print a pretty-printed JSON object. The object spans
    # multiple lines, so the line-oriented parser above cannot decode it. Keep
    # the fallback deliberately narrow: only accept a JSON string value for the
    # well-known ``summary`` field and require the file to exist.
    match = re.search(r'"summary"\s*:\s*"((?:\\.|[^"\\])*)"', str(stdout or ""))
    if match:
        try:
            candidate = json.loads(f'"{match.group(1)}"')
        except json.JSONDecodeError:
            candidate = None
        if candidate:
            path = Path(str(candidate)).expanduser()
            if path.exists():
                return str(path)
    return None


def _run_command(command: Optional[str]) -> Dict[str, Any]:
    if not command:
        return {"executed": False}
    started = time.time()
    proc = subprocess.run(shlex.split(command), capture_output=True, text=True, check=False)
    result = {
        "executed": True,
        "command": command,
        "returncode": proc.returncode,
        "elapsed_s": time.time() - started,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    summary_path = _summary_path_from_stdout(proc.stdout)
    if summary_path:
        result["summary_path"] = summary_path
        result["summary_source"] = "runner_stdout"
    return result


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if raw.strip():
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("benchmark manifest rows must be JSON objects")
            rows.append(value)
    return rows


def _request_fingerprint(rows: List[Dict[str, Any]]) -> str:
    # The client/scenario runner should additionally emit this value.  Keeping a
    # canonical manifest digest in the matrix makes accidental workload drift
    # auditable even when commands are supplied externally.
    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _raw_artifact_gate(summary: Dict[str, Any], *, label: str) -> List[str]:
    """Require raw evidence paths before a real performance claim can pass."""

    raw = summary.get("raw_artifacts")
    if not isinstance(raw, dict):
        return [f"{label}:missing_raw_artifacts"]
    required = ("requests_jsonl", "responses_jsonl", "service_logs", "metrics", "environment")
    failures = []
    for field in required:
        value = raw.get(field)
        if not value or not Path(str(value)).exists():
            failures.append(f"{label}:missing_raw_artifact:{field}")
    return failures


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

    baseline_summary = baseline_summary or baseline_run.get("summary_path")
    epd_summary = epd_summary or epd_run.get("summary_path")
    if run_baseline_cmd and not baseline_summary:
        raise RuntimeError(
            "baseline command completed but did not expose a summary path; "
            "pass --baseline-summary or print a JSON object containing summary"
        )
    if run_epd_cmd and not epd_summary:
        raise RuntimeError(
            "EPD command completed but did not expose a summary path; "
            "pass --epd-summary or print a JSON object containing summary"
        )

    dataset_rows = 0
    dataset_path = Path(dataset)
    rows: List[Dict[str, Any]] = []
    if dataset_path.exists():
        rows = _load_jsonl(str(dataset_path))
        dataset_rows = len(rows)
    baseline_payload = _load(baseline_summary)
    epd_payload = _load(epd_summary)
    matrix = {
        "generated_at_unix": time.time(),
        "protocol": str(protocol),
        "dataset": {
            "path": str(dataset),
            "rows": dataset_rows,
            "request_fingerprint": _request_fingerprint(rows) if rows else "",
        },
        "baseline": baseline_payload,
        "epd": epd_payload,
        "executed_steps": {"baseline": baseline_run, "epd": epd_run},
    }
    matrix["claim_gate"] = evaluate_claims(matrix)
    raw_failures = _raw_artifact_gate(baseline_payload, label="baseline") + _raw_artifact_gate(
        epd_payload, label="epd"
    )
    if raw_failures:
        matrix["claim_gate"]["pass"] = False
        matrix["claim_gate"]["failures"] = list(matrix["claim_gate"].get("failures") or []) + raw_failures
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
