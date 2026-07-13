#!/usr/bin/env python3
"""Export RFC-style WorkflowTrace JSON for the synthetic dataset."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mooncake_epd.benchmarks.workflow_trace import export_workflow_traces


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent.parent / "artifacts" / "workflow_traces.json"),
    )
    args = ap.parse_args()

    out = export_workflow_traces(args.output)
    print(out)


if __name__ == "__main__":
    main()
