#!/usr/bin/env python3
"""Evaluate or explicitly execute a manifest-driven real EPD benchmark campaign."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.benchmarks.epd_benchmark_campaign import (  # noqa: E402
    load_and_run_campaign,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="JSON campaign manifest")
    parser.add_argument("--output", required=True, help="JSON report path")
    parser.add_argument(
        "--execute-commands",
        action="store_true",
        help="Run manifest commands sequentially; omitted by default for artifact-only evaluation.",
    )
    parser.add_argument(
        "--enforce-complete",
        action="store_true",
        help="Return nonzero unless every required scenario passed.",
    )
    args = parser.parse_args()
    report = load_and_run_campaign(
        args.manifest,
        execute_commands=bool(args.execute_commands),
    )
    output = Path(args.output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(output),
                "coverage": report["coverage"],
                "comparison_statuses": {
                    item["id"]: item["status"]
                    for item in report.get("comparisons", [])
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if args.enforce_complete and not bool(report["coverage"].get("complete")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
