#!/usr/bin/env python3
"""P02 reader A/B measurement driver.

Two independent comparisons are produced:

  base-vs-p02-json : unmodified baseline JSON decoder (arm A) versus the P02
                     dispatch entry point reading the same JSON corpus (arm P).
                     This isolates the cost the dispatch adds to the legacy
                     path. Independent builds, same corpus, alternating passes.
  p02-json-vs-binary : the P02 entry point on JSON (arm A) versus the framed
                     binary corpus (arm P). Same process image, so this is a
                     dispatch/parse comparison rather than a cross-build one.

Every measurement is a separate process invocation, so each one is an
independent sampling unit. Raw per-repeat samples are kept, and the paired
statistics are computed separately by analysis/paired_stats.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

CORPORA = ("small", "key", "binary")
CSV_HEADER = (
    "label,format,corpus,repeat,iterations,seconds,mb_per_s,ns_per_batch,"
    "ns_per_byte,wire_bytes,entries,payload_bytes,key_bytes,checksum"
)


def log(message: str) -> None:
    print(message, flush=True)


def run_bench(
    binary: Path, fmt: str, corpus: str, label: str, csv_path: Path, args
) -> dict[str, float]:
    command = [
        str(binary),
        f"--format={fmt}",
        f"--corpus={corpus}",
        f"--iterations={args.iterations}",
        f"--warmup={args.warmup}",
        f"--repeats={args.repeats}",
        f"--csv={csv_path}",
        f"--label={label}",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            f"bench failed ({label} {fmt} {corpus}): {result.stderr[-400:]}"
        )
    log(f"  {result.stdout.strip()}")
    values: dict[str, float] = {}
    with csv_path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["label"] != label:
                continue
            values.setdefault("mb_per_s", []).append(float(row["mb_per_s"]))
            values.setdefault("ns_per_batch", []).append(
                float(row["ns_per_batch"])
            )
    ordered = sorted(values["mb_per_s"])
    median = statistics.median(ordered)
    mad = statistics.median([abs(v - median) for v in ordered]) if len(ordered) > 1 else 0.0
    return {
        "mb_per_s_median": median,
        "mb_per_s_min": ordered[0],
        "mb_per_s_max": ordered[-1],
        "mb_per_s_spread": mad,
        "ns_per_batch_median": statistics.median(values["ns_per_batch"]),
        "samples": len(ordered),
    }


def emit_block(
    jsonl: Path,
    session_id: str,
    block_id: str,
    comparison_id: str,
    workload_hash: str,
    metric: str,
    measurements: list[tuple[str, float]],
) -> None:
    """Writes one position-ordered block. Measurements already follow APPA/PAAP."""
    with jsonl.open("a", encoding="utf-8") as handle:
        for position, (arm, value) in enumerate(measurements):
            handle.write(
                json.dumps(
                    {
                        "session_id": session_id,
                        "block_id": block_id,
                        "position": position,
                        "arm": arm,
                        "value": value,
                        "metric": metric,
                        "workload_hash": workload_hash,
                        "comparison_id": comparison_id,
                        "valid": True,
                    },
                    sort_keys=True,
                )
                + "\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-bench", required=True)
    parser.add_argument("--p02-bench", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--passes", type=int, default=4)
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "p02-reader-bench-samples.csv"
    jsonl = out_dir / "p02-reader-ab-arms.jsonl"
    for path in (csv_path, jsonl):
        if path.exists():
            path.unlink()
    csv_path.write_text(CSV_HEADER + "\n", encoding="utf-8")

    baseline = Path(args.baseline_bench)
    p02 = Path(args.p02_bench)
    summary: dict[str, object] = {
        "kind": "p02-reader-ab-measurement",
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "iterations": args.iterations,
        "warmup": args.warmup,
        "repeats_per_run": args.repeats,
        "passes": args.passes,
        "comparisons": {},
    }

    for corpus in CORPORA:
        workload = f"p02-reader-{corpus}"
        log(f"corpus={corpus}")

        # --- base-vs-p02-json, alternating order per pass -------------------
        base_samples: list[float] = []
        p02_json_samples: list[float] = []
        for pass_index in range(args.passes):
            if pass_index % 2 == 0:
                log(f" pass {pass_index}: baseline then p02-json")
                base = run_bench(
                    baseline, "json", corpus, f"base-json-{corpus}", csv_path, args
                )
                p02j = run_bench(
                    p02, "json", corpus, f"p02-json-{corpus}", csv_path, args
                )
            else:
                log(f" pass {pass_index}: p02-json then baseline")
                p02j = run_bench(
                    p02, "json", corpus, f"p02-json-{corpus}", csv_path, args
                )
                base = run_bench(
                    baseline, "json", corpus, f"base-json-{corpus}", csv_path, args
                )
            base_samples.append(base["mb_per_s_median"])
            p02_json_samples.append(p02j["mb_per_s_median"])

        # Balanced pairs: for each pair, one A-B and one B-A ordering.
        for pair_index in range(args.passes // 2):
            block = f"{corpus}-baseP02-{pair_index}"
            emit_block(
                jsonl,
                f"ab-base-p02-{corpus}-{pair_index}",
                block,
                "base-vs-p02-json",
                workload,
                "mb_per_s",
                [
                    ("A", base_samples[2 * pair_index]),
                    ("P", p02_json_samples[2 * pair_index]),
                    ("P", p02_json_samples[2 * pair_index + 1]),
                    ("A", base_samples[2 * pair_index + 1]),
                ],
            )
        summary["comparisons"].setdefault("base-vs-p02-json", {})[corpus] = {
            "baseline_mb_per_s_median": statistics.median(base_samples),
            "p02_mb_per_s_median": statistics.median(p02_json_samples),
            "samples": len(base_samples),
        }

        # --- p02-json-vs-binary, alternating order per pass ----------------
        json_samples: list[float] = []
        binary_samples: list[float] = []
        for pass_index in range(args.passes):
            if pass_index % 2 == 0:
                log(f" pass {pass_index}: p02-json then p02-binary")
                p02j = run_bench(
                    p02, "json", corpus, f"p02-json-{corpus}", csv_path, args
                )
                p02b = run_bench(
                    p02, "binary", corpus, f"p02-binary-{corpus}", csv_path, args
                )
            else:
                log(f" pass {pass_index}: p02-binary then p02-json")
                p02b = run_bench(
                    p02, "binary", corpus, f"p02-binary-{corpus}", csv_path, args
                )
                p02j = run_bench(
                    p02, "json", corpus, f"p02-json-{corpus}", csv_path, args
                )
            json_samples.append(p02j["mb_per_s_median"])
            binary_samples.append(p02b["mb_per_s_median"])

        for pair_index in range(args.passes // 2):
            block = f"{corpus}-jsonBinary-{pair_index}"
            emit_block(
                jsonl,
                f"ab-json-binary-{corpus}-{pair_index}",
                block,
                "p02-json-vs-binary",
                workload,
                "mb_per_s",
                [
                    ("A", json_samples[2 * pair_index]),
                    ("P", binary_samples[2 * pair_index]),
                    ("P", binary_samples[2 * pair_index + 1]),
                    ("A", json_samples[2 * pair_index + 1]),
                ],
            )
        summary["comparisons"].setdefault("p02-json-vs-binary", {})[corpus] = {
            "json_mb_per_s_median": statistics.median(json_samples),
            "binary_mb_per_s_median": statistics.median(binary_samples),
            "samples": len(json_samples),
        }

    summary["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    summary_path = out_dir / "p02-reader-bench-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    log(f"summary written to {summary_path}")
    log(json.dumps(summary["comparisons"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
