#!/usr/bin/env python3
"""Summarize repeatable batch-record OpLog benchmark samples."""

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def _stats(values):
    median = statistics.median(values)
    mean = statistics.mean(values)
    return {
        "median": median,
        "min": min(values),
        "max": max(values),
        "cv": statistics.pstdev(values) / mean if mean else None,
    }


def _median_field(rows, name):
    values = [row[name] for row in rows if isinstance(row.get(name), (int, float))]
    return statistics.median(values) if len(values) == len(rows) else None


def _summarize_case(name, rows):
    result = {"case": name, "repeats": len(rows), "status": "ok", "missing": []}
    throughput = [
        row["entries_per_sec"]
        for row in rows
        if isinstance(row.get("entries_per_sec"), (int, float))
    ]
    if throughput:
        result["entries_per_sec"] = _stats(throughput)
    else:
        result["missing"].append("entries_per_sec")

    cpu_cores = [
        row["cpu_cores_used"]
        for row in rows
        if isinstance(row.get("cpu_cores_used"), (int, float))
    ]
    if len(cpu_cores) == len(rows):
        result["cpu_cores_used"] = _stats(cpu_cores)

    if len(rows) < 3:
        result["missing"].append("at least 3 repeats")

    entries = _median_field(rows, "entries")
    encoded_bytes = _median_field(rows, "encoded_bytes")
    if entries and encoded_bytes is not None:
        result["bytes_per_entry"] = encoded_bytes / entries
    else:
        result["missing"].append("encoded_bytes and entries")

    stage_names = sorted(
        set().union(*(row.get("stage_latency_us", {}).keys() for row in rows))
    )
    stage_medians = {}
    for stage in stage_names:
        values = [row.get("stage_latency_us", {}).get(stage) for row in rows]
        if all(isinstance(value, (int, float)) for value in values):
            stage_medians[stage] = statistics.median(values)
    total_stage_latency = sum(stage_medians.values())
    if total_stage_latency:
        result["stage_latency_us"] = stage_medians
        result["stage_share"] = {
            stage: value / total_stage_latency for stage, value in stage_medians.items()
        }
        result["bottleneck_stage"] = max(stage_medians, key=stage_medians.get)

    txn_us = stage_medians.get("txn")
    batch_entries = _median_field(rows, "batch_entries_mean")
    if txn_us and batch_entries:
        theoretical = batch_entries * 1_000_000 / txn_us
        result["theoretical_entries_per_sec"] = theoretical
        if throughput:
            result["observed_efficiency"] = statistics.median(throughput) / theoretical
    else:
        result["missing"].append("stage_latency_us.txn")
        if not batch_entries:
            result["missing"].append("batch_entries_mean")

    if result["missing"]:
        result["status"] = "insufficient_data"
    return result


def _write_markdown(path, summary):
    lines = [
        "# OpLog Batch Performance Report",
        "",
        "| Case | Status | Repeats | Median entries/s | CV | CPU cores | Bytes/entry | Bottleneck | Theoretical entries/s | Efficiency |",
        "|---|---:|---:|---:|---:|---:|---:|---|---:|---:|",
    ]
    for case in summary["cases"]:
        throughput = case.get("entries_per_sec", {})
        lines.append(
            "| {case} | {status} | {repeats} | {median} | {cv} | {cpu} | {bytes_per_entry} | {bottleneck} | {theoretical} | {efficiency} |".format(
                case=case["case"],
                status=case["status"],
                repeats=case["repeats"],
                median=throughput.get("median", "n/a"),
                cv=_format_number(throughput.get("cv")),
                cpu=_format_number(case.get("cpu_cores_used", {}).get("median")),
                bytes_per_entry=_format_number(case.get("bytes_per_entry")),
                bottleneck=case.get("bottleneck_stage", "n/a"),
                theoretical=_format_number(case.get("theoretical_entries_per_sec")),
                efficiency=_format_number(case.get("observed_efficiency")),
            )
        )
    path.write_text("\n".join(lines) + "\n")


def _format_number(value):
    return "n/a" if value is None else f"{value:.4g}"


def generate_report(samples_path, output_dir):
    samples_path = Path(samples_path)
    output_dir = Path(output_dir)
    grouped = defaultdict(list)
    with samples_path.open() as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("schema_version") != 1 or not row.get("case"):
                raise ValueError(f"invalid sample at line {line_number}")
            grouped[row["case"]].append(row)

    summary = {
        "schema_version": 1,
        "sample_file": str(samples_path),
        "cases": [_summarize_case(name, grouped[name]) for name in sorted(grouped)],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    _write_markdown(output_dir / "report.md", summary)
    with (output_dir / "cases.csv").open("w", newline="") as stream:
        fields = [
            "case",
            "status",
            "repeats",
            "median_entries_per_sec",
            "cv",
            "cpu_cores_used",
            "bytes_per_entry",
            "bottleneck_stage",
            "theoretical_entries_per_sec",
            "observed_efficiency",
        ]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for case in summary["cases"]:
            writer.writerow(
                {
                    "case": case["case"],
                    "status": case["status"],
                    "repeats": case["repeats"],
                    "median_entries_per_sec": case.get("entries_per_sec", {}).get(
                        "median"
                    ),
                    "cv": case.get("entries_per_sec", {}).get("cv"),
                    "cpu_cores_used": case.get("cpu_cores_used", {}).get("median"),
                    "bytes_per_entry": case.get("bytes_per_entry"),
                    "bottleneck_stage": case.get("bottleneck_stage"),
                    "theoretical_entries_per_sec": case.get(
                        "theoretical_entries_per_sec"
                    ),
                    "observed_efficiency": case.get("observed_efficiency"),
                }
            )
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    summary = generate_report(args.samples, args.output_dir)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
