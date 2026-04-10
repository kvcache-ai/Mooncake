#!/usr/bin/env python3

import argparse
import glob
import json
from pathlib import Path
from typing import Any


def expand_inputs(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            files.extend(Path(match) for match in matches)
        else:
            files.append(Path(pattern))
    deduped = sorted({path for path in files if path.exists()})
    return deduped


def load_records(paths: list[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
                payload["_source_file"] = str(path)
                records.append(payload)
    return records


def filter_records(records: list[dict[str, Any]], correlation_id: str,
                   trace_id: str) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for record in records:
        if correlation_id and record.get("correlation.id") != correlation_id:
            continue
        if trace_id and record.get("trace_id") != trace_id:
            continue
        filtered.append(record)
    return filtered


def relative_ms(record: dict[str, Any], base_time: int) -> tuple[float, float]:
    start = int(record.get("start_time_unix_nano", 0))
    end = int(record.get("end_time_unix_nano", start))
    start_ms = (start - base_time) / 1_000_000.0
    duration_ms = max(0.0, (end - start) / 1_000_000.0)
    return start_ms, duration_ms


def build_markdown(records: list[dict[str, Any]]) -> str:
    if not records:
        return "# Trace Report\n\nNo matching spans.\n"

    base_time = min(int(record.get("start_time_unix_nano", 0)) for record in records)
    lines = [
        "# Trace Report",
        "",
        f"- span_count: {len(records)}",
        f"- trace_id: {records[0].get('trace_id', '')}",
        f"- correlation.id: {records[0].get('correlation.id', '')}",
        "",
        "## Waterfall",
        "",
        "| start_ms | duration_ms | service | role | span | status |",
        "| ---: | ---: | --- | --- | --- | --- |",
    ]
    for record in records:
        start_ms, duration_ms = relative_ms(record, base_time)
        lines.append(
            "| "
            f"{start_ms:.3f} | {duration_ms:.3f} | "
            f"{record.get('service.name', '')} | "
            f"{record.get('process.role', '')} | "
            f"{record.get('span.name', '')} | "
            f"{record.get('status', '')} |"
        )

    lines.extend(["", "## Phase Table", "",
                  "| span | parent_span_id | node | source_file |",
                  "| --- | --- | --- | --- |"])
    for record in records:
        lines.append(
            "| "
            f"{record.get('span.name', '')} | "
            f"{record.get('parent_span_id', '')} | "
            f"{record.get('node.id', '')} | "
            f"{record.get('_source_file', '')} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Merge Mooncake trace JSONL files into a single report.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input JSONL files or glob patterns, for example '/var/log/mooncake/*.jsonl'.",
    )
    parser.add_argument("--correlation-id", default="", help="Filter by correlation.id.")
    parser.add_argument("--trace-id", default="", help="Filter by trace_id.")
    parser.add_argument("--output-json", default="", help="Write merged JSON to this path.")
    parser.add_argument("--output-md", default="", help="Write markdown report to this path.")
    args = parser.parse_args()

    if not args.correlation_id and not args.trace_id:
        parser.error("one of --correlation-id or --trace-id is required")

    input_paths = expand_inputs(args.inputs)
    if not input_paths:
        raise SystemExit("no input files matched")

    records = load_records(input_paths)
    records = filter_records(records, args.correlation_id, args.trace_id)
    records.sort(key=lambda record: (
        int(record.get("start_time_unix_nano", 0)),
        str(record.get("span_id", "")),
    ))

    merged = {
        "trace_id": records[0].get("trace_id", "") if records else "",
        "correlation.id": records[0].get("correlation.id", "") if records else "",
        "span_count": len(records),
        "spans": records,
    }

    if args.output_json:
        output_json = Path(args.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(merged, indent=2), encoding="utf-8")

    markdown = build_markdown(records)
    if args.output_md:
        output_md = Path(args.output_md)
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
