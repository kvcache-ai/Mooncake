#!/usr/bin/env python3
"""Mechanically verify receiver-credit experiment JSONL records."""

import argparse
import glob
import json
import math
import sys


def csv_set(value, cast=str):
    return {cast(item) for item in value.split(",") if item}


def load_records(patterns):
    records = []
    for pattern in patterns:
        for path in sorted(glob.glob(pattern)):
            with open(path, encoding="utf-8") as source:
                for line_number, line in enumerate(source, 1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise ValueError(f"{path}:{line_number}: {error}") from error
                    if record.get("schema_version") != 1:
                        raise ValueError(
                            f"{path}:{line_number}: unsupported schema_version"
                        )
                    records.append(record)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--require-modes", default="")
    parser.add_argument("--require-senders", default="")
    parser.add_argument("--require-conditions", default="")
    parser.add_argument("--min-repetitions", type=int, default=1)
    parser.add_argument(
        "--require-credit-capacity-violations", type=int, default=0
    )
    parser.add_argument("--require-data-errors", type=int, default=0)
    parser.add_argument("--require-fixed-overload-evidence", action="store_true")
    parser.add_argument("--min-credit-oracle-utilization-ratio", type=float)
    args = parser.parse_args()

    try:
        records = load_records(args.input)
    except (OSError, ValueError) as error:
        print(f"VERIFY status=FAIL error={error}")
        return 2
    if not records:
        print("VERIFY status=FAIL error=no_records")
        return 2

    required_modes = csv_set(args.require_modes)
    required_senders = csv_set(args.require_senders, int)
    required_conditions = csv_set(args.require_conditions)
    errors = []

    grouped = {}
    for record in records:
        key = (
            record.get("mode"),
            record.get("sender_count"),
            record.get("condition"),
        )
        grouped.setdefault(key, []).append(record)
        if record.get("data_errors") != args.require_data_errors:
            errors.append(f"data_errors:{record.get('run_id', '?')}")
        if record.get("completed") != record.get("offered"):
            errors.append(f"completion_mismatch:{record.get('run_id', '?')}")
        receiver = record.get("receiver", {})
        if receiver.get("invalid_release_total", 0) != 0:
            errors.append(f"invalid_release:{record.get('run_id', '?')}")
        if record.get("mode") == "credit":
            if (
                receiver.get("capacity_violation_total")
                != args.require_credit_capacity_violations
            ):
                errors.append(f"credit_capacity_violation:{record.get('run_id', '?')}")
            if receiver.get("peak_bytes", 0) > receiver.get("capacity_bytes", 0):
                errors.append(f"credit_peak_bytes:{record.get('run_id', '?')}")
            if receiver.get("peak_slots", 0) > receiver.get("capacity_slots", 0):
                errors.append(f"credit_peak_slots:{record.get('run_id', '?')}")

    for mode in required_modes:
        for senders in required_senders:
            for condition in required_conditions:
                count = len(grouped.get((mode, senders, condition), []))
                if count < args.min_repetitions:
                    errors.append(
                        f"missing:{mode}:{senders}:{condition}:{count}/"
                        f"{args.min_repetitions}"
                    )

    if args.require_fixed_overload_evidence:
        has_overload = any(
            record.get("mode") in {"disabled", "fixed"}
            and record.get("receiver", {}).get("capacity_violation_total", 0) > 0
            for record in records
        )
        if not has_overload:
            errors.append("missing_fixed_overload_evidence")

    if args.min_credit_oracle_utilization_ratio is not None:
        ratios = []
        for record in records:
            if record.get("mode") != "credit":
                continue
            oracle = record.get("oracle_throughput_gbps", 0.0)
            throughput = record.get("throughput_gbps", 0.0)
            if oracle > 0.0 and math.isfinite(oracle) and math.isfinite(throughput):
                ratios.append(throughput / oracle)
        if not ratios:
            errors.append("missing_credit_oracle_ratio")
        elif min(ratios) < args.min_credit_oracle_utilization_ratio:
            errors.append(
                f"credit_oracle_ratio:{min(ratios):.6f}/"
                f"{args.min_credit_oracle_utilization_ratio:.6f}"
            )

    credit_violations = sum(
        record.get("receiver", {}).get("capacity_violation_total", 0)
        for record in records
        if record.get("mode") == "credit"
    )
    if errors:
        print(
            f"VERIFY metric=receiver_capacity_violations "
            f"value={credit_violations} status=FAIL errors={'|'.join(errors)}"
        )
        return 1
    print(
        f"VERIFY metric=receiver_capacity_violations "
        f"value={credit_violations} status=PASS"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
