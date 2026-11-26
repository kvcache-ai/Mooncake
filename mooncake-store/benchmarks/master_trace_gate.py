#!/usr/bin/env python3
import argparse
import json
import math
import sys

PLAN_FIELDS = (
    "model",
    "profile_hash",
    "seed",
    "amplify",
    "object_bytes",
    "key_bytes",
    "keys_per_clone",
    "speed",
    "clone_spread_us",
    "segment_bytes",
    "allow_eviction",
)

OP_KINDS = ("put", "get", "exist")


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(2)


def load(path):
    try:
        with open(path, "r", encoding="utf-8") as stream:
            report = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path}: {error}")
    if not isinstance(report, dict) or report.get("benchmark") != "master_trace":
        fail(f"not a master_trace result: {path}")
    return report


def finite_number(value, field):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        fail(f"invalid result: {field} is not a finite number")
    return float(value)


def non_negative_int(report, field, path):
    if field not in report:
        fail(f"invalid result ({path}): missing {field}")
    value = report[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        fail(f"invalid result ({path}): {field} must be a non-negative integer")
    return value


def latency_stats(report, op, path):
    field = f"{op}_latency_us"
    value = report.get(field)
    if not isinstance(value, dict):
        fail(f"invalid result ({path}): {field} is missing")
    p50 = finite_number(value.get("p50"), f"{field}.p50")
    p99 = finite_number(value.get("p99"), f"{field}.p99")
    p95 = finite_number(value["p95"], f"{field}.p95") if "p95" in value else 0.0
    return p50, p95, p99


def pct_increase(base, cur):
    if base <= 0:
        return None
    return (cur - base) / base * 100.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--max-rps-drop-pct", type=float, default=10.0)
    parser.add_argument("--max-p50-increase-pct", type=float, default=10.0)
    parser.add_argument("--min-latency-samples", type=int, default=30)
    args = parser.parse_args()

    if args.max_rps_drop_pct < 0 or not math.isfinite(args.max_rps_drop_pct):
        fail("--max-rps-drop-pct must be non-negative")
    if args.max_p50_increase_pct < 0 or not math.isfinite(args.max_p50_increase_pct):
        fail("--max-p50-increase-pct must be non-negative")
    if args.min_latency_samples < 1:
        fail("--min-latency-samples must be >= 1")

    baseline = load(args.baseline)
    current = load(args.current)
    for field in PLAN_FIELDS:
        if field not in baseline or field not in current:
            fail(f"missing workload plan field: {field}")
        if baseline[field] != current[field]:
            fail(f"results were produced by different workloads: {field}")

    allow_eviction = baseline["allow_eviction"]
    if not isinstance(allow_eviction, bool):
        fail("invalid result: allow_eviction must be a boolean")

    for report, path in ((baseline, args.baseline), (current, args.current)):
        errors = non_negative_int(report, "errors", path)
        missing = non_negative_int(report, "missing", path)
        failed_ops = non_negative_int(report, "failed_ops", path)
        planned_ops = non_negative_int(report, "planned_ops", path)
        completed_ops = non_negative_int(report, "completed_ops", path)
        skipped_ops = non_negative_int(report, "skipped_ops", path)
        keys_planned = non_negative_int(report, "keys_planned", path)
        keys_total = non_negative_int(report, "keys_total", path)
        if errors != 0 or missing != 0 or failed_ops != 0:
            fail(f"result has errors, missing reads, or failed ops: {path}")
        if not allow_eviction and skipped_ops != 0:
            fail(f"non-eviction result has skipped_ops={skipped_ops}: {path}")
        if planned_ops == 0 or completed_ops + skipped_ops != planned_ops:
            fail(f"result did not complete the planned trace: {path}")
        if keys_planned == 0 or keys_total != keys_planned:
            fail(f"result did not create the planned keys: {path}")

    base_rps = finite_number(baseline.get("ops_per_second"), "ops_per_second")
    cur_rps = finite_number(current.get("ops_per_second"), "ops_per_second")
    base_ops = non_negative_int(baseline, "completed_ops", args.baseline)
    cur_ops = non_negative_int(current, "completed_ops", args.current)
    if not allow_eviction and base_ops != cur_ops:
        fail("baseline and current completed_ops differ")
    if base_ops == 0 or cur_ops == 0:
        fail("baseline and current completed_ops must be positive")
    if base_rps <= 0 or cur_rps <= 0:
        fail("baseline and current ops_per_second must be positive")

    rps_drop = (base_rps - cur_rps) / base_rps * 100.0
    rps_ok = rps_drop <= args.max_rps_drop_pct

    base_kps = (
        finite_number(baseline["keys_per_second"], "keys_per_second")
        if "keys_per_second" in baseline
        else 0.0
    )
    cur_kps = (
        finite_number(current["keys_per_second"], "keys_per_second")
        if "keys_per_second" in current
        else 0.0
    )
    kps_drop = (
        (base_kps - cur_kps) / base_kps * 100.0 if base_kps > 0 and cur_kps > 0 else 0.0
    )

    p50_ok = True
    gated_p50_ops = []
    advisory_p50_ops = []
    op_metrics = {}

    for op in OP_KINDS:
        count_field = f"{op}_ops"
        base_count = non_negative_int(baseline, count_field, args.baseline)
        cur_count = non_negative_int(current, count_field, args.current)
        if not (allow_eviction and op == "get") and base_count != cur_count:
            fail(f"baseline and current {count_field} differ")
        sample_count = min(base_count, cur_count)
        if sample_count == 0:
            op_metrics[op] = {
                "samples": 0,
                "gated": False,
                "p50_increase_pct": None,
                "p95_increase_pct": None,
                "p99_increase_pct": None,
            }
            continue

        base_p50, base_p95, base_p99 = latency_stats(baseline, op, args.baseline)
        cur_p50, cur_p95, cur_p99 = latency_stats(current, op, args.current)
        if base_p50 <= 0 or cur_p50 <= 0:
            fail(f"baseline/current {op} p50 must be positive")

        p50_inc = pct_increase(base_p50, cur_p50)
        p95_inc = pct_increase(base_p95, cur_p95)
        p99_inc = pct_increase(base_p99, cur_p99)
        is_gated = sample_count >= args.min_latency_samples
        if is_gated:
            gated_p50_ops.append(op)
            if p50_inc > args.max_p50_increase_pct:
                p50_ok = False
        else:
            advisory_p50_ops.append(op)

        op_metrics[op] = {
            "samples": sample_count,
            "gated": is_gated,
            "baseline_p50_us": round(base_p50, 3),
            "current_p50_us": round(cur_p50, 3),
            "p50_increase_pct": round(p50_inc, 3),
            "p95_increase_pct": round(p95_inc, 3) if p95_inc is not None else None,
            "p99_increase_pct": round(p99_inc, 3) if p99_inc is not None else None,
        }

    if not gated_p50_ops:
        fail(
            f"no operation kind reached --min-latency-samples={args.min_latency_samples}"
        )

    passed = rps_ok and p50_ok
    result = {
        "passed": passed,
        "baseline_ops_per_second": round(base_rps, 3),
        "current_ops_per_second": round(cur_rps, 3),
        "rps_drop_pct": round(rps_drop, 3),
        "kps_drop_pct": round(kps_drop, 3),
        "completed_ops_ratio": round(cur_ops / base_ops, 6),
        "put_p50_increase_pct": op_metrics["put"]["p50_increase_pct"],
        "get_p50_increase_pct": op_metrics["get"]["p50_increase_pct"],
        "exist_p50_increase_pct": op_metrics["exist"]["p50_increase_pct"],
        "put_p99_increase_pct": op_metrics["put"]["p99_increase_pct"],
        "get_p99_increase_pct": op_metrics["get"]["p99_increase_pct"],
        "exist_p99_increase_pct": op_metrics["exist"]["p99_increase_pct"],
        "op_metrics": op_metrics,
        "gated_p50_ops": gated_p50_ops,
        "advisory_p50_ops": advisory_p50_ops,
        "min_latency_samples": args.min_latency_samples,
        "max_rps_drop_pct": args.max_rps_drop_pct,
        "max_p50_increase_pct": args.max_p50_increase_pct,
        "baseline_missed_deadlines": baseline.get("missed_deadlines", 0),
        "current_missed_deadlines": current.get("missed_deadlines", 0),
        "baseline_evicted_misses": baseline.get("evicted_misses", 0),
        "current_evicted_misses": current.get("evicted_misses", 0),
        "baseline_skipped_ops": baseline.get("skipped_ops", 0),
        "current_skipped_ops": current.get("skipped_ops", 0),
        "rps_ok": rps_ok,
        "p50_ok": p50_ok,
    }
    print(json.dumps(result, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
