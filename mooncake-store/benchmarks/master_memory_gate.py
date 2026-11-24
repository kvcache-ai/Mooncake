import argparse
import json
import sys


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(2)


def load(path):
    try:
        with open(path, "r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def number(value, field):
    if not isinstance(value, (int, float)):
        fail(f"invalid result: {field} is not a number")
    return float(value)


def plan(report):
    read = report.get("read", {})
    return (
        report.get("keys"),
        report.get("object_bytes"),
        report.get("batch_size"),
        report.get("fill_threads"),
        report.get("seed"),
        report.get("segments"),
        report.get("segment_bytes"),
        report.get("model"),
        read.get("threads"),
        read.get("rate_target"),
        read.get("duration_s"),
        read.get("amplify"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--current", required=True)
    parser.add_argument("--max-rps-drop-pct", type=float, default=5.0)
    parser.add_argument("--max-p99-increase-pct", type=float, default=10.0)
    args = parser.parse_args()

    baseline = load(args.baseline)
    current = load(args.current)
    for report, path in ((baseline, args.baseline), (current, args.current)):
        if report.get("benchmark") != "master_memory_segment":
            fail(f"not a master_memory_segment result: {path}")

    if plan(baseline) != plan(current):
        fail("results were produced by different workloads")

    base_read = baseline["read"]
    cur_read = current["read"]
    base_fill = baseline["fill"]
    cur_fill = current["fill"]
    if base_read.get("errors", 0) or cur_read.get("errors", 0):
        errors_ok = False
    elif base_read.get("missing", 0) or cur_read.get("missing", 0):
        errors_ok = False
    elif base_fill.get("errors", 0) or cur_fill.get("errors", 0):
        errors_ok = False
    else:
        errors_ok = True

    base_rps = number(base_read.get("batches_per_second"), "read.batches_per_second")
    cur_rps = number(cur_read.get("batches_per_second"), "read.batches_per_second")
    base_p99 = number(base_read.get("latency_us", {}).get("p99"), "read.latency_us.p99")
    cur_p99 = number(cur_read.get("latency_us", {}).get("p99"), "read.latency_us.p99")
    if base_rps <= 0 or base_p99 <= 0:
        fail("baseline throughput and p99 must be positive")

    rps_drop = (base_rps - cur_rps) / base_rps * 100.0
    p99_increase = (cur_p99 - base_p99) / base_p99 * 100.0
    rps_ok = rps_drop <= args.max_rps_drop_pct
    p99_ok = p99_increase <= args.max_p99_increase_pct
    passed = rps_ok and p99_ok and errors_ok

    result = {
        "passed": passed,
        "rps_drop_pct": round(rps_drop, 3),
        "p99_increase_pct": round(p99_increase, 3),
        "max_rps_drop_pct": args.max_rps_drop_pct,
        "max_p99_increase_pct": args.max_p99_increase_pct,
        "rps_ok": rps_ok,
        "p99_ok": p99_ok,
        "errors_ok": errors_ok,
    }
    print(json.dumps(result, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
