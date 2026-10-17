#!/usr/bin/env python3
"""Collect paired CPU benchmarks and publish a fail-closed performance gate."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import statistics
import subprocess
import sys
import time


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, minimum=0):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value > minimum
    except OverflowError:
        return False


def load_policy(path):
    policy = json.loads(Path(path).read_text())
    require(isinstance(policy, dict), "policy must be an object")
    require(policy["schema_version"] == 1, "unsupported policy version")
    for field in ("arguments", "cases", "metrics"):
        require(
            isinstance(policy[field], dict) and policy[field],
            f"{field} must be a nonempty object",
        )
    require(
        type(policy["repetitions"]) is int and policy["repetitions"] >= 5,
        "at least five repetitions are required",
    )
    require(
        type(policy["cpu_count"]) is int and policy["cpu_count"] > 0,
        "invalid CPU count",
    )
    require(number(policy["timeout_seconds"]), "invalid timeout")
    rdma = policy.get("suite", "cpu") == "rdma"
    require(policy.get("suite", "cpu") in ("cpu", "rdma"), "unknown suite")
    if not rdma:
        require(
            type(policy["arguments"]["iterations"]) is int
            and policy["arguments"]["iterations"] > 0,
            "invalid operation count",
        )
    else:
        for key in ("duration", "total_buffer_size"):
            require(
                type(policy["arguments"][key]) is int and policy["arguments"][key] > 0,
                f"invalid {key}",
            )
        require(
            policy["arguments"]["duration"] + 1 < policy["timeout_seconds"] <= 60,
            "RDMA timeout must include warmup/setup and fit the 60-second sample watchdog",
        )
    for name, case in policy["cases"].items():
        require(re.fullmatch(r"[a-z0-9_-]+", name), "invalid case name")
        require(isinstance(case, dict), f"{name}: case must be an object")
        require(
            isinstance(case.get("max_regression_percent", {}), dict),
            f"{name}: threshold overrides must be an object",
        )
        require(
            type(case["threads"]) is int and 0 < case["threads"] <= policy["cpu_count"],
            "invalid thread count",
        )
        if rdma:
            require(
                isinstance(case.get("metrics"), list)
                and case["metrics"]
                and all(isinstance(key, str) for key in case["metrics"])
                and len(set(case["metrics"])) == len(case["metrics"]),
                f"{name}: metrics must be a nonempty list of distinct names",
            )
            require(case["op_type"] in ("read", "write"), "invalid RDMA operation")
            for key in ("block_size", "batch_size"):
                require(type(case[key]) is int and case[key] > 0, f"invalid {key}")
            require(
                case["block_size"] * case["batch_size"] * case["threads"]
                <= policy["arguments"]["total_buffer_size"],
                "RDMA buffer too small",
            )
        else:
            require("metrics" not in case, "CPU collector uses all policy metrics")
            require(
                case["layer"] in ("offset_allocator", "put_allocation"), "invalid layer"
            )
            require(
                case["scenario"] in ("success", "capacity", "fragmentation", "mixed"),
                "invalid scenario",
            )
        require(
            case.get("metrics", list(policy["metrics"]))
            and set(case.get("metrics", policy["metrics"])) <= set(policy["metrics"]),
            "invalid case metrics",
        )
        for key, limit in case.get("max_regression_percent", {}).items():
            require(
                key in policy["metrics"] and number(limit, -1) and limit >= 0,
                "invalid case threshold override",
            )
    for name, metric in policy["metrics"].items():
        require(isinstance(metric, dict), f"{name}: metric must be an object")
        allowed = ("bandwidth_gbs", "p99_us") if rdma else ("qps", "p50_ns", "p99_ns")
        require(name in allowed, "unsupported benchmark metric")
        require(
            metric["direction"] in ("higher", "lower")
            and isinstance(metric["unit"], str)
            and metric["unit"],
            "invalid metric direction or unit",
        )
        require(
            number(metric["max_regression_percent"], -1)
            and metric["max_regression_percent"] >= 0
            and number(metric["max_mad_percent"]),
            "invalid threshold",
        )
    return policy


def policy_digest(policy):
    return hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def annotation(message):
    escaped = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error title=Performance Gate::{escaped}")


def stop_process(process):
    """Stop only the process group started for this invocation, including descendants."""
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # The group already exited; its direct child still needs to be reaped.
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # It exited between the wait and the signal.
        process.wait(timeout=5)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # No descendants remain after the parent was reaped.


def environment(cpu_count):
    cpus = sorted(os.sched_getaffinity(0))[:cpu_count]
    require(len(cpus) == cpu_count, "not enough CPUs for the configured suite")
    os.sched_setaffinity(0, cpus)
    cpu_info = Path("/proc/cpuinfo").read_text()
    model = next(
        (
            line.split(":", 1)[1].strip()
            for line in cpu_info.splitlines()
            if line.startswith(("model name", "Hardware"))
        ),
        platform.machine(),
    )
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "cpu_model": model,
        "cpu_affinity": cpus,
        "compiler": subprocess.check_output(
            ["c++", "--version"], text=True
        ).splitlines()[0],
        "harness_sha": os.environ.get("PERF_HARNESS_SHA", "local"),
    }


def parse_sample(output, case, policy):
    rows = list(csv.DictReader(io.StringIO(output)))
    require(len(rows) == 1, "expected exactly one benchmark result")
    row = rows[0]
    require(
        row["layer"] == case["layer"] and row["scenario"] == case["result_scenario"],
        "benchmark returned a different case",
    )
    require(
        int(row["threads"]) == case["threads"]
        and int(row["iterations_per_thread"]) == policy["arguments"]["iterations"],
        "benchmark workload differs from the policy",
    )
    require(
        int(row["operations"]) == case["threads"] * policy["arguments"]["iterations"],
        "benchmark did not complete all operations",
    )
    require(
        int(row["throughput_unexpected_results"]) == 0
        and int(row["latency_unexpected_results"]) == 0,
        "benchmark correctness failure",
    )
    require(number(float(row["throughput_seconds"])), "invalid measurement duration")
    sample = {name: float(row[name]) for name in policy["metrics"]}
    require(
        all(number(value) for value in sample.values()), "invalid benchmark measurement"
    )
    return sample


def collect(args):
    policy = load_policy(args.policy)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    # Build metadata is prepared before collection; prior measurements are not reusable.
    require(
        not any(path.name != "build-config.json" for path in out.glob("*.json")),
        "output directory already contains results",
    )
    env = environment(policy["cpu_count"])
    budget = getattr(args, "collection_timeout_seconds", 900)
    require(number(budget), "invalid collection time budget")
    deadline = time.monotonic() + budget
    results = {}
    for side in ("baseline", "candidate"):
        binary = Path(getattr(args, side)).resolve()
        results[side] = {
            "schema_version": 1,
            "revision": getattr(args, f"{side}_sha"),
            "environment": env,
            "policy_sha256": policy_digest(policy),
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "cases": {},
            "errors": [],
        }
    for name, case in policy["cases"].items():
        flags = {
            **policy["arguments"],
            **{k: case[k] for k in ("layer", "scenario", "threads")},
        }
        for repeat in range(policy["repetitions"]):
            # Reverse each pair to reduce bias from temperature / temporal drift.
            order = (
                ("baseline", "candidate")
                if repeat % 2 == 0
                else ("candidate", "baseline")
            )
            for side in order:
                log = out / f"{name}-{repeat}-{side}"
                command = [str(Path(getattr(args, side)).resolve())]
                command += [f"--{key}={value}" for key, value in flags.items()]
                print(f"{name} round {repeat + 1}: {side}", flush=True)
                try:
                    remaining = deadline - time.monotonic()
                    require(remaining > 0, "collection time budget exhausted")
                    with log.with_suffix(".csv").open("w") as stdout, log.with_suffix(
                        ".log"
                    ).open("w") as stderr:
                        process = subprocess.Popen(
                            command,
                            stdout=stdout,
                            stderr=stderr,
                            start_new_session=True,
                        )
                        try:
                            code = process.wait(
                                timeout=min(policy["timeout_seconds"], remaining)
                            )
                            if code:
                                raise subprocess.CalledProcessError(code, command)
                        finally:
                            stop_process(process)
                    sample = parse_sample(
                        log.with_suffix(".csv").read_text(), case, policy
                    )
                    values = results[side]["cases"].setdefault(
                        name,
                        {
                            key: {"unit": metric["unit"], "samples": []}
                            for key, metric in policy["metrics"].items()
                        },
                    )
                    for key, value in sample.items():
                        values[key]["samples"].append(value)
                except (
                    OSError,
                    ValueError,
                    KeyError,
                    TypeError,
                    RuntimeError,
                    subprocess.SubprocessError,
                ) as exc:
                    results[side]["errors"].append(f"{name} round {repeat + 1}: {exc}")
                    # A failed invocation invalidates the run. Save diagnostics below,
                    # then leave time for comparison/upload instead of retrying it.
                    return 2
                finally:
                    for result_side, result in results.items():
                        write_json(out / f"{result_side}.json", result)
    return 0


def compare(policy, baseline, candidate, baseline_sha, candidate_sha):
    for result, sha in ((baseline, baseline_sha), (candidate, candidate_sha)):
        require(re.fullmatch(r"[0-9a-f]{40}", sha or ""), "expected a full commit SHA")
        require(
            result["schema_version"] == 1 and result["revision"] == sha,
            "result version or commit SHA mismatch",
        )
        require(
            result["policy_sha256"] == policy_digest(policy), "result policy mismatch"
        )
        require(
            not result["errors"], "benchmark errors: " + "; ".join(result["errors"])
        )
        require(
            set(result["cases"]) == set(policy["cases"]),
            "missing or unexpected benchmark cases",
        )
    require(
        baseline["environment"] and baseline["environment"] == candidate["environment"],
        "baseline and candidate environments differ",
    )
    rows = []
    for name in policy["cases"]:
        require(
            set(baseline["cases"][name])
            == set(candidate["cases"][name])
            == set(policy["cases"][name].get("metrics", policy["metrics"])),
            f"{name}: missing or unexpected metrics",
        )
        for key in policy["cases"][name].get("metrics", policy["metrics"]):
            metric = policy["metrics"][key]
            medians, noise = [], []
            for result in (baseline, candidate):
                data = result["cases"][name][key]
                samples = data["samples"]
                require(data["unit"] == metric["unit"], f"{name}/{key}: unit mismatch")
                require(
                    len(samples) == policy["repetitions"]
                    and all(number(x) for x in samples),
                    f"{name}/{key}: incomplete or invalid samples",
                )
                median = statistics.median(samples)
                medians.append(median)
                noise.append(
                    100 * statistics.median(abs(x - median) for x in samples) / median
                )
            base, current = medians
            regression = 100 * (current - base) / base
            if metric["direction"] == "higher":
                regression = -regression
            require(
                math.isfinite(regression) and all(math.isfinite(x) for x in noise),
                f"{name}/{key}: measurement ratio overflow",
            )
            limit = (
                policy["cases"][name]
                .get("max_regression_percent", {})
                .get(key, metric["max_regression_percent"])
            )
            status = "PASS"
            if regression > limit and not math.isclose(regression, limit, abs_tol=1e-9):
                status = "FAIL"
            if max(noise) > metric["max_mad_percent"]:
                status = "ERROR"
            rows.append(
                {
                    "case": name,
                    "metric": key,
                    "unit": metric["unit"],
                    "baseline": base,
                    "candidate": current,
                    "regression_percent": regression,
                    "limit_percent": limit,
                    "mad_percent": noise,
                    "status": status,
                }
            )
    return rows


def report(args):
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    rows, errors, revisions = [], [], []
    try:
        policy = load_policy(args.policy)
        base = json.loads(Path(args.baseline).read_text())
        candidate = json.loads(Path(args.candidate).read_text())
        rows = compare(policy, base, candidate, args.baseline_sha, args.candidate_sha)
        revisions = [base["revision"], candidate["revision"]]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(str(exc))
    code = (
        2
        if errors or any(row["status"] == "ERROR" for row in rows)
        else int(any(row["status"] == "FAIL" for row in rows))
    )
    status = ("PASSED", "FAILED", "ERROR")[code]
    lines = [f"## Performance Gate: {status}", ""]
    if revisions:
        lines += [f"Baseline: `{revisions[0]}` · Candidate: `{revisions[1]}`", ""]
    lines += [
        "Positive change means a regression. Values are medians; MAD is median absolute deviation / median.",
        "",
        "| Case | Metric | Baseline | Candidate | Regression | Limit | MAD base / candidate | Result |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['case']} | {row['metric']} ({row['unit']}) | {row['baseline']:.3f} | "
            f"{row['candidate']:.3f} | {row['regression_percent']:+.2f}% | {row['limit_percent']:g}% | "
            f"{row['mad_percent'][0]:.2f}% / {row['mad_percent'][1]:.2f}% | {row['status']} |"
        )
        if row["status"] != "PASS":
            reason = (
                "unstable measurements"
                if row["status"] == "ERROR"
                else f"regressed {row['regression_percent']:.2f}% (limit {row['limit_percent']:g}%)"
            )
            annotation(f"{row['case']} {row['metric']}: {reason}")
    for error in errors:
        lines += ["", "ERROR: " + error.replace("\n", " ").replace("|", "\\|")]
        annotation(error)
    lines += [
        "",
        f"Normalized samples, environment and comparison.json are in the {getattr(args, 'artifact_name', 'performance-results')} artifact. "
        "Consult the job logs for raw benchmark diagnostics.",
    ]
    markdown = "\n".join(lines) + "\n"
    (out / "summary.md").write_text(markdown)
    write_json(
        out / "comparison.json", {"status": status, "rows": rows, "errors": errors}
    )
    if args.summary:
        with Path(args.summary).open("a") as summary:
            summary.write(markdown)
    print(markdown)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("collect", "compare"))
    parser.add_argument(
        "--policy", default=str(Path(__file__).with_name("policy.json"))
    )
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--baseline-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--collection-timeout-seconds", type=float, default=900)
    parser.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))
    args = parser.parse_args()
    return collect(args) if args.mode == "collect" else report(args)


if __name__ == "__main__":

    def interrupted(signum, frame):
        raise RuntimeError(f"interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    sys.exit(main())
