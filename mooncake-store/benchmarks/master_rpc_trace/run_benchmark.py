# Copyright 2026 Alibaba Cloud and its affiliates
# Licensed under the Apache License, Version 2.0.
"""Replay a trace against a dedicated local master and collect Linux metrics.

Only metadata is replayed. The fake segments cannot serve payload transfers.
This launcher owns both child processes and stores all output in a fresh folder.
"""

import argparse
from collections import defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import socket
import subprocess
import time
import urllib.error
import urllib.request

STORE_COUNTERS = {
    "master_successful_evictions_total",
    "master_attempted_evictions_total",
    "master_evicted_key_count",
    "master_evicted_size_bytes",
    "master_put_start_alloc_failures_total",
    "master_put_start_partial_allocations_total",
}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def cpu_set(text):
    result = set()
    for item in text.split(","):
        bounds = [int(value) for value in item.split("-")]
        if len(bounds) == 1:
            result.add(bounds[0])
        elif len(bounds) == 2 and bounds[0] <= bounds[1]:
            result.update(range(bounds[0], bounds[1] + 1))
        else:
            raise ValueError("invalid CPU list")
    if not result or not result <= os.sched_getaffinity(0):
        raise ValueError("CPU list includes unavailable CPUs")
    return result


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def proc_sample(pid):
    # stat comm may contain whitespace and parentheses; fields after the last
    # closing parenthesis begin at Linux stat field 3 (state).
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return {
        "cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE"),
        "threads": int(fields[17]),
    }


def process_summary(rows, start, finish):
    output = {}
    for name in ("master", "replayer"):
        values = [
            (row["monotonic_s"], row[name])
            for row in rows
            if start <= row["monotonic_s"] <= finish and name in row
        ]
        result = {"samples": len(values)}
        if values:
            result["peak_rss_bytes"] = max(value["rss_bytes"] for _, value in values)
            if len(values) >= 2 and values[-1][0] > values[0][0]:
                result["mean_cpu_cores"] = (
                    values[-1][1]["cpu_seconds"] - values[0][1]["cpu_seconds"]
                ) / (values[-1][0] - values[0][0])
        output[name] = result
    return output


def store_metrics(text):
    names = {
        "master_total_capacity_bytes",
        "master_allocated_bytes",
        "master_key_count",
        "master_active_clients",
    } | STORE_COUNTERS
    values = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in names:
            value = float(fields[1])
            if math.isfinite(value):
                values[fields[0]] = value
    return values


def eviction_summary(rows, start, finish):
    """Count sampled eviction activity during workload, excluding teardown."""
    before = [row for row in rows if row["monotonic_s"] <= start]
    during = [row for row in rows if start < row["monotonic_s"] <= finish]
    if not during:
        return {"observed": False, "counter_deltas": {}, "intervals": []}
    baseline = before[-1] if before else during[0]
    previous = baseline
    intervals = []
    for row in during:
        changes = {
            name: row[name] - previous[name]
            for name in STORE_COUNTERS
            if name in row and name in previous
        }
        if changes.get("master_attempted_evictions_total", 0) > 0:
            intervals.append(
                {"end_s": row["monotonic_s"] - start, "counter_deltas": changes}
            )
        previous = row
    deltas = {
        name: during[-1][name] - baseline[name]
        for name in STORE_COUNTERS
        if name in during[-1] and name in baseline
    }
    return {
        "observed": deltas.get("master_successful_evictions_total", 0) > 0
        and deltas.get("master_evicted_size_bytes", 0) > 0,
        "sample_start_s": baseline["monotonic_s"] - start,
        "sample_end_s": during[-1]["monotonic_s"] - start,
        "counter_deltas": deltas,
        "intervals": intervals,
    }


def traffic_summary(samples):
    buckets = defaultdict(
        lambda: {
            "offered_calls": 0,
            "sent_calls": 0,
            "completed_calls": 0,
            "issued_keys": 0,
        }
    )
    edges = []
    end_us = 0
    arrival_end_us = 0
    for row in samples:
        if row["phase"] != "workload":
            continue
        origin = row["phase_origin_us"]
        due, start, finish = (
            row[key] - origin for key in ("scheduled_us", "start_us", "finish_us")
        )
        end_us = max(end_us, finish)
        arrival_end_us = max(arrival_end_us, due)
        buckets[due // 1_000_000]["offered_calls"] += 1
        if row["rpc_sent"]:
            buckets[start // 1_000_000]["sent_calls"] += 1
            buckets[finish // 1_000_000]["completed_calls"] += 1
            buckets[start // 1_000_000]["issued_keys"] += (
                row["planned_keys"] - row["key_status"]["skipped"]
            )
            if finish > start:
                edges.extend(((start, 1), (finish, -1)))
    inflight = peak = 0
    # Half-open intervals at microsecond resolution; finishes precede starts.
    # Calls rounded to zero duration do not contribute an overlap interval.
    for _, delta in sorted(edges):
        inflight += delta
        peak = max(peak, inflight)
    seconds = end_us / 1_000_000
    totals = {
        key: sum(bucket[key] for bucket in buckets.values())
        for key in ("offered_calls", "sent_calls", "completed_calls", "issued_keys")
    }
    return {
        "duration_s": seconds,
        "arrival_span_s": arrival_end_us / 1_000_000,
        "offered_calls_per_second": totals["offered_calls"] * 1_000_000 / arrival_end_us
        if arrival_end_us
        else None,
        "peak_client_calls_inflight": peak,
        "totals": totals,
        "mean_per_second": {
            key: value / seconds if seconds else None for key, value in totals.items()
        },
        "buckets": [
            {"second": second, **bucket} for second, bucket in sorted(buckets.items())
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", required=True, type=Path)
    parser.add_argument("--master", required=True, type=Path)
    parser.add_argument("--replayer", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--master-cpus", required=True)
    parser.add_argument("--replay-cpus", required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--rpc-threads", type=int, default=4)
    parser.add_argument("--speed", type=float, default=1)
    parser.add_argument("--sample-interval", type=float, default=0.2)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--eviction-high-watermark-ratio", type=float)
    parser.add_argument("--eviction-ratio", type=float)
    parser.add_argument(
        "--require-eviction",
        action="store_true",
        help="Fail the run unless successful eviction is sampled during workload",
    )
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("process sampling and CPU affinity require Linux")
    if cpu_set(args.master_cpus) & cpu_set(args.replay_cpus):
        parser.error("master and replay CPU sets must be disjoint")
    if (
        args.workers < 1
        or args.rpc_threads < 1
        or any(
            not math.isfinite(value) or value <= 0
            for value in (args.speed, args.sample_interval, args.timeout)
        )
    ):
        parser.error("counts, speed, intervals and timeout must be positive")
    for value in (args.eviction_high_watermark_ratio, args.eviction_ratio):
        if value is not None and (not math.isfinite(value) or not 0 <= value <= 1):
            parser.error("eviction ratios must be between zero and one")
    trace, master_bin, replay_bin = (
        path.resolve(strict=True) for path in (args.trace, args.master, args.replayer)
    )
    directory = args.output_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, MC_STORE_RPC_CLIENT_IO_THREADS="2")
    env.pop("MOONCAKE_CONFIG_PATH", None)
    subprocess.run(
        [str(replay_bin), f"--trace={trace}", "--validate_only"],
        check=True,
        env=env,
        timeout=args.timeout,
    )
    rpc_port, metrics_port = port(), port()
    while metrics_port == rpc_port:
        metrics_port = port()
    master_command = [
        "taskset",
        "-c",
        args.master_cpus,
        str(master_bin),
        f"--rpc_port={rpc_port}",
        "--rpc_address=127.0.0.1",
        f"--rpc_thread_num={args.rpc_threads}",
        f"--metrics_port={metrics_port}",
        "--metrics_host=127.0.0.1",
        "--enable_metric_reporting=true",
        "--default_kv_lease_ttl=0ms",
        "--logtostderr=1",
    ]
    for flag in ("eviction_high_watermark_ratio", "eviction_ratio"):
        value = getattr(args, flag)
        if value is not None:
            master_command.append(f"--{flag}={value}")
    replay_command = [
        "taskset",
        "-c",
        args.replay_cpus,
        str(replay_bin),
        f"--trace={trace}",
        f"--master_server=127.0.0.1:{rpc_port}",
        f"--workers={args.workers}",
        f"--speed={args.speed}",
        f"--output={directory / 'replay.json'}",
        f"--samples={directory / 'samples.jsonl'}",
        "--logtostderr=1",
    ]
    manifest = {
        "trace": str(trace),
        "trace_sha256": hashlib.sha256(trace.read_bytes()).hexdigest(),
        "master_command": master_command,
        "replay_command": replay_command,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "parameters": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "started_unix_s": time.time(),
        "payload": "metadata_only",
        "binary_sha256": {
            name: hashlib.sha256(path.read_bytes()).hexdigest()
            for name, path in (("master", master_bin), ("replayer", replay_bin))
        },
    }
    write_json(directory / "manifest.json", manifest)
    processes = []
    rows = []
    metric_rows = []
    result = {"success": False}
    url = f"http://127.0.0.1:{metrics_port}/metrics"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with (directory / "master.log").open("w") as master_log, (
            directory / "replayer.log"
        ).open("w") as replay_log, (directory / "process.jsonl").open(
            "w"
        ) as proc_file, (directory / "metrics.jsonl").open("w") as metrics_file:
            master = subprocess.Popen(
                master_command,
                stdout=master_log,
                stderr=master_log,
                cwd=directory,
                env=env,
            )
            processes.append(master)
            deadline = time.monotonic() + 30
            while True:
                if master.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("master failed to start; see master.log")
                try:
                    with opener.open(url, timeout=1) as response:
                        initial_metrics = response.read().decode()
                    with socket.create_connection(("127.0.0.1", rpc_port), timeout=1):
                        pass
                    break
                except (OSError, urllib.error.URLError):
                    time.sleep(0.05)
            (directory / "metrics-before.prom").write_text(initial_metrics)
            replayer = subprocess.Popen(
                replay_command,
                stdout=replay_log,
                stderr=replay_log,
                cwd=directory,
                env=env,
            )
            processes.append(replayer)
            write_json(
                directory / "pids.json",
                {"master": master.pid, "replayer": replayer.pid},
            )
            deadline = time.monotonic() + args.timeout
            while replayer.poll() is None:
                tick = time.monotonic()
                if master.poll() is not None:
                    raise RuntimeError("master exited during replay")
                if tick > deadline:
                    raise TimeoutError("replay timeout")
                row = {"monotonic_s": tick}
                for name, process in (("master", master), ("replayer", replayer)):
                    try:
                        row[name] = proc_sample(process.pid)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                rows.append(row)
                proc_file.write(json.dumps(row) + "\n")
                metric = {"monotonic_s": time.monotonic()}
                try:
                    with opener.open(
                        url, timeout=min(args.sample_interval, 1)
                    ) as response:
                        metric["prometheus"] = response.read().decode()
                    metric["values"] = store_metrics(metric["prometheus"])
                    metric_rows.append(
                        {"monotonic_s": metric["monotonic_s"], **metric["values"]}
                    )
                except (OSError, urllib.error.URLError) as error:
                    metric["error"] = str(error)
                metrics_file.write(json.dumps(metric) + "\n")
                time.sleep(max(0, args.sample_interval - (time.monotonic() - tick)))
            with opener.open(url, timeout=3) as response:
                after = response.read().decode()
                (directory / "metrics-after.prom").write_text(after)
                result["store_after_replay"] = store_metrics(after)
            result["replayer_exit_code"] = replayer.returncode
            if not (directory / "replay.json").exists():
                raise RuntimeError("replayer produced no summary; see replayer.log")
            replay = json.loads((directory / "replay.json").read_text())
            with (directory / "samples.jsonl").open() as stream:
                samples = [json.loads(line) for line in stream]
            workload = [row for row in samples if row["phase"] == "workload"]
            if not workload:
                raise RuntimeError("trace contains no workload events")
            origin = replay["replay_origin_monotonic_us"] / 1_000_000
            start = origin + workload[0]["phase_origin_us"] / 1_000_000
            finish = origin + max(row["finish_us"] for row in workload) / 1_000_000
            selected_metrics = [
                row for row in metric_rows if start <= row["monotonic_s"] <= finish
            ]
            result["store_workload_sampled_peak"] = {
                name: max(row[name] for row in selected_metrics if name in row)
                for name in (
                    "master_total_capacity_bytes",
                    "master_allocated_bytes",
                    "master_key_count",
                    "master_active_clients",
                )
                if any(name in row for row in selected_metrics)
            }
            evictions = eviction_summary(metric_rows, start, finish)
            result.update(
                {
                    "success": replayer.returncode == 0
                    and replay["healthy_heartbeats"]
                    and not replay["has_errors"]
                    and (not args.require_eviction or evictions["observed"]),
                    "workload_evictions": evictions,
                    "eviction_requirement_met": not args.require_eviction
                    or evictions["observed"],
                    "workload_process": process_summary(rows, start, finish),
                    "workload_monotonic_window_s": [start, finish],
                    "traffic": traffic_summary(samples),
                    "phases": replay["phases"],
                }
            )
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        for process in reversed(processes):
            stop(process)
        write_json(directory / "result.json", result)
    summary = {
        key: value for key, value in result.items() if key not in ("phases", "traffic")
    }
    if "workload_evictions" in summary:
        summary["workload_evictions"] = {
            key: value
            for key, value in summary["workload_evictions"].items()
            if key != "intervals"
        }
    print(json.dumps(summary, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
