import argparse
import copy
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a base and a current Master against the same memory "
        "segment workload, aggregate medians, and call master_memory_gate.py."
    )
    parser.add_argument("--base-master", required=True)
    parser.add_argument("--current-master", required=True)
    parser.add_argument("--bench", required=True)
    parser.add_argument("--gate", default=str(Path(__file__).with_name("master_memory_gate.py")))
    parser.add_argument("--workdir", default="./master-memory-run")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--base-port", type=int, default=50101)
    parser.add_argument("--current-port", type=int, default=50102)
    parser.add_argument("--rpc-threads", type=int, default=16)
    parser.add_argument("--keys", type=int, default=100000000)
    parser.add_argument("--object-bytes", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--segments", type=int, default=2)
    parser.add_argument("--segment-bytes", type=int, default=64 * 1024 * 1024 * 1024)
    parser.add_argument("--fill-threads", type=int, default=16)
    parser.add_argument("--read-threads", type=int, default=16)
    parser.add_argument("--read-duration", type=int, default=10)
    parser.add_argument("--read-rate", type=int, default=1000)
    parser.add_argument("--amplify", type=int, default=1)
    parser.add_argument("--model", default="qwen3-8b")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--stop-timeout", type=float, default=5.0)
    parser.add_argument("--max-rps-drop-pct", type=float, default=5.0)
    parser.add_argument("--max-p99-increase-pct", type=float, default=10.0)
    parser.add_argument("--master-arg", action="append", default=[])
    return parser.parse_args()


def require_executable(path, label):
    if not os.path.isfile(path) or not os.access(path, os.X_OK):
        raise SystemExit(f"{label} is not an executable file: {path}")


def wait_for_port(port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.2)
    raise SystemExit(f"Master did not listen on port {port} within {timeout}s")


def stop_process(process, timeout):
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
        return
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def start_master(binary, port, log_path, args):
    command = [
        binary,
        f"--port={port}",
        f"--rpc_thread_num={args.rpc_threads}",
        "--memory_allocator=offset",
        "--eviction_high_watermark_ratio=1.0",
        "--allow_evict_soft_pinned_objects=false",
        *args.master_arg,
    ]
    log = open(log_path, "wb")
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return process, log


def run_benchmark(binary, result_path, log_path, args, port):
    command = [
        binary,
        f"--master=127.0.0.1:{port}",
        f"--keys={args.keys}",
        f"--object_bytes={args.object_bytes}",
        f"--batch_size={args.batch_size}",
        f"--segments={args.segments}",
        f"--segment_bytes={args.segment_bytes}",
        f"--fill_threads={args.fill_threads}",
        f"--read_threads={args.read_threads}",
        f"--read_duration={args.read_duration}",
        f"--read_rate={args.read_rate}",
        f"--amplify={args.amplify}",
        f"--model={args.model}",
        f"--seed={args.seed}",
        f"--result_file={result_path}",
    ]
    with open(log_path, "wb") as log:
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    if completed.returncode != 0:
        raise SystemExit(f"benchmark failed with exit code {completed.returncode}; see {log_path}")
    if not os.path.isfile(result_path):
        raise SystemExit(f"benchmark did not write {result_path}")
    with open(result_path, encoding="utf-8") as source:
        return json.load(source)


def median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def aggregate(runs, side):
    if not runs:
        raise SystemExit(f"no runs for {side}")
    expected = runs[0]
    for report in runs[1:]:
        for field in ("keys", "object_bytes", "batch_size", "fill_threads", "seed", "segments", "segment_bytes"):
            if report[field] != expected[field]:
                raise SystemExit(f"{side} runs disagree on {field}")
    result = copy.deepcopy(expected)
    result["runs"] = len(runs)
    result["aggregate"] = "median"
    for field in ("seconds", "keys_per_second"):
        result["fill"][field] = median([report["fill"][field] for report in runs])
    for field in ("seconds", "batches_per_second", "keys_per_second"):
        result["read"][field] = median([report["read"][field] for report in runs])
    for field in ("p50", "p90", "p99", "p999"):
        result["read"]["latency_us"][field] = median(
            [report["read"]["latency_us"][field] for report in runs]
        )
    result["fill"]["errors"] = max(report["fill"]["errors"] for report in runs)
    result["read"]["errors"] = max(report["read"]["errors"] for report in runs)
    result["read"]["missing"] = max(report["read"]["missing"] for report in runs)
    return result


def run_side(side, master, port, args, workdir):
    side_dir = workdir / side
    side_dir.mkdir(parents=True, exist_ok=True)
    reports = []
    for index in range(1, args.runs + 1):
        log_path = side_dir / f"master-{index}.log"
        result_path = side_dir / f"run-{index}.json"
        bench_log = side_dir / f"bench-{index}.log"
        process, log = start_master(master, port, log_path, args)
        try:
            wait_for_port(port, args.startup_timeout)
            report = run_benchmark(args.bench, str(result_path), str(bench_log), args, port)
            reports.append(report)
        finally:
            stop_process(process, args.stop_timeout)
            log.close()
        print(f"{side} run {index}/{args.runs}: "
              f"fill {report['fill']['keys_per_second']:.1f} keys/s, "
              f"read {report['read']['batches_per_second']:.1f} batches/s, "
              f"p99 {report['read']['latency_us']['p99']:.1f} us")
    aggregate_report = aggregate(reports, side)
    aggregate_path = side_dir / "median.json"
    with open(aggregate_path, "w", encoding="utf-8") as output:
        json.dump(aggregate_report, output, indent=2)
        output.write("\n")
    return aggregate_path


def main():
    args = parse_args()
    if args.runs < 1:
        raise SystemExit("--runs must be at least 1")
    require_executable(args.base_master, "base master")
    require_executable(args.current_master, "current master")
    require_executable(args.bench, "benchmark")
    if not os.path.isfile(args.gate):
        raise SystemExit(f"gate script not found: {args.gate}")

    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    base = run_side("base", args.base_master, args.base_port, args, workdir)
    current = run_side("current", args.current_master, args.current_port, args, workdir)
    gate_log = workdir / "gate.log"
    command = [
        sys.executable,
        args.gate,
        "--baseline", str(base),
        "--current", str(current),
        "--max-rps-drop-pct", str(args.max_rps_drop_pct),
        "--max-p99-increase-pct", str(args.max_p99_increase_pct),
    ]
    completed = subprocess.run(command, text=True, capture_output=True)
    gate_log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
