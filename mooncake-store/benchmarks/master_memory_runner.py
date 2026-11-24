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
    parser.add_argument(
        "--base-bench",
        default="",
        help="Benchmark binary for the base side; defaults to --bench",
    )
    parser.add_argument(
        "--current-bench",
        default="",
        help="Benchmark binary for the current side; defaults to --bench",
    )
    parser.add_argument(
        "--gate", default=str(Path(__file__).with_name("master_memory_gate.py"))
    )
    parser.add_argument("--workdir", default="./master-memory-run")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--base-port", type=int, default=50101)
    parser.add_argument("--current-port", type=int, default=50102)
    parser.add_argument("--rpc-threads", type=int, default=16)
    parser.add_argument("--keys", type=int, default=100000000)
    parser.add_argument("--object-bytes", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--segments", type=int, default=2)
    parser.add_argument(
        "--segment-bytes", type=int, default=64 * 1024 * 1024 * 1024
    )
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
    parser.add_argument("--max-p50-increase-pct", type=float, default=10.0)
    parser.add_argument("--master-arg", action="append", default=[])
    return parser.parse_args()


def require_executable(path, label):
    if not os.path.isfile(path) or not os.access(path, os.X_OK):
        raise SystemExit(f"{label} is not an executable file: {path}")


def log_tail(path, lines=20):
    try:
        with open(path, encoding="utf-8", errors="replace") as source:
            return "".join(source.readlines()[-lines:])
    except OSError:
        return ""


def ensure_port_free(port, side):
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        probe.bind(("127.0.0.1", port))
    except OSError as error:
        raise SystemExit(
            f"{side} port {port} is already in use: {error}"
        ) from error
    finally:
        probe.close()


def listener_inodes(port):
    inodes = set()
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(name, encoding="ascii") as source:
                next(source, None)
                for line in source:
                    fields = line.split()
                    if len(fields) < 10 or fields[3] != "0A":
                        continue
                    local_port = int(fields[1].rsplit(":", 1)[1], 16)
                    if local_port == port:
                        inodes.add(fields[9])
        except OSError:
            continue
    return inodes


def process_owns_port(process, port):
    inodes = listener_inodes(port)
    if not inodes:
        return False
    fd_dir = f"/proc/{process.pid}/fd"
    try:
        for entry in os.listdir(fd_dir):
            try:
                target = os.readlink(os.path.join(fd_dir, entry))
            except OSError:
                continue
            if target.startswith("socket:[") and target[8:-1] in inodes:
                return True
    except OSError:
        return False
    return False


def wait_for_port(port, timeout, process, log_path):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SystemExit(
                f"Master exited before listening on port {port}:\n"
                f"{log_tail(log_path)}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                break
        except OSError:
            time.sleep(0.2)
    else:
        raise SystemExit(f"Master did not listen on port {port} within {timeout}s")
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SystemExit(
                f"Master exited while starting on port {port}:\n"
                f"{log_tail(log_path)}"
            )
        if process_owns_port(process, port):
            return
        time.sleep(0.05)
    raise SystemExit(
        f"port {port} is not owned by the spawned Master; "
        f"another process is listening:\n{log_tail(log_path)}"
    )


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


def process_cpu_ticks(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        fields = stat[stat.rfind(")") + 2 :].split()
        return int(fields[11]) + int(fields[12])
    except (OSError, IndexError, ValueError):
        return None


def process_rss_kb(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text(
            encoding="ascii"
        ).splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, IndexError, ValueError):
        pass
    return None


def process_usage(pid, cpu_start, wall_start):
    cpu_end = process_cpu_ticks(pid)
    rss_kb = process_rss_kb(pid)
    if cpu_end is None or cpu_start is None:
        return {"cpu_percent": None, "rss_kb": rss_kb}
    elapsed = max(time.monotonic() - wall_start, 1e-9)
    ticks = os.sysconf("SC_CLK_TCK")
    return {
        "cpu_percent": round((cpu_end - cpu_start) / ticks / elapsed * 100.0, 3),
        "rss_kb": rss_kb,
    }


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
    process = subprocess.Popen(
        command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
    )
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
        raise SystemExit(
            f"benchmark failed with exit code {completed.returncode}; see {log_path}"
        )
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
        for field in (
            "keys",
            "object_bytes",
            "batch_size",
            "fill_threads",
            "seed",
            "segments",
            "segment_bytes",
        ):
            if report[field] != expected[field]:
                raise SystemExit(f"{side} runs disagree on {field}")
    for report in runs:
        if report["fill"]["completed"] != expected["keys"]:
            raise SystemExit(f"{side} fill did not complete")
    result = copy.deepcopy(expected)
    result["runs"] = len(runs)
    result["aggregate"] = "median"
    for field in ("seconds", "keys_per_second"):
        result["fill"][field] = median([report["fill"][field] for report in runs])
    for field in ("seconds", "batches_per_second", "keys_per_second"):
        result["read"][field] = median([report["read"][field] for report in runs])
    result["read"]["batches"] = median(
        [report["read"]["batches"] for report in runs]
    )
    result["read"]["exist_batches"] = median(
        [report["read"]["exist_batches"] for report in runs]
    )
    result["read"]["get_batches"] = median(
        [report["read"]["get_batches"] for report in runs]
    )
    result["read"]["keys"] = median([report["read"]["keys"] for report in runs])
    result["read"]["missed_deadlines"] = median(
        [report["read"].get("missed_deadlines", 0) for report in runs]
    )
    for latency_key in ("latency_us", "exist_latency_us", "get_latency_us"):
        if latency_key not in result["read"]:
            continue
        for field in ("p50", "p90", "p99", "p999"):
            result["read"][latency_key][field] = median(
                [report["read"][latency_key][field] for report in runs]
            )
    for latency_key in ("put_start_latency_us", "put_end_latency_us"):
        if latency_key not in result["fill"]:
            continue
        for field in ("p50", "p90", "p99", "p999"):
            result["fill"][latency_key][field] = median(
                [report["fill"][latency_key][field] for report in runs]
            )
    usages = [report.get("master_usage", {}) for report in runs]
    cpu = [usage["cpu_percent"] for usage in usages if usage.get("cpu_percent") is not None]
    rss = [usage["rss_kb"] for usage in usages if usage.get("rss_kb") is not None]
    if cpu or rss:
        result["master_usage"] = {
            "cpu_percent": median(cpu) if cpu else None,
            "rss_kb": median(rss) if rss else None,
        }
    result["fill"]["errors"] = max(report["fill"]["errors"] for report in runs)
    result["read"]["errors"] = max(report["read"]["errors"] for report in runs)
    result["read"]["missing"] = max(report["read"]["missing"] for report in runs)
    return result


def run_single(side, master, port, bench, args, workdir, index):
    side_dir = workdir / side
    side_dir.mkdir(parents=True, exist_ok=True)
    log_path = side_dir / f"master-{index}.log"
    result_path = side_dir / f"run-{index}.json"
    bench_log = side_dir / f"bench-{index}.log"
    ensure_port_free(port, side)
    process, log = start_master(master, port, log_path, args)
    try:
        wait_for_port(port, args.startup_timeout, process, log_path)
        cpu_start = process_cpu_ticks(process.pid)
        wall_start = time.monotonic()
        report = run_benchmark(
            bench, str(result_path), str(bench_log), args, port
        )
        report["master_usage"] = process_usage(process.pid, cpu_start, wall_start)
    finally:
        stop_process(process, args.stop_timeout)
        log.close()
    print(f"{side} run {index}/{args.runs}: "
          f"fill {report['fill']['keys_per_second']:.1f} keys/s, "
          f"read {report['read']['batches_per_second']:.1f} batches/s, "
          f"p99 {report['read']['latency_us']['p99']:.1f} us, "
          f"cpu {report['master_usage']['cpu_percent']}%")
    return report


def write_aggregate(side, reports, workdir):
    aggregate_report = aggregate(reports, side)
    aggregate_path = workdir / side / "median.json"
    with open(aggregate_path, "w", encoding="utf-8") as output:
        json.dump(aggregate_report, output, indent=2)
        output.write("\n")
    return aggregate_path


def main():
    args = parse_args()
    if args.runs < 1:
        raise SystemExit("--runs must be at least 1")
    if args.base_port == args.current_port:
        raise SystemExit("--base-port and --current-port must differ")
    require_executable(args.base_master, "base master")
    require_executable(args.current_master, "current master")
    require_executable(args.bench, "benchmark")
    base_bench = args.base_bench or args.bench
    current_bench = args.current_bench or args.bench
    require_executable(base_bench, "base benchmark")
    require_executable(current_bench, "current benchmark")
    if not os.path.isfile(args.gate):
        raise SystemExit(f"gate script not found: {args.gate}")

    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    base_reports = []
    current_reports = []
    for index in range(1, args.runs + 1):
        sides = (
            (("base", args.base_master, args.base_port, base_bench, base_reports),
             ("current", args.current_master, args.current_port, current_bench, current_reports))
            if index % 2
            else (("current", args.current_master, args.current_port, current_bench, current_reports),
                  ("base", args.base_master, args.base_port, base_bench, base_reports))
        )
        for side, master, port, bench, reports in sides:
            reports.append(
                run_single(side, master, port, bench, args, workdir, index)
            )
    base = write_aggregate("base", base_reports, workdir)
    current = write_aggregate("current", current_reports, workdir)
    gate_log = workdir / "gate.log"
    command = [
        sys.executable,
        args.gate,
        "--baseline", str(base),
        "--current", str(current),
        "--max-rps-drop-pct", str(args.max_rps_drop_pct),
        "--max-p50-increase-pct", str(args.max_p50_increase_pct),
    ]
    completed = subprocess.run(command, text=True, capture_output=True)
    gate_log.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
