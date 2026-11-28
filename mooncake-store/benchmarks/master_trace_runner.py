#!/usr/bin/env python3
import argparse
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

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
LATENCY_FIELDS = ("mean", "p50", "p95", "p99")


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(2)


def log_tail(path, limit=30):
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-limit:])


def port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def check_executable(path, label):
    p = Path(path)
    if not p.is_file() or not os.access(p, os.X_OK):
        fail(f"{label} is not an executable file: {path}")


def metric_reporting_disabled(master_args):
    for arg in master_args:
        if arg in (
            "--enable_metric_reporting=false",
            "--enable_metric_reporting=0",
            "--noenable_metric_reporting",
        ):
            return True
    return False


def health_ready(metrics_port):
    url = f"http://127.0.0.1:{metrics_port}/health"
    try:
        with urllib.request.urlopen(url, timeout=0.5) as resp:
            if resp.status != 200:
                return False
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            return bool(payload.get("service_ready"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError):
        return False


def wait_for_master(rpc_port, metrics_port, use_health, process, log_path, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"master exited with {process.returncode}\n{log_tail(log_path)}"
            )
        if use_health:
            if health_ready(metrics_port):
                return
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.2)
                if sock.connect_ex(("127.0.0.1", rpc_port)) == 0:
                    return
        time.sleep(0.2)
    raise RuntimeError(
        f"master did not become ready on rpc={rpc_port}, metrics={metrics_port}\n"
        f"{log_tail(log_path)}"
    )


def signal_process_group(process, sig):
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            process.send_signal(sig)
        except ProcessLookupError:
            pass


def stop(process, ports, timeout):
    if process.poll() is None:
        signal_process_group(process, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            print(
                f"master did not exit within {timeout}s after SIGTERM; sending SIGKILL",
                file=sys.stderr,
                flush=True,
            )
            signal_process_group(process, signal.SIGKILL)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                print(
                    "warning: master still exiting after SIGKILL",
                    file=sys.stderr,
                    flush=True,
                )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not all(port_free(p) for p in ports):
        time.sleep(0.2)
    busy = [p for p in ports if not port_free(p)]
    if busy:
        print(
            f"warning: ports still busy after master stop: {busy}",
            file=sys.stderr,
            flush=True,
        )


def validate_single_run(result, side, run_index):
    allow_eviction = bool(result.get("allow_eviction", False))
    skipped_ops = result.get("skipped_ops", 0)
    if (
        result.get("errors") != 0
        or result.get("missing") != 0
        or result.get("failed_ops") != 0
    ):
        raise RuntimeError(
            f"{side} run {run_index} had errors={result.get('errors')}, "
            f"missing={result.get('missing')}, failed_ops={result.get('failed_ops')}"
        )
    if not allow_eviction and skipped_ops != 0:
        raise RuntimeError(
            f"{side} run {run_index} had unexpected skipped_ops={skipped_ops}"
        )
    if result.get("completed_ops", 0) + skipped_ops != result.get("planned_ops"):
        raise RuntimeError(
            f"{side} run {run_index} completed {result.get('completed_ops')} "
            f"(skipped {skipped_ops}) of {result.get('planned_ops')} ops"
        )
    if result.get("keys_total") != result.get("keys_planned"):
        raise RuntimeError(
            f"{side} run {run_index} wrote {result.get('keys_total')} "
            f"of {result.get('keys_planned')} keys"
        )


def run_once(args, side, master, rpc_port, metrics_port, bench, run_index):
    master_log = args.result_dir / f"{side}-master-{run_index}.log"
    bench_log = args.result_dir / f"{side}-bench-{run_index}.log"
    result_path = args.result_dir / f"{side}-{run_index}.json"
    if result_path.exists():
        result_path.unlink()

    master_args = [
        f"--rpc_port={rpc_port}",
        "--rpc_address=127.0.0.1",
        f"--metrics_port={metrics_port}",
        "--metrics_host=127.0.0.1",
        f"--rpc_thread_num={args.rpc_threads}",
        "--memory_allocator=offset",
        f"--eviction_high_watermark_ratio={args.eviction_high_watermark_ratio}",
        "--allow_evict_soft_pinned_objects=false",
    ] + args.master_arg
    use_health = not metric_reporting_disabled(master_args)

    with open(master_log, "w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [master] + master_args,
            cwd=args.workdir,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        wait_for_master(
            rpc_port,
            metrics_port,
            use_health,
            process,
            master_log,
            args.startup_timeout,
        )
        command = [
            bench,
            f"--master=127.0.0.1:{rpc_port}",
            f"--model={args.model}",
            f"--profile_dir={args.profile_dir}",
            f"--seed={args.seed}",
            f"--amplify={args.amplify}",
            f"--speed={args.speed}",
            f"--clone_spread_us={args.clone_spread_us}",
            f"--object_bytes={args.object_bytes}",
            f"--segment_bytes={args.segment_bytes}",
            f"--rpc_timeout_ms={args.rpc_timeout_ms}",
            f"--allow_eviction={'true' if args.allow_eviction else 'false'}",
            f"--result_file={result_path}",
        ]
        try:
            with open(bench_log, "w", encoding="utf-8") as blog:
                completed = subprocess.run(
                    command,
                    cwd=args.workdir,
                    stdout=blog,
                    stderr=subprocess.STDOUT,
                    timeout=args.bench_timeout,
                    check=False,
                )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"{side} benchmark timed out after {args.bench_timeout}s\n"
                f"--- bench log ---\n{log_tail(bench_log)}\n"
                f"--- master log ---\n{log_tail(master_log)}"
            ) from exc
        if completed.returncode != 0:
            raise RuntimeError(
                f"{side} benchmark exited with {completed.returncode}\n"
                f"--- bench log ---\n{log_tail(bench_log)}\n"
                f"--- master log ---\n{log_tail(master_log)}"
            )
    finally:
        stop(process, (rpc_port, metrics_port), args.stop_timeout)

    if not result_path.is_file():
        raise RuntimeError(f"{side} benchmark did not write {result_path}")
    with open(result_path, "r", encoding="utf-8") as stream:
        result = json.load(stream)
    validate_single_run(result, side, run_index)
    return result


def median(values):
    return statistics.median(values)


def aggregate(results):
    first = results[0]
    out = {
        "benchmark": "master_trace",
        "runs": len(results),
    }
    out.update({field: first[field] for field in PLAN_FIELDS})
    out["planned_ops"] = first["planned_ops"]
    out["keys_planned"] = first["keys_planned"]
    # Use min for completion counts and max for failure counts so a partial or
    # failing run cannot be masked by the median.
    out["completed_ops"] = min(r["completed_ops"] for r in results)
    out["skipped_ops"] = max(r.get("skipped_ops", 0) for r in results)
    out["keys_total"] = min(r["keys_total"] for r in results)
    out["failed_ops"] = max(r["failed_ops"] for r in results)
    out["errors"] = max(r["errors"] for r in results)
    out["missing"] = max(r["missing"] for r in results)
    out["evicted_misses"] = max(r.get("evicted_misses", 0) for r in results)
    out["seconds"] = median([r["seconds"] for r in results])
    out["ops_per_second"] = median([r["ops_per_second"] for r in results])
    out["keys_per_second"] = median([r.get("keys_per_second", 0.0) for r in results])
    out["missed_deadlines"] = int(median([r["missed_deadlines"] for r in results]))
    out["exist_not_found"] = max(r["exist_not_found"] for r in results)

    for op in OP_KINDS:
        out[f"{op}_ops"] = min(r[f"{op}_ops"] for r in results)
        out[f"{op}_keys"] = min(r[f"{op}_keys"] for r in results)
        lat = {
            "count": min(r[f"{op}_latency_us"]["count"] for r in results),
        }
        for key in LATENCY_FIELDS:
            lat[key] = median([r[f"{op}_latency_us"][key] for r in results])
        out[f"{op}_latency_us"] = lat

    out["runs_seconds"] = [round(r["seconds"], 4) for r in results]
    out["runs_ops_per_second"] = [round(r["ops_per_second"], 3) for r in results]
    return out


def install_signal_handlers():
    def handle_signal(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)


def main():
    install_signal_handlers()

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-master", required=True)
    parser.add_argument("--current-master", required=True)
    parser.add_argument(
        "--bench",
        default=None,
        help="Single benchmark binary used for both base and current sides",
    )
    parser.add_argument("--base-bench", default=None)
    parser.add_argument("--current-bench", default=None)
    parser.add_argument(
        "--gate",
        default=str(SCRIPT_DIR / "master_trace_gate.py"),
    )
    parser.add_argument("--workdir", default=str(Path.cwd()))
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--model", default="qwen3.5-9b")
    parser.add_argument(
        "--profile-dir",
        default=str(SCRIPT_DIR / "profiles"),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--amplify", type=int, default=1)
    parser.add_argument("--speed", type=int, default=0)
    parser.add_argument("--clone-spread-us", type=int, default=1000)
    parser.add_argument(
        "--object-bytes",
        type=int,
        default=0,
        help="Allocated bytes per object; 0 uses the profile default",
    )
    parser.add_argument("--segment-bytes", type=int, default=64 * 1024**3)
    parser.add_argument("--rpc-timeout-ms", type=int, default=60000)
    parser.add_argument("--rpc-threads", type=int, default=32)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--base-port", type=int, default=50071)
    parser.add_argument("--current-port", type=int, default=50072)
    parser.add_argument("--base-metrics-port", type=int, default=None)
    parser.add_argument("--current-metrics-port", type=int, default=None)
    parser.add_argument("--startup-timeout", type=float, default=60)
    parser.add_argument("--stop-timeout", type=float, default=60)
    parser.add_argument("--bench-timeout", type=float, default=3600)
    parser.add_argument("--max-rps-drop-pct", type=float, default=10.0)
    parser.add_argument("--max-p50-increase-pct", type=float, default=10.0)
    parser.add_argument("--min-latency-samples", type=int, default=30)
    parser.add_argument("--eviction-high-watermark-ratio", type=float, default=1.0)
    parser.add_argument("--allow-eviction", action="store_true")
    parser.add_argument("--master-arg", action="append", default=[])
    args = parser.parse_args()

    if args.runs < 1 or args.runs % 2 == 0:
        fail("--runs must be a positive odd number")
    if args.rpc_threads < 1:
        fail("--rpc-threads must be positive")
    if args.amplify < 1:
        fail("--amplify must be >= 1")
    if args.seed < 0:
        fail("--seed must be >= 0")
    if args.speed < 0:
        fail("--speed must be >= 0")
    if args.clone_spread_us < 0:
        fail("--clone-spread-us must be >= 0")
    if args.segment_bytes < 1:
        fail("--segment-bytes must be positive")
    if args.rpc_timeout_ms < 0:
        fail("--rpc-timeout-ms must be >= 0")
    if args.startup_timeout <= 0 or args.stop_timeout <= 0 or args.bench_timeout <= 0:
        fail("timeouts must be positive")
    if not (0.0 < args.eviction_high_watermark_ratio <= 1.0):
        fail("--eviction-high-watermark-ratio must be in (0, 1]")
    if args.eviction_high_watermark_ratio < 1.0 and not args.allow_eviction:
        print(
            "note: --eviction-high-watermark-ratio < 1.0 without "
            "--allow-eviction; evicted reads will fail",
            file=sys.stderr,
            flush=True,
        )

    args.workdir = str(Path(args.workdir).resolve())
    if not Path(args.workdir).is_dir():
        fail(f"--workdir does not exist: {args.workdir}")

    base_metrics_port = (
        args.base_metrics_port
        if args.base_metrics_port is not None
        else args.base_port + 1000
    )
    current_metrics_port = (
        args.current_metrics_port
        if args.current_metrics_port is not None
        else args.current_port + 1000
    )
    all_ports = (
        args.base_port,
        args.current_port,
        base_metrics_port,
        current_metrics_port,
    )
    if len(set(all_ports)) != len(all_ports):
        fail("base/current RPC and metrics ports must all be distinct")
    for port in all_ports:
        if not (1 <= port <= 65535):
            fail(f"port out of range: {port}")
        if not port_free(port):
            fail(f"port {port} is already in use")

    base_bench = args.base_bench or args.bench
    current_bench = args.current_bench or args.bench
    if not base_bench or not current_bench:
        fail("specify --bench or both --base-bench and --current-bench")
    if args.object_bytes < 0:
        fail("--object-bytes must be >= 0")

    # Resolve all paths against caller CWD before launching subprocesses under
    # --workdir so relative paths remain valid when --workdir differs from CWD.
    base_master = str(Path(args.base_master).resolve())
    current_master = str(Path(args.current_master).resolve())
    base_bench = str(Path(base_bench).resolve())
    current_bench = str(Path(current_bench).resolve())
    args.gate = str(Path(args.gate).resolve())
    args.profile_dir = str(Path(args.profile_dir).resolve())

    check_executable(base_master, "--base-master")
    check_executable(current_master, "--current-master")
    check_executable(base_bench, "--base-bench")
    check_executable(current_bench, "--current-bench")
    if not Path(args.gate).is_file():
        fail(f"--gate does not exist: {args.gate}")
    if not Path(args.profile_dir).is_dir():
        fail(f"--profile-dir does not exist: {args.profile_dir}")

    args.result_dir = Path(args.result_dir).resolve()
    args.result_dir.mkdir(parents=True, exist_ok=True)

    sides = {
        "base": (base_master, base_bench, args.base_port, base_metrics_port),
        "current": (
            current_master,
            current_bench,
            args.current_port,
            current_metrics_port,
        ),
    }
    runs = {"base": [], "current": []}
    for index in range(args.runs):
        order = ("base", "current") if index % 2 == 0 else ("current", "base")
        for side in order:
            master, bench, rpc_port, metrics_port = sides[side]
            print(f"run {index + 1}/{args.runs}: {side}", flush=True)
            runs[side].append(
                run_once(args, side, master, rpc_port, metrics_port, bench, index)
            )

    reports = {side: aggregate(values) for side, values in runs.items()}
    baseline_path = args.result_dir / "baseline.json"
    current_path = args.result_dir / "current.json"
    gate_path = args.result_dir / "gate.json"
    baseline_path.write_text(
        json.dumps(reports["base"], indent=2) + "\n", encoding="utf-8"
    )
    current_path.write_text(
        json.dumps(reports["current"], indent=2) + "\n", encoding="utf-8"
    )

    gate = subprocess.run(
        [
            sys.executable,
            args.gate,
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--max-rps-drop-pct",
            str(args.max_rps_drop_pct),
            "--max-p50-increase-pct",
            str(args.max_p50_increase_pct),
            "--min-latency-samples",
            str(args.min_latency_samples),
        ],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        text=True,
        check=False,
    )
    if gate.stdout:
        print(gate.stdout, end="")
        gate_path.write_text(gate.stdout, encoding="utf-8")
    return gate.returncode


if __name__ == "__main__":
    sys.exit(main())
