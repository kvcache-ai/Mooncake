#!/usr/bin/env python3
"""
This script integrates P2P single-run performance testing and multi-dimensional matrix sweeping.

!!! IMPORTANT: DEPENDENCIES REQUIRED !!!
This script is a WRAPPER that orchestrates the following C++ binaries:
1. mooncake_master_p2p (The P2P master node service)
2. stress_workload_test_p2p (The P2P benchmark client)

The locations of these two binaries MUST be correctly provided via the MASTER_BIN and TEST_BIN
variables before running this script. Moreover, bad rounds (non-100% PUT/GET success) are excluded from averages and
surfaced in the CSV as bad_round_count.

Usage:
------
# Single P2P run with batch=4:
  python3 stress_single_workload_runner.py --threads 8 --value_size 1048576 --batch 4

# P2P only, sweep async worker counts (memcpy mode):
  python3 stress_single_workload_runner.py --threads 8 --value_size 1048576 \
      --p2p_local_transfer_mode memcpy --local_memcpy_async_worker_num 32

# Matrix sweep: all combinations of threads/value_size/batch/transfer-mode, save to CSV:
  python3 stress_single_workload_runner.py --matrix --rounds 10 --ops 200 --rpc_threads 32 --ram_buffer_size_gb 70 \
      --threads 16 --batch 1,4,16,32 --value_size 1048576,4194304,8388608 \
      --p2p_local_transfer_mode te,memcpy --local_memcpy_async_worker_num 32 \
      --output results.csv
Arguments:
----------
--rounds:            Number of rounds per configuration to average (default: 5)
--ops:               Number of operations per thread, supports lists like "100,200" (default: 100)
--threads:           Number of worker threads, supports lists like "4,8,16"
--value_size:        Value size in bytes, supports lists like "1048576,4194304"
--rpc_threads:       Master RPC thread count, supports lists like "4,16,32"
--batch:             Batch size for read/write operations, supports lists like "1,4,16" (default: 1)
--ram_buffer_size_gb: Client's RAM buffer size in GB, supports lists like "8,16" (default: 15)
--output:            Path to save results (.csv or .json)
--matrix:            Enable matrix sweep mode (Cartesian product over all list-valued arguments)

--p2p_local_transfer_mode:      Local transfer mode: te|memcpy, supports lists like "te,memcpy" (default: te)
--local_memcpy_async_worker_num:  Async memcpy worker threads, supports lists like "4,16,32" (default: 32, memcpy mode only)
"""

import subprocess
import time
import re
import os
import statistics
import argparse
import json
import csv
import itertools
import socket
import urllib.request
import urllib.error

# --- Configuration ---
# PROJECT_ROOT points to the repository root directory.
PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
MASTER_BIN = os.path.join(PROJECT_ROOT, "build/mooncake-store/src/mooncake_master_p2p")
TEST_BIN = os.path.join(
    PROJECT_ROOT, "build/mooncake-store/tests/p2p/stress_workload_test_p2p"
)
RPC_PORT = 50051
METRICS_PORT = 9003

# --- Utils ---
def run_command(cmd):
    """Run a terminal command and return its output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
        return result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return "ERROR: Command timed out"
    except Exception as e:
        return f"ERROR: {str(e)}"

def kill_existing_processes():
    """Clean up any existing master or test processes."""
    subprocess.run("killall -9 mooncake_master_p2p stress_workload_test_p2p 2>/dev/null", shell=True)
    time.sleep(1)

def wait_master_ready(http_port, timeout_s=30):
    """Poll P2P master's metrics /health endpoint until it responds
    with HTTP 200 + body "OK", indicating the master process has completed
    startup and is accepting requests. Replaces a brittle time.sleep(2)."""
    url = f"http://127.0.0.1:{http_port}/health"
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.5) as resp:
                if resp.status == 200 and resp.read().strip() == b"OK":
                    return True
        except (urllib.error.URLError, OSError, socket.timeout) as e:
            last_err = e
        time.sleep(0.1)
    raise RuntimeError(
        f"Master not ready on {url} within {timeout_s}s (last error: {last_err})"
    )

def parse_metrics(output):
    """Parse the benchmark output. Returns (metrics_dict, is_bad_round).
    A round is considered bad if the benchmark emitted a
    PUT_SUCCESS_RATE_NOT_FULL or GET_SUCCESS_RATE_NOT_FULL marker — in that
    case the measured throughput/latency reflects a partial run and must be
    excluded from config-level averages."""
    metrics = {
        'put_throughput': 0.0,
        'get_throughput': 0.0,
        'put_ops_sec': 0.0,
        'get_ops_sec': 0.0,
        'put_success_rate': 0.0,
        'get_success_rate': 0.0,
        'put_p50': 0.0, 'put_p70': 0.0, 'put_p90': 0.0, 'put_p95': 0.0, 'put_p99': 0.0,
        'get_p50': 0.0, 'get_p70': 0.0, 'get_p90': 0.0, 'get_p95': 0.0, 'get_p99': 0.0,
        'put_batch_p50': 0.0, 'put_batch_p90': 0.0, 'put_batch_p99': 0.0,
        'get_batch_p50': 0.0, 'get_batch_p90': 0.0, 'get_batch_p99': 0.0,
    }

    def grab(pattern, key):
        m = re.search(pattern, output)
        if m:
            metrics[key] = float(m.group(1))

    grab(r"PUT Data Throughput \(MB/s\): ([\d.]+)", 'put_throughput')
    grab(r"GET Data Throughput \(MB/s\): ([\d.]+)", 'get_throughput')
    grab(r"PUT Operations/sec: ([\d.]+)", 'put_ops_sec')
    grab(r"GET Operations/sec: ([\d.]+)", 'get_ops_sec')
    grab(r"PUT Success Rate: ([\d.]+)%", 'put_success_rate')
    grab(r"GET Success Rate: ([\d.]+)%", 'get_success_rate')

    # Per-key latency percentiles
    for op in ["PUT", "GET"]:
        pattern = rf"{op} Operations - P50: ([\d.]+), P70: ([\d.]+), P90: ([\d.]+), P95: ([\d.]+), P99: ([\d.]+)"
        match = re.search(pattern, output)
        if match:
            metrics[f'{op.lower()}_p50'] = float(match.group(1))
            metrics[f'{op.lower()}_p70'] = float(match.group(2))
            metrics[f'{op.lower()}_p90'] = float(match.group(3))
            metrics[f'{op.lower()}_p95'] = float(match.group(4))
            metrics[f'{op.lower()}_p99'] = float(match.group(5))

    # Batch-completion latency percentiles (P50, P90, P99 only — covers the
    # practical questions "typical batch wait" and "worst-case batch wait")
    for op in ["PUT", "GET"]:
        pattern = rf"{op} Batch - P50: ([\d.]+), P70: [\d.]+, P90: ([\d.]+), P95: [\d.]+, P99: ([\d.]+)"
        match = re.search(pattern, output)
        if match:
            metrics[f'{op.lower()}_batch_p50'] = float(match.group(1))
            metrics[f'{op.lower()}_batch_p90'] = float(match.group(2))
            metrics[f'{op.lower()}_batch_p99'] = float(match.group(3))

    is_bad = ("PUT_SUCCESS_RATE_NOT_FULL" in output or
              "GET_SUCCESS_RATE_NOT_FULL" in output)
    # Also bad if test binary crashed or produced no metrics at all.
    if metrics['put_ops_sec'] == 0.0 and metrics['get_ops_sec'] == 0.0:
        is_bad = True
    return metrics, is_bad

def run_benchmark_config(rounds, threads, value_size, ops, rpc_threads, ram_buffer_size_gb,
                         batch=1, p2p_local_transfer_mode="te",
                         local_memcpy_async_worker_num=32,
                         route_cache_max_memory_mb=300, route_cache_ttl_ms=60000):
    """Run a specific P2P configuration."""
    kill_existing_processes()

    master_cmd = (
        f"{MASTER_BIN} --rpc_port={RPC_PORT} --metrics_port={METRICS_PORT} "
        f"--rpc_thread_num={rpc_threads}"
    )
    master_proc = subprocess.Popen(master_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_master_ready(METRICS_PORT)
    except RuntimeError as e:
        master_proc.terminate()
        print(f"    [!] Master startup failed: {e}")
        return None

    good_rounds = []
    bad_rounds = []
    for r in range(1, rounds + 1):
        test_cmd = (f"{TEST_BIN} --num_threads={threads} --value_size={value_size} "
                    f"--test_operation_nums={ops} --ram_buffer_size_gb={ram_buffer_size_gb} --batch_size={batch}")

        test_cmd += (
            f" --p2p_local_transfer_mode={p2p_local_transfer_mode}"
            f" --route_cache_max_memory_mb={route_cache_max_memory_mb}"
            f" --route_cache_ttl_ms={route_cache_ttl_ms}"
        )
        if p2p_local_transfer_mode == "memcpy":
            test_cmd += (
                f" --local_memcpy_async_worker_num={local_memcpy_async_worker_num}"
            )

        output = run_command(test_cmd)
        metrics, is_bad = parse_metrics(output)
        if is_bad:
            bad_rounds.append(metrics)
            print(f"    [!] Mode P2P Round {r}/{rounds} BAD "
                  f"(put_success={metrics['put_success_rate']:.1f}% "
                  f"get_success={metrics['get_success_rate']:.1f}%) — excluded from average.")
        else:
            good_rounds.append(metrics)

    master_proc.terminate()
    try:
        master_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        master_proc.kill()

    if not good_rounds:
        print(f"    [!] All {rounds} rounds bad for this config — no data recorded.")
        return None

    # Aggregate: mean + min + max + stdev over the good rounds. Bad rounds
    # (partial PUT/GET) are excluded so the averaged numbers reflect a clean
    # run rather than a mix of clean and degraded runs.
    keys = good_rounds[0].keys()
    agg = {'bad_round_count': len(bad_rounds), 'good_round_count': len(good_rounds)}
    for key in keys:
        vals = [r[key] for r in good_rounds]
        agg[key] = statistics.mean(vals)
        agg[f'{key}_min'] = min(vals)
        agg[f'{key}_max'] = max(vals)
        agg[f'{key}_std'] = statistics.stdev(vals) if len(vals) > 1 else 0.0
    return agg

# --- Execution ---
def parse_list_arg(arg, type_fn=int):
    if not arg: return []
    return [type_fn(x.strip()) for x in str(arg).split(",")]

def main():
    parser = argparse.ArgumentParser(description="Mooncake P2P Stress Workload Runner")
    # Basic Config
    parser.add_argument("--rounds", type=int, default=5, help="Rounds per configuration")
    parser.add_argument("--ops", type=str, default="100", help="Batch operations per thread (list: 100,500,1000)")
    
    # Matrix Dimensions (can be single values or comma-separated lists)
    parser.add_argument("--threads", type=str, default="8", help="Worker threads (list: 4,8,16)")
    parser.add_argument("--value_size", type=str, default="1048576", help="Value size in bytes (list: 1048576,4194304)")
    parser.add_argument("--rpc_threads", type=str, default="32", help="RPC threads (list: 4,16,32)")
    parser.add_argument("--batch", type=str, default="1", help="Batch size (list: 1,4,16)")
    parser.add_argument("--ram_buffer_size_gb", type=str, default="15", help="RAM buffer size in GB (list: 8,16,32)")
    
    # P2P Specific parameters
    parser.add_argument("--p2p_local_transfer_mode", type=str, default="te",
                        help="P2P local transfer mode: te|memcpy (list: te,memcpy)")
    parser.add_argument("--local_memcpy_async_worker_num", type=str, default="32",
                        help="Async memcpy worker threads (memcpy mode only, list: 4,16,32)")
    parser.add_argument("--route_cache_max_memory_mb", type=str, default="300",
                        help="Max memory for RouteCache in MB (P2P mode, list: 100,300,600)")
    parser.add_argument("--route_cache_ttl_ms", type=str, default="60000",
                        help="TTL for RouteCache entries in ms (P2P mode, list: 60000,300000)")
    # Flags
    parser.add_argument("--matrix", action="store_true", help="Enable matrix sweep mode")
    parser.add_argument("--output", type=str, help="Output file path (ends in .csv or .json)")
    
    args = parser.parse_args()
    
    # Parse dimensions
    threads_list = parse_list_arg(args.threads)
    value_list = parse_list_arg(args.value_size)
    rpc_threads_list = parse_list_arg(args.rpc_threads)
    ops_list = parse_list_arg(args.ops)
    ram_list = parse_list_arg(args.ram_buffer_size_gb)
    
    # Common P2P sweep dimensions.
    base_sweep_dims = {
        "value_size": value_list,
        "rpc_threads": rpc_threads_list,
        "threads": threads_list,
        "ops": ops_list,
        "ram_buffer_size_gb": ram_list,
        "batch": parse_list_arg(args.batch),
    }

    # P2P specific sweep dims — local_memcpy_async_worker_num only applies to memcpy mode.
    transfer_modes = parse_list_arg(args.p2p_local_transfer_mode, type_fn=str)
    worker_nums = parse_list_arg(args.local_memcpy_async_worker_num)
    route_cache_mems = parse_list_arg(args.route_cache_max_memory_mb)
    route_cache_ttls = parse_list_arg(args.route_cache_ttl_ms)

    p2p_combinations = []
    for transfer_mode, rc_mem, rc_ttl in itertools.product(transfer_modes, route_cache_mems, route_cache_ttls):
        if transfer_mode == "te":
            p2p_combinations.append({
                "p2p_local_transfer_mode": transfer_mode,
                "local_memcpy_async_worker_num": None,
                "route_cache_max_memory_mb": rc_mem,
                "route_cache_ttl_ms": rc_ttl,
            })
        else:
            for wk in worker_nums:
                p2p_combinations.append({
                    "p2p_local_transfer_mode": transfer_mode,
                    "local_memcpy_async_worker_num": wk,
                    "route_cache_max_memory_mb": rc_mem,
                    "route_cache_ttl_ms": rc_ttl,
                })

    results = []

    base_keys = list(base_sweep_dims.keys())
    base_combinations = list(itertools.product(*base_sweep_dims.values()))

    total_runs = len(base_combinations) * len(p2p_combinations)

    current = 0
    print(f"Starting {('matrix' if args.matrix else 'comparison')} benchmark...")
    print(f"Total configurations to test: {total_runs}")
    
    for base_combo in base_combinations:
        base_cfg = dict(zip(base_keys, base_combo))
        v_size, r_th, th, ops, r_buf_gb, batch = [base_cfg[k] for k in base_keys]
        
        for p2p_cfg in p2p_combinations:
            current += 1
            transfer_mode = p2p_cfg["p2p_local_transfer_mode"]
            wk = p2p_cfg["local_memcpy_async_worker_num"]
            rc_mem = p2p_cfg["route_cache_max_memory_mb"]
            rc_ttl = p2p_cfg["route_cache_ttl_ms"]

            print(f"\n[{current}/{total_runs}] Testing: mode=P2P")
            print(f"    threads={th}, batch={batch}, val={v_size/1024/1024:.1f}MB, rpc_threads={r_th}, ops={ops}, ram={r_buf_gb}GB")
            extra = f"p2p_local_transfer_mode={transfer_mode}, route_cache={rc_mem}MB/{rc_ttl}ms"
            if transfer_mode == "memcpy":
                extra += f", local_memcpy_async_worker_num={wk}"
            print(f"    {extra}")

            avg = run_benchmark_config(args.rounds, th, v_size, ops, r_th, r_buf_gb,
                                       batch=batch, p2p_local_transfer_mode=transfer_mode,
                                       local_memcpy_async_worker_num=wk,
                                       route_cache_max_memory_mb=rc_mem,
                                       route_cache_ttl_ms=rc_ttl)
            if avg:
                entry = {"mode": "P2P", **base_cfg, **p2p_cfg, **avg}
                results.append(entry)
                print_single_result("P2P", avg)

    # If matrix mode or we have many results, show final summary
    if args.matrix:
        print_matrix_summary(results)
        
    # Save output
    if args.output and results:
        save_results(results, args.output)

def print_single_result(mode, avg):
    bad = avg.get('bad_round_count', 0)
    good = avg.get('good_round_count', 0)
    print(f"    [{mode}] PUT: {avg['put_throughput']:.1f} MB/s  GET: {avg['get_throughput']:.1f} MB/s  "
          f"PUT Ops/s: {avg['put_ops_sec']:.1f}  GET Ops/s: {avg['get_ops_sec']:.1f}  "
          f"PUT P50/P99: {avg['put_p50']:.1f}/{avg['put_p99']:.1f} us  "
          f"GET P50/P99: {avg['get_p50']:.1f}/{avg['get_p99']:.1f} us  "
          f"(good={good}, bad={bad})")

def print_matrix_summary(results):
    print("\n" + "="*210)
    print("MATRIX SWEEP SUMMARY  (Bad = rounds excluded from average due to PUT/GET failures)")
    print("-" * 210)
    latency_hdr = " | ".join([f"{'P-P50':<6} {'P-P90':<6} {'P-P99':<6}",
                               f"{'G-P50':<6} {'G-P90':<6} {'G-P99':<6}"])
    header = (
        f"{'Mode':<15} | {'Th':<3} | {'Bch':<4} | {'Val(MB)':<7} | {'RPC':<3} | "
        f"{'Ops':<5} | {'Buf':<3} | {'TransferMode':<12} | {'Workers':<7} | {'Bad':<3} | "
        f"{'PUT-MB/s':<10} | {'GET-MB/s':<10} | {latency_hdr}"
    )
    print(header)
    print("-" * 210)
    for r in results:
        put_lat = f"{r['put_p50']:<6.1f} {r['put_p90']:<6.1f} {r['put_p99']:<6.1f}"
        get_lat = f"{r['get_p50']:<6.1f} {r['get_p90']:<6.1f} {r['get_p99']:<6.1f}"
        transfer_mode = str(r.get('p2p_local_transfer_mode', '-'))
        wk_val = r.get('local_memcpy_async_worker_num')
        wk = '-' if wk_val is None else str(wk_val)
        bad = str(r.get('bad_round_count', 0))
        print(f"{r['mode']:<15} | {r['threads']:<3} | {r.get('batch', 1):<4} | {r['value_size']/1024/1024:<7.1f} | {r['rpc_threads']:<3} | {r['ops']:<5} | {r['ram_buffer_size_gb']:<3} | "
              f"{transfer_mode:<12} | {wk:<7} | {bad:<3} | "
              f"{r['put_throughput']:<10.1f} | {r['get_throughput']:<10.1f} | "
              f"{put_lat} | {get_lat}")
    print("=" * 210 + "\n")

def save_results(results, path):
    print(f"Saving results to {path}...")
    if path.endswith(".json"):
        with open(path, 'w') as f:
            json.dump(results, f, indent=2)
    elif path.endswith(".csv"):
        if not results: return
        # Collect all unique keys in insertion order across all rows (P2P rows have extra fields)
        seen = set()
        keys = []
        for r in results:
            for k in r.keys():
                if k not in seen:
                    keys.append(k)
                    seen.add(k)
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys, restval='')
            writer.writeheader()
            writer.writerows(results)
    else:
        print(f"[!] Unknown file extension for {path}. Skipping save.")

if __name__ == "__main__":
    main()
