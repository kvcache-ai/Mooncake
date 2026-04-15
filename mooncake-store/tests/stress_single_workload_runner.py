#!/usr/bin/env python3
"""
This script integrates single-run performance comparison and multi-dimensional matrix sweeping.

!!! IMPORTANT: DEPENDENCIES REQUIRED !!!
This script is a WRAPPER that orchestrates the following C++ binaries:
1. mooncake_master (The master node service)
2. stress_workload_test (The benchmark client)

The locations of these two binaries MUST be correctly provided via the MASTER_BIN and TEST_BIN
variables before running this script.

Usage:
------
# Single comparison run (Centralization vs P2P) with batch=4:
  python3 stress_single_workload_runner.py --mode both --threads 8 --value_size 1048576 --batch 4

# P2P only, sweep async worker counts (memcpy mode):
  python3 stress_single_workload_runner.py --mode P2P --threads 8 --value_size 1048576 \
      --p2p_local_transfer_mode memcpy --local_memcpy_async_worker_num 32

# Matrix sweep: all combinations of threads/value_size/batch/transfer-mode, save to CSV:
  python3 stress_single_workload_runner.py --matrix --threads 16 --value_size 1048576,4194304 \
      --batch 1,4,16,32 --p2p_local_transfer_mode te,memcpy --local_memcpy_async_worker_num 32 --local_memcpy_async_queue_depth 2048 \
      --output results.csv

Arguments:
----------
--mode:              [Centralization, P2P, both] (default: both)
--rounds:            Number of rounds per configuration to average (default: 5)
--ops:               Number of operations per thread, supports lists like "100,200" (default: 100)
--threads:           Number of worker threads, supports lists like "4,8,16"
--value_size:        Value size in bytes, supports lists like "1048576,4194304"
--rpc_threads:       Master RPC thread count, supports lists like "4,16,32"
--batch:             Batch size for read/write operations, supports lists like "1,4,16" (default: 1)
--ram_buffer_size_gb: Client's RAM buffer size in GB, supports lists like "8,16" (default: 15)
--output:            Path to save results (.csv or .json)
--matrix:            Enable matrix sweep mode (Cartesian product over all list-valued arguments)

P2P-Specific (ignored in Centralization mode):
----------------------------------------------
--p2p_local_transfer_mode:      Local transfer mode: te|memcpy, supports lists like "te,memcpy" (default: te)
--local_memcpy_async_worker_num:  Async memcpy worker threads, supports lists like "4,16,32" (default: 32, memcpy mode only)
--local_memcpy_async_queue_depth: Async memcpy queue depth, supports lists like "256,1024,2048" (default: 2048, memcpy mode only)
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

# --- Configuration ---
# PROJECT_ROOT points to the repository root directory (two levels up from mooncake-store/tests/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MASTER_BIN = os.path.join(PROJECT_ROOT, "build/mooncake-store/src/mooncake_master")
TEST_BIN = os.path.join(PROJECT_ROOT, "build/mooncake-store/tests/stress_workload_test")
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
    subprocess.run("killall -9 mooncake_master stress_workload_test 2>/dev/null", shell=True)
    time.sleep(1)

def parse_metrics(output):
    """Parse the benchmark output for key performance indicators."""
    # Initialize with default values to avoid KeyError
    metrics = {
        'ops_sec': 0.0,
        'throughput_mb_s': 0.0,
        'put_throughput': 0.0,
        'get_throughput': 0.0,
        'success_rate': 0.0,
        'put_p50': 0.0, 'put_p70': 0.0, 'put_p90': 0.0, 'put_p95': 0.0, 'put_p99': 0.0,
        'get_p50': 0.0, 'get_p70': 0.0, 'get_p90': 0.0, 'get_p95': 0.0, 'get_p99': 0.0
    }
    
    # Throughput
    ops_sec = re.search(r"Total Operations/sec: ([\d.]+)", output)
    if ops_sec: metrics['ops_sec'] = float(ops_sec.group(1))
    
    put_throughput = re.search(r"PUT Data Throughput \(MB/s\): ([\d.]+)", output)
    get_throughput = re.search(r"GET Data Throughput \(MB/s\): ([\d.]+)", output)
    
    success_rate = re.search(r"Success Rate: ([\d.]+)%", output)
    if success_rate: metrics['success_rate'] = float(success_rate.group(1))
    
    put_tp_val = float(put_throughput.group(1)) if put_throughput else 0.0
    get_tp_val = float(get_throughput.group(1)) if get_throughput else 0.0
    metrics['throughput_mb_s'] = put_tp_val + get_tp_val
    metrics['put_throughput'] = put_tp_val
    metrics['get_throughput'] = get_tp_val
    
    # Latency Regex Patterns
    for op in ["PUT", "GET"]:
        pattern = rf"{op} Operations - P50: ([\d.]+), P70: ([\d.]+), P90: ([\d.]+), P95: ([\d.]+), P99: ([\d.]+)"
        match = re.search(pattern, output)
        if match:
            metrics[f'{op.lower()}_p50'] = float(match.group(1))
            metrics[f'{op.lower()}_p70'] = float(match.group(2))
            metrics[f'{op.lower()}_p90'] = float(match.group(3))
            metrics[f'{op.lower()}_p95'] = float(match.group(4))
            metrics[f'{op.lower()}_p99'] = float(match.group(5))
            
    return metrics

def run_benchmark_config(mode, rounds, threads, value_size, ops, rpc_threads, ram_buffer_size_gb,
                         batch=1, p2p_local_transfer_mode="te",
                         local_memcpy_async_worker_num=32, local_memcpy_async_queue_depth=2048):
    """Run a specific configuration for a single mode."""
    kill_existing_processes()

    master_cmd = f"{MASTER_BIN} --rpc_port={RPC_PORT} --metrics_port={METRICS_PORT} --deployment_mode={mode} --rpc_thread_num={rpc_threads}"
    master_proc = subprocess.Popen(master_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2) # Wait for master

    all_rounds_metrics = []
    for r in range(1, rounds + 1):
        test_cmd = (f"{TEST_BIN} --client_type={mode} --num_threads={threads} --value_size={value_size} "
                    f"--test_operation_nums={ops} --ram_buffer_size_gb={ram_buffer_size_gb} --batch_size={batch}")

        if mode == "P2P":
            test_cmd += f" --p2p_local_transfer_mode={p2p_local_transfer_mode}"
            if p2p_local_transfer_mode == "memcpy":
                test_cmd += (
                    f" --local_memcpy_async_worker_num={local_memcpy_async_worker_num}"
                    f" --local_memcpy_async_queue_depth={local_memcpy_async_queue_depth}"
                )
            
        output = run_command(test_cmd)
        metrics = parse_metrics(output)
        if metrics:
            all_rounds_metrics.append(metrics)
        else:
            print(f"    [!] Mode {mode} Round {r}/{rounds} Failed.")
            
    master_proc.terminate()
    try:
        master_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        master_proc.kill()
        
    if not all_rounds_metrics:
        return None
        
    # Average results
    avg = {}
    keys = all_rounds_metrics[0].keys()
    for key in keys:
        avg[key] = statistics.mean([r[key] for r in all_rounds_metrics if key in r])
    return avg

# --- Execution ---
def parse_list_arg(arg, type_fn=int):
    if not arg: return []
    return [type_fn(x.strip()) for x in str(arg).split(",")]

def main():
    parser = argparse.ArgumentParser(description="Mooncake Binary Stress Workload Runner")
    # Basic Config
    parser.add_argument("--mode", type=str, default="both", choices=["Centralization", "P2P", "both"], help="Test mode")
    parser.add_argument("--rounds", type=int, default=5, help="Rounds per configuration")
    parser.add_argument("--ops", type=str, default="100", help="Operations per thread (list: 100,500,1000)")
    
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
    parser.add_argument("--local_memcpy_async_queue_depth", type=str, default="2048",
                        help="Async memcpy queue depth (memcpy mode only, list: 256,1024,2048)")
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
    modes = ["Centralization", "P2P"] if args.mode == "both" else [args.mode]
    
    # Global sweep dims (apply to all modes)
    base_sweep_dims = {
        "value_size": value_list,
        "rpc_threads": rpc_threads_list,
        "threads": threads_list,
        "ops": ops_list,
        "ram_buffer_size_gb": ram_list,
        "batch": parse_list_arg(args.batch),
    }

    # P2P specific sweep dims
    p2p_extra_dims = {
        "p2p_local_transfer_mode": parse_list_arg(args.p2p_local_transfer_mode, type_fn=str),
        "local_memcpy_async_worker_num": parse_list_arg(args.local_memcpy_async_worker_num),
        "local_memcpy_async_queue_depth": parse_list_arg(args.local_memcpy_async_queue_depth),
    }

    results = []
    
    # Logic: Iterating through external configurations
    # For each config, if Centralization is requested, run it once (ignoring P2P dims)
    # If P2P is requested, run it for all combinations of P2P dims
    
    base_keys = list(base_sweep_dims.keys())
    base_combinations = list(itertools.product(*base_sweep_dims.values()))
    
    p2p_keys = list(p2p_extra_dims.keys())
    p2p_combinations = list(itertools.product(*p2p_extra_dims.values()))

    total_runs = 0
    if "Centralization" in modes:
        total_runs += len(base_combinations)
    if "P2P" in modes:
        total_runs += len(base_combinations) * len(p2p_combinations)

    current = 0
    print(f"Starting {('matrix' if args.matrix else 'comparison')} benchmark...")
    print(f"Total configurations to test: {total_runs}")
    
    for base_combo in base_combinations:
        base_cfg = dict(zip(base_keys, base_combo))
        v_size, r_th, th, ops, r_buf_gb, batch = [base_cfg[k] for k in base_keys]
        
        config_results = {}
        
        # 1. Run Centralization
        if "Centralization" in modes:
            current += 1
            print(f"\n[{current}/{total_runs}] Testing: mode=Centralization")
            print(f"    threads={th}, batch={batch}, val={v_size/1024/1024:.1f}MB, rpc_threads={r_th}, ops={ops}, ram={r_buf_gb}GB")
            avg = run_benchmark_config("Centralization", args.rounds, th, v_size, ops, r_th, r_buf_gb, batch=batch)
            if avg:
                entry = {"mode": "Centralization", **base_cfg, **avg}
                results.append(entry)
                config_results["Centralization"] = avg
                print_single_result("Centralization", avg)

        # 2. Run P2P with all extra dims
        if "P2P" in modes:
            for p2p_combo in p2p_combinations:
                current += 1
                p2p_cfg = dict(zip(p2p_keys, p2p_combo))
                transfer_mode = p2p_cfg["p2p_local_transfer_mode"]
                wk = p2p_cfg["local_memcpy_async_worker_num"]
                qd = p2p_cfg["local_memcpy_async_queue_depth"]

                print(f"\n[{current}/{total_runs}] Testing: mode=P2P")
                print(f"    threads={th}, batch={batch}, val={v_size/1024/1024:.1f}MB, rpc_threads={r_th}, ops={ops}, ram={r_buf_gb}GB")
                extra = f"p2p_local_transfer_mode={transfer_mode}"
                if transfer_mode == "memcpy":
                    extra += f", local_memcpy_async_worker_num={wk}, local_memcpy_async_queue_depth={qd}"
                print(f"    {extra}")

                avg = run_benchmark_config("P2P", args.rounds, th, v_size, ops, r_th, r_buf_gb,
                                           batch=batch, p2p_local_transfer_mode=transfer_mode,
                                           local_memcpy_async_worker_num=wk,
                                           local_memcpy_async_queue_depth=qd)
                if avg:
                    entry = {"mode": "P2P", **base_cfg, **p2p_cfg, **avg}
                    results.append(entry)
                    config_results["P2P"] = avg  # only last p2p result is kept for print_comparison_table
                    print_single_result("P2P", avg)

        # If comparison mode (single combo), we just show the comparison of Centralization vs LAST P2P run
        if not args.matrix and "Centralization" in config_results and "P2P" in config_results:
            print_comparison_table(th, v_size, r_th, ops, r_buf_gb, config_results, args.rounds)

    # If matrix mode or we have many results, show final summary
    if args.matrix:
        print_matrix_summary(results)
        
    # Save output
    if args.output and results:
        save_results(results, args.output)

def print_single_result(mode, avg):
    print(f"    [{mode}] PUT: {avg['put_throughput']:.1f} MB/s  GET: {avg['get_throughput']:.1f} MB/s  "
          f"Ops/s: {avg['ops_sec']:.1f}  Success: {avg['success_rate']:.1f}%  "
          f"PUT P50/P99: {avg['put_p50']:.1f}/{avg['put_p99']:.1f} us  "
          f"GET P50/P99: {avg['get_p50']:.1f}/{avg['get_p99']:.1f} us")

def print_comparison_table(threads, value_size, rpc_threads, ops, ram_buffer_size_gb, results, rounds):
    print("\n" + "="*80)
    print(f"Comparison Summary (Averages over {rounds} rounds)")
    print(f"Config: threads={threads}, value_size={value_size}, rpc_threads={rpc_threads}, ops={ops}, ram_buffer={ram_buffer_size_gb}GB")
    print("-" * 80)
    print(f"{'Metric':<30} | {'Centralization':<20} | {'P2P':<20}")
    print("-" * 80)
    
    metrics_to_show = [
        ("Success Rate (%)", "success_rate", "{:.2f}"),
        ("Total Ops/sec", "ops_sec", "{:.2f}"),
        ("Total Throughput (MB/s)", "throughput_mb_s", "{:.2f}"),
        ("PUT Throughput (MB/s)", "put_throughput", "{:.2f}"),
        ("GET Throughput (MB/s)", "get_throughput", "{:.2f}"),
        ("PUT P50 (us)", "put_p50", "{:.1f}"),
        ("PUT P70 (us)", "put_p70", "{:.1f}"),
        ("PUT P99 (us)", "put_p99", "{:.1f}"),
        ("GET P50 (us)", "get_p50", "{:.1f}"),
        ("GET P70 (us)", "get_p70", "{:.1f}"),
        ("GET P99 (us)", "get_p99", "{:.1f}"),
    ]
    
    for label, key, fmt in metrics_to_show:
        c_val = fmt.format(results["Centralization"].get(key, 0)) if "Centralization" in results else "N/A"
        p_val = fmt.format(results["P2P"].get(key, 0)) if "P2P" in results else "N/A"
        print(f"{label:<30} | {c_val:<20} | {p_val:<20}")
    print("=" * 80 + "\n")

def print_matrix_summary(results):
    print("\n" + "="*200)
    print("MATRIX SWEEP SUMMARY")
    print("-" * 200)
    latency_hdr = " | ".join([f"{'P-P50':<6} {'P-P90':<6} {'P-P99':<6}",
                               f"{'G-P50':<6} {'G-P90':<6} {'G-P99':<6}"])
    header = (
        f"{'Mode':<15} | {'Th':<3} | {'Bch':<3} | {'Val(MB)':<7} | {'RPC':<3} | "
        f"{'Ops':<5} | {'Buf':<3} | {'TransferMode':<12} | {'Workers':<7} | {'QDepth':<7} | "
        f"{'PUT-MB/s':<10} | {'GET-MB/s':<10} | {latency_hdr}"
    )
    print(header)
    print("-" * 200)
    for r in results:
        put_lat = f"{r['put_p50']:<6.1f} {r['put_p90']:<6.1f} {r['put_p99']:<6.1f}"
        get_lat = f"{r['get_p50']:<6.1f} {r['get_p90']:<6.1f} {r['get_p99']:<6.1f}"
        transfer_mode = str(r.get('p2p_local_transfer_mode', '-'))
        wk = str(r.get('local_memcpy_async_worker_num', '-'))
        qd = str(r.get('local_memcpy_async_queue_depth', '-'))
        print(f"{r['mode']:<15} | {r['threads']:<3} | {r.get('batch', 1):<3} | {r['value_size']/1024/1024:<7.1f} | {r['rpc_threads']:<3} | {r['ops']:<5} | {r['ram_buffer_size_gb']:<3} | "
              f"{transfer_mode:<12} | {wk:<7} | {qd:<7} | "
              f"{r['put_throughput']:<10.1f} | {r['get_throughput']:<10.1f} | "
              f"{put_lat} | {get_lat}")
    print("=" * 200 + "\n")

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
