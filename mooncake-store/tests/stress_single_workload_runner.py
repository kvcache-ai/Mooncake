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
1. Mode Selection:
   Use --mode to choose Centralization, P2P, or both (default: both).
   Example: python3 stress_single_workload_runner.py --mode Centralization --threads 8 --value_size 1048576

2. Matrix Sweep:
   Enabled with the --matrix flag. Supports comma-separated lists for key dimensions.
   Example: python3 stress_single_workload_runner.py --matrix --threads 4,8,16 --value_size 1048576,4194304 --output results.csv
   Note: Matrix mode automatically iterates through all combinations (Cartesian product).

Core Arguments:
--------------
--mode: [Centralization, P2P, both] (default: both)
--rounds: Number of rounds per configuration to average (default: 5)
--ops: Number of operations per thread, supports lists like "100,200" (default: 100)
--threads: Number of worker threads, supports lists like "4,8,16"
--value_size: Value size in bytes, supports lists like "1048576,4194304"
--rpc_threads: Master RPC thread count, supports lists like "4,16,32"
--ram_buffer_size_gb: Client's RAM buffer size in GB, supports lists like "8,16" (default: 15)
--output: Path to save results (.csv or .json)
"""

import subprocess
import time
import re
import os
import signal
import statistics
import argparse
import json
import csv
import itertools
from datetime import datetime

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

def run_benchmark_config(mode, rounds, threads, value_size, ops, rpc_threads, ram_buffer_size_gb):
    """Run a specific configuration for a single mode."""
    kill_existing_processes()
    
    master_cmd = f"{MASTER_BIN} --rpc_port={RPC_PORT} --metrics_port={METRICS_PORT} --deployment_mode={mode} --rpc_thread_num={rpc_threads}"
    master_proc = subprocess.Popen(master_cmd.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2) # Wait for master
    
    all_rounds_metrics = []
    for r in range(1, rounds + 1):
        test_cmd = f"{TEST_BIN} --client_type={mode} --num_threads={threads} --value_size={value_size} --test_operation_nums={ops} --ram_buffer_size_gb={ram_buffer_size_gb}"
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
    parser.add_argument("--ram_buffer_size_gb", type=str, default="15", help="RAM buffer size in GB (list: 8,16,32)")
    
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
    
    # Define dimensions for the sweep
    # We group external dimensions (size, rpc_th, threads) to allow easy comparison of modes (Centralization vs P2P)
    sweep_dims = {
        "value_size": value_list,
        "rpc_threads": rpc_threads_list,
        "threads": threads_list,
        "ops": ops_list,
        "ram_buffer_size_gb": ram_list,
    }
    dim_keys = list(sweep_dims.keys())
    dim_combinations = list(itertools.product(*sweep_dims.values()))
    
    results = []
    
    total_configs = len(modes) * len(dim_combinations)
    current = 0
    
    print(f"Starting {('matrix' if args.matrix else 'comparison')} benchmark...")
    print(f"Total configurations to test: {total_configs}")
    
    for combo in dim_combinations:
        # Unpack configuration
        cfg = dict(zip(dim_keys, combo))
        v_size, r_th, th, ops, r_buf_gb = cfg["value_size"], cfg["rpc_threads"], cfg["threads"], cfg["ops"], cfg["ram_buffer_size_gb"]
        
        config_results = {}
        for m in modes:
            current += 1
            print(f"[{current}/{total_configs}] Testing: mode={m}, threads={th}, val={v_size/1024/1024:.1f}MB, rpc_th={r_th}, ops={ops}, buffer={r_buf_gb}GB...")
            
            avg = run_benchmark_config(m, args.rounds, th, v_size, ops, r_th, r_buf_gb)
            if avg:
                entry = {
                    "mode": m,
                    "threads": th,
                    "value_size": v_size,
                    "rpc_threads": r_th,
                    "ops": ops,
                    "ram_buffer_size_gb": r_buf_gb,
                    **avg
                }
                results.append(entry)
                config_results[m] = avg
        
        # If not matrix mode and we have both results, print a comparison table for this config
        if not args.matrix and "Centralization" in config_results and "P2P" in config_results:
            print_comparison_table(th, v_size, r_th, ops, r_buf_gb, config_results, args.rounds)

    # If matrix mode or we have many results, show final summary
    if args.matrix:
        print_matrix_summary(results)
        
    # Save output
    if args.output and results:
        save_results(results, args.output)

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
    print("\n" + "="*160)
    print("MATRIX SWEEP SUMMARY")
    print("-" * 160)
    # Header with PUT and GET latency columns
    latency_hdr = " | ".join([f"{'P-P50':<6} {'P-P70':<6} {'P-P90':<6} {'P-P99':<6}", 
                               f"{'G-P50':<6} {'G-P70':<6} {'G-P90':<6} {'G-P99':<6}"])
    header = f"{'Mode':<15} | {'Th':<3} | {'Val(MB)':<7} | {'RPC':<3} | {'Ops':<5} | {'Buf':<3} | {'Success%':<8} | {'PUT-MB/s':<10} | {'GET-MB/s':<10} | {latency_hdr}"
    print(header)
    print("-" * 180)
    for r in results:
        put_lat = f"{r['put_p50']:<6.1f} {r['put_p70']:<6.1f} {r['put_p90']:<6.1f} {r['put_p99']:<6.1f}"
        get_lat = f"{r['get_p50']:<6.1f} {r['get_p70']:<6.1f} {r['get_p90']:<6.1f} {r['get_p99']:<6.1f}"
        print(f"{r['mode']:<15} | {r['threads']:<3} | {r['value_size']/1024/1024:<7.1f} | {r['rpc_threads']:<3} | {r['ops']:<5} | {r['ram_buffer_size_gb']:<3} | "
              f"{r['success_rate']:<8.1f} | {r['put_throughput']:<10.1f} | {r['get_throughput']:<10.1f} | "
              f"{put_lat} | {get_lat}")
    print("=" * 160 + "\n")

def save_results(results, path):
    print(f"Saving results to {path}...")
    if path.endswith(".json"):
        with open(path, 'w') as f:
            json.dump(results, f, indent=2)
    elif path.endswith(".csv"):
        if not results: return
        keys = results[0].keys()
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
    else:
        print(f"[!] Unknown file extension for {path}. Skipping save.")

if __name__ == "__main__":
    main()
