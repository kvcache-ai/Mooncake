#!/usr/bin/env python3
"""
Multi-threaded KVCache E2E benchmark for Mooncake Store.
Tests under EVICTION PRESSURE to reveal lock-contention optimizations.

Key design:
1. Pre-fill store to target utilization (default 90%) → triggers eviction on new PUTs
2. Multi-threaded PUT under eviction pressure → CMS increment contention
3. Multi-threaded GET → SharedMutex/read-lock benefits

CMS lock-free only shows benefits when eviction path is active (store near full).
Without eviction pressure, the CMS mutex is never contended and improvements are invisible.

Usage:
  python3 test_kvcache_e2e.py --threads 1,2,4,8,16 --prefill-ratio 0.90
"""

import os
import sys
import time
import threading
import argparse
import json
import statistics
import numpy as np

_script_dir = os.path.dirname(os.path.abspath(__file__))
_default_conda = os.environ.get('CONDA_PREFIX', '/data/lizhijun/anaconda3')
_ld_paths = [
    f"{_default_conda}/lib",
    f"{_script_dir}/builddir/mooncake-common",
    f"{_script_dir}/builddir/mooncake-store/src",
    f"{_script_dir}/builddir/mooncake-transfer-engine/src",
    os.environ.get('LD_LIBRARY_PATH', ''),
]
os.environ["LD_LIBRARY_PATH"] = ":".join(_ld_paths)
# NOTE: Setting LD_LIBRARY_PATH via os.environ only affects subprocesses,
# not the current process's dynamic linker. The caller MUST set it before
# invoking python. We keep this for subprocess consistency.

def _load_store_module(store_so_path=None):
    if store_so_path:
        candidates = [store_so_path]
    else:
        candidates = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "builddir/mooncake-integration/store.cpython-313-x86_64-linux-gnu.so"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "builddir_py312/mooncake-integration/store.cpython-312-x86_64-linux-gnu.so"),
        ]
    import importlib.util
    for path in candidates:
        if os.path.exists(path):
            spec = importlib.util.spec_from_file_location("store", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise FileNotFoundError(f"No store .so found in {candidates}")

store_mod = _load_store_module(os.environ.get("MOONCAKE_STORE_SO", ""))
MooncakeDistributedStore = store_mod.MooncakeDistributedStore
ReplicateConfig = store_mod.ReplicateConfig
KVCACHE = store_mod.KVCACHE


def bench_concurrent_put_eviction(store, pairs, num_threads, ops_per_thread):
    """
    Multi-threaded PUT under eviction pressure.
    Each thread randomly picks keys and writes new random values.
    With store near full, every PUT triggers eviction check → CMS.increment().
    """
    results_lock = threading.Lock()
    all_latencies = []
    total_bytes = [0]
    errors = [0]
    ready = threading.Barrier(num_threads + 1)

    config = ReplicateConfig()
    config.replica_num = 1

    def worker(tid):
        local_latencies = []
        local_bytes = 0
        local_errors = 0
        rng = np.random.RandomState(42 + tid)

        try:
            indices = rng.randint(0, len(pairs), size=ops_per_thread)
            ready.wait()
            for idx in indices:
                key, _ = pairs[idx]
                # New random value of same size → forces allocation + eviction
                value = np.random.bytes(len(pairs[idx][1]))
                t0 = time.perf_counter()
                rc = store.put(key, value, config)
                local_latencies.append((time.perf_counter() - t0) * 1e6)
                if rc != 0:
                    local_errors += 1
                local_bytes += len(value)
        except Exception as e:
            print(f"  Thread {tid} error: {e}")

        with results_lock:
            all_latencies.extend(local_latencies)
            total_bytes[0] += local_bytes
            errors[0] += local_errors

    threads = []
    for t in range(num_threads):
        th = threading.Thread(target=worker, args=(t,))
        threads.append(th)
        th.start()

    ready.wait()
    t0 = time.perf_counter()

    for th in threads:
        th.join()

    elapsed = time.perf_counter() - t0
    total_ops = len(all_latencies)

    return {
        "elapsed_s": elapsed,
        "total_bytes": total_bytes[0],
        "num_ops": total_ops,
        "errors": errors[0],
        "throughput_mbps": total_bytes[0] / elapsed / (1024 * 1024) if elapsed > 0 else 0,
        "ops_per_sec": total_ops / elapsed if elapsed > 0 else 0,
        "latency_us_p50": float(np.percentile(all_latencies, 50)) if all_latencies else 0,
        "latency_us_p99": float(np.percentile(all_latencies, 99)) if all_latencies else 0,
        "threads": num_threads,
    }


def bench_concurrent_get(store, keys, num_threads, ops_per_thread):
    """Multi-threaded GET benchmark."""
    results_lock = threading.Lock()
    all_latencies = []
    total_bytes = [0]
    hits = [0]
    misses = [0]
    ready = threading.Barrier(num_threads + 1)

    def worker(tid):
        local_latencies = []
        local_bytes = 0
        local_hits = 0
        local_misses = 0
        rng = np.random.RandomState(100 + tid)

        try:
            indices = rng.randint(0, len(keys), size=ops_per_thread)
            ready.wait()
            for idx in indices:
                key = keys[idx]
                t0 = time.perf_counter()
                value = store.get(key)
                local_latencies.append((time.perf_counter() - t0) * 1e6)
                if value is not None and len(value) > 0:
                    local_bytes += len(value)
                    local_hits += 1
                else:
                    local_misses += 1
        except Exception as e:
            print(f"  Thread {tid} error: {e}")

        with results_lock:
            all_latencies.extend(local_latencies)
            total_bytes[0] += local_bytes
            hits[0] += local_hits
            misses[0] += local_misses

    threads = []
    for t in range(num_threads):
        th = threading.Thread(target=worker, args=(t,))
        threads.append(th)
        th.start()

    ready.wait()
    t0 = time.perf_counter()

    for th in threads:
        th.join()

    elapsed = time.perf_counter() - t0
    total_ops = hits[0] + misses[0]

    return {
        "elapsed_s": elapsed,
        "total_bytes": total_bytes[0],
        "num_ops": total_ops,
        "hits": hits[0],
        "misses": misses[0],
        "throughput_mbps": total_bytes[0] / elapsed / (1024 * 1024) if elapsed > 0 else 0,
        "ops_per_sec": total_ops / elapsed if elapsed > 0 else 0,
        "latency_us_p50": float(np.percentile(all_latencies, 50)) if all_latencies else 0,
        "latency_us_p99": float(np.percentile(all_latencies, 99)) if all_latencies else 0,
        "threads": num_threads,
    }


def bench_sequential_put(store, pairs):
    """Sequential PUT (single-threaded baseline)."""
    config = ReplicateConfig()
    config.replica_num = 1
    total_bytes = 0
    errors = 0
    latencies = []

    start = time.perf_counter()
    for key, value in pairs:
        t0 = time.perf_counter()
        rc = store.put(key, value, config)
        latencies.append((time.perf_counter() - t0) * 1e6)
        if rc != 0:
            errors += 1
        total_bytes += len(value)

    elapsed = time.perf_counter() - start
    return {
        "elapsed_s": elapsed,
        "total_bytes": total_bytes,
        "num_ops": len(pairs),
        "errors": errors,
        "throughput_mbps": total_bytes / elapsed / (1024 * 1024),
        "ops_per_sec": len(pairs) / elapsed,
        "latency_us_p50": float(np.percentile(latencies, 50)) if latencies else 0,
        "latency_us_p99": float(np.percentile(latencies, 99)) if latencies else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Mooncake Store KVCache E2E Benchmark (with eviction pressure)")
    parser.add_argument("--master-addr", default="127.0.0.1:50051")
    parser.add_argument("--threads", default="1,2,4,8,16",
                        help="Comma-separated thread counts")
    parser.add_argument("--ops-per-thread", type=int, default=500)
    parser.add_argument("--value-size", type=int, default=128 * 1024,
                        help="Value size in bytes (default 128KB)")
    parser.add_argument("--num-keys", type=int, default=8000,
                        help="Number of unique keys in the pool")
    parser.add_argument("--prefill-ratio", type=float, default=0.90,
                        help="Pre-fill store to this fraction of capacity (0=skip)")
    parser.add_argument("--capacity-mb", type=int, default=1024,
                        help="Store capacity in MB")
    parser.add_argument("--output", default="", help="Output JSON file")
    parser.add_argument("--label", default="", help="Label for this run")
    parser.add_argument("--store-so", default="", help="Path to store.cpython .so file (for A/B testing)")
    args = parser.parse_args()

    thread_counts = [int(t) for t in args.threads.split(",")]
    vs_label = f"{args.value_size//1024}KB" if args.value_size < 1024*1024 else f"{args.value_size//(1024*1024)}MB"

    print(f"=== Mooncake Store E2E Benchmark (Eviction Pressure) ===")
    print(f"Master: {args.master_addr}")
    print(f"Value size: {vs_label} ({args.value_size} bytes)")
    print(f"Key pool: {args.num_keys}")
    print(f"Pre-fill ratio: {args.prefill_ratio*100:.0f}%")
    print(f"Capacity: {args.capacity_mb} MB")
    print(f"Thread counts: {thread_counts}")
    print(f"Ops per thread: {args.ops_per_thread}")
    print()

    # Initialize store client
    store = MooncakeDistributedStore()
    rc = store.setup(
        "localhost",
        "http://127.0.0.1:8080/metadata",
        args.capacity_mb * 1024 * 1024,   # global segment
        64 * 1024 * 1024,                  # local buffer
        "tcp",
        "",
        args.master_addr,
    )
    if rc != 0:
        print(f"ERROR: Store.Setup failed with rc={rc}")
        sys.exit(1)
    print("Store client connected!\n")

    all_results = []

    # =========================================================
    # Phase 1: Pre-fill store to target utilization
    # =========================================================
    if args.prefill_ratio > 0:
        capacity_bytes = args.capacity_mb * 1024 * 1024
        target_bytes = int(capacity_bytes * args.prefill_ratio)
        prefill_keys_needed = target_bytes // args.value_size

        print(f"{'='*60}")
        print(f"  PHASE 1: Pre-fill store to {args.prefill_ratio*100:.0f}% capacity")
        print(f"  Target: {target_bytes/(1024*1024):.0f} MB = {prefill_keys_needed} keys × {vs_label}")
        print(f"{'='*60}")

        config = ReplicateConfig()
        config.replica_num = 1

        prefill_pairs = []
        for i in range(prefill_keys_needed):
            key = f"__fill__{i:06d}"
            value = np.random.bytes(args.value_size)
            prefill_pairs.append((key, value))

        t0 = time.perf_counter()
        for i, (key, value) in enumerate(prefill_pairs):
            rc = store.put(key, value, config)
            if rc != 0:
                print(f"  WARNING: Pre-fill PUT failed at key {i}, rc={rc}")
                break
            if (i + 1) % 1000 == 0:
                elapsed = time.perf_counter() - t0
                progress_pct = (i + 1) / prefill_keys_needed * 100
                rate = (i + 1) / elapsed
                print(f"  Pre-fill: {i+1}/{prefill_keys_needed} keys ({progress_pct:.0f}%), "
                      f"{rate:.0f} ops/s, {elapsed:.1f}s elapsed")

        elapsed = time.perf_counter() - t0
        print(f"  Pre-fill complete: {prefill_keys_needed} keys in {elapsed:.1f}s "
              f"({prefill_keys_needed/elapsed:.0f} ops/s, "
              f"{target_bytes/elapsed/(1024*1024):.1f} MB/s)")
        print()
    else:
        prefill_keys_needed = 0

    # =========================================================
    # Phase 2: Benchmark under eviction pressure
    # =========================================================
    # Use separate key namespace from prefill keys
    bench_pairs = []
    for i in range(args.num_keys):
        key = f"__bench__{i:06d}"
        value = np.random.bytes(args.value_size)
        bench_pairs.append((key, value))
    bench_keys = [k for k, _ in bench_pairs]

    print(f"{'='*60}")
    print(f"  PHASE 2: Concurrent benchmarks under eviction pressure")
    print(f"{'='*60}")

    # Sequential baseline
    if 1 in thread_counts:
        print(f"\n  --- Sequential (single-threaded) ---")
        r = bench_sequential_put(store, bench_pairs[:min(500, len(bench_pairs))])
        r["test"] = "seq_put"
        r["value_size"] = vs_label
        r["label"] = args.label
        all_results.append(r)
        print(f"  SEQ_PUT {vs_label}: {r['ops_per_sec']:.0f} ops/s, "
              f"{r['throughput_mbps']:.1f} MB/s, "
              f"P50={r['latency_us_p50']:.0f}us, P99={r['latency_us_p99']:.0f}us")

    # Concurrent benchmarks
    print(f"\n  --- Concurrent (eviction pressure) ---")
    for nt in thread_counts:
        if nt == 1:
            continue

        # PUT under eviction pressure
        r = bench_concurrent_put_eviction(store, bench_pairs, nt, args.ops_per_thread)
        r["test"] = "conc_put_eviction"
        r["value_size"] = vs_label
        r["label"] = args.label
        all_results.append(r)
        print(f"  CONC_PUT {vs_label} t={nt:2d}: {r['ops_per_sec']:.0f} ops/s, "
              f"{r['throughput_mbps']:.1f} MB/s, "
              f"P50={r['latency_us_p50']:.0f}us, P99={r['latency_us_p99']:.0f}us, "
              f"err={r['errors']}")

        # GET
        r = bench_concurrent_get(store, bench_keys, nt, args.ops_per_thread)
        r["test"] = "conc_get"
        r["value_size"] = vs_label
        r["label"] = args.label
        all_results.append(r)
        print(f"  CONC_GET {vs_label} t={nt:2d}: {r['ops_per_sec']:.0f} ops/s, "
              f"{r['throughput_mbps']:.1f} MB/s, "
              f"P50={r['latency_us_p50']:.0f}us, P99={r['latency_us_p99']:.0f}us, "
              f"hits={r.get('hits', 0)}, misses={r.get('misses', 0)}")

    # Summary table
    print(f"\n{'='*70}")
    print(f"  SUMMARY: Throughput (ops/s) under {args.prefill_ratio*100:.0f}% eviction pressure")
    print(f"{'='*70}")
    header = f"{'Test':<22} {'Size':>8}"
    for nt in thread_counts:
        header += f" {'t=' + str(nt):>12}"
    print(header)
    print("-" * (22 + 8 + 14 * len(thread_counts)))

    for test_prefix, test_name in [("seq_put", "SEQ_PUT"),
                                     ("conc_put_eviction", "CONC_PUT_EVICT"),
                                     ("conc_get", "CONC_GET")]:
        print(f"{test_name:<22} {vs_label:>8}", end="")
        for nt in thread_counts:
            if test_prefix == "seq_put" and nt > 1:
                print(f" {'---':>12}", end="")
                continue
            matching = [r for r in all_results
                       if r["test"] == test_prefix
                       and r.get("threads", 1) == (1 if test_prefix == "seq_put" else nt)]
            if matching:
                print(f" {matching[0]['ops_per_sec']:>11.0f}", end="")
            else:
                print(f" {'N/A':>12}", end="")
        print()

    if args.output:
        with open(args.output, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nResults saved to {args.output}")

    print("\n=== Benchmark Complete ===")


if __name__ == "__main__":
    main()
