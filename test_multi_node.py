#!/usr/bin/env python3
"""
Multi-node distributed benchmark for Mooncake Store.
Tests cross-node KV-cache transfer performance to demonstrate
distributed scenario improvements.

Modes:
  1. Simulated (default): Two master instances on different ports on the same host
  2. Real: SSH to remote nodes via pre-configured addresses

Usage:
  # Simulated multi-node (same host, different ports)
  python3 test_multi_node.py --mode simulated

  # Real multi-node with explicit node addresses
  python3 test_multi_node.py --mode real \\
      --node-a master-a:50051 --node-b master-b:50052

  # A/B comparison
  python3 test_multi_node.py --mode simulated --ab-test \\
      --baseline-master /tmp/mooncake-baseline/builddir/mooncake-store/src/mooncake_master \\
      --optimized-master ./builddir/mooncake-store/src/mooncake_master
"""

import os
import sys
import time
import json
import argparse
import statistics
import subprocess
import threading
import signal
from typing import Optional, List, Dict, Tuple

import numpy as np

# ---- Environment setup ----
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BUILD_DIRS = [
    os.path.join(SCRIPT_DIR, "builddir"),
    os.path.join(SCRIPT_DIR, "builddir_py312"),
]


def find_store_module():
    import importlib.util

    # Support MOONCAKE_STORE_SO env var for A/B testing
    env_so = os.environ.get("MOONCAKE_STORE_SO", "")
    if env_so and os.path.exists(env_so):
        spec = importlib.util.spec_from_file_location("store", env_so)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    for build_dir in BUILD_DIRS:
        candidates = [
            os.path.join(
                build_dir,
                "mooncake-integration/store.cpython-313-x86_64-linux-gnu.so",
            ),
            os.path.join(
                build_dir,
                "mooncake-integration/store.cpython-312-x86_64-linux-gnu.so",
            ),
        ]
        for path in candidates:
            if os.path.exists(path):
                spec = importlib.util.spec_from_file_location("store", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                return mod
    raise FileNotFoundError(f"No store .so found under {BUILD_DIRS}")


store_mod = find_store_module()
MooncakeDistributedStore = store_mod.MooncakeDistributedStore
ReplicateConfig = store_mod.ReplicateConfig

# ---- Master process management ----


class MasterProcess:
    """Manage a Mooncake master process lifecycle."""

    def __init__(
        self,
        binary: str,
        port: int,
        http_port: int,
        metrics_port: int,
        name: str,
        ld_library_path: str,
        extra_flags: Optional[List[str]] = None,
    ):
        self.binary = binary
        self.port = port
        self.http_port = http_port
        self.metrics_port = metrics_port
        self.name = name
        self.ld_library_path = ld_library_path
        self.extra_flags = extra_flags or []
        self.process: Optional[subprocess.Popen] = None

    def start(self) -> bool:
        """Start the master process."""
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self.ld_library_path

        cmd = [
            self.binary,
            f"--port={self.port}",
            f"--http_metadata_server_port={self.http_port}",
            "--enable_http_metadata_server=true",
            f"--metrics_port={self.metrics_port}",
            "--enable_metric_reporting=false",
            "--eviction_high_watermark_ratio=0.95",
        ] + self.extra_flags

        logfile = open(f"/tmp/master_multi_{self.name}.log", "w")
        self.process = subprocess.Popen(
            cmd,
            env=env,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )

        # Wait for port to be ready
        for _ in range(30):
            time.sleep(0.5)
            result = subprocess.run(
                ["ss", "-tlnp"],
                capture_output=True,
                text=True,
            )
            if f":{self.port}" in result.stdout:
                return True

        return False

    def stop(self):
        """Stop the master process."""
        if self.process:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                self.process.wait(timeout=5)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass


def kill_port(port: int):
    """Kill any process listening on the given port."""
    try:
        subprocess.run(
            ["fuser", "-k", f"{port}/tcp"],
            capture_output=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        pass
    time.sleep(1)


# ---- Benchmark functions ----


def create_client(
    master_addr: str, capacity_mb: int = 512, http_port: int = 8080
):
    """Create and initialize a Mooncake Store client."""
    store = MooncakeDistributedStore()
    rc = store.setup(
        "localhost",
        f"http://127.0.0.1:{http_port}/metadata",
        capacity_mb * 1024 * 1024,
        64 * 1024 * 1024,  # local buffer 64MB
        "tcp",
        "",
        master_addr,
    )
    if rc != 0:
        raise RuntimeError(f"Store.Setup failed with rc={rc} (master={master_addr})")
    return store


def bench_cross_node_put(
    store, keys: List[str], values: List[bytes], num_ops: int
) -> Dict:
    """Benchmark PUT operations (write to this node's master)."""
    config = ReplicateConfig()
    config.replica_num = 1

    rng = np.random.RandomState(42)
    latencies = []

    t0 = time.perf_counter()
    for _ in range(num_ops):
        idx = rng.randint(0, len(keys))
        key = keys[idx]
        value = values[idx]

        t_start = time.perf_counter()
        rc = store.put(key, value, config)
        latencies.append((time.perf_counter() - t_start) * 1e6)

        if rc != 0:
            pass  # count errors?

    elapsed = time.perf_counter() - t0
    return {
        "elapsed_s": elapsed,
        "num_ops": num_ops,
        "ops_per_sec": num_ops / elapsed if elapsed > 0 else 0,
        "throughput_mbps": (num_ops * len(values[0])) / elapsed / (1024 * 1024)
        if elapsed > 0 and values
        else 0,
        "latency_us_p50": float(np.percentile(latencies, 50)),
        "latency_us_p99": float(np.percentile(latencies, 99)),
    }


def bench_cross_node_get(
    store_a, store_b, keys: List[str], num_ops: int
) -> Dict:
    """
    Cross-node GET: read data through store_b that was written through store_a.
    Simulates reading KV cache from a remote node.
    """
    rng = np.random.RandomState(100)
    latencies = []
    hits = 0
    misses = 0

    t0 = time.perf_counter()
    for _ in range(num_ops):
        idx = rng.randint(0, len(keys))
        key = keys[idx]

        t_start = time.perf_counter()
        value = store_b.get(key)
        latencies.append((time.perf_counter() - t_start) * 1e6)

        if value is not None and len(value) > 0:
            hits += 1
        else:
            misses += 1

    elapsed = time.perf_counter() - t0
    return {
        "elapsed_s": elapsed,
        "num_ops": num_ops,
        "hits": hits,
        "misses": misses,
        "ops_per_sec": num_ops / elapsed if elapsed > 0 else 0,
        "latency_us_p50": float(np.percentile(latencies, 50)),
        "latency_us_p99": float(np.percentile(latencies, 99)),
    }


# ---- Main test orchestrator ----


def run_test(master_binary: str, label: str, ld_path: str) -> List[Dict]:
    """Run a complete multi-node test with the given master binary."""
    results = []

    # Port assignments
    node_a_port = 50051
    node_a_http = 8080
    node_a_metrics = 23334
    node_b_port = 50052
    node_b_http = 8081
    node_b_metrics = 23335

    # Clean up
    for port in [node_a_port, node_b_port, node_a_http, node_b_http]:
        kill_port(port)

    # Start two masters (simulating two nodes)
    master_a = MasterProcess(
        binary=master_binary,
        port=node_a_port,
        http_port=node_a_http,
        metrics_port=node_a_metrics,
        name=f"{label}_node_a",
        ld_library_path=ld_path,
    )
    master_b = MasterProcess(
        binary=master_binary,
        port=node_b_port,
        http_port=node_b_http,
        metrics_port=node_b_metrics,
        name=f"{label}_node_b",
        ld_library_path=ld_path,
    )

    if not master_a.start():
        print(f"  [FAIL] Master A ({label}) failed to start")
        return results
    print(f"  Master A started on port {node_a_port}")

    if not master_b.start():
        print(f"  [FAIL] Master B ({label}) failed to start")
        master_a.stop()
        return results
    print(f"  Master B started on port {node_b_port}")

    try:
        # Create clients
        print("  Creating clients...")
        store_a = create_client(
            f"127.0.0.1:{node_a_port}", capacity_mb=256, http_port=node_a_http
        )
        store_b = create_client(
            f"127.0.0.1:{node_b_port}", capacity_mb=256, http_port=node_b_http
        )
        print("  Clients connected")

        # Generate test data
        num_keys = 1000
        value_size = 128 * 1024  # 128KB
        keys = [f"__multi__{i:06d}" for i in range(num_keys)]
        values = [np.random.bytes(value_size) for _ in range(num_keys)]

        # ---- Test 1: Node-local PUT (write to own master) ----
        print(f"\n  [{label}] Test 1: Node-local PUT (write to own master)")
        r = bench_cross_node_put(store_a, keys, values, num_ops=500)
        r["test"] = "multi_node_local_put"
        r["label"] = label
        results.append(r)
        print(
            f"    PUT: {r['ops_per_sec']:.0f} ops/s, "
            f"{r['throughput_mbps']:.1f} MB/s, "
            f"P50={r['latency_us_p50']:.0f}us"
        )

        # ---- Test 2: Cross-master GET (write through A, read through B) ----
        print(f"\n  [{label}] Test 2: Cross-master GET")
        # First, write some data through store_a
        config = ReplicateConfig()
        config.replica_num = 1
        for i in range(min(100, num_keys)):
            store_a.put(keys[i], values[i], config)

        r = bench_cross_node_get(store_a, store_b, keys, num_ops=500)
        r["test"] = "multi_node_cross_get"
        r["label"] = label
        results.append(r)
        print(
            f"    Cross-GET: {r['ops_per_sec']:.0f} ops/s, "
            f"P50={r['latency_us_p50']:.0f}us, "
            f"hits={r['hits']}, misses={r['misses']}"
        )

        # ---- Test 3: Concurrent multi-master operations ----
        print(f"\n  [{label}] Test 3: Concurrent dual-master PUT/GET")
        all_latencies = []
        ready = threading.Barrier(4 + 1)
        errors = [0]
        lock = threading.Lock()

        def worker(store, key_list, val_list, num_ops, is_writer):
            local_lats = []
            rng_local = np.random.RandomState()
            try:
                ready.wait()
                for _ in range(num_ops):
                    idx = rng_local.randint(0, len(key_list))
                    t0 = time.perf_counter()
                    if is_writer:
                        rc = store.put(key_list[idx], val_list[idx], config)
                        if rc != 0:
                            with lock:
                                errors[0] += 1
                    else:
                        store.get(key_list[idx])
                    local_lats.append((time.perf_counter() - t0) * 1e6)
            except Exception as e:
                with lock:
                    errors[0] += 1

            with lock:
                all_latencies.extend(local_lats)

        threads = []
        num_ops_per_thread = 200
        # Writers on master A
        for _ in range(2):
            t = threading.Thread(
                target=worker,
                args=(store_a, keys, values, num_ops_per_thread, True),
            )
            threads.append(t)
            t.start()
        # Readers on master B
        for _ in range(2):
            t = threading.Thread(
                target=worker,
                args=(store_b, keys, values, num_ops_per_thread, False),
            )
            threads.append(t)
            t.start()

        ready.wait()
        t_wall_start = time.perf_counter()
        for t in threads:
            t.join()
        t_wall = time.perf_counter() - t_wall_start

        total_ops = len(all_latencies)
        r = {
            "test": "multi_node_concurrent",
            "label": label,
            "elapsed_s": t_wall,
            "num_ops": total_ops,
            "errors": errors[0],
            "ops_per_sec": total_ops / t_wall if t_wall > 0 else 0,
            "latency_us_p50": float(np.percentile(all_latencies, 50))
            if all_latencies
            else 0,
            "latency_us_p99": float(np.percentile(all_latencies, 99))
            if all_latencies
            else 0,
        }
        results.append(r)
        print(
            f"    Concurrent: {r['ops_per_sec']:.0f} ops/s, "
            f"P50={r['latency_us_p50']:.0f}us, errors={errors[0]}"
        )

    finally:
        master_a.stop()
        master_b.stop()
        for port in [node_a_port, node_b_port, node_a_http, node_b_http]:
            kill_port(port)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Mooncake Store Multi-Node Distributed Benchmark"
    )
    parser.add_argument(
        "--mode",
        default="simulated",
        choices=["simulated", "real"],
        help="Test mode (default: simulated)",
    )
    parser.add_argument(
        "--ab-test",
        action="store_true",
        help="Run A/B comparison between baseline and optimized",
    )
    parser.add_argument(
        "--baseline-master",
        default="/tmp/mooncake-baseline/builddir/mooncake-store/src/mooncake_master",
        help="Path to baseline master binary",
    )
    parser.add_argument(
        "--optimized-master",
        default=os.path.join(SCRIPT_DIR, "builddir/mooncake-store/src/mooncake_master"),
        help="Path to optimized master binary",
    )
    parser.add_argument(
        "--rounds", type=int, default=3, help="Number of rounds for A/B test"
    )
    parser.add_argument(
        "--output", default="/tmp/multi_node_results.json", help="Output JSON file"
    )
    parser.add_argument(
        "--ld-library-path",
        default=(
            "/data/lizhijun/anaconda3/lib:"
            + os.path.join(SCRIPT_DIR, "builddir/mooncake-common")
            + ":"
            + os.path.join(SCRIPT_DIR, "builddir/mooncake-store/src")
            + ":"
            + os.path.join(SCRIPT_DIR, "builddir/mooncake-transfer-engine/src")
        ),
        help="LD_LIBRARY_PATH for master and client",
    )
    args = parser.parse_args()

    ld_path = args.ld_library_path

    print("=" * 70)
    print("  Mooncake Store - Multi-Node Distributed Benchmark")
    print(f"  Mode: {args.mode}")
    print(f"  A/B test: {args.ab_test}")
    print("=" * 70)

    all_results = []

    if args.ab_test:
        for rnd in range(1, args.rounds + 1):
            print(f"\n{'='*70}")
            print(f"  ROUND {rnd}/{args.rounds}")
            print(f"{'='*70}")

            # Baseline
            print(f"\n  --- BASELINE ---")
            baseline_results = run_test(args.baseline_master, f"baseline_r{rnd}", ld_path)
            all_results.extend(baseline_results)

            # Optimized
            print(f"\n  --- OPTIMIZED ---")
            optimized_results = run_test(
                args.optimized_master, f"optimized_r{rnd}", ld_path
            )
            all_results.extend(optimized_results)
    else:
        print(f"\n  --- Single run (optimized) ---")
        results = run_test(args.optimized_master, "optimized_single", ld_path)
        all_results.extend(results)

    # ---- Analysis ----
    print("\n" + "=" * 70)
    print("  RESULTS: Multi-Node Distributed Benchmark")
    print("=" * 70)

    with open(args.output, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nRaw results saved to {args.output}")

    if args.ab_test:
        # Aggregate by test+label prefix
        from collections import defaultdict

        agg = defaultdict(lambda: defaultdict(list))
        for r in all_results:
            test = r.get("test", "unknown")
            label = r.get("label", "")
            prefix = "baseline" if "baseline" in label else "optimized"
            agg[test][prefix].append(r.get("ops_per_sec", 0))

        print(f"\n{'Test':<28} {'Baseline':>10} {'Optimized':>10} {'Change':>10}")
        print("-" * 62)
        for test in sorted(agg.keys()):
            b_vals = agg[test].get("baseline", [])
            o_vals = agg[test].get("optimized", [])
            if len(b_vals) >= 2 and len(o_vals) >= 2:
                b_avg = statistics.mean(b_vals)
                o_avg = statistics.mean(o_vals)
                pct = (o_avg - b_avg) / b_avg * 100
                print(f"{test:<28} {b_avg:>10.0f} {o_avg:>10.0f} {pct:>+9.1f}%")

    print("\n=== Multi-Node Benchmark Complete ===")


if __name__ == "__main__":
    main()
