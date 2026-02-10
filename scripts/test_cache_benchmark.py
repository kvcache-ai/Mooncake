#!/usr/bin/env python3
"""
Cache-on-Get Benchmark Test

Usage:
  On writer machine:
    python test_cache_benchmark.py --role writer

  On reader machine:
    python test_cache_benchmark.py --role reader

Environment variables (set on both machines):
  MOONCAKE_MASTER=<master_ip>:50051
  MOONCAKE_LOCAL_HOSTNAME=<this_machine_ip>
  MOONCAKE_PROTOCOL=tcp          # or rdma
  MOONCAKE_DEVICE=""             # RDMA device if needed
  MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE
  MOONCAKE_GLOBAL_SEGMENT_SIZE=16gb
  MOONCAKE_LOCAL_BUFFER_SIZE=8gb

Caching is async (fire-and-forget): the first cache=True call triggers
background caching and proceeds with remote transfer at normal speed.
Subsequent calls find the local replica and read from local memory.
"""

import argparse
import ctypes
import os
import sys
import time

from mooncake.store import MooncakeDistributedStore
from mooncake.mooncake_config import MooncakeConfig

# Key prefixes — each benchmark group uses its own key set to avoid
# cross-contamination (e.g., group A caching data that group B reads).
KEY_PREFIXES = {
    "get_buffer_nocache":       "bench_gbn_",
    "get_buffer_cache":         "bench_gbc_",
    "get_into_nocache":         "bench_gin_",
    "get_into_cache":           "bench_gic_",
    "batch_get_buffer_nocache": "bench_bgbn_",
    "batch_get_buffer_cache":   "bench_bgbc_",
    "batch_get_into_nocache":   "bench_bgin_",
    "batch_get_into_cache":     "bench_bgic_",
}


def make_keys(prefix, num_keys):
    return [f"{prefix}{i}" for i in range(num_keys)]


def create_store():
    """Create and initialize a MooncakeDistributedStore from env config."""
    config = MooncakeConfig.load_from_env()
    store = MooncakeDistributedStore()
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name or "",
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup store, error code: {rc}")
    print(f"Store connected: hostname={config.local_hostname}, "
          f"master={config.master_server_address}, protocol={config.protocol}")
    return store


def run_writer(store, args):
    """Writer: put test data into the store (separate key set per group)."""
    size_bytes = int(args.size_mb * 1024 * 1024)
    num_keys = args.num_keys

    all_prefixes = list(KEY_PREFIXES.values())
    total_keys = num_keys * len(all_prefixes)
    print(f"\n=== Writer: putting {total_keys} keys "
          f"({num_keys} x {len(all_prefixes)} groups), "
          f"{args.size_mb} MB each ===")

    for prefix in all_prefixes:
        keys = make_keys(prefix, num_keys)
        for key in keys:
            data = os.urandom(size_bytes)
            rc = store.put(key, data)
            if rc != 0:
                print(f"  put({key}) failed with rc={rc}")
                return
        print(f"  {prefix}* : {num_keys} keys OK")

    print(f"\nWriter done. {total_keys} keys written.")
    print("Press Ctrl+C to exit (keep running so data stays alive)...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nWriter exiting.")


def bench_get_buffer(store, keys, cache):
    """Benchmark get_buffer for all keys, return total time in seconds."""
    # Warmup
    _ = store.get_buffer(keys[0], cache=False)

    start = time.perf_counter()
    for key in keys:
        buf = store.get_buffer(key, cache=cache)
        if buf is None:
            print(f"  ERROR: get_buffer({key}, cache={cache}) returned None")
            return -1
    elapsed = time.perf_counter() - start
    return elapsed


def bench_get_into(store, keys, size_bytes, cache):
    """Benchmark get_into for all keys using pre-registered buffer."""
    num_keys = len(keys)
    buf_size = int(size_bytes)
    total_size = buf_size * num_keys

    large_buf = (ctypes.c_ubyte * total_size)()
    large_ptr = ctypes.addressof(large_buf)
    rc = store.register_buffer(large_ptr, total_size)
    if rc != 0:
        print(f"  ERROR: register_buffer failed with rc={rc}")
        return -1

    # Warmup
    _ = store.get_into(keys[0], large_ptr, buf_size, cache=False)

    start = time.perf_counter()
    for i, key in enumerate(keys):
        ptr = large_ptr + i * buf_size
        nbytes = store.get_into(key, ptr, buf_size, cache=cache)
        if nbytes < 0:
            print(f"  ERROR: get_into({key}, cache={cache}) returned {nbytes}")
            store.unregister_buffer(large_ptr)
            return -1
    elapsed = time.perf_counter() - start

    store.unregister_buffer(large_ptr)
    return elapsed


def bench_batch_get_buffer(store, keys, cache):
    """Benchmark batch_get_buffer for all keys in one call, return total time."""
    # Warmup
    _ = store.batch_get_buffer(keys[:1], cache=False)

    start = time.perf_counter()
    results = store.batch_get_buffer(keys, cache=cache)
    elapsed = time.perf_counter() - start
    if results is None or len(results) != len(keys):
        print(f"  ERROR: batch_get_buffer(cache={cache}) returned {len(results) if results else 0}/{len(keys)} results")
        return -1
    for i, buf in enumerate(results):
        if buf is None:
            print(f"  ERROR: batch_get_buffer result[{i}] ({keys[i]}) is None")
            return -1
    return elapsed


def bench_batch_get_into(store, keys, size_bytes, cache):
    """Benchmark batch_get_into for all keys in one call using pre-registered buffer."""
    num_keys = len(keys)
    buf_size = int(size_bytes)
    total_size = buf_size * num_keys

    large_buf = (ctypes.c_ubyte * total_size)()
    large_ptr = ctypes.addressof(large_buf)
    rc = store.register_buffer(large_ptr, total_size)
    if rc != 0:
        print(f"  ERROR: register_buffer failed with rc={rc}")
        return -1

    buffer_ptrs = [large_ptr + i * buf_size for i in range(num_keys)]
    sizes = [buf_size] * num_keys

    # Warmup
    _ = store.batch_get_into(keys[:1], buffer_ptrs[:1], sizes[:1], cache=False)

    start = time.perf_counter()
    lengths = store.batch_get_into(keys, buffer_ptrs, sizes, cache=cache)
    elapsed = time.perf_counter() - start

    if lengths is None or len(lengths) != num_keys:
        print(f"  ERROR: batch_get_into(cache={cache}) returned unexpected result")
        store.unregister_buffer(large_ptr)
        return -1
    for i, nbytes in enumerate(lengths):
        if nbytes < 0:
            print(f"  ERROR: batch_get_into result[{i}] ({keys[i]}) returned {nbytes}")
            store.unregister_buffer(large_ptr)
            return -1

    store.unregister_buffer(large_ptr)
    return elapsed


def run_reader(store, args):
    """Reader: benchmark get with and without cache using separate key sets."""
    size_bytes = args.size_mb * 1024 * 1024
    num_keys = args.num_keys
    rounds = args.rounds
    total_mb = args.size_mb * num_keys

    print(f"\n=== Reader: benchmarking {num_keys} keys, "
          f"{args.size_mb} MB each, {rounds} rounds ===")

    # Verify all key sets exist
    for group, prefix in KEY_PREFIXES.items():
        keys = make_keys(prefix, num_keys)
        buf = store.get_buffer(keys[0], cache=False)
        if buf is None:
            print(f"  ERROR: key '{keys[0]}' not found. Is the writer running?")
            return
    print(f"  All key sets verified.\n")

    # --- Benchmark 1: get_buffer without cache (baseline) ---
    keys = make_keys(KEY_PREFIXES["get_buffer_nocache"], num_keys)
    print(f"--- get_buffer (cache=False) x {rounds} rounds ---")
    for r in range(rounds):
        t = bench_get_buffer(store, keys, cache=False)
        bw = total_mb / t if t > 0 else 0
        print(f"  Round {r+1}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 2: get_buffer with cache ---
    # Round 1: triggers async caching (same speed as no-cache)
    # Round 2: caching may still be in progress (background)
    # Round 3+: local replica available, should be faster
    keys = make_keys(KEY_PREFIXES["get_buffer_cache"], num_keys)
    print(f"\n--- get_buffer (cache=True) x {rounds} rounds ---")
    print("  (Round 1 triggers async caching; later rounds read from local)")
    for r in range(rounds):
        t = bench_get_buffer(store, keys, cache=True)
        bw = total_mb / t if t > 0 else 0
        if r == 0:
            tag = " [async caching triggered]"
        else:
            tag = " [local]"
        print(f"  Round {r+1}{tag}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 3: get_into without cache (baseline) ---
    keys = make_keys(KEY_PREFIXES["get_into_nocache"], num_keys)
    print(f"\n--- get_into (cache=False) x {rounds} rounds ---")
    for r in range(rounds):
        t = bench_get_into(store, keys, size_bytes, cache=False)
        bw = total_mb / t if t > 0 else 0
        print(f"  Round {r+1}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 4: get_into with cache ---
    keys = make_keys(KEY_PREFIXES["get_into_cache"], num_keys)
    print(f"\n--- get_into (cache=True) x {rounds} rounds ---")
    print("  (Round 1 triggers async caching; later rounds read from local)")
    for r in range(rounds):
        t = bench_get_into(store, keys, size_bytes, cache=True)
        bw = total_mb / t if t > 0 else 0
        if r == 0:
            tag = " [async caching triggered]"
        else:
            tag = " [local]"
        print(f"  Round {r+1}{tag}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 5: batch_get_buffer without cache (baseline) ---
    keys = make_keys(KEY_PREFIXES["batch_get_buffer_nocache"], num_keys)
    print(f"\n--- batch_get_buffer (cache=False) x {rounds} rounds ---")
    for r in range(rounds):
        t = bench_batch_get_buffer(store, keys, cache=False)
        bw = total_mb / t if t > 0 else 0
        print(f"  Round {r+1}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 6: batch_get_buffer with cache ---
    keys = make_keys(KEY_PREFIXES["batch_get_buffer_cache"], num_keys)
    print(f"\n--- batch_get_buffer (cache=True) x {rounds} rounds ---")
    print("  (Round 1 triggers async caching; later rounds read from local)")
    for r in range(rounds):
        t = bench_batch_get_buffer(store, keys, cache=True)
        bw = total_mb / t if t > 0 else 0
        if r == 0:
            tag = " [async caching triggered]"
        else:
            tag = " [local]"
        print(f"  Round {r+1}{tag}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 7: batch_get_into without cache (baseline) ---
    keys = make_keys(KEY_PREFIXES["batch_get_into_nocache"], num_keys)
    print(f"\n--- batch_get_into (cache=False) x {rounds} rounds ---")
    for r in range(rounds):
        t = bench_batch_get_into(store, keys, size_bytes, cache=False)
        bw = total_mb / t if t > 0 else 0
        print(f"  Round {r+1}: {t:.4f}s, {bw:.2f} MB/s")

    # --- Benchmark 8: batch_get_into with cache ---
    keys = make_keys(KEY_PREFIXES["batch_get_into_cache"], num_keys)
    print(f"\n--- batch_get_into (cache=True) x {rounds} rounds ---")
    print("  (Round 1 triggers async caching; later rounds read from local)")
    for r in range(rounds):
        t = bench_batch_get_into(store, keys, size_bytes, cache=True)
        bw = total_mb / t if t > 0 else 0
        if r == 0:
            tag = " [async caching triggered]"
        else:
            tag = " [local]"
        print(f"  Round {r+1}{tag}: {t:.4f}s, {bw:.2f} MB/s")

    print("\nReader done.")


def main():
    parser = argparse.ArgumentParser(
        description="Cache-on-Get Benchmark Test")
    parser.add_argument(
        "--role", required=True, choices=["writer", "reader"],
        help="Role: 'writer' puts data, 'reader' benchmarks gets")
    parser.add_argument(
        "--num_keys", type=int, default=10,
        help="Number of keys per group to put/get (default: 10)")
    parser.add_argument(
        "--size_mb", type=float, default=64.0,
        help="Size of each value in MB (default: 64)")
    parser.add_argument(
        "--rounds", type=int, default=5,
        help="Number of benchmark rounds for reader (default: 5)")
    args = parser.parse_args()

    store = create_store()

    try:
        if args.role == "writer":
            run_writer(store, args)
        else:
            run_reader(store, args)
    finally:
        store.close()


if __name__ == "__main__":
    main()
