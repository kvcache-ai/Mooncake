"""
Local Cache Test for Mooncake Store (Two-Machine Setup)

Usage:
  Machine A (writer): python test_local_cache.py --role writer
  Machine B (reader): python test_local_cache.py --role reader

Both machines must connect to the same master server.
The writer puts data on Machine A, the reader on Machine B
fetches with cache=True and verifies cache hits skip
remote transfers.

Environment variables (via MooncakeConfig):
  MOONCAKE_CONFIG_PATH or individual env vars for
  local_hostname, metadata_server, master_server_address, etc.
"""

import ctypes
import os
import sys
import time
import argparse
import numpy as np
from mooncake.store import MooncakeDistributedStore
from mooncake.mooncake_config import MooncakeConfig


# ==========================================
#  Constants
# ==========================================

BARRIER_KEY = "__local_cache_test_barrier__"
DONE_KEY = "__local_cache_test_done__"
POLL_INTERVAL = 0.5   # seconds
POLL_TIMEOUT = 120     # seconds

# Test data keys
SINGLE_KEY = "lc_test_single"
BATCH_KEYS = [f"lc_test_batch_{i}" for i in range(8)]
INTO_KEY = "lc_test_into"
BATCH_INTO_KEYS = [f"lc_test_batch_into_{i}" for i in range(4)]
REMOVE_KEY = "lc_test_remove"
PARTIAL_KEYS = [f"lc_test_partial_{i}" for i in range(4)]

# Data size for benchmark
BENCH_DATA_SIZE = 1 * 1024 * 1024  # 1MB
BENCH_ITERATIONS = 10
BENCH_KEYS = [f"lc_bench_{i}" for i in range(8)]

# Extended benchmark keys for get_into / batch_get_into
BENCH_INTO_KEYS = [f"lc_bench_into_{i}" for i in range(8)]

# Data size sweep
BENCH_SIZES = [4 * 1024, 64 * 1024, 256 * 1024, 1 * 1024 * 1024, 4 * 1024 * 1024]
BENCH_SIZE_KEYS = {sz: f"lc_bench_size_{sz}" for sz in BENCH_SIZES}

# Higher iteration count for percentile accuracy
PERF_ITERATIONS = 100


# ==========================================
#  Helpers
# ==========================================

def create_store():
    """Create and connect to the Mooncake Store."""
    store = MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
    print(f"[{os.getpid()}] Connecting to master at "
          f"{config.master_server_address} "
          f"(local: {config.local_hostname}, "
          f"protocol: {config.protocol})")
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup store, error code: {rc}")
    print("Store connection established.")
    return store


def wait_for_key(store, key, timeout=POLL_TIMEOUT):
    """Poll until a key exists in the store."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if store.is_exist(key):
            return True
        time.sleep(POLL_INTERVAL)
    return False


def wait_for_key_absent(store, key, timeout=POLL_TIMEOUT):
    """Poll until a key no longer exists in the store."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        if not store.is_exist(key):
            return True
        time.sleep(POLL_INTERVAL)
    return False


def make_data(prefix, size=3000):
    """Generate deterministic test data from a prefix string."""
    base = prefix.encode()
    repeats = (size // len(base)) + 1
    return (base * repeats)[:size]


def format_size(n):
    """Pretty-print byte sizes (e.g., '4 KB', '1 MB')."""
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.0f} MB"
    elif n >= 1024:
        return f"{n / 1024:.0f} KB"
    return f"{n} B"


def percentile(data, p):
    """Compute the p-th percentile of a list using numpy."""
    return np.percentile(data, p)


# ==========================================
#  Writer Role (Machine A)
# ==========================================

def run_writer(store):
    """Writer: put test data, signal reader, wait for reader to finish."""
    print("\n===== WRITER: Starting =====")

    # Clean up any leftover data
    store.remove_all()
    time.sleep(1)

    # --- Put test data ---
    all_keys = []

    # Single key
    data = make_data(SINGLE_KEY)
    rc = store.put(SINGLE_KEY, data)
    assert rc == 0, f"put {SINGLE_KEY} failed: {rc}"
    all_keys.append(SINGLE_KEY)
    print(f"  Put {SINGLE_KEY} ({len(data)} bytes)")

    # Batch keys
    for key in BATCH_KEYS:
        data = make_data(key)
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(BATCH_KEYS)} batch keys")

    # Into key
    data = make_data(INTO_KEY)
    rc = store.put(INTO_KEY, data)
    assert rc == 0, f"put {INTO_KEY} failed: {rc}"
    all_keys.append(INTO_KEY)
    print(f"  Put {INTO_KEY}")

    # Batch into keys
    for key in BATCH_INTO_KEYS:
        data = make_data(key)
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(BATCH_INTO_KEYS)} batch_into keys")

    # Remove test key
    data = make_data(REMOVE_KEY)
    rc = store.put(REMOVE_KEY, data)
    assert rc == 0, f"put {REMOVE_KEY} failed: {rc}"
    all_keys.append(REMOVE_KEY)
    print(f"  Put {REMOVE_KEY}")

    # Partial cache keys
    for key in PARTIAL_KEYS:
        data = make_data(key)
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(PARTIAL_KEYS)} partial keys")

    # Benchmark keys (larger data)
    for key in BENCH_KEYS:
        data = b"\xAB" * BENCH_DATA_SIZE
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(BENCH_KEYS)} bench keys ({BENCH_DATA_SIZE} bytes each)")

    # Benchmark into keys (1MB each for get_into/batch_get_into benchmarks)
    for key in BENCH_INTO_KEYS:
        data = b"\xCD" * BENCH_DATA_SIZE
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(BENCH_INTO_KEYS)} bench_into keys ({BENCH_DATA_SIZE} bytes each)")

    # Data size sweep keys
    for sz, key in BENCH_SIZE_KEYS.items():
        data = b"\xEF" * sz
        rc = store.put(key, data)
        assert rc == 0, f"put {key} failed: {rc}"
        all_keys.append(key)
    print(f"  Put {len(BENCH_SIZE_KEYS)} size-sweep keys "
          f"({', '.join(format_size(s) for s in BENCH_SIZES)})")

    # Signal reader that data is ready
    rc = store.put(BARRIER_KEY, b"ready")
    assert rc == 0, "put barrier failed"
    print(f"\n  All data written. Signaled reader via {BARRIER_KEY}.")

    # Wait for reader to finish
    print("  Waiting for reader to finish...")
    if not wait_for_key(store, DONE_KEY):
        print("  ERROR: Timed out waiting for reader!")
        sys.exit(1)

    print("\n===== WRITER: Reader finished. Cleaning up... =====")
    store.remove_all()
    print("===== WRITER: Done =====\n")


# ==========================================
#  Reader Role (Machine B)
# ==========================================

def run_reader(store):
    """Reader: wait for writer, then run all local cache tests."""
    print("\n===== READER: Waiting for writer to put data... =====")
    if not wait_for_key(store, BARRIER_KEY):
        print("  ERROR: Timed out waiting for writer!")
        sys.exit(1)
    print("  Writer data is ready. Starting tests.\n")

    passed = 0
    failed = 0
    errors = []

    def check(name, condition, msg=""):
        nonlocal passed, failed, errors
        if condition:
            passed += 1
        else:
            failed += 1
            errors.append(f"{name}: {msg}")
            print(f"  FAIL {name}: {msg}")

    # ------------------------------------------
    # Test 1: get_buffer without local_cache (backward compat)
    # ------------------------------------------
    print("--- Test 1: get_buffer without local_cache ---")
    expected = make_data(SINGLE_KEY)
    buf = store.get_buffer(SINGLE_KEY)  # default local_cache=False
    check("test_01", buf is not None, "get_buffer returned None")
    if buf is not None:
        check("test_01_data", bytes(buf) == expected, "data mismatch")
    print("  OK")

    # ------------------------------------------
    # Test 2: get_buffer with cache=True (miss then hit)
    # ------------------------------------------
    print("--- Test 2: get_buffer cache miss then hit ---")
    expected = make_data(SINGLE_KEY)

    t0 = time.perf_counter()
    buf1 = store.get_buffer(SINGLE_KEY, cache=True)
    t_miss = time.perf_counter() - t0
    check("test_02_miss", buf1 is not None, "cache miss returned None")
    if buf1 is not None:
        check("test_02_miss_data", bytes(buf1) == expected, "miss data mismatch")

    t0 = time.perf_counter()
    buf2 = store.get_buffer(SINGLE_KEY, cache=True)
    t_hit = time.perf_counter() - t0
    check("test_02_hit", buf2 is not None, "cache hit returned None")
    if buf2 is not None:
        check("test_02_hit_data", bytes(buf2) == expected, "hit data mismatch")

    print(f"  miss={t_miss*1000:.3f}ms, hit={t_hit*1000:.3f}ms  OK")

    # ------------------------------------------
    # Test 3: batch_get_buffer with cache=True
    # ------------------------------------------
    print("--- Test 3: batch_get_buffer cache miss then hit ---")
    expected_batch = [make_data(k) for k in BATCH_KEYS]

    t0 = time.perf_counter()
    bufs1 = store.batch_get_buffer(BATCH_KEYS, cache=True)
    t_miss = time.perf_counter() - t0
    check("test_03_len", len(bufs1) == len(BATCH_KEYS),
          f"expected {len(BATCH_KEYS)}, got {len(bufs1)}")
    for i, buf in enumerate(bufs1):
        check(f"test_03_miss_{i}", buf is not None and bytes(buf) == expected_batch[i],
              f"key {i} data mismatch on miss")

    t0 = time.perf_counter()
    bufs2 = store.batch_get_buffer(BATCH_KEYS, cache=True)
    t_hit = time.perf_counter() - t0
    for i, buf in enumerate(bufs2):
        check(f"test_03_hit_{i}", buf is not None and bytes(buf) == expected_batch[i],
              f"key {i} data mismatch on hit")

    print(f"  miss={t_miss*1000:.3f}ms, hit={t_hit*1000:.3f}ms  OK")

    # ------------------------------------------
    # Test 4: get_into with cache=True
    # ------------------------------------------
    print("--- Test 4: get_into cache miss then hit ---")
    expected = make_data(INTO_KEY)
    buf_size = 64 * 1024 * 1024
    buffer = (ctypes.c_ubyte * buf_size)()
    buffer_ptr = ctypes.addressof(buffer)
    res = store.register_buffer(buffer_ptr, buf_size)
    check("test_04_reg", res == 0, f"register_buffer failed: {res}")

    t0 = time.perf_counter()
    length1 = store.get_into(INTO_KEY, buffer_ptr, buf_size, cache=True)
    t_miss = time.perf_counter() - t0
    check("test_04_miss", length1 > 0, f"get_into miss failed: {length1}")
    if length1 > 0:
        check("test_04_miss_data", bytes(buffer[:length1]) == expected,
              "miss data mismatch")

    # Clear buffer, then cache hit
    ctypes.memset(buffer_ptr, 0, buf_size)

    t0 = time.perf_counter()
    length2 = store.get_into(INTO_KEY, buffer_ptr, buf_size, cache=True)
    t_hit = time.perf_counter() - t0
    check("test_04_hit", length2 > 0, f"get_into hit failed: {length2}")
    if length2 > 0:
        check("test_04_hit_data", bytes(buffer[:length2]) == expected,
              "hit data mismatch")
        check("test_04_len_eq", length1 == length2,
              f"lengths differ: {length1} vs {length2}")

    store.unregister_buffer(buffer_ptr)
    print(f"  miss={t_miss*1000:.3f}ms, hit={t_hit*1000:.3f}ms  OK")

    # ------------------------------------------
    # Test 5: batch_get_into with cache=True
    # ------------------------------------------
    print("--- Test 5: batch_get_into cache miss then hit ---")
    num_keys = len(BATCH_INTO_KEYS)
    expected_into = [make_data(k) for k in BATCH_INTO_KEYS]
    buf_each = 64 * 1024 * 1024
    total_buf = buf_each * num_keys
    large_buf = (ctypes.c_ubyte * total_buf)()
    large_buf_ptr = ctypes.addressof(large_buf)
    res = store.register_buffer(large_buf_ptr, total_buf)
    check("test_05_reg", res == 0, f"register_buffer failed: {res}")

    ptrs = [large_buf_ptr + i * buf_each for i in range(num_keys)]
    sizes = [buf_each] * num_keys

    t0 = time.perf_counter()
    lens1 = store.batch_get_into(BATCH_INTO_KEYS, ptrs, sizes,
                                  cache=True)
    t_miss = time.perf_counter() - t0
    for i in range(num_keys):
        check(f"test_05_miss_{i}", lens1[i] > 0,
              f"batch_get_into miss failed for key {i}")
        if lens1[i] > 0:
            result = bytes((ctypes.c_ubyte * lens1[i]).from_address(ptrs[i]))
            check(f"test_05_miss_data_{i}", result == expected_into[i],
                  f"miss data mismatch for key {i}")

    ctypes.memset(large_buf_ptr, 0, total_buf)

    t0 = time.perf_counter()
    lens2 = store.batch_get_into(BATCH_INTO_KEYS, ptrs, sizes,
                                  cache=True)
    t_hit = time.perf_counter() - t0
    for i in range(num_keys):
        check(f"test_05_hit_{i}", lens2[i] > 0,
              f"batch_get_into hit failed for key {i}")
        if lens2[i] > 0:
            result = bytes((ctypes.c_ubyte * lens2[i]).from_address(ptrs[i]))
            check(f"test_05_hit_data_{i}", result == expected_into[i],
                  f"hit data mismatch for key {i}")

    store.unregister_buffer(large_buf_ptr)
    print(f"  miss={t_miss*1000:.3f}ms, hit={t_hit*1000:.3f}ms  OK")

    # ------------------------------------------
    # Test 6: remove clears local cache
    # ------------------------------------------
    print("--- Test 6: remove clears local cache ---")
    expected = make_data(REMOVE_KEY)
    buf = store.get_buffer(REMOVE_KEY, cache=True)
    check("test_06_cache", buf is not None, "initial get returned None")
    if buf is not None:
        check("test_06_cache_data", bytes(buf) == expected, "data mismatch")

    # Reader removes the key (clears both remote and local cache)
    # Use force=True because the preceding get_buffer acquired a lease
    rc = store.remove(REMOVE_KEY, force=True)
    check("test_06_remove", rc == 0, f"remove failed: {rc}")

    # After remove, get should fail
    buf = store.get_buffer(REMOVE_KEY, cache=True)
    check("test_06_after", buf is None,
          "get_buffer should return None after remove")
    print("  OK")

    # ------------------------------------------
    # Test 7: Partial cache hit in batch
    # ------------------------------------------
    print("--- Test 7: batch partial cache hit ---")
    expected_partial = [make_data(k) for k in PARTIAL_KEYS]

    # Cache only first 2 keys
    for i in range(2):
        buf = store.get_buffer(PARTIAL_KEYS[i], cache=True)
        check(f"test_07_pre_{i}", buf is not None, f"pre-cache failed for {i}")

    # Batch get all 4: first 2 hit cache, last 2 miss
    bufs = store.batch_get_buffer(PARTIAL_KEYS, cache=True)
    check("test_07_len", len(bufs) == len(PARTIAL_KEYS),
          f"expected {len(PARTIAL_KEYS)}, got {len(bufs)}")
    for i, buf in enumerate(bufs):
        check(f"test_07_kv_{i}",
              buf is not None and bytes(buf) == expected_partial[i],
              f"key-value mismatch for key {i}")
    print("  OK")

    # ------------------------------------------
    # Test 8: Cache read even with local_cache=False
    # ------------------------------------------
    print("--- Test 8: cache read with local_cache=False ---")
    # SINGLE_KEY was cached in test 2
    expected = make_data(SINGLE_KEY)
    buf = store.get_buffer(SINGLE_KEY, local_cache=False)
    check("test_08", buf is not None, "get returned None")
    if buf is not None:
        check("test_08_data", bytes(buf) == expected,
              "cached data not found with local_cache=False")
    print("  OK")

    # ------------------------------------------
    # Benchmark: cache miss vs hit latency
    # ------------------------------------------
    print("\n--- Benchmark: get_buffer cache hit speedup ---")
    bench_data = b"\xAB" * BENCH_DATA_SIZE

    # All BENCH_KEYS are already put by writer.
    # First batch get: cache miss (remote transfer)
    miss_times = []
    for key in BENCH_KEYS:
        t0 = time.perf_counter()
        buf = store.get_buffer(key, cache=True)
        miss_times.append(time.perf_counter() - t0)
        assert buf is not None

    # Repeated gets: cache hit (local)
    hit_times = []
    for _ in range(BENCH_ITERATIONS):
        for key in BENCH_KEYS:
            t0 = time.perf_counter()
            buf = store.get_buffer(key, cache=True)
            hit_times.append(time.perf_counter() - t0)

    avg_miss = np.mean(miss_times)
    avg_hit = np.mean(hit_times)
    speedup = avg_miss / avg_hit if avg_hit > 0 else float('inf')
    print(f"  Data size:  {BENCH_DATA_SIZE / 1024:.0f} KB x {len(BENCH_KEYS)} keys")
    print(f"  Avg miss:   {avg_miss*1000:.3f} ms (remote transfer)")
    print(f"  Avg hit:    {avg_hit*1000:.3f} ms (local cache)")
    print(f"  Speedup:    {speedup:.1f}x")

    print("\n--- Benchmark: batch_get_buffer cache hit speedup ---")
    t0 = time.perf_counter()
    bufs = store.batch_get_buffer(BENCH_KEYS, cache=True)
    t_batch_hit = time.perf_counter() - t0
    print(f"  Batch hit ({len(BENCH_KEYS)} keys): {t_batch_hit*1000:.3f} ms")

    # ------------------------------------------
    # Benchmark: get_into cache hit speedup
    # ------------------------------------------
    print("\n--- Benchmark: get_into cache hit speedup ---")
    into_buf_size = 64 * 1024 * 1024
    into_buf = (ctypes.c_ubyte * into_buf_size)()
    into_buf_ptr = ctypes.addressof(into_buf)
    res = store.register_buffer(into_buf_ptr, into_buf_size)
    assert res == 0, f"register_buffer failed: {res}"

    into_miss_times = []
    for key in BENCH_INTO_KEYS:
        t0 = time.perf_counter()
        length = store.get_into(key, into_buf_ptr, into_buf_size, cache=True)
        into_miss_times.append(time.perf_counter() - t0)
        assert length > 0, f"get_into miss failed for {key}"

    into_hit_times = []
    for _ in range(BENCH_ITERATIONS):
        for key in BENCH_INTO_KEYS:
            t0 = time.perf_counter()
            length = store.get_into(key, into_buf_ptr, into_buf_size, cache=True)
            into_hit_times.append(time.perf_counter() - t0)

    avg_into_miss = np.mean(into_miss_times)
    avg_into_hit = np.mean(into_hit_times)
    into_speedup = avg_into_miss / avg_into_hit if avg_into_hit > 0 else float('inf')
    print(f"  Data size:  {BENCH_DATA_SIZE / 1024:.0f} KB x {len(BENCH_INTO_KEYS)} keys")
    print(f"  Avg miss:   {avg_into_miss*1000:.3f} ms (remote transfer)")
    print(f"  Avg hit:    {avg_into_hit*1000:.3f} ms (local cache)")
    print(f"  Speedup:    {into_speedup:.1f}x")

    store.unregister_buffer(into_buf_ptr)

    # ------------------------------------------
    # Benchmark: batch_get_into cache hit speedup
    # ------------------------------------------
    print("\n--- Benchmark: batch_get_into cache hit speedup ---")
    num_bench_into = len(BENCH_INTO_KEYS)
    bgi_each = 64 * 1024 * 1024
    bgi_total = bgi_each * num_bench_into
    bgi_buf = (ctypes.c_ubyte * bgi_total)()
    bgi_buf_ptr = ctypes.addressof(bgi_buf)
    res = store.register_buffer(bgi_buf_ptr, bgi_total)
    assert res == 0, f"register_buffer failed: {res}"

    bgi_ptrs = [bgi_buf_ptr + i * bgi_each for i in range(num_bench_into)]
    bgi_sizes = [bgi_each] * num_bench_into

    # Cache miss (already cached from get_into above, so this is a hit)
    # Force a fresh miss by using a batch call — keys are already cached,
    # so we just measure the batch hit directly.
    bgi_hit_times = []
    for _ in range(BENCH_ITERATIONS):
        t0 = time.perf_counter()
        lens = store.batch_get_into(BENCH_INTO_KEYS, bgi_ptrs, bgi_sizes,
                                     cache=True)
        bgi_hit_times.append(time.perf_counter() - t0)
        for i, l in enumerate(lens):
            assert l > 0, f"batch_get_into hit failed for key {i}"

    avg_bgi_hit = np.mean(bgi_hit_times)
    print(f"  Batch hit ({num_bench_into} keys x {BENCH_DATA_SIZE / 1024:.0f} KB): "
          f"{avg_bgi_hit*1000:.3f} ms avg")

    store.unregister_buffer(bgi_buf_ptr)

    # ------------------------------------------
    # Benchmark: Latency percentiles (get_buffer)
    # ------------------------------------------
    print("\n--- Benchmark: Latency percentiles (get_buffer cache hit) ---")
    pct_times = []
    for _ in range(PERF_ITERATIONS):
        for key in BENCH_KEYS:
            t0 = time.perf_counter()
            buf = store.get_buffer(key, cache=True)
            pct_times.append(time.perf_counter() - t0)

    pct_times_ms = [t * 1000 for t in pct_times]
    print(f"  Samples:  {len(pct_times_ms)}")
    print(f"  Min:      {min(pct_times_ms):.3f} ms")
    print(f"  p50:      {percentile(pct_times_ms, 50):.3f} ms")
    print(f"  p95:      {percentile(pct_times_ms, 95):.3f} ms")
    print(f"  p99:      {percentile(pct_times_ms, 99):.3f} ms")
    print(f"  Max:      {max(pct_times_ms):.3f} ms")

    # ------------------------------------------
    # Benchmark: Data size sweep
    # ------------------------------------------
    print("\n--- Benchmark: Data size sweep (get_buffer) ---")
    print(f"  {'Size':>8s} | {'Avg Miss':>10s} | {'Avg Hit':>10s} | "
          f"{'Speedup':>8s} | {'Throughput':>12s}")
    print(f"  {'-'*8}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*12}")

    sweep_iters = BENCH_ITERATIONS
    for sz in BENCH_SIZES:
        key = BENCH_SIZE_KEYS[sz]

        # Cache miss
        t0 = time.perf_counter()
        buf = store.get_buffer(key, cache=True)
        t_sz_miss = time.perf_counter() - t0
        assert buf is not None, f"size sweep miss failed for {key}"

        # Cache hits
        sz_hit_times = []
        for _ in range(sweep_iters):
            t0 = time.perf_counter()
            buf = store.get_buffer(key, cache=True)
            sz_hit_times.append(time.perf_counter() - t0)

        avg_sz_hit = np.mean(sz_hit_times)
        sz_speedup = t_sz_miss / avg_sz_hit if avg_sz_hit > 0 else float('inf')
        throughput = sz / avg_sz_hit / (1024 ** 3) if avg_sz_hit > 0 else 0
        print(f"  {format_size(sz):>8s} | {t_sz_miss*1000:>9.3f}ms | "
              f"{avg_sz_hit*1000:>9.3f}ms | {sz_speedup:>7.1f}x | "
              f"{throughput:>9.2f} GB/s")

    # ------------------------------------------
    # Benchmark: Data size sweep (get_into)
    # ------------------------------------------
    print("\n--- Benchmark: Data size sweep (get_into) ---")
    print(f"  {'Size':>8s} | {'Avg Miss':>10s} | {'Avg Hit':>10s} | "
          f"{'Speedup':>8s} | {'Throughput':>12s}")
    print(f"  {'-'*8}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*12}")

    sweep_into_buf_size = 64 * 1024 * 1024
    sweep_into_buf = (ctypes.c_ubyte * sweep_into_buf_size)()
    sweep_into_ptr = ctypes.addressof(sweep_into_buf)
    res = store.register_buffer(sweep_into_ptr, sweep_into_buf_size)
    assert res == 0, f"register_buffer failed: {res}"

    for sz in BENCH_SIZES:
        key = BENCH_SIZE_KEYS[sz]

        # Cache miss — keys were already cached by get_buffer sweep above,
        # so get_into will be a cache hit. We still measure the first call
        # as "miss" for get_into path (first get_into call for this key).
        t0 = time.perf_counter()
        length = store.get_into(key, sweep_into_ptr, sweep_into_buf_size,
                                cache=True)
        t_sz_miss = time.perf_counter() - t0
        assert length > 0, f"size sweep get_into failed for {key}"

        # Cache hits
        sz_hit_times = []
        for _ in range(sweep_iters):
            t0 = time.perf_counter()
            length = store.get_into(key, sweep_into_ptr, sweep_into_buf_size,
                                    cache=True)
            sz_hit_times.append(time.perf_counter() - t0)

        avg_sz_hit = np.mean(sz_hit_times)
        sz_speedup = t_sz_miss / avg_sz_hit if avg_sz_hit > 0 else float('inf')
        throughput = sz / avg_sz_hit / (1024 ** 3) if avg_sz_hit > 0 else 0
        print(f"  {format_size(sz):>8s} | {t_sz_miss*1000:>9.3f}ms | "
              f"{avg_sz_hit*1000:>9.3f}ms | {sz_speedup:>7.1f}x | "
              f"{throughput:>9.2f} GB/s")

    store.unregister_buffer(sweep_into_ptr)

    # ------------------------------------------
    # Benchmark: Throughput summary (batch_get_buffer)
    # ------------------------------------------
    print("\n--- Benchmark: Throughput summary (batch_get_buffer cache hit) ---")
    total_data = BENCH_DATA_SIZE * len(BENCH_KEYS)
    tp_times = []
    for _ in range(BENCH_ITERATIONS):
        t0 = time.perf_counter()
        bufs = store.batch_get_buffer(BENCH_KEYS, cache=True)
        tp_times.append(time.perf_counter() - t0)

    avg_tp_time = np.mean(tp_times)
    agg_throughput = total_data / avg_tp_time / (1024 ** 3) if avg_tp_time > 0 else 0
    print(f"  Batch size:   {len(BENCH_KEYS)} keys x {format_size(BENCH_DATA_SIZE)} "
          f"= {format_size(total_data)}")
    print(f"  Avg latency:  {avg_tp_time*1000:.3f} ms")
    print(f"  Throughput:   {agg_throughput:.2f} GB/s")

    # ------------------------------------------
    # Benchmark: Throughput summary (batch_get_into)
    # ------------------------------------------
    print("\n--- Benchmark: Throughput summary (batch_get_into cache hit) ---")
    total_into_data = BENCH_DATA_SIZE * len(BENCH_INTO_KEYS)
    num_bi = len(BENCH_INTO_KEYS)
    bi_each = 64 * 1024 * 1024
    bi_total = bi_each * num_bi
    bi_buf = (ctypes.c_ubyte * bi_total)()
    bi_buf_ptr = ctypes.addressof(bi_buf)
    res = store.register_buffer(bi_buf_ptr, bi_total)
    assert res == 0, f"register_buffer failed: {res}"

    bi_ptrs = [bi_buf_ptr + i * bi_each for i in range(num_bi)]
    bi_sizes = [bi_each] * num_bi

    tp_into_times = []
    for _ in range(BENCH_ITERATIONS):
        t0 = time.perf_counter()
        lens = store.batch_get_into(BENCH_INTO_KEYS, bi_ptrs, bi_sizes,
                                     cache=True)
        tp_into_times.append(time.perf_counter() - t0)

    store.unregister_buffer(bi_buf_ptr)

    avg_tp_into = np.mean(tp_into_times)
    into_throughput = (total_into_data / avg_tp_into / (1024 ** 3)
                       if avg_tp_into > 0 else 0)
    print(f"  Batch size:   {num_bi} keys x {format_size(BENCH_DATA_SIZE)} "
          f"= {format_size(total_into_data)}")
    print(f"  Avg latency:  {avg_tp_into*1000:.3f} ms")
    print(f"  Throughput:   {into_throughput:.2f} GB/s")

    # ------------------------------------------
    # Summary
    # ------------------------------------------
    print(f"\n{'='*50}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    if errors:
        print(f"  Failures:")
        for e in errors:
            print(f"    - {e}")
    print(f"{'='*50}")

    # Signal writer that we're done
    store.put(DONE_KEY, b"done")
    print("\n===== READER: Done =====\n")

    return failed == 0


# ==========================================
#  Main
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mooncake Local Cache Test (Two-Machine)")
    parser.add_argument(
        "--role", type=str, required=True,
        choices=["writer", "reader"],
        help="Role: 'writer' on Machine A (puts data), "
             "'reader' on Machine B (gets data with local_cache)")
    args = parser.parse_args()

    store = create_store()

    try:
        if args.role == "writer":
            run_writer(store)
        else:
            success = run_reader(store)
            if not success:
                sys.exit(1)
    finally:
        store.close()
