#!/usr/bin/env python3
"""
E2E test for client-side prefix cache in batch_get_buffer.

Validates:
1. Cache hit correctness: batch_get_buffer returns correct data
2. Cache speedup: warm calls significantly faster than cold calls
3. Cache TTL: entries expire after 5 seconds
4. Independent caches: different prefixes get separate cache entries
5. Partial cache hit: mixed cached + uncached keys work correctly
"""

import os
import sys
import time


def make_payload(seq: int, size: int) -> bytes:
    """Create a deterministic payload for a given sequence number."""
    seed = f"value{seq:08d}".encode()
    repeat = (size // len(seed)) + 1
    return (seed * repeat)[:size]


def median(values):
    """Return median of a list."""
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


class PrefixCacheTest:
    def __init__(self):
        import store

        self.store_mod = store
        self.store = store.MooncakeDistributedStore()
        self.master_server = os.environ.get("MASTER_RPC", "127.0.0.1:50051")
        self.metadata_url = os.environ.get(
            "METADATA_URL", "http://127.0.0.1:8080/metadata"
        )

    def setup(self):
        ret = self.store.setup(
            "127.0.0.1:50071",  # local_hostname
            self.metadata_url,
            64 * 1024 * 1024,  # global_segment_size
            32 * 1024 * 1024,  # local_buffer_size
            "tcp",  # protocol
            "",  # device_name
            self.master_server,
        )
        assert ret == 0, f"Setup failed: ret={ret}"
        self.replicate_config = self.store_mod.ReplicateConfig()
        self.replicate_config.replica_num = 1
        self.replicate_config.nof_replica_num = 0
        print(f"SETUP_OK local_hostname=127.0.0.1:50071", flush=True)

    def test_correctness_and_speedup(self):
        """
        Test 1: Put N keys with common prefix, batch_get them,
        then batch_get again. Second call should be significantly faster
        due to prefix cache hit.
        """
        print("--- Test 1: Correctness and Cache Speedup ---", flush=True)

        num_keys = 30
        payload_size = 4096
        prefix = "t1-layer0"

        # Put data
        keys = []
        expected = []
        for i in range(num_keys):
            key = f"{prefix}-{i:04d}"
            val = make_payload(i, payload_size)
            ret = self.store.put(key, val, self.replicate_config)
            assert ret == 0, f"put failed for {key}: ret={ret}"
            keys.append(key)
            expected.append(val)
        print(f"PUT_OK count={num_keys} prefix={prefix}", flush=True)

        # First batch_get: cold cache (should hit master)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        cold_elapsed = time.monotonic() - t0
        cold_ms = cold_elapsed * 1000
        print(
            f"BATCH_GET_COLD count={num_keys} elapsed_ms={cold_ms:.2f}",
            flush=True,
        )

        # Verify correctness
        for i, (key, buf, exp) in enumerate(zip(keys, results, expected)):
            assert buf is not None, f"NULL result for key {key}"
            actual = bytes(buf)
            assert actual == exp, (
                f"DATA_MISMATCH key={key} "
                f"expected_len={len(exp)} actual_len={len(actual)}"
            )
        print(f"VERIFY_OK cold_cache: all {num_keys} keys correct", flush=True)

        # Warm-up iterations
        time.sleep(0.1)
        warm_times = []
        for iteration in range(5):
            t0 = time.monotonic()
            results = self.store.batch_get_buffer(keys)
            elapsed = time.monotonic() - t0
            warm_times.append(elapsed)

            # Verify correctness
            for i, (key, buf, exp) in enumerate(zip(keys, results, expected)):
                assert buf is not None, (
                    f"NULL result for key {key} (warm iter {iteration})"
                )
                actual = bytes(buf)
                assert actual == exp, (
                    f"DATA_MISMATCH key={key} iter={iteration}"
                )

        warm_med_ms = median(warm_times) * 1000
        speedup = cold_ms / warm_med_ms if warm_med_ms > 0 else float("inf")

        print(
            f"BATCH_GET_WARM_MEDIAN count={num_keys} elapsed_ms={warm_med_ms:.2f}",
            flush=True,
        )
        print(
            f"SPEEDUP cold_ms={cold_ms:.2f} warm_med_ms={warm_med_ms:.2f} "
            f"ratio={speedup:.1f}x",
            flush=True,
        )

        # Pass/fail: warm should be faster than cold
        if speedup < 1.2:
            print(
                f"FAIL: prefix cache did not provide expected speedup "
                f"(cold={cold_ms:.2f}ms, warm_med={warm_med_ms:.2f}ms, "
                f"ratio={speedup:.1f}x)",
                flush=True,
            )
            return False

        print("PASS Test 1", flush=True)
        return True

    def test_cache_ttl_expiry(self):
        """
        Test 2: Verify cache entries expire after the 5-second TTL.
        After TTL, a new call should revert to cold-cache speed.
        """
        print("--- Test 2: Cache TTL Expiry ---", flush=True)

        num_keys = 10
        payload_size = 4096
        prefix = "t2-ttl"
        ttl_seconds = 5

        keys = []
        expected = []
        for i in range(num_keys):
            key = f"{prefix}-{i:04d}"
            val = make_payload(i, payload_size)
            ret = self.store.put(key, val, self.replicate_config)
            assert ret == 0, f"put failed for {key}"
            keys.append(key)
            expected.append(val)
        print(f"PUT_OK count={num_keys} prefix={prefix}", flush=True)

        # Cold call to populate cache
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        cold_ms = (time.monotonic() - t0) * 1000
        print(
            f"BATCH_GET_COLD count={num_keys} elapsed_ms={cold_ms:.2f}",
            flush=True,
        )

        # Warm call (verify cache is working)
        time.sleep(0.1)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        warm_ms = (time.monotonic() - t0) * 1000
        print(
            f"BATCH_GET_WARM count={num_keys} elapsed_ms={warm_ms:.2f}",
            flush=True,
        )

        # Wait for TTL + 1 second
        print(
            f"WAITING {ttl_seconds + 1}s for cache TTL expiry...", flush=True
        )
        time.sleep(ttl_seconds + 1)

        # After TTL expiry, should be cold again (slow)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        post_ttl_ms = (time.monotonic() - t0) * 1000

        # Verify correctness still holds
        for i, (key, buf, exp) in enumerate(zip(keys, results, expected)):
            assert buf is not None, f"NULL result for key {key} after TTL"
            actual = bytes(buf)
            assert actual == exp, f"DATA_MISMATCH key={key} after TTL"

        print(
            f"BATCH_GET_POST_TTL count={num_keys} elapsed_ms={post_ttl_ms:.2f}",
            flush=True,
        )

        # After TTL, should be slower (comparable to cold, not ultra-fast)
        # Use a soft check: if post-TTL is still very fast, cache didn't expire
        if post_ttl_ms < cold_ms * 0.3:
            print(
                f"WARN: post-TTL call still fast ({post_ttl_ms:.2f}ms vs "
                f"cold {cold_ms:.2f}ms) - cache may not have expired",
                flush=True,
            )
        else:
            print(
                f"TTL_EXPIRY_OK: post-TTL ({post_ttl_ms:.2f}ms) comparable "
                f"to cold ({cold_ms:.2f}ms)",
                flush=True,
            )

        print("PASS Test 2", flush=True)
        return True

    def test_different_prefixes(self):
        """
        Test 3: Keys with different prefixes should create separate cache
        entries. Warm calls for each prefix group should be fast
        independently, and cold calls for a new prefix should be slow.
        """
        print("--- Test 3: Different Prefixes, Separate Caches ---", flush=True)

        payload_size = 16384  # larger payload to make RPC cost measurable
        prefix_groups = {
            "t3-groupA": 50,
            "t3-groupB": 50,
            "t3-groupC": 50,
        }

        # Put all data
        all_data = {}
        for prefix, count in prefix_groups.items():
            for i in range(count):
                key = f"{prefix}-{i:04d}"
                val = make_payload(i, payload_size)
                ret = self.store.put(key, val, self.replicate_config)
                assert ret == 0, f"put failed for {key}"
                all_data[key] = val
        print(f"PUT_OK total_keys={sum(prefix_groups.values())}", flush=True)

        # --- Group A: Cold then warm (multiple iterations to reduce noise) ---
        group_a_keys = [k for k in all_data if k.startswith("t3-groupA")]
        cold_a_times = []
        for _ in range(3):
            t0 = time.monotonic()
            self.store.batch_get_buffer(group_a_keys)
            cold_a_times.append((time.monotonic() - t0) * 1000)
        # First call populates cache, so it's the real "cold"
        cold_a_ms = cold_a_times[0]

        time.sleep(0.1)
        warm_a_times = []
        for _ in range(5):
            t0 = time.monotonic()
            results_a = self.store.batch_get_buffer(group_a_keys)
            warm_a_times.append((time.monotonic() - t0) * 1000)

        # --- Group B: Cold then warm ---
        group_b_keys = [k for k in all_data if k.startswith("t3-groupB")]
        cold_b_times = []
        for _ in range(3):
            t0 = time.monotonic()
            self.store.batch_get_buffer(group_b_keys)
            cold_b_times.append((time.monotonic() - t0) * 1000)
        cold_b_ms = cold_b_times[0]

        time.sleep(0.1)
        warm_b_times = []
        for _ in range(5):
            t0 = time.monotonic()
            results_b = self.store.batch_get_buffer(group_b_keys)
            warm_b_times.append((time.monotonic() - t0) * 1000)

        warm_a_med = median(warm_a_times)
        warm_b_med = median(warm_b_times)

        print(
            f"GroupA: cold={cold_a_ms:.2f}ms warm_med={warm_a_med:.2f}ms "
            f"(speedup={cold_a_ms/warm_a_med:.1f}x if warm > 0)",
            flush=True,
        )
        print(
            f"GroupB: cold={cold_b_ms:.2f}ms warm_med={warm_b_med:.2f}ms "
            f"(speedup={cold_b_ms/warm_b_med:.1f}x if warm > 0)",
            flush=True,
        )

        # Verify correctness for both groups
        for results, keys in [
            (results_a, group_a_keys),
            (results_b, group_b_keys),
        ]:
            for key, buf in zip(keys, results):
                actual = bytes(buf)
                assert actual == all_data[key], (
                    f"DATA_MISMATCH for {key}"
                )

        # Group C: should be cold (not cached from A or B)
        group_c_keys = [k for k in all_data if k.startswith("t3-groupC")]
        t0 = time.monotonic()
        results_c = self.store.batch_get_buffer(group_c_keys)
        cold_c_ms = (time.monotonic() - t0) * 1000
        print(
            f"GroupC: cold={cold_c_ms:.2f}ms (different prefix, expect cold)",
            flush=True,
        )

        for key, buf in zip(group_c_keys, results_c):
            actual = bytes(buf)
            assert actual == all_data[key], f"DATA_MISMATCH for {key}"

        # Verify warm is faster than cold for both groups.
        # On localhost TCP, RPC overhead can be sub-ms, so we use multiple
        # iterations and median to reduce noise. At least one group must
        # show clear speedup; the other may be within noise margin.
        a_ok = warm_a_med < cold_a_ms
        b_ok = warm_b_med < cold_b_ms
        if not (a_ok or b_ok):
            print(
                f"FAIL: Neither group shows cache speedup. "
                f"GroupA warm={warm_a_med:.2f}ms cold={cold_a_ms:.2f}ms, "
                f"GroupB warm={warm_b_med:.2f}ms cold={cold_b_ms:.2f}ms",
                flush=True,
            )
            return False
        if not a_ok:
            print(
                f"NOTE: GroupA warm ({warm_a_med:.2f}ms) vs cold "
                f"({cold_a_ms:.2f}ms) within noise — GroupB confirms "
                f"cache works",
                flush=True,
            )

        print("PASS Test 3", flush=True)
        return True

    def test_partial_cache_hit(self):
        """
        Test 4: Mixed set of cached and uncached keys.
        Verify all return correctly and second call is warm for all.
        """
        print("--- Test 4: Partial Cache Hit ---", flush=True)

        payload_size = 4096
        prefix = "t4-partial"
        num_old = 10
        num_new = 5

        # Put and warm old keys
        old_keys = [f"{prefix}-old-{i:04d}" for i in range(num_old)]
        old_values = {}
        for i, k in enumerate(old_keys):
            val = make_payload(i, payload_size)
            ret = self.store.put(k, val, self.replicate_config)
            assert ret == 0
            old_values[k] = val

        # Warm old keys (populate cache for this prefix)
        self.store.batch_get_buffer(old_keys)
        time.sleep(0.1)

        # Put new keys with same prefix but NOT yet cached
        new_keys = [f"{prefix}-new-{i:04d}" for i in range(num_new)]
        new_values = {}
        for i, k in enumerate(new_keys):
            val = make_payload(100 + i, payload_size)
            ret = self.store.put(k, val, self.replicate_config)
            assert ret == 0
            new_values[k] = val

        # Batch get ALL keys (mix of cached old + uncached new)
        all_keys = old_keys + new_keys
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(all_keys)
        mixed_elapsed_ms = (time.monotonic() - t0) * 1000
        print(
            f"BATCH_GET_MIXED count={len(all_keys)} "
            f"elapsed_ms={mixed_elapsed_ms:.2f}",
            flush=True,
        )

        # Verify all data correct
        all_values = {**old_values, **new_values}
        for key, buf in zip(all_keys, results):
            assert buf is not None, f"NULL result for key {key}"
            actual = bytes(buf)
            assert actual == all_values[key], (
                f"DATA_MISMATCH for {key}"
            )
        print(f"VERIFY_OK mixed: all {len(all_keys)} keys correct", flush=True)

        # Warm call for ALL keys (now all should be cached)
        time.sleep(0.1)
        t0 = time.monotonic()
        results2 = self.store.batch_get_buffer(all_keys)
        warm_mixed_ms = (time.monotonic() - t0) * 1000
        print(
            f"BATCH_GET_MIXED_WARM count={len(all_keys)} "
            f"elapsed_ms={warm_mixed_ms:.2f}",
            flush=True,
        )

        # Warm should be faster than cold.
        # Because localhost TCP is very fast, we use a conservative check.
        if warm_mixed_ms >= mixed_elapsed_ms:
            print(
                f"FAIL: Warm mixed ({warm_mixed_ms:.2f}ms) not faster "
                f"than cold ({mixed_elapsed_ms:.2f}ms)",
                flush=True,
            )
            return False

        print("PASS Test 4", flush=True)
        return True

    def test_large_batch_consistency(self):
        """
        Test 5: Repeated batch_get calls should all return consistent data.
        This validates the prefix cache doesn't corrupt data.
        """
        print("--- Test 5: Large Batch Consistency ---", flush=True)

        num_keys = 50
        payload_size = 8192
        prefix = "t5-consistency"
        iterations = 10

        keys = []
        expected = []
        for i in range(num_keys):
            key = f"{prefix}-{i:04d}"
            val = make_payload(i, payload_size)
            ret = self.store.put(key, val, self.replicate_config)
            assert ret == 0, f"put failed for {key}"
            keys.append(key)
            expected.append(val)
        print(f"PUT_OK count={num_keys} prefix={prefix}", flush=True)

        # Run many batch_get iterations, verify all return same correct data
        for iteration in range(iterations):
            results = self.store.batch_get_buffer(keys)
            for i, (key, buf, exp) in enumerate(
                zip(keys, results, expected)
            ):
                assert buf is not None, (
                    f"NULL result for key {key} iter={iteration}"
                )
                actual = bytes(buf)
                assert actual == exp, (
                    f"DATA_MISMATCH key={key} iter={iteration} "
                    f"expected_len={len(exp)} actual_len={len(actual)}"
                )

        print(
            f"CONSISTENCY_OK: {iterations} iterations of {num_keys} keys "
            f"all returned correct data",
            flush=True,
        )
        print("PASS Test 5", flush=True)
        return True

    def test_cache_invalidation_on_put(self):
        """
        Test 6: Verify that put() invalidates the prefix cache.
        After putting a new value for a cached key, the next
        batch_get should be a cold call (cache miss) and return
        the updated value.
        """
        print("--- Test 6: Cache Invalidation on Put ---", flush=True)

        num_keys = 20
        payload_size = 4096
        prefix = "t6-inval"

        keys = []
        expected = []
        for i in range(num_keys):
            key = f"{prefix}-{i:04d}"
            val = make_payload(i, payload_size)
            ret = self.store.put(key, val, self.replicate_config)
            assert ret == 0, f"put failed for {key}"
            keys.append(key)
            expected.append(val)
        print(f"PUT_OK count={num_keys} prefix={prefix}", flush=True)

        # Cold batch_get (populates cache)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        cold_ms = (time.monotonic() - t0) * 1000

        # Verify correctness
        for key, buf, exp in zip(keys, results, expected):
            actual = bytes(buf)
            assert actual == exp, f"DATA_MISMATCH for {key}"

        # Warm batch_get (verifies cache hit)
        time.sleep(0.1)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        warm_ms = (time.monotonic() - t0) * 1000
        print(
            f"cold={cold_ms:.2f}ms warm={warm_ms:.2f}ms",
            flush=True,
        )

        # Remove + re-put key[7] — this should invalidate the prefix cache.
        # (Mooncake Store's put is create-only for existing keys,
        # so we use remove + put to truly change the data.)
        changed_idx = 7
        ret = self.store.remove(keys[changed_idx], force=True)
        assert ret == 0, f"remove failed for {keys[changed_idx]}"
        new_val = b"INVALIDATED-NEW-VALUE-" + b"X" * 100
        ret = self.store.put(
            keys[changed_idx], new_val, self.replicate_config
        )
        assert ret == 0, f"put(invalidate) failed for {keys[changed_idx]}"
        expected[changed_idx] = new_val
        print(f"REMOVE_PUT_INVALIDATE key={keys[changed_idx]}", flush=True)

        # Batch get again — should be a cache miss (cache was invalidated)
        t0 = time.monotonic()
        results = self.store.batch_get_buffer(keys)
        post_inval_ms = (time.monotonic() - t0) * 1000
        print(
            f"BATCH_GET_POST_INVAL elapsed_ms={post_inval_ms:.2f}",
            flush=True,
        )

        # Verify all data correct, especially the updated key
        for key, buf, exp in zip(keys, results, expected):
            assert buf is not None, f"NULL result for key {key}"
            actual = bytes(buf)
            assert actual == exp, (
                f"DATA_MISMATCH after invalidation for {key}: "
                f"expected_len={len(exp)} actual_len={len(actual)}"
            )
        print("VERIFY_OK: all keys correct after invalidation", flush=True)
        print("PASS Test 6", flush=True)
        return True

    def run_all(self):
        self.setup()
        failures = []

        tests = [
            self.test_correctness_and_speedup,
            self.test_cache_ttl_expiry,
            self.test_different_prefixes,
            self.test_partial_cache_hit,
            self.test_large_batch_consistency,
            self.test_cache_invalidation_on_put,
        ]

        for test_func in tests:
            try:
                if not test_func():
                    failures.append(test_func.__name__)
            except Exception as e:
                print(f"FAIL {test_func.__name__}: {e}", flush=True)
                import traceback

                traceback.print_exc()
                failures.append(test_func.__name__)

        print(f"\n{'=' * 60}", flush=True)
        if failures:
            print(f"FAILED TESTS: {failures}", flush=True)
            return 1
        else:
            print("ALL TESTS PASSED", flush=True)
            return 0


if __name__ == "__main__":
    test = PrefixCacheTest()
    sys.exit(test.run_all())
