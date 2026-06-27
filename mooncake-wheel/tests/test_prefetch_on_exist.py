"""Python-binding test for the SSD-to-DRAM prefetch-on-exist feature (RFC #2213).

Validates that ``is_exist(key, ExistOptions(prefetch_to_memory=True))`` /
``batch_is_exist(keys, options)`` triggers asynchronous SSD-to-DRAM promotion
via the dedicated ``FileStorage::PrefetchKeys`` path (not promotion-on-hit
heartbeat queue).

Test scenario:
  1. Push enough data to overflow DRAM, forcing eviction + offload to turn
     warm keys into LOCAL_DISK-only objects.
  2. Identify a LOCAL_DISK-only key from the replica descriptors.
  3. Call ``is_exist(key, options)`` with ``prefetch_to_memory=True``.
  4. Wait for async ``PrefetchKeys`` (thread-pool + batched metadata query).
  5. Assert the key now has a MEMORY replica (prefetch succeeded).
  6. Assert that a subsequent ``get`` serves from MEMORY (not SSD).

Prerequisites:
  - ``mooncake_master`` running with:
      ``--enable_offload=true``
      ``--offload_on_evict=true``
      ``--root_fs_dir=<dir>``
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` set on the client side
    (or pass ``ssd_offload_path`` via setup).
  - ``MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10`` and
    ``MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760`` on the client.
"""

import os
import statistics
import time
import unittest

try:
    import torch_npu

    torch_npu.npu.set_device(int(os.getenv("NPU_DEVICE_ID", "0")))
except ImportError:
    pass

from mooncake.store import ExistOptions, MooncakeDistributedStore


def _prefetch_options(enabled: bool = True) -> ExistOptions:
    options = ExistOptions()
    options.prefetch_to_memory = enabled
    return options


DEFAULT_KV_LEASE_TTL = 5000  # ms
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_KV_LEASE_TTL))

SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(32 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(64 * 1024 * 1024)))

# Async prefetch runs on a bounded thread pool with batched metadata queries;
# poll briefly instead of relying on a single fixed sleep.
PREFETCH_WAIT_SECONDS = int(os.getenv("PREFETCH_WAIT_SECONDS", "15"))
PREFETCH_POLL_INTERVAL_SECONDS = float(
    os.getenv("PREFETCH_POLL_INTERVAL_SECONDS", "0.2")
)
EVICTION_WAIT_SECONDS = int(os.getenv("EVICTION_WAIT_SECONDS", "25"))


def setup_store(store, local_hostname=None):
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "")
    if local_hostname is None:
        local_hostname = os.getenv("LOCAL_HOSTNAME", "127.0.0.1")
    metadata_server = os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE")
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
    ssd_offload_path = os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "")
    tenant_id = os.getenv("MOONCAKE_TENANT_ID", "default")
    prefetch_cooldown_sec = int(os.getenv("SSD_PREFETCH_COOLDOWN_SEC", "5"))
    prefetch_dedup_ttl_sec = int(os.getenv("SSD_PREFETCH_DEDUP_TTL_SEC", "30"))

    retcode = store.setup(
        local_hostname,
        metadata_server,
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        protocol,
        device_name,
        master_server_address,
        None,  # engine
        True,  # enable_ssd_offload
        ssd_offload_path,
        tenant_id,
        prefetch_cooldown_sec,
        prefetch_dedup_ttl_sec,
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


def _replica_types(descs, key):
    infos = descs.get(key) if isinstance(descs, dict) else None
    if infos is None:
        return []
    if not isinstance(infos, (list, tuple)):
        infos = [infos]
    tags = []
    for info in infos:
        if hasattr(info, "is_memory_replica") and info.is_memory_replica():
            tags.append("MEMORY")
        elif hasattr(info, "is_local_disk_replica") and info.is_local_disk_replica():
            tags.append("LOCAL_DISK")
        elif hasattr(info, "is_disk_replica") and info.is_disk_replica():
            tags.append("DISK")
        else:
            tags.append("UNKNOWN")
    return tags


def _find_cold_key(store, keys):
    """Find a key that exists only on LOCAL_DISK (no MEMORY replica)."""
    descs = store.batch_get_replica_desc(list(keys))
    for key in keys:
        types = _replica_types(descs, key)
        if types and all("MEMORY" not in t for t in types) and any("LOCAL_DISK" in t for t in types):
            return key
    return None


def _find_cold_keys(store, keys):
    """Return all LOCAL_DISK-only keys from *keys*."""
    descs = store.batch_get_replica_desc(list(keys))
    cold_keys = []
    for key in keys:
        types = _replica_types(descs, key)
        if types and all("MEMORY" not in t for t in types) and any("LOCAL_DISK" in t for t in types):
            cold_keys.append(key)
    return cold_keys


def _count_memory_replicas(store, keys):
    descs = store.batch_get_replica_desc(list(keys))
    return sum(1 for key in keys if "MEMORY" in _replica_types(descs, key))


def _wait_for_memory_replicas(
    store,
    keys,
    min_promoted=1,
    timeout_seconds=PREFETCH_WAIT_SECONDS,
):
    """Poll replica descriptors until at least *min_promoted* keys have MEMORY."""
    deadline = time.time() + timeout_seconds
    promoted = 0
    while time.time() < deadline:
        promoted = _count_memory_replicas(store, keys)
        if promoted >= min_promoted:
            return promoted
        time.sleep(PREFETCH_POLL_INTERVAL_SECONDS)
    return promoted


def _overflow_dram(store, num_keys=96, value_size=1024 * 1024):
    """Put enough data to overflow DRAM and trigger offload-on-evict.
    Returns {key: value} for successfully stored keys."""
    timestamp = int(time.time())
    keys = [f"prefetch_{i}_{timestamp}" for i in range(num_keys)]
    reference = {}
    for key in keys:
        value = os.urandom(value_size)
        if store.put(key, value) == 0:
            reference[key] = value
    return reference


class TestPrefetchOnExist(unittest.TestCase):
    """Validates that is_exist(prefetch=True) triggers SSD→DRAM promotion."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        setup_store(cls.store)

    def test_single_key_prefetch(self):
        """is_exist(key, prefetch=True) should promote a LOCAL_DISK-only key."""
        reference = _overflow_dram(self.store)
        self.assertGreater(len(reference), 0, "No PUTs succeeded")

        try:
            # Wait for eviction + offload to create LOCAL_DISK replicas
            time.sleep(EVICTION_WAIT_SECONDS)

            cold_key = _find_cold_key(self.store, reference.keys())
            self.assertIsNotNone(
                cold_key,
                "No LOCAL_DISK-only key found. Is offload_on_evict=true "
                "and the segment small enough to overflow?",
            )
            print(f"Found LOCAL_DISK-only key: {cold_key}")

            # Trigger prefetch via is_exist
            result = self.store.is_exist(cold_key, _prefetch_options())
            self.assertEqual(result, 1, "is_exist should return 1 for existing key")

            # Wait for async prefetch (batched metadata + thread pool)
            promoted = _wait_for_memory_replicas(self.store, [cold_key], min_promoted=1)
            descs = self.store.batch_get_replica_desc([cold_key])
            types = _replica_types(descs, cold_key)
            print(f"Replica types after prefetch: {types} (promoted={promoted})")
            self.assertIn(
                "MEMORY",
                types,
                f"Expected a MEMORY replica for {cold_key} after prefetch, "
                f"got types={types}. Prefetch may have failed or not triggered.",
            )

            # Verify the data is correct and served from MEMORY (not SSD)
            offload_count_before = self.store.get_offload_rpc_read_count()
            got = self.store.get(cold_key)
            offload_count_after = self.store.get_offload_rpc_read_count()
            self.assertEqual(
                got,
                reference[cold_key],
                "Data mismatch after prefetch",
            )
            self.assertEqual(
                offload_count_after,
                offload_count_before,
                f"Post-prefetch read still hit LOCAL_DISK (offload RPC count "
                f"went from {offload_count_before} to {offload_count_after}). "
                f"MEMORY replica may not be preferred.",
            )
            print("PASS: is_exist(prefetch=True) successfully promoted key to DRAM")
        finally:
            for key in reference:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)

    def test_batch_prefetch(self):
        """batch_is_exist(keys, prefetch=True) should promote LOCAL_DISK-only keys."""
        reference = _overflow_dram(self.store)
        self.assertGreater(len(reference), 0, "No PUTs succeeded")

        try:
            time.sleep(EVICTION_WAIT_SECONDS)

            all_keys = list(reference.keys())
            cold_keys = _find_cold_keys(self.store, all_keys)

            self.assertGreater(len(cold_keys), 0, "No LOCAL_DISK-only keys found")
            print(f"Found {len(cold_keys)} LOCAL_DISK-only keys for batch prefetch")

            # Trigger batch prefetch
            results = self.store.batch_is_exist(cold_keys, _prefetch_options())
            self.assertEqual(len(results), len(cold_keys))
            for r in results:
                self.assertEqual(r, 1, "batch_is_exist should return 1 for existing keys")

            promoted_count = _wait_for_memory_replicas(
                self.store, cold_keys, min_promoted=1
            )
            print(f"Promoted {promoted_count}/{len(cold_keys)} keys to DRAM")
            self.assertGreater(
                promoted_count,
                0,
                f"No keys were promoted after batch_is_exist(prefetch=True). "
                f"Prefetch may not be triggering.",
            )
        finally:
            for key in reference:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)

    def test_prefetch_false_does_not_promote(self):
        """is_exist(key, prefetch=False) should NOT trigger promotion."""
        reference = _overflow_dram(self.store)
        self.assertGreater(len(reference), 0)

        try:
            time.sleep(EVICTION_WAIT_SECONDS)

            cold_key = _find_cold_key(self.store, reference.keys())
            if cold_key is None:
                self.skipTest("No LOCAL_DISK-only key found; cannot run negative test")

            # Call without prefetch
            result = self.store.is_exist(cold_key, _prefetch_options(False))
            self.assertEqual(result, 1)

            # Short wait; if prefetch were triggered, it would complete quickly
            time.sleep(5)

            descs = self.store.batch_get_replica_desc([cold_key])
            types = _replica_types(descs, cold_key)
            self.assertNotIn(
                "MEMORY",
                types,
                f"Key {cold_key} got a MEMORY replica after is_exist(prefetch=False). "
                f"Prefetch should NOT fire when prefetch=False.",
            )
            print("PASS: is_exist(prefetch=False) correctly did NOT trigger promotion")
        finally:
            for key in reference:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)

    def test_prefetch_memory_key_is_noop(self):
        """Prefetch on a key already in MEMORY should be a no-op."""
        key = f"mem_only_{int(time.time())}"
        value = os.urandom(1024)
        try:
            self.assertEqual(self.store.put(key, value), 0)

            # Key should be in MEMORY
            result = self.store.is_exist(key, _prefetch_options())
            self.assertEqual(result, 1)

            # Verify it's still just MEMORY (no extra replicas)
            descs = self.store.batch_get_replica_desc([key])
            types = _replica_types(descs, key)
            self.assertIn("MEMORY", types)
            print(f"PASS: prefetch on MEMORY key is no-op, types={types}")
        finally:
            self.store.remove(key)
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


@unittest.skipUnless(
    os.getenv("MC_BENCH_PREFETCH_LATENCY"),
    "opt-in benchmark — set MC_BENCH_PREFETCH_LATENCY=1 to run.",
)
class BenchPrefetchLatency(unittest.TestCase):
    """Latency benchmark comparing reads before and after prefetch.

    Measures:
    - Phase A: LOCAL_DISK read latency (before prefetch)
    - Phase B: MEMORY read latency (after prefetch via is_exist)
    - Speedup: phase_A / phase_B at p50/p95/p99
    """

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        setup_store(cls.store)

    def test_latency_comparison(self):
        reference = _overflow_dram(self.store)
        self.assertGreater(len(reference), 0)

        try:
            time.sleep(EVICTION_WAIT_SECONDS)

            cold_key = _find_cold_key(self.store, reference.keys())
            self.assertIsNotNone(cold_key)
            expected_bytes = reference[cold_key]

            N = int(os.getenv("LATENCY_SAMPLES", "200"))

            # Phase A: timed reads from LOCAL_DISK (before prefetch)
            pre_latencies_ms = []
            for _ in range(N):
                t0 = time.perf_counter()
                got = self.store.get(cold_key)
                t1 = time.perf_counter()
                self.assertEqual(got, expected_bytes)
                pre_latencies_ms.append((t1 - t0) * 1000.0)

            # Trigger prefetch
            self.store.is_exist(cold_key, _prefetch_options())
            promoted = _wait_for_memory_replicas(self.store, [cold_key], min_promoted=1)

            # Verify promotion happened
            descs = self.store.batch_get_replica_desc([cold_key])
            types = _replica_types(descs, cold_key)
            self.assertIn(
                "MEMORY",
                types,
                f"Prefetch did not create MEMORY replica (promoted={promoted})",
            )

            # Phase B: timed reads from MEMORY (after prefetch)
            post_latencies_ms = []
            for _ in range(N):
                t0 = time.perf_counter()
                got = self.store.get(cold_key)
                t1 = time.perf_counter()
                self.assertEqual(got, expected_bytes)
                post_latencies_ms.append((t1 - t0) * 1000.0)

            def _pcts(samples):
                if len(samples) >= 2:
                    cuts = statistics.quantiles(samples, n=100)
                    return {50: cuts[49], 95: cuts[94], 99: cuts[98]}
                v = samples[0] if samples else 0.0
                return {50: v, 95: v, 99: v}

            pre = _pcts(pre_latencies_ms)
            post = _pcts(post_latencies_ms)

            print()
            print(f"=== prefetch-on-exist latency comparison (key={cold_key}) ===")
            print(
                f"BEFORE prefetch (LOCAL_DISK): n={len(pre_latencies_ms)}, "
                f"p50={pre[50]:.2f}ms, p95={pre[95]:.2f}ms, p99={pre[99]:.2f}ms, "
                f"min={min(pre_latencies_ms):.2f}ms, max={max(pre_latencies_ms):.2f}ms"
            )
            print(
                f"AFTER  prefetch (MEMORY):     n={len(post_latencies_ms)}, "
                f"p50={post[50]:.2f}ms, p95={post[95]:.2f}ms, p99={post[99]:.2f}ms, "
                f"min={min(post_latencies_ms):.2f}ms, max={max(post_latencies_ms):.2f}ms"
            )
            speedup_50 = pre[50] / post[50] if post[50] > 0 else float("inf")
            speedup_95 = pre[95] / post[95] if post[95] > 0 else float("inf")
            speedup_99 = pre[99] / post[99] if post[99] > 0 else float("inf")
            print(f"speedup: p50={speedup_50:.1f}x | p95={speedup_95:.1f}x | p99={speedup_99:.1f}x")

            self.assertLess(
                post[50],
                pre[50] / 1.3,
                f"Post-prefetch p50 ({post[50]:.2f}ms) not meaningfully lower than "
                f"pre-prefetch p50 ({pre[50]:.2f}ms). MEMORY may not be preferred.",
            )
        finally:
            for key in reference:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


@unittest.skipUnless(
    os.getenv("MC_TEST_CROSS_NODE"),
    "opt-in cross-node test — set MC_TEST_CROSS_NODE=1 and provide two "
    "endpoints via NODE_A_HOSTNAME / NODE_B_HOSTNAME (e.g. ip:port).",
)
class TestCrossNodePrefetch(unittest.TestCase):
    """Validates方案B: cross-node SSD prefetch delegation to the holder node.

    Scenario (two store clients sharing one Master):
      1. Node A puts keys and overflows DRAM so they are evicted to A's SSD.
         A is therefore the LOCAL_DISK holder (replica transport_endpoint == A).
      2. Node B calls batch_is_exist(keys, prefetch_to_memory=True). Because the
         holder endpoint != B's own endpoint, B delegates the promotion to A via
         the prefetch_offload_object RPC instead of calling RegisterPrefetchTask
         locally (which would fail Master's holder check with INVALID_PARAMS).
      3. A registers the promotion task with its own client_id and stages the
         object SSD->DRAM. The key gains a MEMORY replica.

    Requires a running master with --enable_offload=true --offload_on_evict=true
    and two reachable endpoints. Both stores run in this process but bind
    distinct offload RPC servers / segments.
    """

    @classmethod
    def setUpClass(cls):
        node_a = os.getenv("NODE_A_HOSTNAME", "127.0.0.1:17001")
        node_b = os.getenv("NODE_B_HOSTNAME", "127.0.0.1:17002")
        cls.store_a = MooncakeDistributedStore()
        cls.store_b = MooncakeDistributedStore()
        setup_store(cls.store_a, local_hostname=node_a)
        setup_store(cls.store_b, local_hostname=node_b)

    def test_remote_holder_prefetch(self):
        # Node A populates and overflows so keys land as LOCAL_DISK-only on A.
        reference = _overflow_dram(self.store_a)
        self.assertGreater(len(reference), 0, "No PUTs succeeded on node A")

        try:
            time.sleep(EVICTION_WAIT_SECONDS)

            all_keys = list(reference.keys())
            cold_keys = _find_cold_keys(self.store_a, all_keys)
            self.assertGreater(
                len(cold_keys), 0, "No LOCAL_DISK-only keys produced on node A"
            )
            print(f"Node A holds {len(cold_keys)} LOCAL_DISK-only key(s)")

            # Node B triggers prefetch. B must delegate to holder A via RPC.
            results = self.store_b.batch_is_exist(cold_keys, _prefetch_options())
            self.assertEqual(len(results), len(cold_keys))
            for r in results:
                self.assertEqual(r, 1, "batch_is_exist should return 1")

            promoted = _wait_for_memory_replicas(
                self.store_b, cold_keys, min_promoted=1
            )
            print(f"Cross-node promoted {promoted}/{len(cold_keys)} key(s) to DRAM")
            self.assertGreater(
                promoted,
                0,
                "No keys were promoted via cross-node delegation. Check that the "
                "holder received prefetch_offload_object and no INVALID_PARAMS "
                "(RegisterPrefetchTask holder check) was logged.",
            )
        finally:
            for key in reference:
                try:
                    self.store_a.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


if __name__ == "__main__":
    unittest.main()
