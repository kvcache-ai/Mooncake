"""Python-binding test for the L2->L1 promotion-on-hit feature.

Mirror of ``test_offload_on_eviction.py``: that test asserts data flows
DRAM -> SSD on eviction; this test asserts the reverse - once an object
exists only on LOCAL_DISK and a client reads it enough times to clear the
admission threshold, the master enqueues a promotion task and the client's
next heartbeat tick stages a fresh MEMORY replica.

Test scenario:
  1. Push enough data to overflow DRAM, so eviction + offload turns warm
     keys into LOCAL_DISK-only objects.
  2. Identify one such key from the replica descriptors.
  3. Read it repeatedly to clear ``promotion_admission_threshold``.
  4. Wait long enough for one master heartbeat (which queues the task) plus
     one FileStorage heartbeat (which executes it).
  5. Assert the key now also has a MEMORY replica.

Prerequisites:
  - ``mooncake_master`` running with master config containing:
      ``--enable_offload=true``
      ``--offload_on_evict=true``
      ``--promotion_on_hit=true``
      ``--promotion_admission_threshold=1``  (any positive int; we send
          enough reads to clear up to 4)
      ``--root_fs_dir=<dir>``  (so the master tells the client to init
          its FileStorage; without this, no offload heartbeat runs and
          no LOCAL_DISK replicas are ever created)
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` set on the client side.
  - ``MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10`` and
    ``MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760`` on the client.
    The default bucket-flush thresholds (500 keys / 256 MB) are too high
    for a single-process test workload; the client's BucketStorageBackend
    would buffer everything in the ungrouped pool and never write to disk.
"""

import os
import statistics
import time
import unittest
from mooncake.store import MooncakeDistributedStore

DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # ms
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)

# Keep the segment small so we can overflow it cheaply in CI.
# 32 MB - small enough that the 96 x 1 MB workload reliably overflows past
# the eviction high watermark even with OffsetAllocator block padding.
# Larger values (e.g. 64 MB) sometimes fit the full workload because the
# allocator's effective capacity exceeds the nominal segment size.
SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(32 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(64 * 1024 * 1024)))

# Wait window for a full master+client heartbeat round-trip. The default
# heartbeat interval is 10s on both sides, so 25s is comfortably > 2 ticks.
PROMOTION_WAIT_SECONDS = int(os.getenv("PROMOTION_WAIT_SECONDS", "25"))


def get_client(store):
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE")
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")

    retcode = store.setup(
        local_hostname,
        metadata_server,
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        protocol,
        device_name,
        master_server_address,
        None,  # engine
        True,  # enable_ssd_offload - required for FileStorage / LOCAL_DISK
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


def _replica_types(descs, key):
    """Return a list of replica type tags ('MEMORY', 'LOCAL_DISK', 'DISK')
    for a key from a batch_get_replica_desc result. Tolerant to either
    list-of-info or single-info shape."""
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


def _has_memory_replica(descs, key):
    return "MEMORY" in _replica_types(descs, key)


def _collect_cold_keys(descs, keys):
    cold_keys = []
    type_hist = {}
    for key in keys:
        types = _replica_types(descs, key)
        hist_key = ",".join(sorted(set(types)))
        type_hist[hist_key] = type_hist.get(hist_key, 0) + 1
        if (
            types
            and all("MEMORY" not in t for t in types)
            and any("LOCAL_DISK" in t for t in types)
        ):
            cold_keys.append(key)
    return cold_keys, type_hist


def _percentiles(samples, ps):
    if not samples:
        return {p: 0.0 for p in ps}
    ordered = sorted(samples)
    result = {}
    for p in ps:
        if len(ordered) == 1:
            result[p] = ordered[0]
            continue
        rank = (len(ordered) - 1) * (p / 100.0)
        lo = int(rank)
        hi = min(lo + 1, len(ordered) - 1)
        frac = rank - lo
        result[p] = ordered[lo] + (ordered[hi] - ordered[lo]) * frac
    return result


def _wait_until_memory_replica(store, key, timeout_seconds, poll_interval_seconds):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        descs = store.batch_get_replica_desc([key])
        if _has_memory_replica(descs, key):
            return True
        time.sleep(poll_interval_seconds)
    return False


class TestPromotionOnHit(unittest.TestCase):
    """Python-binding test for the L2->L1 promotion-on-hit behavioral
    contract."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_promotion_after_repeated_hits(self):
        """A LOCAL_DISK-only key must regain a MEMORY replica after
        clearing the admission threshold via repeated reads.

        A failure here either means the master is not running with
        ``promotion_on_hit: true``, or the FileStorage promotion executor
        did not run / did not complete byte movement before the timeout."""

        VALUE_SIZE = 1024 * 1024  # 1 MB
        # Enough keys to comfortably overflow a 64 MB segment and trigger
        # offload-on-evict for at least the older portion of the workload.
        NUM_KEYS = 96

        timestamp = int(time.time())
        keys = [f"poh_{i}_{timestamp}" for i in range(NUM_KEYS)]
        reference = {}

        try:
            # Phase 1: overflow DRAM, force offload-on-evict.
            for key in keys:
                value = os.urandom(VALUE_SIZE)
                retcode = self.store.put(key, value)
                # NO_AVAILABLE_HANDLE-style failures (-200) under pressure
                # are expected and not fatal - we just need *some* keys to
                # successfully land on LOCAL_DISK.
                if retcode == 0:
                    reference[key] = value

            self.assertGreater(
                len(reference), 0, "No PUTs succeeded - cannot run promotion test"
            )

            # Phase 2: wait long enough for offload heartbeat to flush the
            # evicted keys to LOCAL_DISK.
            time.sleep(PROMOTION_WAIT_SECONDS)

            descs = self.store.batch_get_replica_desc(list(reference.keys()))
            type_hist = {}
            cold_key = None
            for key in reference.keys():
                types = _replica_types(descs, key)
                hist_key = ",".join(sorted(set(types)))
                type_hist[hist_key] = type_hist.get(hist_key, 0) + 1
                if (
                    cold_key is None
                    and types
                    and all("MEMORY" not in t for t in types)
                    and any("LOCAL_DISK" in t for t in types)
                ):
                    cold_key = key
            print(f"replica-type histogram after phase 2: {type_hist}")

            self.assertIsNotNone(
                cold_key,
                "No LOCAL_DISK-only key found after eviction - is "
                "offload_on_evict=true and the segment small enough to "
                "overflow? master config / SEGMENT_SIZE_BYTES env may need "
                "tuning. Histogram above shows the actual replica state.",
            )

            # Phase 3: clear the admission threshold via per-key reads.
            # ``store.get`` goes through ``Client::Query`` -> master's
            # ``GetReplicaList``, so each call fires the promotion gate
            # once; 4 calls comfortably clear any reasonable admission
            # threshold. We also assert bit-exact bytes back, exercising
            # the LOCAL_DISK read path end-to-end via the offload-RPC
            # route (this transitively guards the read-side fix the
            # promotion feature relies on; reads only return the
            # offloaded bytes if the peer's offload-RPC + segment-name
            # resolution work).
            expected_bytes = reference[cold_key]
            for _ in range(4):
                got = self.store.get(cold_key)
                self.assertEqual(
                    got,
                    expected_bytes,
                    f"store.get on LOCAL_DISK-only key {cold_key} returned "
                    f"wrong/empty bytes (got len={len(got) if got else 0}, "
                    f"expected len={len(expected_bytes)}). The LOCAL_DISK "
                    f"read path is broken - promotion cannot be tested.",
                )

            # Phase 4: wait for the master to enqueue the promotion task and
            # for the client's next FileStorage heartbeat to execute it.
            time.sleep(PROMOTION_WAIT_SECONDS)

            descs_after = self.store.batch_get_replica_desc([cold_key])
            types_after = _replica_types(descs_after, cold_key)
            print(f"replica types for {cold_key} after promotion: {types_after}")
            self.assertTrue(
                "MEMORY" in types_after,
                f"Expected a MEMORY replica for {cold_key} after promotion, "
                f"got types={types_after}. Either promotion_on_hit is not "
                f"enabled in the master, the admission threshold was not "
                f"cleared, or the heartbeat tick did not fire within "
                f"{PROMOTION_WAIT_SECONDS}s.",
            )

            # Phase 5: post-promotion bytes-back AND prove MEMORY was the
            # replica actually served. SelectBestReplica should prefer the
            # newly-COMPLETE MEMORY replica over the older LOCAL_DISK; if
            # it does, no offload-RPC call is issued for this read.
            #
            # offload_rpc_read_count counts every invocation of
            # batch_get_into_offload_object_internal - the single
            # chokepoint for LOCAL_DISK reads served via peer offload-RPC.
            # We snapshot the counter, do a read, then assert the counter
            # didn't move. Bytes-back alone wouldn't distinguish MEMORY
            # from LOCAL_DISK (the offload-RPC path returns correct bytes
            # too); the counter delta is the hard correctness signal that
            # the MEMORY path was used. The BenchPromotionLatency class
            # below offers a heavier latency-based view of the same
            # invariant when richer numbers are wanted.
            offload_count_before = self.store.get_offload_rpc_read_count()
            self.assertEqual(
                self.store.get(cold_key),
                expected_bytes,
                f"post-promotion store.get on {cold_key} returned wrong "
                f"bytes; the new MEMORY replica is metadata-visible but "
                f"not byte-correct. NotifyPromotionSuccess flipped "
                f"PROCESSING -> COMPLETE before the TE write finished, "
                f"or the TE write landed at the wrong address.",
            )
            offload_count_after = self.store.get_offload_rpc_read_count()
            self.assertEqual(
                offload_count_after,
                offload_count_before,
                f"post-promotion store.get on {cold_key} bumped the "
                f"offload-RPC counter from {offload_count_before} to "
                f"{offload_count_after}: it served from LOCAL_DISK SSD, "
                f"not the freshly-promoted MEMORY replica. The RFC's "
                f"'subsequent reads avoid SSD' invariant is broken; "
                f"check SelectBestReplica preference and the post-"
                f"promotion replica order in metadata.",
            )
        finally:
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


class TestPromotionDoesNotFire(unittest.TestCase):
    """Negative-case e2e. With workloads that don't produce LOCAL_DISK
    replicas, promotion has nothing to do and must not spuriously stage
    MEMORY replicas. This is the inverse invariant of the positive test
    and the only Python-level guard against:
      - regressions that auto-create LOCAL_DISK at PutEnd (would silently
        break offload semantics and trigger spurious promotions),
      - regressions that fire promotion without a LOCAL_DISK source.
    """

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_below_watermark_workload_stays_memory_only(self):
        VALUE_SIZE = 1024  # 1 KB
        NUM_KEYS = 16  # 16 KB total, well below the 32 MB segment

        timestamp = int(time.time())
        keys = [f"neg_poh_small_{i}_{timestamp}" for i in range(NUM_KEYS)]

        try:
            for key in keys:
                value = os.urandom(VALUE_SIZE)
                self.assertEqual(self.store.put(key, value), 0)

            # One heartbeat cycle is plenty for any spurious offload to
            # have surfaced.
            time.sleep(PROMOTION_WAIT_SECONDS)

            descs = self.store.batch_get_replica_desc(keys)
            unexpected = []
            for key in keys:
                types = _replica_types(descs, key)
                if not any(t == "MEMORY" for t in types):
                    unexpected.append((key, types))
                if any(t == "LOCAL_DISK" for t in types):
                    unexpected.append((key, types))

            self.assertEqual(
                unexpected,
                [],
                f"Below-watermark workload produced unexpected non-MEMORY "
                f"or LOCAL_DISK replicas: {unexpected[:5]}. Either "
                f"eviction is firing prematurely or PutEnd is auto-"
                f"creating LOCAL_DISK.",
            )
        finally:
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


@unittest.skipUnless(
    os.getenv("MC_BENCH_PROMOTION_LATENCY"),
    "opt-in benchmark - set MC_BENCH_PROMOTION_LATENCY=1 to run. "
    "Not part of CI; gives p50/p95/p99 latency comparison of LOCAL_DISK "
    "reads (pre-promotion) vs MEMORY reads (post-promotion).",
)
class BenchPromotionLatency(unittest.TestCase):
    """Ad-hoc latency benchmark for the L2->L1 promotion feature.

    Runs the same overflow -> eviction -> promote workflow as
    ``TestPromotionOnHit`` but:
      - times every read with ``time.perf_counter``;
      - aborts the pre-promotion sample loop the moment a read serves
        from MEMORY (detected via the offload-RPC counter) to keep the
        LOCAL_DISK distribution uncontaminated;
      - prints p50/p95/p99 + min/max for both phases and a per-percentile
        speedup table;
      - asserts a conservative 1.3x p50 speedup so a regression where
        MEMORY isn't being preferred breaks the bench.

    Sample size is controlled by ``LATENCY_SAMPLES`` (default 1000).
    For larger samples (e.g. N=10000) bump
    ``MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS`` so the worker
    doesn't race the pre-promotion read loop.

    Expected speedup on docker tmpfs with 1 MiB objects is ~2x at every
    percentile (most wall-time is Python/C++ marshalling, not the
    SSD-vs-DRAM cost gap). On real SSDs + RDMA NICs expect 10-50x.
    """

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_latency_comparison(self):
        VALUE_SIZE = 1024 * 1024  # 1 MB
        NUM_KEYS = 96

        timestamp = int(time.time())
        keys = [f"bench_poh_{i}_{timestamp}" for i in range(NUM_KEYS)]
        reference = {}

        try:
            # Overflow DRAM, force offload-on-evict.
            for key in keys:
                value = os.urandom(VALUE_SIZE)
                if self.store.put(key, value) == 0:
                    reference[key] = value
            self.assertGreater(len(reference), 0)

            time.sleep(PROMOTION_WAIT_SECONDS)

            descs = self.store.batch_get_replica_desc(list(reference.keys()))
            cold_key = None
            for key in reference.keys():
                types = _replica_types(descs, key)
                if (
                    cold_key is None
                    and types
                    and all("MEMORY" not in t for t in types)
                    and any("LOCAL_DISK" in t for t in types)
                ):
                    cold_key = key
            self.assertIsNotNone(cold_key, "no LOCAL_DISK-only key after eviction")

            # Pre-promotion: timed LOCAL_DISK reads, bail when a read
            # serves from MEMORY (= worker raced the loop).
            N = int(os.getenv("LATENCY_SAMPLES", "1000"))
            expected_bytes = reference[cold_key]
            pre_latencies_ms = []
            for i in range(N):
                count_before = self.store.get_offload_rpc_read_count()
                t0 = time.perf_counter()
                got = self.store.get(cold_key)
                t1 = time.perf_counter()
                count_after = self.store.get_offload_rpc_read_count()
                self.assertEqual(got, expected_bytes)
                if count_after == count_before:
                    print(
                        f"  pre-promotion loop: promotion fired at "
                        f"sample {i}/{N}; stopping to keep the "
                        f"LOCAL_DISK sample uncontaminated"
                    )
                    break
                pre_latencies_ms.append((t1 - t0) * 1000.0)
            self.assertGreater(
                len(pre_latencies_ms),
                1,
                "collected <2 pre-promotion samples - promotion fired "
                "before measurement could capture LOCAL_DISK latency. "
                "Bump MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS or "
                "lower LATENCY_SAMPLES.",
            )

            # Wait for promotion to complete.
            time.sleep(PROMOTION_WAIT_SECONDS)
            descs_after = self.store.batch_get_replica_desc([cold_key])
            types_after = _replica_types(descs_after, cold_key)
            self.assertIn("MEMORY", types_after)

            # Post-promotion: timed MEMORY reads; the counter must stay
            # put (otherwise at least one read still hit LOCAL_DISK).
            count_before = self.store.get_offload_rpc_read_count()
            post_latencies_ms = []
            for _ in range(N):
                t0 = time.perf_counter()
                got = self.store.get(cold_key)
                t1 = time.perf_counter()
                self.assertEqual(got, expected_bytes)
                post_latencies_ms.append((t1 - t0) * 1000.0)
            count_after = self.store.get_offload_rpc_read_count()
            self.assertEqual(
                count_after,
                count_before,
                f"post-promotion reads bumped the offload-RPC counter "
                f"from {count_before} to {count_after}: at least one "
                f"read served from LOCAL_DISK SSD instead of MEMORY.",
            )

            def _pcts(samples):
                """{p: value_ms} for p in [50, 95, 99]."""
                if len(samples) >= 2:
                    cuts = statistics.quantiles(samples, n=100)
                    return {50: cuts[49], 95: cuts[94], 99: cuts[98]}
                v = samples[0] if samples else 0.0
                return {50: v, 95: v, 99: v}

            def _speedup(a, b):
                return a / b if b > 0 else float("inf")

            pre = _pcts(pre_latencies_ms)
            post = _pcts(post_latencies_ms)

            print()
            print(f"=== promotion latency comparison "
                  f"(cold_key={cold_key}) ===")
            print(
                f"pre-promotion  (LOCAL_DISK via offload-RPC): "
                f"n={len(pre_latencies_ms)}, "
                f"p50={pre[50]:.2f} ms, p95={pre[95]:.2f} ms, "
                f"p99={pre[99]:.2f} ms, "
                f"min={min(pre_latencies_ms):.2f} ms, "
                f"max={max(pre_latencies_ms):.2f} ms"
            )
            print(
                f"post-promotion (MEMORY direct):              "
                f"n={len(post_latencies_ms)}, "
                f"p50={post[50]:.2f} ms, p95={post[95]:.2f} ms, "
                f"p99={post[99]:.2f} ms, "
                f"min={min(post_latencies_ms):.2f} ms, "
                f"max={max(post_latencies_ms):.2f} ms"
            )
            print(
                f"speedup: p50={_speedup(pre[50], post[50]):.1f}x  "
                f"|  p95={_speedup(pre[95], post[95]):.1f}x  "
                f"|  p99={_speedup(pre[99], post[99]):.1f}x"
            )

            self.assertLess(
                post[50],
                pre[50] / 1.3,
                f"post-promotion p50 latency ({post[50]:.2f} ms) is not "
                f"meaningfully lower than pre-promotion p50 "
                f"({pre[50]:.2f} ms). Real-hardware speedup is typically "
                f"10-50x; failing this 1.3x check means MEMORY likely "
                f"isn't being preferred.",
            )
        finally:
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


@unittest.skipUnless(
    os.getenv("MC_BENCH_PROMOTION_DRAIN"),
    "opt-in benchmark - set MC_BENCH_PROMOTION_DRAIN=1 to run. "
    "Measures how quickly a batch of LOCAL_DISK-only keys regain MEMORY "
    "replicas after promotion admission fires.",
)
class BenchPromotionDrain(unittest.TestCase):
    """Promotion backlog-drain benchmark for fair main-vs-PR comparison.

    Assumes the master runs with ``--promotion_admission_threshold=2``:
    the replica-descriptor scan used to discover LOCAL_DISK-only keys
    provides the first hit, and the timed ``selected_keys`` query provides
    the second hit that actually admits promotion.
    """

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_promotion_drain(self):
        value_size = int(
            os.getenv("PROMOTION_DRAIN_VALUE_SIZE_BYTES", str(1024 * 1024))
        )
        total_keys = int(os.getenv("PROMOTION_DRAIN_TOTAL_KEYS", "128"))
        target_cold_keys = int(os.getenv("PROMOTION_DRAIN_TARGET_KEYS", "32"))
        poll_interval_seconds = float(
            os.getenv("PROMOTION_DRAIN_POLL_INTERVAL_SECONDS", "0.2")
        )
        timeout_seconds = float(
            os.getenv(
                "PROMOTION_DRAIN_TIMEOUT_SECONDS",
                str(max(PROMOTION_WAIT_SECONDS * 3, 60)),
            )
        )
        expected_admission_threshold = int(
            os.getenv("PROMOTION_DRAIN_EXPECT_ADMISSION_THRESHOLD", "2")
        )
        post_read_samples = int(os.getenv("PROMOTION_DRAIN_POST_READ_SAMPLES", "64"))
        fail_on_timeout = os.getenv("PROMOTION_DRAIN_FAIL_ON_TIMEOUT", "1") != "0"

        timestamp = int(time.time())
        keys = [f"bench_poh_drain_{i}_{timestamp}" for i in range(total_keys)]
        reference = {}

        try:
            for key in keys:
                value = os.urandom(value_size)
                if self.store.put(key, value) == 0:
                    reference[key] = value

            self.assertGreater(
                len(reference), 0, "No PUTs succeeded - cannot run drain benchmark"
            )
            self.assertEqual(
                expected_admission_threshold,
                2,
                "BenchPromotionDrain assumes promotion_admission_threshold=2: "
                "the cold-key discovery scan contributes hit #1 and the timed "
                "selected-keys query contributes hit #2. Use "
                "PROMOTION_DRAIN_EXPECT_ADMISSION_THRESHOLD=2 and run the "
                "master with --promotion_admission_threshold=2 for fair data.",
            )

            time.sleep(PROMOTION_WAIT_SECONDS)

            descs = self.store.batch_get_replica_desc(list(reference.keys()))
            cold_keys, type_hist = _collect_cold_keys(descs, list(reference.keys()))
            print(f"replica-type histogram after offload: {type_hist}")
            self.assertGreater(
                len(cold_keys),
                0,
                "No LOCAL_DISK-only key found after eviction/offload",
            )

            selected_keys = cold_keys[: min(target_cold_keys, len(cold_keys))]
            self.assertGreaterEqual(
                len(selected_keys),
                1,
                "Need at least one LOCAL_DISK-only key to measure promotion drain",
            )

            offload_reads_before = self.store.get_offload_rpc_read_count()
            admission_t0 = time.perf_counter()
            admission_descs = self.store.batch_get_replica_desc(selected_keys)
            admission_t1 = time.perf_counter()
            print(
                "promotion admission batch_get_replica_desc: "
                f"keys={len(selected_keys)}, "
                f"elapsed_ms={(admission_t1 - admission_t0) * 1000.0:.2f}, "
                f"assumed_threshold={expected_admission_threshold}"
            )

            for key in selected_keys:
                self.assertIn(
                    "LOCAL_DISK",
                    _replica_types(admission_descs, key),
                    f"selected key {key} lost LOCAL_DISK state before benchmark started",
                )

            start = admission_t0
            promoted_at = {}
            poll_count = 0
            while len(promoted_at) < len(selected_keys):
                elapsed = time.perf_counter() - start
                if elapsed > timeout_seconds:
                    break
                poll_count += 1
                descs_now = self.store.batch_get_replica_desc(selected_keys)
                now = time.perf_counter()
                for key in selected_keys:
                    if key in promoted_at:
                        continue
                    if _has_memory_replica(descs_now, key):
                        promoted_at[key] = now - start
                if len(promoted_at) == len(selected_keys):
                    break
                time.sleep(poll_interval_seconds)

            offload_reads_after = self.store.get_offload_rpc_read_count()
            completed_all = len(promoted_at) == len(selected_keys)
            completion_times = sorted(promoted_at.values())
            observed_pcts = _percentiles(completion_times, [50, 95, 100])

            post_latency_line = "skipped (not all selected keys promoted)"
            if completed_all:
                post_latencies_ms = []
                sample_key = selected_keys[0]
                expected = reference[sample_key]
                post_offload_before = self.store.get_offload_rpc_read_count()
                for _ in range(post_read_samples):
                    t0 = time.perf_counter()
                    got = self.store.get(sample_key)
                    t1 = time.perf_counter()
                    self.assertEqual(got, expected)
                    post_latencies_ms.append((t1 - t0) * 1000.0)
                post_offload_after = self.store.get_offload_rpc_read_count()
                self.assertEqual(
                    post_offload_after,
                    post_offload_before,
                    "Post-promotion reads still hit offload RPC path; MEMORY replica is not serving reads",
                )
                latency_pcts = _percentiles(post_latencies_ms, [50, 95, 99])
                post_latency_line = (
                    f"p50={latency_pcts[50]:.2f}, "
                    f"p95={latency_pcts[95]:.2f}, p99={latency_pcts[99]:.2f}"
                )

            print()
            print("=== promotion drain benchmark ===")
            print(
                f"selected_keys={len(selected_keys)} / cold_keys={len(cold_keys)} / "
                f"total_successful_puts={len(reference)} / polls={poll_count} / "
                f"promoted_before_timeout={len(promoted_at)}"
            )
            if completed_all:
                time_to_all = completion_times[-1]
                promoted_per_sec = (
                    len(selected_keys) / time_to_all
                    if time_to_all > 0
                    else float("inf")
                )
                print(
                    f"time_to_50pct_promoted={observed_pcts[50] * 1000.0:.2f} ms | "
                    f"time_to_95pct_promoted={observed_pcts[95] * 1000.0:.2f} ms | "
                    f"time_to_all_promoted={observed_pcts[100] * 1000.0:.2f} ms"
                )
                print(f"promoted_keys_per_sec={promoted_per_sec:.2f}")
            else:
                print(
                    f"timed_out_before_all_promoted=yes | timeout_seconds={timeout_seconds:.2f} | "
                    f"observed_time_to_last_promoted={observed_pcts[100] * 1000.0:.2f} ms"
                )
            print(f"post_promotion_read_latency_ms: {post_latency_line}")
            print(
                f"offload_rpc_reads_during_benchmark="
                f"{offload_reads_after - offload_reads_before}"
            )
            if fail_on_timeout:
                self.assertEqual(
                    len(promoted_at),
                    len(selected_keys),
                    "Timed out waiting for all selected LOCAL_DISK-only keys to regain MEMORY replicas",
                )
        finally:
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


if __name__ == "__main__":
    unittest.main()
