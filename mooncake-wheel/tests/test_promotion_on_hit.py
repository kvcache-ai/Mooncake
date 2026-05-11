"""Python-binding test for the L2->L1 promotion-on-hit feature.

Mirror of ``test_offload_on_eviction.py``: that test asserts data flows
DRAM -> SSD on eviction; this test asserts the reverse — once an object
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
import time
import unittest
from mooncake.store import MooncakeDistributedStore

DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # ms
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)

# Keep the segment small so we can overflow it cheaply in CI.
# 32 MB — small enough that the 96 x 1 MB workload reliably overflows past
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
        True,  # enable_ssd_offload — required for FileStorage / LOCAL_DISK
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
                # are expected and not fatal — we just need *some* keys to
                # successfully land on LOCAL_DISK.
                if retcode == 0:
                    reference[key] = value

            self.assertGreater(
                len(reference), 0, "No PUTs succeeded — cannot run promotion test"
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
                "No LOCAL_DISK-only key found after eviction — is "
                "offload_on_evict=true and the segment small enough to "
                "overflow? master config / SEGMENT_SIZE_BYTES env may need "
                "tuning. Histogram above shows the actual replica state.",
            )

            # Phase 3: clear the admission threshold via per-key reads.
            # ``store.get`` goes through ``Client::Query`` → master's
            # ``GetReplicaList`` (the same trigger ``batch_get_replica_desc``
            # walks), so each call fires the promotion gate once; 4 calls
            # comfortably clear any reasonable admission threshold. We also
            # assert bit-exact bytes back, exercising the LOCAL_DISK
            # read path end-to-end via the offload-RPC route (this
            # transitively guards the read-side fix the promotion feature
            # relies on; reads only return the offloaded bytes if the
            # peer's offload-RPC + segment-name resolution work).
            expected_bytes = reference[cold_key]
            for _ in range(4):
                got = self.store.get(cold_key)
                self.assertEqual(
                    got,
                    expected_bytes,
                    f"store.get on LOCAL_DISK-only key {cold_key} returned "
                    f"wrong/empty bytes (got len={len(got) if got else 0}, "
                    f"expected len={len(expected_bytes)}). The LOCAL_DISK "
                    f"read path is broken — promotion cannot be tested.",
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
            # batch_get_into_offload_object_internal — the single
            # chokepoint for LOCAL_DISK reads served via peer offload-RPC.
            # We snapshot the counter before the post-promotion read and
            # assert it didn't move, which means MEMORY served the read.
            # Bytes-back alone wouldn't distinguish MEMORY from LOCAL_DISK
            # (the offload-RPC path returns the correct bytes too).
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


if __name__ == "__main__":
    unittest.main()
