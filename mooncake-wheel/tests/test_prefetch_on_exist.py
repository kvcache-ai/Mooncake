"""Python-binding test for SSD prefetch-on-exist (ExistOptions.prefetch_to_memory).

Companion to ``test_promotion_on_hit.py``: that test asserts reads promote
SSD-only objects after clearing the admission threshold; this test asserts
``is_exist`` with ``prefetch_to_memory=True`` triggers a best-effort
SSD->DRAM promotion immediately, without any read and without the
promotion-on-hit admission gates.

Test scenario:
  1. Push enough data to overflow DRAM, forcing eviction + offload to turn
     warm keys into LOCAL_DISK-only objects.
  2. Identify a LOCAL_DISK-only key from the replica descriptors
     (condition-polled, not a fixed sleep).
  3. Call ``is_exist(key, options)`` with ``prefetch_to_memory=True``.
  4. Poll ``batch_get_replica_desc`` until the key regains a COMPLETE
     MEMORY replica (prefetch runs on a bounded thread pool with batched
     metadata queries, so poll instead of relying on one fixed sleep).
  5. Assert a subsequent ``get`` returns bit-exact bytes.
  6. Negative control: ``is_exist`` without prefetch options must NOT
     promote (within a short observation window).

Prerequisites:
  - ``mooncake_master`` running with master config containing:
      ``--enable_offload=true``
      ``--offload_on_evict=true``
      ``--promotion_on_hit=false``   (prefetch bypasses promotion-on-hit;
          the negative control is only meaningful if reads themselves
          cannot promote)
      ``--root_fs_dir=<dir>``
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` set on the client side.
  - ``MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=2`` recommended on the
    client (so offload drains predictably in CI).
  - ``MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10`` and
    ``MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760`` on the client.
"""

import ctypes
import os
import time
import unittest

from mooncake.store import ExistOptions, MooncakeDistributedStore

SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(32 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(64 * 1024 * 1024)))

# Async prefetch runs on a bounded thread pool with batched read-only
# metadata queries; poll instead of relying on a single fixed sleep.
PREFETCH_WAIT_SECONDS = int(os.getenv("PREFETCH_WAIT_SECONDS", "15"))
EVICTION_WAIT_SECONDS = int(os.getenv("EVICTION_WAIT_SECONDS", "25"))
# How long the negative control observes that nothing got promoted. Must
# comfortably exceed the prefetch trigger+execute latency.
NEGATIVE_OBSERVE_SECONDS = float(os.getenv("NEGATIVE_OBSERVE_SECONDS", "5"))


def _prefetch_options(enabled: bool = True) -> ExistOptions:
    options = ExistOptions()
    options.prefetch_to_memory = enabled
    return options


def setup_store(store):
    """Dict-based setup so enable_ssd_prefetch and the throttle knobs are
    passed by name (positional setup() does not carry prefetch config)."""
    config = {
        "local_hostname": os.getenv("LOCAL_HOSTNAME", "127.0.0.1"),
        "metadata_server": os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        "global_segment_size": str(SEGMENT_SIZE),
        "local_buffer_size": str(LOCAL_BUFFER_SIZE),
        "protocol": os.getenv("PROTOCOL", "tcp"),
        "rdma_devices": os.getenv("DEVICE_NAME", ""),
        "master_server_addr": os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        "enable_ssd_offload": "true",
        "enable_ssd_prefetch": "true",
        "ssd_get_wait_ms": os.getenv("SSD_GET_WAIT_MS", "0"),
    }
    ssd_offload_path = os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "")
    if ssd_offload_path:
        config["ssd_offload_path"] = ssd_offload_path

    retcode = store.setup(config)
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


def _replica_types(descs, key):
    """Return replica type tags ('MEMORY', 'LOCAL_DISK', 'DISK') for a key."""
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


# NOTE on probe interval: batch_get_replica_desc goes through the normal
# (lease-granting) replica query, so every poll extends the read lease of
# every probed key, and leased objects cannot be evicted. The poll interval
# MUST be larger than the master's default_kv_lease_ttl (500 ms in the CI
# smoke); an interval at or below the lease TTL keeps every key leased
# forever and livelocks eviction+offload (observed 2026-09-20: 2034
# eviction rounds, zero victims).
POLL_INTERVAL_SECONDS = float(os.getenv("PREFETCH_POLL_INTERVAL_SECONDS", "1.0"))


def wait_until(cond, timeout_s, interval=None, desc=""):
    """Poll ``cond()`` (returns (ok, detail)) until ok or timeout."""
    if interval is None:
        interval = POLL_INTERVAL_SECONDS
    t0 = time.monotonic()
    while True:
        ok, detail = cond()
        if ok:
            return detail
        if time.monotonic() - t0 >= timeout_s:
            raise AssertionError(
                f"timeout ({timeout_s}s) waiting for {desc}; last={detail}"
            )
        time.sleep(interval)


class TestPrefetchOnExist(unittest.TestCase):
    """Behavioral contract of ExistOptions.prefetch_to_memory."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        setup_store(cls.store)

    @classmethod
    def tearDownClass(cls):
        # Unmount this class's segment so it does not inflate the master's
        # capacity accounting for the rest of the run.
        if getattr(cls, "store", None) is not None:
            cls.store.close()
            cls.store = None

    def _make_cold_keys(self, tag, num_keys=96, value_size=1024 * 1024):
        """Overflow DRAM so eviction + offload leaves some keys
        LOCAL_DISK-only; returns {key: value} of successful puts."""
        timestamp = int(time.time() * 1000)
        keys = [f"prefetch_{tag}_{i}_{timestamp}" for i in range(num_keys)]
        reference = {}
        for key in keys:
            value = os.urandom(value_size)
            # NO_AVAILABLE_HANDLE under pressure is expected and not fatal.
            if self.store.put(key, value) == 0:
                reference[key] = value
        self.assertGreater(
            len(reference), 0, "No PUTs succeeded - cannot run prefetch test"
        )
        return reference

    def _find_cold_key(self, candidates):
        """Poll until one of candidates is LOCAL_DISK-only."""
        # Let the leases granted by the put path / previous polls expire
        # before the first probe (they are refreshed by every probe).
        time.sleep(1.0)

        def _probe():
            descs = self.store.batch_get_replica_desc(list(candidates))
            type_hist = {}
            for key in candidates:
                types = _replica_types(descs, key)
                hist_key = ",".join(sorted(set(types))) if types else "MISSING"
                type_hist[hist_key] = type_hist.get(hist_key, 0) + 1
                if (
                    types
                    and all("MEMORY" not in t for t in types)
                    and any("LOCAL_DISK" in t for t in types)
                ):
                    return True, (key, type_hist)
            return False, type_hist

        return wait_until(
            _probe,
            EVICTION_WAIT_SECONDS,
            desc="a LOCAL_DISK-only key after eviction - is "
            "offload_on_evict=true and the segment small enough to overflow?",
        )

    def _has_memory_replica(self, key):
        types = _replica_types(self.store.batch_get_replica_desc([key]), key)
        return "MEMORY" in types

    def test_exist_with_prefetch_promotes_ssd_only_key(self):
        """is_exist(prefetch_to_memory=True) on a LOCAL_DISK-only key must
        start a promotion: the key gains a MEMORY replica without any read,
        and a subsequent get returns bit-exact bytes."""
        reference = self._make_cold_keys("promote")
        cold_key, type_hist = self._find_cold_key(list(reference.keys()))
        print(f"replica-type histogram: {type_hist}")
        expected = reference[cold_key]

        self.assertEqual(self.store.is_exist(cold_key, _prefetch_options(True)), 1)

        def _promoted():
            return self._has_memory_replica(cold_key), _replica_types(
                self.store.batch_get_replica_desc([cold_key]), cold_key
            )

        wait_until(
            _promoted,
            PREFETCH_WAIT_SECONDS,
            desc=f"prefetch promotion of {cold_key} to a MEMORY replica",
        )

        got = self.store.get(cold_key)
        self.assertEqual(bytes(got), expected, "get after prefetch must be bit-exact")

    def test_get_wait_success_carries_live_lease(self):
        """ssd_get_wait_ms > 0: a get that arrives while the promotion is
        still in flight waits for it, and the post-wait transfer must carry
        a live lease. A QueryReadOnly result (lease_ttl_ms forced to 0)
        would turn every successful wait into LEASE_EXPIRED at BatchGet's
        post-transfer lease check."""
        # The class shares one store across test methods; earlier tests
        # already filled the 32MB segment, so reset it first.
        # remove_all returns the number of removed objects (>= 0 on success).
        self.assertGreaterEqual(self.store.remove_all(True), 0)
        # Put the big key FIRST (empty segment has room for it), then
        # overflow with small keys to evict it. After _make_cold_keys the
        # segment is full and this put would fail with NO_AVAILABLE_HANDLE.
        # 8MB stays under the offload bucket size limit (10MB in CI).
        timestamp = int(time.time() * 1000)
        big_key = f"prefetch_waitlease_big_{timestamp}"
        big_value = os.urandom(8 * 1024 * 1024)
        self.assertEqual(self.store.put(big_key, big_value), 0)
        # The put lease (default_kv_lease_ttl=500ms) blocks eviction, and
        # the overflow burst fits inside it; wait the lease out first, or
        # nothing ever forces this object out of DRAM.
        time.sleep(1.5)
        self._make_cold_keys("waitlease")
        # _find_cold_key polls every 1s and each poll grants a fresh lease,
        # pinning the object in DRAM until the probe times out. Probe more
        # rarely and keep applying overflow pressure only while unleased.
        types = []
        for round_i in range(6):
            time.sleep(2.0)
            descs = self.store.batch_get_replica_desc([big_key])
            types = _replica_types(descs, big_key)
            print(f"waitlease probe {round_i}: {types}")
            if (
                types
                and all("MEMORY" not in t for t in types)
                and any("LOCAL_DISK" in t for t in types)
            ):
                break
            self._make_cold_keys(f"waitlease_more{round_i}", num_keys=40)
        else:
            self.fail(f"big key stayed resident, last={types}")
        print(f"replica-type histogram: {types}")

        self.assertEqual(self.store.is_exist(big_key, _prefetch_options(True)), 1)
        # No sleep: the get path's own replica query takes longer than the
        # pool job's task registration, so firing immediately lands the
        # wait inside the promotion's in-flight window. Even a 25ms sleep
        # lands after kCompleted on a fast SSD and skips the wait path.

        capacity = len(big_value)
        destination = (ctypes.c_ubyte * capacity)()
        destination_ptr = ctypes.addressof(destination)
        self.assertEqual(self.store.register_buffer(destination_ptr, capacity), 0)
        try:
            results = self.store.batch_get_into_multi_buffers(
                [big_key], [[destination_ptr]], [[capacity]], False
            )
            print("batch_get results", list(results))
            self.assertEqual(
                list(results),
                [capacity],
                "post-wait get must succeed; a forced-expired lease turns "
                "it into LEASE_EXPIRED",
            )
            self.assertEqual(bytes(destination), big_value)
        finally:
            self.store.unregister_buffer(destination_ptr)

    def test_exist_without_prefetch_does_not_promote(self):
        """Negative control: plain is_exist must not promote. Only
        meaningful when the master runs promotion_on_hit=false (reads
        cannot promote either)."""
        reference = self._make_cold_keys("control")
        cold_key, type_hist = self._find_cold_key(list(reference.keys()))
        print(f"replica-type histogram: {type_hist}")

        self.assertEqual(self.store.is_exist(cold_key), 1)
        self.assertEqual(self.store.is_exist(cold_key, _prefetch_options(False)), 1)

        deadline = time.monotonic() + NEGATIVE_OBSERVE_SECONDS
        while time.monotonic() < deadline:
            self.assertFalse(
                self._has_memory_replica(cold_key),
                f"{cold_key} gained a MEMORY replica without prefetch; "
                "is promotion_on_hit accidentally enabled on the master?",
            )
            time.sleep(0.5)


if __name__ == "__main__":
    unittest.main()
