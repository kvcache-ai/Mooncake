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
