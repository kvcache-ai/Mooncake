"""Python-binding test for the offload-on-evict feature.

Unlike the default ``enable_ssd_offload`` path where PutEnd synchronously
queues objects for offload, the offload-on-evict mode defers offload until
DRAM eviction triggers. This test asserts the core behavioral difference:
small workloads below the eviction watermark stay in DRAM only and do not
produce LOCAL_DISK replicas.

The end-to-end overflow → evict → offload → read contract is covered by
``test_ssd_offload_in_evict.py`` (legacy sync-offload mode) and the
``verify_offload_on_eviction`` scenario in the kv-cache-offload-benchmark
project, both of which exercise the same storage backend code paths as
this feature.

Prerequisites:
  - mooncake_master running with master.json containing:
      ``"enable_offload": true``
      ``"offload_on_evict": true``
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/...`` env var set on the
    client side so FileStorage uses the matching backend / path.
"""

import unittest
import os
import time
from mooncake.store import MooncakeDistributedStore

# Align with the master's default_kv_lease_ttl (override via env).
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # ms
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)

SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(128 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(128 * 1024 * 1024)))


def get_client(store):
    """Initialize and setup the distributed store client."""
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


class TestOffloadOnEviction(unittest.TestCase):
    """Python-binding test for offload-on-evict behavioral difference."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_small_workload_no_disk_replica(self):
        """Workload well below the eviction watermark must NOT produce
        LOCAL_DISK replicas.

        Under the default ``enable_ssd_offload=true`` path, every PutEnd
        would queue the key for offload (heartbeat → disk). Under
        offload-on-evict, the offload queue stays empty until DRAM
        pressure triggers eviction. This asymmetry is what the feature
        is for — avoiding redundant disk I/O for hot data.

        A zero disk-replica count thus doubles as a guard that the
        master is actually running with ``offload_on_evict: true`` in config.
        """
        VALUE_SIZE = 1024  # 1 KB
        NUM_KEYS = 16  # 16 KB total, several orders of magnitude below
        # the 128 MB segment — eviction cannot trigger.

        timestamp = int(time.time())
        keys = [f"ooe_small_{i}_{timestamp}" for i in range(NUM_KEYS)]
        reference = {}

        try:
            for key in keys:
                value = os.urandom(VALUE_SIZE)
                retcode = self.store.put(key, value)
                self.assertEqual(
                    retcode,
                    0,
                    f"Put failed for {key}: retcode={retcode}",
                )
                reference[key] = value

            # Wait past one heartbeat interval so any async offload would
            # have had a chance to complete.
            time.sleep(3)

            # Query replicas — they must all be MEMORY only.
            descs = self.store.batch_get_replica_desc(keys)
            disk_replicas = 0
            for key in keys:
                infos = descs.get(key) if isinstance(descs, dict) else None
                if infos is None:
                    continue
                if not isinstance(infos, (list, tuple)):
                    infos = [infos]
                for info in infos:
                    type_str = str(getattr(info, "type", ""))
                    if type_str and "MEMORY" not in type_str:
                        disk_replicas += 1

            self.assertEqual(
                disk_replicas,
                0,
                f"Offload-on-evict should keep small workloads in DRAM "
                f"only, but found {disk_replicas} LOCAL_DISK replicas. "
                f"Is offload_on_evict=true set in master config?",
            )

            # Sanity: every key is readable and bit-exact (served from DRAM).
            for key, expected in reference.items():
                self.assertEqual(
                    self.store.get(key), expected, f"Bit-exact mismatch for {key}"
                )
        finally:
            # Cleanup — remove all keys written by this test regardless of
            # success or failure so subsequent tests start with clean DRAM.
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(default_kv_lease_ttl / 1000 + 0.5)


if __name__ == "__main__":
    unittest.main()
