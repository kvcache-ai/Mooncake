"""Python-binding test for the DFS replica predicate.

Companion to ``test_batch_query_read_only.py``: that file asserts the
read-only query path returns the same descriptors as the lease-granting
path; this file asserts the DFS tier itself is observable from Python.

Test scenario:
  1. Start with a master whose DFS allocator is backed by a POSIX
     filesystem (``MOONCAKE_DFS_FS_ADAPTER=posix``), so no real
     distributed filesystem is needed.
  2. Put an object with ``replica_num=1, dfs_replica_num=1`` — memory
     replica only, DFS replica only.
  3. Drop the memory replica via ``batch_replica_clear`` after lease
     expiry, leaving the object DFS-only.
  4. Assert the descriptor reports ``is_dfs_replica() == True`` and
     every other tier predicate false, through BOTH
     ``batch_get_replica_desc`` and ``batch_query_read_only``.

Prerequisites (master side):
  - ``MOONCAKE_ENABLE_DFS=1``
  - ``MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend``
  - ``MOONCAKE_DFS_FS_ADAPTER=posix``
  - ``MOONCAKE_DFS_ROOT_DIR=<dir>``
  - ``MOONCAKE_DFS_SHARD_COUNT=1``
  - ``MOONCAKE_DFS_SHARD_CAPACITY=16777216``
  - ``MOONCAKE_DFS_ALIGNMENT=4096``
  - ``MOONCAKE_DFS_EVICTION_ENABLED=0``
  - ``MOONCAKE_DFS_DEFERRED_FREE_SECONDS=0``
  - ``MOONCAKE_DFS_SINGLE_TENANT=true``
  - ``--default_kv_lease_ttl=500`` (or matching DEFAULT_KV_LEASE_TTL)

Client side needs the same ``MOONCAKE_ENABLE_DFS`` /
``MOONCAKE_DFS_*`` variables plus
``MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=16777216`` so the client's
FileStorage initializes the DFS backend.
"""

import os
import time
import unittest

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

DEFAULT_DEFAULT_KV_LEASE_TTL = 500  # ms; must match master's
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)

SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(16 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(16 * 1024 * 1024)))


def get_client(store):
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata")
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
        True,  # enable_ssd_offload — initializes FileStorage / DFS backend
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


class TestIsDfsReplica(unittest.TestCase):
    """Python-binding test for is_dfs_replica on a DFS-only object."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "store", None) is not None:
            cls.store.close()
            cls.store = None

    def _put_dfs_only_key(self, key, data):
        """Put with memory+DFS replicas, then drop the memory replica
        after lease expiry so only the DFS replica remains."""
        config = ReplicateConfig()
        config.replica_num = 1
        config.dfs_replica_num = 1
        self.assertEqual(self.store.put(key, data, config), 0)

        time.sleep(default_kv_lease_ttl / 1000 + 0.5)
        cleared = self.store.batch_replica_clear([key])
        self.assertEqual(cleared, [key])

    def test_dfs_only_replica_predicates(self):
        """A DFS-only object must report is_dfs_replica() True and every
        other tier predicate False, via both query paths."""
        key = "test_is_dfs_replica_dfs_only"
        data = b"dfs replica predicate test data"
        self._put_dfs_only_key(key, data)

        try:
            for query_name, query in (
                ("batch_get_replica_desc", self.store.batch_get_replica_desc),
                ("batch_query_read_only", self.store.batch_query_read_only),
            ):
                with self.subTest(query=query_name):
                    descs = query([key])
                    self.assertIn(key, descs)
                    self.assertEqual(len(descs[key]), 1)

                    info = descs[key][0]
                    self.assertTrue(
                        info.is_dfs_replica(),
                        f"{query_name} must classify the DFS replica",
                    )
                    self.assertFalse(info.is_memory_replica())
                    self.assertFalse(info.is_local_disk_replica())
                    self.assertFalse(info.is_disk_replica())
        finally:
            self.store.remove(key)

    def test_dfs_data_survives_memory_replica_clear(self):
        """The DFS replica must be readable after the memory replica is
        gone — proving the DFS descriptor refers to real, loadable data
        and not a stale metadata entry."""
        key = "test_is_dfs_replica_readback"
        data = b"dfs replica readback payload"
        self._put_dfs_only_key(key, data)

        try:
            retrieved = self.store.get(key)
            self.assertEqual(retrieved, data)
        finally:
            self.store.remove(key)


if __name__ == "__main__":
    unittest.main()
