"""Python-binding test for the read-only replica query and DFS predicate.

``batch_query_read_only`` must return the same replica descriptors as
``batch_get_replica_desc`` (same master-side descriptor list, including
DFS replicas) while granting no lease and not firing promotion-on-hit.
This test asserts the observable contract a tier-aware admission caller
(e.g. vLLM's MooncakeStoreConnector) relies on:

  1. Same shape: dict[key, list[ReplicaDescriptor]].
  2. Same tier classification per key as batch_get_replica_desc.
  3. Keys that fail the query (e.g. never-written) are omitted.
  4. The empty-batch call is a valid no-op.

The DFS predicate (``is_dfs_replica``) is exercised structurally: the
descriptor for a memory-resident key must report every tier predicate
except memory as false, and the attribute must exist on the class so
callers can duck-type it against older wheels.

Prerequisites:
  - ``mooncake_master`` running (plain config; no offload flags needed).
  - ``DEFAULT_KV_LEASE_TTL`` env var matching the master's
    ``--default_kv_lease_ttl`` if the master was started with a
    non-default value.
"""

import os
import unittest

from mooncake.store import MooncakeDistributedStore

DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # ms
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)

SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(64 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(64 * 1024 * 1024)))


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
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


def tier_tags(descs):
    """Classify each descriptor with the first matching tier predicate,
    mirroring how an admission caller picks the fastest tier."""
    tags = []
    for info in descs:
        if info.is_memory_replica():
            tags.append("MEMORY")
        elif info.is_local_disk_replica():
            tags.append("LOCAL_DISK")
        elif info.is_disk_replica():
            tags.append("DISK")
        elif info.is_dfs_replica():
            tags.append("DFS")
        else:
            tags.append("UNKNOWN")
    return tags


class TestBatchQueryReadOnly(unittest.TestCase):
    """Python-binding test for batch_query_read_only + is_dfs_replica."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "store", None) is not None:
            cls.store.close()
            cls.store = None

    def test_same_descriptors_as_batch_get_replica_desc(self):
        """batch_query_read_only must classify tiers identically to
        batch_get_replica_desc for the same keys."""
        keys = [f"test_ro_query_{i}" for i in range(3)]
        test_data = b"test data for read-only query"

        for key in keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        try:
            leased = self.store.batch_get_replica_desc(keys)
            read_only = self.store.batch_query_read_only(keys)

            self.assertIsInstance(read_only, dict)
            for key in keys:
                self.assertIn(key, read_only)
                self.assertIn(key, leased)
                self.assertEqual(tier_tags(read_only[key]), tier_tags(leased[key]))
        finally:
            for key in keys:
                self.store.remove(key)

    def test_memory_replica_reports_single_tier(self):
        """A freshly-put key lives in memory: exactly the memory predicate
        fires, and the DFS predicate (the new binding) returns false."""
        key = "test_ro_query_memory_tier"
        self.assertEqual(self.store.put(key, b"memory tier data"), 0)

        try:
            descs = self.store.batch_query_read_only([key])
            self.assertIn(key, descs)
            self.assertTrue(len(descs[key]) > 0)

            for info in descs[key]:
                self.assertTrue(info.is_memory_replica())
                self.assertFalse(info.is_local_disk_replica())
                self.assertFalse(info.is_disk_replica())
                self.assertFalse(info.is_dfs_replica())
        finally:
            self.store.remove(key)

    def test_missing_keys_omitted(self):
        """Keys that fail the query must be absent from the result dict,
        matching batch_get_replica_desc semantics."""
        read_only = self.store.batch_query_read_only(["test_ro_query_never_written"])
        self.assertNotIn("test_ro_query_never_written", read_only)

        leased = self.store.batch_get_replica_desc(["test_ro_query_never_written"])
        self.assertNotIn("test_ro_query_never_written", leased)

    def test_empty_batch(self):
        """An empty key list is a valid no-op returning an empty dict."""
        self.assertEqual(self.store.batch_query_read_only([]), {})

    def test_mixed_batch(self):
        """A batch mixing existing and missing keys returns descriptors
        only for the existing ones."""
        key = "test_ro_query_mixed_present"
        self.assertEqual(self.store.put(key, b"mixed batch data"), 0)

        try:
            read_only = self.store.batch_query_read_only(
                [key, "test_ro_query_mixed_absent"]
            )
            self.assertIn(key, read_only)
            self.assertNotIn("test_ro_query_mixed_absent", read_only)
        finally:
            self.store.remove(key)


if __name__ == "__main__":
    unittest.main()
