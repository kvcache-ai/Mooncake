import unittest
import os
import time
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)


def get_clients(stores, local_buffer_size_param=None):
    """Initialize and setup the distributed store clients."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    base_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata")
    segment_size = 1600 * 1024 * 1024  # 1600 MB per segment
    local_buffer_size = (
        local_buffer_size_param
        if local_buffer_size_param is not None
        else 512 * 1024 * 1024  # 512 MB
    )
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")

    base_port = 12345
    for i, store in enumerate(stores):
        hostname = f"{base_hostname}:{base_port + i}"
        retcode = store.setup(
            hostname,
            metadata_server,
            segment_size,
            local_buffer_size,
            protocol,
            device_name,
            master_server_address,
        )

        if retcode:
            raise RuntimeError(f"Failed to setup segment. Return code: {retcode}")


def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    return get_clients([store], local_buffer_size_param)


class TestBatchReplicaClearSingleStore(unittest.TestCase):
    """Test batch_replica_clear with a single store (no replication)."""

    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_clear_after_lease_expiry(self):
        """Keys with expired leases should be cleared successfully."""
        keys = [f"test_clear_expired_{i}" for i in range(3)]
        test_data = b"test data for clear"

        # Put keys
        for key in keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        # Verify keys exist
        for key in keys:
            self.assertEqual(self.store.is_exist(key), 1)

        # Wait for lease to expire
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Clear replicas
        cleared = self.store.batch_replica_clear(keys)

        # Verify returned keys match what was cleared
        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), len(keys))
        for key in keys:
            self.assertIn(key, cleared)

        # Verify keys are no longer accessible
        for key in keys:
            retrieved = self.store.get(key)
            self.assertEqual(retrieved, b"")

    def test_clear_with_active_lease(self):
        """Keys with active leases should be skipped (not cleared)."""
        keys = [f"test_clear_active_{i}" for i in range(3)]
        test_data = b"test data active lease"

        # Put keys
        for key in keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        # Immediately try to clear (lease still active)
        cleared = self.store.batch_replica_clear(keys)

        # Active-lease keys should be skipped
        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), 0)

        # Verify keys still exist and are accessible
        for key in keys:
            self.assertEqual(self.store.is_exist(key), 1)
            retrieved = self.store.get(key)
            self.assertEqual(retrieved, test_data)

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
        for key in keys:
            self.store.remove(key)

    def test_clear_empty_keys(self):
        """Calling with an empty key list should return an empty list."""
        cleared = self.store.batch_replica_clear([])
        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), 0)

    def test_clear_nonexistent_keys(self):
        """Non-existent keys should be handled gracefully."""
        keys = ["nonexistent_key_1", "nonexistent_key_2", "nonexistent_key_3"]

        cleared = self.store.batch_replica_clear(keys)
        self.assertIsInstance(cleared, list)
        # Non-existent keys have no replicas to clear
        self.assertEqual(len(cleared), 0)

    def test_clear_mixed_expired_and_active(self):
        """Mix of expired and active-lease keys; only expired should clear."""
        expired_keys = [f"test_mixed_expired_{i}" for i in range(2)]
        active_keys = [f"test_mixed_active_{i}" for i in range(2)]
        test_data = b"mixed test data"

        # Put expired keys first
        for key in expired_keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        # Wait for lease to expire on first batch
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Put active keys (lease still valid)
        for key in active_keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        # Clear all keys at once
        all_keys = expired_keys + active_keys
        cleared = self.store.batch_replica_clear(all_keys)

        # Only expired keys should be cleared
        self.assertIsInstance(cleared, list)
        for key in expired_keys:
            self.assertIn(key, cleared)
        for key in active_keys:
            self.assertNotIn(key, cleared)

        # Verify active keys still accessible
        for key in active_keys:
            retrieved = self.store.get(key)
            self.assertEqual(retrieved, test_data)

        # Cleanup active keys
        time.sleep(default_kv_lease_ttl / 1000)
        for key in active_keys:
            self.store.remove(key)

    def test_clear_with_segment_name_self(self):
        """Clear with own hostname as segment_name removes MEMORY replicas."""
        key = "test_clear_segment_self"
        test_data = b"segment name test data"

        self.assertEqual(self.store.put(key, test_data), 0)

        # Wait for lease to expire
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Clear with own hostname
        hostname = self.store.get_hostname()
        cleared = self.store.batch_replica_clear([key], segment_name=hostname)

        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0], key)

    def test_clear_with_invalid_segment_name(self):
        """Clear with a non-matching segment_name should not remove anything."""
        key = "test_clear_invalid_segment"
        test_data = b"invalid segment test"

        self.assertEqual(self.store.put(key, test_data), 0)

        # Wait for lease to expire
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Clear with a segment name that doesn't host any replica
        cleared = self.store.batch_replica_clear(
            [key], segment_name="nonexistent_host:99999"
        )

        # No replica on that segment, so nothing cleared
        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), 0)

        # Key should still exist (other replicas untouched)
        self.assertEqual(self.store.is_exist(key), 1)

        # Cleanup
        self.store.remove(key)

    def test_clear_single_key(self):
        """Clearing a single key should work correctly."""
        key = "test_clear_single"
        test_data = b"single key clear test"

        self.assertEqual(self.store.put(key, test_data), 0)
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        cleared = self.store.batch_replica_clear([key])

        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0], key)
        self.assertEqual(self.store.get(key), b"")

    def test_clear_large_batch(self):
        """Clearing a large batch of keys should work correctly."""
        batch_size = 50
        keys = [f"test_clear_large_{i}" for i in range(batch_size)]
        test_data = b"large batch clear test"

        for key in keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        cleared = self.store.batch_replica_clear(keys)

        self.assertEqual(len(cleared), batch_size)
        for key in keys:
            self.assertIn(key, cleared)


class TestBatchReplicaClearReplication(unittest.TestCase):
    """Test batch_replica_clear with replication (multiple stores)."""

    @classmethod
    def setUpClass(cls):
        """Initialize the main store and additional stores for replication."""
        cls.store = MooncakeDistributedStore()

        cls.additional_stores = []
        cls.max_replicate_num = 2
        for _ in range(cls.max_replicate_num - 1):
            cls.additional_stores.append(MooncakeDistributedStore())

        get_clients([cls.store] + cls.additional_stores)

    def test_clear_replicated_key_all_replicas(self):
        """Clear all replicas of a replicated key (empty segment_name)."""
        key = "test_clear_replicated_all"
        test_data = b"replicated clear all test"

        config = ReplicateConfig()
        config.replica_num = self.max_replicate_num

        self.assertEqual(self.store.put(key, test_data, config=config), 0)

        # Verify accessible from both stores
        self.assertEqual(self.store.get(key), test_data)
        self.assertEqual(self.additional_stores[0].get(key), test_data)

        # Wait for lease to expire
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Clear all replicas
        cleared = self.store.batch_replica_clear([key])

        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0], key)

    def test_clear_specific_segment_replica(self):
        """Clear only MEMORY replicas on a specific segment."""
        key = "test_clear_specific_segment"
        test_data = b"specific segment clear test"

        config = ReplicateConfig()
        config.replica_num = self.max_replicate_num

        self.assertEqual(self.store.put(key, test_data, config=config), 0)

        # Wait for lease to expire
        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Clear only main store's replica
        main_hostname = self.store.get_hostname()
        cleared = self.store.batch_replica_clear([key], segment_name=main_hostname)

        self.assertIsInstance(cleared, list)
        self.assertEqual(len(cleared), 1)
        self.assertEqual(cleared[0], key)

        # Data should still be accessible from the other store's replica
        retrieved = self.additional_stores[0].get(key)
        self.assertEqual(retrieved, test_data)

        # Cleanup remaining replica
        self.store.remove(key)

    def test_clear_does_not_affect_other_keys(self):
        """Clearing specific keys should not affect unrelated keys."""
        clear_key = "test_clear_target"
        keep_key = "test_clear_keep"
        test_data = b"isolation test data"

        self.assertEqual(self.store.put(clear_key, test_data), 0)
        self.assertEqual(self.store.put(keep_key, test_data), 0)

        time.sleep(default_kv_lease_ttl / 1000 + 0.5)

        # Only clear one key
        cleared = self.store.batch_replica_clear([clear_key])

        self.assertIn(clear_key, cleared)
        self.assertNotIn(keep_key, cleared)

        # Kept key should still be accessible
        self.assertEqual(self.store.is_exist(keep_key), 1)
        self.assertEqual(self.store.get(keep_key), test_data)

        # Cleanup
        self.store.remove(keep_key)


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
