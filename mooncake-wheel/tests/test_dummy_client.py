import unittest
import os
import time
import threading
import random
from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000 # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL))


def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata")
    global_segment_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = (
        local_buffer_size_param if local_buffer_size_param is not None
        else 512 * 1024 * 1024  # 512 MB
    )
    real_client_address = "127.0.0.1:50052"

    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        real_client_address,
        use_dummy_client=True
    )

    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")

class TestDistributedObjectStoreSingleStore(unittest.TestCase):
    """Test class for single store operations (no replication)."""

    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    def test_basic_put_get_exist_operations(self):
        """Test basic Put/Get/Exist operations through the Python interface."""
        test_data = b"Hello, World!"
        key = "test_basic_key"

        # Test Put operation
        self.assertEqual(self.store.put(key, test_data), 0)

        # Verify data through Get operation
        self.assertEqual(self.store.get_size(key), len(test_data))
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)

        # Put again with the same key, should succeed
        self.assertEqual(self.store.put(key, test_data), 0)

        # Remove the key
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.remove(key), 0)

    def test_batch_is_exist_operations(self):
        """Test batch is_exist operations through the Python interface."""
        batch_size = 20
        test_data = b"Hello, Batch World!"

        # Create test keys
        keys = [f"test_batch_exist_key_{i}" for i in range(batch_size)]

        # Put only the first half of the keys
        existing_keys = keys[:batch_size // 2]
        for key in existing_keys:
            self.assertEqual(self.store.put(key, test_data), 0)

        # Test batch_is_exist with mixed existing and non-existing keys
        results = self.store.batch_is_exist(keys)

        # Verify results
        self.assertEqual(len(results), len(keys))

        # First half should exist (result = 1)
        for i in range(batch_size // 2):
            self.assertEqual(results[i], 1, f"Key {keys[i]} should exist but got {results[i]}")

        # Second half should not exist (result = 0)
        for i in range(batch_size // 2, batch_size):
            self.assertEqual(results[i], 0, f"Key {keys[i]} should not exist but got {results[i]}")

        # Test with empty keys list
        empty_results = self.store.batch_is_exist([])
        self.assertEqual(len(empty_results), 0)

        # Test with single key
        single_result = self.store.batch_is_exist([existing_keys[0]])
        self.assertEqual(len(single_result), 1)
        self.assertEqual(single_result[0], 1)

        # Test with non-existent key
        non_existent_result = self.store.batch_is_exist(["non_existent_key"])
        self.assertEqual(len(non_existent_result), 1)
        self.assertEqual(non_existent_result[0], 0)

        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        for key in existing_keys:
            self.assertEqual(self.store.remove(key), 0)

    def test_batch_get_into_operations(self):
        """Test batch_get_into operations for multiple keys."""
        import ctypes

        # Test data
        batch_size = 3
        test_data = [
            b"Hello, Batch World 1! " * 100,  # ~2.3KB
            b"Hello, Batch World 2! " * 200,  # ~4.6KB
            b"Hello, Batch World 3! " * 150,  # ~3.5KB
        ]
        keys = [f"test_batch_get_into_key_{i}" for i in range(batch_size)]

        # First, put the test data using regular put operations
        for i, (key, data) in enumerate(zip(keys, test_data)):
            result = self.store.put(key, data)
            self.assertEqual(result, 0, f"Failed to put data for key {key}")

        # Use a large spacing between buffers to avoid any overlap detection
        buffer_spacing = 1024 * 1024  # 1MB spacing between buffers

        # Allocate one large buffer with significant spacing
        total_buffer_size = buffer_spacing * batch_size
        large_buffer_ptr = self.store.alloc_from_mem_pool(total_buffer_size)
        large_buffer = (ctypes.c_char * total_buffer_size).from_address(large_buffer_ptr)

        # Register the entire large buffer once
        result = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(result, 0, "Buffer registration should succeed")

        # Create individual buffer views within the large buffer with spacing
        buffers = []
        buffer_ptrs = []
        buffer_sizes = []

        for i, data in enumerate(test_data):
            # Calculate offset with large spacing to avoid any overlap issues
            offset = i * buffer_spacing
            buffer_ptr = large_buffer_ptr + offset

            buffers.append(large_buffer)  # Keep reference to prevent GC
            buffer_ptrs.append(buffer_ptr)
            buffer_sizes.append(buffer_spacing)  # Use full spacing as buffer size

        # Test batch_get_into
        results = self.store.batch_get_into(keys, buffer_ptrs, buffer_sizes)

        # Verify results
        self.assertEqual(len(results), batch_size, "Should return result for each key")

        for i, (expected_data, result) in enumerate(zip(test_data, results)):
            self.assertGreater(result, 0, f"batch_get_into should succeed for key {keys[i]}")
            self.assertEqual(result, len(expected_data), f"Should read correct number of bytes for key {keys[i]}")

            # Verify data integrity - read from the correct offset in the large buffer
            offset = i * buffer_spacing
            read_data = bytes(large_buffer[offset:offset + result])
            self.assertEqual(read_data, expected_data, f"Data should match for key {keys[i]}")

        # Test error cases
        # Test with mismatched array sizes
        mismatched_results = self.store.batch_get_into(keys[:2], buffer_ptrs[:3], buffer_sizes[:3])
        self.assertEqual(len(mismatched_results), 2, "Should return results for provided keys")
        for result in mismatched_results:
            self.assertLess(result, 0, "Should fail with mismatched array sizes")

        # Test with empty arrays
        empty_results = self.store.batch_get_into([], [], [])
        self.assertEqual(len(empty_results), 0, "Should return empty results for empty input")

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration should succeed")
        for key in keys:
            self.assertEqual(self.store.remove(key), 0)

    def test_batch_put_from_operations(self):
        """Test batch_put_from operations for multiple keys."""
        import ctypes

        # Test data
        batch_size = 3
        test_data = [
            b"Batch Put Data 1! " * 100,  # ~1.8KB
            b"Batch Put Data 2! " * 200,  # ~3.6KB
            b"Batch Put Data 3! " * 150,  # ~2.7KB
        ]
        keys = [f"test_batch_put_from_key_{i}" for i in range(batch_size)]

        # Use a large spacing between buffers to avoid any overlap detection
        buffer_spacing = 1024 * 1024  # 1MB spacing between buffers

        # Allocate one large buffer with significant spacing
        total_buffer_size = buffer_spacing * batch_size
        large_buffer_ptr = self.store.alloc_from_mem_pool(total_buffer_size)
        large_buffer = (ctypes.c_char * total_buffer_size).from_address(large_buffer_ptr)

        # Register the entire large buffer once
        result = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(result, 0, "Buffer registration should succeed")

        # Create individual buffer views within the large buffer with spacing
        buffers = []
        buffer_ptrs = []
        buffer_sizes = []

        for i, data in enumerate(test_data):
            # Calculate offset with large spacing to avoid any overlap issues
            offset = i * buffer_spacing
            buffer_ptr = large_buffer_ptr + offset

            # Copy test data to buffer
            ctypes.memmove(ctypes.c_void_p(buffer_ptr), data, len(data))

            buffers.append(large_buffer)  # Keep reference to prevent GC
            buffer_ptrs.append(buffer_ptr)
            buffer_sizes.append(len(data))  # Use actual data size for put_from

        # Test batch_put_from
        results = self.store.batch_put_from(keys, buffer_ptrs, buffer_sizes)

        # Verify results
        self.assertEqual(len(results), batch_size, "Should return result for each key")

        for i, result in enumerate(results):
            self.assertEqual(result, 0, f"batch_put_from should succeed for key {keys[i]}")

        # Verify data was stored correctly using regular get
        for i, (key, expected_data) in enumerate(zip(keys, test_data)):
            retrieved_data = self.store.get(key)
            self.assertEqual(retrieved_data, expected_data, f"Data should match after batch_put_from for key {key}")

        # Test error cases
        # Test with mismatched array sizes
        mismatched_results = self.store.batch_put_from(keys[:2], buffer_ptrs[:3], buffer_sizes[:3])
        self.assertEqual(len(mismatched_results), 2, "Should return results for provided keys")
        for result in mismatched_results:
            self.assertLess(result, 0, "Should fail with mismatched array sizes")

        # Test with empty arrays
        empty_results = self.store.batch_put_from([], [], [])
        self.assertEqual(len(empty_results), 0, "Should return empty results for empty input")

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration should succeed")
        for key in keys:
            self.assertEqual(self.store.remove(key), 0)

    # Mark this test as zzz_ so that it is the last test to run
    def zzz_test_dict_fuzz_e2e(self):
         """End-to-end fuzz test comparing distributed store behavior with dict.
         Performs ~1000 random operations (put, get, remove) with random value sizes between 1KB and 64MB.
         After testing, all keys are removed.
         """
         import random
         # Local reference dict to simulate expected dict behavior
         reference = {}
         operations = 1000
         # Use a pool of keys to limit memory consumption
         keys_pool = [f"key_{i}" for i in range(100)]
         # Track which keys have values assigned to ensure consistency
         key_values = {}
         # Fuzz record for debugging in case of errors
         fuzz_record = []
         try:
             for i in range(operations):
                 op = random.choice(["put", "get", "remove"])
                 key = random.choice(keys_pool)
                 if op == "put":
                     # If key already exists, use the same value to ensure consistency
                     if key in key_values:
                         value = key_values[key]
                         size = len(value)
                     else:
                         size = random.randint(1, 64 * 1024 * 1024)
                         value = os.urandom(size)
                         key_values[key] = value

                     fuzz_record.append(f"{i}: put {key} [size: {size}]")
                     error_code = self.store.put(key, value)
                     if error_code == -200:
                         # The space is not enough, continue to next operation
                         continue
                     elif error_code == 0:
                         reference[key] = value
                     else:
                         raise RuntimeError(f"Put operation failed for key {key}. Error code: {error_code}")
                 elif op == "get":
                     fuzz_record.append(f"{i}: get {key}")
                     retrieved = self.store.get(key)
                     if retrieved != b"": # Otherwise the key may have been evicted
                        expected = reference.get(key, b"")
                        self.assertEqual(retrieved, expected)
                 elif op == "remove":
                     fuzz_record.append(f"{i}: remove {key}")
                     error_code = self.store.remove(key)
                     # if remove did not fail due to the key has a lease
                     if error_code != -706:
                        reference.pop(key, None)
                        # Also remove from key_values to allow new value if key is reused
                        key_values.pop(key, None)
         except Exception as e:
             print(f"Error: {e}")
             print('\nFuzz record (operations so far):')
             for record in fuzz_record:
                 print(record)
             raise e
         # Cleanup: ensure all remaining keys are removed
         time.sleep(default_kv_lease_ttl / 1000)
         for key in list(reference.keys()):
             self.store.remove(key)

    def test_replicate_config_creation_and_properties(self):
        """Test ReplicateConfig class creation and property access."""
        from mooncake.store import ReplicateConfig

        # Test default constructor
        config = ReplicateConfig()
        self.assertEqual(config.replica_num, 1)
        self.assertEqual(config.with_soft_pin, False)
        self.assertEqual(config.preferred_segment, "")

        # Test property assignment
        config.replica_num = 3
        config.with_soft_pin = True
        config.preferred_segment = "node1:12345"

        self.assertEqual(config.replica_num, 3)
        self.assertEqual(config.with_soft_pin, True)
        self.assertEqual(config.preferred_segment, "node1:12345")

        # Test string representation
        config_str = str(config)
        self.assertIsInstance(config_str, str)
        self.assertIn("3", config_str)  # Should contain replica_num

if __name__ == '__main__':
    # Show which test is running; stop on first failure
    unittest.main(verbosity=2, failfast=True)
