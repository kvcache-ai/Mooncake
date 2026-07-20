import unittest
import os
import time
import threading
import random

try:
    import torch
except ImportError:
    torch = None

from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000 # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL))


def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    mem_pool_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = (
        local_buffer_size_param if local_buffer_size_param is not None
        else 512 * 1024 * 1024  # 512 MB
    )
    real_client_address = "127.0.0.1:50052"

    retcode = store.setup_dummy(
        mem_pool_size,
        local_buffer_size,
        real_client_address
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

    def test_get_into_ranges_operations(self):
        """Test buffer-major multi-key range reads through the dummy client."""
        import ctypes

        key1 = "test_dummy_get_into_ranges_key_1"
        key2 = "test_dummy_get_into_ranges_key_2"
        data1 = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        data2 = b"abcdefghijklmnopqrstuvwxyz0123456789"
        buffer_size = 32

        self.assertEqual(self.store.put(key1, data1), 0)
        self.assertEqual(self.store.put(key2, data2), 0)

        buffer_ptr0 = self.store.alloc_from_mem_pool(buffer_size)
        buffer_ptr1 = self.store.alloc_from_mem_pool(buffer_size)
        buffer0 = (ctypes.c_ubyte * buffer_size).from_address(buffer_ptr0)
        buffer1 = (ctypes.c_ubyte * buffer_size).from_address(buffer_ptr1)
        self.assertEqual(self.store.register_buffer(buffer_ptr0, buffer_size), 0)
        self.assertEqual(self.store.register_buffer(buffer_ptr1, buffer_size), 0)

        ctypes.memset(buffer_ptr0, ord("_"), buffer_size)
        ctypes.memset(buffer_ptr1, ord("_"), buffer_size)

        results = self.store.get_into_ranges(
            [buffer_ptr0, buffer_ptr1],
            [[key1, key2], [key2, key1]],
            [[[0, 20], [8]], [[4], [16]]],
            [[[2, 30], [10]], [[0], [12]]],
            [[[4, 3], [6]], [[6], [4]]],
        )

        self.assertEqual(results, [[[4, 3], [6]], [[6], [4]]])
        self.assertEqual(bytes(buffer0[0:4]), data1[2:6])
        self.assertEqual(bytes(buffer0[8:14]), data2[10:16])
        self.assertEqual(bytes(buffer0[20:23]), data1[30:33])
        self.assertEqual(bytes(buffer1[4:10]), data2[0:6])
        self.assertEqual(bytes(buffer1[16:20]), data1[12:16])

        mismatch_results = self.store.get_into_ranges(
            [buffer_ptr0], [[key1, key2]], [[[0], []]], [[[0, 1], []]], [[[4, 4], []]]
        )
        self.assertEqual(len(mismatch_results), 1)
        self.assertEqual(len(mismatch_results[0]), 2)
        self.assertLess(mismatch_results[0][0][0], 0)
        self.assertEqual(len(mismatch_results[0][1]), 0)

        source_overflow_results = self.store.get_into_ranges(
            [buffer_ptr0], [[key1]], [[[0]]], [[[len(data1) - 1]]], [[[4]]]
        )
        self.assertLess(source_overflow_results[0][0][0], 0)

        destination_overflow_results = self.store.get_into_ranges(
            [buffer_ptr0], [[key1]], [[[buffer_size - 2]]], [[[0]]], [[[4]]]
        )
        self.assertLess(destination_overflow_results[0][0][0], 0)

        missing_key_results = self.store.get_into_ranges(
            [buffer_ptr0], [["missing-key", key1]], [[[0], [8]]], [[[0], [0]]], [[[4], [4]]]
        )
        self.assertLess(missing_key_results[0][0][0], 0)
        self.assertEqual(missing_key_results[0][1][0], 4)

        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(buffer_ptr0), 0)
        self.assertEqual(self.store.unregister_buffer(buffer_ptr1), 0)
        self.assertEqual(self.store.remove(key1), 0)
        self.assertEqual(self.store.remove(key2), 0)

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

    def test_tensor_operations(self):
        """Test tensor wrappers through dummy-client shared memory."""
        import ctypes

        if torch is None:
            self.skipTest("PyTorch is not available")

        key = f"test_dummy_tensor_{os.getpid()}"
        key_from = f"{key}_from"
        batch_keys = [f"{key}_batch_{i}" for i in range(2)]
        upsert_key = f"{key}_upsert"
        pub_key = f"{key}_pub"
        tp_key = f"{key}_tp"
        batch_tp_key = f"{key}_batch_tp"
        parallel_key = f"{key}_parallel"
        parallel_batch_keys = [f"{key}_parallel_batch_{i}" for i in range(2)]
        parallel_from_key = f"{key}_parallel_from"
        parallel_upsert_key = f"{key}_parallel_upsert"
        parallel_upsert_from_key = f"{key}_parallel_upsert_from"
        cleanup_keys = [key, key_from, *batch_keys, upsert_key, pub_key]
        cleanup_keys.extend(
            [
                parallel_key,
                *parallel_batch_keys,
                parallel_from_key,
                parallel_upsert_key,
                parallel_upsert_from_key,
            ]
        )
        cleanup_keys.extend(f"{tp_key}_tp_{rank}" for rank in range(2))
        cleanup_keys.extend(f"{batch_tp_key}_tp_{rank}" for rank in range(2))
        tensor = torch.arange(12, dtype=torch.float32).reshape(3, 4)

        self.assertEqual(self.store.put_tensor(key, tensor), 0)
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.dtype, tensor.dtype)
        self.assertEqual(tuple(retrieved.shape), tuple(tensor.shape))
        self.assertTrue(torch.equal(retrieved, tensor))

        buffer_size = self.store.get_size(key)
        self.assertGreater(buffer_size, 0)
        buffer_ptr = self.store.alloc_from_mem_pool(buffer_size)
        self.assertNotEqual(buffer_ptr, 0)
        buffer = (ctypes.c_ubyte * buffer_size).from_address(buffer_ptr)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_size), 0)
        try:
            ctypes.memset(buffer_ptr, 0, buffer_size)
            into = self.store.get_tensor_into(key, buffer_ptr, buffer_size)
            self.assertIsNotNone(into)
            self.assertTrue(torch.equal(into, tensor))

            mismatch = self.store.batch_get_tensor_into(
                [key, f"{key}_missing_arg"], [buffer_ptr], [buffer_size]
            )
            self.assertEqual(len(mismatch), 2)
            self.assertTrue(all(result < 0 for result in mismatch))

            self.assertEqual(
                self.store.put_tensor_from(key_from, buffer_ptr, buffer_size),
                0,
            )
            from_tensor = self.store.get_tensor(key_from)
            self.assertIsNotNone(from_tensor)
            self.assertTrue(torch.equal(from_tensor, tensor))

            batch_tensors = [tensor, tensor + 1]
            batch_results = self.store.batch_put_tensor(batch_keys, batch_tensors)
            self.assertEqual(list(batch_results), [0, 0])
            batch_retrieved = self.store.batch_get_tensor(batch_keys)
            for expected, actual in zip(batch_tensors, batch_retrieved):
                self.assertIsNotNone(actual)
                self.assertTrue(torch.equal(actual, expected))

            updated = tensor + 2
            self.assertEqual(self.store.upsert_tensor(upsert_key, updated), 0)
            self.assertTrue(torch.equal(self.store.get_tensor(upsert_key), updated))

            self.assertEqual(self.store.pub_tensor(pub_key, tensor + 3), 0)
            self.assertTrue(torch.equal(self.store.get_tensor(pub_key), tensor + 3))

            self.assertEqual(
                self.store.put_tensor_with_tp(
                    tp_key, tensor, tp_size=2, split_dim=1
                ),
                0,
            )
            tp_shards = torch.chunk(tensor, 2, dim=1)
            for rank, expected in enumerate(tp_shards):
                actual = self.store.get_tensor_with_tp(
                    tp_key, tp_rank=rank, tp_size=2
                )
                self.assertIsNotNone(actual)
                self.assertTrue(torch.equal(actual, expected.contiguous()))

            batch_tp_results = self.store.batch_put_tensor_with_tp(
                [batch_tp_key], [tensor], tp_size=2, split_dim=1
            )
            self.assertEqual(list(batch_tp_results), [0])

            self.assertEqual(
                self.store.put_tensor_with_parallelism(parallel_key, tensor),
                0,
            )
            self.assertTrue(torch.equal(self.store.get_tensor(parallel_key), tensor))

            parallel_batch = [tensor + 4, tensor + 5]
            parallel_batch_results = self.store.batch_put_tensor_with_parallelism(
                parallel_batch_keys, parallel_batch
            )
            self.assertEqual(list(parallel_batch_results), [0, 0])
            parallel_batch_retrieved = self.store.batch_get_tensor(
                parallel_batch_keys
            )
            for expected, actual in zip(parallel_batch, parallel_batch_retrieved):
                self.assertIsNotNone(actual)
                self.assertTrue(torch.equal(actual, expected))

            self.assertEqual(
                self.store.put_tensor_with_parallelism_from(
                    parallel_from_key, buffer_ptr, buffer_size
                ),
                0,
            )
            self.assertTrue(
                torch.equal(self.store.get_tensor(parallel_from_key), tensor)
            )

            parallel_update = tensor + 6
            self.assertEqual(
                self.store.upsert_tensor_with_parallelism(
                    parallel_upsert_key, parallel_update
                ),
                0,
            )
            self.assertTrue(
                torch.equal(
                    self.store.get_tensor(parallel_upsert_key), parallel_update
                )
            )

            parallel_upsert_from_results = (
                self.store.batch_upsert_tensor_with_parallelism_from(
                    [parallel_upsert_from_key], [buffer_ptr], [buffer_size]
                )
            )
            self.assertEqual(list(parallel_upsert_from_results), [0])
            self.assertTrue(
                torch.equal(
                    self.store.get_tensor(parallel_upsert_from_key), tensor
                )
            )
        finally:
            self.store.unregister_buffer(buffer_ptr)
            for cleanup_key in cleanup_keys:
                self.store.remove(cleanup_key)

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
