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
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
    
    retcode = store.setup(
        local_hostname, 
        metadata_server, 
        global_segment_size,
        local_buffer_size, 
        protocol, 
        device_name,
        master_server_address
    )
    
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")

class TestZeroLocalBufferSize(unittest.TestCase):
    """Test class for zero local buffer size scenarios."""
    
    def test_zero_local_buffer_size(self):
        """Test that put operations fail when local_buffer_size is set to zero."""
        test_data = b"test_zero_buffer_value"
        key = "test_zero_buffer_key"
        
        # Create a new store instance with zero local buffer size
        zero_buffer_store = MooncakeDistributedStore()
        get_client(zero_buffer_store, local_buffer_size_param=0)
        
        # Attempt to put data - this should fail
        result = zero_buffer_store.put(key, test_data)
        self.assertNotEqual(result, 0, "Put operation should fail with zero local buffer size")
        
        # Verify that the key doesn't exist
        result = zero_buffer_store.is_exist(key)
        self.assertEqual(result, 0, "Key should not exist after failed put")

class TestDistributedObjectStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)
    
    @unittest.skipIf(os.getenv("MOONCAKE_STORAGE_ROOT_DIR"), 
                     "Skipping test_client_tear_down because SSD environment variable is set")
    def test_client_tear_down(self):
        """Test client tear down and re-initialization."""
        test_data = b"Hello, World!"
        key = "test_teardown_key"
        
        # Put data and verify teardown clears it
        self.assertEqual(self.store.put(key, test_data), 0)
        self.assertEqual(self.store.close(), 0)
        time.sleep(1)  # Allow time for teardown to complete
        
        # Re-initialize the store
        get_client(self.store)
        
        # Verify data is gone after teardown
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, b"")
        
        # Verify store is functional after re-initialization
        self.assertEqual(self.store.put(key, test_data), 0)
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)
            
    def test_basic_get_hostname(self):
        # No port is set in the config, so it should pick a random one between 12300 and 14300
        hostname = self.store.get_hostname()
        self.assertTrue(hostname.startswith("localhost:"))
        port = int(hostname.split(":")[1])
        self.assertTrue(12300 <= port <= 14300)


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
        

    
    def test_zero_copy_operations(self):
        """Test zero-copy get_into and put_from operations."""
        import ctypes

        # Test data
        test_data = b"Hello, Zero-Copy World! " * 1000  # ~24KB test data
        key = "test_zero_copy_key"

        # Allocate a buffer and register it
        buffer_size = len(test_data) + 1024  # Extra space for safety
        buffer = (ctypes.c_ubyte * buffer_size)()
        buffer_ptr = ctypes.addressof(buffer)

        # Register the buffer for zero-copy operations
        result = self.store.register_buffer(buffer_ptr, buffer_size)
        self.assertEqual(result, 0, "Buffer registration should succeed")

        # Copy test data to buffer
        ctypes.memmove(buffer, test_data, len(test_data))

        # Test put_from (zero-copy write)
        result = self.store.put_from(key, buffer_ptr, len(test_data))
        self.assertEqual(result, 0, "put_from should succeed")

        # Verify data was stored correctly using regular get
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data, "Data should match after put_from")

        # Clear buffer for get_into test
        ctypes.memset(buffer, 0, buffer_size)

        # Test get_into (zero-copy read)
        bytes_read = self.store.get_into(key, buffer_ptr, buffer_size)
        self.assertEqual(bytes_read, len(test_data), "get_into should return correct byte count")

        # Verify data was read correctly
        read_data = bytes(buffer[:bytes_read])
        self.assertEqual(read_data, test_data, "Data should match after get_into")

        # Test error cases
        # Test get_into with buffer too small
        small_buffer_size = len(test_data) // 2
        small_buffer = (ctypes.c_ubyte * small_buffer_size)()
        small_buffer_ptr = ctypes.addressof(small_buffer)

        # Register small buffer
        result = self.store.register_buffer(small_buffer_ptr, small_buffer_size)
        self.assertEqual(result, 0, "Small buffer registration should succeed")

        # get_into should fail with buffer too small
        bytes_read = self.store.get_into(key, small_buffer_ptr, small_buffer_size)
        self.assertLess(bytes_read, 0, "get_into should fail with small buffer")

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0, "Buffer unregistration should succeed")
        self.assertEqual(self.store.unregister_buffer(small_buffer_ptr), 0)
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
        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        
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
        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        
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


    def test_concurrent_stress_with_barrier(self):
        """Test concurrent Put/Get operations with multiple threads using barrier."""
        NUM_THREADS = 8
        VALUE_SIZE = 1024 * 1024  # 1MB
        OPERATIONS_PER_THREAD = 100
        
        # Create barriers for synchronization
        start_barrier = threading.Barrier(NUM_THREADS + 1)  # +1 for main thread
        put_barrier = threading.Barrier(NUM_THREADS + 1)    # Barrier after put operations
        get_barrier = threading.Barrier(NUM_THREADS + 1)    # Barrier after get operations
        
        # Statistics for system-wide timing
        system_stats = {
            'put_start': 0,
            'put_end': 0,
            'get_start': 0,
            'get_end': 0
        }
        thread_exceptions = []
        
        def worker(thread_id):
            try:
                # Generate test data (1MB)
                test_data = os.urandom(VALUE_SIZE)
                thread_keys = [f"key_{thread_id}_{i}" for i in range(OPERATIONS_PER_THREAD)]
                
                # Wait for all threads to be ready
                start_barrier.wait()
                
                # Put operations
                for key in thread_keys:
                    result = self.store.put(key, test_data)
                    self.assertEqual(result, 0, f"Put operation failed for key {key}")
                
                # Wait for all threads to complete put operations
                put_barrier.wait()
                
                # Get operations
                for key in thread_keys:
                    retrieved_data = self.store.get(key)
                    self.assertEqual(len(retrieved_data), VALUE_SIZE, 
                                    f"Retrieved data size mismatch for key {key}")
                    self.assertEqual(retrieved_data, test_data, 
                                    f"Retrieved data content mismatch for key {key}")
                
                # Wait for all threads to complete get operations
                get_barrier.wait()
                
                # Remove all keys
                time.sleep(default_kv_lease_ttl / 1000)
                for key in thread_keys:
                    self.assertEqual(self.store.remove(key), 0)
                
                
            except Exception as e:
                thread_exceptions.append(f"Thread {thread_id} failed: {str(e)}")
        
        # Create and start threads
        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(i,), name=f"Worker-{i}")
            threads.append(t)
            t.start()
        
        # Wait for all threads to be ready and start the test
        start_barrier.wait()
        
        # Record put start time
        system_stats['put_start'] = time.time()
        
        # Wait for all put operations to complete
        put_barrier.wait()
        system_stats['put_end'] = time.time()
        
        # Record get start time
        system_stats['get_start'] = time.time()
        
        # Wait for all get operations to complete
        get_barrier.wait()
        system_stats['get_end'] = time.time()
        
        
        # Join all threads
        for t in threads:
            t.join()
        
        # Check for any exceptions
        self.assertEqual(len(thread_exceptions), 0, "\n".join(thread_exceptions))
        
        # Calculate system-wide statistics
        total_operations = NUM_THREADS * OPERATIONS_PER_THREAD
        put_duration = system_stats['put_end'] - system_stats['put_start']
        get_duration = system_stats['get_end'] - system_stats['get_start']
        total_data_size_gb = (VALUE_SIZE * total_operations) / (1024**3)
        
        print(f"\nConcurrent Stress Test Results:")
        print(f"Total threads: {NUM_THREADS}")
        print(f"Operations per thread: {OPERATIONS_PER_THREAD}")
        print(f"Total operations: {total_operations}")
        print(f"Data block size: {VALUE_SIZE/1024/1024:.2f}MB")
        print(f"Total data processed: {total_data_size_gb:.2f}GB")
        print(f"Put duration: {put_duration:.2f} seconds")
        print(f"Get duration: {get_duration:.2f} seconds")
        print(f"System Put throughput: {total_operations/put_duration:.2f} ops/sec")
        print(f"System Get throughput: {total_operations/get_duration:.2f} ops/sec")
        print(f"System Put bandwidth: {total_data_size_gb/put_duration:.2f} GB/sec")
        print(f"System Get bandwidth: {total_data_size_gb/get_duration:.2f} GB/sec")

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

    def test_put_with_config_parameter(self):
        """Test put method with config parameter."""
        from mooncake.store import ReplicateConfig
        
        test_data = b"Hello, Config World!"
        key = "test_put_config_key"
        
        # Test with default config (backward compatibility)
        result = self.store.put(key=key, value=test_data)
        self.assertEqual(result, 0)
        
        # Verify data
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.remove(key), 0)
        
        # Test with custom config
        config = ReplicateConfig()
        config.replica_num = 2
        
        key2 = "test_put_config_key2"
        result = self.store.put(key=key2, value=test_data, config=config)
        self.assertEqual(result, 0)
        
        # Verify data
        retrieved_data = self.store.get(key2)
        self.assertEqual(retrieved_data, test_data)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.remove(key2), 0)
        
        with self.assertRaises(TypeError):
            result = self.store.put(key_arg_name_error=key, value=test_data, config=config)
        
        with self.assertRaises(TypeError):
            result = self.store.put(key=key, value_arg_name_error=test_data, config=config)
            
        with self.assertRaises(TypeError):
            result = self.store.put(key=key, value=test_data, config_arg_name_error=config)


    def test_put_batch_with_config_parameter(self):
        """Test put_batch method with config parameter."""
        from mooncake.store import ReplicateConfig
        
        keys = ["test_batch_config_key1", "test_batch_config_key2", "test_batch_config_key3"]
        values = [b"Batch Data 1", b"Batch Data 2", b"Batch Data 3"]
        
        # Test with default config (backward compatibility)
        result = self.store.put_batch(keys, values)
        self.assertEqual(result, 0)
        
        # Verify data
        for key, expected_value in zip(keys, values):
            retrieved_data = self.store.get(key)
            self.assertEqual(retrieved_data, expected_value)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        for key in keys:
            self.assertEqual(self.store.remove(key), 0)
        
        # Test with custom config
        config = ReplicateConfig()
        config.replica_num = 2
        
        keys2 = ["test_batch_config_key4", "test_batch_config_key5", "test_batch_config_key6"]
        result = self.store.put_batch(keys=keys2, values=values, config=config)
        self.assertEqual(result, 0)
        
        # Verify data
        for key, expected_value in zip(keys2, values):
            retrieved_data = self.store.get(key)
            self.assertEqual(retrieved_data, expected_value)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        for key in keys2:
            self.assertEqual(self.store.remove(key), 0)

    def test_put_from_with_config_parameter(self):
        """Test put_from method with config parameter."""
        import ctypes
        from mooncake.store import ReplicateConfig
        
        test_data = b"Hello, put_from config world!"
        key = "test_put_from_config_key"
        buffer_size = len(test_data)
        
        # Allocate and register buffer
        buffer = (ctypes.c_ubyte * buffer_size)()
        buffer_ptr = ctypes.addressof(buffer)
        
        result = self.store.register_buffer(buffer_ptr, buffer_size)
        self.assertEqual(result, 0)
        
        # Copy test data to buffer
        ctypes.memmove(buffer, test_data, len(test_data))
        
        # Test with default config (backward compatibility)
        result = self.store.put_from(key=key, buffer_ptr=buffer_ptr, size=len(test_data))
        self.assertEqual(result, 0)
        
        # Verify data
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.remove(key), 0)
        
        # Test with custom config
        config = ReplicateConfig()
        config.replica_num = 2
        config.with_soft_pin = False
        
        key2 = "test_put_from_config_key2"
        result = self.store.put_from(key=key2, buffer_ptr=buffer_ptr, size=len(test_data), config=config)
        self.assertEqual(result, 0)
        
        # Verify data
        retrieved_data = self.store.get(key2)
        self.assertEqual(retrieved_data, test_data)
        
        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)
        self.assertEqual(self.store.remove(key2), 0)

    def test_batch_put_from_with_config_parameter(self):
        """Test batch_put_from method with config parameter."""
        import ctypes
        from mooncake.store import ReplicateConfig

        # Test data
        test_data = [
            b"Batch Config Data 1",
            b"Batch Config Data 2",
            b"Batch Config Data 3"
        ]
        keys = ["test_batch_put_from_config_key1", "test_batch_put_from_config_key2", "test_batch_put_from_config_key3"]

        # Use large spacing between buffers
        buffer_spacing = 1024 * 1024  # 1MB spacing
        total_buffer_size = buffer_spacing * len(test_data)
        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)

        # Register buffer
        result = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(result, 0)

        # Prepare individual buffers
        buffer_ptrs = []
        buffer_sizes = []

        for i, data in enumerate(test_data):
            offset = i * buffer_spacing
            buffer_ptr = large_buffer_ptr + offset

            # Copy test data to buffer
            ctypes.memmove(ctypes.c_void_p(buffer_ptr), data, len(data))

            buffer_ptrs.append(buffer_ptr)
            buffer_sizes.append(len(data))

        # Test with default config (backward compatibility)
        results = self.store.batch_put_from(keys=keys, buffer_ptrs=buffer_ptrs, sizes=buffer_sizes)
        self.assertEqual(len(results), len(keys))
        for result in results:
            self.assertEqual(result, 0)

        # Verify data
        for key, expected_data in zip(keys, test_data):
            retrieved_data = self.store.get(key)
            self.assertEqual(retrieved_data, expected_data)

        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        for key in keys:
            self.assertEqual(self.store.remove(key), 0)

        # Test with custom config
        config = ReplicateConfig()
        config.replica_num = 2
        config.with_soft_pin = False

        keys2 = ["test_batch_put_from_config_key4", "test_batch_put_from_config_key5", "test_batch_put_from_config_key6"]
        results = self.store.batch_put_from(keys=keys2, buffer_ptrs=buffer_ptrs, sizes=buffer_sizes, config=config)
        self.assertEqual(len(results), len(keys2))
        for result in results:
            self.assertEqual(result, 0)

        # Verify data
        for key, expected_data in zip(keys2, test_data):
            retrieved_data = self.store.get(key)
            self.assertEqual(retrieved_data, expected_data)

        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0)
        for key in keys2:
            self.assertEqual(self.store.remove(key), 0)

    def test_batch_get_buffer_operations(self):
        """Test batch_get_buffer operations for multiple keys."""
        # Test data
        test_data = [
            b"Batch Buffer Data 1! " * 100,  # ~2.1KB
            b"Batch Buffer Data 2! " * 200,  # ~4.2KB
        ]
        keys = ["test_batch_get_buffer_key1", "test_batch_get_buffer_key2"]

        # First, put the test data using regular put operations
        for key, data in zip(keys, test_data):
            result = self.store.put(key, data)
            self.assertEqual(result, 0, f"Failed to put data for key {key}")

        # Test 1: Batch get two successful keys
        results = self.store.batch_get_buffer(keys)

        # Verify results
        self.assertEqual(len(results), len(keys), "Should return result for each key")

        for i, (expected_data, buffer) in enumerate(zip(test_data, results)):
            self.assertIsNotNone(buffer, f"batch_get_buffer should succeed for key {keys[i]}")
            self.assertEqual(buffer.size(), len(expected_data), f"Buffer size should match for key {keys[i]}")

            # Verify data integrity using buffer protocol
            buffer_data = bytes(buffer)
            self.assertEqual(buffer_data, expected_data, f"Data should match for key {keys[i]}")

        # Test 2: Batch get one successful key and one non-existent key
        mixed_keys = [keys[0], "non_existent_key"]
        mixed_results = self.store.batch_get_buffer(mixed_keys)

        # Verify mixed results
        self.assertEqual(len(mixed_results), len(mixed_keys), "Should return result for each key")

        # First key should succeed
        self.assertIsNotNone(mixed_results[0], "First key should exist")
        self.assertEqual(mixed_results[0].size(), len(test_data[0]), "First buffer size should match")
        buffer_data = bytes(mixed_results[0])
        self.assertEqual(buffer_data, test_data[0], "First buffer data should match")

        # Second key should fail (return None)
        self.assertIsNone(mixed_results[1], "Second key should not exist")

        # Test edge cases
        # Test with empty keys list
        empty_results = self.store.batch_get_buffer([])
        self.assertEqual(len(empty_results), 0, "Should return empty results for empty input")

        # Test with single existing key
        single_result = self.store.batch_get_buffer([keys[0]])
        self.assertEqual(len(single_result), 1, "Should return one result")
        self.assertIsNotNone(single_result[0], "Single key should exist")

        # Test with single non-existent key
        non_existent_result = self.store.batch_get_buffer(["definitely_non_existent_key"])
        self.assertEqual(len(non_existent_result), 1, "Should return one result")
        self.assertIsNone(non_existent_result[0], "Non-existent key should return None")

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
        for key in keys:
            self.assertEqual(self.store.remove(key), 0)

if __name__ == '__main__':
    unittest.main()