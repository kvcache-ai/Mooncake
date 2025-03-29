import unittest
import os
import time
import threading
import random
from mooncake import MooncakeDistributedStore


def get_client(store):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
    global_segment_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = 512 * 1024 * 1024     # 512 MB
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


class TestDistributedObjectStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize the store once for all tests."""
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)
    
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

    def test_basic_put_get_exist_operations(self):
        """Test basic Put/Get/Exist operations through the Python interface."""
        test_data = b"Hello, World!"
        key = "test_basic_key"

        # Test Put operation
        self.assertEqual(self.store.put(key, test_data), 0)

        # Verify data through Get operation
        self.assertEqual(self.store.getSize(key), len(test_data))
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)

        # Put again with the same key, should succeed
        self.assertEqual(self.store.put(key, test_data), 0)

        # Remove the key
        self.assertEqual(self.store.remove(key), 0)

        # Get after remove should return empty bytes
        self.assertLess(self.store.getSize(key), 0)
        empty_data = self.store.get(key)
        self.assertEqual(empty_data, b"")

        # Test isExist functionality
        test_data_2 = b"Testing exists!"
        key_2 = "test_exist_key"
        
        # Should not exist initially
        self.assertLess(self.store.getSize(key_2), 0)
        self.assertEqual(self.store.isExist(key_2), 0)
        
        # Should exist after put
        self.assertEqual(self.store.put(key_2, test_data_2), 0)
        self.assertEqual(self.store.isExist(key_2), 1)
        self.assertEqual(self.store.getSize(key_2), len(test_data_2))
        
        # Should not exist after remove
        self.assertEqual(self.store.remove(key_2), 0)
        self.assertLess(self.store.getSize(key_2), 0)
        self.assertEqual(self.store.isExist(key_2), 0)

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

    def test_dict_fuzz_e2e(self):
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
                     expected = reference.get(key, b"")
                     self.assertEqual(retrieved, expected)
                 elif op == "remove":
                     fuzz_record.append(f"{i}: remove {key}")
                     self.store.remove(key)
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
         for key in list(reference.keys()):
             self.store.remove(key)
             
if __name__ == '__main__':
    unittest.main()
