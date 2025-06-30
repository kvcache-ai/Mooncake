import unittest
import os
import time
import threading
import random
from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_KV_LEASE_TTL = 200 # 200 milliseconds
# This test need to set the environment valid client storage root path. 
# Put data is more than segment size,so some data will be eviced ,and get from local file later
def get_client(store):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "eth0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
    global_segment_size = 512 * 1024 * 1024  # 3200 MB
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

    # @unittest.skipUnless(os.getenv("MOONCAKE_STORAGE_ROOT_DIR"), 
    #                      "Skipping test_put_get_in_evict_operations because SSD environment variable is not set")
    def test_put_get_in_evict_operations(self):
        """Test basic Put/Get operations with eviction scenario
        
        Verifies:
        1. Data eviction to file when exceeding memory capacity
        2. Correct retrieval of evicted data
        """ 
        VALUE_SIZE = 1024*1024 
        MAX_REQUESTS = 1000
        reference = {}

        # --------------------------
        # Phase 1: Data population (trigger eviction)
        # --------------------------
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            retcode = self.store.put(key, value)
            if retcode == -200:
                # The space is not enough, continue to next operation
                continue
            elif retcode == 0:
                reference[key] = value
            else:
                raise RuntimeError(f"Put operation failed for key {key}. Error code: {error_code}")
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
        time.sleep(5)


        # --------------------------
        # Phase 2: Data verification
        # --------------------------
        index = 0
        count = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            retrieved = self.store.get(key)
            if len(retrieved) != 0:
                expected = reference.get(key)
                self.assertEqual(retrieved, expected, "Data mismatch for key:" + key)
                count = count + 1
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
            
        print("Total get count:", count)    
        # --------------------------
        # Phase 3: Cleanup
        # --------------------------
        time.sleep(DEFAULT_KV_LEASE_TTL / 1000)
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            self.store.remove(key)
            index = index + 1
        print("Cleanup completed")  

    # @unittest.skipUnless(os.getenv("MOONCAKE_STORAGE_ROOT_DIR"), 
    #                      "Skipping test_concurrent_stress because SSD environment variable is not set")
    def test_concurrent_stress(self):
        """Multi-threaded stress test for Put/Get operations
        
        Verifies:
        1. Correctness under concurrent access
        2. Thread safety
        """
        NUM_THREADS = 4
        VALUE_SIZE = 1024*1024
        OPERATIONS_PER_THREAD = 1000

        thread_exceptions = []
        references = {}
        index = 0
        while index < OPERATIONS_PER_THREAD:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            references[key] = value
            index = index + 1
        
        def worker(thread_id):
            try:
                print(f"Thread {thread_id} started") 
                # Generate test data (1MB)
                
                # Put operations
                index = 0
                while index < OPERATIONS_PER_THREAD:
                    key = "k_" + str(index)
                    test_data = references[key]
                    result = self.store.put(key, test_data)
                    index = index + 1
                    if index % 500 == 0:
                        print("put", "completed", index, "entries")
            

                # Get operations
                index = 0
                while index < OPERATIONS_PER_THREAD:
                    key = "k_" + str(index)
                    retrieved_data = self.store.get(key)
                    if len(retrieved_data) != 0:
                        expected_value = references[key]
                        self.assertEqual(retrieved_data, expected_value, "Data mismatch for key:" + key)
                    index = index + 1
                    if index % 500 == 0:
                        print("get", "completed", index, "entries")

                print(f"Thread {thread_id} completed")  
                
            except Exception as e:
                thread_exceptions.append(f"Thread {thread_id} failed: {str(e)}")
                print("Exception in thread:", str(e))
        
        
        
        # Create and start threads
        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(i,), name=f"Worker-{i}")
            threads.append(t)
            t.start()

        # Join all threads
        for t in threads:
            t.join()

        # Check for any exceptions
        self.assertEqual(len(thread_exceptions), 0, "\n".join(thread_exceptions))

        # Remove all keys
        time.sleep(DEFAULT_KV_LEASE_TTL / 1000)
        index = 0
        while index < OPERATIONS_PER_THREAD:
            key = "k_" + str(index)
            self.store.remove(key)
            index += 1
        print("Cleanup completed")  

    def test_batch_get_in_evict_operations(self):
        """Test batch Put/Get operations with eviction scenario
        
        Verifies:
        1. Data eviction to file when exceeding memory capacity using batch operations
        2. Correct retrieval of evicted data in batches
        """ 
        VALUE_SIZE = 1024 * 1024 
        MAX_REQUESTS = 1000
        BATCH_SIZE = 4
        reference = {}

        # --------------------------
        # Phase 1: data population (trigger eviction)
        # --------------------------
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            retcode = self.store.put(key, value)
            if retcode == -200:
                # The space is not enough, continue to next operation
                continue
            elif retcode == 0:
                reference[key] = value
            else:
                raise RuntimeError(f"Put operation failed for key {key}. Error code: {error_code}")
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
        time.sleep(5)

        # --------------------------
        # Phase 2: Batch data verification using batch_get_into
        # --------------------------
        import ctypes

        # Use a large spacing between buffers to avoid any overlap detection
        buffer_spacing = 2 * 1024 * 1024  # 2MB spacing between buffers

        # Allocate one large buffer with significant spacing
        total_buffer_size = buffer_spacing * BATCH_SIZE
        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)

        # Register the entire large buffer once
        result = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(result, 0, "Buffer registration should succeed")

        index = 0
        count = 0
        while index < MAX_REQUESTS:
            batch_keys = []
            
            # Prepare a batch of keys to get
            for _ in range(BATCH_SIZE):
                if index >= MAX_REQUESTS:
                    break
                key = "k_" + str(index)
                batch_keys.append(key)
                index += 1
                
            if not batch_keys:  # No more keys to process
                break
                
            # Prepare buffer pointers and sizes for this batch
            buffer_ptrs = []
            buffer_sizes = []
            
            for i in range(len(batch_keys)):
                offset = i * buffer_spacing
                buffer_ptr = large_buffer_ptr + offset
                buffer_ptrs.append(buffer_ptr)
                buffer_sizes.append(VALUE_SIZE)  # Each value is 1MB
                
            # Execute batch_get_into
            results = self.store.batch_get_into(batch_keys, buffer_ptrs, buffer_sizes)
            
            # Verify results
            self.assertEqual(len(results), len(batch_keys), "Should return result for each key")
            
            for i, result in enumerate(results):
                current_key = batch_keys[i]
                expected = reference.get(current_key)
                
                if expected is not None:
                    if result > 0:
                        self.assertEqual(result, len(expected), f"Should read correct number of bytes for key {current_key}")
                        
                        # Verify data integrity - read from the correct offset in the large buffer
                        offset = i * buffer_spacing
                        read_data = bytes(large_buffer[offset:offset + result])
                        self.assertEqual(read_data, expected, f"Data should match for key {current_key}")
                        count += 1
            
            if index % 500 == 0:
                print("completed", index, "entries")
        
        print("Total get count:", count)
        
        # --------------------------
        # Phase 3: Cleanup (still using single remove for simplicity)
        # --------------------------
        time.sleep(DEFAULT_KV_LEASE_TTL / 1000)
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            self.store.remove(key)
            index += 1
        print("Cleanup completed")
    

if __name__ == "__main__":
    unittest.main()


