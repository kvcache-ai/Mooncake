import unittest
import os
import time
import threading
import random
from mooncake.store import MooncakeDistributedStore
import statistics
import math
from collections import defaultdict

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

class TestStats:
    """Helper class for collecting and reporting test statistics."""
    def __init__(self, test_name):
        self.test_name = test_name
        self.reset()
        
    def reset(self):
        self.start_time = 0
        self.end_time = 0
        self.operation_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.latencies = []
        self.error_codes = defaultdict(int)
        self.data_transferred = 0
        
    def start_timer(self):
        self.start_time = time.perf_counter()
        
    def stop_timer(self):
        self.end_time = time.perf_counter()
        
    def get_duration(self):
        return self.end_time - self.start_time if self.end_time > self.start_time else 0
        
    def record_operation(self, success, latency, data_size=0, error_code=None):
        self.operation_count += 1
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1
        if error_code is not None:
            self.error_codes[error_code] += 1
        if latency is not None:
            self.latencies.append(latency)
        if data_size > 0:
            self.data_transferred += data_size
            
    def print_stats(self):
        """Print comprehensive statistics for the test."""
        duration = self.get_duration()
        success_rate = (self.success_count / self.operation_count * 100) if self.operation_count > 0 else 0
        
        # Calculate latency statistics
        min_latency = min(self.latencies) * 1000 if self.latencies else 0
        max_latency = max(self.latencies) * 1000 if self.latencies else 0
        avg_latency = (sum(self.latencies) / len(self.latencies) * 1000) if self.latencies else 0
        
        # Calculate percentiles
        p90 = p99 = p999 = 0
        if self.latencies:
            sorted_latencies = sorted(self.latencies)
            p90 = sorted_latencies[int(len(sorted_latencies) * 0.9)] * 1000
            p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] * 1000 if len(sorted_latencies) > 100 else p90
            p999 = sorted_latencies[int(len(sorted_latencies) * 0.999)] * 1000 if len(sorted_latencies) > 1000 else p99
        
        # Calculate throughput
        ops_per_sec = self.operation_count / duration if duration > 0 else 0
        mb_per_sec = self.data_transferred / duration / (1024 * 1024) if duration > 0 else 0
        
        print(f"\n=== {self.test_name} STATISTICS ===")
        print(f"Operations: {self.operation_count} (Success: {self.success_count}, Failure: {self.failure_count})")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Data Transferred: {self.data_transferred / (1024*1024):.2f} MB")
        print(f"Latency (ms): Min={min_latency:.2f}, Max={max_latency:.2f}, Avg={avg_latency:.2f}")
        print(f"Percentiles (ms): P90={p90:.2f}, P99={p99:.2f}, P999={p999:.2f}")
        print(f"Throughput: {ops_per_sec:.2f} ops/sec, {mb_per_sec:.2f} MB/s")
        
        if self.error_codes:
            print("Error Codes:")
            for code, count in self.error_codes.items():
                print(f"  Code {code}: {count} times")
        
        print("=" * 50)
    
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
        
        # Initialize statistics
        put_stats = TestStats("Put Operations")
        get_stats = TestStats("Get Operations")

        # --------------------------
        # Phase 1: Data population (trigger eviction)
        # --------------------------
        put_stats.start_timer()
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            
            # Record operation start time
            op_start = time.perf_counter()
            retcode = self.store.put(key, value)
            op_end = time.perf_counter()
            latency = op_end - op_start
            
            if retcode == -200:
                # The space is not enough, continue to next operation
                put_stats.record_operation(False, latency, VALUE_SIZE, retcode)
                continue
            elif retcode == 0:
                reference[key] = value
                put_stats.record_operation(True, latency, VALUE_SIZE)
            else:
                put_stats.record_operation(False, latency, VALUE_SIZE, retcode)
                raise RuntimeError(f"Put operation failed for key {key}. Error code: {retcode}")
            
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
        
        put_stats.stop_timer()
        time.sleep(5)


        # --------------------------
        # Phase 2: Data verification
        # --------------------------
        get_stats.start_timer()
        index = 0
        count = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            
            op_start = time.perf_counter()
            retrieved = self.store.get(key)
            op_end = time.perf_counter()
            latency = op_end - op_start
            
            if len(retrieved) != 0:
                expected = reference.get(key)
                success = (retrieved == expected)
                get_stats.record_operation(success, latency, VALUE_SIZE)
                if not success:
                    print(f"Data mismatch for key: {key}")
            else:
                get_stats.record_operation(False, latency, VALUE_SIZE, -1)
            
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
        
        get_stats.stop_timer()
        
        # Print statistics
        put_stats.print_stats()
        get_stats.print_stats()
            
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
        
        # Create shared statistics objects
        put_stats = TestStats("Concurrent Put Operations")
        get_stats = TestStats("Concurrent Get Operations")
        
        index = 0
        while index < OPERATIONS_PER_THREAD:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            references[key] = value
            index = index + 1
        
        def worker(thread_id, stats):
            try:
                print(f"Thread {thread_id} started") 
                
                # Put operations
                index = 0
                while index < OPERATIONS_PER_THREAD:
                    key = "k_" + str(index)
                    test_data = references[key]
                    
                    op_start = time.perf_counter()
                    result = self.store.put(key, test_data)
                    op_end = time.perf_counter()
                    latency = op_end - op_start
                    
                    success = (result == 0)
                    stats.record_operation(success, latency, VALUE_SIZE, result if not success else None)
                    
                    index = index + 1
                    if index % 500 == 0:
                        print(f"Thread {thread_id}: put completed {index} entries")
            
                # Get operations
                index = 0
                while index < OPERATIONS_PER_THREAD:
                    key = "k_" + str(index)
                    
                    op_start = time.perf_counter()
                    retrieved_data = self.store.get(key)
                    op_end = time.perf_counter()
                    latency = op_end - op_start
                    
                    if len(retrieved_data) != 0:
                        expected_value = references[key]
                        success = (retrieved_data == expected_value)
                        stats.record_operation(success, latency, VALUE_SIZE)
                        if not success:
                            print(f"Thread {thread_id}: Data mismatch for key {key}")
                    else:
                        stats.record_operation(False, latency, VALUE_SIZE, -1)
                    
                    index = index + 1
                    if index % 500 == 0:
                        print(f"Thread {thread_id}: get completed {index} entries")

                print(f"Thread {thread_id} completed")  
                
            except Exception as e:
                thread_exceptions.append(f"Thread {thread_id} failed: {str(e)}")
                print("Exception in thread:", str(e))
        
        
        # Create and start threads
        threads = []
        thread_stats = [TestStats(f"Thread-{i}") for i in range(NUM_THREADS)]
        put_stats.start_timer()
        
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, args=(i, thread_stats[i]), name=f"Worker-{i}")
            threads.append(t)
            t.start()

        # Join all threads
        for t in threads:
            t.join()
            
        put_stats.stop_timer()
        
        # Combine thread stats
        for stats in thread_stats:
            put_stats.operation_count += stats.operation_count
            put_stats.success_count += stats.success_count
            put_stats.failure_count += stats.failure_count
            put_stats.latencies.extend(stats.latencies)
            put_stats.data_transferred += stats.data_transferred
            for code, count in stats.error_codes.items():
                put_stats.error_codes[code] += count

        # Check for any exceptions
        self.assertEqual(len(thread_exceptions), 0, "\n".join(thread_exceptions))

        # Print combined statistics
        put_stats.print_stats()

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
        
        # Initialize statistics
        put_stats = TestStats("Batch Put Operations")
        get_stats = TestStats("Batch Get Operations")

        # --------------------------
        # Phase 1: data population (trigger eviction)
        # --------------------------
        put_stats.start_timer()
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            
            op_start = time.perf_counter()
            retcode = self.store.put(key, value)
            op_end = time.perf_counter()
            latency = op_end - op_start
            
            if retcode == -200:
                # The space is not enough, continue to next operation
                put_stats.record_operation(False, latency, VALUE_SIZE, retcode)
                continue
            elif retcode == 0:
                reference[key] = value
                put_stats.record_operation(True, latency, VALUE_SIZE)
            else:
                put_stats.record_operation(False, latency, VALUE_SIZE, retcode)
                raise RuntimeError(f"Put operation failed for key {key}. Error code: {retcode}")
            
            index = index + 1
            if index % 500 == 0:
                print("completed", index, "entries")
        
        put_stats.stop_timer()
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

        get_stats.start_timer()
        index = 0
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
            op_start = time.perf_counter()
            results = self.store.batch_get_into(batch_keys, buffer_ptrs, buffer_sizes)
            op_end = time.perf_counter()
            latency = op_end - op_start
            
            # Verify results
            self.assertEqual(len(results), len(batch_keys), "Should return result for each key")
            
            batch_success = 0
            for i, result in enumerate(results):
                current_key = batch_keys[i]
                expected = reference.get(current_key)
                
                if expected is not None:
                    if result > 0:
                        # Verify data integrity
                        offset = i * buffer_spacing
                        read_data = bytes(large_buffer[offset:offset + result])
                        success = (read_data == expected)
                        get_stats.record_operation(success, latency, VALUE_SIZE)
                        if success:
                            batch_success += 1
                        else:
                            print(f"Data mismatch for key {current_key}")
                    else:
                        get_stats.record_operation(False, latency, VALUE_SIZE, result)
                else:
                    get_stats.record_operation(False, latency, VALUE_SIZE, -2)
            
            # Record batch latency
            get_stats.record_operation(batch_success == len(batch_keys), latency, 
                                      len(batch_keys) * VALUE_SIZE)
            
            if index % 500 == 0:
                print("completed", index, "entries")
        
        get_stats.stop_timer()
        
        # Print statistics
        put_stats.print_stats()
        get_stats.print_stats()
        
        # --------------------------
        # Phase 3: Cleanup (still using single remove for simplicity)
        # --------------------------
        time.sleep(DEFAULT_KV_LEASE_TTL / 1000)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration should succeed")
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            self.store.remove(key)
            index += 1
        print("Cleanup completed")
    

if __name__ == "__main__":
    unittest.main()