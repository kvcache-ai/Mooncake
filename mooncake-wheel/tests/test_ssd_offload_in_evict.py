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
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000 # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL))
ADD_OPERATION_COUNT_ONE = 1 
ADD_OPERATION_COUNT_ZERO = 0   
NO_ADD_LATENCY = 0              
NO_ADD_DATA_SIZE = 0            

# This test need to set the environment valid client storage root path. 
# Put data is more than segment size,so some data will be eviced ,and get from local file later
def get_client(store):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "eth0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
    global_segment_size = 512 * 1024 * 1024   # 512 MB
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

    def combine(self, other):
        """Merge data from another TestStats object into the current one"""
        # Merge operation counts
        self.operation_count += other.operation_count
        self.success_count += other.success_count
        self.failure_count += other.failure_count
        
        # Merge latency data
        self.latencies.extend(other.latencies)
        
        # Merge error codes
        for code, count in other.error_codes.items():
            self.error_codes[code] += count
        
        # Merge data volume
        self.data_transferred += other.data_transferred
        
        # Merge thread duration (used for computing concurrency efficiency)
        if hasattr(other, 'thread_duration'):
            if not hasattr(self, 'thread_duration'):
                self.thread_duration = 0
            self.thread_duration = max(self.thread_duration, other.thread_duration)

        
    def record_operation(self, success, num, latency, data_size=0, error_code=None):
        self.operation_count += num
        if success:
            self.success_count += num
        else:
            self.failure_count += num
        if error_code is not None:
            self.error_codes[error_code] += num
        if latency > 0:
            self.latencies.append(latency)
        if data_size > 0:
            self.data_transferred += data_size

    def record_throughput(self, total_ops, total_data, duration):
        """Record overall throughput metrics"""
        self.total_ops = total_ops
        self.total_data = total_data
        self.duration = duration
        self.ops_per_sec = total_ops / duration if duration > 0 else 0
        self.mb_per_sec = total_data / duration / (1024 * 1024) if duration > 0 else 0
        
    def print_throughput(self, thread_put_stats, thread_get_stats):
        """Print throughput statistics"""
        # Calculate PUT concurrency efficiency
        if thread_put_stats:
            # Calculate average thread time
            total_put_time = sum(s.thread_duration for s in thread_put_stats)
            avg_put_time = total_put_time / len(thread_put_stats)
            
            # Calculate slowest thread time
            max_put_time = max(s.thread_duration for s in thread_put_stats)
            
            # Calculate concurrency efficiency (using average time)
            put_efficiency = total_put_time / avg_put_time if avg_put_time > 0 else 0
        else:
            avg_put_time = 0
            max_put_time = 0
            put_efficiency = 0

        # Calculate GET concurrency efficiency
        if thread_get_stats:
            # Calculate average thread time
            total_get_time = sum(s.thread_duration for s in thread_get_stats)
            avg_get_time = total_get_time / len(thread_get_stats)
            
            # Calculate slowest thread time
            max_get_time = max(s.thread_duration for s in thread_get_stats)
            
            # Calculate concurrency efficiency (using average time)
            get_efficiency = total_get_time / avg_get_time if avg_get_time > 0 else 0
        else:
            avg_get_time = 0
            max_get_time = 0
            get_efficiency = 0
            
        print(f"\n=== {self.test_name} STATISTICS ===")
        print(f"Concurrent Throughput: {self.ops_per_sec:.2f} ops/sec, {self.mb_per_sec:.2f} MB/s")
        print(f"Total Operations: {self.total_ops}, Duration: {self.duration:.2f} seconds")

        print(f"Put Concurrency Efficiency: {put_efficiency*100:.2f}%")
        print(f"  • Average Thread Time: {avg_put_time:.4f}s")
        print(f"  • Slowest Thread Time: {max_put_time:.4f}s")
        print(f"  • Efficiency Ratio: {avg_put_time/max_put_time:.2f}")

        print(f"Get Concurrency Efficiency: {get_efficiency*100:.2f}%")
        print(f"  • Average Thread Time: {avg_get_time:.4f}s")
        print(f"  • Slowest Thread Time: {max_get_time:.4f}s")
        print(f"  • Efficiency Ratio: {avg_get_time/max_get_time:.2f}")
            
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
        
        print(f"\n=== {self.test_name} STATISTICS ===")
        print(f"Operations: {self.operation_count} (Success: {self.success_count}, Failure: {self.failure_count})")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Data Transferred: {self.data_transferred / (1024 * 1024):.2f} MB")
        print(f"Latency (ms): Min={min_latency:.2f}, Max={max_latency:.2f}, Avg={avg_latency:.2f}")
        print(f"Percentiles (ms): P90={p90:.2f}, P99={p99:.2f}, P999={p999:.2f}")
        
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

    def test_put_get_in_evict_operations(self):
        """Test basic Put/Get operations with eviction scenario
        
        Verifies:
        1. Data eviction to file when exceeding memory capacity
        2. Correct retrieval of evicted data
        """ 
        VALUE_SIZE = 1024 * 1024 
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
                put_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, retcode)
                continue
            elif retcode == 0:
                reference[key] = value
                put_stats.record_operation(True, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE)
            else:
                put_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, retcode)
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
                get_stats.record_operation(success, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE)
                if not success:
                    print(f"Data mismatch for key: {key}")
            else:
                get_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, -1)
            
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
        time.sleep(default_kv_lease_ttl / 1000)
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            self.store.remove(key)
            index = index + 1
        print("Cleanup completed")  

    def test_concurrent_stress(self):
        """Multi-threaded stress test for Put/Get operations
        
        Verifies:
        1. Correctness under concurrent access
        2. Thread safety
        """
        NUM_THREADS = 4
        VALUE_SIZE = 1024 * 1024
        OPERATIONS_PER_THREAD = 1000

        thread_exceptions = []
        references = {}
        
        # Create independent PUT and GET statistics objects
        put_stats = TestStats("Concurrent Put Operations")
        get_stats = TestStats("Concurrent Get Operations")
        
        # Create thread-level statistics objects
        thread_put_stats = [TestStats(f"Thread-{i}-Put") for i in range(NUM_THREADS)]
        thread_get_stats = [TestStats(f"Thread-{i}-Get") for i in range(NUM_THREADS)]
        
        index = 0
        while index < OPERATIONS_PER_THREAD:
            key = "k_" + str(index)
            value = os.urandom(VALUE_SIZE)
            references[key] = value
            index = index + 1
        
        def worker(thread_id, put_stats, get_stats):
            try:
                print(f"Thread {thread_id} started") 
                
                # PUT operation phase
                put_start = time.perf_counter()
                index = 0
                while index < OPERATIONS_PER_THREAD:
                    key = "k_" + str(index)
                    test_data = references[key]
                    
                    op_start = time.perf_counter()
                    result = self.store.put(key, test_data)
                    op_end = time.perf_counter()
                    latency = op_end - op_start
                    success = (result == 0)
                    put_stats.record_operation(success, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, result if not success else None)
                    index = index + 1
                put_end = time.perf_counter()
                put_stats.thread_duration = put_end - put_start  # Record thread PUT duration
                
                # GET operation phase
                get_start = time.perf_counter()
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
                        get_stats.record_operation(success, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE)
                        if not success:
                            print(f"Thread {thread_id}: Data mismatch for key {key}")
                    else:
                        get_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, -1)
                    
                    index = index + 1
                get_end = time.perf_counter()
                get_stats.thread_duration = get_end - get_start  # Record thread GET duration
                
                print(f"Thread {thread_id} completed")  
            except Exception as e:
                thread_exceptions.append(f"Thread {thread_id} failed: {str(e)}")
                print("Exception in thread:", str(e))
        
        # Record overall start time
        overall_start = time.perf_counter()
        
        # Create and start threads
        threads = []
        for i in range(NUM_THREADS):
            t = threading.Thread(target=worker, 
                                args=(i, thread_put_stats[i], thread_get_stats[i]))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()
        
        # Record overall end time
        overall_end = time.perf_counter()
        
        # Check for exceptions
        self.assertEqual(len(thread_exceptions), 0, "\n".join(thread_exceptions))

        # Merge thread statistics into main statistics objects
        for i in range(NUM_THREADS):
            put_stats.combine(thread_put_stats[i])
            get_stats.combine(thread_get_stats[i])
        
        # Calculate overall throughput statistics
        total_duration = overall_end - overall_start
        total_put_ops = OPERATIONS_PER_THREAD * NUM_THREADS
        total_get_ops = OPERATIONS_PER_THREAD * NUM_THREADS
        total_ops = total_put_ops + total_get_ops
        total_data = total_ops * VALUE_SIZE
        
        # Create throughput statistics object
        throughput_stats = TestStats("Concurrent overall")
        throughput_stats.record_throughput(total_ops, total_data, total_duration)
        
        # Print detailed statistics
        put_stats.print_stats()
        get_stats.print_stats()
        throughput_stats.print_throughput(thread_put_stats, thread_get_stats)

        # Cleanup
        time.sleep(default_kv_lease_ttl / 1000)
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
        put_stats = TestStats("Put Operations")
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
                put_stats.record_operation(False, ADD_OPERATION_COUNT_ONE ,latency, VALUE_SIZE, retcode)
                continue
            elif retcode == 0:
                reference[key] = value
                put_stats.record_operation(True, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE)
            else:
                put_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, latency, VALUE_SIZE, retcode)
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
            
            success_counter = 0
            failed_counter = 0
            count=0
            for i, result in enumerate(results):
                current_key = batch_keys[i]
                expected = reference.get(current_key)
                
                if expected is not None:
                    if result > 0:
                        # Verify data integrity
                        offset = i * buffer_spacing
                        read_data = bytes(large_buffer[offset:offset + result])
                        success = (read_data == expected)
                        get_stats.record_operation(success, ADD_OPERATION_COUNT_ONE, NO_ADD_LATENCY, VALUE_SIZE)
                        if not success:
                            print(f"Data mismatch for key {current_key}")
                    else:
                        get_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, NO_ADD_LATENCY, VALUE_SIZE, result)
                else:
                    get_stats.record_operation(False, ADD_OPERATION_COUNT_ONE, NO_ADD_LATENCY, VALUE_SIZE, -2)

            if success_counter == BATCH_SIZE:
                get_stats.record_operation(True, ADD_OPERATION_COUNT_ZERO, latency, NO_ADD_DATA_SIZE)
            else:
                get_stats.record_operation(False, ADD_OPERATION_COUNT_ZERO, latency, NO_ADD_DATA_SIZE)
            
            if index % 500 == 0:
                print("completed", index, "entries")
        
        get_stats.stop_timer()
        
        # Print statistics
        put_stats.print_stats()
        get_stats.print_stats()
        
        # --------------------------
        # Phase 3: Cleanup (still using single remove for simplicity)
        # --------------------------
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration should succeed")
        index = 0
        while index < MAX_REQUESTS:
            key = "k_" + str(index)
            self.store.remove(key)
            index += 1
        print("Cleanup completed")
    

if __name__ == "__main__":
    unittest.main()