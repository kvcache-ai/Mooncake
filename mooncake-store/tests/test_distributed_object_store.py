import ctypes
import struct
import unittest
import os
import time
import threading
import random
from mooncake.store import MooncakeDistributedStore


# Define a test class for serialization
class TestClass:
    def __init__(self, version=1, shape=(1, 2, 3)):
        self.version = version
        self.shape = shape
    
    def serialize_into(self, buffer):
        struct.pack_into("i", buffer, 0, self.version)
        struct.pack_into("3i", buffer, 4, *self.shape)
        
    def serialize_into(self):
        version_bytes = struct.pack("i", self.version)
        shape_bytes = struct.pack("3i", *self.shape)
        return (version_bytes, shape_bytes)
        
    def deserialize_from(buffer):
        version = struct.unpack_from("i", buffer, 0)[0]
        shape = struct.unpack_from("3i", buffer, 4)
        return TestClass(version, shape)
         
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
        self.assertEqual(self.store.get_size(key), len(test_data))
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data)

        # Put again with the same key, should succeed
        self.assertEqual(self.store.put(key, test_data), 0)

        # Remove the key
        self.assertEqual(self.store.remove(key), 0)

        # Get after remove should return empty bytes
        self.assertLess(self.store.get_size(key), 0)
        empty_data = self.store.get(key)
        self.assertEqual(empty_data, b"")

        # Test is_exist functionality
        test_data_2 = b"Testing exists!"
        key_2 = "test_exist_key"
        
        # Should not exist initially
        self.assertLess(self.store.get_size(key_2), 0)
        self.assertEqual(self.store.is_exist(key_2), 0)
        
        # Should exist after put
        self.assertEqual(self.store.put(key_2, test_data_2), 0)
        self.assertEqual(self.store.is_exist(key_2), 1)
        self.assertEqual(self.store.get_size(key_2), len(test_data_2))
        
        # Should not exist after remove
        self.assertEqual(self.store.remove(key_2), 0)
        self.assertLess(self.store.get_size(key_2), 0)
        self.assertEqual(self.store.is_exist(key_2), 0)
        
    def test_large_buffer(self):
        """Test SliceBuffer with large data that might span multiple slices."""
        # Create a large data buffer (10MB)
        size = 20 * 1024 * 1024
        test_data = os.urandom(size)
        key = "test_large_slice_buffer_key"
        
        # Put data
        self.assertEqual(self.store.put(key, test_data), 0)
        
        # Get buffer
        buffer = self.store.get_buffer(key)
        self.assertIsNotNone(buffer)
        
        # Check size
        self.assertEqual(buffer.size(), size)
        
        # Get consolidated pointer
        ptr = buffer.consolidated_ptr()
        self.assertNotEqual(ptr, 0)
        
        # Create a ctypes buffer from the pointer
        c_buffer = (ctypes.c_char * buffer.size()).from_address(ptr)
        retrieved_data = bytes(c_buffer)
        
        # Verify data
        self.assertEqual(retrieved_data, test_data)
        
        # Clean up
        self.assertEqual(self.store.remove(key), 0)
    
    def test_serialization_and_deserialization(self):
        """Test serialization and deserialization of custom objects."""
        test_object = TestClass()
        key = "test_serialization_key"
        
        # Serialize the object into a buffer
        buffer = bytearray(16)  # 16 bytes for the test object
        test_object.serialize_into(buffer)
        
        # Put the serialized data
        self.assertEqual(self.store.put(key, buffer), 0)
        
        # Get the serialized data
        retrieved_buffer = self.store.get_buffer(key)
        retrieved_buffer = memoryview(retrieved_buffer)
        self.assertEqual(len(retrieved_buffer), 16)
        
        # Deserialize the object from the retrieved buffer
        deserialized_object = TestClass.deserialize_from(retrieved_buffer)
        
        # Verify deserialized object
        self.assertEqual(deserialized_object.version, 1)
        self.assertEqual(deserialized_object.shape, (1, 2, 3))
        
        # Clean up
        self.assertEqual(self.store.remove(key), 0)
        
        (version_bytes, shape_bytes) = test_object.serialize_into()
        self.assertEqual(self.store.put_parts(key, version_bytes, shape_bytes),0)
        
        retrieved_buffer = self.store.get_buffer(key)
        retrieved_buffer = memoryview(retrieved_buffer)
        self.assertEqual(len(retrieved_buffer), 16)
        deserialized_object = TestClass.deserialize_from(retrieved_buffer)
        self.assertEqual(deserialized_object.version, 1)
        self.assertEqual(deserialized_object.shape, (1, 2, 3))
        
        self.assertEqual(self.store.remove(key), 0)

    def test_concurrent_stress_with_barrier(self):
        """Test concurrent Put/Get operations with multiple threads using barrier."""
        NUM_THREADS = 8
        VALUE_SIZE = 5 * 1024 * 1024  # 1MB
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
                    if result != 0:
                        thread_exceptions.append(f"Put operation failed for key {key} with result {result}")
                
                # Wait for all threads to complete put operations
                put_barrier.wait()
                
                # Get operations
                for key in thread_keys:
                    buffer = self.store.get_buffer(key)
                    assert buffer is not None
                    assert buffer.size() == VALUE_SIZE
                    
                    retrieved_data = memoryview(buffer)
                    if len(retrieved_data) != VALUE_SIZE:
                        thread_exceptions.append(f"Retrieved data size mismatch for key {key}: expected {VALUE_SIZE}, got {len(retrieved_data)}")
                
                # Wait for all threads to complete get operations
                get_barrier.wait()
                
                # Remove all keys
                for key in thread_keys:
                    result = self.store.remove(key)
                    if result != 0:
                        thread_exceptions.append(f"Remove operation failed for key {key} with result {result}")
                
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
        
        # Check for any exceptions in the main thread
        if thread_exceptions:
            self.fail(f"Test failed with the following errors:\n" + "\n".join(thread_exceptions))
        
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

def test_put_get_tensor(self):
    """Test storing and retrieving PyTorch tensors using put_tensor/get_tensor."""
    import torch

    # Float tensor
    tensor = torch.randn(5, 7, dtype=torch.float32)
    key = "test_tensor_float"
    result = self.store.put_tensor(key, tensor)
    self.assertEqual(result, 0)
    retrieved = self.store.get_tensor(key)
    self.assertIsNotNone(retrieved)
    self.assertEqual(tuple(retrieved.shape), tuple(tensor.shape))
    self.assertEqual(retrieved.dtype, tensor.dtype)
    self.assertTrue(torch.allclose(tensor, retrieved))

    # Int tensor
    tensor_int = torch.randint(0, 100, (3, 4), dtype=torch.int32)
    key_int = "test_tensor_int"
    result = self.store.put_tensor(key_int, tensor_int)
    self.assertEqual(result, 0)
    retrieved_int = self.store.get_tensor(key_int)
    self.assertIsNotNone(retrieved_int)
    self.assertEqual(tuple(retrieved_int.shape), tuple(tensor_int.shape))
    self.assertEqual(retrieved_int.dtype, tensor_int.dtype)
    self.assertTrue(torch.equal(tensor_int, retrieved_int))

    # Bool tensor
    tensor_bool = torch.tensor([[True, False], [False, True]], dtype=torch.bool)
    key_bool = "test_tensor_bool"
    result = self.store.put_tensor(key_bool, tensor_bool)
    self.assertEqual(result, 0)
    retrieved_bool = self.store.get_tensor(key_bool)
    self.assertIsNotNone(retrieved_bool)
    self.assertEqual(tuple(retrieved_bool.shape), tuple(tensor_bool.shape))
    self.assertEqual(retrieved_bool.dtype, tensor_bool.dtype)
    self.assertTrue(torch.equal(tensor_bool, retrieved_bool))

    # Clean up
    self.store.remove(key)
    self.store.remove(key_int)
    self.store.remove(key_bool)
             
if __name__ == '__main__':
    unittest.main()