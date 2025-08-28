import unittest
import os
import time
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000 # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL))


def get_clients(stores, local_buffer_size_param=None):
    """Initialize and setup the distributed store clients."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    base_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata")
    segment_size = 1600 * 1024 * 1024  # 1600 MB per segment
    local_buffer_size = (
        local_buffer_size_param if local_buffer_size_param is not None 
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
            master_server_address
        )
        
        if retcode:
            raise RuntimeError(f"Failed to setup segment. Return code: {retcode}")

def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    return get_clients([store], local_buffer_size_param)

class TestDistributedObjectStoreReplication(unittest.TestCase):
    """Test class for replication operations (multiple stores)."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize the main store and additional stores for replication."""
        cls.store = MooncakeDistributedStore()
        
        # Additional stores for replication testing
        cls.additional_stores = []
        cls.max_replicate_num = 2
        for _ in range(cls.max_replicate_num - 1):  # -1 because main store is already created
            cls.additional_stores.append(MooncakeDistributedStore())
        
        get_clients([cls.store] + cls.additional_stores)

    def test_put_with_config_parameter(self):
        """Test put method with config parameter."""
        
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
        config.replica_num = self.max_replicate_num
        
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
        config.replica_num = self.max_replicate_num
        
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
        config.replica_num = self.max_replicate_num
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
        config.replica_num = self.max_replicate_num
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

    def test_replication_failure_tolerance(self):
        """Test that replicated data remains accessible after main store failure and reinit."""
        
        test_data = b"Replicated failure tolerance test data!"
        key = "test_replication_failure_key"
        
        # Create config with replication
        config = ReplicateConfig()
        config.replica_num = self.max_replicate_num
        
        # Put data with replication
        result = self.store.put(key=key, value=test_data, config=config)
        self.assertEqual(result, 0, "Put with replication should succeed")
        
        # Verify data is initially accessible
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data, "Data should be accessible after put")
        
        # Teardown the main store (simulate failure)
        result = self.store.close()
        self.assertEqual(result, 0, "Store teardown should succeed")
        time.sleep(1)  # Allow time for teardown to complete
        
        # Verify data is still accessible from replica stores
        # Since main store is down, read from one of the additional stores
        replica_store = self.additional_stores[0]  # Use first replica
        retrieved_data = replica_store.get(key)
        self.assertEqual(retrieved_data, test_data, "Data should remain accessible from replica after main store teardown")
        
        # Reinitialize the main store
        get_client(self.store)
        
        # Verify data is still accessible after main store reinit
        retrieved_data = self.store.get(key)
        self.assertEqual(retrieved_data, test_data, "Data should remain accessible after main store reinit")

        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
        self.assertEqual(self.store.remove(key), 0)

if __name__ == '__main__':
    unittest.main()
