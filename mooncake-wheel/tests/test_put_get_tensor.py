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

    def test_put_get_tensor(self):
        """Test storing and retrieving PyTorch tensors using put_tensor/get_tensor."""
        import torch

        # Float tensor
        tensor = torch.tensor([1.0, 2.0, 3.0, 4.0], dtype=torch.float32)
        key = "test_tensor_float"
        result = self.store.put_tensor(key, tensor)
        self.assertEqual(result, 0)
        retrieved = self.store.get_tensor(key, "float32")
        self.assertIsNotNone(retrieved)
        # self.assertEqual(tuple(retrieved.shape), tuple(tensor.shape))
        self.assertEqual(retrieved.dtype, tensor.dtype)
        self.assertTrue(torch.allclose(tensor, retrieved))

        # Int tensor
        tensor_int = torch.tensor([1,2,3,4], dtype=torch.int32)
        key_int = "test_tensor_int"
        result = self.store.put_tensor(key_int, tensor_int)
        self.assertEqual(result, 0)
        retrieved_int = self.store.get_tensor(key_int, "int32")
        self.assertIsNotNone(retrieved_int)
        # self.assertEqual(tuple(retrieved_int.shape), tuple(tensor_int.shape))
        self.assertEqual(retrieved_int.dtype, tensor_int.dtype)
        self.assertTrue(torch.equal(tensor_int, retrieved_int))

        # Bool tensor
        tensor_bool = torch.tensor([True, False, False, True], dtype=torch.bool)
        key_bool = "test_tensor_bool"
        result = self.store.put_tensor(key_bool, tensor_bool)
        self.assertEqual(result, 0)
        retrieved_bool = self.store.get_tensor(key_bool, "bool")
        self.assertIsNotNone(retrieved_bool)
        # self.assertEqual(tuple(retrieved_bool.shape), tuple(tensor_bool.shape))
        self.assertEqual(retrieved_bool.dtype, tensor_bool.dtype)
        self.assertTrue(torch.equal(tensor_bool, retrieved_bool))

        # Bool tensor
        tensor_rand = torch.rand(1000, dtype=torch.float32)
        key_rand = "test_tensor_rand"
        result = self.store.put_tensor(key_rand, tensor_rand)
        self.assertEqual(result, 0)
        retrieved_rand = self.store.get_tensor(key_rand, "float32")
        self.assertIsNotNone(retrieved_rand)
        # self.assertEqual(tuple(retrieved_bool.shape), tuple(tensor_bool.shape))
        self.assertEqual(retrieved_rand.dtype, tensor_rand.dtype)
        self.assertTrue(torch.equal(tensor_rand, retrieved_rand))

        # Clean up
        self.store.remove(key)
        self.store.remove(key_int)
        self.store.remove(key_bool)
        self.store.remove(key_rand)

    def test_put_get_tensor_with_metadata(self):
        """Test storing and retrieving PyTorch tensors with metadata using put_tensor_with_metadata/get_tensor_with_metadata."""
        import torch

        # Test with 2D float tensor
        tensor_2d = torch.tensor([[1.0, 2.0], [3.0, 4.0]], dtype=torch.float32)
        key_2d = "test_tensor_with_metadata_2d"
        shape_str = str(list(tensor_2d.shape))
        dtype_str = str(tensor_2d.dtype)
        
        result = self.store.put_tensor_with_metadata(key_2d, tensor_2d)
        self.assertEqual(result, 0)
        
        retrieved_tensor, retrieved_shape, retrieved_dtype = self.store.get_tensor_with_metadata(key_2d)
        self.assertIsNotNone(retrieved_tensor)
        self.assertEqual(retrieved_shape, shape_str)
        self.assertEqual(retrieved_dtype, dtype_str)
        self.assertEqual(retrieved_tensor.dtype, tensor_2d.dtype)
        self.assertTrue(torch.allclose(tensor_2d, retrieved_tensor))

        # Test with 3D int tensor
        tensor_3d = torch.tensor([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=torch.int64)
        key_3d = "test_tensor_with_metadata_3d"
        shape_str_3d = str(list(tensor_3d.shape))
        dtype_str_3d = str(tensor_3d.dtype)
        
        result = self.store.put_tensor_with_metadata(key_3d, tensor_3d)
        self.assertEqual(result, 0)
        
        retrieved_tensor_3d, retrieved_shape_3d, retrieved_dtype_3d = self.store.get_tensor_with_metadata(key_3d)
        self.assertIsNotNone(retrieved_tensor_3d)
        self.assertEqual(retrieved_shape_3d, shape_str_3d)
        self.assertEqual(retrieved_dtype_3d, dtype_str_3d)
        self.assertEqual(retrieved_tensor_3d.dtype, tensor_3d.dtype)
        self.assertTrue(torch.equal(tensor_3d, retrieved_tensor_3d))

        # Clean up
        self.store.remove(key_2d)
        self.store.remove(key_3d)

    def test_batch_put_get_tensors(self):
        """Test batch storing and retrieving PyTorch tensors using batch_put_tensors/batch_get_tensors."""
        import torch

        # Prepare multiple tensors
        tensor1 = torch.tensor([1.0, 2.0, 3.0], dtype=torch.float32)
        tensor2 = torch.tensor([[1, 2], [3, 4]], dtype=torch.int32)
        tensor3 = torch.tensor([True, False, True], dtype=torch.bool)
        tensor4 = torch.randn(5, 10, dtype=torch.float64)
        tensor5 = torch.tensor([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]], dtype=torch.int64)
        
        tensors = [tensor1, tensor2, tensor3, tensor4, tensor5]
        keys = ["batch_tensor_1", "batch_tensor_2", "batch_tensor_3", "batch_tensor_4", "batch_tensor_5"]
        dtypes = ["float32", "int32", "bool", "float64", "int64"]

        # Test batch put
        results = self.store.batch_put_tensors(keys, tensors)
        self.assertEqual(len(results), len(keys))
        for result in results:
            self.assertEqual(result, 0)  # All operations should succeed

        # Test batch get
        retrieved_tensors = self.store.batch_get_tensors(keys, dtypes)
        self.assertEqual(len(retrieved_tensors), len(keys))
        
        for i, (original, retrieved) in enumerate(zip(tensors, retrieved_tensors)):
            self.assertIsNotNone(retrieved)
            self.assertEqual(original.dtype, retrieved.dtype)
            if original.dtype == torch.float32 or original.dtype == torch.float64:
                self.assertTrue(torch.allclose(original, retrieved))
            else:
                self.assertTrue(torch.equal(original, retrieved))

        # Test with partial failures (non-existent keys)
        mixed_keys = keys + ["non_existent_key"]
        mixed_dtypes = dtypes + ["float32"]
        retrieved_mixed = self.store.batch_get_tensors(mixed_keys, mixed_dtypes)
        self.assertEqual(len(retrieved_mixed), len(mixed_keys))
        
        # First 5 should be valid tensors, last one should be None
        for i in range(5):
            self.assertIsNotNone(retrieved_mixed[i])
        # The non-existent key should return None
        self.assertIsNone(retrieved_mixed[5])

        # Clean up
        for key in keys:
            self.store.remove(key)
             
if __name__ == '__main__':
    unittest.main()