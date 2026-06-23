import ctypes
import struct
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
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved)
        # self.assertEqual(tuple(retrieved.shape), tuple(tensor.shape))
        self.assertEqual(retrieved.dtype, tensor.dtype)
        self.assertTrue(torch.allclose(tensor, retrieved))

        # Int tensor
        tensor_int = torch.tensor([1,2,3,4], dtype=torch.int32)
        key_int = "test_tensor_int"
        result = self.store.put_tensor(key_int, tensor_int)
        self.assertEqual(result, 0)
        retrieved_int = self.store.get_tensor(key_int)
        self.assertIsNotNone(retrieved_int)
        # self.assertEqual(tuple(retrieved_int.shape), tuple(tensor_int.shape))
        self.assertEqual(retrieved_int.dtype, tensor_int.dtype)
        self.assertTrue(torch.equal(tensor_int, retrieved_int))

        # Bool tensor
        tensor_bool = torch.tensor([True, False, False, True], dtype=torch.bool)
        key_bool = "test_tensor_bool"
        result = self.store.put_tensor(key_bool, tensor_bool)
        self.assertEqual(result, 0)
        retrieved_bool = self.store.get_tensor(key_bool)
        self.assertIsNotNone(retrieved_bool)
        # self.assertEqual(tuple(retrieved_bool.shape), tuple(tensor_bool.shape))
        self.assertEqual(retrieved_bool.dtype, tensor_bool.dtype)
        self.assertTrue(torch.equal(tensor_bool, retrieved_bool))

        # Bool tensor
        tensor_rand = torch.rand(1000, dtype=torch.float32)
        key_rand = "test_tensor_rand"
        result = self.store.put_tensor(key_rand, tensor_rand)
        self.assertEqual(result, 0)
        retrieved_rand = self.store.get_tensor(key_rand)
        self.assertIsNotNone(retrieved_rand)
        # self.assertEqual(tuple(retrieved_bool.shape), tuple(tensor_bool.shape))
        self.assertEqual(retrieved_rand.dtype, tensor_rand.dtype)
        self.assertTrue(torch.equal(tensor_rand, retrieved_rand))

        # Clean up
        time.sleep(default_kv_lease_ttl / 1000)
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
        result = self.store.put_tensor(key_2d, tensor_2d)
        self.assertEqual(result, 0)

        retrieved_tensor = self.store.get_tensor(key_2d)
        self.assertIsNotNone(retrieved_tensor)
        self.assertEqual(retrieved_tensor.shape, tensor_2d.shape)
        self.assertEqual(retrieved_tensor.dtype, tensor_2d.dtype)
        self.assertTrue(torch.allclose(tensor_2d, retrieved_tensor))

        # Test with 3D int tensor
        tensor_3d = torch.tensor([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=torch.int64)
        key_3d = "test_tensor_with_metadata_3d"
        
        result = self.store.put_tensor(key_3d, tensor_3d)
        self.assertEqual(result, 0)

        retrieved_tensor_3d = self.store.get_tensor(key_3d)
        self.assertIsNotNone(retrieved_tensor_3d)
        self.assertEqual(retrieved_tensor_3d.shape, tensor_3d.shape)
        self.assertEqual(retrieved_tensor_3d.dtype, tensor_3d.dtype)
        self.assertTrue(torch.equal(tensor_3d, retrieved_tensor_3d))

        # Clean up
        self.store.remove(key_2d)
        self.store.remove(key_3d)

             
    def _assert_empty_tensor_equal(self, expected, actual):
        import torch

        self.assertIsNotNone(actual)
        self.assertEqual(tuple(actual.shape), tuple(expected.shape))
        self.assertEqual(actual.dtype, expected.dtype)
        self.assertEqual(actual.numel(), 0)
        self.assertTrue(torch.equal(actual, expected))

    def test_put_get_zero_tensor(self):
        """Test storing and retrieving zero-sized PyTorch tensors."""
        import torch

        tensors = [
            torch.empty((0,), dtype=torch.float32),
            torch.empty((0, 4096), dtype=torch.float16),
            torch.empty((2, 0, 3), dtype=torch.bfloat16),
            torch.empty((1, 0), dtype=torch.int64),
        ]
        keys = [f"test_zero_tensor_{os.getpid()}_{i}" for i in range(len(tensors))]

        for key, tensor in zip(keys, tensors):
            self.assertEqual(self.store.put_tensor(key, tensor), 0)
            self._assert_empty_tensor_equal(tensor, self.store.get_tensor(key))

        batch_results = self.store.batch_get_tensor(keys)
        self.assertEqual(len(batch_results), len(tensors))
        for tensor, retrieved in zip(tensors, batch_results):
            self._assert_empty_tensor_equal(tensor, retrieved)

        for key in keys:
            self.store.remove(key)

    def test_zero_tensor_into_and_from(self):
        """Test metadata-only zero tensor get_into and put_tensor_from."""
        import torch

        tensor = torch.empty((2, 0, 3), dtype=torch.float32)
        key = f"test_zero_tensor_into_{os.getpid()}"
        key_from = f"test_zero_tensor_from_{os.getpid()}"
        self.assertEqual(self.store.put_tensor(key, tensor), 0)

        buffer_size = 4096
        buffer = (ctypes.c_ubyte * buffer_size)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_size), 0)
        try:
            total_length = self.store.get_size(key)
            self.assertGreater(total_length, 0)
            self.assertLess(total_length, buffer_size)
            retrieved = self.store.get_tensor_into(key, buffer_ptr, buffer_size)
            self._assert_empty_tensor_equal(tensor, retrieved)

            self.assertEqual(self.store.put_tensor_from(key_from, buffer_ptr, total_length), 0)
            self._assert_empty_tensor_equal(tensor, self.store.get_tensor(key_from))
        finally:
            self.store.unregister_buffer(buffer_ptr)
            self.store.remove(key)
            self.store.remove(key_from)

    def test_zero_tensor_with_tp(self):
        """Test zero-sized tensors through legacy tensor-parallel APIs."""
        import torch

        tensor = torch.empty((2, 0, 3), dtype=torch.float32)
        key = f"test_zero_tensor_tp_{os.getpid()}"
        tp_size = 2
        split_dim = 0

        self.assertEqual(
            self.store.put_tensor_with_tp(key, tensor, tp_size=tp_size, split_dim=split_dim),
            0,
        )
        for rank in range(tp_size):
            shard = self.store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size)
            expected = tensor.narrow(split_dim, rank, 1).contiguous()
            self._assert_empty_tensor_equal(expected, shard)

        for rank in range(tp_size):
            self.store.remove(f"{key}_tp_{rank}")


from mooncake.structured_object_store import (
    MISSING,
    _choose_leaf_codec,
    _escape_key,
    infer_structure,
)


class TestCodecInference(unittest.TestCase):

    def test_tensor(self):
        import torch
        d = _choose_leaf_codec([torch.tensor([1, 2]), torch.tensor([3])])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "ragged_tensor")

    def test_tensor_mixed_dtype_rejected(self):
        import torch
        d = _choose_leaf_codec([torch.tensor([1], dtype=torch.float32), torch.tensor([1], dtype=torch.int64)])
        self.assertFalse(d.accepted)

    def test_tensor_mixed_ndim_rejected(self):
        import torch
        d = _choose_leaf_codec([torch.tensor([1]), torch.tensor([[1, 2]])])
        self.assertFalse(d.accepted)

    def test_numeric_sequence(self):
        d = _choose_leaf_codec([[1, 2, 3], [4, 5]])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "typed_ragged")

    def test_bytes(self):
        d = _choose_leaf_codec([b"hello", b"world"])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "bytes_ragged")

    def test_text(self):
        d = _choose_leaf_codec(["hello", "world"])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "utf8_ragged")

    def test_json(self):
        d = _choose_leaf_codec([{"a": 1}, {"b": 2}])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "json_ragged")

    def test_json_rejects_late_non_serializable(self):
        values = [{"ok": i} for i in range(200)] + [object()]
        d = _choose_leaf_codec(values)
        self.assertFalse(d.accepted)

    def test_scalar(self):
        d = _choose_leaf_codec([1, 2.0, 3])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "ndarray")

    def test_fallback(self):
        d = _choose_leaf_codec([object(), object()])
        self.assertFalse(d.accepted)
        self.assertEqual(d.codec, "pickle_ragged_fallback")

    def test_with_nulls(self):
        d = _choose_leaf_codec(["hello", None, "world"])
        self.assertTrue(d.accepted)
        self.assertEqual(d.codec, "utf8_ragged")

    def test_empty_values(self):
        d = _choose_leaf_codec([])
        self.assertFalse(d.accepted)

    def test_all_none(self):
        d = _choose_leaf_codec([None, None, None])
        self.assertFalse(d.accepted)

    def test_infer_flat(self):
        leaves, nodes = [], []
        infer_structure("root", ["a", "b", "c"], leaves, nodes)
        self.assertEqual(len(leaves), 1)
        self.assertEqual(len(nodes), 0)
        self.assertEqual(leaves[0].decision.codec, "utf8_ragged")

    def test_infer_dict(self):
        leaves, nodes = [], []
        infer_structure("root", [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], leaves, nodes)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].node_type, "dict")
        self.assertEqual(sorted(nodes[0].children), ["x", "y"])
        self.assertEqual(sorted(leaf.path for leaf in leaves), ["root.x", "root.y"])

    def test_infer_dict_with_none_rows(self):
        leaves, nodes = [], []
        infer_structure("r", [{"x": 1}, None, {"x": 3}], leaves, nodes)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(len(leaves), 1)
        self.assertEqual(leaves[0].path, "r.x")

    def test_infer_nested(self):
        leaves, nodes = [], []
        infer_structure("r", [{"a": {"b": 1}}, {"a": {"b": 2}}], leaves, nodes)
        self.assertEqual(len(nodes), 2)
        self.assertEqual(leaves[0].path, "r.a.b")
        self.assertEqual(leaves[0].decision.codec, "ndarray")

    def test_infer_list(self):
        leaves, nodes = [], []
        infer_structure("r", [[{"k": 1}], [{"k": 2}]], leaves, nodes)
        list_nodes = [n for n in nodes if n.node_type == "list"]
        self.assertEqual(len(list_nodes), 1)
        self.assertEqual(list_nodes[0].lengths, [1, 1])
        dict_nodes = [n for n in nodes if n.node_type == "dict"]
        self.assertEqual(len(dict_nodes), 1)
        self.assertEqual(len(leaves), 1)
        self.assertEqual(leaves[0].path, "r[0].k")

    def test_infer_list_with_none_items(self):
        leaves, nodes = [], []
        infer_structure("r", [[{"k": 1}, None], [{"k": 2}, None]], leaves, nodes)
        list_nodes = [n for n in nodes if n.node_type == "list"]
        self.assertEqual(len(list_nodes), 1)

    def test_dict_missing_key_vs_none_value(self):
        leaves, nodes = [], []
        infer_structure("r", [{"x": None}, {"y": 2}, None], leaves, nodes)
        self.assertIsNotNone(nodes[0].row_mask)
        self.assertEqual(nodes[0].row_mask, [True, True, False])
        x_leaf = next(leaf for leaf in leaves if leaf.path == "r.x")
        self.assertIsNone(x_leaf.values[0])
        self.assertIsInstance(x_leaf.values[1], type(MISSING))
        self.assertIsNone(x_leaf.values[2])

    def test_escape_key_in_path(self):
        self.assertEqual(_escape_key("simple"), "simple")
        self.assertEqual(_escape_key("a.b"), "a\\.b")
        self.assertEqual(_escape_key("a[0]"), "a\\[0]")
        self.assertEqual(_escape_key("a\\b"), "a\\\\b")

    def test_dict_with_dot_key(self):
        leaves, nodes = [], []
        infer_structure("r", [{"a.b": 1}, {"a.b": 2}], leaves, nodes)
        self.assertEqual(leaves[0].path, "r.a\\.b")

    def test_depth_limit(self):
        deep = 1
        for _ in range(40):
            deep = {"a": deep}
        with self.assertRaises(ValueError):
            infer_structure("r", [deep, deep], [], [])


if __name__ == '__main__':
    unittest.main()
