import ctypes
import struct
import unittest
import os
import time

import numpy as np

from mooncake.remote_tensor_batch import (
    BytearrayBufferAllocator,
    RemoteTensorBatch,
    RemoteTensorBatchMaterializer,
    TensorDimSelection,
    TensorFieldRef,
    TensorReadRequest,
    dtype_byte_size,
    selection_output_shape,
    tensor_nbytes,
    writable_buffer_region,
)
from mooncake.store import MooncakeDistributedStore

# The lease time of the kv object, should be set equal to
# the master's value.
DEFAULT_DEFAULT_KV_LEASE_TTL = 5000  # 5000 milliseconds
# Use environment variable if set, otherwise use default
default_kv_lease_ttl = int(
    os.getenv("DEFAULT_KV_LEASE_TTL", DEFAULT_DEFAULT_KV_LEASE_TTL)
)


# Define a test class for serialization
class TestClass:
    def __init__(self, version=1, shape=(1, 2, 3)):
        self.version = version
        self.shape = shape

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
    local_buffer_size = 512 * 1024 * 1024  # 512 MB
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")

    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server_address,
    )

    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


class FakeStore:
    def __init__(self):
        self.calls = []

    def register_buffer(self, buffer_ptr, size):
        self.calls.append(("register_buffer", buffer_ptr, size))
        return 0

    def unregister_buffer(self, buffer_ptr):
        self.calls.append(("unregister_buffer", buffer_ptr))
        return 0

    def get_tensor_dim_selection_into(
        self, key, buffer_ptr, size, shape, dtype, dim, selections, data_offset
    ):
        self.calls.append(
            (
                "get_tensor_dim_selection_into",
                key,
                buffer_ptr,
                size,
                shape,
                dtype,
                dim,
                selections,
                data_offset,
            )
        )
        return size


class TestRemoteTensorBatchHelpers(unittest.TestCase):
    def test_field_ref_and_size_helpers(self):
        ref = TensorFieldRef("batch/input_ids", (4, 2), "torch.int64", 128)

        self.assertEqual(dtype_byte_size("torch.int64"), 8)
        self.assertEqual(tensor_nbytes(ref.shape, ref.dtype), 64)
        self.assertEqual(ref.nbytes(), 64)

    def test_selection_shape_helpers(self):
        selections = (
            TensorDimSelection.select_idxs([3, 1]),
            TensorDimSelection.repeat(2, True),
        )

        self.assertEqual(selection_output_shape((4, 2), selections), (4, 2))
        request = TensorReadRequest(
            "input_ids",
            TensorFieldRef("batch/input_ids", (4, 2), "int64"),
            selections=selections,
        )
        self.assertEqual(request.output_shape(), (4, 2))
        self.assertEqual(request.output_nbytes(), 64)
        self.assertEqual(
            request.store_selections(), [("select_idxs", [3, 1]), ("repeat", (2, True))]
        )

    def test_remote_batch_records_lazy_operations(self):
        remote = RemoteTensorBatch(
            fields={
                "input_ids": TensorFieldRef("batch/input_ids", (4, 2), "int64"),
                "attention_mask": TensorFieldRef(
                    "batch/attention_mask", (4, 2), "int64"
                ),
            },
            batch_size=4,
        )

        selected = (
            remote.select_fields(["input_ids"])
            .select_indices([True, False, True, False])
            .slice(0, 1, None)
        )
        repeated = selected.repeat(3, False)

        self.assertEqual(selected.keys(), ["input_ids"])
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(repeated), 3)
        self.assertEqual(
            [selection.as_tuple() for selection in repeated.selections],
            [
                ("select_idxs", [True, False, True, False]),
                ("slice", (0, 1, None)),
                ("repeat", (3, False)),
            ],
        )

    def test_union_rejects_duplicate_fields(self):
        lhs = RemoteTensorBatch(
            {"input_ids": TensorFieldRef("batch/input_ids", (2,), "int64")}, 2
        )
        rhs = RemoteTensorBatch(
            {"input_ids": TensorFieldRef("other/input_ids", (2,), "int64")}, 2
        )

        with self.assertRaises(ValueError):
            lhs.union(rhs)

    def test_cat_records_selection_groups(self):
        remote = RemoteTensorBatch(
            {"input_ids": TensorFieldRef("batch/input_ids", (4,), "int64")}, 4
        )
        lhs = remote.select_indices([3, 1])
        rhs = remote.slice(0, 2, None)

        concatenated = RemoteTensorBatch.cat([lhs, rhs])

        self.assertEqual(len(concatenated), 4)
        self.assertEqual(
            [selection.op for selection in concatenated.selections], ["cat"]
        )
        self.assertEqual(selection_output_shape((4,), concatenated.selections), (4,))
        self.assertEqual(
            [
                [selection.op for selection in group]
                for group in concatenated.selections[0].value
            ],
            [["select_idxs"], ["slice"]],
        )
        self.assertEqual(
            concatenated.read_requests(["input_ids"])[0].store_selections(),
            [("cat", ((("select_idxs", [3, 1]),), (("slice", (0, 2, None)),)))],
        )

    def test_materializer_calls_store_api(self):
        store = FakeStore()
        request = TensorReadRequest(
            name="input_ids",
            ref=TensorFieldRef("batch/input_ids", (4, 2), "int64", 16),
            selections=(TensorDimSelection.select_idxs([3, 1]),),
        )
        buffer = bytearray(request.output_nbytes())

        ret = RemoteTensorBatchMaterializer(store).materialize_request_into(
            request, buffer
        )

        self.assertEqual(ret, len(buffer))
        self.assertEqual(store.calls[0][0], "register_buffer")
        self.assertEqual(store.calls[1][0], "get_tensor_dim_selection_into")
        self.assertEqual(store.calls[1][2], store.calls[0][1])
        self.assertEqual(store.calls[1][3], len(buffer))
        self.assertEqual(store.calls[1][4], [4, 2])
        self.assertEqual(store.calls[1][5], "int64")
        self.assertEqual(store.calls[1][6], 0)
        self.assertEqual(store.calls[1][7], [("select_idxs", [3, 1])])
        self.assertEqual(store.calls[1][8], 16)
        self.assertEqual(store.calls[2][0], "unregister_buffer")

    def test_materializer_accepts_preallocated_regions(self):
        store = FakeStore()
        request = TensorReadRequest(
            "input_ids", TensorFieldRef("batch/input_ids", (2, 2), "int64")
        )
        buffer = bytearray(request.output_nbytes() + 16)

        with writable_buffer_region(buffer) as region:
            ret = RemoteTensorBatchMaterializer(store).materialize_request_into_region(
                request, region
            )

        self.assertEqual(ret, request.output_nbytes())
        self.assertEqual(
            store.calls[0], ("register_buffer", store.calls[0][1], len(buffer))
        )
        self.assertEqual(store.calls[1][3], request.output_nbytes())

    def test_writable_buffer_region_releases_cast_source_view(self):
        buffer = memoryview(bytearray(8)).cast("H")

        with writable_buffer_region(buffer) as region:
            self.assertEqual(region.size, 8)

        buffer.release()

    def test_writable_buffer_region_close_is_idempotent(self):
        with writable_buffer_region(bytearray(8)) as region:
            region.close()
            region.close()

    def test_materializer_uses_allocator(self):
        store = FakeStore()
        remote = RemoteTensorBatch(
            {"input_ids": TensorFieldRef("batch/input_ids", (2, 2), "int64")}, 2
        )
        outputs = RemoteTensorBatchMaterializer(
            store, BytearrayBufferAllocator()
        ).materialize_buffers(remote)

        buffer, shape, dtype = outputs["input_ids"]
        self.assertEqual(len(buffer), tensor_nbytes((2, 2), "int64"))
        self.assertEqual(shape, (2, 2))
        self.assertEqual(dtype, "int64")

    def test_zero_byte_materialization_skips_store_calls(self):
        store = FakeStore()
        request = TensorReadRequest(
            "input_ids",
            TensorFieldRef("batch/input_ids", (2, 2), "int64"),
            selections=(TensorDimSelection.repeat(0),),
        )

        ret = RemoteTensorBatchMaterializer(store).materialize_request_into(
            request, bytearray()
        )

        self.assertEqual(ret, 0)
        self.assertEqual(store.calls, [])

    def test_materializer_rejects_oversized_outputs_before_allocation(self):
        store = FakeStore()
        remote = RemoteTensorBatch(
            {
                "input_ids": TensorFieldRef(
                    "batch/input_ids", (64 * 1024 * 1024 * 1024 + 1,), "uint8"
                )
            },
            64 * 1024 * 1024 * 1024 + 1,
        )

        with self.assertRaises(ValueError):
            RemoteTensorBatchMaterializer(store).materialize_buffers(remote)

        self.assertEqual(store.calls, [])

    def test_bool_mask_length_must_match(self):
        with self.assertRaises(IndexError):
            selection_output_shape(
                (4, 2), (TensorDimSelection.select_idxs([True, False]),)
            )


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
        tensor_int = torch.tensor([1, 2, 3, 4], dtype=torch.int32)
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
        tensor_3d = torch.tensor(
            [[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=torch.int64
        )
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

    def test_tensor_dim_selection_into(self):
        source = np.arange(4 * 2, dtype=np.int64).reshape(4, 2)
        output = self._materialize_selection(
            "test_tensor_dim_selection_into_input_ids",
            source,
            0,
            [("select_idxs", [3, 1]), ("repeat", (2, True))],
            (4, 2),
        )
        np.testing.assert_array_equal(output, source[[3, 3, 1, 1]])

        source = np.arange(2 * 4 * 3, dtype=np.int64).reshape(2, 4, 3)
        output = self._materialize_selection(
            "test_tensor_dim_selection_into_tokens",
            source,
            1,
            [("slice", (1, 4, 2))],
            (2, 2, 3),
        )
        np.testing.assert_array_equal(output, source[:, 1:4:2, :])

        source = np.arange(2 * 3 * 4, dtype=np.int64).reshape(2, 3, 4)
        output = self._materialize_selection(
            "test_tensor_dim_selection_into_hidden",
            source,
            -1,
            [("select_idxs", [3, 1])],
            (2, 3, 2),
        )
        np.testing.assert_array_equal(output, source[:, :, [3, 1]])

        source = np.arange(4, dtype=np.int64)
        output = self._materialize_selection(
            "test_tensor_dim_selection_into_cat",
            source,
            0,
            [("cat", [[("select_idxs", [3, 1])], [("slice", (0, 2, None))]])],
            (4,),
        )
        np.testing.assert_array_equal(output, source[[3, 1, 0, 1]])

        source = np.arange(6, dtype=np.int64)
        output = self._materialize_selection(
            "test_tensor_dim_selection_into_cat_after_slice",
            source,
            0,
            [
                ("slice", (1, 5, None)),
                ("cat", [[("select_idxs", [2, 0])], [("slice", (1, 3, None))]]),
            ],
            (4,),
        )
        np.testing.assert_array_equal(output, source[[3, 1, 2, 3]])

    def _materialize_selection(self, key, source, dim, selections, output_shape):
        output_size = int(np.prod(output_shape)) * source.dtype.itemsize
        buffer = (ctypes.c_ubyte * output_size)()
        buffer_ptr = ctypes.addressof(buffer)

        self.assertEqual(self.store.put(key, source.tobytes()), 0)
        self.assertEqual(self.store.register_buffer(buffer_ptr, output_size), 0)

        try:
            ret = self.store.get_tensor_dim_selection_into(
                key,
                buffer_ptr,
                output_size,
                list(source.shape),
                "int64",
                dim,
                selections,
            )
            self.assertEqual(ret, output_size)
            return np.frombuffer(bytes(buffer), dtype=np.int64).reshape(output_shape)
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)
            self.assertEqual(self.store.remove(key, True), 0)


if __name__ == "__main__":
    unittest.main()
