import ctypes
import os
import sys
import json
import time
import argparse
import unittest
import torch
import numpy as np
from dataclasses import dataclass
from mooncake.store import MooncakeDistributedStore
from mooncake.store import ReplicateConfig

from mooncake.mooncake_config import MooncakeConfig

import concurrent.futures

# ==========================================
#  Global Variables & Configuration
# ==========================================

# Global Store instance to ensure only one connection is established during the entire test session
GLOBAL_STORE = None
GLOBAL_CONFIG = None

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 16 * 1024 * 1024 * 1024  # 16 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 8 * 1024 * 1024 * 1024    # 8 GB
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_CHECK_SERVER = False

# Must match C++ TensorMetadata: int32_t dtype + int32_t ndim + int64_t shape[4]
TENSOR_METADATA_SIZE = 4 + 4 + 8 * 4  # 40 bytes


def serialized_tensor_size(tensor):
    """Size in bytes of [TensorMetadata][tensor data] as stored by get_tensor_into."""
    return TENSOR_METADATA_SIZE + tensor.numel() * tensor.element_size()

def verify_tensor_equality(original, received, rtol=0, atol=0, verbose=True):
    """
    compare two tensors.
    """
    def to_numpy(x):
        if isinstance(x, torch.Tensor):
            if x.is_cuda:
                x = x.cpu()
            return x.detach().numpy()
        elif isinstance(x, np.ndarray):
            return x
        else:
            raise TypeError(f"Unsupported tensor type: {type(x)}")

    try:
        orig_np = to_numpy(original)
        recv_np = to_numpy(received)
    except Exception as e:
        if verbose:
            print(f"❌ Error converting tensors: {e}")
        return False

    if orig_np.shape != recv_np.shape:
        if verbose:
            print(f"❌ Shape mismatch: original {orig_np.shape} vs received {recv_np.shape}")
        return False

    if orig_np.dtype != recv_np.dtype:
        if verbose:
            print(f"❌ Dtype mismatch: original {orig_np.dtype} vs received {recv_np.dtype}")
        return False

    if np.array_equal(orig_np, recv_np):
#        if verbose:
#            print("✅ Tensors are identical!")
        return True
    else:
        diff_mask = orig_np != recv_np
        diff_indices = np.where(diff_mask)
        if len(diff_indices[0]) > 0:
            first_diff_idx = tuple(idx[0] for idx in diff_indices)
            orig_val = orig_np[first_diff_idx]
            recv_val = recv_np[first_diff_idx]
            if verbose:
                print(f"❌ Tensors differ at index {first_diff_idx}")
                print(f"   Original: {orig_val}")
                print(f"   Received: {recv_val}")
                print(f"   Difference: {abs(orig_val - recv_val)}")
        return False

def parse_global_segment_size(value) -> int:
    """Parse human-readable size strings (e.g., '4GB') into bytes."""
    if isinstance(value, int): return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            return int(s[:-2].strip()) * 1024**3
        return int(s)
    return int(value)

def create_store_connection():
    """Create and connect to the Store (called only once by setUpModule)."""
    store = MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
    print(f"[{os.getpid()}] Connecting to Mooncake Master at {config.master_server_address} using {config.protocol}...")
    rc = store.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup mooncake store, error code: {rc}")
    return store, config

def generate_tensors(num_tensors, size_mb):
    """Generate random Tensors suitable for TP testing."""
    size_bytes = int(size_mb * 1024 * 1024)
    element_size = 4  # float32
    num_elements = size_bytes // element_size
    dim = int(np.sqrt(num_elements))
    dim = (dim // 8) * 8  # Adjust dimension to be divisible by common TP sizes (2, 4, 8)

    # Use random data and ensure the tensor is contiguous in memory
    tensors = [torch.randn(dim, dim, dtype=torch.float32).contiguous() for _ in range(num_tensors)]
    # Use timestamp to prevent key collision in rare edge cases (though we remove_all anyway)
    keys = [f"test_tensor_{i}_{int(time.time()*1000)}" for i in range(num_tensors)]
    return keys, tensors

# ==========================================
#  Module Level Setup/Teardown (Hooks)
# ==========================================

def setUpModule():
    """Executed once before all tests in this file: establishes the global connection."""
    global GLOBAL_STORE, GLOBAL_CONFIG
    try:
        GLOBAL_STORE, GLOBAL_CONFIG = create_store_connection()
        print("✅ Global Store connection established.")
    except Exception as e:
        print(f"❌ Failed to establish global store connection: {e}")
        sys.exit(1)

def tearDownModule():
    """Executed once after all tests in this file: closes the global connection."""
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None

# ==========================================
#  Base Test Class
# ==========================================

class MooncakeTestBase(unittest.TestCase):
    def setUp(self):
        """Executed before each test method (test_xxx)."""
        # 1. Access the global connection
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")

        self.store = GLOBAL_STORE
        self.config = GLOBAL_CONFIG

        # 2. [Critical] Clean environment before the test starts
        # This ensures no stale data from previous tests affects the current one
        self.store.remove_all()

# ==========================================
#  Functional Tests
# ==========================================

class TestMooncakeFunctional(MooncakeTestBase):
    def test_01_basic_put_get(self):
        """Verify basic put and get functionality."""
        key = "func_test_single"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)

        # Perform Put
        rc = self.store.put_tensor(key, tensor)
        self.assertEqual(rc, 0, f"put_tensor failed with rc={rc}")
        self.assertTrue(self.store.is_exist(key), "Key not found after put")

        # Perform Get
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved, "Get returned None")
        self.assertTrue(torch.equal(tensor, retrieved), "Data mismatch between original and retrieved tensor")

    def test_02_tp_single_tensor(self):
        """Verify TP (Tensor Parallelism) splitting and reconstruction for a single Tensor."""
        tp_size = 4
        split_dim = 1
        key = "func_test_tp_single"

        # Create a small tensor (e.g., 16MB)
        _, tensors = generate_tensors(1, 16)
        target_tensor = tensors[0]

        # 1. Put with TP
        rc = self.store.put_tensor_with_tp(key, target_tensor, tp_size=tp_size, split_dim=split_dim)
        self.assertEqual(rc, 0, "put_tensor_with_tp failed")

        # 2. Verify existence of shards (White-box check: key_tp_0, key_tp_1...)
        for rank in range(tp_size):
            shard_key = f"{key}_tp_{rank}"
            self.assertTrue(self.store.is_exist(shard_key), f"Shard key {shard_key} is missing in store")

        # 3. Get shards and Reconstruct
        slices = []
        expected_chunks = target_tensor.chunk(tp_size, split_dim)
        for rank in range(tp_size):
            t_slice = self.store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
            self.assertIsNotNone(t_slice, f"Slice for rank {rank} is None")
            self.assertTrue(torch.equal(t_slice, expected_chunks[rank]), f"Data mismatch for rank {rank}")
            slices.append(t_slice)

        reconstructed = torch.cat(slices, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, target_tensor), "Reconstructed tensor does not match original")

    def test_03_tp_batch(self):
        """Verify TP splitting and reconstruction for a Batch of Tensors."""
        tp_size = 2
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8) # Small size for functional testing

        # 1. Batch Put with TP
        results = self.store.batch_put_tensor_with_tp(keys, tensors, tp_size=tp_size, split_dim=split_dim)
        self.assertTrue(all(r == 0 for r in results), f"Batch put failed. Results: {results}")

        # 2. Batch Get per Rank
        all_shards = [] # List of lists: [ [shards_rank0...], [shards_rank1...] ]
        for rank in range(tp_size):
            shards = self.store.batch_get_tensor_with_tp(keys, tp_rank=rank, tp_size=tp_size)
            self.assertEqual(len(shards), num_tensors)
            all_shards.append(shards)

        # 3. Verify & Reconstruct
        for i in range(num_tensors):
            original = tensors[i]
            expected_chunks = original.chunk(tp_size, split_dim)
            reconstruction_parts = []

            for rank in range(tp_size):
                shard = all_shards[rank][i]
                self.assertTrue(torch.equal(shard, expected_chunks[rank]), 
                                f"Tensor {i} Rank {rank} data mismatch")
                reconstruction_parts.append(shard)

            recon = torch.cat(reconstruction_parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, original), f"Tensor {i} final reconstruction mismatch")

    def test_04_tp_consistency(self):
        input_tensor = torch.arange(12).view(3, 4)
        tp_size = 2
        split_dim = 1
        buffer_spacing = 1 * 1024 * 1024
        buffer_2 = (ctypes.c_ubyte * buffer_spacing)()
        buffer_3 = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr_2 = ctypes.addressof(buffer_2)
        buffer_ptr_3 = ctypes.addressof(buffer_3)
        res = self.store.register_buffer(buffer_ptr_2, buffer_spacing)
        self.assertEqual(res, 0, f"Buffer registration failed for buffer at {buffer_ptr_2}")
        res = self.store.register_buffer(buffer_ptr_3, buffer_spacing)
        self.assertEqual(res, 0, f"Buffer registration failed for buffer at {buffer_ptr_3}")
        # Zero-copy put: fill buffers from seed, then put_from (use actual serialized size)
        self.store.put_tensor_with_tp("key_seed", input_tensor, tp_size=tp_size, split_dim=split_dim)
        shard0 = self.store.get_tensor_with_tp_into("key_seed", buffer_ptr_2, buffer_spacing, tp_rank=0, tp_size=tp_size, split_dim=split_dim)
        shard1 = self.store.get_tensor_with_tp_into("key_seed", buffer_ptr_3, buffer_spacing, tp_rank=1, tp_size=tp_size, split_dim=split_dim)
        self.assertIsNotNone(shard0)
        self.assertIsNotNone(shard1)
        size0 = serialized_tensor_size(shard0)
        size1 = serialized_tensor_size(shard1)
        self.store.put_tensor_with_tp_from("key", buffer_ptr_2, size0, tp_rank=0, tp_size=tp_size, split_dim=split_dim)
        self.store.put_tensor_with_tp_from("key", buffer_ptr_3, size1, tp_rank=1, tp_size=tp_size, split_dim=split_dim)
        chunked_tensors = input_tensor.chunk(chunks=2, dim=1)
        tmp_tensor_0 = self.store.batch_get_tensor_with_tp(['key'], tp_rank=0, tp_size=tp_size)[0]
        tmp_tensor_1 = self.store.batch_get_tensor_with_tp(['key'], tp_rank=1, tp_size=tp_size)[0]
        self.assertTrue(tmp_tensor_0.sum() == chunked_tensors[0].sum())
        self.assertTrue(tmp_tensor_1.sum() == chunked_tensors[1].sum())
        tmp_tensor_2 = self.store.batch_get_tensor_with_tp_into(['key'], [buffer_ptr_2], [buffer_spacing], tp_rank=0, tp_size=tp_size)[0]
        tmp_tensor_3 = self.store.batch_get_tensor_with_tp_into(['key'], [buffer_ptr_3], [buffer_spacing], tp_rank=1, tp_size=tp_size)[0]
        self.assertTrue(tmp_tensor_2.sum() == chunked_tensors[0].sum())
        self.assertTrue(tmp_tensor_3.sum() == chunked_tensors[1].sum())
        res = self.store.unregister_buffer(buffer_ptr_2)
        self.assertEqual(res, 0, f"Buffer unregistration failed for buffer at {buffer_ptr_2}")
        res = self.store.unregister_buffer(buffer_ptr_3)
        self.assertEqual(res, 0, f"Buffer unregistration failed for buffer at {buffer_ptr_3}")

    def test_05_put_get_into(self):
        """Verify basic put and get into functionality (zero-copy put + get_into)."""
        key = "get_into_test"
        seed_key = "get_into_test_seed"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)
        buffer_spacing = 64 * 1024 * 1024
        total_buffer_size = buffer_spacing

        buf_put = (ctypes.c_ubyte * total_buffer_size)()
        buf_get = (ctypes.c_ubyte * total_buffer_size)()
        buf_put_ptr = ctypes.addressof(buf_put)
        buf_get_ptr = ctypes.addressof(buf_get)
        res = self.store.register_buffer(buf_put_ptr, total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration failed for put buffer")
        res = self.store.register_buffer(buf_get_ptr, total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration failed for get buffer")

        # Zero-copy put: fill buffer from seed, then put_tensor_from (use actual serialized size)
        rc = self.store.put_tensor(seed_key, tensor)
        self.assertEqual(rc, 0, f"put_tensor(seed) failed with rc={rc}")
        retrieved_seed = self.store.get_tensor_into(seed_key, buf_put_ptr, total_buffer_size)
        self.assertIsNotNone(retrieved_seed)
        put_size = serialized_tensor_size(retrieved_seed)
        rc = self.store.put_tensor_from(key, buf_put_ptr, put_size)
        self.assertEqual(rc, 0, f"put_tensor_from failed with rc={rc}")
        self.assertTrue(self.store.is_exist(key), "Key not found after put")

        retrieved = self.store.get_tensor_into(key, buf_get_ptr, total_buffer_size)
        self.assertIsNotNone(retrieved, "Get returned None")
        self.assertTrue(torch.equal(tensor, retrieved), f"Data mismatch between original and retrieved tensor, tensor: {tensor}, retrieved: {retrieved}")
        self.assertEqual(self.store.unregister_buffer(buf_put_ptr), 0, "Buffer unregistration failed for put buffer")
        self.assertEqual(self.store.unregister_buffer(buf_get_ptr), 0, "Buffer unregistration failed for get buffer")

    def test_06_batch_put_get_into(self):
        """Zero copy Batch Put and Batch Get."""
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)
        seed_keys = [f"{k}_seed" for k in keys]
        buffer_spacing = 64 * 1024 * 1024  # 64MB per tensor slot
        batch_size = len(keys)
        total_buffer_size = buffer_spacing * batch_size * 2  # put slots + get slots

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        put_ptrs = [large_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
        get_ptrs = [large_buffer_ptr + (batch_size + i) * buffer_spacing for i in range(batch_size)]
        buffer_sizes = [buffer_spacing] * batch_size

        res = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration failed")

        results = self.store.batch_put_tensor(seed_keys, tensors)
        self.assertTrue(all(r == 0 for r in results), f"Batch put(seed) failed. Results: {results}")
        self.store.batch_get_tensor_into(seed_keys, put_ptrs, buffer_sizes)
        put_sizes = [serialized_tensor_size(tensors[j]) for j in range(batch_size)]
        results = self.store.batch_put_tensor_from(keys, put_ptrs, put_sizes)
        self.assertTrue(all(r == 0 for r in results), f"Batch put_tensor_from failed. Results: {results}")

        res = self.store.batch_get_tensor_into(keys, get_ptrs, buffer_sizes)
        self.assertEqual(len(res), len(tensors))
        for j in range(batch_size):
            self.assertTrue(
                verify_tensor_equality(tensors[j], res[j]),
                f"Tensor {j} content mismatch, tensor: {tensors[j]}, res: {res[j]}"
            )
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration failed")

    def test_07_put_get_into_with_tp(self):
        """Zero copy Put/Get with TP — each rank has put buffer and get buffer."""
        tp_size = 4
        split_dim = 0
        key = "get_into_with_tp_test"
        seed_key = "get_into_with_tp_seed"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)

        # Step 1: Put seed with TP, then for each rank fill put buffer and put_tensor_with_tp_from
        result = self.store.put_tensor_with_tp(
            seed_key, tensor, tp_size=tp_size, split_dim=split_dim
        )
        self.assertEqual(result, 0, f"Put(seed) failed. Result: {result}")

        all_shards = []
        registered_buffers = []  # (put_buf, put_ptr, get_buf, get_ptr, size) for cleanup

        for rank in range(tp_size):
            buffer_spacing = 64 * 1024 * 1024
            total_buffer_size = buffer_spacing
            put_buffer = (ctypes.c_ubyte * total_buffer_size)()
            get_buffer = (ctypes.c_ubyte * total_buffer_size)()
            put_ptr = ctypes.addressof(put_buffer)
            get_ptr = ctypes.addressof(get_buffer)
            res = self.store.register_buffer(put_ptr, total_buffer_size)
            self.assertEqual(res, 0, f"Buffer registration failed for rank {rank} put")
            res = self.store.register_buffer(get_ptr, total_buffer_size)
            self.assertEqual(res, 0, f"Buffer registration failed for rank {rank} get")
            registered_buffers.append((put_buffer, put_ptr, get_buffer, get_ptr, total_buffer_size))

            shard = self.store.get_tensor_with_tp_into(
                seed_key, put_ptr, total_buffer_size,
                tp_rank=rank, tp_size=tp_size
            )
            self.assertIsNotNone(shard)
            put_size = serialized_tensor_size(shard)
            rc = self.store.put_tensor_with_tp_from(
                key, put_ptr, put_size,
                tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_with_tp_from failed for rank {rank}")
            shard = self.store.get_tensor_with_tp_into(
                key, get_ptr, total_buffer_size,
                tp_rank=rank, tp_size=tp_size
            )
            all_shards.append(shard)

        original = tensor
        expected_chunks = original.chunk(tp_size, split_dim)
        reconstruction_parts = []
        for rank in range(tp_size):
            shard = all_shards[rank]
            self.assertTrue(
                torch.equal(shard, expected_chunks[rank]),
                f"Tensor Rank {rank} data mismatch"
            )
            reconstruction_parts.append(shard)
        recon = torch.cat(reconstruction_parts, dim=split_dim)
        self.assertTrue(torch.equal(recon, original), "Tensor final reconstruction mismatch")

        for put_buf, put_ptr, get_buf, get_ptr, size in registered_buffers:
            self.assertEqual(self.store.unregister_buffer(put_ptr), 0, "Unregister put buffer failed")
            self.assertEqual(self.store.unregister_buffer(get_ptr), 0, "Unregister get buffer failed")

    def test_08_batch_put_get_into_with_tp(self):
        """Zero copy Batch Put/Get with TP — each rank has put and get buffers."""
        tp_size = 4
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)
        seed_keys = [f"{k}_seed" for k in keys]
        batch_size = len(keys)

        results = self.store.batch_put_tensor_with_tp(
            seed_keys, tensors, tp_size=tp_size, split_dim=split_dim
        )
        self.assertTrue(all(r == 0 for r in results), f"Batch put(seed) failed. Results: {results}")

        all_shards = []
        registered_buffers = []

        for rank in range(tp_size):
            buffer_spacing = 64 * 1024 * 1024
            total_buffer_size = buffer_spacing * batch_size * 2  # put + get
            large_buffer = (ctypes.c_ubyte * total_buffer_size)()
            large_buffer_ptr = ctypes.addressof(large_buffer)
            put_ptrs = [large_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
            get_ptrs = [large_buffer_ptr + (batch_size + i) * buffer_spacing for i in range(batch_size)]
            buffer_sizes = [buffer_spacing] * batch_size

            res = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
            self.assertEqual(res, 0, f"Buffer registration failed for rank {rank}")

            shards = self.store.batch_get_tensor_with_tp_into(
                seed_keys, put_ptrs, buffer_sizes,
                tp_rank=rank, tp_size=tp_size
            )
            self.assertEqual(len(shards), num_tensors)
            put_sizes = [serialized_tensor_size(shards[j]) for j in range(num_tensors)]
            results = self.store.batch_put_tensor_with_tp_from(
                keys, put_ptrs, put_sizes,
                tp_rank=rank, tp_size=tp_size
            )
            self.assertTrue(all(r == 0 for r in results), f"batch_put_tensor_with_tp_from failed for rank {rank}")

            shards = self.store.batch_get_tensor_with_tp_into(
                keys, get_ptrs, buffer_sizes,
                tp_rank=rank, tp_size=tp_size
            )
            self.assertEqual(len(shards), num_tensors)
            all_shards.append(shards)
            registered_buffers.append((large_buffer, large_buffer_ptr))

        for i in range(num_tensors):
            original = tensors[i]
            expected_chunks = original.chunk(tp_size, split_dim)
            reconstruction_parts = []
            for rank in range(tp_size):
                shard = all_shards[rank][i]
                self.assertTrue(
                    torch.equal(shard, expected_chunks[rank]),
                    f"Tensor {i} Rank {rank} data mismatch"
                )
                reconstruction_parts.append(shard)
            recon = torch.cat(reconstruction_parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, original), f"Tensor {i} final reconstruction mismatch")

        for large_buffer, ptr in registered_buffers:
            self.assertEqual(self.store.unregister_buffer(ptr), 0, "Buffer unregistration failed")

    def test_09_pub_get(self):
        """Verify pub and get functionality."""
        key = "func_pub_test"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)

        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        # Perform Put
        rc = self.store.pub_tensor(key, tensor, repconfig)
        self.assertEqual(rc, 0, f"put_tensor failed with rc={rc}")
        self.assertTrue(self.store.is_exist(key), "Key not found after put")

        # Perform Get
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved, "Get returned None")
        self.assertTrue(torch.equal(tensor, retrieved), "Data mismatch between original and retrieved tensor")

    def test_10_pub_tp_single_tensor(self):
        """Verify TP (Tensor Parallelism) splitting and reconstruction for a single Tensor."""
        tp_size = 4
        split_dim = 1
        key = "func_pub_tp_single"

        # Create a small tensor (e.g., 16MB)
        _, tensors = generate_tensors(1, 16)
        target_tensor = tensors[0]

        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        # 1. Pub with TP
        rc = self.store.pub_tensor_with_tp(key, target_tensor, config=repconfig, tp_size=tp_size, split_dim=split_dim)
        self.assertEqual(rc, 0, "pub_tensor_with_tp failed")

        # 2. Verify existence of shards (White-box check: key_tp_0, key_tp_1...)
        for rank in range(tp_size):
            shard_key = f"{key}_tp_{rank}"
            self.assertTrue(self.store.is_exist(shard_key), f"Shard key {shard_key} is missing in store")

        # 3. Get shards and Reconstruct
        slices = []
        expected_chunks = target_tensor.chunk(tp_size, split_dim)
        for rank in range(tp_size):
            t_slice = self.store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
            self.assertIsNotNone(t_slice, f"Slice for rank {rank} is None")
            self.assertTrue(torch.equal(t_slice, expected_chunks[rank]), f"Data mismatch for rank {rank}")
            slices.append(t_slice)

        reconstructed = torch.cat(slices, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, target_tensor), "Reconstructed tensor does not match original")

    def test_11_pub_tp_batch(self):
        """Verify TP splitting and reconstruction for a Batch of Tensors."""
        tp_size = 2
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8) # Small size for functional testing

        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        # 1. Batch Pub with TP
        results = self.store.batch_pub_tensor_with_tp(keys, tensors, config=repconfig, tp_size=tp_size, split_dim=split_dim)
        self.assertTrue(all(r == 0 for r in results), f"Batch put failed. Results: {results}")

        # 2. Batch Get per Rank
        all_shards = [] # List of lists: [ [shards_rank0...], [shards_rank1...] ]
        for rank in range(tp_size):
            shards = self.store.batch_get_tensor_with_tp(keys, tp_rank=rank, tp_size=tp_size)
            self.assertEqual(len(shards), num_tensors)
            all_shards.append(shards)

        # 3. Verify & Reconstruct
        for i in range(num_tensors):
            original = tensors[i]
            expected_chunks = original.chunk(tp_size, split_dim)
            reconstruction_parts = []

            for rank in range(tp_size):
                shard = all_shards[rank][i]
                self.assertTrue(torch.equal(shard, expected_chunks[rank]),
                                f"Tensor {i} Rank {rank} data mismatch")
                reconstruction_parts.append(shard)

            recon = torch.cat(reconstruction_parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, original), f"Tensor {i} final reconstruction mismatch")

# ==========================================
#  Performance/Benchmark Tests
# ==========================================

class TestMooncakeBenchmark(MooncakeTestBase):
    # Benchmark Settings
    BENCH_ITERATIONS = 5
    TENSOR_SIZE_MB = 16
    TOTAL_SIZE_MB = 256

    def setUp(self):
        """Benchmark-specific setUp."""
        # 1. Call parent setUp to clean the store (remove_all)
        super().setUp()

        # 2. Generate test data
        total_bytes = int(self.TOTAL_SIZE_MB * 1024**2)
        tensor_bytes = self.TENSOR_SIZE_MB * 1024**2
        self.num_tensors = max(1, total_bytes // tensor_bytes)

        print(f"\n[Gen] Generating {self.num_tensors} tensors (~{self.TENSOR_SIZE_MB}MB each)...")
        self.keys, self.tensors = generate_tensors(self.num_tensors, self.TENSOR_SIZE_MB)
        self.total_bits = (tensor_bytes * self.num_tensors) * 8

    def _print_perf(self, name, times):
        avg_time = np.mean(times)
        avg_gbps = (self.total_bits / 1e9) / avg_time
        print(f"👉 [Result] {name:30} | Avg Time: {avg_time:.4f}s | Throughput: {avg_gbps:.2f} Gbps")

    def test_benchmark_01_batch_put_get(self):
        """Benchmark: Standard Batch Put/Get."""
        put_times = []
        get_times = []

        print(f"--- Running Standard Batch Benchmark ({self.BENCH_ITERATIONS} iters) ---")
        for i in range(self.BENCH_ITERATIONS):
            # Clean store before each iteration for "cold" writes
            self.store.remove_all()

            # Measure Put
            t0 = time.perf_counter()
            self.store.batch_put_tensor(self.keys, self.tensors)
            put_times.append(time.perf_counter() - t0)

            # Measure Get
            t0 = time.perf_counter()
            res = self.store.batch_get_tensor(self.keys)
            get_times.append(time.perf_counter() - t0)
            self.assertEqual(len(res), len(self.tensors))

        self._print_perf("Standard Batch Put", put_times)
        self._print_perf("Standard Batch Get", get_times)

    def test_benchmark_02_tp_batch(self):
        """Benchmark: TP Batch Put/Get."""
        tp_size = 4
        split_dim = 0
        put_times = []
        get_times = []

        print(f"--- Running TP Batch Benchmark (TP={tp_size}) ---")
        for i in range(self.BENCH_ITERATIONS):
            self.store.remove_all()

            # Measure TP Put (Auto-chunking)
            t0 = time.perf_counter()
            self.store.batch_put_tensor_with_tp(self.keys, self.tensors, tp_size=tp_size, split_dim=split_dim)
            put_times.append(time.perf_counter() - t0)

            # Measure TP Get (Simulating gathering all ranks)
            t_get_start = time.perf_counter()
            for rank in range(tp_size):
                res = self.store.batch_get_tensor_with_tp(self.keys, tp_rank=rank, tp_size=tp_size)
                self.assertEqual(len(res), len(self.tensors))
            get_times.append(time.perf_counter() - t_get_start)

        self._print_perf(f"TP Batch Put (TP={tp_size})", put_times)
        self._print_perf(f"TP Batch Get (TP={tp_size})", get_times)

    def test_benchmark_03_batch_put_get_into(self):
        """Benchmark: Zero copy Batch Put and Batch Get."""
        self.store.remove_all()
        buffer_spacing = 300 * 1024 * 1024  # 300MB per tensor slot
        batch_size = len(self.keys)
        total_buffer_size = buffer_spacing * batch_size
        seed_keys = [f"seed_{k}" for k in self.keys]

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        buffer_ptrs = []
        buffer_sizes = []
        for i in range(batch_size):
            offset = i * buffer_spacing
            buffer_ptrs.append(large_buffer_ptr + offset)
            buffer_sizes.append(buffer_spacing)

        res = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration should succeed")

        print(f"--- Running zero copy Batch Put/Get Benchmark ({self.BENCH_ITERATIONS} iters) ---")
        put_times = []
        get_times = []

        put_sizes = [serialized_tensor_size(self.tensors[j]) for j in range(batch_size)]
        for i in range(self.BENCH_ITERATIONS):
            self.store.remove_all()
            self.store.batch_put_tensor(seed_keys, self.tensors)
            self.store.batch_get_tensor_into(seed_keys, buffer_ptrs, buffer_sizes)

            t0 = time.perf_counter()
            self.store.batch_put_tensor_from(self.keys, buffer_ptrs, put_sizes)
            put_times.append(time.perf_counter() - t0)

            t0 = time.perf_counter()
            res = self.store.batch_get_tensor_into(self.keys, buffer_ptrs, buffer_sizes)
            get_times.append(time.perf_counter() - t0)
            self.assertEqual(len(res), len(self.tensors))
            for j in range(batch_size):
                self.assertTrue(
                    verify_tensor_equality(self.tensors[j], res[j]),
                    f"Tensor {j} content mismatch"
                )

        self._print_perf("Zero copy Batch Put (put_tensor_from)", put_times)
        self._print_perf("Zero copy Batch Get", get_times)
        self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration failed")

    def test_benchmark_04_batch_put_get_into_with_tp(self):
        """Benchmark: Zero copy Batch Put/Get with TP."""
        tp_size = 4
        split_dim = 0
        batch_size = len(self.keys)
        self.store.remove_all()
        buffer_spacing = 64 * 1024 * 1024  # 64MB per tensor slot
        seed_keys = [f"seed_{k}" for k in self.keys]

        rank_buffers = []
        for rank in range(tp_size):
            total_buffer_size = buffer_spacing * batch_size
            large_buffer = (ctypes.c_ubyte * total_buffer_size)()
            large_buffer_ptr = ctypes.addressof(large_buffer)
            buffer_ptrs = [large_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
            buffer_sizes = [buffer_spacing] * batch_size
            res = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
            self.assertEqual(res, 0, f"Buffer registration failed for rank {rank}")
            rank_buffers.append({
                'buffer_obj': large_buffer,
                'ptrs': buffer_ptrs,
                'sizes': buffer_sizes,
                'base_ptr': large_buffer_ptr,
            })

        print(f"--- Running zero copy Batch Put/Get Benchmark (TP={tp_size}, {self.BENCH_ITERATIONS} iters) ---")
        put_times = []
        get_times = []

        for i in range(self.BENCH_ITERATIONS):
            self.store.remove_all()
            self.store.batch_put_tensor_with_tp(seed_keys, self.tensors, tp_size=tp_size, split_dim=split_dim)
            rank_put_sizes = []
            for rank in range(tp_size):
                shards = self.store.batch_get_tensor_with_tp_into(
                    seed_keys, rank_buffers[rank]['ptrs'], rank_buffers[rank]['sizes'],
                    tp_rank=rank, tp_size=tp_size
                )
                rank_put_sizes.append([serialized_tensor_size(shards[j]) for j in range(batch_size)])

            t0 = time.perf_counter()
            for rank in range(tp_size):
                self.store.batch_put_tensor_with_tp_from(
                    self.keys, rank_buffers[rank]['ptrs'], rank_put_sizes[rank],
                    tp_rank=rank, tp_size=tp_size
                )
            put_times.append(time.perf_counter() - t0)

            t0 = time.perf_counter()
            all_res = []
            for rank in range(tp_size):
                res = self.store.batch_get_tensor_with_tp_into(
                    self.keys,
                    rank_buffers[rank]['ptrs'],
                    rank_buffers[rank]['sizes'],
                    tp_rank=rank,
                    tp_size=tp_size
                )
                self.assertEqual(len(res), batch_size)
                all_res.append(res)
            get_times.append(time.perf_counter() - t0)

            for j in range(batch_size):
                original = self.tensors[j]
                expected_shard = original.chunk(tp_size, split_dim)[0]
                actual = all_res[0][j]
                self.assertTrue(
                    torch.equal(actual, expected_shard),
                    f"Tensor {j} content mismatch on rank 0"
                )

        self._print_perf(f"Zero copy Batch Put with tp (TP={tp_size})", put_times)
        self._print_perf(f"Zero copy Batch Get with tp (TP={tp_size})", get_times)
        for buf_info in rank_buffers:
            self.assertEqual(self.store.unregister_buffer(buf_info['base_ptr']), 0, "Buffer unregistration failed")

    def test_benchmark_05_batch_pub_get(self):
        """Benchmark: Standard Batch Pub/Get."""
        put_times = []
        get_times = []
        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        print(f"--- Running Standard Batch Benchmark ({self.BENCH_ITERATIONS} iters) ---")
        for i in range(self.BENCH_ITERATIONS):
            # Clean store before each iteration for "cold" writes
            self.store.remove_all()

            # Measure Put
            t0 = time.perf_counter()
            self.store.batch_pub_tensor(self.keys, self.tensors, repconfig)
            put_times.append(time.perf_counter() - t0)

            # Measure Get
            t0 = time.perf_counter()
            res = self.store.batch_get_tensor(self.keys)
            get_times.append(time.perf_counter() - t0)
            self.assertEqual(len(res), len(self.tensors))

        self._print_perf("Standard Batch Pub", put_times)
        self._print_perf("Standard Batch Get", get_times)

    def test_benchmark_06_pub_tp_batch(self):
        """Benchmark: TP Batch Pub/Get."""
        tp_size = 4
        split_dim = 0
        put_times = []
        get_times = []
        repconfig = ReplicateConfig()
        repconfig.replica_num = 1

        print(f"--- Running TP Batch Benchmark (TP={tp_size}) ---")
        for i in range(self.BENCH_ITERATIONS):
            self.store.remove_all()

            # Measure TP Put (Auto-chunking)
            t0 = time.perf_counter()
            self.store.batch_pub_tensor_with_tp(self.keys, self.tensors, config=repconfig, tp_size=tp_size, split_dim=split_dim)
            put_times.append(time.perf_counter() - t0)

            # Measure TP Get (Simulating gathering all ranks)
            t_get_start = time.perf_counter()
            for rank in range(tp_size):
                res = self.store.batch_get_tensor_with_tp(self.keys, tp_rank=rank, tp_size=tp_size)
                self.assertEqual(len(res), len(self.tensors))
            get_times.append(time.perf_counter() - t_get_start)

        self._print_perf(f"TP Batch Pub (TP={tp_size})", put_times)
        self._print_perf(f"TP Batch Get (TP={tp_size})", get_times)

# ==========================================
#  Stress/Concurrency Tests
# ==========================================
class TestMooncakeStress(MooncakeTestBase):
    """
    Stress tests with Fixed Operation Count and Pre-generated Data.
    """
    # Default Config (Overridden by main)
    NUM_THREADS = 8
    TOTAL_ITEMS = 800    # Total number of items to process across all threads
    TENSOR_SIZE_MB = 4    # Size per tensor

    def _run_stress_worker(self, thread_id, items_per_thread):
        """
        Worker function:
        1. PRE-GENERATES data (to exclude generation time from benchmark).
        2. Performs Put -> Get -> Verify loop.
        """
        ops_count = 0
        failure_msg = None

        # Pre-calculate dimensions
        element_size = 4 # float32
        num_elements = (self.TENSOR_SIZE_MB * 1024 * 1024) // element_size
        dim = int(np.sqrt(num_elements))

        # --- Phase 1: Pre-generate Data ---
        # "Don't keep generating random data" -> We generate a pool first.
        # This ensures we measure store performance, not RNG performance.
        print(f"   [Thread {thread_id}] Pre-generating {items_per_thread} tensors...")
        data_pool = []
        for i in range(items_per_thread):
            key = f"stress_fixed_t{thread_id}_{i}"
            # Create random tensor
            tensor = torch.randn(dim, dim, dtype=torch.float32)
            data_pool.append((key, tensor))

        # Barrier logic simulation: wait for main test to indicate start? 
        # In simple unittest, we just start processing.

        # --- Phase 2: Execution (Timed) ---
        t_start = time.perf_counter()

        try:
            for key, original_tensor in data_pool:
                # 1. WRITE (Put)
                rc = self.store.put_tensor(key, original_tensor)
                if rc != 0:
                    raise RuntimeError(f"Put failed for {key}, rc={rc}")

                # 2. READ (Get)
                retrieved_tensor = self.store.get_tensor(key)

                # 3. VALIDATE
                if retrieved_tensor is None:
                    raise RuntimeError(f"Get returned None for key {key}")

                if not torch.equal(original_tensor, retrieved_tensor):
                    raise RuntimeError(f"Data Mismatch for {key}!")

                ops_count += 1

        except Exception as e:
            failure_msg = str(e)

        t_duration = time.perf_counter() - t_start
        return ops_count, t_duration, failure_msg

    def test_stress_consistency_fixed(self):
        """
        Run a fixed number of operations with data consistency checks.
        """
        items_per_thread = self.TOTAL_ITEMS // self.NUM_THREADS
        # Adjust for remainder if any

        print(f"\n--- [Stress] Running Fixed Count Test ({self.TOTAL_ITEMS} items total) ---")
        print(f"--- Config: {self.NUM_THREADS} Threads, ~{items_per_thread} items/thread, {self.TENSOR_SIZE_MB}MB each ---")

        futures = []
        total_ops = 0
        errors = []

        # We measure wall time from when threads are submitted until all are done
        t0 = time.perf_counter()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.NUM_THREADS) as executor:
            # Distribute work
            for i in range(self.NUM_THREADS):
                count = items_per_thread + (1 if i < (self.TOTAL_ITEMS % self.NUM_THREADS) else 0)
                futures.append(executor.submit(self._run_stress_worker, i, count))

            # Gather results
            for future in concurrent.futures.as_completed(futures):
                ops, duration, error = future.result()
                total_ops += ops
                if error:
                    errors.append(error)

        elapsed = time.perf_counter() - t0

        # Reporting
        print(f"\n--- [Stress Report] ---")
        if errors:
            print(f"❌ FAILED with {len(errors)} errors.")
            print(f"First Error: {errors[0]}")
            self.fail(f"Stress test failed with {len(errors)} errors.")
        else:
            total_data_gb = (total_ops * self.TENSOR_SIZE_MB) / 1024
            throughput_gbps = (total_data_gb * 8) / elapsed

            print(f"✅ PASSED (No Consistency Errors)")
            print(f"Total Items:    {total_ops}")
            print(f"Wall Time:      {elapsed:.4f} s")
            print(f"Avg QPS:        {total_ops / elapsed:.2f} ops/s")
            print(f"Avg Goodput:    {throughput_gbps:.2f} Gbps")


# ==========================================
#  Data Type & Precision Tests (Full Enum)
# ==========================================

class TestMooncakeDataTypes(MooncakeTestBase):
    def _test_dtype_roundtrip(self, dtype, name, expected_enum_name=None):
        """
        Generic test for put/get consistency.
        Args:
            dtype: The torch.dtype to test.
            name: Readable name for logging.
            expected_enum_name: (Optional) If we could inspect the C++ enum value, we would check this.
        """
        key = f"dtype_check_{name}"
        shape = (64, 64)

        if dtype == torch.bool:
            original = torch.randint(0, 2, shape).bool()
        elif dtype.is_floating_point:
            original = torch.randn(shape, dtype=torch.float32).to(dtype)
        else:
            if dtype == torch.int8:
                original = torch.randint(-128, 127, shape, dtype=dtype)
            elif dtype == torch.uint8:
                original = torch.randint(0, 255, shape, dtype=dtype)
            else:
                original = torch.randint(-1000, 1000, shape, dtype=dtype)

        # The C++ store will infer the Enum based on original.dtype
        rc = self.store.put_tensor(key, original)
        if rc != 0:
            print(f"   [Fail] {name:<15} Put failed with rc={rc}")
            self.fail(f"Put failed for {name}")

        retrieved = self.store.get_tensor(key)
        if retrieved is None:
            print(f"   [Fail] {name:<15} Get returned None")
            self.fail(f"Get returned None for {name}")

        # We expect the retrieved tensor to have the same dtype as input
        if original.dtype != retrieved.dtype:
            msg = f"Dtype mismatch for {name}! Input: {original.dtype}, Output: {retrieved.dtype}"
            print(f"   [Fail] {name:<15} {msg}")
            self.fail(msg)

        # Use byte-view comparison for robustness (especially for FP8/BF16 on CPU)
        try:
            # Cast to untyped storage byte view (or uint8 view)
            t1_bytes = original.view(torch.uint8) if original.element_size() > 0 else original
            t2_bytes = retrieved.view(torch.uint8) if retrieved.element_size() > 0 else retrieved
            is_equal = torch.equal(t1_bytes, t2_bytes)
        except Exception:
            # Fallback for types that might fail view() or equal()
            is_equal = torch.equal(original.cpu(), retrieved.cpu())

        if not is_equal:
            print(f"   [Fail] {name:<15} Data content mismatch")
            self.fail(f"Data content mismatch for {name}")

        buffer_spacing = 1 * 1024 * 1024
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        res = self.store.register_buffer(buffer_ptr, buffer_spacing)
        self.assertEqual(res, 0, f"Buffer registration failed for buffer at {buffer_ptr}")
        retrieved = self.store.get_tensor_into(key, buffer_ptr, buffer_spacing)
        if retrieved is None:
            print(f"   [Fail] {name:<15} Get returned None")
            self.fail(f"Get returned None for {name}")

        # We expect the retrieved tensor to have the same dtype as input
        if original.dtype != retrieved.dtype:
            msg = f"Dtype mismatch for {name}! Input: {original.dtype}, Output: {retrieved.dtype}"
            print(f"   [Fail] {name:<15} {msg}")
            self.fail(msg)

        # Use byte-view comparison for robustness (especially for FP8/BF16 on CPU)
        try:
            # Cast to untyped storage byte view (or uint8 view)
            t1_bytes = original.view(torch.uint8) if original.element_size() > 0 else original
            t2_bytes = retrieved.view(torch.uint8) if retrieved.element_size() > 0 else retrieved
            is_equal = torch.equal(t1_bytes, t2_bytes)
        except Exception:
            # Fallback for types that might fail view() or equal()
            is_equal = torch.equal(original.cpu(), retrieved.cpu())

        if not is_equal:
            print(f"   [Fail] {name:<15} Data content mismatch")
            self.fail(f"Data content mismatch for {name}")
        res = self.store.unregister_buffer(buffer_ptr)
        self.assertEqual(res, 0, f"Buffer unregistration failed for buffer at {buffer_ptr}")

        print(f"   [Pass] {name:<15} {str(dtype)}")


    def test_all_dtypes(self):
        print("\n--- Testing All Supported PyTorch Data Types ---")

        test_cases = [
            ("FLOAT32",     torch.float32),
            ("FLOAT64",     torch.float64),
            ("INT8",        torch.int8),
            ("UINT8",       torch.uint8),
            ("INT16",       torch.int16),
            ("INT32",       torch.int32),
            ("INT64",       torch.int64),
            ("BOOL",        torch.bool),
            ("FLOAT16",     torch.float16),
            ("BFLOAT16",    torch.bfloat16),
        ]

        for name, dtype in test_cases:
            with self.subTest(dtype=name):
                self._test_dtype_roundtrip(dtype, name)

    def test_fp8_types(self):
        print("\n--- Testing FP8 Types ---")

        fp8_cases = []
        # Check support dynamically
        if hasattr(torch, 'float8_e4m3fn'):
            fp8_cases.append(("FLOAT8_E4M3", torch.float8_e4m3fn)) # Enum 13
        else:
            print("   [Skip] FLOAT8_E4M3 (Not supported in this PyTorch version)")

        if hasattr(torch, 'float8_e5m2'):
            fp8_cases.append(("FLOAT8_E5M2", torch.float8_e5m2))   # Enum 14
        else:
            print("   [Skip] FLOAT8_E5M2 (Not supported in this PyTorch version)")

        for name, dtype in fp8_cases:
            with self.subTest(dtype=name):
                self._test_dtype_roundtrip(dtype, name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake Distributed Store Tests")
    parser.add_argument("--mode", type=str, default="all", choices=["all", "func", "perf", "stress", "types"],
                        help="Run mode")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads")
    parser.add_argument("--count", type=int, default=800, help="Total number of items to process")
    parser.add_argument("--size_mb", type=float, default=0.5, help="Tensor size in MB")

    args, unknown = parser.parse_known_args()

    # Update Stress Test Config
    TestMooncakeStress.NUM_THREADS = args.threads
    TestMooncakeStress.TOTAL_ITEMS = args.count
    TestMooncakeStress.TENSOR_SIZE_MB = args.size_mb

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    if args.mode in ["all", "func"]:
        print(">> Loading Functional Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeFunctional))

    if args.mode in ["all", "perf"]:
        print(">> Loading Performance Benchmark Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeBenchmark))

    if args.mode in ["all", "stress"]:
        print(f">> Loading Stress Tests ({args.count} items, {args.threads} threads)...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeStress))

    if args.mode in ["all", "types", "func"]: # 'types' can be part of 'func' or standalone
        print(">> Loading Data Type Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeDataTypes))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    sys.exit(not result.wasSuccessful())
