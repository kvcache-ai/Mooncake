import ctypes
import os
import sys
import time
import argparse
import unittest
import torch
import numpy as np
from mooncake.store import MooncakeDistributedStore
from mooncake.store import ReplicateConfig
from mooncake.mooncake_config import MooncakeConfig

# ==========================================
#  Global Variables & Configuration
# ==========================================

# Global Store instance to ensure only one connection is established during the entire test session
GLOBAL_STORE = None
GLOBAL_CONFIG = None

def verify_tensor_equality(original, received, rtol=0, atol=0, verbose=True):
    """
    Compare two tensors.
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
#  Functional Tests for Chunk API
# ==========================================

class TestTensorChunkAPI(MooncakeTestBase):
    """Test cases for put_tensor_chunk_with_tp and get_tensor_with_tp APIs."""

    def test_01_basic_chunk_put_get_fast_path(self):
        """Test basic chunk put/get with fast path (put_tp_size == get_tp_size)."""
        key = "chunk_test_fast_path"
        put_tp_size = 4
        get_tp_size = 4  # Same as put_tp_size, should use fast path
        split_dim = 0

        # Create a tensor that can be evenly split
        tensor = torch.randn(8, 16, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk independently
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get each chunk (should use fast path)
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, chunks[rank]),
                f"Data mismatch for rank {rank}"
            )

        # Verify reconstruction
        all_chunks = []
        for rank in range(get_tp_size):
            chunk = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            all_chunks.append(chunk)
        reconstructed = torch.cat(all_chunks, dim=split_dim)
        self.assertTrue(
            torch.equal(reconstructed, tensor),
            "Reconstructed tensor does not match original"
        )

    def test_02_chunk_put_get_slow_path(self):
        """Test chunk put/get with slow path (put_tp_size != get_tp_size)."""
        key = "chunk_test_slow_path"
        put_tp_size = 4
        get_tp_size = 2  # Different from put_tp_size, should use slow path
        split_dim = 0

        # Create a tensor
        tensor = torch.randn(8, 16, dtype=torch.float32)
        put_chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk independently
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, put_chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get with different tp_size (should use slow path to reconstruct)
        expected_get_chunks = tensor.chunk(get_tp_size, split_dim)
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, expected_get_chunks[rank]),
                f"Data mismatch for rank {rank}"
            )

        # Verify reconstruction
        all_chunks = []
        for rank in range(get_tp_size):
            chunk = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            all_chunks.append(chunk)
        reconstructed = torch.cat(all_chunks, dim=split_dim)
        if not verify_tensor_equality(reconstructed, tensor, verbose=True):
            self.fail("Reconstructed tensor does not match original")

    def test_03_chunk_split_dim_0(self):
        """Test chunk put/get with split_dim=0 (row-wise splitting)."""
        key = "chunk_test_split_dim_0"
        put_tp_size = 4
        get_tp_size = 4
        split_dim = 0

        # Create a 2D tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get and verify
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, chunks[rank]),
                f"Data mismatch for rank {rank} with split_dim=0"
            )

    def test_04_chunk_split_dim_1(self):
        """Test chunk put/get with split_dim=1 (column-wise splitting)."""
        key = "chunk_test_split_dim_1"
        put_tp_size = 4
        get_tp_size = 4
        split_dim = 1

        # Create a 2D tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get and verify
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, chunks[rank]),
                f"Data mismatch for rank {rank} with split_dim=1"
            )

    def test_05_chunk_3d_tensor(self):
        """Test chunk put/get with 3D tensor."""
        key = "chunk_test_3d"
        put_tp_size = 2
        get_tp_size = 2
        split_dim = 0

        # Create a 3D tensor
        tensor = torch.randn(8, 16, 32, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get and verify
        all_chunks = []
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            all_chunks.append(retrieved)

        reconstructed = torch.cat(all_chunks, dim=split_dim)
        self.assertTrue(
            torch.equal(reconstructed, tensor),
            "3D tensor reconstruction failed"
        )

    def test_06_chunk_4d_tensor(self):
        """Test chunk put/get with 4D tensor."""
        key = "chunk_test_4d"
        put_tp_size = 2
        get_tp_size = 2
        split_dim = 1

        # Create a 4D tensor
        tensor = torch.randn(4, 8, 16, 32, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)

        # Put each chunk
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get and verify
        all_chunks = []
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            all_chunks.append(retrieved)

        reconstructed = torch.cat(all_chunks, dim=split_dim)
        self.assertTrue(
            torch.equal(reconstructed, tensor),
            "4D tensor reconstruction failed"
        )

    def test_07_chunk_uneven_split(self):
        """Test chunk put/get with uneven split (dim_size not divisible by tp_size)."""
        key = "chunk_test_uneven"
        put_tp_size = 3
        get_tp_size = 3
        split_dim = 0

        # Create a tensor with size not divisible by tp_size
        tensor = torch.randn(10, 16, dtype=torch.float32)
        # Use specific split sizes to match Mooncake's expected distribution (Base+1, Base, Base)
        # 10 % 3 = 1 -> First rank gets 4, others get 3.
        chunks = list(torch.split(tensor, [4, 3, 3], dim=split_dim))

        # Put each chunk
        for rank in range(put_tp_size):
            # Pass full_shape explicitly for uneven splits
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim,
                full_shape=list(tensor.shape)
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get and verify
        all_chunks = []
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            if not verify_tensor_equality(retrieved, chunks[rank], verbose=True):
                 self.fail(f"Data mismatch for rank {rank} with uneven split")
            all_chunks.append(retrieved)

        reconstructed = torch.cat(all_chunks, dim=split_dim)
        self.assertTrue(
            torch.equal(reconstructed, tensor),
            "Uneven split reconstruction failed"
        )

    def test_08_chunk_put_get_different_tp_sizes(self):
        """Test chunk put/get with different TP sizes (put_tp_size=4, get_tp_size=2)."""
        key = "chunk_test_diff_tp"
        put_tp_size = 4
        get_tp_size = 2
        split_dim = 0

        # Create a tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        put_chunks = tensor.chunk(put_tp_size, split_dim)

        # Put with put_tp_size=4
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, put_chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get with get_tp_size=2 (should reconstruct from 4 chunks)
        expected_get_chunks = tensor.chunk(get_tp_size, split_dim)
        all_chunks = []
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, expected_get_chunks[rank]),
                f"Data mismatch for rank {rank} when get_tp_size != put_tp_size"
            )
            all_chunks.append(retrieved)

        reconstructed = torch.cat(all_chunks, dim=split_dim)
        if not verify_tensor_equality(reconstructed, tensor, verbose=True):
            self.fail("Reconstruction failed with different TP sizes")

    def test_09_chunk_put_get_different_tp_sizes_reverse(self):
        """Test chunk put/get with different TP sizes (put_tp_size=2, get_tp_size=4)."""
        key = "chunk_test_diff_tp_reverse"
        put_tp_size = 2
        get_tp_size = 4
        split_dim = 0

        # Create a tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        put_chunks = tensor.chunk(put_tp_size, split_dim)

        # Put with put_tp_size=2
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, put_chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get with get_tp_size=4 (should split the 2 chunks into 4)
        expected_get_chunks = tensor.chunk(get_tp_size, split_dim)
        all_chunks = []
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, expected_get_chunks[rank]),
                f"Data mismatch for rank {rank} when get_tp_size > put_tp_size"
            )
            all_chunks.append(retrieved)

        reconstructed = torch.cat(all_chunks, dim=split_dim)
        if not verify_tensor_equality(reconstructed, tensor, verbose=True):
            self.fail("Reconstruction failed with get_tp_size > put_tp_size")

    def test_10_chunk_split_dim_mismatch_error(self):
        """Test that split_dim mismatch returns error."""
        key = "chunk_test_split_dim_mismatch"
        put_tp_size = 4
        get_tp_size = 4
        put_split_dim = 0
        get_split_dim = 1  # Different from put_split_dim

        # Create a tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, put_split_dim)

        # Put with split_dim=0
        for rank in range(put_tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=put_split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Try to get with split_dim=1 (should fail)
        retrieved = self.store.get_tensor_with_tp(
            key, tp_rank=0, tp_size=get_tp_size, split_dim=get_split_dim
        )
        self.assertIsNone(
            retrieved,
            "get_tensor_with_tp should return None when split_dim mismatches"
        )

    def test_11_chunk_batch_put_get(self):
        """Test batch operations with chunk API."""
        num_tensors = 4
        put_tp_size = 4
        get_tp_size = 4
        split_dim = 0

        # Generate test tensors
        keys = [f"chunk_batch_test_{i}" for i in range(num_tensors)]
        tensors = [torch.randn(8, 16, dtype=torch.float32) for _ in range(num_tensors)]

        # Put each tensor's chunks
        for i, tensor in enumerate(tensors):
            chunks = tensor.chunk(put_tp_size, split_dim)
            for rank in range(put_tp_size):
                rc = self.store.put_tensor_chunk_with_tp(
                    keys[i], chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
                )
                self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for tensor {i}, rank {rank}")

        # Batch get for each rank
        for rank in range(get_tp_size):
            retrieved_chunks = self.store.batch_get_tensor_with_tp(
                keys, tp_rank=rank, tp_size=get_tp_size
            )
            self.assertEqual(len(retrieved_chunks), num_tensors, f"Batch get returned wrong number of chunks for rank {rank}")

            # Verify each chunk
            for i, tensor in enumerate(tensors):
                expected_chunk = tensor.chunk(put_tp_size, split_dim)[rank]
                self.assertTrue(
                    torch.equal(retrieved_chunks[i], expected_chunk),
                    f"Batch chunk mismatch for tensor {i}, rank {rank}"
                )

    def test_12_chunk_fast_path_verification(self):
        """Verify that fast path is used when put_tp_size == get_tp_size."""
        key = "chunk_test_fast_path_verify"
        tp_size = 4
        split_dim = 0

        # Create a tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)
        chunks = tensor.chunk(tp_size, split_dim)

        # Put each chunk
        for rank in range(tp_size):
            rc = self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_chunk_with_tp failed for rank {rank}")

        # Get with same tp_size (should use fast path)
        # Fast path should directly return the chunk without reconstruction
        for rank in range(tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            # Verify it's exactly the same chunk (fast path should return the stored chunk directly)
            self.assertTrue(
                torch.equal(retrieved, chunks[rank]),
                f"Fast path returned incorrect data for rank {rank}"
            )

    def test_13_chunk_with_put_tensor_with_tp(self):
        """Test that put_tensor_with_tp also stores metadata correctly."""
        key = "chunk_test_put_with_tp"
        put_tp_size = 4
        get_tp_size = 4
        split_dim = 0

        # Create a tensor
        tensor = torch.randn(16, 32, dtype=torch.float32)

        # Use put_tensor_with_tp (which internally calls put_tensor_chunk_with_tp)
        rc = self.store.put_tensor_with_tp(
            key, tensor, tp_size=put_tp_size, split_dim=split_dim
        )
        self.assertEqual(rc, 0, "put_tensor_with_tp failed")

        # Get with get_tensor_with_tp (should use fast path)
        expected_chunks = tensor.chunk(put_tp_size, split_dim)
        for rank in range(get_tp_size):
            retrieved = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(retrieved, f"Get returned None for rank {rank}")
            self.assertTrue(
                torch.equal(retrieved, expected_chunks[rank]),
                f"Data mismatch for rank {rank}"
            )

        # Verify reconstruction
        all_chunks = []
        for rank in range(get_tp_size):
            chunk = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=get_tp_size, split_dim=split_dim
            )
            all_chunks.append(chunk)
        reconstructed = torch.cat(all_chunks, dim=split_dim)
        self.assertTrue(
            torch.equal(reconstructed, tensor),
            "Reconstruction failed"
        )

    def test_14_chunk_deletion(self):
        """Test automatic metadata deletion logic."""
        key = "chunk_deletion_test"
        tp_size = 2
        split_dim = 0
        tensor = torch.randn(8, 8, dtype=torch.float32)
        chunks = tensor.chunk(tp_size, split_dim)

        # 1. Put chunks
        for rank in range(tp_size):
            self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=tp_size, split_dim=split_dim
            )

        # 2. Verify existence of all parts
        self.assertEqual(self.store.is_exist(f"{key}_tp_0"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_tp_1"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_tp_0_meta"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_tp_1_meta"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 1)

        # 3. Remove chunk 0
        # Wait for potential lease expiration (default 5s) to allow removal
        time.sleep(6)
        rc = self.store.remove(f"{key}_tp_0")
        self.assertEqual(rc, 0, f"Remove chunk 0 failed with rc {rc}")

        # Verify chunk 0 and its meta are gone
        self.assertEqual(self.store.is_exist(f"{key}_tp_0"), 0)
        self.assertEqual(self.store.is_exist(f"{key}_tp_0_meta"), 0)

        # Verify chunk 1 and global meta still exist (global meta stays if any chunk remains)
        self.assertEqual(self.store.is_exist(f"{key}_tp_1"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_tp_1_meta"), 1)
        self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 1)

        # 4. Remove chunk 1
        time.sleep(6)
        rc = self.store.remove(f"{key}_tp_1")
        self.assertEqual(rc, 0, f"Remove chunk 1 failed with rc {rc}")

        # Verify chunk 1 and its meta are gone
        self.assertEqual(self.store.is_exist(f"{key}_tp_1"), 0)
        self.assertEqual(self.store.is_exist(f"{key}_tp_1_meta"), 0)

        # Verify global meta is gone (since no chunks remain)
        self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 0)

    def test_15_batch_get_tp_slow_path(self):
        """Test batch_get_tensor_with_tp with reconstruction (slow path)."""
        put_tp_size = 4
        get_tp_size = 2
        split_dim = 1

        # Create multiple tensors
        tensors = []
        keys = []
        for i in range(3):
            t = torch.randn(8, 8, dtype=torch.float32)
            tensors.append(t)
            keys.append(f"batch_get_tp_slow_test_{i}")

            chunks = t.chunk(put_tp_size, split_dim)
            for rank in range(put_tp_size):
                 self.store.put_tensor_chunk_with_tp(
                    keys[-1], chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
                )

        # Batch get with different tp size
        # Expected chunks for get_tp_size=2
        expected_chunks_list = [t.chunk(get_tp_size, split_dim) for t in tensors]

        # Get rank 0
        retrieved_chunks_0 = self.store.batch_get_tensor_with_tp(keys, tp_rank=0, tp_size=get_tp_size, split_dim=split_dim)
        self.assertEqual(len(retrieved_chunks_0), 3)
        for i in range(3):
            self.assertTrue(torch.allclose(retrieved_chunks_0[i], expected_chunks_list[i][0]), f"Tensor {i} rank 0 mismatch")

        # Get rank 1
        retrieved_chunks_1 = self.store.batch_get_tensor_with_tp(keys, tp_rank=1, tp_size=get_tp_size, split_dim=split_dim)
        self.assertEqual(len(retrieved_chunks_1), 3)
        for i in range(3):
            self.assertTrue(torch.allclose(retrieved_chunks_1[i], expected_chunks_list[i][1]), f"Tensor {i} rank 1 mismatch")

    def test_16_get_into_tp_slow_path(self):
        """Test get_tensor_with_tp_into with reconstruction."""
        key = "get_into_tp_slow_test"
        put_tp_size = 4
        get_tp_size = 2
        split_dim = 0

        tensor = torch.randn(8, 8, dtype=torch.float32)
        chunks = tensor.chunk(put_tp_size, split_dim)
        for rank in range(put_tp_size):
             self.store.put_tensor_chunk_with_tp(
                key, chunks[rank], put_tp_rank=rank, put_tp_size=put_tp_size, split_dim=split_dim
            )

        expected_chunks = tensor.chunk(get_tp_size, split_dim)

        # Test get_into for rank 0
        # Calculate size: Meta + Data
        # Shape of expected chunk [4, 8] float32
        chunk_elem = 4 * 8
        chunk_size = chunk_elem * 4
        # Python buffer (bytearray)
        # Note: store expects buffer to hold Meta + Data.
        # But we verify matching against expected tensor.
        # Wait, get_tensor_with_tp_into returns a tensor wrapping the buffer.

        # We need a buffer large enough.
        # How to get sizeof(TensorMetadata) in python?
        # It's not exposed. But we know it's roughly 48 bytes. Let's start with large buffer.
        meta_size = 128 # Safe upper bound
        buffer_size = meta_size + chunk_size

        # Using a pre-allocated tensor's storage as buffer is tricky because of the metadata header.
        # So we use a bytearray.
        buf0 = bytearray(buffer_size)
        ptr0 = ((ctypes.c_char * len(buf0)).from_buffer(buf0))
        addr0 = ctypes.addressof(ptr0)

        ret0 = self.store.get_tensor_with_tp_into(key, addr0, len(buf0), tp_rank=0, tp_size=get_tp_size, split_dim=split_dim)

        self.assertIsNotNone(ret0)
        self.assertTrue(torch.is_tensor(ret0))
        self.assertTrue(torch.allclose(ret0, expected_chunks[0]))

        # Verify batch_get_into
        buf1 = bytearray(buffer_size)
        addr1 = ctypes.addressof(((ctypes.c_char * len(buf1)).from_buffer(buf1)))

        ret_batch = self.store.batch_get_tensor_with_tp_into([key], [addr1], [len(buf1)], tp_rank=1, tp_size=get_tp_size, split_dim=split_dim)
        self.assertEqual(len(ret_batch), 1)
        self.assertTrue(torch.allclose(ret_batch[0], expected_chunks[1]))

    def test_17_batch_put_tensor_chunk_with_tp(self):
        """Test batch_put_tensor_chunk_with_tp."""
        keys = ["batch_chunk_1", "batch_chunk_2"]
        tensors = [torch.randn(8, 16, dtype=torch.float32), torch.randn(8, 16, dtype=torch.float32)]

        tp_size = 4
        split_dim = 0

        # We manually chunk them first
        chunks_1 = tensors[0].chunk(tp_size, split_dim)
        chunks_2 = tensors[1].chunk(tp_size, split_dim)

        for rank in range(tp_size):
            rank_chunks = [chunks_1[rank], chunks_2[rank]]

            res = self.store.batch_put_tensor_chunk_with_tp(
                keys, rank_chunks, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )

            for rc in res:
                self.assertEqual(rc, 0)

        # Verification
        for i, key in enumerate(keys):
            # Reconstruct by fetching chunks
            retrieved_chunks = []
            for rank in range(tp_size):
                chunk = self.store.get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
                self.assertIsNotNone(chunk)
                retrieved_chunks.append(chunk)

            reconstructed = torch.cat(retrieved_chunks, dim=split_dim)
            self.assertTrue(torch.equal(reconstructed, tensors[i]))

    def test_18_batch_put_tensor_chunk_with_full_shape(self):
        """Test batch_put_tensor_chunk_with_tp with full_shapes (uneven split)."""
        keys = ["batch_chunk_fullshape_1"]
        tensor = torch.randn(10, 16, dtype=torch.float32) # Uneven split 10/3 -> 4,3,3
        tp_size = 3
        split_dim = 0
        chunks = list(tensor.split([4, 3, 3], dim=split_dim))

        full_shapes = [[10, 16]]

        for rank in range(tp_size):
            rank_chunks = [chunks[rank]]
            res = self.store.batch_put_tensor_chunk_with_tp(
                keys, rank_chunks, tp_rank=rank, tp_size=tp_size, split_dim=split_dim,
                full_shapes=full_shapes
            )
            self.assertEqual(res[0], 0)

        # Verify
        retrieved_chunks = []
        for rank in range(tp_size):
             chunk = self.store.get_tensor_with_tp(keys[0], tp_rank=rank, tp_size=tp_size, split_dim=split_dim)
             self.assertIsNotNone(chunk)
             retrieved_chunks.append(chunk)

        reconstructed = torch.cat(retrieved_chunks, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, tensor))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake Tensor Chunk API Tests")
    parser.add_argument("--mode", type=str, default="all", choices=["all", "func"],
                        help="Run mode")

    args, unknown = parser.parse_known_args()

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    if args.mode in ["all", "func"]:
        print(">> Loading Tensor Chunk API Tests...")
        suite.addTests(loader.loadTestsFromTestCase(TestTensorChunkAPI))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    sys.exit(not result.wasSuccessful())

