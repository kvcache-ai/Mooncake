import ctypes
import os
import sys
import time
import argparse
import unittest
import torch
import numpy as np
import asyncio

from mooncake.mooncake_config import MooncakeConfig
from dataclasses import dataclass


try:
    from mooncake.async_store import MooncakeDistributedStoreAsync
except ImportError:
    print("Warning: Could not import MooncakeDistributedStoreAsync from async_store.py")
    MooncakeDistributedStoreAsync = None

# ==========================================
#  Global Variables & Configuration
# ==========================================

# Global Store instance
GLOBAL_STORE = None
GLOBAL_CONFIG = None

DEFAULT_MOONCAKE_CONFIG_PATH_ENV = "MOONCAKE_CONFIG_PATH"
DEFAULT_GLOBAL_SEGMENT_SIZE = 16 * 1024 * 1024 * 1024  # 16 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 8 * 1024 * 1024 * 1024    # 8 GB
DEFAULT_MASTER_METRICS_PORT = 9003
DEFAULT_CHECK_SERVER = False

def verify_tensor_equality(original, received, rtol=0, atol=0, verbose=True):
    """
    Utility to compare two tensors (CPU/GPU/Numpy).
    Identical to the synchronous version.
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
        # Simple diff reporting
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
        return False

def parse_global_segment_size(value) -> int:
    if isinstance(value, int): return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            return int(s[:-2].strip()) * 1024**3
        return int(s)
    return int(value)


def generate_tensors(num_tensors, size_mb):
    size_bytes = int(size_mb * 1024 * 1024)
    element_size = 4  # float32
    num_elements = size_bytes // element_size
    dim = int(np.sqrt(num_elements))
    dim = (dim // 8) * 8

    tensors = [torch.randn(dim, dim, dtype=torch.float32).contiguous() for _ in range(num_tensors)]
    keys = [f"test_tensor_{i}_{int(time.time()*1000)}" for i in range(num_tensors)]
    return keys, tensors

# ==========================================
#  Module Level Setup/Teardown
# ==========================================

def setUpModule():
    """
    Initialize the Async wrapper globally.
    Note: We invoke setup() synchronously via asyncio.run() to ensure C++ client is ready 
    before any tests run. This mimics the sync script's behavior.
    """
    global GLOBAL_STORE, GLOBAL_CONFIG

    if MooncakeDistributedStoreAsync is None:
        raise ImportError("Could not find MooncakeDistributedStoreAsync class.")

    try:
        GLOBAL_CONFIG = MooncakeConfig.load_from_env()
        # Increase max_workers to simulate high async concurrency
        GLOBAL_STORE = MooncakeDistributedStoreAsync()

        print(f"[{os.getpid()}] (Async) Connecting to Mooncake Master at {GLOBAL_CONFIG.master_server_address}...")

        def _do_setup():
            return GLOBAL_STORE.setup(
                GLOBAL_CONFIG.local_hostname,
                GLOBAL_CONFIG.metadata_server,
                GLOBAL_CONFIG.global_segment_size,
                GLOBAL_CONFIG.local_buffer_size,
                GLOBAL_CONFIG.protocol,
                GLOBAL_CONFIG.device_name,
                GLOBAL_CONFIG.master_server_address,
            )

        # Run setup in a temporary loop just for initialization
        rc = _do_setup()

        if rc != 0:
            raise RuntimeError(f"Failed to setup mooncake store, error code: {rc}")

        print("✅ Global Async Store connection established.")

    except Exception as e:
        print(f"❌ Failed to establish global store connection: {e}")
        sys.exit(1)

def tearDownModule():
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        try:
            GLOBAL_STORE.close()
        except Exception as e:
            print(f"Error during close: {e}")
        finally:
            GLOBAL_STORE = None

# ==========================================
#  Base Test Class (Async)
# ==========================================

class MooncakeAsyncTestBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Executed before each test method."""
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")

        self.store = GLOBAL_STORE
        self.config = GLOBAL_CONFIG

        # Asynchronously clear the store
        # Note: Depending on implementation, you might want to call
        # await self.store.async_remove_all()
        # For now, we assume remove_all wraps C++ synchronous logic nicely in a thread.
        await self.store.async_remove_all()

# ==========================================
#  Basic Bytes I/O & Auxiliary Tests
# ==========================================

class TestMooncakeBasicFunctional(MooncakeAsyncTestBase):
    async def test_01_bytes_put_get(self):
        """Test basic single key-value (bytes) storage."""
        key = "test_bytes_single"
        value = b"Hello Mooncake Async World!"

        # 1. Put Bytes
        # Note: put wrapper in Python usually defaults config if None
        rc = await self.store.async_put(key, value)
        self.assertEqual(rc, 0, f"put failed with rc={rc}")

        # 2. Get Bytes
        retrieved = await self.store.async_get(key)
        self.assertIsInstance(retrieved, bytes)
        self.assertEqual(retrieved, value, "Retrieved bytes mismatch")

    async def test_02_bytes_batch_io(self):
        """Test batch put and get for bytes."""
        count = 10
        keys = [f"test_bytes_batch_{i}" for i in range(count)]
        # Generate different length values
        values = [f"value_{i}_{'x'*i}".encode('utf-8') for i in range(count)]

        # 1. Batch Put
        rcs = await self.store.async_put_batch(keys, values)
        self.assertEqual(rcs, 0, f"batch_put failed with rc={rcs}")

        # 2. Batch Get
        retrieved_values = await self.store.async_get_batch(keys)
        self.assertEqual(len(retrieved_values), count)

        for i in range(count):
            self.assertEqual(retrieved_values[i], values[i], f"Mismatch at index {i}")

    async def test_03_existence_check(self):
        """Test is_exist and batch_is_exist."""
        key_exist = "test_exist_yes"
        key_missing = "test_exist_no_such_key"

        await self.store.async_put(key_exist, b"exist")

        # Single check
        self.assertTrue(await self.store.async_is_exist(key_exist))
        self.assertFalse(await self.store.async_is_exist(key_missing))

        # Batch check
        keys = [key_exist, key_missing, key_exist]
        # C++ batchIsExist returns list of ints: 1=exist, 0=not exist, -1=error
        results = await self.store.async_batch_is_exist(keys)

        self.assertEqual(len(results), 3)
        self.assertEqual(results[0], 1)
        self.assertEqual(results[1], 0)
        self.assertEqual(results[2], 1)

    async def test_04_remove(self):
        """Test remove and remove_all."""
        key = "test_remove_key"
        await self.store.async_put(key, b"data")

        self.assertTrue(await self.store.async_is_exist(key))

        await asyncio.sleep(6)
        # Remove single
        await self.store.async_remove(key)
        self.assertFalse(await self.store.async_is_exist(key))

        # Test Remove All (implicitly tested in setup, but explicit here)
        keys = ["rm_all_1", "rm_all_2"]
        await self.store.async_put_batch(keys, [b"1", b"2"])
        self.assertTrue(await self.store.async_is_exist(keys[0]))
        await asyncio.sleep(6)
        await self.store.async_remove_all()
        self.assertFalse(await self.store.async_is_exist(keys[0]))
        self.assertFalse(await self.store.async_is_exist(keys[1]))

    async def test_05_remove_by_regex(self):
        """Test remove_by_regex."""
        # Setup keys with specific patterns
        prefix_keys = [f"regex_target_{i}" for i in range(5)]
        other_keys = [f"regex_safe_{i}" for i in range(5)]

        all_keys = prefix_keys + other_keys
        all_vals = [b"x"] * len(all_keys)

        await self.store.async_put_batch(all_keys, all_vals)

        # Verify all exist
        results = await self.store.async_batch_is_exist(all_keys)
        self.assertTrue(all(r == 1 for r in results))

        # Remove by regex pattern "regex_target_.*"
        pattern = "regex_target_.*"
        await asyncio.sleep(6)
        await self.store.async_remove_by_regex(pattern)

        # Verify targets are gone
        target_res = await self.store.async_batch_is_exist(prefix_keys)
        self.assertTrue(all(r == 0 for r in target_res), "Targets should be removed")

        # Verify others stay
        safe_res = await self.store.async_batch_is_exist(other_keys)
        self.assertTrue(all(r == 1 for r in safe_res), "Safe keys should remain")

    async def test_07_large_bytes_io(self):
        """Test larger bytes payload (non-tensor path)."""
        key = "test_large_bytes"
        size = 10 * 1024 * 1024 # 10MB
        # Create random bytes
        data = os.urandom(size)

        await self.store.async_put(key, data)

        # Retrieve
        retrieved = await self.store.async_get(key)
        self.assertEqual(len(retrieved), size)
        self.assertEqual(retrieved, data)

# ==========================================
#  Tensor Functional Tests
# ==========================================

class TestMooncakeTensorFunctional(MooncakeAsyncTestBase):
    async def test_01_basic_put_get(self):
        """Verify basic put and get functionality."""
        key = "func_test_single"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)

        # Await Put
        rc = await self.store.async_put_tensor(key, tensor)
        self.assertEqual(rc, 0, f"put_tensor failed with rc={rc}")

        exists = await self.store.async_is_exist(key)
        self.assertTrue(exists, "Key not found after put")

        # Await Get
        retrieved = await self.store.async_get_tensor(key)
        self.assertIsNotNone(retrieved, "Get returned None")
        self.assertTrue(torch.equal(tensor, retrieved), "Data mismatch")

    async def test_02_tp_single_tensor(self):
        tp_size = 4
        split_dim = 1
        key = "func_test_tp_single"

        _, tensors = generate_tensors(1, 16)
        target_tensor = tensors[0]

        # 1. Put with TP
        rc = await self.store.async_put_tensor_with_tp(key, target_tensor, tp_size=tp_size, split_dim=split_dim)
        self.assertEqual(rc, 0, "put_tensor_with_tp failed")

        # 2. Verify existence of shards
        for rank in range(tp_size):
            shard_key = f"{key}_tp_{rank}"
            exists = await self.store.async_is_exist(shard_key)
            self.assertTrue(exists, f"Shard key {shard_key} is missing")

        # 3. Get shards and Reconstruct (Concurrently!)
        expected_chunks = target_tensor.chunk(tp_size, split_dim)

        # Launch all gets in parallel
        tasks = []
        for rank in range(tp_size):
            tasks.append(self.store.async_get_tensor_with_tp(key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim))

        slices = await asyncio.gather(*tasks)

        for rank, t_slice in enumerate(slices):
            self.assertIsNotNone(t_slice, f"Slice for rank {rank} is None")
            self.assertTrue(torch.equal(t_slice, expected_chunks[rank]), f"Data mismatch for rank {rank}")

        reconstructed = torch.cat(slices, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, target_tensor), "Reconstruction mismatch")

    async def test_03_tp_batch(self):
        tp_size = 2
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)

        # 1. Batch Put with TP
        results = await self.store.async_batch_put_tensor_with_tp(keys, tensors, tp_size=tp_size, split_dim=split_dim)
        self.assertTrue(all(r == 0 for r in results), f"Batch put failed. Results: {results}")

        # 2. Batch Get per Rank (Concurrently fetch both ranks)
        tasks = []
        for rank in range(tp_size):
            tasks.append(self.store.async_batch_get_tensor_with_tp(keys, tp_rank=rank, tp_size=tp_size))

        all_shards = await asyncio.gather(*tasks) # List of lists

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
            self.assertTrue(torch.equal(recon, original), f"Tensor {i} final mismatch")

    async def test_04_tp_consistency(self):
        input_tensor = torch.arange(12).view(3, 4)
        tp_size = 2

        await self.store.async_batch_put_tensor_with_tp(['key'], [input_tensor], tp_size=tp_size, split_dim=1)
        chunked_tensors = input_tensor.chunk(chunks=2, dim=1)

        # Parallel fetch
        t0, t1 = await asyncio.gather(
            self.store.async_batch_get_tensor_with_tp(['key'], tp_rank=0, tp_size=tp_size),
            self.store.async_batch_get_tensor_with_tp(['key'], tp_rank=1, tp_size=tp_size)
        )
        tmp_tensor_0 = t0[0]
        tmp_tensor_1 = t1[0]

        self.assertTrue(tmp_tensor_0.sum() == chunked_tensors[0].sum())
        self.assertTrue(tmp_tensor_1.sum() == chunked_tensors[1].sum())

        buffer_spacing = 1 * 1024 * 1024
        buffer_2 = (ctypes.c_ubyte * buffer_spacing)()
        buffer_3 = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr_2 = ctypes.addressof(buffer_2)
        buffer_ptr_3 = ctypes.addressof(buffer_3)

        # Important: register_buffer needs to be supported in Async wrapper
        # Assuming Async wrapper has register_buffer that delegates to C++
        res = await self.store.async_register_buffer(buffer_ptr_2, buffer_spacing)
        self.assertEqual(res, 0)
        res = await self.store.async_register_buffer(buffer_ptr_3, buffer_spacing)
        self.assertEqual(res, 0)

        # Parallel get_into
        t2_task = self.store.async_batch_get_tensor_with_tp_into(
            ['key'], [buffer_ptr_2], [buffer_spacing], tp_rank=0, tp_size=tp_size)
        t3_task = self.store.async_batch_get_tensor_with_tp_into(
            ['key'], [buffer_ptr_3], [buffer_spacing], tp_rank=1, tp_size=tp_size)

        t2_res, t3_res = await asyncio.gather(t2_task, t3_task)
        tmp_tensor_2 = t2_res[0]
        tmp_tensor_3 = t3_res[0]

        self.assertTrue(tmp_tensor_2.sum() == chunked_tensors[0].sum())
        self.assertTrue(tmp_tensor_3.sum() == chunked_tensors[1].sum())

        await self.store.async_unregister_buffer(buffer_ptr_2)
        await self.store.async_unregister_buffer(buffer_ptr_3)

    async def test_05_put_get_into(self):
        key = "get_into_test"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)
        total_buffer_size = 64 * 1024 * 1024

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)

        await self.store.async_register_buffer(large_buffer_ptr, total_buffer_size)

        rc = await self.store.async_put_tensor(key, tensor)
        self.assertEqual(rc, 0)

        retrieved = await self.store.async_get_tensor_into(key, large_buffer_ptr, total_buffer_size)
        self.assertIsNotNone(retrieved)
        self.assertTrue(torch.equal(tensor, retrieved))

        await self.store.async_unregister_buffer(large_buffer_ptr)

    async def test_06_batch_put_get_into(self):
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)
        buffer_spacing = 64 * 1024 * 1024
        batch_size = len(keys)
        total_buffer_size = buffer_spacing * batch_size

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)

        buffer_ptrs = []
        buffer_sizes = []
        for i in range(batch_size):
            offset = i * buffer_spacing
            buffer_ptrs.append(large_buffer_ptr + offset)
            buffer_sizes.append(buffer_spacing)

        await self.store.async_register_buffer(large_buffer_ptr, total_buffer_size)

        results = await self.store.async_batch_put_tensor(keys, tensors)
        self.assertTrue(all(r == 0 for r in results))

        res = await self.store.async_batch_get_tensor_into(keys, buffer_ptrs, buffer_sizes)

        self.assertEqual(len(res), len(tensors))
        for j in range(batch_size):
            self.assertTrue(verify_tensor_equality(tensors[j], res[j]))

        await self.store.async_unregister_buffer(large_buffer_ptr)

    async def test_07_put_get_into_with_tp(self):
        tp_size = 4
        split_dim = 0
        key = "get_into_with_tp_test"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)

        result = await self.store.async_put_tensor_with_tp(key, tensor, tp_size=tp_size, split_dim=split_dim)
        self.assertEqual(result, 0)

        all_shards = []
        registered_buffers = []

        # We can launch these concurrently too!
        async def _fetch_rank(rank):
            buf_size = 64 * 1024 * 1024
            l_buf = (ctypes.c_ubyte * buf_size)()
            l_ptr = ctypes.addressof(l_buf)

            await self.store.async_register_buffer(l_ptr, buf_size)

            shard = await self.store.async_get_tensor_with_tp_into(
                key, l_ptr, buf_size, tp_rank=rank, tp_size=tp_size
            )
            return rank, shard, l_buf, l_ptr

        tasks = [_fetch_rank(r) for r in range(tp_size)]
        results = await asyncio.gather(*tasks)

        # Sort by rank to ensure order
        results.sort(key=lambda x: x[0])

        for _, shard, buf, ptr in results:
            all_shards.append(shard)
            registered_buffers.append((buf, ptr)) # Keep buf alive

        # Validate
        original = tensor
        expected_chunks = original.chunk(tp_size, split_dim)
        reconstruction_parts = []
        for rank in range(tp_size):
            self.assertTrue(torch.equal(all_shards[rank], expected_chunks[rank]))
            reconstruction_parts.append(all_shards[rank])

        recon = torch.cat(reconstruction_parts, dim=split_dim)
        self.assertTrue(torch.equal(recon, original))

        # Cleanup
        for _, ptr in registered_buffers:
            await self.store.async_unregister_buffer(ptr)

    async def test_08_batch_put_get_into_with_tp(self):
        # Similar logic to test_07 but for batch
        tp_size = 4
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)

        results = await self.store.async_batch_put_tensor_with_tp(keys, tensors, tp_size=tp_size, split_dim=split_dim)
        self.assertTrue(all(r == 0 for r in results))

        async def _fetch_batch_rank(rank):
            batch_size = len(keys)
            spacing = 64 * 1024 * 1024
            total_size = spacing * batch_size

            l_buf = (ctypes.c_ubyte * total_size)()
            l_ptr = ctypes.addressof(l_buf)

            ptrs = [l_ptr + i*spacing for i in range(batch_size)]
            sizes = [spacing] * batch_size

            await self.store.async_register_buffer(l_ptr, total_size)

            shards = await self.store.async_batch_get_tensor_with_tp_into(
                keys, ptrs, sizes, tp_rank=rank, tp_size=tp_size
            )
            return rank, shards, l_buf, l_ptr

        tasks = [_fetch_batch_rank(r) for r in range(tp_size)]
        results = await asyncio.gather(*tasks)
        results.sort(key=lambda x: x[0])

        all_shards_by_rank = [r[1] for r in results]
        registered_buffers = [(r[2], r[3]) for r in results]

        # Verify
        for i in range(num_tensors):
            original = tensors[i]
            expected = original.chunk(tp_size, split_dim)
            parts = []
            for rank in range(tp_size):
                shard = all_shards_by_rank[rank][i]
                self.assertTrue(torch.equal(shard, expected[rank]))
                parts.append(shard)
            recon = torch.cat(parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, original))

        for _, ptr in registered_buffers:
            await self.store.async_unregister_buffer(ptr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Async Mooncake Store Tests")
    parser.add_argument("--mode", type=str, default="all", choices=["all", "Tensorfunc", "Basicfunc"])
    parser.add_argument("--threads", type=int, default=8, help="Number of concurrent tasks")
    parser.add_argument("--count", type=int, default=800, help="Total items")
    parser.add_argument("--size_mb", type=float, default=0.5, help="Tensor MB")

    args, unknown = parser.parse_known_args()

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # Note: IsolatedAsyncioTestCase handles the async loop internally for each test
    if args.mode in ["all", "Tensorfunc"]:
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeTensorFunctional))
    if args.mode in ["all", "Basicfunc"]:
        suite.addTests(loader.loadTestsFromTestCase(TestMooncakeBasicFunctional))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(not result.wasSuccessful())