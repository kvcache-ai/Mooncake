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
import mooncake.store as mooncake_store
from mooncake.store import MooncakeDistributedStore
from mooncake.store import ReplicateConfig

from mooncake.mooncake_config import MooncakeConfig

import concurrent.futures


@dataclass
class WriterPartition:
    rank: int
    size: int
    split_dim: int


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

# Must match current C++ TensorMetadata layout.
# TensorObjectHeader (40) + TensorLayoutMetadata (264) = 304 bytes.
TENSOR_METADATA_SIZE = 304


def serialized_tensor_size(tensor):
    """Size in bytes of [TensorObjectHeader+layout metadata][tensor data]."""
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


def require_unified_parallelism_api(test_case):
    missing = [
        name for name in ["ParallelAxis", "TensorParallelism", "ReadTarget"]
        if not hasattr(mooncake_store, name)
    ]
    if missing:
        test_case.skipTest(
            "Unified parallelism Python bindings are not available in the installed extension: "
            + ", ".join(missing)
        )


def make_parallel_axis(kind, rank, size, split_dim=None):
    axis = mooncake_store.ParallelAxis()
    axis.kind = kind
    axis.rank = rank
    axis.size = size
    axis.split_dim = split_dim
    return axis


def make_tensor_parallelism(axes):
    parallelism = mooncake_store.TensorParallelism()
    parallelism.axes = axes
    return parallelism


def make_read_target(mode, parallelism=None):
    target = mooncake_store.ReadTarget()
    target.mode = mode
    target.parallelism = parallelism
    return target


def make_writer_partition(rank, size, split_dim):
    return WriterPartition(rank=rank, size=size, split_dim=split_dim)


def build_tp_parallelism(tp_size, split_dim, rank=0):
    return make_tensor_parallelism([
        make_parallel_axis("tp", rank=rank, size=tp_size, split_dim=split_dim)
    ])


def build_dp_tp_parallelism(dp_rank, dp_size, tp_rank, tp_size, split_dim):
    return make_tensor_parallelism([
        make_parallel_axis("dp", rank=dp_rank, size=dp_size),
        make_parallel_axis("tp", rank=tp_rank, size=tp_size, split_dim=split_dim),
    ])


def build_pp_tp_parallelism(pp_rank, pp_size, stage_id, tp_rank, tp_size, split_dim):
    pp_axis = make_parallel_axis("pp", rank=pp_rank, size=pp_size)
    pp_axis.stage_id = stage_id
    return make_tensor_parallelism([
        pp_axis,
        make_parallel_axis("tp", rank=tp_rank, size=tp_size, split_dim=split_dim),
    ])


def build_ep_parallelism(ep_rank, ep_size, expert_id):
    ep_axis = make_parallel_axis("ep", rank=ep_rank, size=ep_size)
    ep_axis.expert_id = expert_id
    return make_tensor_parallelism([ep_axis])


def build_ep_tp_parallelism(ep_rank, ep_size, expert_id, tp_rank, tp_size, split_dim):
    ep_axis = make_parallel_axis("ep", rank=ep_rank, size=ep_size)
    ep_axis.expert_id = expert_id
    return make_tensor_parallelism([
        ep_axis,
        make_parallel_axis("tp", rank=tp_rank, size=tp_size, split_dim=split_dim),
    ])


def build_parallelism_mode(mode, tp_rank, tp_size, split_dim):
    if mode == "tp":
        return build_tp_parallelism(tp_size, split_dim, rank=tp_rank)
    if mode == "dp_tp":
        return build_dp_tp_parallelism(0, 2, tp_rank, tp_size, split_dim)
    if mode == "pp_tp":
        return build_pp_tp_parallelism(0, 2, 7, tp_rank, tp_size, split_dim)
    if mode == "ep_tp":
        return build_ep_tp_parallelism(0, 2, 11, tp_rank, tp_size, split_dim)
    raise ValueError(f"unsupported parallelism mode: {mode}")


def put_full_tensor_with_parallelism_mode(store, key, tensor, split_dim, put_tp_size, mode):
    if mode == "tp":
        shards = tensor.chunk(put_tp_size, split_dim)
        for rank, shard in enumerate(shards):
            rc = store.put_tensor_with_parallelism(
                key,
                shard.contiguous(),
                build_parallelism_mode(mode, rank, put_tp_size, split_dim),
            )
            if rc != 0:
                return rc
        return 0

    for rank in range(put_tp_size):
        rc = store.put_tensor_with_parallelism(
            key,
            tensor,
            build_parallelism_mode(mode, rank, put_tp_size, split_dim),
        )
        if rc != 0:
            return rc
    return 0


def clone_parallelism(parallelism):
    cloned_axes = []
    for axis in parallelism.axes:
        cloned_axis = make_parallel_axis(
            axis.kind,
            rank=axis.rank,
            size=axis.size,
            split_dim=axis.split_dim,
        )
        if getattr(axis, "expert_id", None) is not None:
            cloned_axis.expert_id = axis.expert_id
        if getattr(axis, "stage_id", None) is not None:
            cloned_axis.stage_id = axis.stage_id
        cloned_axes.append(cloned_axis)
    return make_tensor_parallelism(cloned_axes)


def put_full_tensor_with_parallelism_template(store, key, tensor, parallelism):
    tp_axis = next(axis for axis in parallelism.axes if axis.kind == "tp")
    for rank in range(tp_axis.size):
        shard_parallelism = clone_parallelism(parallelism)
        for axis in shard_parallelism.axes:
            if axis.kind == "tp":
                axis.rank = rank
                axis.size = tp_axis.size
                axis.split_dim = tp_axis.split_dim
                break
        rc = store.put_tensor_with_parallelism(
            key,
            tensor,
            shard_parallelism,
        )
        if rc != 0:
            return rc
    return 0


def expected_target_shard(tensor, split_dim, target_tp_size, target_rank):
    return chunk_tensor_for_rank(tensor, target_tp_size, split_dim, target_rank).contiguous()


def reorder_parallelism_axes(parallelism, ordered_kinds):
    axis_by_kind = {axis.kind: axis for axis in parallelism.axes}
    return make_tensor_parallelism([axis_by_kind[kind] for kind in ordered_kinds])


def make_deterministic_tensor(shape, dtype=torch.float32):
    numel = 1
    for dim in shape:
        numel *= dim
    return torch.arange(numel, dtype=dtype).view(*shape).contiguous()


def chunk_tensor_for_rank(tensor, tp_size, split_dim, rank):
    return tensor.chunk(tp_size, split_dim)[rank]


def put_full_tensor_with_unified_tp(store, key, tensor, tp_size, split_dim):
    for rank, shard in enumerate(tensor.chunk(tp_size, split_dim)):
        rc = store.put_tensor_with_parallelism(
            key,
            shard.contiguous(),
            build_tp_parallelism(tp_size, split_dim, rank=rank),
        )
        if rc != 0:
            return rc
    return 0


def batch_put_full_tensors_with_unified_tp(store, keys, tensors, tp_size, split_dim):
    results = [0 for _ in keys]
    for rank in range(tp_size):
        shard_tensors = [chunk_tensor_for_rank(tensor, tp_size, split_dim, rank).contiguous() for tensor in tensors]
        rank_results = store.batch_put_tensor_with_parallelism(
            keys,
            shard_tensors,
            [build_tp_parallelism(tp_size, split_dim, rank=rank) for _ in keys],
        )
        results = rank_results
        if any(rc != 0 for rc in rank_results):
            return rank_results
    return results


def reconstruction_case_name(shape, split_dim, tp_size):
    shape_str = "x".join(str(dim) for dim in shape)
    return f"shape={shape_str}, split_dim={split_dim}, tp_size={tp_size}"


def is_uniform_tp_case(shape, split_dim, tp_size):
    return shape[split_dim] % tp_size == 0


def put_uniform_full_tensor_with_unified_tp(store, key, tensor, tp_size, split_dim):
    if not is_uniform_tp_case(tuple(tensor.shape), split_dim, tp_size):
        return -1
    return put_full_tensor_with_unified_tp(store, key, tensor, tp_size, split_dim)


def make_mb_scale_original_tensor(dtype=torch.float32):
    return make_deterministic_tensor((16, 16, 16), dtype=dtype)


def assert_tensor_matches_expected_shard(test_case, original_tensor, received_tensor,
                                         split_dim, target_tp_size, target_rank,
                                         context):
    expected_tensor = expected_target_shard(
        original_tensor, split_dim, target_tp_size, target_rank
    )
    test_case.assertIsNotNone(received_tensor, f"missing tensor for {context}")
    test_case.assertTrue(
        torch.equal(received_tensor, expected_tensor),
        f"mismatch for {context}",
    )


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
    def assert_full_reconstruction_case(self, key, tensor, split_dim, tp_size):
        rc = put_uniform_full_tensor_with_unified_tp(
            self.store, key, tensor, tp_size, split_dim
        )
        self.assertEqual(rc, 0, f"put_tensor_with_parallelism failed for {key}")

        shard_reads = []
        expected_shards = tensor.chunk(tp_size, split_dim)
        for rank, expected_shard in enumerate(expected_shards):
            wrapper_shard = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(wrapper_shard, f"wrapper shard missing for rank {rank}")
            self.assertTrue(
                torch.equal(wrapper_shard, expected_shard),
                f"wrapper shard mismatch for rank {rank}",
            )

            shard_parallelism = build_tp_parallelism(tp_size, split_dim, rank=rank)
            shard_target = make_read_target("shard", shard_parallelism)
            unified_shard = self.store.get_tensor_with_parallelism(key, shard_target)
            self.assertIsNotNone(unified_shard, f"unified shard missing for rank {rank}")
            self.assertTrue(
                torch.equal(unified_shard, expected_shard),
                f"unified shard mismatch for rank {rank}",
            )
            self.assertTrue(torch.equal(unified_shard, wrapper_shard))
            shard_reads.append(unified_shard)

        reconstructed_from_shards = torch.cat(shard_reads, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed_from_shards, tensor))

        full_parallelism = build_tp_parallelism(tp_size, split_dim, rank=min(tp_size - 1, 1))
        full_target = make_read_target("full", full_parallelism)
        full_tensor = self.store.get_tensor_with_parallelism(key, full_target)
        self.assertIsNotNone(full_tensor)
        self.assertTrue(torch.equal(full_tensor, tensor))

    def assert_full_into_reconstruction_case(self, key, tensor, split_dim, tp_size, buffer_spacing):
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            parallelism = build_tp_parallelism(tp_size, split_dim, rank=min(tp_size - 1, 1))
            rc = put_uniform_full_tensor_with_unified_tp(
                self.store, key, tensor, tp_size, split_dim
            )
            self.assertEqual(rc, 0)

            target = make_read_target("full", parallelism)
            full_tensor = self.store.get_tensor_with_parallelism(key, target)
            reconstructed = self.store.get_tensor_with_parallelism_into(
                key, buffer_ptr, buffer_spacing, target=target
            )
            self.assertIsNotNone(full_tensor)
            self.assertIsNotNone(reconstructed)
            self.assertTrue(torch.equal(full_tensor, tensor))
            self.assertTrue(torch.equal(reconstructed, tensor))
            self.assertTrue(torch.equal(reconstructed, full_tensor))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def assert_full_reconstruction_for_all_read_ranks(self, key, tensor, split_dim, tp_size):
        rc = put_uniform_full_tensor_with_unified_tp(
            self.store, key, tensor, tp_size, split_dim
        )
        self.assertEqual(rc, 0, f"put_tensor_with_parallelism failed for {key}")

        expected_rank_count = len(tensor.chunk(tp_size, split_dim))
        for rank in range(tp_size):
            full_target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=rank))
            full_tensor = self.store.get_tensor_with_parallelism(key, full_target)
            self.assertIsNotNone(full_tensor, f"full tensor missing for read rank {rank}")
            self.assertTrue(torch.equal(full_tensor, tensor), f"full tensor mismatch for read rank {rank}")

            if rank < expected_rank_count:
                shard_target = make_read_target("shard", build_tp_parallelism(tp_size, split_dim, rank=rank))
                shard_tensor = self.store.get_tensor_with_parallelism(key, shard_target)
                expected_shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, rank)
                self.assertIsNotNone(shard_tensor, f"shard missing for read rank {rank}")
                self.assertTrue(torch.equal(shard_tensor, expected_shard), f"shard mismatch for read rank {rank}")

    def assert_writer_shard_full_reconstruction_case(self, key, tensor, split_dim, shard_count):
        for rank in range(shard_count):
            rc = self.store.put_tensor_with_parallelism(
                key,
                tensor,
                writer_partition=make_writer_partition(rank, shard_count, split_dim),
            )
            self.assertEqual(rc, 0, f"writer shard put failed for rank {rank}")

        full_target = make_read_target("full")
        full_tensor = self.store.get_tensor_with_parallelism(key, full_target)
        self.assertIsNotNone(full_tensor)
        self.assertTrue(torch.equal(full_tensor, tensor))

        for rank in range(shard_count):
            shard_target = make_read_target(
                "shard", build_tp_parallelism(shard_count, split_dim, rank=rank)
            )
            shard_tensor = self.store.get_tensor_with_parallelism(key, shard_target)
            self.assertIsNotNone(shard_tensor)
            self.assertTrue(torch.equal(shard_tensor, tensor.chunk(shard_count, split_dim)[rank]))

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
        full_buffer = (ctypes.c_ubyte * buffer_spacing)()
        shard_buffer_0 = (ctypes.c_ubyte * buffer_spacing)()
        shard_buffer_1 = (ctypes.c_ubyte * buffer_spacing)()
        full_buffer_ptr = ctypes.addressof(full_buffer)
        shard_buffer_ptr_0 = ctypes.addressof(shard_buffer_0)
        shard_buffer_ptr_1 = ctypes.addressof(shard_buffer_1)

        for ptr in [full_buffer_ptr, shard_buffer_ptr_0, shard_buffer_ptr_1]:
            res = self.store.register_buffer(ptr, buffer_spacing)
            self.assertEqual(res, 0, f"Buffer registration failed for buffer at {ptr}")

        rc = self.store.put_tensor("key_seed_full", input_tensor)
        self.assertEqual(rc, 0, f"put_tensor(seed) failed with rc={rc}")
        retrieved = self.store.get_tensor_into("key_seed_full", full_buffer_ptr, buffer_spacing)
        self.assertIsNotNone(retrieved)
        full_size = serialized_tensor_size(retrieved)

        rc = self.store.put_tensor_with_tp_from(
            "key", full_buffer_ptr, full_size,
            tp_rank=1, tp_size=tp_size, split_dim=split_dim
        )
        self.assertEqual(rc, 0, f"put_tensor_with_tp_from failed with rc={rc}")

        chunked_tensors = input_tensor.chunk(chunks=2, dim=split_dim)
        tmp_tensor_0 = self.store.batch_get_tensor_with_tp(['key'], tp_rank=0, tp_size=tp_size)[0]
        tmp_tensor_1 = self.store.batch_get_tensor_with_tp(['key'], tp_rank=1, tp_size=tp_size)[0]
        self.assertTrue(torch.equal(tmp_tensor_0, chunked_tensors[0]))
        self.assertTrue(torch.equal(tmp_tensor_1, chunked_tensors[1]))

        tmp_tensor_2 = self.store.get_tensor_with_tp_into(
            'key', shard_buffer_ptr_0, buffer_spacing,
            tp_rank=0, tp_size=tp_size, split_dim=split_dim
        )
        tmp_tensor_3 = self.store.get_tensor_with_tp_into(
            'key', shard_buffer_ptr_1, buffer_spacing,
            tp_rank=1, tp_size=tp_size, split_dim=split_dim
        )
        self.assertTrue(torch.equal(tmp_tensor_2, chunked_tensors[0]))
        self.assertTrue(torch.equal(tmp_tensor_3, chunked_tensors[1]))

        for ptr in [full_buffer_ptr, shard_buffer_ptr_0, shard_buffer_ptr_1]:
            res = self.store.unregister_buffer(ptr)
            self.assertEqual(res, 0, f"Buffer unregistration failed for buffer at {ptr}")

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
        """Zero-copy TP put_from consumes one full tensor buffer and writes all shards."""
        tp_size = 4
        split_dim = 0
        key = "get_into_with_tp_test"
        seed_key = "get_into_with_tp_seed"
        tensor = torch.randn(1024, 1024, dtype=torch.float32)
        buffer_spacing = 64 * 1024 * 1024
        total_buffer_size = buffer_spacing * (1 + tp_size)

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        full_ptr = large_buffer_ptr
        get_ptrs = [large_buffer_ptr + (rank + 1) * buffer_spacing for rank in range(tp_size)]

        self.assertEqual(self.store.register_buffer(large_buffer_ptr, total_buffer_size), 0)
        try:
            rc = self.store.put_tensor(seed_key, tensor)
            self.assertEqual(rc, 0, f"Put(seed) failed. Result: {rc}")
            full_tensor = self.store.get_tensor_into(seed_key, full_ptr, buffer_spacing)
            self.assertIsNotNone(full_tensor)
            full_size = serialized_tensor_size(full_tensor)

            rc = self.store.put_tensor_with_tp_from(
                key, full_ptr, full_size,
                tp_rank=2, tp_size=tp_size, split_dim=split_dim
            )
            self.assertEqual(rc, 0, f"put_tensor_with_tp_from failed with rc={rc}")

            all_shards = []
            for rank in range(tp_size):
                shard = self.store.get_tensor_with_tp_into(
                    key, get_ptrs[rank], buffer_spacing,
                    tp_rank=rank, tp_size=tp_size
                )
                self.assertIsNotNone(shard)
                all_shards.append(shard)

            expected_chunks = tensor.chunk(tp_size, split_dim)
            reconstruction_parts = []
            for rank in range(tp_size):
                shard = all_shards[rank]
                self.assertTrue(
                    torch.equal(shard, expected_chunks[rank]),
                    f"Tensor Rank {rank} data mismatch"
                )
                reconstruction_parts.append(shard)
            recon = torch.cat(reconstruction_parts, dim=split_dim)
            self.assertTrue(torch.equal(recon, tensor), "Tensor final reconstruction mismatch")
        finally:
            self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Unregister buffer failed")

    def test_08_batch_put_get_into_with_tp(self):
        """Zero-copy batch TP put_from consumes full tensor buffers for each item."""
        tp_size = 4
        split_dim = 0
        num_tensors = 4
        keys, tensors = generate_tensors(num_tensors, 8)
        seed_keys = [f"{k}_seed" for k in keys]
        batch_size = len(keys)
        buffer_spacing = 64 * 1024 * 1024
        total_buffer_size = buffer_spacing * batch_size * (1 + tp_size)

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        full_ptrs = [large_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
        get_ptrs_by_rank = [
            [large_buffer_ptr + ((rank + 1) * batch_size + i) * buffer_spacing for i in range(batch_size)]
            for rank in range(tp_size)
        ]
        buffer_sizes = [buffer_spacing] * batch_size

        res = self.store.register_buffer(large_buffer_ptr, total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration failed")
        try:
            results = self.store.batch_put_tensor(seed_keys, tensors)
            self.assertTrue(all(r == 0 for r in results), f"Batch put(seed) failed. Results: {results}")
            full_tensors = self.store.batch_get_tensor_into(seed_keys, full_ptrs, buffer_sizes)
            self.assertEqual(len(full_tensors), num_tensors)
            put_sizes = [serialized_tensor_size(full_tensors[j]) for j in range(num_tensors)]

            results = self.store.batch_put_tensor_with_tp_from(
                keys, full_ptrs, put_sizes,
                tp_rank=3, tp_size=tp_size, split_dim=split_dim
            )
            self.assertTrue(all(r == 0 for r in results), f"batch_put_tensor_with_tp_from failed: {results}")

            all_shards = []
            for rank in range(tp_size):
                shards = self.store.batch_get_tensor_with_tp_into(
                    keys, get_ptrs_by_rank[rank], buffer_sizes,
                    tp_rank=rank, tp_size=tp_size
                )
                self.assertEqual(len(shards), num_tensors)
                all_shards.append(shards)

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
        finally:
            self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0, "Buffer unregistration failed")

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

    def test_12_unified_parallelism_as_stored_round_trip(self):
        require_unified_parallelism_api(self)
        key = "func_unified_as_stored"
        tensor = torch.randn(16, 16, dtype=torch.float32)

        rc = self.store.put_tensor_with_parallelism(key, tensor)
        self.assertEqual(rc, 0, f"put_tensor_with_parallelism failed with rc={rc}")

        retrieved_default = self.store.get_tensor_with_parallelism(key)
        self.assertIsNotNone(retrieved_default)
        self.assertTrue(torch.equal(retrieved_default, tensor))

        target = make_read_target("as_stored")
        retrieved_target = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(retrieved_target)
        self.assertTrue(torch.equal(retrieved_target, tensor))

    def test_13_unified_parallelism_tp_matches_wrapper(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_single"
        tensor = torch.arange(24, dtype=torch.float32).view(4, 6).contiguous()
        tp_size = 3
        split_dim = 1

        parallelism = make_tensor_parallelism([
            make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)
        ])

        shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, 0).contiguous()
        rc = self.store.put_tensor_with_parallelism(key, shard, parallelism)
        self.assertEqual(rc, 0, f"put_tensor_with_parallelism failed with rc={rc}")

        expected = shard
        rank = 0
        wrapper_shard = self.store.get_tensor_with_tp(
            key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
        )
        self.assertIsNotNone(wrapper_shard)
        self.assertTrue(torch.equal(wrapper_shard, expected))

        shard_parallelism = make_tensor_parallelism([
            make_parallel_axis("tp", rank=rank, size=tp_size, split_dim=split_dim)
        ])

        target = make_read_target("shard", shard_parallelism)
        unified_shard = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(unified_shard)
        self.assertTrue(torch.equal(unified_shard, expected))
        self.assertTrue(torch.equal(unified_shard, wrapper_shard))

        for rank in range(1, tp_size):
            wrapper_shard = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )
            self.assertIsNone(wrapper_shard)

            shard_parallelism = make_tensor_parallelism([
                make_parallel_axis("tp", rank=rank, size=tp_size, split_dim=split_dim)
            ])

            target = make_read_target("shard", shard_parallelism)
            unified_shard = self.store.get_tensor_with_parallelism(key, target)
            self.assertIsNone(unified_shard)

    def test_14_batch_unified_parallelism_tp_matches_wrapper(self):
        require_unified_parallelism_api(self)
        tp_size = 2
        split_dim = 0
        num_tensors = 2
        keys, tensors = generate_tensors(num_tensors, 1)

        results = batch_put_full_tensors_with_unified_tp(
            self.store, keys, tensors, tp_size, split_dim
        )
        self.assertTrue(all(r == 0 for r in results), f"Batch unified put failed. Results: {results}")

        parallelism = make_tensor_parallelism([
            make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)
        ])
        invalid_results = self.store.batch_put_tensor_with_parallelism(
            keys,
            tensors,
            [parallelism for _ in keys],
            writer_partitions=[make_writer_partition(0, 2, split_dim) for _ in keys],
        )
        self.assertTrue(all(r != 0 for r in invalid_results), f"expected invalid combined routing request: {invalid_results}")

        for rank in range(tp_size):
            wrapper_shards = self.store.batch_get_tensor_with_tp(
                keys, tp_rank=rank, tp_size=tp_size
            )

            shard_parallelism = make_tensor_parallelism([
                make_parallel_axis("tp", rank=rank, size=tp_size, split_dim=split_dim)
            ])

            target = make_read_target("shard", shard_parallelism)
            unified_shards = self.store.batch_get_tensor_with_parallelism(
                keys, [target for _ in keys]
            )

            self.assertEqual(len(wrapper_shards), num_tensors)
            self.assertEqual(len(unified_shards), num_tensors)
            for i in range(num_tensors):
                expected = tensors[i].chunk(tp_size, split_dim)[rank]
                self.assertTrue(torch.equal(wrapper_shards[i], expected))
                self.assertTrue(torch.equal(unified_shards[i], expected))
                self.assertTrue(torch.equal(unified_shards[i], wrapper_shards[i]))

    def test_15_unified_parallelism_multi_axis_shard_round_trip(self):
        require_unified_parallelism_api(self)
        key = "func_unified_dp_tp_shard"
        tensor = make_deterministic_tensor((4, 6))
        parallelism = build_dp_tp_parallelism(
            dp_rank=1, dp_size=2, tp_rank=0, tp_size=3, split_dim=1
        )

        rc = put_full_tensor_with_parallelism_template(
            self.store, key, tensor, parallelism
        )
        self.assertEqual(rc, 0)

        target = make_read_target("shard", parallelism)
        result = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(result)
        self.assertTrue(torch.equal(result, tensor.chunk(3, 1)[0]))

    def test_16_unified_parallelism_multi_axis_into_round_trip(self):
        require_unified_parallelism_api(self)
        key = "func_unified_pp_tp_into"
        tensor = make_deterministic_tensor((6, 8))
        parallelism = build_pp_tp_parallelism(
            pp_rank=0, pp_size=2, stage_id=7, tp_rank=1, tp_size=2, split_dim=0
        )
        buffer_spacing = 1 * 1024 * 1024
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            rc = put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            self.assertEqual(rc, 0)
            target = make_read_target("shard", parallelism)
            result = self.store.get_tensor_with_parallelism_into(
                key, buffer_ptr, buffer_spacing, target=target
            )
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, tensor.chunk(2, 0)[1]))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_17_unified_parallelism_rejects_invalid_axis_fields(self):
        require_unified_parallelism_api(self)
        key = "func_unified_invalid_axis"
        tensor = torch.randn(8, 8, dtype=torch.float32)

        invalid_dp = make_tensor_parallelism([
            make_parallel_axis("dp", rank=0, size=2, split_dim=0)
        ])
        self.assertNotEqual(
            self.store.put_tensor_with_parallelism(key, tensor, invalid_dp), 0
        )

        invalid_ep = make_tensor_parallelism([
            make_parallel_axis("ep", rank=0, size=2)
        ])
        self.assertNotEqual(
            self.store.put_tensor_with_parallelism(key, tensor, invalid_ep), 0
        )

    def test_18_batch_unified_parallelism_multi_axis_round_trip(self):
        require_unified_parallelism_api(self)
        cases = [
            (
                "func_unified_batch_dp_tp_0",
                make_deterministic_tensor((4, 6)),
                build_dp_tp_parallelism(0, 2, 1, 3, 1),
                lambda tensor: tensor.chunk(3, 1)[1],
            ),
            (
                "func_unified_batch_pp_tp_1",
                make_deterministic_tensor((6, 8)),
                build_pp_tp_parallelism(1, 2, 9, 0, 2, 0),
                lambda tensor: tensor.chunk(2, 0)[0],
            ),
        ]
        keys = [key for key, _, _, _ in cases]
        tensors = [tensor for _, tensor, _, _ in cases]
        parallelisms = [parallelism for _, _, parallelism, _ in cases]
        results = [
            put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            for key, tensor, parallelism in zip(keys, tensors, parallelisms)
        ]
        self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        targets = [make_read_target("shard", parallelism) for parallelism in parallelisms]
        shard_reads = self.store.batch_get_tensor_with_parallelism(keys, targets)
        self.assertEqual(len(shard_reads), len(cases))
        for i, (_, tensor, _, expected_fn) in enumerate(cases):
            self.assertIsNotNone(shard_reads[i], f"batch shard missing for tensor {i}")
            self.assertTrue(torch.equal(shard_reads[i], expected_fn(tensor)))

    def test_19_batch_unified_parallelism_multi_axis_into_round_trip(self):
        require_unified_parallelism_api(self)
        cases = [
            (
                "func_unified_batch_into_dp_tp_0",
                make_deterministic_tensor((4, 9)),
                build_dp_tp_parallelism(1, 2, 2, 3, 1),
                lambda tensor: tensor.chunk(3, 1)[2],
            ),
            (
                "func_unified_batch_into_pp_tp_1",
                make_deterministic_tensor((8, 6)),
                build_pp_tp_parallelism(0, 2, 5, 1, 2, 0),
                lambda tensor: tensor.chunk(2, 0)[1],
            ),
        ]
        keys = [key for key, _, _, _ in cases]
        tensors = [tensor for _, tensor, _, _ in cases]
        parallelisms = [parallelism for _, _, parallelism, _ in cases]
        results = [
            put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            for key, tensor, parallelism in zip(keys, tensors, parallelisms)
        ]
        self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        buffer_sizes = [max(serialized_tensor_size(tensor) + 4096, 1 * 1024 * 1024) for tensor in tensors]
        total_buffer_size = sum(buffer_sizes)
        backing_buffer = (ctypes.c_ubyte * total_buffer_size)()
        backing_buffer_ptr = ctypes.addressof(backing_buffer)
        buffer_ptrs = []
        offset = 0
        for size in buffer_sizes:
            buffer_ptrs.append(backing_buffer_ptr + offset)
            offset += size

        targets = [make_read_target("shard", parallelism) for parallelism in parallelisms]
        self.assertEqual(self.store.register_buffer(backing_buffer_ptr, total_buffer_size), 0)
        try:
            shard_reads = self.store.batch_get_tensor_with_parallelism_into(
                keys, buffer_ptrs, buffer_sizes, targets
            )
            self.assertEqual(len(shard_reads), len(cases))
            for i, (_, tensor, _, expected_fn) in enumerate(cases):
                self.assertIsNotNone(shard_reads[i], f"batch shard into missing for tensor {i}")
                self.assertTrue(torch.equal(shard_reads[i], expected_fn(tensor)))
        finally:
            self.assertEqual(self.store.unregister_buffer(backing_buffer_ptr), 0)

    def test_20_unified_parallelism_full_reconstructs_tensor(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_full"
        tensor = torch.arange(32, dtype=torch.float32).view(4, 8).contiguous()
        tp_size = 4
        split_dim = 1

        rc = put_uniform_full_tensor_with_unified_tp(
            self.store, key, tensor, tp_size, split_dim
        )
        self.assertEqual(rc, 0)

        target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=0))
        unified_full = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(unified_full)
        self.assertTrue(torch.equal(unified_full, tensor))

    def test_21_unified_parallelism_multi_axis_full_reconstructs_tensor(self):
        require_unified_parallelism_api(self)
        key = "func_unified_multi_axis_full"
        tensor = make_deterministic_tensor((6, 8))
        parallelism = build_dp_tp_parallelism(
            dp_rank=1, dp_size=2, tp_rank=0, tp_size=4, split_dim=1
        )

        rc = put_full_tensor_with_parallelism_template(
            self.store, key, tensor, parallelism
        )
        self.assertEqual(rc, 0)

        full_target = make_read_target(
            "full",
            build_dp_tp_parallelism(
                dp_rank=1, dp_size=2, tp_rank=2, tp_size=4, split_dim=1
            ),
        )
        result = self.store.get_tensor_with_parallelism(key, full_target)
        self.assertIsNotNone(result)
        self.assertTrue(torch.equal(result, tensor))

    def test_22_unified_parallelism_multi_axis_full_into_reconstructs_tensor(self):
        require_unified_parallelism_api(self)
        key = "func_unified_multi_axis_full_into"
        tensor = make_deterministic_tensor((8, 6))
        parallelism = build_pp_tp_parallelism(
            pp_rank=0, pp_size=2, stage_id=4, tp_rank=0, tp_size=2, split_dim=0
        )
        buffer_spacing = 1 * 1024 * 1024
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            rc = put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            self.assertEqual(rc, 0)
            full_target = make_read_target(
                "full",
                build_pp_tp_parallelism(
                    pp_rank=0, pp_size=2, stage_id=4, tp_rank=1, tp_size=2, split_dim=0
                ),
            )
            result = self.store.get_tensor_with_parallelism_into(
                key, buffer_ptr, buffer_spacing, target=full_target
            )
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, tensor))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_23_unified_parallelism_into_matches_wrapper(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_into"
        tensor = torch.randn(16, 16, dtype=torch.float32)
        tp_size = 2
        split_dim = 0
        buffer_spacing = 1 * 1024 * 1024
        total_buffer_size = buffer_spacing * 2

        large_buffer = (ctypes.c_ubyte * total_buffer_size)()
        large_buffer_ptr = ctypes.addressof(large_buffer)
        wrapper_ptr = large_buffer_ptr
        unified_ptr = large_buffer_ptr + buffer_spacing

        self.assertEqual(self.store.register_buffer(large_buffer_ptr, total_buffer_size), 0)
        try:
            parallelism = make_tensor_parallelism([
                make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)
            ])
            shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, 0).contiguous()
            rc = self.store.put_tensor_with_parallelism(key, shard, parallelism)
            self.assertEqual(rc, 0)

            target = make_read_target("shard", parallelism)
            wrapper_tensor = self.store.get_tensor_with_tp_into(
                key, wrapper_ptr, buffer_spacing, tp_rank=0, tp_size=tp_size, split_dim=split_dim
            )
            unified_tensor = self.store.get_tensor_with_parallelism_into(
                key, unified_ptr, buffer_spacing, target=target
            )
            self.assertIsNotNone(wrapper_tensor)
            self.assertIsNotNone(unified_tensor)
            self.assertTrue(torch.equal(unified_tensor, wrapper_tensor))
        finally:
            self.assertEqual(self.store.unregister_buffer(large_buffer_ptr), 0)

    def test_24_unified_parallelism_from_matches_wrapper(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_from"
        seed_key = "func_unified_tp_from_seed"
        tensor = torch.randn(16, 16, dtype=torch.float32)
        tp_size = 2
        split_dim = 1
        buffer_spacing = 1 * 1024 * 1024

        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            rc = self.store.put_tensor(seed_key, tensor)
            self.assertEqual(rc, 0)
            full_tensor = self.store.get_tensor_into(seed_key, buffer_ptr, buffer_spacing)
            self.assertIsNotNone(full_tensor)
            full_size = serialized_tensor_size(full_tensor)

            parallelism = make_tensor_parallelism([
                make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)
            ])
            rc = self.store.put_tensor_with_parallelism_from(
                key, buffer_ptr, full_size, parallelism
            )
            self.assertEqual(rc, 0)

            target = make_read_target("shard", parallelism)
            unified_shard = self.store.get_tensor_with_parallelism(key, target)
            wrapper_shard = self.store.get_tensor_with_tp(
                key, tp_rank=0, tp_size=tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(unified_shard)
            self.assertTrue(torch.equal(unified_shard, wrapper_shard))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_25_unified_parallelism_full_into_reconstructs_tensor(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_full_into"
        tensor = torch.arange(4 * 9 * 6, dtype=torch.float32).view(4, 9, 6).contiguous()
        self.assert_full_into_reconstruction_case(
            key=key,
            tensor=tensor,
            split_dim=1,
            tp_size=3,
            buffer_spacing=1 * 1024 * 1024,
        )

    def test_26_unified_parallelism_full_reconstruction_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            ((8, 12), 0, 4),
            ((8, 12), 0, 8),
            ((8, 12), 0, 16),
            ((10, 12), 0, 4),
            ((12, 9), 1, 3),
            ((12, 9), 1, 9),
            ((12, 9), 1, 16),
            ((12, 10), 1, 4),
            ((4, 10, 6), 1, 4),
            ((4, 5, 7), 2, 8),
        ]

        for index, (shape, split_dim, tp_size) in enumerate(cases):
            with self.subTest(case=reconstruction_case_name(shape, split_dim, tp_size)):
                tensor = make_deterministic_tensor(shape)
                key = f"func_unified_tp_matrix_{index}"
                if is_uniform_tp_case(shape, split_dim, tp_size):
                    self.assert_full_reconstruction_case(
                        key=key,
                        tensor=tensor,
                        split_dim=split_dim,
                        tp_size=tp_size,
                    )
                else:
                    rc = put_uniform_full_tensor_with_unified_tp(
                        self.store, key, tensor, tp_size, split_dim
                    )
                    self.assertNotEqual(rc, 0)

    def test_27_unified_parallelism_full_into_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            ((8, 12), 0, 16),
            ((12, 10), 1, 4),
            ((4, 10, 6), 1, 4),
            ((4, 5, 7), 2, 8),
        ]

        for index, (shape, split_dim, tp_size) in enumerate(cases):
            with self.subTest(case=reconstruction_case_name(shape, split_dim, tp_size)):
                tensor = make_deterministic_tensor(shape)
                key = f"func_unified_tp_full_into_matrix_{index}"
                if is_uniform_tp_case(shape, split_dim, tp_size):
                    buffer_spacing = max(serialized_tensor_size(tensor) + 4096, 1 * 1024 * 1024)
                    self.assert_full_into_reconstruction_case(
                        key=key,
                        tensor=tensor,
                        split_dim=split_dim,
                        tp_size=tp_size,
                        buffer_spacing=buffer_spacing,
                    )
                else:
                    rc = put_uniform_full_tensor_with_unified_tp(
                        self.store, key, tensor, tp_size, split_dim
                    )
                    self.assertNotEqual(rc, 0)

    def test_28_writer_shard_full_reconstruction(self):
        require_unified_parallelism_api(self)
        key = "func_writer_shard_full"
        tensor = make_deterministic_tensor((6, 8))
        self.assert_writer_shard_full_reconstruction_case(
            key=key, tensor=tensor, split_dim=1, shard_count=4
        )

    def test_29_writer_shard_full_into_reconstruction(self):
        require_unified_parallelism_api(self)
        key = "func_writer_shard_full_into"
        tensor = make_deterministic_tensor((8, 6))
        buffer_spacing = max(serialized_tensor_size(tensor) + 4096, 1 * 1024 * 1024)
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            for rank in range(3):
                rc = self.store.put_tensor_with_parallelism(
                    key,
                    tensor,
                    writer_partition=make_writer_partition(rank, 3, 1),
                )
                self.assertEqual(rc, 0)

            result = self.store.get_tensor_with_parallelism_into(
                key, buffer_ptr, buffer_spacing, target=make_read_target("full")
            )
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, tensor))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_30_batch_writer_partition_full_reconstruction(self):
        require_unified_parallelism_api(self)
        tensors = [
            make_deterministic_tensor((8, 12)),
            make_deterministic_tensor((6, 9, 4)),
            make_deterministic_tensor((12, 12)),
        ]
        keys = [f"func_writer_partition_batch_{i}" for i in range(len(tensors))]
        writer_partitions = [
            make_writer_partition(rank=0, size=4, split_dim=1),
            make_writer_partition(rank=1, size=3, split_dim=0),
            make_writer_partition(rank=2, size=4, split_dim=1),
        ]

        for rank in range(4):
            batch_writer_partitions = [
                make_writer_partition(rank=min(rank, part.size - 1), size=part.size, split_dim=part.split_dim)
                for part in writer_partitions
            ]
            results = self.store.batch_put_tensor_with_parallelism(
                keys,
                tensors,
                writer_partitions=batch_writer_partitions,
            )
            self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        full_targets = [make_read_target("full") for _ in tensors]
        full_reads = self.store.batch_get_tensor_with_parallelism(keys, full_targets)
        self.assertEqual(len(full_reads), len(tensors))
        for i, tensor in enumerate(tensors):
            self.assertTrue(torch.equal(full_reads[i], tensor), f"full reconstruction mismatch for tensor {i}")

    def test_31_batch_writer_partition_into_round_trip(self):
        require_unified_parallelism_api(self)
        tensors = [
            make_deterministic_tensor((7, 9)),
            make_deterministic_tensor((8, 6)),
        ]
        keys = [f"func_writer_partition_batch_into_{i}" for i in range(len(tensors))]
        writer_layouts = [
            make_writer_partition(rank=0, size=3, split_dim=1),
            make_writer_partition(rank=0, size=2, split_dim=0),
        ]
        for rank in range(3):
            batch_writer_partitions = [
                make_writer_partition(rank=min(rank, layout.size - 1), size=layout.size, split_dim=layout.split_dim)
                for layout in writer_layouts
            ]
            results = self.store.batch_put_tensor_with_parallelism(
                keys,
                tensors,
                writer_partitions=batch_writer_partitions,
            )
            self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        buffer_sizes = [max(serialized_tensor_size(tensor) + 4096, 1 * 1024 * 1024) for tensor in tensors]
        total_buffer_size = sum(buffer_sizes)
        backing_buffer = (ctypes.c_ubyte * total_buffer_size)()
        backing_buffer_ptr = ctypes.addressof(backing_buffer)
        buffer_ptrs = []
        offset = 0
        for size in buffer_sizes:
            buffer_ptrs.append(backing_buffer_ptr + offset)
            offset += size

        self.assertEqual(self.store.register_buffer(backing_buffer_ptr, total_buffer_size), 0)
        try:
            full_reads = self.store.batch_get_tensor_with_parallelism_into(
                keys, buffer_ptrs, buffer_sizes, [make_read_target("full") for _ in tensors]
            )
            self.assertEqual(len(full_reads), len(tensors))
            for i, tensor in enumerate(tensors):
                self.assertIsNotNone(full_reads[i], f"batch full into missing for tensor {i}")
                self.assertTrue(torch.equal(full_reads[i], tensor), f"batch full into mismatch for tensor {i}")
        finally:
            self.assertEqual(self.store.unregister_buffer(backing_buffer_ptr), 0)

    def test_32_batch_unified_parallelism_full_reconstruction(self):
        require_unified_parallelism_api(self)
        tp_size = 4
        split_dim = 1
        tensors = [
            make_deterministic_tensor((8, 12)),
            make_deterministic_tensor((6, 12, 4)),
            make_deterministic_tensor((12, 12)),
        ]
        keys = [f"func_unified_tp_full_batch_{i}" for i in range(len(tensors))]
        parallelisms = [build_tp_parallelism(tp_size, split_dim, rank=0) for _ in tensors]
        results = batch_put_full_tensors_with_unified_tp(self.store, keys, tensors, tp_size, split_dim)
        self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        full_targets = [make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=1)) for _ in tensors]
        full_reads = self.store.batch_get_tensor_with_parallelism(keys, full_targets)
        self.assertEqual(len(full_reads), len(tensors))
        for i, tensor in enumerate(tensors):
            self.assertTrue(torch.equal(full_reads[i], tensor), f"full reconstruction mismatch for tensor {i}")

        shard_targets = [make_read_target("shard", build_tp_parallelism(tp_size, split_dim, rank=2)) for _ in tensors]
        shard_reads = self.store.batch_get_tensor_with_parallelism(keys, shard_targets)
        self.assertEqual(len(shard_reads), len(tensors))
        for i, tensor in enumerate(tensors):
            expected_shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, 2)
            self.assertTrue(torch.equal(shard_reads[i], expected_shard), f"shard mismatch for tensor {i}")

    def test_31_batch_unified_parallelism_multi_axis_full_reconstruction(self):
        require_unified_parallelism_api(self)
        cases = [
            (
                "func_unified_batch_multi_full_0",
                make_deterministic_tensor((6, 8)),
                build_dp_tp_parallelism(1, 2, 0, 4, 1),
                build_dp_tp_parallelism(1, 2, 2, 4, 1),
            ),
            (
                "func_unified_batch_multi_full_1",
                make_deterministic_tensor((8, 6)),
                build_pp_tp_parallelism(0, 2, 6, 0, 2, 0),
                build_pp_tp_parallelism(0, 2, 6, 1, 2, 0),
            ),
        ]
        keys = [key for key, _, _, _ in cases]
        tensors = [tensor for _, tensor, _, _ in cases]
        write_parallelisms = [parallelism for _, _, parallelism, _ in cases]
        results = [
            put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            for key, tensor, parallelism in zip(keys, tensors, write_parallelisms)
        ]
        self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        full_targets = [make_read_target("full", target_parallelism) for _, _, _, target_parallelism in cases]
        full_reads = self.store.batch_get_tensor_with_parallelism(keys, full_targets)
        self.assertEqual(len(full_reads), len(cases))
        for i, tensor in enumerate(tensors):
            self.assertIsNotNone(full_reads[i], f"multi-axis full missing for tensor {i}")
            self.assertTrue(torch.equal(full_reads[i], tensor), f"multi-axis full mismatch for tensor {i}")

    def test_32_unified_parallelism_multi_axis_full_remaps_axis_order(self):
        require_unified_parallelism_api(self)
        key = "func_unified_multi_axis_full_remap_order"
        tensor = make_deterministic_tensor((6, 8))
        write_parallelism = build_dp_tp_parallelism(
            dp_rank=1, dp_size=2, tp_rank=0, tp_size=4, split_dim=1
        )
        read_parallelism = reorder_parallelism_axes(write_parallelism, ["tp", "dp"])
        read_parallelism.axes[0].rank = 3

        rc = put_full_tensor_with_parallelism_template(
            self.store, key, tensor, write_parallelism
        )
        self.assertEqual(rc, 0)

        full_target = make_read_target("full", read_parallelism)
        result = self.store.get_tensor_with_parallelism(key, full_target)
        self.assertIsNotNone(result)
        self.assertTrue(torch.equal(result, tensor))

    def test_33_unified_parallelism_multi_axis_full_into_remaps_axis_order(self):
        require_unified_parallelism_api(self)
        key = "func_unified_multi_axis_full_into_remap_order"
        tensor = make_deterministic_tensor((8, 6))
        write_parallelism = build_pp_tp_parallelism(
            pp_rank=0, pp_size=2, stage_id=4, tp_rank=0, tp_size=2, split_dim=0
        )
        read_parallelism = reorder_parallelism_axes(write_parallelism, ["tp", "pp"])
        read_parallelism.axes[0].rank = 1
        buffer_spacing = 1 * 1024 * 1024
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        try:
            rc = put_full_tensor_with_parallelism_template(
                self.store, key, tensor, write_parallelism
            )
            self.assertEqual(rc, 0)
            full_target = make_read_target("full", read_parallelism)
            result = self.store.get_tensor_with_parallelism_into(
                key, buffer_ptr, buffer_spacing, target=full_target
            )
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, tensor))
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_34_batch_unified_parallelism_multi_axis_full_remaps_axis_order(self):
        require_unified_parallelism_api(self)
        cases = [
            (
                "func_unified_batch_multi_full_remap_0",
                make_deterministic_tensor((6, 8)),
                build_dp_tp_parallelism(1, 2, 0, 4, 1),
                reorder_parallelism_axes(build_dp_tp_parallelism(1, 2, 2, 4, 1), ["tp", "dp"]),
            ),
            (
                "func_unified_batch_multi_full_remap_1",
                make_deterministic_tensor((8, 6)),
                build_pp_tp_parallelism(0, 2, 6, 0, 2, 0),
                reorder_parallelism_axes(build_pp_tp_parallelism(0, 2, 6, 1, 2, 0), ["tp", "pp"]),
            ),
        ]
        keys = [key for key, _, _, _ in cases]
        tensors = [tensor for _, tensor, _, _ in cases]
        write_parallelisms = [parallelism for _, _, parallelism, _ in cases]
        read_parallelisms = [parallelism for _, _, _, parallelism in cases]
        results = [
            put_full_tensor_with_parallelism_template(
                self.store, key, tensor, parallelism
            )
            for key, tensor, parallelism in zip(keys, tensors, write_parallelisms)
        ]
        self.assertTrue(all(r == 0 for r in results), f"batch put failed: {results}")

        full_targets = [make_read_target("full", target_parallelism) for target_parallelism in read_parallelisms]
        full_reads = self.store.batch_get_tensor_with_parallelism(keys, full_targets)
        self.assertEqual(len(full_reads), len(cases))
        for i, tensor in enumerate(tensors):
            self.assertIsNotNone(full_reads[i], f"multi-axis remap full missing for tensor {i}")
            self.assertTrue(torch.equal(full_reads[i], tensor), f"multi-axis remap full mismatch for tensor {i}")

    def test_35_unified_parallelism_full_large_payload(self):
        require_unified_parallelism_api(self)
        key = "func_unified_tp_full_large"
        tensor = make_deterministic_tensor((32, 32))
        split_dim = 1
        tp_size = 4
        buffer_spacing = serialized_tensor_size(tensor) + 1 * 1024 * 1024

        rc = put_uniform_full_tensor_with_unified_tp(
            self.store, key, tensor, tp_size, split_dim
        )
        self.assertEqual(rc, 0)

        full_target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=1))
        full_tensor = self.store.get_tensor_with_parallelism(key, full_target)
        self.assertIsNotNone(full_tensor)
        self.assertTrue(torch.equal(full_tensor, tensor))

        shard_rank = 3
        shard_target = make_read_target("shard", build_tp_parallelism(tp_size, split_dim, rank=shard_rank))
        shard_tensor = self.store.get_tensor_with_parallelism(key, shard_target)
        expected_shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, shard_rank)
        self.assertIsNotNone(shard_tensor)
        self.assertTrue(torch.equal(shard_tensor, expected_shard))

        self.assert_full_into_reconstruction_case(
            key=f"{key}_into",
            tensor=tensor,
            split_dim=split_dim,
            tp_size=tp_size,
            buffer_spacing=buffer_spacing,
        )

    def test_36_unified_parallelism_full_reconstruction_edge_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            ((7,), 0, 8),
            ((9,), 0, 4),
            ((3, 5, 7), 2, 16),
            ((5, 11, 3), 1, 8),
        ]

        for index, (shape, split_dim, tp_size) in enumerate(cases):
            with self.subTest(case=reconstruction_case_name(shape, split_dim, tp_size)):
                tensor = make_deterministic_tensor(shape)
                key = f"func_unified_tp_edge_matrix_{index}"
                rc = put_uniform_full_tensor_with_unified_tp(
                    self.store, key, tensor, tp_size, split_dim
                )
                self.assertNotEqual(rc, 0)

    def test_37_batch_unified_parallelism_full_into_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            (make_deterministic_tensor((7,)), 0, 8),
            (make_deterministic_tensor((8, 12)), 0, 16),
            (make_deterministic_tensor((4, 5, 7)), 2, 8),
        ]
        for index, (tensor, split_dim, tp_size) in enumerate(cases):
            with self.subTest(case=reconstruction_case_name(tuple(tensor.shape), split_dim, tp_size)):
                rc = put_uniform_full_tensor_with_unified_tp(
                    self.store,
                    f"func_unified_tp_batch_full_into_{index}",
                    tensor,
                    tp_size,
                    split_dim,
                )
                self.assertNotEqual(rc, 0)

    def test_38_batch_unified_parallelism_full_ragged_cases(self):
        require_unified_parallelism_api(self)
        cases = [
            (make_deterministic_tensor((7,)), 0, 8),
            (make_deterministic_tensor((8, 12)), 0, 16),
            (make_deterministic_tensor((11, 9)), 1, 4),
            (make_deterministic_tensor((3, 5, 7)), 2, 16),
        ]
        for index, (tensor, split_dim, tp_size) in enumerate(cases):
            with self.subTest(case=reconstruction_case_name(tuple(tensor.shape), split_dim, tp_size)):
                rc = put_uniform_full_tensor_with_unified_tp(
                    self.store,
                    f"func_unified_tp_batch_ragged_{index}",
                    tensor,
                    tp_size,
                    split_dim,
                )
                self.assertNotEqual(rc, 0)

    def test_39_unified_parallelism_full_dtype_smoke(self):
        require_unified_parallelism_api(self)
        cases = [
            (torch.float16, (8, 12), 1, 4),
            (torch.bfloat16, (9,), 0, 16),
        ]

        for index, (dtype, shape, split_dim, tp_size) in enumerate(cases):
            with self.subTest(dtype=str(dtype), case=reconstruction_case_name(shape, split_dim, tp_size)):
                tensor = make_deterministic_tensor(shape, dtype=dtype)
                key = f"func_unified_tp_dtype_{index}"
                if is_uniform_tp_case(shape, split_dim, tp_size):
                    self.assert_full_reconstruction_case(
                        key=key,
                        tensor=tensor,
                        split_dim=split_dim,
                        tp_size=tp_size,
                    )
                else:
                    rc = put_uniform_full_tensor_with_unified_tp(
                        self.store, key, tensor, tp_size, split_dim
                    )
                    self.assertNotEqual(rc, 0)

    def test_40_unified_parallelism_target_shard_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            ((8, 12, 6), 0, 4, 2, "tp"),
            ((8, 12, 6), 1, 4, 2, "dp_tp"),
            ((8, 12, 6), 2, 2, 4, "pp_tp"),
            ((8, 12, 6), 1, 2, 4, "ep_tp"),
            ((12, 8, 6), 0, 2, 2, "tp"),
            ((12, 8, 8), 2, 4, 4, "dp_tp"),
        ]

        for index, (shape, split_dim, put_tp_size, get_tp_size, mode) in enumerate(cases):
            with self.subTest(shape=shape, split_dim=split_dim, put_tp_size=put_tp_size, get_tp_size=get_tp_size, mode=mode):
                tensor = make_deterministic_tensor(shape)
                key = f"func_unified_target_shard_matrix_{index}"
                rc = put_full_tensor_with_parallelism_mode(
                    self.store, key, tensor, split_dim, put_tp_size, mode
                )
                self.assertEqual(rc, 0)

                if not is_uniform_tp_case(shape, split_dim, get_tp_size):
                    target_parallelism = build_parallelism_mode(
                        mode, 0, get_tp_size, split_dim
                    )
                    target = make_read_target("shard", target_parallelism)
                    shard = self.store.get_tensor_with_parallelism(key, target)
                    self.assertIsNone(shard)
                    continue

                for target_rank in range(get_tp_size):
                    target_parallelism = build_parallelism_mode(
                        mode, target_rank, get_tp_size, split_dim
                    )
                    target = make_read_target("shard", target_parallelism)
                    shard = self.store.get_tensor_with_parallelism(key, target)
                    expected = expected_target_shard(
                        tensor, split_dim, get_tp_size, target_rank
                    )
                    self.assertIsNotNone(shard)
                    self.assertTrue(
                        torch.equal(shard, expected),
                        f"target shard mismatch for mode={mode}, rank={target_rank}",
                    )

    def test_41_unified_parallelism_target_shard_into_matrix(self):
        require_unified_parallelism_api(self)
        cases = [
            ((8, 12, 6), 0, 4, 2, "tp"),
            ((8, 12, 6), 1, 4, 2, "dp_tp"),
            ((8, 12, 6), 2, 2, 4, "pp_tp"),
            ((8, 12, 6), 1, 2, 4, "ep_tp"),
        ]

        for index, (shape, split_dim, put_tp_size, get_tp_size, mode) in enumerate(cases):
            with self.subTest(shape=shape, split_dim=split_dim, put_tp_size=put_tp_size, get_tp_size=get_tp_size, mode=mode):
                tensor = make_deterministic_tensor(shape)
                key = f"func_unified_target_shard_into_matrix_{index}"
                rc = put_full_tensor_with_parallelism_mode(
                    self.store, key, tensor, split_dim, put_tp_size, mode
                )
                self.assertEqual(rc, 0)

                if not is_uniform_tp_case(shape, split_dim, get_tp_size):
                    buffer_spacing = 1 * 1024 * 1024
                    buffer = (ctypes.c_ubyte * buffer_spacing)()
                    buffer_ptr = ctypes.addressof(buffer)
                    self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
                    try:
                        target_parallelism = build_parallelism_mode(
                            mode, 0, get_tp_size, split_dim
                        )
                        target = make_read_target("shard", target_parallelism)
                        shard = self.store.get_tensor_with_parallelism_into(
                            key, buffer_ptr, buffer_spacing, target=target
                        )
                        self.assertIsNone(shard)
                    finally:
                        self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)
                    continue

                for target_rank in range(get_tp_size):
                    expected = expected_target_shard(
                        tensor, split_dim, get_tp_size, target_rank
                    )
                    buffer_spacing = max(serialized_tensor_size(expected) + 4096, 1 * 1024 * 1024)
                    buffer = (ctypes.c_ubyte * buffer_spacing)()
                    buffer_ptr = ctypes.addressof(buffer)
                    self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
                    try:
                        target_parallelism = build_parallelism_mode(
                            mode, target_rank, get_tp_size, split_dim
                        )
                        target = make_read_target("shard", target_parallelism)
                        shard = self.store.get_tensor_with_parallelism_into(
                            key, buffer_ptr, buffer_spacing, target=target
                        )
                        self.assertIsNotNone(shard)
                        self.assertTrue(
                            torch.equal(shard, expected),
                            f"target shard into mismatch for mode={mode}, rank={target_rank}",
                        )
                    finally:
                        self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_42_unified_parallelism_mb_scale_target_shard_matrix(self):
        require_unified_parallelism_api(self)
        original = make_mb_scale_original_tensor()
        cases = [
            (0, 4, 2, "tp"),
            (1, 4, 2, "dp_tp"),
            (2, 4, 2, "pp_tp"),
            (2, 4, 2, "ep_tp"),
        ]

        for index, (split_dim, put_tp_size, get_tp_size, mode) in enumerate(cases):
            key = f"func_unified_mb_target_shard_{index}"
            with self.subTest(split_dim=split_dim, put_tp_size=put_tp_size,
                              get_tp_size=get_tp_size, mode=mode):
                rc = put_full_tensor_with_parallelism_mode(
                    self.store, key, original, split_dim, put_tp_size, mode
                )
                self.assertEqual(rc, 0)
                for target_rank in range(get_tp_size):
                    target_parallelism = build_parallelism_mode(
                        mode, target_rank, get_tp_size, split_dim
                    )
                    target = make_read_target("shard", target_parallelism)
                    shard = self.store.get_tensor_with_parallelism(key, target)
                    assert_tensor_matches_expected_shard(
                        self, original, shard, split_dim, get_tp_size, target_rank,
                        context=(
                            f"mb shard read mode={mode} split_dim={split_dim} "
                            f"target_rank={target_rank}"
                        ),
                    )

    def test_43_unified_parallelism_mb_scale_target_shard_into_matrix(self):
        require_unified_parallelism_api(self)
        original = make_mb_scale_original_tensor()
        cases = [
            (0, 4, 2, "tp"),
            (1, 4, 2, "dp_tp"),
            (2, 4, 2, "pp_tp"),
            (2, 4, 2, "ep_tp"),
        ]

        for index, (split_dim, put_tp_size, get_tp_size, mode) in enumerate(cases):
            key = f"func_unified_mb_target_shard_into_{index}"
            with self.subTest(split_dim=split_dim, put_tp_size=put_tp_size,
                              get_tp_size=get_tp_size, mode=mode):
                rc = put_full_tensor_with_parallelism_mode(
                    self.store, key, original, split_dim, put_tp_size, mode
                )
                self.assertEqual(rc, 0)
                for target_rank in range(get_tp_size):
                    expected = expected_target_shard(
                        original, split_dim, get_tp_size, target_rank
                    )
                    buffer_spacing = max(
                        serialized_tensor_size(expected) + 4096,
                        1 * 1024 * 1024,
                    )
                    buffer = (ctypes.c_ubyte * buffer_spacing)()
                    buffer_ptr = ctypes.addressof(buffer)
                    self.assertEqual(
                        self.store.register_buffer(buffer_ptr, buffer_spacing), 0
                    )
                    try:
                        target_parallelism = build_parallelism_mode(
                            mode, target_rank, get_tp_size, split_dim
                        )
                        target = make_read_target("shard", target_parallelism)
                        shard = self.store.get_tensor_with_parallelism_into(
                            key, buffer_ptr, buffer_spacing, target=target
                        )
                        assert_tensor_matches_expected_shard(
                            self, original, shard, split_dim, get_tp_size,
                            target_rank,
                            context=(
                                f"mb shard into mode={mode} split_dim={split_dim} "
                                f"target_rank={target_rank}"
                            ),
                        )
                    finally:
                        self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

    def test_44_unified_parallelism_concurrency_relation_matrix(self):
        require_unified_parallelism_api(self)
        original = make_mb_scale_original_tensor()
        scenarios = [
            (2, 4, [(0, 4, 2, "tp"), (1, 4, 2, "dp_tp"), (2, 4, 2, "pp_tp")]),
            (3, 3, [(0, 4, 2, "tp"), (1, 4, 2, "dp_tp"), (2, 4, 2, "ep_tp")]),
            (6, 3, [(0, 4, 2, "tp"), (1, 4, 2, "pp_tp"), (2, 4, 2, "ep_tp")]),
        ]

        def put_worker(key, split_dim, put_tp_size, mode):
            return put_full_tensor_with_parallelism_mode(
                self.store, key, original, split_dim, put_tp_size, mode
            )

        def get_worker(key, split_dim, get_tp_size, mode):
            for target_rank in range(get_tp_size):
                target_parallelism = build_parallelism_mode(
                    mode, target_rank, get_tp_size, split_dim
                )
                target = make_read_target("shard", target_parallelism)
                shard = self.store.get_tensor_with_parallelism(key, target)
                expected = expected_target_shard(
                    original, split_dim, get_tp_size, target_rank
                )
                if shard is None or not torch.equal(shard, expected):
                    return (
                        f"concurrency shard mismatch key={key} split_dim={split_dim} "
                        f"mode={mode} rank={target_rank}"
                    )
            return None

        for scenario_index, (put_workers, get_workers, cases) in enumerate(scenarios):
            with self.subTest(put_workers=put_workers, get_workers=get_workers):
                keys = [
                    f"func_unified_concurrency_{scenario_index}_{case_index}"
                    for case_index in range(len(cases))
                ]
                with concurrent.futures.ThreadPoolExecutor(max_workers=max(put_workers, get_workers)) as executor:
                    put_futures = [
                        executor.submit(put_worker, key, split_dim, put_tp_size, mode)
                        for key, (split_dim, put_tp_size, _get_tp_size, mode) in zip(keys, cases)
                        for _ in range(max(1, put_workers // len(cases)))
                    ]
                    for future in concurrent.futures.as_completed(put_futures):
                        self.assertEqual(future.result(), 0)

                    get_futures = [
                        executor.submit(get_worker, key, split_dim, get_tp_size, mode)
                        for key, (split_dim, _put_tp_size, get_tp_size, mode) in zip(keys, cases)
                        for _ in range(max(1, get_workers // len(cases)))
                    ]
                    for future in concurrent.futures.as_completed(get_futures):
                        error = future.result()
                        if error:
                            self.fail(error)

# ==========================================
#  Performance/Benchmark Tests
# ==========================================

class TestMooncakeBenchmark(MooncakeTestBase):
    # Benchmark Settings
    BENCH_ITERATIONS = 5
    TENSOR_SIZE_MB = 16
    TOTAL_SIZE_MB = 256

    def _run_unified_full_manual_gather(self, key, tensor, split_dim, tp_size):
        shards = []
        for rank in range(tp_size):
            shard = self.store.get_tensor_with_tp(
                key, tp_rank=rank, tp_size=tp_size, split_dim=split_dim
            )
            self.assertIsNotNone(shard)
            shards.append(shard)
        reconstructed = torch.cat(shards, dim=split_dim)
        self.assertTrue(torch.equal(reconstructed, tensor))
        return reconstructed

    def _run_unified_full_into(self, key, target, buffer_ptr, buffer_spacing, tensor):
        reconstructed = self.store.get_tensor_with_parallelism_into(
            key, buffer_ptr, buffer_spacing, target
        )
        self.assertIsNotNone(reconstructed)
        self.assertTrue(torch.equal(reconstructed, tensor))
        return reconstructed

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
        """Benchmark: Zero-copy Batch Put/Get with TP using full tensor buffers."""
        tp_size = 4
        split_dim = 0
        batch_size = len(self.keys)
        self.store.remove_all()
        buffer_spacing = 64 * 1024 * 1024  # 64MB per tensor slot
        seed_keys = [f"seed_{k}" for k in self.keys]

        full_total_buffer_size = buffer_spacing * batch_size
        full_buffer = (ctypes.c_ubyte * full_total_buffer_size)()
        full_buffer_ptr = ctypes.addressof(full_buffer)
        full_ptrs = [full_buffer_ptr + i * buffer_spacing for i in range(batch_size)]
        full_sizes = [buffer_spacing] * batch_size
        res = self.store.register_buffer(full_buffer_ptr, full_total_buffer_size)
        self.assertEqual(res, 0, "Buffer registration failed for full buffers")

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
            self.store.batch_put_tensor(seed_keys, self.tensors)
            self.store.batch_get_tensor_into(seed_keys, full_ptrs, full_sizes)
            put_sizes = [serialized_tensor_size(self.tensors[j]) for j in range(batch_size)]

            t0 = time.perf_counter()
            self.store.batch_put_tensor_with_tp_from(
                self.keys, full_ptrs, put_sizes,
                tp_rank=1, tp_size=tp_size, split_dim=split_dim
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
        self.assertEqual(self.store.unregister_buffer(full_buffer_ptr), 0, "Full buffer unregistration failed")
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

    def test_benchmark_07_unified_full_reconstruction(self):
        require_unified_parallelism_api(self)
        key = "bench_unified_full"
        tensor = make_deterministic_tensor((4, 8))
        tp_size = 4
        split_dim = 1
        full_target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=1))
        manual_times = []
        unified_times = []

        print(f"--- Running Unified Full Reconstruction Benchmark (TP={tp_size}) ---")
        for _ in range(self.BENCH_ITERATIONS):
            self.store.remove_all()
            rc = put_uniform_full_tensor_with_unified_tp(
                self.store, key, tensor, tp_size, split_dim
            )
            self.assertEqual(rc, 0)

            t0 = time.perf_counter()
            unified_tensor = self.store.get_tensor_with_parallelism(key, full_target)
            unified_times.append(time.perf_counter() - t0)
            self.assertIsNotNone(unified_tensor)
            self.assertTrue(torch.equal(unified_tensor, tensor))

            t0 = time.perf_counter()
            self._run_unified_full_manual_gather(key, tensor, split_dim, tp_size)
            manual_times.append(time.perf_counter() - t0)

        self.total_bits = tensor.numel() * tensor.element_size() * 8
        self._print_perf("Unified Full Get", unified_times)
        self._print_perf("Manual TP Gather", manual_times)

    def test_benchmark_08_unified_full_into(self):
        require_unified_parallelism_api(self)
        key = "bench_unified_full_into"
        tensor = self.tensors[0]
        tp_size = 4
        split_dim = 0
        full_target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=2))
        buffer_spacing = serialized_tensor_size(tensor) + 1 * 1024 * 1024
        buffer = (ctypes.c_ubyte * buffer_spacing)()
        buffer_ptr = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(buffer_ptr, buffer_spacing), 0)
        into_times = []
        try:
            print(f"--- Running Unified Full Into Benchmark (TP={tp_size}) ---")
            for _ in range(self.BENCH_ITERATIONS):
                self.store.remove_all()
                rc = put_uniform_full_tensor_with_unified_tp(
                    self.store, key, tensor, tp_size, split_dim
                )
                self.assertEqual(rc, 0)

                t0 = time.perf_counter()
                self._run_unified_full_into(key, full_target, buffer_ptr, buffer_spacing, tensor)
                into_times.append(time.perf_counter() - t0)
        finally:
            self.assertEqual(self.store.unregister_buffer(buffer_ptr), 0)

        self.total_bits = tensor.numel() * tensor.element_size() * 8
        self._print_perf("Unified Full Into", into_times)

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

    def _run_unified_full_worker(self, worker_id, split_dim, tp_size, use_into=False, mixed_reads=False):
        key = f"stress_unified_full_{worker_id}"
        if split_dim == 0:
            tensor = make_deterministic_tensor((8, 8))
        elif split_dim == 1:
            tensor = make_deterministic_tensor((4, 8))
        else:
            tensor = make_deterministic_tensor((2, 2, 8))

        rc = put_uniform_full_tensor_with_unified_tp(
            self.store, key, tensor, tp_size, split_dim
        )
        if rc != 0:
            return f"put failed for {key}, rc={rc}"

        full_target = make_read_target("full", build_tp_parallelism(tp_size, split_dim, rank=min(tp_size - 1, 1)))
        full_tensor = self.store.get_tensor_with_parallelism(key, full_target)
        if full_tensor is None or not torch.equal(full_tensor, tensor):
            return f"full reconstruction mismatch for {key}, split_dim={split_dim}, tp_size={tp_size}"

        if mixed_reads:
            shard_rank = min(tp_size - 1, 2)
            shard_target = make_read_target("shard", build_tp_parallelism(tp_size, split_dim, rank=shard_rank))
            shard_tensor = self.store.get_tensor_with_parallelism(key, shard_target)
            expected_shard = chunk_tensor_for_rank(tensor, tp_size, split_dim, shard_rank)
            if shard_tensor is None or not torch.equal(shard_tensor, expected_shard):
                return f"shard mismatch for {key}, split_dim={split_dim}, tp_size={tp_size}, rank={shard_rank}"

        if use_into:
            buffer_spacing = serialized_tensor_size(tensor) + 64 * 1024
            buffer = (ctypes.c_ubyte * buffer_spacing)()
            buffer_ptr = ctypes.addressof(buffer)
            if self.store.register_buffer(buffer_ptr, buffer_spacing) != 0:
                return f"register_buffer failed for {key}"
            try:
                into_tensor = self.store.get_tensor_with_parallelism_into(
                    key, buffer_ptr, buffer_spacing, target=full_target
                )
                if into_tensor is None or not torch.equal(into_tensor, tensor):
                    return f"full into mismatch for {key}, split_dim={split_dim}, tp_size={tp_size}"
            finally:
                self.store.unregister_buffer(buffer_ptr)

        return None

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

                remove_rc = self.store.remove(key, True)
                if remove_rc != 0:
                    raise RuntimeError(f"Remove failed for {key}, rc={remove_rc}")

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

    def test_stress_unified_parallelism_full_reads(self):
        require_unified_parallelism_api(self)
        worker_cases = [
            (0, 0, 2, False, False),
            (1, 1, 2, False, True),
            (2, 0, 4, True, False),
            (3, 1, 4, True, True),
            (4, 2, 4, False, True),
            (5, 2, 4, True, True),
        ]
        errors = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(worker_cases)) as executor:
            futures = [
                executor.submit(self._run_unified_full_worker, *case)
                for case in worker_cases
            ]
            for future in concurrent.futures.as_completed(futures):
                error = future.result()
                if error:
                    errors.append(error)

        if errors:
            self.fail(errors[0])


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
