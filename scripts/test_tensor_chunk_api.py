import argparse
import ctypes
import os
import sys
import unittest

import torch

from mooncake.mooncake_config import MooncakeConfig
from mooncake.store import MooncakeDistributedStore


TENSOR_METADATA_SIZE = 4 + 4 + 8 * 4


def serialized_tensor_size(tensor):
    return TENSOR_METADATA_SIZE + tensor.numel() * tensor.element_size()


GLOBAL_STORE = None
GLOBAL_CONFIG = None


def create_store_connection():
    store = MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
    print(
        f"[{os.getpid()}] Connecting to Mooncake Master at "
        f"{config.master_server_address} using {config.protocol}..."
    )
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


def setUpModule():
    global GLOBAL_STORE, GLOBAL_CONFIG
    try:
        GLOBAL_STORE, GLOBAL_CONFIG = create_store_connection()
        print("✅ Global Store connection established.")
    except Exception as e:
        print(f"❌ Failed to establish global store connection: {e}")
        sys.exit(1)


def tearDownModule():
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None


class MooncakeTestBase(unittest.TestCase):
    def setUp(self):
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")
        self.store = GLOBAL_STORE
        self.config = GLOBAL_CONFIG
        self.store.remove_all()


class TestTensorChunkAPI(MooncakeTestBase):
    def test_put_tensor_chunk_with_tp_invalid_split_dim(self):
        key = "chunk_invalid_split_dim"
        chunk = torch.arange(16, dtype=torch.float32).reshape(4, 4)

        rc = self.store.put_tensor_chunk_with_tp(
            key,
            chunk,
            tp_rank=0,
            tp_size=2,
            split_dim=2,
            full_shape=[8, 4],
        )
        self.assertNotEqual(rc, 0)
        self.assertEqual(self.store.is_exist(f"{key}_tp_0"), 0)
        self.assertEqual(self.store.is_exist(f"{key}_tp_0_meta"), 0)
        self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 0)

    def test_put_tensor_chunk_with_tp_writes_metadata_and_shard(self):
        key = "chunk_test_single"
        tensor = torch.arange(32, dtype=torch.float32).reshape(8, 4)
        tp_size = 4
        split_dim = 0
        chunks = tensor.chunk(tp_size, split_dim)

        for rank, chunk in enumerate(chunks):
            rc = self.store.put_tensor_chunk_with_tp(
                key,
                chunk,
                tp_rank=rank,
                tp_size=tp_size,
                split_dim=split_dim,
                full_shape=list(tensor.shape),
            )
            self.assertEqual(rc, 0)

            shard = self.store.get_tensor(f"{key}_tp_{rank}")
            self.assertTrue(torch.equal(shard, chunk))
            self.assertEqual(self.store.is_exist(f"{key}_tp_{rank}_meta"), 1)

        self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 1)

        chunk_meta = self.store.get(f"{key}_tp_2_meta")
        self.assertEqual(len(chunk_meta), 16)
        start_idx = int.from_bytes(chunk_meta[:8], byteorder="little", signed=True)
        shard_size = int.from_bytes(chunk_meta[8:16], byteorder="little", signed=True)
        self.assertEqual(start_idx, 4)
        self.assertEqual(shard_size, 2)

        global_meta = self.store.get(f"{key}_global_meta")
        self.assertEqual(len(global_meta), 48)
        split_dim_meta = int.from_bytes(global_meta[8:12], byteorder="little", signed=True)
        put_tp_size = int.from_bytes(global_meta[12:16], byteorder="little", signed=True)
        dim0 = int.from_bytes(global_meta[16:24], byteorder="little", signed=True)
        dim1 = int.from_bytes(global_meta[24:32], byteorder="little", signed=True)
        self.assertEqual(split_dim_meta, split_dim)
        self.assertEqual(put_tp_size, tp_size)
        self.assertEqual((dim0, dim1), tensor.shape)

        fast_path = self.store.get_tensor_with_tp(
            key, tp_rank=2, tp_size=tp_size, split_dim=split_dim
        )
        self.assertTrue(torch.equal(fast_path, chunks[2]))

    def test_batch_put_tensor_chunk_with_tp_writes_all_metadata(self):
        keys = ["chunk_batch_0", "chunk_batch_1"]
        tensors = [
            torch.arange(24, dtype=torch.float32).reshape(6, 4),
            torch.arange(48, dtype=torch.float32).reshape(6, 8),
        ]
        tp_rank = 1
        tp_size = 2
        split_dim = 1
        chunks = [tensor.chunk(tp_size, split_dim)[tp_rank].contiguous() for tensor in tensors]
        full_shapes = [list(tensor.shape) for tensor in tensors]

        rc = self.store.batch_put_tensor_chunk_with_tp(
            keys,
            chunks,
            tp_rank=tp_rank,
            tp_size=tp_size,
            split_dim=split_dim,
            full_shapes=full_shapes,
        )
        self.assertEqual(rc, [0, 0])

        for key, chunk in zip(keys, chunks):
            shard = self.store.get_tensor(f"{key}_tp_{tp_rank}")
            self.assertTrue(torch.equal(shard, chunk))
            self.assertEqual(self.store.is_exist(f"{key}_tp_{tp_rank}_meta"), 1)
            self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 0)

    def test_rank_zero_batch_put_tensor_chunk_with_tp_writes_global_metadata(self):
        keys = ["chunk_batch_rank0_0", "chunk_batch_rank0_1"]
        tensors = [
            torch.arange(60, dtype=torch.float32).reshape(5, 12),
            torch.arange(80, dtype=torch.float32).reshape(5, 16),
        ]
        tp_rank = 0
        tp_size = 4
        split_dim = 1
        chunks = [tensor.chunk(tp_size, split_dim)[tp_rank].contiguous() for tensor in tensors]
        full_shapes = [list(tensor.shape) for tensor in tensors]

        rc = self.store.batch_put_tensor_chunk_with_tp(
            keys,
            chunks,
            tp_rank=tp_rank,
            tp_size=tp_size,
            split_dim=split_dim,
            full_shapes=full_shapes,
        )
        self.assertEqual(rc, [0, 0])

        for key in keys:
            self.assertEqual(self.store.is_exist(f"{key}_tp_{tp_rank}_meta"), 1)
            self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 1)

    def test_put_tensor_chunk_with_tp_from_stores_serialized_shard(self):
        key = "chunk_from_single"
        full_tensor = torch.arange(30, dtype=torch.float32).reshape(5, 6)
        tp_rank = 1
        tp_size = 3
        split_dim = 1
        chunk = full_tensor.chunk(tp_size, split_dim)[tp_rank].contiguous()

        seed_key = f"{key}_seed"
        self.assertEqual(self.store.put_tensor(seed_key, chunk), 0)
        size = serialized_tensor_size(chunk)
        payload = (ctypes.c_ubyte * size)()
        addr = ctypes.addressof(payload)
        self.assertEqual(self.store.register_buffer(addr, size), 0)
        copied = self.store.get_tensor_into(seed_key, addr, size)
        self.assertIsNotNone(copied)
        try:
            rc = self.store.put_tensor_chunk_with_tp_from(
                key,
                addr,
                size,
                tp_rank=tp_rank,
                tp_size=tp_size,
                split_dim=split_dim,
                full_shape=list(full_tensor.shape),
            )
            self.assertEqual(rc, 0)
        finally:
            self.store.unregister_buffer(addr)

        shard = self.store.get_tensor(f"{key}_tp_{tp_rank}")
        self.assertTrue(torch.equal(shard, chunk))
        self.assertEqual(self.store.is_exist(f"{key}_tp_{tp_rank}_meta"), 1)

    def test_batch_put_tensor_chunk_with_tp_from_writes_metadata(self):
        keys = ["chunk_from_batch_0", "chunk_from_batch_1"]
        full_tensors = [
            torch.arange(24, dtype=torch.float32).reshape(6, 4),
            torch.arange(36, dtype=torch.float32).reshape(6, 6),
        ]
        tp_rank = 0
        tp_size = 2
        split_dim = 0
        chunks = [tensor.chunk(tp_size, split_dim)[tp_rank].contiguous() for tensor in full_tensors]
        seed_keys = [f"{key}_seed" for key in keys]
        sizes = [serialized_tensor_size(chunk) for chunk in chunks]
        payloads = [(ctypes.c_ubyte * size)() for size in sizes]
        ptrs = [ctypes.addressof(payload) for payload in payloads]
        full_shapes = [list(tensor.shape) for tensor in full_tensors]

        for seed_key, chunk in zip(seed_keys, chunks):
            self.assertEqual(self.store.put_tensor(seed_key, chunk), 0)
        for ptr, size, seed_key in zip(ptrs, sizes, seed_keys):
            self.assertEqual(self.store.register_buffer(ptr, size), 0)
            copied = self.store.get_tensor_into(seed_key, ptr, size)
            self.assertIsNotNone(copied)
        try:
            rc = self.store.batch_put_tensor_chunk_with_tp_from(
                keys,
                ptrs,
                sizes,
                tp_rank=tp_rank,
                tp_size=tp_size,
                split_dim=split_dim,
                full_shapes=full_shapes,
            )
            self.assertEqual(rc, [0, 0])
        finally:
            for ptr in ptrs:
                self.store.unregister_buffer(ptr)

        for key, chunk in zip(keys, chunks):
            shard = self.store.get_tensor(f"{key}_tp_{tp_rank}")
            self.assertTrue(torch.equal(shard, chunk))
            self.assertEqual(self.store.is_exist(f"{key}_tp_{tp_rank}_meta"), 1)
            self.assertEqual(self.store.is_exist(f"{key}_global_meta"), 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", help="Run tests with verbose output")
    args, remaining = parser.parse_known_args()

    sys.argv = [sys.argv[0]] + remaining
    if args.verbose:
        unittest.main(verbosity=2)
    else:
        unittest.main()
