"""
Comprehensive upsert API test suite.

Covers all upsert Python interfaces across three dimensions:
  - tensor / non-tensor (raw bytes)
  - zero-copy / non-zero-copy
  - single key / batch

Requires:
  - A running mooncake_master
  - PyTorch installed
  - mooncake.store Python bindings built

Usage:
  # Start master first:
  ./build/mooncake-store/src/mooncake_master \
    --rpc_port=50051 \
    --enable_http_metadata_server=true \
    --http_metadata_server_port=18080 \
    --alsologtostderr

  # Then run tests (configure via env vars or use defaults):
  python scripts/test_upsert_api.py -v

  # Or with custom addresses:
  MOONCAKE_MASTER=127.0.0.1:50051 python scripts/test_upsert_api.py -v
"""

import ctypes
import os
import sys
import time
import unittest
from dataclasses import dataclass

import torch
import numpy as np
import mooncake.store as mooncake_store

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

# ==========================================
#  Constants
# ==========================================

# Must match current C++ TensorMetadata layout.
# TensorObjectHeader (40) + TensorLayoutMetadata (264) = 304 bytes.
TENSOR_METADATA_SIZE = 304


def serialized_tensor_size(tensor):
    """Size of [TensorObjectHeader+layout metadata][tensor data]."""
    return TENSOR_METADATA_SIZE + tensor.numel() * tensor.element_size()


# ==========================================
#  Global Store Connection
# ==========================================

GLOBAL_STORE = None


def setUpModule():
    global GLOBAL_STORE
    master_addr = os.getenv("MOONCAKE_MASTER", "127.0.0.1:50051")
    metadata_server = os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE")
    protocol = os.getenv("MOONCAKE_PROTOCOL", "tcp")
    device_name = os.getenv("MOONCAKE_DEVICE", "")
    local_hostname = os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost")
    segment_size = int(os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE",
                                  str(64 * 1024 * 1024)))  # 64 MB default
    buffer_size = int(os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE",
                                 str(64 * 1024 * 1024)))   # 64 MB default

    print(f"\n[{os.getpid()}] Connecting to master at {master_addr} "
          f"({protocol})...")
    store = MooncakeDistributedStore()
    rc = store.setup(
        local_hostname,
        metadata_server,
        segment_size,
        buffer_size,
        protocol,
        device_name,
        master_addr,
    )
    if rc != 0:
        raise RuntimeError(f"setup failed: {rc}")
    GLOBAL_STORE = store
    print("Connected.\n")


def tearDownModule():
    global GLOBAL_STORE
    if GLOBAL_STORE:
        GLOBAL_STORE.close()
        GLOBAL_STORE = None


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


@dataclass
class WriterPartition:
    rank: int
    size: int
    split_dim: int


def make_writer_partition(rank, size, split_dim):
    return WriterPartition(rank=rank, size=size, split_dim=split_dim)


class UpsertTestBase(unittest.TestCase):
    """Base class that provides self.store and cleans up before each test."""

    def setUp(self):
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")
        self.store = GLOBAL_STORE
        self.store.remove_all()


# ==========================================
#  1. Raw Bytes — Non-Zero-Copy
# ==========================================

class TestUpsertRawBytes(UpsertTestBase):
    """upsert(key, bytes) — copy semantics, single key."""

    def test_case_a_new_key(self):
        """Upsert a key that does not exist (Case A)."""
        data = b"hello_upsert_case_a!"
        rc = self.store.upsert("raw_a", data)
        self.assertEqual(rc, 0)
        got = self.store.get("raw_a")
        self.assertEqual(got, data)

    def test_case_b_same_size(self):
        """Upsert existing key with same size (Case B — in-place)."""
        data_v1 = b"AAAAAAAAAAAAAAAA"  # 16 bytes
        data_v2 = b"BBBBBBBBBBBBBBBB"  # 16 bytes
        self.assertEqual(self.store.put("raw_b", data_v1), 0)
        self.assertEqual(self.store.upsert("raw_b", data_v2), 0)
        self.assertEqual(self.store.get("raw_b"), data_v2)

    def test_case_c_different_size(self):
        """Upsert existing key with different size (Case C — reallocate)."""
        data_short = b"short"
        data_long = b"this_is_a_longer_value!"
        self.assertEqual(self.store.put("raw_c", data_short), 0)
        self.assertEqual(self.store.upsert("raw_c", data_long), 0)
        self.assertEqual(self.store.get("raw_c"), data_long)


class TestUpsertParts(UpsertTestBase):
    """upsert_parts(key, *parts) — multi-part copy, single key."""

    def test_basic(self):
        """Upsert from multiple byte parts."""
        p1, p2, p3 = b"Hello, ", b"World", b"!"
        rc = self.store.upsert_parts("parts_basic", p1, p2, p3)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.get("parts_basic"), p1 + p2 + p3)

    def test_overwrite_same_size(self):
        """Upsert parts over existing key with same total size."""
        self.store.put("parts_ow", b"1234567890123")  # 13 bytes
        rc = self.store.upsert_parts("parts_ow", b"Hello, ", b"World!")  # 13 bytes
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.get("parts_ow"), b"Hello, World!")


class TestUpsertBatch(UpsertTestBase):
    """upsert_batch(keys, values) — copy semantics, batch."""

    def test_basic(self):
        keys = ["batch_0", "batch_1", "batch_2"]
        vals = [b"value_zero", b"value__one", b"value__two"]
        rc = self.store.upsert_batch(keys, vals)
        self.assertEqual(rc, 0)
        for k, v in zip(keys, vals):
            self.assertEqual(self.store.get(k), v)

    def test_overwrite(self):
        """Batch upsert over existing keys."""
        keys = ["batchow_0", "batchow_1"]
        self.store.put("batchow_0", b"old_0___________")
        self.store.put("batchow_1", b"old_1___________")
        new_vals = [b"new_0___________", b"new_1___________"]
        rc = self.store.upsert_batch(keys, new_vals)
        self.assertEqual(rc, 0)
        for k, v in zip(keys, new_vals):
            self.assertEqual(self.store.get(k), v)


# ==========================================
#  2. Raw Bytes — Zero-Copy
# ==========================================

class TestUpsertFrom(UpsertTestBase):
    """upsert_from(key, buffer_ptr, size) — zero-copy, single key.

    upsert_from expects an integer buffer pointer (registered memory),
    not bytes. We use numpy arrays as registered buffers.
    """

    def _make_registered_buffer(self, data: bytes):
        """Create a numpy buffer with data and register it."""
        buf = np.frombuffer(data, dtype=np.uint8).copy()
        ptr = buf.ctypes.data
        self.store.register_buffer(ptr, len(buf))
        return buf, ptr

    def test_basic(self):
        data = b"zero_copy_upsert_from_!!"  # 24 bytes
        buf, ptr = self._make_registered_buffer(data)
        rc = self.store.upsert_from("from_basic", ptr, len(data))
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.get("from_basic"), data)
        self.store.unregister_buffer(ptr)

    def test_overwrite_same_size(self):
        v1 = b"XXXXXXXXXXXXXXXX"
        v2 = b"YYYYYYYYYYYYYYYY"
        self.store.put("from_ow", v1)
        buf, ptr = self._make_registered_buffer(v2)
        rc = self.store.upsert_from("from_ow", ptr, len(v2))
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.get("from_ow"), v2)
        self.store.unregister_buffer(ptr)


class TestBatchUpsertFrom(UpsertTestBase):
    """batch_upsert_from(keys, buffer_ptrs, sizes) — zero-copy, batch.

    Requires integer buffer pointers, not bytes objects.
    """

    def _make_registered_buffers(self, data_list):
        """Create registered numpy buffers for each bytes object."""
        bufs, ptrs, sizes = [], [], []
        for data in data_list:
            buf = np.frombuffer(data, dtype=np.uint8).copy()
            ptr = buf.ctypes.data
            self.store.register_buffer(ptr, len(buf))
            bufs.append(buf)
            ptrs.append(ptr)
            sizes.append(len(buf))
        return bufs, ptrs, sizes

    def test_basic(self):
        keys = ["bfrom_0", "bfrom_1", "bfrom_2"]
        vals = [b"data_zero_______", b"data_one________", b"data_two________"]
        bufs, ptrs, sizes = self._make_registered_buffers(vals)
        rets = self.store.batch_upsert_from(keys, ptrs, sizes)
        self.assertEqual(list(rets), [0, 0, 0])
        for k, v in zip(keys, vals):
            self.assertEqual(self.store.get(k), v)
        for ptr in ptrs:
            self.store.unregister_buffer(ptr)

    def test_overwrite(self):
        keys = ["bfromow_0", "bfromow_1"]
        old = [b"old_data_0______", b"old_data_1______"]
        new = [b"NEW_data_0______", b"NEW_data_1______"]
        for k, v in zip(keys, old):
            self.store.put(k, v)
        bufs, ptrs, sizes = self._make_registered_buffers(new)
        rets = self.store.batch_upsert_from(keys, ptrs, sizes)
        self.assertEqual(list(rets), [0, 0])
        for k, v in zip(keys, new):
            self.assertEqual(self.store.get(k), v)
        for ptr in ptrs:
            self.store.unregister_buffer(ptr)


# ==========================================
#  3. Tensor — Non-Zero-Copy
# ==========================================

class TestUpsertTensor(UpsertTestBase):
    """upsert_tensor(key, tensor) — tensor copy, single key."""

    def test_new_key(self):
        t = torch.randn(50, 50, dtype=torch.float32).contiguous()
        rc = self.store.upsert_tensor("tensor_new", t)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("tensor_new")
        self.assertIsNotNone(got)
        self.assertTrue(torch.equal(t, got))

    def test_overwrite_same_shape(self):
        """Case B: same size tensor overwrite."""
        t1 = torch.ones(30, 30, dtype=torch.float32).contiguous()
        t2 = torch.zeros(30, 30, dtype=torch.float32).contiguous()
        self.store.put_tensor("tensor_ow", t1)
        rc = self.store.upsert_tensor("tensor_ow", t2)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("tensor_ow")
        self.assertTrue(torch.equal(t2, got))

    def test_overwrite_different_shape(self):
        """Case C: different size tensor overwrite."""
        t1 = torch.randn(10, 10, dtype=torch.float32).contiguous()
        t2 = torch.randn(20, 20, dtype=torch.float32).contiguous()
        self.store.put_tensor("tensor_diff", t1)
        rc = self.store.upsert_tensor("tensor_diff", t2)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("tensor_diff")
        self.assertTrue(torch.equal(t2, got))

    def test_multiple_dtypes(self):
        """Verify upsert works for different tensor dtypes."""
        dtypes = [torch.float32, torch.float64, torch.int32, torch.int8]
        for dt in dtypes:
            key = f"tensor_dtype_{dt}"
            t = torch.ones(100, dtype=dt).contiguous()
            rc = self.store.upsert_tensor(key, t)
            self.assertEqual(rc, 0, f"upsert failed for {dt}")
            got = self.store.get_tensor(key)
            self.assertIsNotNone(got, f"get_tensor returned None for {dt}")
            self.assertTrue(torch.equal(t, got), f"data mismatch for {dt}")


class TestBatchUpsertTensor(UpsertTestBase):
    """batch_upsert_tensor(keys, tensors) — tensor copy, batch."""

    def test_basic(self):
        keys = ["btensor_0", "btensor_1", "btensor_2"]
        tensors = [torch.randn(20, 20, dtype=torch.float32).contiguous()
                   for _ in range(3)]
        rets = self.store.batch_upsert_tensor(keys, tensors)
        self.assertEqual(list(rets), [0, 0, 0])
        for k, t in zip(keys, tensors):
            got = self.store.get_tensor(k)
            self.assertIsNotNone(got)
            self.assertTrue(torch.equal(t, got))

    def test_overwrite(self):
        keys = ["btensor_ow_0", "btensor_ow_1"]
        old = [torch.ones(10, 10).contiguous(), torch.ones(10, 10).contiguous()]
        new = [torch.zeros(10, 10).contiguous(), torch.zeros(10, 10).contiguous()]
        self.store.batch_put_tensor(keys, old)
        rets = self.store.batch_upsert_tensor(keys, new)
        self.assertEqual(list(rets), [0, 0])
        for k, t in zip(keys, new):
            got = self.store.get_tensor(k)
            self.assertTrue(torch.equal(t, got))


# ==========================================
#  4. Tensor — Zero-Copy
# ==========================================

class TestUpsertTensorFrom(UpsertTestBase):
    """upsert_tensor_from(key, buffer_ptr, size) — tensor zero-copy, single key."""

    def _alloc_and_register(self, size):
        """Allocate a registered buffer using ctypes."""
        buf = ctypes.create_string_buffer(size)
        ptr = ctypes.addressof(buf)
        rc = self.store.register_buffer(ptr, size)
        self.assertEqual(rc, 0, "register_buffer failed")
        return buf, ptr

    def test_put_then_upsert_via_buffer(self):
        """Put a tensor, get_tensor_into a buffer, modify, upsert_tensor_from."""
        key = "tensor_from_rw"
        original = torch.ones(100, dtype=torch.float32).contiguous()
        self.assertEqual(self.store.put_tensor(key, original), 0)

        buf_size = serialized_tensor_size(original)
        buf, ptr = self._alloc_and_register(buf_size)

        # get_tensor_into → buffer now has [metadata | data]
        got = self.store.get_tensor_into(key, ptr, buf_size)
        self.assertIsNotNone(got)
        self.assertTrue(torch.equal(original, got))

        # Modify the tensor in-place (shares memory with buffer)
        got.fill_(42.0)

        # upsert_tensor_from — writes modified buffer back, zero-copy
        rc = self.store.upsert_tensor_from(key, ptr, buf_size)
        self.assertEqual(rc, 0)

        # Verify
        result = self.store.get_tensor(key)
        self.assertIsNotNone(result)
        expected = torch.full((100,), 42.0, dtype=torch.float32)
        self.assertTrue(torch.equal(result, expected))

        self.store.unregister_buffer(ptr)


class TestBatchUpsertTensorFrom(UpsertTestBase):
    """batch_upsert_tensor_from(keys, ptrs, sizes) — tensor zero-copy, batch."""

    def _alloc_and_register(self, size):
        buf = ctypes.create_string_buffer(size)
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, size), 0)
        return buf, ptr

    def test_basic(self):
        keys = ["btfrom_0", "btfrom_1"]
        tensors = [torch.randn(50, dtype=torch.float32).contiguous(),
                   torch.randn(50, dtype=torch.float32).contiguous()]

        # Put initial tensors
        for k, t in zip(keys, tensors):
            self.assertEqual(self.store.put_tensor(k, t), 0)

        # Allocate buffers, get_tensor_into each
        bufs = []
        ptrs = []
        sizes = []
        for k, t in zip(keys, tensors):
            sz = serialized_tensor_size(t)
            buf, ptr = self._alloc_and_register(sz)
            got = self.store.get_tensor_into(k, ptr, sz)
            self.assertIsNotNone(got)
            # Modify in-place
            got.fill_(99.0)
            bufs.append(buf)
            ptrs.append(ptr)
            sizes.append(sz)

        # batch_upsert_tensor_from
        rets = self.store.batch_upsert_tensor_from(keys, ptrs, sizes)
        self.assertEqual(list(rets), [0, 0])

        # Verify
        for k in keys:
            result = self.store.get_tensor(k)
            self.assertIsNotNone(result)
            expected = torch.full((50,), 99.0, dtype=torch.float32)
            self.assertTrue(torch.equal(result, expected))

        for ptr in ptrs:
            self.store.unregister_buffer(ptr)


# ==========================================
#  5. Tensor with ReplicateConfig (pub variants)
# ==========================================

class TestUpsertPubTensor(UpsertTestBase):
    """upsert_pub_tensor / batch_upsert_pub_tensor — with ReplicateConfig."""

    def test_single(self):
        config = ReplicateConfig()
        config.replica_num = 1
        t = torch.randn(25, 25, dtype=torch.float32).contiguous()
        rc = self.store.upsert_pub_tensor("pub_single", t, config)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("pub_single")
        self.assertTrue(torch.equal(t, got))

    def test_batch(self):
        config = ReplicateConfig()
        config.replica_num = 1
        keys = ["pub_batch_0", "pub_batch_1"]
        tensors = [torch.randn(15, 15).contiguous(),
                   torch.randn(15, 15).contiguous()]
        rets = self.store.batch_upsert_pub_tensor(keys, tensors, config)
        self.assertEqual(list(rets), [0, 0])
        for k, t in zip(keys, tensors):
            got = self.store.get_tensor(k)
            self.assertTrue(torch.equal(t, got))

    def test_overwrite_with_config(self):
        """Upsert pub tensor over existing key."""
        config = ReplicateConfig()
        config.replica_num = 1
        t1 = torch.ones(20, 20).contiguous()
        t2 = torch.zeros(20, 20).contiguous()
        self.store.put_tensor("pub_ow", t1)
        rc = self.store.upsert_pub_tensor("pub_ow", t2, config)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("pub_ow")
        self.assertTrue(torch.equal(t2, got))


class TestUnifiedParallelismUpsert(UpsertTestBase):
    def test_put_tensor_with_parallelism_rejects_nonuniform_multi_axis(self):
        require_unified_parallelism_api(self)
        key = "unified_nonuniform_multi_axis_reject"
        tensor = torch.arange(6, dtype=torch.float32).view(2, 3).contiguous()
        parallelism = make_tensor_parallelism([
            make_parallel_axis("dp", rank=0, size=2),
            make_parallel_axis("tp", rank=0, size=2, split_dim=1),
        ])

        rc = self.store.put_tensor_with_parallelism(key, tensor, parallelism)
        self.assertNotEqual(rc, 0)
        self.assertIsNone(
            self.store.get_tensor_with_parallelism(key, make_read_target("shard", parallelism))
        )

    def test_put_tensor_with_parallelism_from_rejects_nonuniform_multi_axis(self):
        require_unified_parallelism_api(self)
        seed_key = "unified_nonuniform_multi_axis_from_seed"
        key = "unified_nonuniform_multi_axis_from_reject"
        tensor = torch.arange(6, dtype=torch.float32).view(2, 3).contiguous()
        self.assertEqual(self.store.put_tensor(seed_key, tensor), 0)

        buffer_size = serialized_tensor_size(tensor)
        buf = ctypes.create_string_buffer(buffer_size)
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, buffer_size), 0)
        try:
            got = self.store.get_tensor_into(seed_key, ptr, buffer_size)
            self.assertIsNotNone(got)
            parallelism = make_tensor_parallelism([
                make_parallel_axis("dp", rank=1, size=2),
                make_parallel_axis("tp", rank=1, size=2, split_dim=1),
            ])
            rc = self.store.put_tensor_with_parallelism_from(
                key, ptr, buffer_size, parallelism
            )
            self.assertNotEqual(rc, 0)
            self.assertIsNone(
                self.store.get_tensor_with_parallelism(
                    key, make_read_target("shard", parallelism)
                )
            )
        finally:
            self.store.unregister_buffer(ptr)

    def test_put_tensor_with_writer_partition_rejects_nonuniform(self):
        require_unified_parallelism_api(self)
        key = "writer_partition_nonuniform_reject"
        tensor = torch.arange(6, dtype=torch.float32).view(2, 3).contiguous()

        rc = self.store.put_tensor_with_parallelism(
            key,
            tensor,
            writer_partition=make_writer_partition(rank=0, size=2, split_dim=1),
        )
        self.assertNotEqual(rc, 0)
        self.assertIsNone(self.store.get_tensor_with_parallelism(key, make_read_target("full")))

    def test_batch_put_tensor_with_parallelism_rejects_nonuniform_and_preserves_uniform_data(self):
        require_unified_parallelism_api(self)
        keys = ["uniform_batch_ok", "nonuniform_batch_reject"]
        tensors = [
            torch.arange(8, dtype=torch.float32).view(2, 4).contiguous(),
            torch.arange(6, dtype=torch.float32).view(2, 3).contiguous(),
        ]
        parallelisms = [
            make_tensor_parallelism([
                make_parallel_axis("dp", rank=0, size=2),
                make_parallel_axis("tp", rank=1, size=2, split_dim=1),
            ]),
            make_tensor_parallelism([
                make_parallel_axis("dp", rank=1, size=2),
                make_parallel_axis("tp", rank=0, size=4, split_dim=1),
            ]),
        ]

        results = list(
            self.store.batch_put_tensor_with_parallelism(keys, tensors, parallelisms)
        )
        self.assertEqual(results[0], 0)
        self.assertNotEqual(results[1], 0)

        got_uniform = self.store.get_tensor_with_parallelism(
            keys[0], make_read_target("shard", parallelisms[0])
        )
        self.assertIsNotNone(got_uniform)
        self.assertTrue(torch.equal(got_uniform, tensors[0].chunk(2, 1)[1]))
        self.assertIsNone(
            self.store.get_tensor_with_parallelism(
                keys[1], make_read_target("shard", parallelisms[1])
            )
        )

    def test_upsert_tensor_with_parallelism_multi_axis(self):
        require_unified_parallelism_api(self)
        key = "unified_multi_axis_upsert"
        original = torch.arange(8, dtype=torch.float32).view(2, 4).contiguous()
        updated = (original + 50).contiguous()

        dp_axis = make_parallel_axis("dp", rank=1, size=2)
        tp_axis = make_parallel_axis("tp", rank=0, size=2, split_dim=1)
        parallelism = make_tensor_parallelism([dp_axis, tp_axis])

        for rank in range(2):
            rank_parallelism = clone_parallelism(parallelism)
            rank_parallelism.axes[1].rank = rank
            self.assertEqual(
                self.store.put_tensor_with_parallelism(key, original, rank_parallelism),
                0,
            )
            self.assertEqual(
                self.store.upsert_tensor_with_parallelism(key, updated, rank_parallelism),
                0,
            )

        target = make_read_target("shard", parallelism)
        got = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(got)
        self.assertTrue(torch.equal(got, updated.chunk(2, 1)[0]))

    def test_batch_upsert_tensor_with_parallelism_multi_axis(self):
        require_unified_parallelism_api(self)
        keys = ["unified_multi_axis_batch_upsert_0", "unified_multi_axis_batch_upsert_1"]
        originals = [
            torch.arange(8, dtype=torch.float32).view(2, 4).contiguous(),
            torch.arange(8, 16, dtype=torch.float32).view(4, 2).contiguous(),
        ]
        updates = [(tensor + 100).contiguous() for tensor in originals]

        base_parallelisms = []
        dp_tp = make_tensor_parallelism([
            make_parallel_axis("dp", rank=1, size=2),
            make_parallel_axis("tp", rank=1, size=2, split_dim=1),
        ])
        pp_tp = make_tensor_parallelism([
            make_parallel_axis("pp", rank=0, size=2),
            make_parallel_axis("tp", rank=1, size=2, split_dim=0),
        ])
        pp_tp.axes[0].stage_id = 3
        base_parallelisms.extend([dp_tp, pp_tp])

        rank_parallelisms = []
        for base_parallelism in base_parallelisms:
            tp_axis = next(axis for axis in base_parallelism.axes if axis.kind == "tp")
            for rank in range(tp_axis.size):
                parallelism = clone_parallelism(base_parallelism)
                for axis in parallelism.axes:
                    if axis.kind == "tp":
                        axis.rank = rank
                        break
                rank_parallelisms.append(parallelism)
        batch_keys = [keys[0]] * 2 + [keys[1]] * 2
        batch_originals = [originals[0]] * 2 + [originals[1]] * 2
        batch_updates = [updates[0]] * 2 + [updates[1]] * 2

        self.assertEqual(
            list(self.store.batch_put_tensor_with_parallelism(batch_keys, batch_originals, rank_parallelisms)),
            [0, 0, 0, 0],
        )
        self.assertEqual(
            list(self.store.batch_upsert_tensor_with_parallelism(batch_keys, batch_updates, rank_parallelisms)),
            [0, 0, 0, 0],
        )
        invalid_results = list(
            self.store.batch_put_tensor_with_parallelism(
                keys,
                originals,
                parallelisms=base_parallelisms,
                writer_partitions=[
                    make_writer_partition(0, 2, 0),
                    make_writer_partition(1, 2, 0),
                ],
            )
        )
        self.assertTrue(
            all(result != 0 for result in invalid_results),
            f"expected invalid combined routing request: {invalid_results}",
        )

        targets = []
        for parallelism in base_parallelisms:
            target = mooncake_store.ReadTarget()
            target.mode = "shard"
            target.parallelism = parallelism
            targets.append(target)
        results = self.store.batch_get_tensor_with_parallelism(keys, targets)
        self.assertEqual(len(results), 2)
        self.assertTrue(torch.equal(results[0], updates[0].chunk(2, 1)[1]))
        self.assertTrue(torch.equal(results[1], updates[1].chunk(2, 0)[1]))

    def test_upsert_tensor_with_parallelism_from_multi_axis(self):
        require_unified_parallelism_api(self)
        key = "unified_multi_axis_upsert_from"
        seed_key = "unified_multi_axis_upsert_from_seed"
        tensor = torch.arange(8, dtype=torch.float32).view(2, 4).contiguous()
        updated = (tensor + 77).contiguous()

        self.assertEqual(self.store.put_tensor(seed_key, updated), 0)
        buffer_size = serialized_tensor_size(updated)
        buf = ctypes.create_string_buffer(buffer_size)
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, buffer_size), 0)
        try:
            got = self.store.get_tensor_into(seed_key, ptr, buffer_size)
            self.assertIsNotNone(got)

            dp_axis = make_parallel_axis("dp", rank=0, size=2)
            tp_axis = make_parallel_axis("tp", rank=1, size=2, split_dim=1)
            parallelism = make_tensor_parallelism([dp_axis, tp_axis])

            for rank in range(2):
                rank_parallelism = clone_parallelism(parallelism)
                rank_parallelism.axes[1].rank = rank
                self.assertEqual(
                    self.store.upsert_tensor_with_parallelism_from(
                        key, ptr, buffer_size, rank_parallelism
                    ),
                    0,
                )

            target = mooncake_store.ReadTarget()
            target.mode = "shard"
            target.parallelism = parallelism
            result = self.store.get_tensor_with_parallelism(key, target)
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, updated.chunk(2, 1)[1]))
        finally:
            self.store.unregister_buffer(ptr)

    def test_upsert_tensor_with_writer_partition(self):
        require_unified_parallelism_api(self)
        key = "writer_partition_upsert"
        original = torch.arange(8, dtype=torch.float32).view(2, 4).contiguous()
        updated = (original + 33).contiguous()

        for rank in range(2):
            self.assertEqual(
                self.store.put_tensor_with_parallelism(
                    key,
                    original,
                    writer_partition=make_writer_partition(rank, 2, 1),
                ),
                0,
            )
        for rank in range(2):
            self.assertEqual(
                self.store.upsert_tensor_with_parallelism(
                    key,
                    updated,
                    writer_partition=make_writer_partition(rank, 2, 1),
                ),
                0,
            )

        target = make_read_target("full")
        got = self.store.get_tensor_with_parallelism(key, target)
        self.assertIsNotNone(got)
        self.assertTrue(torch.equal(got, updated))

    def test_upsert_tensor_with_writer_partition_from(self):
        require_unified_parallelism_api(self)
        key = "writer_partition_upsert_from"
        seed_key = "writer_partition_upsert_from_seed"
        tensor = torch.arange(16, dtype=torch.float32).view(4, 4).contiguous()

        self.assertEqual(self.store.put_tensor(seed_key, tensor), 0)
        for rank in range(4):
            self.assertEqual(
                self.store.put_tensor_with_parallelism(
                    key,
                    tensor,
                    writer_partition=make_writer_partition(rank, 4, 0),
                ),
                0,
            )
        buffer_size = serialized_tensor_size(tensor)
        buf = ctypes.create_string_buffer(buffer_size)
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, buffer_size), 0)
        try:
            got = self.store.get_tensor_into(seed_key, ptr, buffer_size)
            self.assertIsNotNone(got)

            for rank in range(4):
                self.assertEqual(
                    self.store.upsert_tensor_with_parallelism_from(
                        key,
                        ptr,
                        buffer_size,
                        writer_partition=make_writer_partition(rank, 4, 0),
                    ),
                    0,
                )

            target = mooncake_store.ReadTarget()
            target.mode = "full"
            target.parallelism = None
            result = self.store.get_tensor_with_parallelism(key, target)
            self.assertIsNotNone(result)
            self.assertTrue(torch.equal(result, tensor))
        finally:
            self.store.unregister_buffer(ptr)

    def test_batch_upsert_tensor_with_writer_partitions(self):
        require_unified_parallelism_api(self)
        keys = ["writer_partition_batch_upsert_0", "writer_partition_batch_upsert_1"]
        originals = [
            torch.arange(8, dtype=torch.float32).view(2, 4).contiguous(),
            torch.arange(8, 16, dtype=torch.float32).view(2, 4).contiguous(),
        ]
        updates = [(tensor + 100).contiguous() for tensor in originals]

        for rank in range(2):
            writer_partitions = [
                make_writer_partition(rank=rank, size=2, split_dim=1),
                make_writer_partition(rank=rank, size=2, split_dim=1),
            ]
            self.assertEqual(
                list(
                    self.store.batch_put_tensor_with_parallelism(
                        keys,
                        originals,
                        writer_partitions=writer_partitions,
                    )
                ),
                [0, 0],
            )
            self.assertEqual(
                list(
                    self.store.batch_upsert_tensor_with_parallelism(
                        keys,
                        updates,
                        writer_partitions=writer_partitions,
                    )
                ),
                [0, 0],
            )

        results = self.store.batch_get_tensor_with_parallelism(
            keys, [make_read_target("full"), make_read_target("full")]
        )
        self.assertEqual(len(results), 2)
        self.assertTrue(torch.equal(results[0], updates[0]))
        self.assertTrue(torch.equal(results[1], updates[1]))

    def test_batch_upsert_tensor_with_writer_partitions_from(self):
        require_unified_parallelism_api(self)
        keys = ["writer_partition_batch_upsert_from_0", "writer_partition_batch_upsert_from_1"]
        seed_keys = [f"{key}_seed" for key in keys]
        tensors = [
            torch.arange(16, dtype=torch.float32).view(4, 4).contiguous(),
            torch.arange(16, 32, dtype=torch.float32).view(4, 4).contiguous(),
        ]

        for seed_key, tensor in zip(seed_keys, tensors):
            self.assertEqual(self.store.put_tensor(seed_key, tensor), 0)

        for rank in range(4):
            writer_partitions = [
                make_writer_partition(rank=rank, size=4, split_dim=0),
                make_writer_partition(rank=rank, size=4, split_dim=0),
            ]
            self.assertEqual(
                list(
                    self.store.batch_put_tensor_with_parallelism(
                        keys,
                        tensors,
                        writer_partitions=writer_partitions,
                    )
                ),
                [0, 0],
            )

        buffer_sizes = [serialized_tensor_size(tensor) for tensor in tensors]
        backing_buffer = ctypes.create_string_buffer(sum(buffer_sizes))
        base_ptr = ctypes.addressof(backing_buffer)
        buffer_ptrs = []
        offset = 0
        for size in buffer_sizes:
            buffer_ptrs.append(base_ptr + offset)
            offset += size
        self.assertEqual(self.store.register_buffer(base_ptr, len(backing_buffer)), 0)
        try:
            for seed_key, ptr, size in zip(seed_keys, buffer_ptrs, buffer_sizes):
                got = self.store.get_tensor_into(seed_key, ptr, size)
                self.assertIsNotNone(got)

            for rank in range(4):
                writer_partitions = [
                    make_writer_partition(rank=rank, size=4, split_dim=0),
                    make_writer_partition(rank=rank, size=4, split_dim=0),
                ]
                self.assertEqual(
                    list(
                        self.store.batch_upsert_tensor_with_parallelism_from(
                            keys,
                            buffer_ptrs,
                            buffer_sizes,
                            writer_partitions=writer_partitions,
                        )
                    ),
                    [0, 0],
                )

            results = self.store.batch_get_tensor_with_parallelism(
                keys, [make_read_target("full"), make_read_target("full")]
            )
            self.assertEqual(len(results), 2)
            self.assertTrue(torch.equal(results[0], tensors[0]))
            self.assertTrue(torch.equal(results[1], tensors[1]))
        finally:
            self.store.unregister_buffer(base_ptr)


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


class TestUpsertParallelTensor(UpsertTestBase):
    """Parallelism-based tensor upsert coverage for TP cases."""

    def test_upsert_tensor_with_parallelism_tp(self):
        require_unified_parallelism_api(self)
        tp_size = 2
        split_dim = 1
        key = "tp_upsert_single"
        original = torch.arange(6, dtype=torch.float32).view(2, 3).contiguous()
        updated = (original + 100).contiguous()
        original_shard = original.chunk(tp_size, split_dim)[0].contiguous()
        updated_shard = updated.chunk(tp_size, split_dim)[0].contiguous()
        parallelism = make_tensor_parallelism(
            [make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)]
        )
        target = make_read_target("shard", parallelism)

        self.assertEqual(
            self.store.put_tensor_with_parallelism(key, original_shard, parallelism),
            0,
        )
        self.assertEqual(
            self.store.upsert_tensor_with_parallelism(key, updated_shard, parallelism),
            0,
        )

        got = self.store.get_tensor_with_parallelism(key, target)
        self.assertTrue(torch.equal(got, updated_shard))

    def test_batch_upsert_tensor_with_parallelism_tp(self):
        require_unified_parallelism_api(self)
        tp_size = 2
        split_dim = 0
        keys = ["tp_batch_upsert_0", "tp_batch_upsert_1"]
        originals = [torch.arange(16, dtype=torch.float32).view(4, 4).contiguous(),
                     torch.arange(16, 32, dtype=torch.float32).view(4, 4).contiguous()]
        updates = [(t + 1000).contiguous() for t in originals]
        original_shards = [tensor.chunk(tp_size, split_dim)[0].contiguous() for tensor in originals]
        update_shards = [tensor.chunk(tp_size, split_dim)[0].contiguous() for tensor in updates]
        parallelism = make_tensor_parallelism(
            [make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)]
        )
        targets = [make_read_target("shard", parallelism) for _ in keys]

        self.assertEqual(
            list(self.store.batch_put_tensor_with_parallelism(keys, original_shards, [parallelism] * len(keys))),
            [0, 0],
        )
        self.assertEqual(
            list(self.store.batch_upsert_tensor_with_parallelism(keys, update_shards, [parallelism] * len(keys))),
            [0, 0],
        )

        got_shards = self.store.batch_get_tensor_with_parallelism(keys, targets)
        for got, expected in zip(got_shards, update_shards):
            self.assertTrue(torch.equal(got, expected))

    def test_upsert_tensor_with_parallelism_tp_with_config(self):
        require_unified_parallelism_api(self)
        tp_size = 2
        split_dim = 1
        key = "tp_upsert_pub_single"
        config = ReplicateConfig()
        config.replica_num = 1
        base = torch.ones(4, 6, dtype=torch.float32).contiguous()
        updated = torch.full((4, 6), 7.0, dtype=torch.float32).contiguous()
        base_shard = base.chunk(tp_size, split_dim)[0].contiguous()
        updated_shard = updated.chunk(tp_size, split_dim)[0].contiguous()
        parallelism = make_tensor_parallelism(
            [make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)]
        )
        target = make_read_target("shard", parallelism)

        self.assertEqual(
            self.store.put_tensor_with_parallelism(key, base_shard, parallelism, config=config),
            0,
        )
        self.assertEqual(
            self.store.upsert_tensor_with_parallelism(key, updated_shard, parallelism, config=config),
            0,
        )

        got = self.store.get_tensor_with_parallelism(key, target)
        self.assertTrue(torch.equal(got, updated_shard))

    def test_batch_upsert_tensor_with_parallelism_tp_with_config(self):
        require_unified_parallelism_api(self)
        tp_size = 2
        split_dim = 0
        config = ReplicateConfig()
        config.replica_num = 1
        keys = ["tp_upsert_pub_batch_0", "tp_upsert_pub_batch_1"]
        base = [torch.ones(4, 4, dtype=torch.float32).contiguous(),
                torch.full((4, 4), 2.0, dtype=torch.float32).contiguous()]
        updates = [torch.full((4, 4), 9.0, dtype=torch.float32).contiguous(),
                   torch.full((4, 4), 11.0, dtype=torch.float32).contiguous()]
        base_shards = [tensor.chunk(tp_size, split_dim)[0].contiguous() for tensor in base]
        update_shards = [tensor.chunk(tp_size, split_dim)[0].contiguous() for tensor in updates]
        parallelism = make_tensor_parallelism(
            [make_parallel_axis("tp", rank=0, size=tp_size, split_dim=split_dim)]
        )
        targets = [make_read_target("shard", parallelism) for _ in keys]

        self.assertEqual(
            list(self.store.batch_put_tensor_with_parallelism(keys, base_shards, [parallelism] * len(keys), config=config)),
            [0, 0],
        )
        self.assertEqual(
            list(self.store.batch_upsert_tensor_with_parallelism(keys, update_shards, [parallelism] * len(keys), config=config)),
            [0, 0],
        )

        got_shards = self.store.batch_get_tensor_with_parallelism(keys, targets)
        for got, expected in zip(got_shards, update_shards):
            self.assertTrue(torch.equal(got, expected))


# ==========================================
#  6. Mixed Scenarios
# ==========================================

class TestUpsertMixed(UpsertTestBase):
    """Cross-interface scenarios."""

    def test_put_then_upsert_different_interface(self):
        """Put with put_tensor, update with upsert_tensor."""
        t1 = torch.ones(40, dtype=torch.float32).contiguous()
        t2 = torch.full((40,), 7.0, dtype=torch.float32).contiguous()
        self.store.put_tensor("mixed_1", t1)
        rc = self.store.upsert_tensor("mixed_1", t2)
        self.assertEqual(rc, 0)
        got = self.store.get_tensor("mixed_1")
        self.assertTrue(torch.equal(t2, got))

    def test_upsert_raw_then_upsert_raw_different_size(self):
        """Upsert raw bytes, then upsert again with different size."""
        self.store.upsert("mixed_2", b"short")
        self.store.upsert("mixed_2", b"a much longer value!!!")
        got = self.store.get("mixed_2")
        self.assertEqual(got, b"a much longer value!!!")

    def test_batch_upsert_mixed_new_and_existing(self):
        """Batch upsert with a mix of new keys and existing keys."""
        self.store.put("mixed_exist", b"old_data________")
        keys = ["mixed_new", "mixed_exist"]
        vals = [b"brand_new_data!!", b"updated_data____"]
        # batch_upsert_from needs integer pointers, use upsert_batch instead
        rc = self.store.upsert_batch(keys, vals)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.get("mixed_new"), vals[0])
        self.assertEqual(self.store.get("mixed_exist"), vals[1])

    def test_multiple_sequential_upserts(self):
        """Upsert the same key many times."""
        key = "mixed_seq"
        for i in range(10):
            data = f"version_{i:04d}____".encode()  # 16 bytes each
            rc = self.store.upsert(key, data)
            self.assertEqual(rc, 0)
        got = self.store.get(key)
        self.assertEqual(got, b"version_0009____")


# ==========================================
#  Runner
# ==========================================

if __name__ == "__main__":
    print(">> Loading Upsert API Tests...")
    print(">> Interfaces covered:")
    print("   Raw bytes:  upsert, upsert_parts, upsert_batch,")
    print("               upsert_from, batch_upsert_from")
    print("   Tensor:     upsert_tensor, batch_upsert_tensor,")
    print("               upsert_tensor_from, batch_upsert_tensor_from,")
    print("               upsert_tensor_with_parallelism,")
    print("               batch_upsert_tensor_with_parallelism")
    print("   Pub tensor: upsert_pub_tensor, batch_upsert_pub_tensor")
    print("   Mixed:      cross-interface, sequential, size changes")
    unittest.main(verbosity=2)
