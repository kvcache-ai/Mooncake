#!/usr/bin/env python3
"""Unit and standalone E2E tests for the coarse-grained AoS Python Store API."""

from __future__ import annotations

import array
import ctypes
import os
import threading
import time
import unittest

from mooncake.store import (
    GetItem,
    GrantLeasePolicy,
    MooncakeDistributedStore,
    PartIndex,
    PartTarget,
    ProbeLeaseMode,
    PutItem,
    RangeTarget,
    ReplicateConfig,
)

try:
    import numpy as np
except Exception:  # pragma: no cover - optional in minimal test envs
    np = None

SEGMENT_SIZE = 64 * 1024 * 1024
LOCAL_BUFFER_SIZE = 32 * 1024 * 1024


def _protocol() -> str:
    return os.getenv("PROTOCOL", "tcp")


def _device_name() -> str:
    return os.getenv("DEVICE_NAME", "")


class TestAoSDescriptors(unittest.TestCase):
    """Pure Python-binding validation tests for AoS descriptors and targets."""

    def test_target_and_policy_types(self) -> None:
        rt = RangeTarget(offset=4096, total_size=8192, write_id="wid-1")
        self.assertEqual(rt.offset, 4096)
        self.assertEqual(rt.total_size, 8192)
        self.assertEqual(rt.write_id, "wid-1")
        self.assertEqual(rt, RangeTarget(4096, 8192, "wid-1"))
        self.assertIn("wid-1", repr(rt))

        pt_default = PartTarget()
        self.assertEqual(pt_default.part_index, 0)
        self.assertEqual(pt_default.part_count, 1)
        self.assertEqual(PartTarget(1, 4), PartTarget(part_index=1, part_count=4))

        pi = PartIndex(3)
        self.assertEqual(pi.index, 3)
        self.assertEqual(pi, PartIndex(index=3))

        policy_default = GrantLeasePolicy()
        self.assertEqual(policy_default.lease_mode, ProbeLeaseMode.ALL_HITS)
        self.assertEqual(policy_default.candidate_size, 1)
        policy_last = GrantLeasePolicy(ProbeLeaseMode.LAST_HIT_ONLY, 4)
        self.assertEqual(policy_last.lease_mode, ProbeLeaseMode.LAST_HIT_ONLY)
        self.assertEqual(policy_last.candidate_size, 4)
        self.assertIn("LAST_HIT_ONLY", repr(policy_last))

    def test_put_get_item_single_and_multi_buffer_forms(self) -> None:
        p1 = PutItem("k1", 0x1000, 4096)
        self.assertEqual(p1.key, "k1")
        self.assertIsNone(p1.target)
        self.assertEqual(p1.num_slices, 1)
        self.assertEqual(p1.total_size, 4096)
        self.assertEqual(p1.ptrs, [0x1000])
        self.assertEqual(p1.sizes, [4096])

        # Keyword args for single-buffer form.
        rt = RangeTarget(0, 8192, "w1")
        p_kw = PutItem("k1", ptr=0x2000, size=2048, target=rt)
        self.assertEqual(p_kw.ptrs, [0x2000])
        self.assertEqual(p_kw.sizes, [2048])
        self.assertEqual(p_kw.target, rt)

        # Python sequence + sequence.
        p_list = PutItem("k2", [0x1000, 0x2000], [1024, 3072])
        self.assertEqual(p_list.num_slices, 2)
        self.assertEqual(p_list.total_size, 4096)
        self.assertEqual(p_list.ptrs, [0x1000, 0x2000])
        self.assertEqual(p_list.sizes, [1024, 3072])

        # Tuple + scalar broadcast.
        p_bcast = PutItem("k3", (0x1000, 0x2000, 0x3000), 4096)
        self.assertEqual(p_bcast.num_slices, 3)
        self.assertEqual(p_bcast.total_size, 12288)
        self.assertEqual(p_bcast.sizes, [4096, 4096, 4096])

        # 1D C-contiguous uint64 buffer protocol (array('Q')) + scalar broadcast.
        ptrs_u64 = array.array("Q", [0x10000, 0x20000, 0x30000])
        sizes_u64 = array.array("Q", [512, 1024, 1536])
        p_buf_bcast = PutItem("k4", ptrs_u64, 2048)
        self.assertEqual(p_buf_bcast.ptrs, [0x10000, 0x20000, 0x30000])
        self.assertEqual(p_buf_bcast.sizes, [2048, 2048, 2048])

        p_buf_both = PutItem("k5", ptrs_u64, sizes_u64)
        self.assertEqual(p_buf_both.sizes, [512, 1024, 1536])
        self.assertEqual(p_buf_both.total_size, 3072)

        # 1D C-contiguous int64 buffer protocol (array('q'), e.g., torch.int64).
        ptrs_i64 = array.array("q", [0x40000, 0x50000])
        sizes_i64 = array.array("q", [4096, 8192])
        g_i64 = GetItem(
            "k6",
            ptrs=ptrs_i64,
            sizes=sizes_i64,
            target=PartIndex(0),
            src_offset=128,
        )
        self.assertEqual(g_i64.key, "k6")
        self.assertEqual(g_i64.ptrs, [0x40000, 0x50000])
        self.assertEqual(g_i64.sizes, [4096, 8192])
        self.assertEqual(g_i64.target, PartIndex(0))
        self.assertEqual(g_i64.src_offset, 128)

        # memoryview cast to 'Q'.
        mv = memoryview(ptrs_u64)
        g_mv = GetItem("k7", mv, 1024)
        self.assertEqual(g_mv.ptrs, [0x10000, 0x20000, 0x30000])
        self.assertEqual(g_mv.sizes, [1024, 1024, 1024])

        # Target setter accepts None and valid target types.
        p1.target = PartTarget(0, 1)
        self.assertEqual(p1.target, PartTarget(0, 1))
        p1.target = None
        self.assertIsNone(p1.target)
        g_i64.target = None
        self.assertIsNone(g_i64.target)

    @unittest.skipIf(np is None, "numpy is not installed")
    def test_numpy_buffers_and_scalars(self) -> None:
        ptrs_np = np.array([0x1000, 0x2000], dtype=np.uint64)
        sizes_np = np.array([1024, 2048], dtype=np.int64)
        item = PutItem("np_k", ptrs_np, sizes_np)
        self.assertEqual(item.ptrs, [0x1000, 0x2000])
        self.assertEqual(item.sizes, [1024, 2048])

        item_bcast = GetItem("np_k", ptrs_np, np.uint64(4096))
        self.assertEqual(item_bcast.sizes, [4096, 4096])

        item_scalar = PutItem("np_s", np.uint64(0x3000), np.uint64(512))
        self.assertEqual(item_scalar.ptrs, [0x3000])
        self.assertEqual(item_scalar.sizes, [512])

    def test_validation_errors(self) -> None:
        # Length mismatch.
        with self.assertRaises(ValueError):
            PutItem("k", [0x1000, 0x2000], [4096])

        # Non-contiguous 1D buffer.
        strided = memoryview(array.array("Q", [1, 2, 3, 4]))[::2]
        with self.assertRaises(ValueError):
            PutItem("k", strided, 64)

        # 2D buffer.
        buf_2d = memoryview(array.array("Q", [1, 2, 3, 4])).cast("B").cast("Q", (2, 2))
        with self.assertRaises(ValueError):
            PutItem("k", buf_2d, 64)

        # Unsupported buffer dtypes.
        with self.assertRaises(ValueError):
            PutItem("k", array.array("I", [1, 2]), 64)
        with self.assertRaises(ValueError):
            GetItem("k", array.array("d", [1.0, 2.0]), 64)
        with self.assertRaises(ValueError):
            PutItem("k", memoryview(b"01234567"), 64)

        # Negative values.
        with self.assertRaises(ValueError):
            PutItem("k", -1, 64)
        with self.assertRaises(ValueError):
            PutItem("k", 0x1000, -1)
        with self.assertRaises(ValueError):
            PutItem("k", [0x1000, -1], 64)
        with self.assertRaises(ValueError):
            PutItem("k", array.array("q", [0x1000, -5]), 64)
        with self.assertRaises(ValueError):
            PutItem("k", [0x1000], array.array("q", [-8]))

        # Disallowed bytes / bytearray / str / bool.
        with self.assertRaises(TypeError):
            PutItem("k", b"01234567", 64)
        with self.assertRaises(TypeError):
            PutItem("k", bytearray(16), 64)
        with self.assertRaises(TypeError):
            PutItem("k", "01234567", 64)
        with self.assertRaises(TypeError):
            PutItem("k", True, 64)
        with self.assertRaises(TypeError):
            PutItem("k", [0x1000], True)
        with self.assertRaises(TypeError):
            PutItem("k", [True], 64)

        # Wrong target type & keyword-only enforcement.
        with self.assertRaises(TypeError):
            PutItem("k", 0x1000, 64, target=PartIndex(0))
        with self.assertRaises(TypeError):
            GetItem("k", 0x1000, 64, target=RangeTarget(0, 64, "w"))
        with self.assertRaises(TypeError):
            PutItem("k", 0x1000, 64, None)  # target is keyword-only
        with self.assertRaises(TypeError):
            GetItem("k", 0x1000, 64, src_offset=-1)


class TestUnifiedAoSE2E(unittest.TestCase):
    """End-to-end tests for unified AoS put/get/exist against EmbeddedMaster."""

    store: MooncakeDistributedStore

    @classmethod
    def setUpClass(cls) -> None:
        cls.store = MooncakeDistributedStore()
        ret = cls.store.setup(
            "localhost",
            "P2PHANDSHAKE",
            SEGMENT_SIZE,
            LOCAL_BUFFER_SIZE,
            _protocol(),
            _device_name(),
            "",
            enable_embedded_master=True,
        )
        if ret != 0:
            raise RuntimeError(f"embedded-master setup failed: {ret}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.store.close()

    def _alloc_registered(self, size: int) -> tuple[ctypes.Array, int]:
        buf = (ctypes.c_ubyte * size)()
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, size), 0)
        self.addCleanup(self.store.unregister_buffer, ptr)
        return buf, ptr

    def _unique_key(self, prefix: str) -> str:
        return f"aos_{prefix}_{os.getpid()}_{time.time_ns()}"

    def test_legacy_put_get_overloads_preserved(self) -> None:
        key = self._unique_key("legacy")
        payload = b"legacy-bytes-api-still-works"
        self.assertEqual(self.store.put(key, payload), 0)
        self.assertEqual(self.store.get(key), payload)

    def test_single_and_multi_slice_put_get(self) -> None:
        chunk = 4096
        src, src_ptr = self._alloc_registered(chunk * 3)
        dst, dst_ptr = self._alloc_registered(chunk * 3)
        payload = bytes((i * 13 + 5) & 0xFF for i in range(chunk * 3))
        ctypes.memmove(src_ptr, payload, len(payload))

        # Single-buffer PutItem / GetItem.
        k_single = self._unique_key("single")
        self.assertEqual(self.store.batch_put([PutItem(k_single, src_ptr, chunk)]), [0])
        self.assertEqual(self.store.batch_get([GetItem(k_single, dst_ptr, chunk)]), [0])
        self.assertEqual(bytes(dst[:chunk]), payload[:chunk])

        # Multi-buffer scatter-gather using array('Q') + scalar size broadcast,
        # read back with different slice boundaries.
        k_multi = self._unique_key("multi")
        src_ptrs = array.array("Q", [src_ptr, src_ptr + chunk, src_ptr + chunk * 2])
        self.assertEqual(self.store.batch_put([PutItem(k_multi, src_ptrs, chunk)]), [0])

        ctypes.memset(dst_ptr, 0, len(payload))
        dst_ptrs = array.array("Q", [dst_ptr, dst_ptr + 2048, dst_ptr + 8192])
        dst_sizes = array.array("Q", [2048, 6144, 4096])
        self.assertEqual(
            self.store.batch_get([GetItem(k_multi, dst_ptrs, dst_sizes)]),
            [0],
        )
        self.assertEqual(bytes(dst), payload)

        # Sub-range read with src_offset > 0.
        sub_buf, sub_ptr = self._alloc_registered(chunk)
        self.assertEqual(
            self.store.batch_get([GetItem(k_multi, sub_ptr, chunk, src_offset=1024)]),
            [0],
        )
        self.assertEqual(bytes(sub_buf), payload[1024 : 1024 + chunk])

        # PartIndex(0) succeeds on single-part object; PartIndex(1) and
        # multi-part PartTarget(0, 2) return negative errors.
        ctypes.memset(dst_ptr, 0, len(payload))
        self.assertEqual(
            self.store.batch_get(
                [GetItem(k_single, dst_ptr, chunk, target=PartIndex(0))]
            ),
            [0],
        )
        self.assertEqual(bytes(dst[:chunk]), payload[:chunk])
        self.assertLess(
            self.store.batch_get(
                [GetItem(k_single, dst_ptr, chunk, target=PartIndex(1))]
            )[0],
            0,
        )
        self.assertLess(
            self.store.batch_put(
                [
                    PutItem(
                        self._unique_key("mp"),
                        src_ptr,
                        chunk,
                        target=PartTarget(0, 2),
                    )
                ]
            )[0],
            0,
        )

    def test_batch_put_mixed_whole_and_concurrent_piece_put(self) -> None:
        piece_size = 4096
        num_pieces = 4
        total_size = piece_size * num_pieces
        src, src_ptr = self._alloc_registered(total_size + piece_size)
        dst, dst_ptr = self._alloc_registered(total_size + piece_size)
        payload = bytes((i * 29 + 11) & 0xFF for i in range(total_size + piece_size))
        ctypes.memmove(src_ptr, payload, len(payload))

        k_whole = self._unique_key("batch_whole")
        k_piece = self._unique_key("batch_piece")
        write_id = f"py-wid-{time.time_ns()}"

        # Mix WholeObject and piece 0 of a RangeTarget in one batch_put.
        rc_first = self.store.batch_put(
            [
                PutItem(k_whole, src_ptr + total_size, piece_size),
                PutItem(
                    k_piece,
                    src_ptr,
                    piece_size,
                    target=RangeTarget(0, total_size, write_id),
                ),
            ],
            ReplicateConfig(),
        )
        self.assertEqual(rc_first, [0, 0])

        # PiecePut object is not readable until every byte is committed.
        self.assertEqual(
            self.store.batch_exist(
                [k_whole, k_piece],
                GrantLeasePolicy(ProbeLeaseMode.NONE),
            ),
            [1, 0],
        )

        # Write remaining 3 pieces concurrently from worker threads (verifies
        # GIL release and thread safety).
        errors: list[tuple[int, int]] = []
        lock = threading.Lock()

        def write_piece(idx: int) -> None:
            offset = idx * piece_size
            rc = self.store.batch_put(
                [
                    PutItem(
                        k_piece,
                        src_ptr + offset,
                        piece_size,
                        target=RangeTarget(offset, total_size, write_id),
                    )
                ]
            )[0]
            if rc != 0:
                with lock:
                    errors.append((idx, rc))

        threads = [
            threading.Thread(target=write_piece, args=(i,))
            for i in range(1, num_pieces)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

        # Now both objects exist and can be batch_get'd with group_by_target.
        self.assertEqual(self.store.batch_exist([k_whole, k_piece]), [1, 1])

        for group_by_target in (False, True):
            ctypes.memset(dst_ptr, 0, total_size + piece_size)
            rcs = self.store.batch_get(
                [
                    GetItem(k_whole, dst_ptr + total_size, piece_size),
                    GetItem(k_piece, dst_ptr, total_size),
                ],
                group_by_target=group_by_target,
            )
            self.assertEqual(rcs, [0, 0])
            self.assertEqual(bytes(dst), payload)

        # Sub-range read spanning across piece 1 and piece 2 boundary.
        cross_buf, cross_ptr = self._alloc_registered(piece_size)
        cross_offset = piece_size + piece_size // 2
        self.assertEqual(
            self.store.batch_get(
                [GetItem(k_piece, cross_ptr, piece_size, src_offset=cross_offset)]
            ),
            [0],
        )
        self.assertEqual(
            bytes(cross_buf), payload[cross_offset : cross_offset + piece_size]
        )

    def test_batch_exist_lease_policies_and_error_isolation(self) -> None:
        buf, ptr = self._alloc_registered(4096)
        keys = [self._unique_key(f"ex_{i}") for i in range(4)]
        a, b, c, missing = keys

        # Put 3 keys in one batch_put, plus one invalid empty-slice item to
        # verify per-item error isolation.
        rcs = self.store.batch_put(
            [
                PutItem(a, ptr, 512),
                PutItem(b, ptr + 512, 512),
                PutItem(c, ptr + 1024, 512),
                PutItem(missing, [], []),
            ]
        )
        self.assertEqual(rcs[:3], [0, 0, 0])
        self.assertLess(rcs[3], 0)

        # ALL_HITS (default), NONE, LAST_HIT_ONLY.
        self.assertEqual(self.store.batch_exist(keys), [1, 1, 1, 0])
        self.assertEqual(
            self.store.batch_exist(keys, GrantLeasePolicy(ProbeLeaseMode.NONE)),
            [1, 1, 1, 0],
        )
        self.assertEqual(
            self.store.batch_exist(
                [a, missing, b, c],
                GrantLeasePolicy(ProbeLeaseMode.LAST_HIT_ONLY, candidate_size=2),
            ),
            [1, 0, 1, 1],
        )
        self.assertEqual(
            self.store.batch_exist(
                [a, b, c, missing],
                GrantLeasePolicy(ProbeLeaseMode.LAST_HIT_ONLY, candidate_size=2),
            ),
            [1, 1, 1, 0],
        )

        # Invalid candidate_size for LAST_HIT_ONLY returns per-key errors.
        invalid = self.store.batch_exist(
            [a, b],
            GrantLeasePolicy(ProbeLeaseMode.LAST_HIT_ONLY, candidate_size=0),
        )
        self.assertEqual(len(invalid), 2)
        self.assertTrue(all(v < 0 for v in invalid))

        # Empty batches.
        self.assertEqual(self.store.batch_put([]), [])
        self.assertEqual(self.store.batch_get([]), [])
        self.assertEqual(self.store.batch_exist([]), [])

        # Per-item error isolation in batch_get.
        get_rcs = self.store.batch_get(
            [
                GetItem(a, ptr + 2048, 512),
                GetItem(missing, ptr + 2560, 512),
            ]
        )
        self.assertEqual(get_rcs[0], 0)
        self.assertLess(get_rcs[1], 0)

    def test_uninitialized_store_returns_per_item_errors(self) -> None:
        uninit = MooncakeDistributedStore()
        self.addCleanup(uninit.close)
        items_put = [PutItem("k1", 0x1000, 64), PutItem("k2", 0x2000, 64)]
        items_get = [GetItem("k1", 0x1000, 64), GetItem("k2", 0x2000, 64)]

        put_res = uninit.batch_put(items_put)
        self.assertEqual(len(put_res), 2)
        self.assertTrue(all(v < 0 for v in put_res))

        get_res = uninit.batch_get(items_get)
        self.assertEqual(len(get_res), 2)
        self.assertTrue(all(v < 0 for v in get_res))

        exist_res = uninit.batch_exist(["k1", "k2"])
        self.assertEqual(len(exist_res), 2)
        self.assertTrue(all(v < 0 for v in exist_res))

        self.assertEqual(uninit.batch_put([]), [])
        self.assertEqual(uninit.batch_get([]), [])
        self.assertEqual(uninit.batch_exist([]), [])


if __name__ == "__main__":
    unittest.main()
