"""CPU/TCP checks of KeyProber against a real Master.

Set MASTER_SERVER to an isolated Master address.
"""

import ctypes
import hashlib
import os
import unittest

import numpy as np
from mooncake.store import MooncakeDistributedStore


def _setup(store):
    return store.setup(
        "127.0.0.1:0",
        "P2PHANDSHAKE",
        32 << 20,
        16 << 20,
        "tcp",
        "",
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )


class TestKeyProber(unittest.TestCase):
    def setUp(self):
        self.store = MooncakeDistributedStore()
        self.addCleanup(self.store.close)
        self.assertEqual(_setup(self.store), 0)
        tag = f"prober-{os.getpid()}-{self.id()}"
        self.groups = [
            [f"{tag}@g0@r0@{{hash}}", f"{tag}@g0@r1@{{hash}}"],
            [f"{tag}@g1@{{hash}}_k", f"{tag}@g1@{{hash}}_v"],
        ]
        self.hashes = [hashlib.sha256(bytes([i])).digest() for i in range(6)]
        self.blob = b"".join(self.hashes)
        self.prober = self.store.create_key_prober(self.groups, 32)
        # Group 0 stores hashes 0-2 under both templates and hash 3 under the
        # first only; group 1 stores hashes 0 and 4.
        stored = [(0, 0, h) for h in range(4)] + [(0, 1, h) for h in range(3)]
        stored += [(1, t, h) for t in range(2) for h in (0, 4)]
        for g, t, h in stored:
            key = self._key(g, t, h)
            self.assertEqual(self.store.put(key, b"x"), 0)
            self.addCleanup(self.store.remove, key, True)

    def _key(self, g, t, h):
        return self.groups[g][t].replace("{hash}", self.hashes[h].hex())

    def _keys(self, g, indices):
        return [self._key(g, t, h) for h in indices for t in range(2)]

    def _codes(self, results):
        return [list(np.frombuffer(r, dtype=np.int8)) for r in results]

    def test_matches_batch_calls(self):
        indices = [np.arange(6, dtype=np.int64), np.array([4, 0, 5], dtype=np.int64)]
        expected = [
            self.store.batch_is_exist(self._keys(g, indices[g])) for g in (0, 1)
        ]
        self.assertEqual(expected[0][:8], [1, 1, 1, 1, 1, 1, 1, 0])
        self.assertEqual(expected[1], [1, 1, 1, 1, 0, 0])
        for call in (self.prober.is_exist, self.prober.probe):
            got = call(self.blob, indices)
            self.assertTrue(all(isinstance(r, bytes) for r in got))
            self.assertEqual(self._codes(got), expected)

    def test_last_hit_only_matches_batch_probe_key(self):
        indices = [np.arange(4, dtype=np.int64), np.array([], dtype=np.int64)]
        keys = self._keys(0, indices[0])
        expected = self.store.batch_probe_key(keys, "LastHitOnly", 2)
        got = self.prober.probe(self.blob, indices, "LastHitOnly", 2)
        self.assertEqual(self._codes(got), [expected, []])
        with self.assertRaises(ValueError):
            self.prober.probe(self.blob, indices, "AllHits")

    def test_index_forms(self):
        expected = [self.store.batch_is_exist(self._keys(g, range(6))) for g in (0, 1)]
        self.assertEqual(self._codes(self.prober.is_exist(self.blob)), expected)
        as_list = [[0, 4], (4,)]
        as_ctypes = [(ctypes.c_int64 * 2)(0, 4), (ctypes.c_int64 * 1)(4)]
        self.assertEqual(memoryview(as_ctypes[0]).format, "<q")
        for indices in (as_list, as_ctypes):
            self.assertEqual(
                self._codes(self.prober.is_exist(memoryview(self.blob), indices)),
                [expected[0][0:2] + expected[0][8:10], expected[1][8:10]],
            )

    def test_empty_lookups(self):
        empty = np.array([], dtype=np.int64)
        self.assertEqual(self.prober.is_exist(self.blob, [empty, empty]), [b"", b""])
        self.assertEqual(self.prober.is_exist(b""), [b"", b""])
        self.assertEqual(self.store.create_key_prober([], 32).is_exist(b""), [])

    def test_rejects_bad_arguments(self):
        for groups, hash_len in (
            ([["no-placeholder"]], 32),
            ([["{hash}@{hash}"]], 32),
            ([["{hash}"]], 0),
        ):
            with self.assertRaises(ValueError):
                self.store.create_key_prober(groups, hash_len)
        one = np.array([0], dtype=np.int64)
        for hashes, indices in (
            (self.blob[:-1], None),
            (self.blob, [one]),
            (self.blob, [one, np.array([0], dtype=np.int32)]),
            (self.blob, [one, np.zeros((1, 1), dtype=np.int64)]),
        ):
            with self.assertRaises(ValueError):
                self.prober.is_exist(hashes, indices)
        with self.assertRaises(IndexError):
            self.prober.is_exist(self.blob, [one, np.array([6], dtype=np.int64)])

    def test_reports_errors_without_client(self):
        uninitialized = MooncakeDistributedStore()
        self.addCleanup(uninitialized.close)
        indices = [[0, 1], []]
        expected = [[-1] * 4, []]
        prober = uninitialized.create_key_prober(self.groups, 32)
        self.assertEqual(self._codes(prober.is_exist(self.blob, indices)), expected)
        closed = MooncakeDistributedStore()
        self.assertEqual(_setup(closed), 0)
        prober = closed.create_key_prober(self.groups, 32)
        closed.close()
        self.assertEqual(self._codes(prober.probe(self.blob, indices)), expected)


if __name__ == "__main__":
    unittest.main()
