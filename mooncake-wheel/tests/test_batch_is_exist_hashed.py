"""CPU/TCP checks of the hashed batch existence calls against a real Master.

Set MASTER_SERVER to an isolated Master address.
"""

import hashlib
import os
import unittest

import numpy as np
from mooncake.store import MooncakeDistributedStore


class TestBatchIsExistHashed(unittest.TestCase):
    def setUp(self):
        self.store = MooncakeDistributedStore()
        self.addCleanup(self.store.close)
        status = self.store.setup(
            "127.0.0.1:0",
            "P2PHANDSHAKE",
            32 << 20,
            16 << 20,
            "tcp",
            "",
            os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        )
        self.assertEqual(status, 0)
        tag = f"hashed-{os.getpid()}-{self.id()}"
        self.prefixes = [[f"{tag}@g0@r0", f"{tag}@g0@r1"], [f"{tag}@g1@r0"]]
        self.hashes = [hashlib.sha256(bytes([i])).digest() for i in range(6)]
        self.blob = b"".join(self.hashes)
        # Group 0 stores hashes 0-2 on both ranks and hash 3 on rank 0 only;
        # group 1 stores hashes 0 and 4.
        stored = [(0, 0, h) for h in range(4)] + [(0, 1, h) for h in range(3)]
        stored += [(1, 0, 0), (1, 0, 4)]
        for g, r, h in stored:
            key = self._key(g, r, h)
            self.assertEqual(self.store.put(key, b"x"), 0)
            self.addCleanup(self.store.remove, key, True)

    def _key(self, g, r, h, sep="@"):
        return f"{self.prefixes[g][r]}{sep}{self.hashes[h].hex()}"

    def _expected_keys(self, indices):
        return [
            self._key(g, r, h)
            for g, group_indices in enumerate(indices)
            for h in group_indices
            for r in range(len(self.prefixes[g]))
        ]

    def test_matches_batch_is_exist(self):
        indices = [np.arange(6, dtype=np.int64), np.array([4, 0, 5], dtype=np.int64)]
        expected = self.store.batch_is_exist(self._expected_keys(indices))
        for call in (
            self.store.batch_is_exist_hashed,
            self.store.batch_probe_key_hashed,
        ):
            got = call(self.blob, 32, self.prefixes, indices)
            self.assertIsInstance(got, bytes)
            self.assertEqual(list(np.frombuffer(got, dtype=np.int8)), expected)
        self.assertEqual(expected[:8], [1, 1, 1, 1, 1, 1, 1, 0])
        self.assertEqual(expected[12:], [1, 1, 0])

    def test_separator_and_empty_groups(self):
        indices = [np.array([0], dtype=np.int64), np.array([], dtype=np.int64)]
        self.assertEqual(
            self.store.batch_is_exist_hashed(self.blob, 32, self.prefixes, indices),
            b"\x01\x01",
        )
        self.assertEqual(
            self.store.batch_is_exist_hashed(
                self.blob, 32, self.prefixes, indices, separator="#"
            ),
            b"\x00\x00",
        )
        self.assertEqual(self.store.batch_is_exist_hashed(b"", 32, [], []), b"")

    def test_rejects_bad_arguments(self):
        ok = [np.array([0], dtype=np.int64)]
        with self.assertRaises(ValueError):
            self.store.batch_is_exist_hashed(self.blob[:-1], 32, self.prefixes[:1], ok)
        with self.assertRaises(ValueError):
            self.store.batch_is_exist_hashed(self.blob, 32, self.prefixes, ok)
        with self.assertRaises(ValueError):
            self.store.batch_is_exist_hashed(
                self.blob, 32, self.prefixes[:1], [np.array([0], dtype=np.int32)]
            )
        with self.assertRaises(IndexError):
            self.store.batch_is_exist_hashed(
                self.blob, 32, self.prefixes[:1], [np.array([6], dtype=np.int64)]
            )

    def test_uninitialized_client_reports_errors(self):
        uninitialized = MooncakeDistributedStore()
        self.addCleanup(uninitialized.close)
        got = uninitialized.batch_is_exist_hashed(
            self.blob, 32, self.prefixes[:1], [np.arange(2, dtype=np.int64)]
        )
        self.assertEqual(list(np.frombuffer(got, dtype=np.int8)), [-1] * 4)


if __name__ == "__main__":
    unittest.main()
