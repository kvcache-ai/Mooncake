"""CPU/TCP policy checks against a real Master, optionally via the client daemon.

Set MASTER_SERVER to an isolated Master address. Set REAL_CLIENT_ADDRESS to
also run this suite through the production DummyClient/RealClient RPC path.
"""

import os
import unittest

from mooncake.store import MooncakeDistributedStore


class TestBatchProbePolicy(unittest.TestCase):
    def setUp(self):
        self.store = MooncakeDistributedStore()
        self.addCleanup(self.store.close)
        daemon = os.getenv("REAL_CLIENT_ADDRESS")
        if daemon:
            status = self.store.setup_dummy(16 << 20, 16 << 20, daemon)
        else:
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
        self.keys = [f"probe-policy-{os.getpid()}-{self.id()}-{i}" for i in range(4)]
        for key in self.keys[:3]:
            self.assertEqual(self.store.put(key, b"checkpoint"), 0)
            self.addCleanup(self.store.remove, key, True)

    def test_default_and_selected_masks(self):
        a, b, c, missing = self.keys
        self.assertEqual(self.store.batch_probe_key(self.keys), [1, 1, 1, 0])
        self.assertEqual(self.store.batch_is_exist(self.keys), [1, 1, 1, 0])
        self.assertEqual(
            self.store.batch_probe_key(self.keys, "LastHitOnly", 2), [1, 1, 0, 0]
        )
        self.assertEqual(
            self.store.batch_probe_key([a, b, b, c], "LastHitOnly", 2),
            [0, 0, 1, 1],
        )
        self.assertEqual(
            self.store.batch_probe_key([a, missing, b, c], "LastHitOnly", 2),
            [0, 0, 1, 1],
        )
        self.assertEqual(
            self.store.batch_probe_key([a, missing], "LastHitOnly", 2), [0, 0]
        )
        self.assertEqual(self.store.get(c), b"checkpoint")

    def test_validation_duplicates_and_empty_input(self):
        a, b, _, _ = self.keys
        self.assertEqual(
            self.store.batch_probe_key([a, b, a, a], "LastHitOnly", 2),
            [0, 0, 1, 1],
        )
        self.assertEqual(self.store.batch_probe_key([a, b], "LastHitOnly"), [0, 1])
        self.assertEqual(self.store.batch_probe_key([a], candidate_size=0), [1])
        self.assertEqual(self.store.batch_probe_key([]), [])
        self.assertEqual(self.store.batch_probe_key([], "LastHitOnly", 0), [])
        for size in (0, 3):
            with self.assertRaises(ValueError):
                self.store.batch_probe_key([a, b], "LastHitOnly", size)
        with self.assertRaises(ValueError):
            self.store.batch_probe_key([a], "unknown")
        with self.assertRaises(TypeError):
            self.store.batch_probe_key([a], "LastHitOnly", -1)

    def test_uninitialized_errors_are_not_misses(self):
        uninitialized = MooncakeDistributedStore()
        self.addCleanup(uninitialized.close)
        for policy in ("none", "LastHitOnly"):
            result = uninitialized.batch_probe_key(self.keys, policy, 2)
            self.assertEqual(len(result), len(self.keys))
            self.assertTrue(all(value < 0 for value in result))


if __name__ == "__main__":
    unittest.main()
