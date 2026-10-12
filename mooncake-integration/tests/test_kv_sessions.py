"""Real TCP Store session integration tests.

Run with an externally started patched master:
    MOONCAKE_MASTER=127.0.0.1:18751 python -m unittest discover \
        -s mooncake-integration/tests -p test_kv_sessions.py -v
The tests mount their own small memory segments and use unique session/keys.
"""

import ctypes
import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from mooncake.store import MooncakeDistributedStore, ReplicateConfig


class KvSessionIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if "MOONCAKE_MASTER" not in os.environ:
            raise unittest.SkipTest("Set MOONCAKE_MASTER to a patched TCP master")
        cls.clients = []
        for _ in range(2):
            client = MooncakeDistributedStore()
            result = client.setup(
                "127.0.0.1",
                "P2PHANDSHAKE",
                64 << 20,
                16 << 20,
                "tcp",
                "",
                os.environ["MOONCAKE_MASTER"],
            )
            if result != 0:
                raise RuntimeError(f"Store setup failed: {result}")
            cls.clients.append(client)

    @classmethod
    def tearDownClass(cls):
        for client in cls.clients:
            client.close()

    def setUp(self):
        self.prefix = uuid.uuid4().hex
        self.a, self.b = self.clients
        self.sessions = []
        self.keys = []

    def tearDown(self):
        for session in self.sessions:
            self.a.close_kv_session(session)
        for key in self.keys:
            self.a.remove(key, force=True)

    def session(self, name):
        session_id = f"{self.prefix}:{name}"
        self.sessions.append(session_id)
        return session_id

    def key(self, name):
        key = f"{self.prefix}:{name}"
        self.keys.append(key)
        return key

    @staticmethod
    def config(*per_key):
        config = ReplicateConfig()
        config.kv_sessions = list(per_key)
        return config

    def test_long_session_ids_preserve_identity(self):
        prefix = "s" * 2000
        a, b = self.session(prefix + "-a"), self.session(prefix + "-b")
        self.assertIsInstance(a, str)
        key = self.key("long-id")
        self.assertEqual(self.a.put(key, b"payload", self.config([a, b])), 0)
        self.a.pin_kv_session(session_id=a)
        self.assertEqual(self.b.attach_kv_session(session_id=a, keys=[key]), [0])
        self.assertTrue(self.b.get_kv_session(session_id=a).pinned)
        self.assertFalse(self.b.get_kv_session(session_id=b).pinned)
        self.assertEqual(self.b.get_kv_session(session_id=a).session_id, a)
        self.assertEqual(self.b.get_kv_session(session_id=b).member_count, 1)
        self.a.close_kv_session(session_id=a)
        self.assertEqual(self.b.get_kv_session(session_id=b).member_count, 1)

    def test_shared_prefix_cross_client_and_close(self):
        a, b = self.session("a"), self.session("b")
        key = self.key("shared")
        self.assertEqual(self.a.put(key, b"original", self.config([a])), 0)
        # Another DP rank only needs the ID; attach creates B on first use.
        self.assertEqual(self.b.pin_kv_session(session_id=a), 0)
        self.assertEqual(self.b.attach_kv_session(b, [key, "absent"]), [0, -704])
        self.assertEqual(self.b.get(key), b"original")
        self.assertEqual(self.b.get_kv_session(b).member_count, 1)
        self.assertEqual(self.a.close_kv_session(a), 0)
        self.assertEqual(self.b.get(key), b"original")
        self.assertEqual(self.b.get_kv_session(b).member_count, 1)
        self.assertEqual(self.b.unpin_kv_session(b), 0)
        self.assertEqual(self.b.update_kv_session(b, []), 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.b.get_kv_session(b)

    def test_update_session_trims_across_clients_without_deleting_objects(self):
        a, b = self.session("a"), self.session("b")
        keep, old, shared, unrelated = [
            self.key(name) for name in ("keep", "old", "shared", "unrelated")
        ]
        self.assertEqual(self.a.put(keep, b"keep", self.config([a])), 0)
        self.assertEqual(self.b.put(old, b"old", self.config([a])), 0)
        self.assertEqual(self.b.put(shared, b"shared", self.config([a, b])), 0)
        self.assertEqual(self.a.put(unrelated, b"other", self.config([b])), 0)
        self.a.pin_kv_session(a)
        self.b.pin_kv_session(b)
        keep_keys = [keep, keep, "absent", unrelated]
        for client in (self.a, self.b):
            self.assertEqual(
                client.update_kv_session(session_id=a, keep_keys=keep_keys), 0
            )
        self.assertEqual(self.a.list_kv_session_keys(a).keys, [keep])
        self.assertTrue(self.b.get_kv_session(a).pinned)
        self.assertEqual(set(self.a.list_kv_session_keys(b).keys), {shared, unrelated})
        self.assertTrue(self.a.get_kv_session(b).pinned)
        for key, payload in ((keep, b"keep"), (old, b"old"), (shared, b"shared")):
            self.assertEqual(self.a.get(key), payload)
        future = self.key("future")
        self.assertEqual(self.b.put(future, b"next", self.config([a])), 0)
        self.assertEqual(self.a.get_kv_session(a).member_count, 2)
        self.assertTrue(self.a.get_kv_session(a).pinned)
        self.assertEqual(self.b.update_kv_session(a, []), 0)
        self.assertEqual(self.a.update_kv_session(a, [keep]), 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.a.get_kv_session(a)
        self.assertEqual(self.b.get(future), b"next")
        self.assertEqual(self.a.attach_kv_session(a, [keep]), [0])
        self.assertFalse(self.b.get_kv_session(a).pinned)

    def test_update_allows_concurrent_object_removal(self):
        session = self.session("updating")
        keep = self.key("keep")
        old = [self.key(f"old-{i:04d}") for i in range(300)]
        self.assertEqual(self.a.put(keep, b"keep", self.config([session])), 0)
        self.assertEqual(
            self.a.put_batch(
                old, [b"old"] * len(old), self.config(*[[session]] * len(old))
            ),
            0,
        )
        self.a.pin_kv_session(session)

        def remove_objects():
            for key in old[::2]:
                self.assertEqual(self.b.remove(key, force=True), 0)

        with ThreadPoolExecutor(max_workers=2) as pool:
            trimming = pool.submit(self.a.update_kv_session, session, [keep])
            removing = pool.submit(remove_objects)
            self.assertEqual(trimming.result(timeout=30), 0)
            removing.result(timeout=30)
        self.assertEqual(self.b.list_kv_session_keys(session).keys, [keep])
        self.assertTrue(self.b.get_kv_session(session).pinned)
        self.assertEqual(self.b.get(keep), b"keep")
        # Update releases ownership, so the objects not explicitly deleted remain.
        self.assertEqual(self.b.get(old[1]), b"old")

    def test_update_session_validation_and_complete_keep_set(self):
        session = self.session("update")
        key = self.key("keep")
        self.assertEqual(self.a.put(key, b"value", self.config([session])), 0)
        self.a.pin_kv_session(session)
        for invalid in ("", "s" * 4097, "s\0x"):
            with self.assertRaisesRegex(RuntimeError, "INVALID_PARAMS"):
                self.b.update_kv_session(invalid, [])
        # More than MAX_BATCH entries: update is one complete set, not a batch
        # of additive operations. Absent candidates must not create membership.
        keys = [key] + [f"absent-{i}" for i in range(4096)]
        self.assertEqual(self.b.update_kv_session(session, keys), 0)
        self.assertEqual(self.a.get_kv_session(session).member_count, 1)
        self.assertTrue(self.a.get_kv_session(session).pinned)

    def test_write_variants_and_deduplicated_membership(self):
        a, b = self.session("a"), self.session("b")
        x, y, z = self.key("x"), self.key("y"), self.key("z")
        self.assertEqual(self.a.put(x, b"x", self.config([a])), 0)
        self.a.pin_kv_session(a)
        # Idempotent Put must register B without replacing X's data.
        self.assertEqual(self.b.put(x, b"different", self.config([b])), 0)
        self.assertEqual(self.b.get(x), b"x")
        self.assertEqual(self.b.get_kv_session(b).member_count, 1)
        self.assertEqual(
            self.a.put_batch([y, z], [b"yy", b"zzz"], self.config([a], [b])), 0
        )
        self.assertEqual(self.a.upsert(x, b"longer-value", self.config([a, b])), 0)
        self.assertEqual(self.b.get(x), b"longer-value")
        page = self.a.list_kv_session_keys(a, limit=1)
        keys = list(page.keys)
        while page.next_cursor:
            page = self.a.list_kv_session_keys(a, cursor=page.next_cursor, limit=1)
            keys.extend(page.keys)
        self.assertEqual(set(keys), {x, y})
        self.assertEqual(self.a.get_kv_session(b).member_count, 2)
        self.assertEqual(
            self.a.put_parts(
                self.key("parts"), b"one", b"two", config=self.config([a])
            ),
            0,
        )
        self.assertEqual(self.a.get_kv_session(a).member_count, 3)

    def test_registered_buffers_range_writes_and_shape_errors(self):
        a, b = self.session("a"), self.session("b")
        key = self.key("buffer")
        buffer = ctypes.create_string_buffer(b"buffer-data")
        address = ctypes.addressof(buffer)
        self.assertEqual(self.a.register_buffer(address, len(buffer)), 0)
        try:
            self.assertEqual(self.a.put_from(key, address, 11, self.config([a])), 0)
            self.assertEqual(self.b.get(key), b"buffer-data")
            r1, r2 = self.key("range1"), self.key("range2")
            self.assertEqual(
                self.a.batch_put_session_start(
                    [r1, r2], [11, 11], self.config([a], [b])
                ),
                [0, 0],
            )
            self.assertEqual(
                self.a.batch_put_from_multi_buffer_ranges(
                    [r1, r2],
                    [[address], [address]],
                    [[11], [11]],
                    [[0], [0]],
                ),
                [11, 11],
            )
            self.assertEqual(self.a.batch_put_session_end([r1, r2]), [0, 0])
            self.assertEqual(self.b.get(r2), b"buffer-data")
            self.assertEqual(self.b.get_kv_session(b).member_count, 1)
            self.assertNotEqual(
                self.a.put(self.key("bad"), b"x", self.config([a], [b])), 0
            )
            self.assertTrue(
                all(
                    code != 0
                    for code in self.a.batch_put_session_start(
                        [self.key("bad-range")], [11], self.config([a], [b])
                    )
                )
            )
        finally:
            self.a.unregister_buffer(address)

    def test_buffer_batches_and_filtered_range_config(self):
        a, b = self.session("a"), self.session("b")
        buffer = ctypes.create_string_buffer(b"abcdefgh")
        address = ctypes.addressof(buffer)
        self.assertEqual(self.a.register_buffer(address, len(buffer)), 0)
        try:
            x, y = self.key("x"), self.key("y")
            self.assertEqual(
                self.a.batch_put_from(
                    [x, y], [address, address], [8, 8], self.config([a], [b])
                ),
                [0, 0],
            )
            self.assertEqual(
                self.a.batch_upsert_from(
                    [x, y], [address, address], [8, 8], self.config([b], [a])
                ),
                [0, 0],
            )
            self.assertEqual(self.a.get_kv_session(a).member_count, 2)
            self.assertEqual(self.a.get_kv_session(b).member_count, 2)
            r1, r2 = self.key("r1"), self.key("r2")
            self.assertEqual(
                self.a.batch_put_session_start([r1], [8], self.config([a])),
                [0],
            )
            # An active ranged write is rejected locally; the remaining key's
            # tags must be selected using its original index.
            statuses = self.a.batch_put_session_start(
                [r1, r2], [8, 8], self.config([a], [b])
            )
            self.assertNotEqual(statuses[0], 0)
            self.assertEqual(statuses[1], 0)
            self.assertEqual(
                self.a.batch_put_from_multi_buffer_ranges(
                    [r1, r2], [[address], [address]], [[8], [8]], [[0], [0]]
                ),
                [8, 8],
            )
            self.assertEqual(self.a.batch_put_session_end([r1, r2]), [0, 0])
            self.assertIn(r1, self.a.list_kv_session_keys(a).keys)
            self.assertNotIn(r1, self.a.list_kv_session_keys(b).keys)
            self.assertIn(r2, self.a.list_kv_session_keys(b).keys)
            self.assertNotIn(r2, self.a.list_kv_session_keys(a).keys)
        finally:
            self.a.unregister_buffer(address)

    def test_tensor_tags_follow_physical_tp_shards(self):
        import torch

        a, b = self.session("a"), self.session("b")
        tensor = torch.arange(16, dtype=torch.float32).reshape(4, 4)
        base = self.key("tensor")
        self.assertEqual(
            self.a.pub_tensor_with_tp(base, tensor, self.config([a]), tp_size=2),
            0,
        )
        page = self.a.list_kv_session_keys(a)
        self.assertEqual(len(page.keys), 2)
        self.keys.extend(page.keys)
        bases = [self.key("tensor-b1"), self.key("tensor-b2")]
        config = self.config([a], [b])
        config.group_ids = [self.prefix + ":g1", self.prefix + ":g2"]
        self.assertEqual(
            self.a.batch_pub_tensor_with_tp(bases, [tensor, tensor], config, tp_size=2),
            [0, 0],
        )
        a_keys = self.a.list_kv_session_keys(a).keys
        b_keys = self.b.list_kv_session_keys(b).keys
        self.assertEqual(len(a_keys), 4)
        self.assertEqual(len(b_keys), 2)
        self.assertFalse(set(a_keys) & set(b_keys))
        self.keys.extend(a_keys)
        self.keys.extend(b_keys)

    def test_tensor_filtering_preserves_groups_and_sessions(self):
        import torch

        a, b = self.session("a"), self.session("b")
        bad, good = self.key("bad-tensor"), self.key("good-tensor")
        config = self.config([a], [b])
        config.group_ids = [self.prefix + ":bad-group", self.prefix + ":good-group"]
        result = self.a.batch_pub_tensor(
            [bad, good], [None, torch.arange(4, dtype=torch.float32)], config
        )
        self.assertNotEqual(result[0], 0)
        self.assertEqual(result[1], 0)
        self.assertEqual(self.b.list_kv_session_keys(b).keys, [good])
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.a.get_kv_session(a)

    def test_rejected_upsert_does_not_register_session(self):
        key, rejected = self.key("rejected"), self.session("rejected")
        original = ReplicateConfig()
        original.group_ids = [self.prefix + "-original"]
        self.assertEqual(self.a.put(key, b"original", original), 0)
        config = self.config([rejected])
        config.group_ids = [self.prefix + "-conflict"]
        self.assertNotEqual(self.a.upsert(key, b"replacement", config), 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.a.get_kv_session(rejected)
        self.assertEqual(self.a.get(key), b"original")

    def test_batch_rejects_mismatched_groups_before_indexing(self):
        keys = [self.key("shape-a"), self.key("shape-b")]
        session = self.session("shape")
        config = self.config([session], [session])
        config.group_ids = [self.prefix + "-one-group"]
        for write in (self.a.put_batch, self.a.upsert_batch):
            result = write(keys, [b"a", b"b"], config)
            self.assertLess(result, 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.a.get_kv_session(session)

    def test_shared_prefix_exceeds_old_session_limit(self):
        key = self.key("popular-prefix")
        sessions = [self.session(f"reader-{i}") for i in range(1024)]
        self.assertEqual(self.a.put(key, b"shared", self.config(sessions)), 0)
        self.assertEqual(self.b.attach_kv_session(sessions[-1], [key]), [0])
        self.b.pin_kv_session(sessions[-1])
        self.assertEqual(self.a.get_kv_session(sessions[-1]).member_count, 1)
        self.assertTrue(self.a.get_kv_session(sessions[-1]).pinned)
        self.assertEqual(self.a.remove(key, force=True), 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.b.get_kv_session(sessions[-1])

    def test_empty_sessions_are_reclaimed(self):
        session = self.session("reclaimed")
        self.assertEqual(
            self.a.attach_kv_session(session, [self.key("absent")]), [-704]
        )
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.a.get_kv_session(session)
        x, y = self.key("x"), self.key("y")
        self.assertEqual(
            self.a.put_batch([x, y], [b"x", b"y"], self.config([session], [session])), 0
        )
        self.a.pin_kv_session(session)
        self.assertEqual(self.a.remove(x, force=True), 0)
        self.assertTrue(self.b.get_kv_session(session).pinned)
        self.assertEqual(self.a.remove(y, force=True), 0)
        with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
            self.b.get_kv_session(session)
        self.assertEqual(self.a.put(x, b"again", self.config([session])), 0)
        self.assertFalse(self.b.get_kv_session(session).pinned)

    def test_concurrent_close_and_late_membership(self):
        a = self.session("a")
        keys = [self.key(str(i)) for i in range(30)]
        for key in keys:
            self.assertEqual(self.a.put(key, b"payload"), 0)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(self.b.attach_kv_session, a, keys[i::3]) for i in range(3)
            ]
            self.a.close_kv_session(a)
            for future in futures:
                self.assertEqual(future.result(), [0] * 10)
        self.a.close_kv_session(a)
        # Pin controls alone do not create an empty session.
        for operation in (self.a.pin_kv_session, self.a.unpin_kv_session):
            with self.assertRaisesRegex(RuntimeError, "SESSION_NOT_FOUND"):
                operation(session_id=a)
        self.assertEqual(self.a.update_kv_session(a, keys[:1]), 0)
        self.assertEqual(self.a.close_kv_session(session_id=a), 0)
        self.assertEqual(self.a.attach_kv_session(a, keys[:1]), [0])
        self.assertFalse(self.a.get_kv_session(a).pinned)
        self.assertEqual(self.a.get(keys[0]), b"payload")


if __name__ == "__main__":
    unittest.main()
