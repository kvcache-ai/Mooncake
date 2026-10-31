"""Fault injection against the real EngramStore implementation and binding."""

import gc
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor

import numpy as np

import engram_store_test_client as native


class PopulateLifetimeTests(unittest.TestCase):
    def make_store(self, mode):
        client = native.FaultClient(mode)
        config = native.EngramStoreConfig()
        config.table_vocab_sizes = [4, 4]
        config.row_bytes = 8
        return client, native.EngramStore({1: config}, client)

    def populate_temporaries(self, table, expect_error=True):
        refs = []

        def array():
            value = np.zeros((4, 8), dtype=np.uint8)
            refs.append(weakref.ref(value))
            return value

        if expect_error:
            with self.assertRaises(RuntimeError):
                table.populate(1, [array(), array()])
        else:
            table.populate(1, [array(), array()])
        gc.collect()
        return refs

    def test_success_releases_owner(self):
        client, table = self.make_store("success")
        refs = self.populate_temporaries(table, expect_error=False)
        self.assertEqual(client.registered_count, 0)
        self.assertEqual(client.published_count, 2)
        self.assertTrue(all(ref() is None for ref in refs))

    def test_unsupported_client_fails_before_registration(self):
        self.assertEqual(native.unsupported_client_result(), (-1, 0, True))

    def test_preflight_failure_does_not_retain_owner(self):
        client, table = self.make_store("preflight")
        refs = self.populate_temporaries(table)
        self.assertEqual(client.register_calls, 0)
        self.assertTrue(all(ref() is None for ref in refs))

    def test_terminal_conflicts_release_owners_without_deleting_tables(self):
        for mode in ("competing_success", "mixed_conflict"):
            with self.subTest(mode=mode):
                client, table = self.make_store(mode)
                for _ in range(2):
                    refs = self.populate_temporaries(table)
                    self.assertEqual(client.registered_count, 0)
                    self.assertEqual(client.published_count, 2)
                    self.assertEqual(client.remove_calls, 0)
                    self.assertTrue(all(ref() is None for ref in refs))

    def test_failed_upload_with_successful_unregister_retains_owner(self):
        client, table = self.make_store("upload_fail")
        refs = self.populate_temporaries(table)
        self.assertEqual(client.registered_count, 0)
        self.assertEqual(client.published_count, 1)
        # A transfer timeout can return an error before DMA completion, even
        # when unregister succeeds. Keep the source allocation conservatively.
        self.assertTrue(all(ref() is not None for ref in refs))

    def test_overlapping_inputs_fail_before_registration(self):
        for overlap in (0, 16):
            with self.subTest(overlap=overlap):
                client, table = self.make_store("success")
                backing = np.zeros(64, dtype=np.uint8)
                first = backing[:32].reshape(4, 8)
                second = backing[overlap : overlap + 32].reshape(4, 8)
                refs = [weakref.ref(first), weakref.ref(second)]
                with self.assertRaises(RuntimeError):
                    table.populate(1, [first, second])
                self.assertEqual(client.register_calls, 0)
                del first, second
                gc.collect()
                self.assertTrue(all(ref() is None for ref in refs))

    def test_uncertain_cleanup_retains_owners_after_store_destruction(self):
        for mode in (
            "cleanup_fail",
            "register_fail",
            "register_partial",
            "register_throw",
            "upload_cleanup_fail",
            "upload_fail",
            "upload_throw",
            "upload_throw_after_publish",
            "unregister_throw",
            "malformed",
            "conflict_cleanup_fail",
        ):
            with self.subTest(mode=mode):
                client, table = self.make_store(mode)
                refs = self.populate_temporaries(table)
                self.assertTrue(all(ref() is not None for ref in refs))
                expected_cleanup_calls = 1 if mode.startswith("register") else 2
                self.assertEqual(client.unregister_calls, expected_cleanup_calls)
                del table, client
                gc.collect()
                self.assertTrue(all(ref() is not None for ref in refs))

    def test_native_cleanup_status_and_publication_are_distinct(self):
        for mode, rc, published in (
            ("success", 0, 2),
            ("preflight", -1, 0),
            ("upload_fail", -3, 1),
            ("cleanup_fail", -2, 2),
            ("unregister_throw", -2, 2),
            ("register_fail", -3, 0),
            ("register_partial", -3, 0),
            ("register_throw", -3, 0),
            ("upload_cleanup_fail", -3, 1),
            ("upload_throw", -3, 0),
            ("upload_throw_after_publish", -3, 2),
            ("malformed", -3, 2),
            ("mixed_conflict", -1, 2),
            ("competing_success", -1, 2),
            ("conflict_cleanup_fail", -3, 2),
        ):
            with self.subTest(mode=mode):
                client, table = self.make_store(mode)
                # Keep raw C++ input memory alive even if the call throws.
                arrays = [np.zeros((4, 8), dtype=np.uint8) for _ in range(2)]
                self.assertEqual(native.populate_raw(table, arrays), rc)
                self.assertEqual(client.published_count, published)
                self.assertEqual(client.remove_calls, 0)

    def test_flexible_replication_is_rejected_before_registration(self):
        client, table = self.make_store("success")
        config = native.ReplicateConfig()
        config.replica_num = 1
        config.nof_replica_num = 1
        arrays = [np.zeros((4, 8), dtype=np.uint8) for _ in range(2)]
        with self.assertRaises(RuntimeError):
            table.populate(1, arrays, config)
        self.assertEqual(client.register_calls, 0)
        self.assertEqual(client.published_count, 0)

    def test_populate_snapshots_config_before_releasing_gil(self):
        client, table = self.make_store("config_mutation")
        config = native.ReplicateConfig()
        config.replica_num = 1
        config.nof_replica_num = 0
        arrays = [np.zeros((4, 8), dtype=np.uint8) for _ in range(2)]
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(table.populate, 1, arrays, config)
            try:
                self.assertTrue(client.wait_for_registration())
                config.nof_replica_num = 1
            finally:
                client.resume_registration()
            result.result(timeout=5)
        self.assertEqual(client.last_nof_replica_num, 0)
        self.assertEqual(config.nof_replica_num, 1)

    def test_failed_write_does_not_remove_competing_publication(self):
        for mode in ("competing_throw", "competing_success"):
            with self.subTest(mode=mode):
                client, table = self.make_store(mode)
                self.populate_temporaries(table)
                self.assertEqual(client.published_count, 2)
                self.assertEqual(client.remove_calls, 0)

    def test_partial_removal_invalidates_snapshot_even_on_exception(self):
        for mode in ("remove_fail", "remove_throw"):
            with self.subTest(mode=mode):
                client, table = self.make_store(mode)
                ids = np.zeros((1, 1, 2), dtype=np.int64)
                output = np.zeros((1, 1, 2, 8), dtype=np.uint8)
                table.lookup_into(1, ids, output)
                table.lookup_into(1, ids, output)
                self.assertEqual(client.query_calls, 1)
                with self.assertRaises(RuntimeError):
                    table.remove_from_store(1, True)
                self.assertEqual(client.published_count, 1)
                with self.assertRaises(RuntimeError):
                    table.lookup_into(1, ids, output)
                self.assertEqual(client.query_calls, 2)
                self.assertEqual(client.read_calls, 2)
                self.assertFalse(np.any(output))

    def test_local_binding_retains_array_snapshot_not_mutable_list(self):
        config = native.EngramStoreConfig()
        config.table_vocab_sizes = [4, 4]
        config.row_bytes = 8
        table = native.EngramStore({1: config})
        arrays = [np.full((4, 8), h + 1, dtype=np.uint8) for h in range(2)]
        refs = [weakref.ref(a) for a in arrays]
        table.bind_local(1, arrays)
        arrays.clear()
        gc.collect()
        self.assertTrue(all(ref() is not None for ref in refs))
        ids = np.zeros((1, 1, 2), dtype=np.int64)
        output = np.zeros((1, 1, 2, 8), dtype=np.uint8)
        table.lookup_into(1, ids, output)
        np.testing.assert_array_equal(output[0, 0, :, 0], [1, 2])
        del table
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))

    def test_inflight_snapshot_read_excludes_force_removal(self):
        self.assertTrue(native.read_excludes_remove())

    def test_later_success_does_not_release_uncertain_owners(self):
        client, table = self.make_store("register_fail")
        retained = self.populate_temporaries(table)
        client.set_mode("register_fail")
        retained_again = self.populate_temporaries(table)
        client.set_mode("success")
        released = self.populate_temporaries(table, expect_error=False)
        self.assertTrue(all(ref() is not None for ref in retained))
        self.assertTrue(all(ref() is not None for ref in retained_again))
        self.assertTrue(all(ref() is None for ref in released))
        del table, client
        gc.collect()
        self.assertTrue(all(ref() is not None for ref in retained))
        self.assertTrue(all(ref() is not None for ref in retained_again))


if __name__ == "__main__":
    unittest.main(verbosity=2)
