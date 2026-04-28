"""Smoke test for the cost-aware routing Python bindings.

Verifies the (value_or_None, err_code) tuple contract introduced after the
review of the LPM PR #2009 — i.e. matches the same binding convention so
Python callers can disambiguate "empty result" from "RPC failed".

Skipped automatically when the environment cannot stand up a local store
(e.g. CI sandbox without etcd / metadata server).
"""

import os
import sys
import unittest

from mooncake.store import MooncakeDistributedStore

# ErrorCode constants must stay in sync with mooncake-store/include/types.h.
COST_QUERY_DISABLED = -1700
COST_REQUEST_EMPTY = -1701
COST_REQUEST_TOO_LARGE = -1702


def _make_store():
    store = MooncakeDistributedStore()
    rc = store.setup(
        os.environ.get("LOCAL_HOSTNAME", "localhost:12345"),
        os.environ.get("METADATA_SERVER", "P2PHANDSHAKE"),
        int(os.environ.get("GLOBAL_SEGMENT_SIZE", str(64 * 1024 * 1024))),
        int(os.environ.get("LOCAL_BUFFER_SIZE", str(16 * 1024 * 1024))),
        os.environ.get("PROTOCOL", "tcp"),
        os.environ.get("DEVICE_NAME", ""),
        os.environ.get("MOONCAKE_MASTER", "localhost:50051"),
    )
    if rc != 0:
        raise unittest.SkipTest(
            f"store.setup failed with rc={rc}; skipping cost-aware smoke test")
    return store


class CostAwareSmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = _make_store()

    def test_query_cost_empty_returns_tuple_of_none_and_err(self):
        result = self.store.query_cost([])
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        ranked, err = result
        self.assertIsNone(ranked)
        self.assertIn(err, (COST_REQUEST_EMPTY, COST_QUERY_DISABLED))

    def test_query_cost_oversized_returns_request_too_large(self):
        big = [f"seg-{i}" for i in range(1025)]
        ranked, err = self.store.query_cost(big)
        self.assertIsNone(ranked)
        self.assertIn(err, (COST_REQUEST_TOO_LARGE, COST_QUERY_DISABLED))

    def test_query_cost_unknown_segments_default_returns_empty_list(self):
        ranked, err = self.store.query_cost(
            ["nonexistent-segment-a", "nonexistent-segment-b"],
            client_host="127.0.0.1",
            client_zone="",
            request_size_bytes=0,
            include_unmounted=False,
        )
        if err == COST_QUERY_DISABLED:
            self.skipTest("cost-aware routing disabled on this master")
        self.assertEqual(err, 0)
        self.assertIsNotNone(ranked)
        self.assertEqual(ranked, [])

    def test_query_cost_include_unmounted_returns_entries(self):
        ranked, err = self.store.query_cost(
            ["nonexistent-1", "nonexistent-2"],
            include_unmounted=True,
        )
        if err == COST_QUERY_DISABLED:
            self.skipTest("cost-aware routing disabled on this master")
        self.assertEqual(err, 0)
        self.assertIsNotNone(ranked)
        self.assertEqual(len(ranked), 2)
        for entry in ranked:
            # tuple shape: (segment_name, cost_score, link_class,
            #               storage_tier, inflight, found)
            self.assertEqual(len(entry), 6)
            self.assertFalse(entry[5])

    def test_inflight_begin_end_returns_int_or_none(self):
        result = self.store.inflight_begin("smoke-seg")
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        count, err = result
        if err == COST_QUERY_DISABLED:
            self.skipTest("cost-aware routing disabled on this master")
        self.assertEqual(err, 0)
        self.assertIsNotNone(count)
        self.assertGreaterEqual(count, 1)

        end_count, end_err = self.store.inflight_end("smoke-seg")
        self.assertEqual(end_err, 0)
        self.assertIsNotNone(end_count)

    def test_inflight_begin_empty_name_returns_error(self):
        count, err = self.store.inflight_begin("")
        self.assertIsNone(count)
        self.assertIn(err, (COST_REQUEST_EMPTY, COST_QUERY_DISABLED))


if __name__ == "__main__":
    sys.exit(0 if unittest.main(exit=False).result.wasSuccessful() else 1)
