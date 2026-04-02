#!/usr/bin/env python3
"""
Test suite for the Mooncake Engram backend.

Tests cover:
1. Metadata and physical table layout
2. Store population and row-id lookup
3. Exact lookup correctness against stored tables
4. Cleanup and error handling
"""

import os
import sys
import unittest

import numpy as np

from mooncake.mooncake_config import MooncakeConfig

GLOBAL_STORE = None
STORE_MODULE = None


def import_store_module():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build_store = os.path.join(repo_root, "build", "mooncake-integration")

    if not os.path.isdir(build_store):
        raise ImportError(
            "build/mooncake-integration not found. Build Mooncake with store support: "
            "cd build && cmake .. -DWITH_STORE=ON && make -j 128"
        )

    if build_store not in sys.path:
        sys.path.insert(0, build_store)

    import importlib

    store_module = importlib.import_module("store")
    print("✅ store.so imported successfully from build/mooncake-integration")
    return store_module


def create_store_connection(store_module):
    MooncakeDistributedStore = store_module.MooncakeDistributedStore
    store = MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
    print(f"[{os.getpid()}] Connecting to Mooncake Master at {config.master_server_address}...")

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

    print("✅ Store connection established")
    return store


def setUpModule():
    global GLOBAL_STORE, STORE_MODULE
    STORE_MODULE = import_store_module()
    GLOBAL_STORE = create_store_connection(STORE_MODULE)


def tearDownModule():
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None


class EngramTestBase(unittest.TestCase):
    def setUp(self):
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")

        self.store = GLOBAL_STORE
        self.Engram = STORE_MODULE.Engram
        self.EngramConfig = STORE_MODULE.EngramConfig
        self.store.remove_all()

    def create_config(self):
        cfg = self.EngramConfig()
        cfg.table_vocab_sizes = [17, 19, 23, 29]
        cfg.embedding_dim = 8
        return cfg

    def create_engram(self, layer_id=1):
        cfg = self.create_config()
        engram = self.Engram(layer_id=layer_id, config=cfg, store=self.store)
        return cfg, engram

    def make_embedding_tables(self, engram):
        embed_dim = engram.get_embedding_dim()
        tables = []
        for head_idx, vocab_size in enumerate(engram.get_table_vocab_sizes()):
            base = np.arange(vocab_size * embed_dim, dtype=np.float32).reshape(vocab_size, embed_dim)
            tables.append(base + head_idx * 1000)
        return tables

    def populate_store(self, engram):
        tables = self.make_embedding_tables(engram)
        engram.populate(tables)
        return tables


class TestEngramMetadata(EngramTestBase):
    def test_creation_and_metadata(self):
        cfg, engram = self.create_engram()
        self.assertEqual(engram.get_num_heads(), len(cfg.table_vocab_sizes))
        self.assertEqual(engram.get_embedding_dim(), cfg.embedding_dim)
        self.assertEqual(engram.get_table_vocab_sizes(), cfg.table_vocab_sizes)
        self.assertEqual(len(engram.get_store_keys()), len(cfg.table_vocab_sizes))


class TestStorePopulateAndLookup(EngramTestBase):
    def test_populate_and_lookup_shape(self):
        _, engram = self.create_engram()
        self.populate_store(engram)

        row_ids = [
            [[0, 1, 2, 3], [4, 5, 6, 7]],
            [[1, 2, 3, 4], [8, 9, 10, 11]],
        ]
        output = engram.lookup(row_ids)

        expected_shape = (
            len(row_ids),
            len(row_ids[0]),
            engram.get_num_heads(),
            engram.get_embedding_dim(),
        )
        self.assertEqual(output.shape, expected_shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))

    def test_lookup_matches_stored_rows(self):
        _, engram = self.create_engram()
        tables = self.populate_store(engram)

        row_ids = [[[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]]
        output = np.asarray(engram.lookup(row_ids))

        for pos in range(len(row_ids[0])):
            for head in range(engram.get_num_heads()):
                idx = row_ids[0][pos][head]
                np.testing.assert_allclose(output[0, pos, head], tables[head][idx])

    def test_remove_from_store(self):
        _, engram = self.create_engram()
        self.populate_store(engram)

        removed = engram.remove_from_store(force=True)
        self.assertEqual(removed, engram.get_num_heads())

        for key in engram.get_store_keys():
            self.assertEqual(self.store.is_exist(key), 0)

        self.assertEqual(engram.remove_from_store(force=True), 0)


class TestErrorHandling(EngramTestBase):
    def test_populate_rejects_wrong_table_shape(self):
        _, engram = self.create_engram()
        tables = self.make_embedding_tables(engram)
        tables[0] = tables[0][:-1]
        with self.assertRaises(Exception):
            engram.populate(tables)

    def test_lookup_rejects_empty_input(self):
        _, engram = self.create_engram()
        self.populate_store(engram)
        with self.assertRaises(Exception):
            engram.lookup([])

    def test_lookup_rejects_out_of_range_row_id(self):
        _, engram = self.create_engram()
        self.populate_store(engram)
        with self.assertRaises(Exception):
            engram.lookup([[[999, 1, 2, 3]]])


if __name__ == "__main__":
    unittest.main(verbosity=2)
