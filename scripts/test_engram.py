#!/usr/bin/env python3
"""
Test suite for the Mooncake Engram storage/query path.

Tests cover:
1. Configuration classes and construction
2. Exact per-head table sizing exposed by get_table_vocab_sizes()
3. Store population and embedding query
4. Query shape / hash shape correctness
5. Error handling for invalid table shapes
"""

import os
import sys
import unittest

import numpy as np

from mooncake.mooncake_config import MooncakeConfig

GLOBAL_STORE = None
STORE_MODULE = None


def import_store_module():
    """Import store.so module from build/mooncake-integration."""
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    build_store = os.path.join(repo_root, "build", "mooncake-integration")

    if not os.path.isdir(build_store):
        raise ImportError(
            "build/mooncake-integration not found. Build Mooncake with Engram: "
            "cd build && cmake .. -DWITH_STORE=ON && make -j 128"
        )

    if build_store not in sys.path:
        sys.path.insert(0, build_store)

    import importlib

    store_module = importlib.import_module("store")
    print("✅ store.so imported successfully from build/mooncake-integration")
    return store_module


def import_engram(store_module):
    return store_module.Engram, store_module.EngramConfig, store_module.BackboneConfig


def create_store_connection(store_module):
    """Create and connect to the Store."""
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
    return store, config


def setUpModule():
    global GLOBAL_STORE, STORE_MODULE
    STORE_MODULE = import_store_module()
    GLOBAL_STORE, _ = create_store_connection(STORE_MODULE)


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
        Engram, EngramConfig, BackboneConfig = import_engram(STORE_MODULE)
        self.Engram = Engram
        self.EngramConfig = EngramConfig
        self.BackboneConfig = BackboneConfig
        self.store.remove_all()

    def create_default_configs(self):
        cfg = self.EngramConfig()
        cfg.tokenizer_name_or_path = ""
        cfg.engram_vocab_size = [1000, 1000]
        cfg.max_ngram_size = 3
        cfg.n_embed_per_ngram = 64
        cfg.n_head_per_ngram = 4
        cfg.layer_ids = [1, 15]
        cfg.pad_id = 2
        cfg.seed = 0
        cfg.kernel_size = 4

        bb = self.BackboneConfig()
        bb.hidden_size = 256
        bb.hc_mult = 4
        bb.vocab_size = 128000
        bb.num_layers = 6
        return cfg, bb

    def create_engram(self, layer_id=1):
        cfg, bb = self.create_default_configs()
        engram = self.Engram(layer_id=layer_id, config=cfg, backbone_cfg=bb, store=self.store)
        return cfg, bb, engram

    def make_embedding_tables(self, engram):
        embed_dim = engram.get_embedding_dim()
        tables = []
        for head_idx, vocab_size in enumerate(engram.get_table_vocab_sizes()):
            base = np.arange(vocab_size * embed_dim, dtype=np.float32).reshape(vocab_size, embed_dim)
            tables.append(base + head_idx)
        return tables

    def populate_store(self, engram):
        tables = self.make_embedding_tables(engram)
        engram.populate_store_from_buffers(tables)
        return tables


class TestEngramMetadata(EngramTestBase):
    def test_creation_and_metadata(self):
        cfg, _, engram = self.create_engram()
        num_heads = (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram
        self.assertEqual(engram.get_num_heads(), num_heads)
        self.assertEqual(engram.get_embedding_dim(), cfg.n_embed_per_ngram // cfg.n_head_per_ngram)
        self.assertEqual(len(engram.get_store_keys()), num_heads)

    def test_table_vocab_sizes_use_per_head_primes(self):
        cfg, _, engram = self.create_engram()
        table_vocab_sizes = engram.get_table_vocab_sizes()
        self.assertEqual(len(table_vocab_sizes), (cfg.max_ngram_size - 1) * cfg.n_head_per_ngram)
        self.assertTrue(all(v >= 1000 for v in table_vocab_sizes))
        self.assertGreater(max(table_vocab_sizes), max(cfg.engram_vocab_size))
        self.assertEqual(len(set(table_vocab_sizes)), len(table_vocab_sizes))


class TestStorePopulateAndQuery(EngramTestBase):
    def test_populate_and_query_shape(self):
        _, _, engram = self.create_engram()
        self.populate_store(engram)

        input_ids = [[1, 2, 3, 4], [4, 3, 2, 1]]
        output = engram.query(input_ids)

        expected_shape = (
            len(input_ids),
            len(input_ids[0]),
            engram.get_num_heads(),
            engram.get_embedding_dim(),
        )
        self.assertEqual(output.shape, expected_shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))

    def test_hash_and_query_are_consistent(self):
        _, _, engram = self.create_engram()
        tables = self.populate_store(engram)

        input_ids = [[10, 11, 12, 13]]
        hash_ids = engram.hash_input_ids(input_ids)
        output = engram.query(input_ids)

        self.assertEqual(hash_ids.shape, (1, 4, engram.get_num_heads()))
        self.assertEqual(output.shape, (1, 4, engram.get_num_heads(), engram.get_embedding_dim()))

        for pos in range(hash_ids.shape[1]):
            for head in range(hash_ids.shape[2]):
                idx = hash_ids[0, pos, head]
                np.testing.assert_allclose(output[0, pos, head], tables[head][idx])

    def test_query_with_workspace(self):
        _, _, engram = self.create_engram()
        self.populate_store(engram)

        input_ids = [[1, 2, 3, 4]]
        ws_bytes = engram.get_query_workspace_size(len(input_ids), len(input_ids[0]))
        workspace = np.empty(ws_bytes, dtype=np.uint8)
        output = engram.query(input_ids, workspace)

        self.assertEqual(output.shape, (1, 4, engram.get_num_heads(), engram.get_embedding_dim()))

    def test_remove_from_store(self):
        _, _, engram = self.create_engram()
        self.populate_store(engram)

        removed = engram.remove_from_store(force=True)
        self.assertEqual(removed, engram.get_num_heads())

        for key in engram.get_store_keys():
            self.assertEqual(self.store.is_exist(key), 0)

        self.assertEqual(engram.remove_from_store(force=True), 0)


class TestErrorHandling(EngramTestBase):
    def test_populate_rejects_wrong_table_shape(self):
        _, _, engram = self.create_engram()
        tables = self.make_embedding_tables(engram)
        tables[0] = tables[0][:-1]
        with self.assertRaises(Exception):
            engram.populate_store_from_buffers(tables)

    def test_query_rejects_empty_input(self):
        _, _, engram = self.create_engram()
        self.populate_store(engram)
        with self.assertRaises(Exception):
            engram.query([])


if __name__ == "__main__":
    unittest.main(verbosity=2)
