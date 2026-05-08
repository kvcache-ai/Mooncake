#!/usr/bin/env python3
"""
Integration tests for the Mooncake EngramStore backend.

This suite validates:
1. metadata-only construction and store-backed construction
2. populate / lookup / remove on the simplified Store interface
3. Python-list and NumPy row-id lookup paths
4. error handling and cleanup behavior
"""

import importlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = os.environ.get("MOONCAKE_BUILD_DIR", "build")
BUILD_STORE = REPO_ROOT / BUILD_DIR / "mooncake-integration"
WHEEL_DIR = REPO_ROOT / "mooncake-wheel"
MASTER_BINARY = REPO_ROOT / BUILD_DIR / "mooncake-store" / "src" / "mooncake_master"

for path in (BUILD_STORE, WHEEL_DIR):
    if path.is_dir() and str(path) not in sys.path:
        sys.path.insert(0, str(path))

from mooncake.mooncake_config import MooncakeConfig

GLOBAL_STORE = None
STORE_MODULE = None
TEST_CONFIG = None
MASTER_PROCESS = None
MASTER_LOG_PATH = None
MASTER_LOG_FILE = None


def import_store_module():
    if not BUILD_STORE.is_dir():
        raise ImportError(
            f"{BUILD_STORE} not found. Build Mooncake with store support: "
            "cd build && cmake .. -DWITH_STORE=ON && make -j 128"
        )

    store_module = importlib.import_module("store")
    print(f"✅ store.so imported successfully from {BUILD_STORE}")
    return store_module


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def resolve_master_binary():
    if MASTER_BINARY.is_file():
        return str(MASTER_BINARY)

    binary = shutil.which("mooncake_master")
    if binary:
        return binary

    raise FileNotFoundError(
        "Cannot find mooncake_master. Build it first or install it into PATH."
    )


def wait_for_tcp_port(host, port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for TCP port {host}:{port}")


def wait_for_metadata_server(metadata_url, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                metadata_url + "?key=engram_store_test_probe", timeout=1.0
            ):
                return
        except urllib.error.HTTPError as exc:
            if exc.code in (200, 400, 404):
                return
        except urllib.error.URLError:
            time.sleep(0.1)
        else:
            return
    raise RuntimeError(f"Timed out waiting for metadata server {metadata_url}")


def read_master_log():
    if MASTER_LOG_PATH is None or not os.path.exists(MASTER_LOG_PATH):
        return "<no master log available>"
    with open(MASTER_LOG_PATH, "r", encoding="utf-8", errors="replace") as fin:
        return fin.read()


def start_local_master():
    global MASTER_PROCESS, MASTER_LOG_FILE, MASTER_LOG_PATH

    rpc_port = find_free_port()
    http_port = find_free_port()
    metrics_port = find_free_port()
    master_binary = resolve_master_binary()

    fd, MASTER_LOG_PATH = tempfile.mkstemp(prefix="engram_store-master-", suffix=".log")
    os.close(fd)

    MASTER_LOG_FILE = open(MASTER_LOG_PATH, "w", encoding="utf-8")
    cmd = [
        master_binary,
        "--default_kv_lease_ttl=500",
        "--enable_http_metadata_server=true",
        "--rpc_address=127.0.0.1",
        f"--rpc_port={rpc_port}",
        "--http_metadata_server_host=127.0.0.1",
        f"--http_metadata_server_port={http_port}",
        f"--metrics_port={metrics_port}",
    ]
    MASTER_PROCESS = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=MASTER_LOG_FILE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    metadata_url = f"http://127.0.0.1:{http_port}/metadata"
    try:
        wait_for_tcp_port("127.0.0.1", rpc_port)
        wait_for_metadata_server(metadata_url)
    except Exception:
        log_output = read_master_log()
        stop_local_master()
        raise RuntimeError(
            "Failed to start local mooncake_master for EngramStore tests.\n"
            f"Log output:\n{log_output}"
        )

    print(
        "✅ Started local mooncake_master for EngramStore tests at "
        f"127.0.0.1:{rpc_port} with metadata {metadata_url}"
    )
    return MooncakeConfig(
        local_hostname="127.0.0.1",
        metadata_server=metadata_url,
        global_segment_size=128 * 1024 * 1024,
        local_buffer_size=64 * 1024 * 1024,
        protocol="tcp",
        device_name="",
        master_server_address=f"127.0.0.1:{rpc_port}",
    )


def stop_local_master():
    global MASTER_PROCESS, MASTER_LOG_FILE, MASTER_LOG_PATH

    if MASTER_PROCESS is not None:
        if MASTER_PROCESS.poll() is None:
            MASTER_PROCESS.terminate()
            try:
                MASTER_PROCESS.wait(timeout=10)
            except subprocess.TimeoutExpired:
                MASTER_PROCESS.kill()
                MASTER_PROCESS.wait(timeout=5)
        MASTER_PROCESS = None

    if MASTER_LOG_FILE is not None:
        MASTER_LOG_FILE.close()
        MASTER_LOG_FILE = None

    if MASTER_LOG_PATH and os.path.exists(MASTER_LOG_PATH):
        os.remove(MASTER_LOG_PATH)
        MASTER_LOG_PATH = None


def load_test_config():
    if os.getenv("MOONCAKE_CONFIG_PATH") or os.getenv("MOONCAKE_MASTER"):
        print("Using Mooncake configuration from environment")
        return MooncakeConfig.load_from_env()

    return start_local_master()


def create_store_connection(store_module, config):
    store = store_module.MooncakeDistributedStore()
    print(
        f"[{os.getpid()}] Connecting to Mooncake Master at "
        f"{config.master_server_address}..."
    )

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
    global GLOBAL_STORE, STORE_MODULE, TEST_CONFIG
    STORE_MODULE = import_store_module()
    try:
        TEST_CONFIG = load_test_config()
        GLOBAL_STORE = create_store_connection(STORE_MODULE, TEST_CONFIG)
    except Exception:
        stop_local_master()
        raise


def tearDownModule():
    global GLOBAL_STORE
    if GLOBAL_STORE:
        print("\nClosing global store connection...")
        GLOBAL_STORE.close()
        GLOBAL_STORE = None
    stop_local_master()


class EngramStoreTestBase(unittest.TestCase):
    def setUp(self):
        if GLOBAL_STORE is None:
            self.skipTest("Store not initialized")

        self.store = GLOBAL_STORE
        self.EngramStore = STORE_MODULE.EngramStore
        self.EngramStoreConfig = STORE_MODULE.EngramStoreConfig
        self._created_engram_stores = []
        self._next_layer_id = uuid.uuid4().int & 0x7FFFFFFF

    def tearDown(self):
        for engram_store in reversed(self._created_engram_stores):
            try:
                engram_store.remove_from_store(force=True)
            except Exception as exc:
                print(f"Warning: failed to clean up EngramStore test layer: {exc}")

    def create_config(self):
        cfg = self.EngramStoreConfig()
        cfg.table_vocab_sizes = [17, 19, 23, 29]
        cfg.embedding_dim = 8
        return cfg

    def create_engram_store(self, layer_id=None, store_marker=Ellipsis):
        if layer_id is None:
            layer_id = self._next_layer_id
            self._next_layer_id += 1

        cfg = self.create_config()
        if store_marker is Ellipsis:
            engram_store = self.EngramStore(layer_id=layer_id, config=cfg, store=self.store)
            self._created_engram_stores.append(engram_store)
        elif store_marker is None:
            engram_store = self.EngramStore(layer_id=layer_id, config=cfg)
        else:
            engram_store = self.EngramStore(layer_id=layer_id, config=cfg, store=store_marker)
            self._created_engram_stores.append(engram_store)
        return cfg, engram_store

    def make_embedding_tables(self, engram_store):
        embed_dim = engram_store.get_embedding_dim()
        tables = []
        for head_idx, vocab_size in enumerate(engram_store.get_table_vocab_sizes()):
            base = np.arange(vocab_size * embed_dim, dtype=np.float32).reshape(
                vocab_size, embed_dim
            )
            tables.append(base + head_idx * 1000)
        return tables

    def populate_store(self, engram_store):
        tables = self.make_embedding_tables(engram_store)
        engram_store.populate(tables)
        return tables


class TestEngramStoreMetadata(EngramStoreTestBase):
    def test_creation_and_metadata(self):
        cfg, engram_store = self.create_engram_store()
        self.assertEqual(engram_store.get_num_heads(), len(cfg.table_vocab_sizes))
        self.assertEqual(engram_store.get_embedding_dim(), cfg.embedding_dim)
        self.assertEqual(engram_store.get_table_vocab_sizes(), cfg.table_vocab_sizes)
        self.assertEqual(len(engram_store.get_store_keys()), len(cfg.table_vocab_sizes))

    def test_creation_without_store_keeps_metadata_accessible(self):
        layer_id = self._next_layer_id
        cfg, engram_store = self.create_engram_store(layer_id=layer_id, store_marker=None)
        self.assertEqual(engram_store.get_num_heads(), len(cfg.table_vocab_sizes))
        self.assertEqual(engram_store.get_embedding_dim(), cfg.embedding_dim)
        self.assertEqual(engram_store.get_store_keys()[0], f"engram:l{layer_id}:h0")


class TestStorePopulateAndLookup(EngramStoreTestBase):
    def test_populate_and_lookup_shape_with_python_lists(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)

        row_ids = [
            [[0, 1, 2, 3], [4, 5, 6, 7]],
            [[1, 2, 3, 4], [8, 9, 10, 11]],
        ]
        output = engram_store.lookup(row_ids)

        expected_shape = (
            len(row_ids),
            len(row_ids[0]),
            engram_store.get_num_heads(),
            engram_store.get_embedding_dim(),
        )
        self.assertEqual(output.shape, expected_shape)
        self.assertFalse(np.any(np.isnan(output)))
        self.assertFalse(np.any(np.isinf(output)))

    def test_lookup_matches_stored_rows_from_numpy_ids(self):
        _, engram_store = self.create_engram_store()
        tables = self.populate_store(engram_store)

        row_ids = np.array(
            [[[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]], dtype=np.int64
        )
        output = np.asarray(engram_store.lookup(row_ids))

        for pos in range(row_ids.shape[1]):
            for head in range(engram_store.get_num_heads()):
                idx = row_ids[0, pos, head]
                np.testing.assert_allclose(output[0, pos, head], tables[head][idx])

    def test_lookup_list_and_numpy_paths_match(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)

        row_ids_list = [
            [[0, 1, 2, 3], [4, 5, 6, 7]],
            [[1, 2, 3, 4], [8, 9, 10, 11]],
        ]
        row_ids_numpy = np.asarray(row_ids_list, dtype=np.int64)

        output_from_list = np.asarray(engram_store.lookup(row_ids_list))
        output_from_numpy = np.asarray(engram_store.lookup(row_ids_numpy))
        np.testing.assert_allclose(output_from_list, output_from_numpy)

    def test_remove_from_store(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)

        removed = engram_store.remove_from_store(force=True)
        self.assertEqual(removed, engram_store.get_num_heads())

        for key in engram_store.get_store_keys():
            self.assertEqual(self.store.is_exist(key), 0)

        self.assertEqual(engram_store.remove_from_store(force=True), 0)


class TestErrorHandling(EngramStoreTestBase):
    def test_populate_rejects_reusing_existing_layer_keys(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        with self.assertRaises(Exception):
            engram_store.populate(self.make_embedding_tables(engram_store))

    def test_lookup_rejects_missing_tables(self):
        _, engram_store = self.create_engram_store()
        with self.assertRaises(Exception):
            engram_store.lookup([[[0, 1, 2, 3]]])

    def test_populate_rejects_wrong_table_shape(self):
        _, engram_store = self.create_engram_store()
        tables = self.make_embedding_tables(engram_store)
        tables[0] = tables[0][:-1]
        with self.assertRaises(Exception):
            engram_store.populate(tables)

    def test_lookup_rejects_empty_input(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        with self.assertRaises(Exception):
            engram_store.lookup([])

    def test_lookup_rejects_wrong_head_dimension(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        row_ids = np.zeros((1, 1, engram_store.get_num_heads() - 1), dtype=np.int64)
        with self.assertRaises(Exception):
            engram_store.lookup(row_ids)

    def test_lookup_rejects_out_of_range_row_id(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        with self.assertRaises(Exception):
            engram_store.lookup([[[999, 1, 2, 3]]])


if __name__ == "__main__":
    unittest.main(verbosity=2)
