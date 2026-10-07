#!/usr/bin/env python3
"""
Integration tests for the Mooncake EngramStore backend.

This suite validates:
1. metadata-only construction and store-backed construction
2. populate / lookup / remove on the simplified Store interface
3. caller-owned registered output buffers and strict NumPy inputs
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
from contextlib import contextmanager
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

from mooncake.mooncake_config import MooncakeConfig  # noqa: E402

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
                for layer_id in engram_store.get_layer_ids():
                    engram_store.remove_from_store(layer_id, force=True)
            except Exception as exc:
                print(f"Warning: failed to clean up EngramStore test layer: {exc}")

    def create_config(self):
        cfg = self.EngramStoreConfig()
        cfg.table_vocab_sizes = [17, 19, 23, 29]
        cfg.row_bytes = 32
        return cfg

    def create_engram_store(self, layer_id=None, store_marker=Ellipsis):
        if layer_id is None:
            layer_id = self._next_layer_id
            self._next_layer_id += 1

        cfg = self.create_config()
        self.layer_id = layer_id
        if store_marker is Ellipsis:
            engram_store = self.EngramStore(layers={layer_id: cfg}, store=self.store)
            self._created_engram_stores.append(engram_store)
        elif store_marker is None:
            engram_store = self.EngramStore(layers={layer_id: cfg})
        else:
            engram_store = self.EngramStore(layers={layer_id: cfg}, store=store_marker)
            self._created_engram_stores.append(engram_store)
        return cfg, engram_store

    def make_embedding_tables(self, engram_store):
        row_bytes = engram_store.get_row_bytes(self.layer_id)
        tables = []
        for head_idx, vocab_size in enumerate(
            engram_store.get_table_vocab_sizes(self.layer_id)
        ):
            base = np.arange(vocab_size * row_bytes, dtype=np.int64).reshape(
                vocab_size, row_bytes
            )
            tables.append((base + head_idx * 37).astype(np.uint8))
        return tables

    def populate_store(self, engram_store):
        tables = self.make_embedding_tables(engram_store)
        engram_store.populate(self.layer_id, tables)
        return tables

    @contextmanager
    def registered_output(self, table, shape, layer_id=None):
        layer_id = self.layer_id if layer_id is None else layer_id
        output = np.empty((*shape, table.get_row_bytes(layer_id)), dtype=np.uint8)
        if output.nbytes:
            self.assertEqual(
                self.store.register_buffer(output.ctypes.data, output.nbytes), 0
            )
        try:
            yield output
        finally:
            if output.nbytes:
                self.assertEqual(self.store.unregister_buffer(output.ctypes.data), 0)

    def read_rows(self, table, row_ids):
        ids = np.asarray(row_ids, dtype=np.int64)
        with self.registered_output(table, ids.shape) as output:
            table.lookup_into(self.layer_id, ids, output)
        return output


class TestEngramStoreMetadata(EngramStoreTestBase):
    def test_local_tables_lifetime_and_bounds(self):
        import gc
        import weakref

        cfg = self.create_config()
        cfg.row_bytes = 264
        table = self.EngramStore({1: cfg, 14: cfg})
        arrays = [
            np.arange(n * 264, dtype=np.uint32).astype(np.uint8).reshape(n, 264)
            for n in cfg.table_vocab_sizes
        ]
        expected = np.stack([a[-1].copy() for a in arrays])
        refs = [weakref.ref(a) for a in arrays]
        for a in arrays:
            a.flags.writeable = False
        table.bind_local(1, arrays)
        del arrays, a
        gc.collect()
        assert all(ref() is not None for ref in refs)
        ids = (np.array(cfg.table_vocab_sizes, dtype=np.int64) - 1)[None, None]
        output = np.empty((1, 1, len(cfg.table_vocab_sizes), 264), dtype=np.uint8)
        table.lookup_into(1, ids, output)  # No Store or registered output.
        np.testing.assert_array_equal(output[0, 0], expected)
        with self.assertRaises(RuntimeError):
            table.lookup_into(14, ids, output)
        ids[0, 0, 0] += 1
        with self.assertRaises(RuntimeError):
            table.lookup_into(1, ids, output)
        assert not output.any()
        del table
        gc.collect()
        assert all(ref() is None for ref in refs)

    def test_creation_and_metadata(self):
        cfg, engram_store = self.create_engram_store()
        self.assertEqual(
            engram_store.get_num_heads(self.layer_id), len(cfg.table_vocab_sizes)
        )
        self.assertEqual(engram_store.get_row_bytes(self.layer_id), cfg.row_bytes)
        self.assertEqual(
            engram_store.get_table_vocab_sizes(self.layer_id), cfg.table_vocab_sizes
        )
        self.assertEqual(
            len(engram_store.get_store_keys(self.layer_id)), len(cfg.table_vocab_sizes)
        )

    def test_creation_without_store_keeps_metadata_accessible(self):
        layer_id = self._next_layer_id
        cfg, engram_store = self.create_engram_store(
            layer_id=layer_id, store_marker=None
        )
        self.assertEqual(
            engram_store.get_num_heads(self.layer_id), len(cfg.table_vocab_sizes)
        )
        self.assertEqual(engram_store.get_row_bytes(self.layer_id), cfg.row_bytes)
        self.assertEqual(
            engram_store.get_store_keys(self.layer_id)[0], f"engram:l{layer_id}:h0"
        )


class TestStorePopulateAndLookup(EngramStoreTestBase):
    def test_populate_and_lookup_batch_shape(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)

        row_ids = [
            [[0, 1, 2, 3], [4, 5, 6, 7]],
            [[1, 2, 3, 4], [8, 9, 10, 11]],
        ]
        output = self.read_rows(engram_store, row_ids)

        expected_shape = (
            len(row_ids),
            len(row_ids[0]),
            engram_store.get_num_heads(self.layer_id),
            engram_store.get_row_bytes(self.layer_id),
        )
        self.assertEqual(output.shape, expected_shape)
        self.assertEqual(output.dtype, np.uint8)

    def test_lookup_matches_stored_rows_from_numpy_ids(self):
        _, engram_store = self.create_engram_store()
        tables = self.populate_store(engram_store)

        row_ids = np.array(
            [[[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]], dtype=np.int64
        )
        output = np.asarray(self.read_rows(engram_store, row_ids))

        for pos in range(row_ids.shape[1]):
            for head in range(engram_store.get_num_heads(self.layer_id)):
                idx = row_ids[0, pos, head]
                np.testing.assert_array_equal(output[0, pos, head], tables[head][idx])

    def test_lookup_rejects_implicit_input_conversion(self):
        _, table = self.create_engram_store()
        self.populate_store(table)
        ids = np.zeros((1, 2, table.get_num_heads(self.layer_id)), dtype=np.int64)
        with self.registered_output(table, ids.shape) as output:
            for bad_ids in (ids.tolist(), ids.astype(np.int32), ids[..., ::-1]):
                with self.subTest(input_type=type(bad_ids)), self.assertRaises(
                    TypeError
                ):
                    table.lookup_into(self.layer_id, bad_ids, output)

    def test_remove_from_store(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)

        removed = engram_store.remove_from_store(self.layer_id, force=True)
        self.assertEqual(removed, engram_store.get_num_heads(self.layer_id))

        for key in engram_store.get_store_keys(self.layer_id):
            self.assertEqual(self.store.is_exist(key), 0)

        self.assertEqual(engram_store.remove_from_store(self.layer_id, force=True), 0)


class TestErrorHandling(EngramStoreTestBase):
    def test_populate_rejects_reusing_existing_layer_keys(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        with self.assertRaises(Exception):
            engram_store.populate(
                self.layer_id, self.make_embedding_tables(engram_store)
            )

    def test_lookup_rejects_missing_tables(self):
        _, engram_store = self.create_engram_store()
        with self.assertRaises(Exception):
            self.read_rows(engram_store, [[[0, 1, 2, 3]]])

    def test_populate_rejects_wrong_table_shape(self):
        _, engram_store = self.create_engram_store()
        tables = self.make_embedding_tables(engram_store)
        tables[0] = tables[0][:-1]
        with self.assertRaises(Exception):
            engram_store.populate(self.layer_id, tables)

    def test_lookup_empty_batch_is_noop(self):
        _, table = self.create_engram_store()
        ids = np.empty((1, 0, table.get_num_heads(self.layer_id)), dtype=np.int64)
        output = np.empty(
            (*ids.shape, table.get_row_bytes(self.layer_id)), dtype=np.uint8
        )
        self.assertIsNone(table.lookup_into(self.layer_id, ids, output))

    def test_lookup_rejects_wrong_head_dimension(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        row_ids = np.zeros(
            (1, 1, engram_store.get_num_heads(self.layer_id) - 1), dtype=np.int64
        )
        with self.assertRaises(Exception):
            self.read_rows(engram_store, row_ids)

    def test_lookup_rejects_out_of_range_row_id(self):
        _, engram_store = self.create_engram_store()
        self.populate_store(engram_store)
        with self.assertRaises(Exception):
            self.read_rows(engram_store, [[[999, 1, 2, 3]]])


class TestByteRows(EngramStoreTestBase):
    def create_config(self):
        cfg = super().create_config()
        cfg.row_bytes = 264
        return cfg

    def test_lookup_and_registered_output(self):
        cfg, table = self.create_engram_store()
        buffers = [
            np.arange(n * cfg.row_bytes, dtype=np.int64).astype(np.uint8).reshape(n, -1)
            for n in cfg.table_vocab_sizes
        ]
        replica = STORE_MODULE.ReplicateConfig()
        replica.with_hard_pin = True
        table.populate(self.layer_id, buffers, replica)
        ids = np.array([[[0, 1, 2, 3], [16, 18, 22, 28], [0, 1, 2, 3]]], dtype=np.int64)
        expected = np.stack([buffers[h][ids[..., h]] for h in range(4)], axis=2)
        with self.registered_output(table, ids.shape) as output:
            for _ in range(3):
                output.fill(99)
                self.assertIsNone(table.lookup_into(self.layer_id, ids, output))
                np.testing.assert_array_equal(output, expected)
            bad_ids = ids.copy()
            bad_ids[0, 0, 0] = -1
            with self.assertRaises(RuntimeError):
                table.lookup_into(self.layer_id, bad_ids, output)
            self.assertFalse(output.any())
            # Failed reads must also preserve the caller's registration.
            table.lookup_into(self.layer_id, ids, output)
            np.testing.assert_array_equal(output, expected)
            with self.assertRaises(RuntimeError):
                table.lookup_into(self.layer_id, ids, output[..., ::-1])
            with self.assertRaises(RuntimeError):
                table.lookup_into(self.layer_id, ids, output.astype(np.float32))
            with self.assertRaises(RuntimeError):
                table.lookup_into(self.layer_id, ids, output[..., :-1])
            output.setflags(write=False)
            with self.assertRaises(RuntimeError):
                table.lookup_into(self.layer_id, ids, output)

    def test_populate_does_not_cast(self):
        cfg, table = self.create_engram_store()
        buffers = [
            np.zeros((n, cfg.row_bytes), dtype=np.float32)
            for n in cfg.table_vocab_sizes
        ]
        with self.assertRaises(RuntimeError):
            table.populate(self.layer_id, buffers)
        buffers = [
            np.zeros((n, cfg.row_bytes * 2), dtype=np.uint8)[:, ::2]
            for n in cfg.table_vocab_sizes
        ]
        with self.assertRaises(RuntimeError):
            table.populate(self.layer_id, buffers)

    def test_row_size_validation(self):
        cfg = self.create_config()
        for row_bytes in (0, -1):
            with self.subTest(row_bytes=row_bytes):
                cfg.row_bytes = row_bytes
                with self.assertRaises(ValueError):
                    self.EngramStore({1: cfg})
        cfg.row_bytes = 264
        cfg.table_vocab_sizes = [2**63 - 1]
        with self.assertRaises(ValueError):
            self.EngramStore({1: cfg})


class TestMultipleLayers(EngramStoreTestBase):
    def test_layer_isolation_and_shared_backend(self):
        first, second = self._next_layer_id, self._next_layer_id + 1
        cfg1 = self.create_config()
        cfg2 = self.EngramStoreConfig()
        cfg2.table_vocab_sizes = [7, 11]
        cfg2.row_bytes = 264
        configs = {first: cfg1, second: cfg2}
        table = self.EngramStore(configs, self.store)
        self._created_engram_stores.append(table)
        self.assertEqual(table.get_layer_ids(), [first, second])
        for layer_id, cfg in configs.items():
            arrays = [
                np.full(
                    (n, cfg.row_bytes), h + 31 + (layer_id - first) * 50, dtype=np.uint8
                )
                for h, n in enumerate(cfg.table_vocab_sizes)
            ]
            table.populate(layer_id, arrays)
        # A second handle reads the same keys without populating another copy.
        peer = self.EngramStore(configs, self.store)
        for layer_id, cfg in configs.items():
            self.assertEqual(
                table.get_store_keys(layer_id), peer.get_store_keys(layer_id)
            )
            ids = np.zeros((1, 2, len(cfg.table_vocab_sizes)), dtype=np.int64)
            with self.registered_output(table, ids.shape, layer_id) as output:
                for reader in (table, peer):
                    reader.lookup_into(layer_id, ids, output)
                    for h in range(ids.shape[-1]):
                        self.assertTrue(
                            np.all(
                                output[..., h, :] == h + 31 + (layer_id - first) * 50
                            )
                        )
        table.remove_from_store(first, force=True)
        ids = np.zeros((1, 1, 2), dtype=np.int64)
        with self.registered_output(table, ids.shape, second) as output:
            peer.lookup_into(second, ids, output)
            self.assertTrue(np.all(output[..., 0, :] == 81))
            with self.assertRaisesRegex(ValueError, "Unknown Engram layer"):
                table.lookup_into(second + 1, ids, output)
            self.assertTrue(np.all(output[..., 0, :] == 81))
        for operation in (
            table.get_store_keys,
            table.get_row_bytes,
            table.remove_from_store,
        ):
            with self.assertRaisesRegex(ValueError, "Unknown Engram layer"):
                operation(second + 1)
        with self.assertRaisesRegex(ValueError, "Unknown Engram layer"):
            table.populate(second + 1, [])

    def test_invalid_layer_configuration(self):
        with self.assertRaises(ValueError):
            self.EngramStore({})
        with self.assertRaises(ValueError):
            self.EngramStore({-1: self.create_config()})


if __name__ == "__main__":
    unittest.main(verbosity=2)
