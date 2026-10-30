"""Synchronous Python Store coverage using the P2P public entry point.

Run against a matching P2P master. Each test owns local DRAM storage and runs
with TE and memcpy; remote routing policies belong to separate tests.
"""

import ctypes
import json
import logging
import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from mooncake.store import MooncakeDistributedStore


class TestP2PDistributedObjectStore(unittest.TestCase):
    LOCAL_TRANSFER_MODE = "te"

    @classmethod
    def setUpClass(cls):
        if not hasattr(MooncakeDistributedStore, "setup_p2p_real_client"):
            logging.error("This test requires a P2P Store wheel")
            raise RuntimeError("This test requires a P2P Store wheel")

    def new_store(self, local_buffer_size=16 << 20):
        store = MooncakeDistributedStore()

        def close():
            result = store.close()
            if result != 0:
                logging.error("Store close failed: return code %s", result)
                raise RuntimeError(f"Store close failed: return code {result}")

        self.addCleanup(close)
        result = store.setup_p2p_real_client(
            local_buffer_size=local_buffer_size, **self.setup_options
        )
        if result != 0:
            logging.error("P2P Store setup failed: return code %s", result)
            raise RuntimeError(f"P2P Store setup failed: return code {result}")
        return store

    def setUp(self):
        self.prefix = f"p2p_python_{uuid.uuid4().hex}"
        tier_capacity = 64 << 20
        if self._testMethodName == "test_concurrent_byte_roundtrips":
            tier_capacity = 1024 << 20
        self.setup_options = {
            "local_hostname": os.getenv("LOCAL_HOSTNAME", "127.0.0.1"),
            "metadata_server": os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
            "tiered_backend_config_json": json.dumps({
                "tiers": [{"type": "DRAM", "capacity": tier_capacity, "priority": 100}],
            }),
            "protocol": os.getenv("PROTOCOL", "tcp"),
            "rdma_devices": os.getenv("DEVICE_NAME", ""),
            "master_server_addr": os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
            "client_rpc_port": 0,
            "client_rpc_thread_num": 2,
            "local_transfer_mode": self.LOCAL_TRANSFER_MODE,
            "local_memcpy_async_worker_num": 2,
            "te_async_poll_worker_num": 2,
            "async_sender_thread_count": 0,
            "enable_http_server": False,
            "enable_metric_collection": False,
            "p2p_transfer_direction_mode": "reverse",
            "runtime_config": json.dumps({"write": {"remote_weight": 0.0}}),
        }
        self.store = self.new_store()

    def key(self, suffix):
        return f"{self.prefix}_{suffix}"

    @contextmanager
    def registered_buffer(self, size):
        buffer = (ctypes.c_ubyte * size)()
        pointer = ctypes.addressof(buffer)
        self.assertEqual(self.store.register_buffer(pointer, size), 0)
        try:
            yield buffer, pointer
        finally:
            # Keep the ctypes owner alive until unregistration completes.
            result = self.store.unregister_buffer(pointer)
            if result != 0:
                logging.error("P2P buffer unregistration failed: return code %s", result)
                raise RuntimeError(f"P2P buffer unregistration failed: return code {result}")

    def test_zero_local_buffer_uses_default_pool(self):
        store = self.new_store(local_buffer_size=0)
        key = self.key("default_pool")
        value = b"zero selects the P2P default pool"
        self.assertEqual(store.put(key, value), 0)
        self.assertEqual(store.get(key), value)
        self.assertEqual(store.get_size(key), len(value))

    def test_close_and_setup_again(self):
        before = self.key("before_close")
        self.assertEqual(self.store.put(before, b"before"), 0)
        self.assertEqual(self.store.get(before), b"before")
        result = self.store.close()
        if result != 0:
            logging.error("Store close failed: return code %s", result)
        self.assertEqual(result, 0)

        # Reuse the Python wrapper; setup creates a new native Real client.
        result = self.store.setup_p2p_real_client(
            local_buffer_size=16 << 20, **self.setup_options
        )
        if result != 0:
            logging.error("P2P Store setup failed: return code %s", result)
            raise RuntimeError(f"P2P Store setup failed: return code {result}")
        after = self.key("after_setup")
        self.assertEqual(self.store.put(after, b"after"), 0)
        self.assertEqual(self.store.get(after), b"after")

    def test_get_hostname_reports_endpoint(self):
        host, separator, port = self.store.get_hostname().rpartition(":")
        self.assertTrue(host)
        self.assertEqual(separator, ":")
        self.assertGreater(int(port), 0)
        self.assertLessEqual(int(port), 65535)

    def test_bytes_roundtrip(self):
        key = self.key("bytes")
        value = b"first\x00second\xff" * 31
        self.assertEqual(self.store.put(key, value), 0)
        self.assertEqual(self.store.is_exist(key), 1)
        self.assertEqual(self.store.get_size(key), len(value))
        self.assertEqual(self.store.get(key), value)
        self.assertEqual(self.store.put(key, value), 0)
        self.assertEqual(self.store.get(key), value)
        self.assertEqual(self.store.get(self.key("missing")), b"")

    def test_batch_is_exist_preserves_order(self):
        first, second, missing = [self.key(name) for name in ("first", "second", "missing")]
        self.assertEqual(self.store.put_batch([first, second], [b"one", b"second"]), 0)
        self.assertEqual(
            self.store.batch_is_exist([second, missing, first, second]), [1, 0, 1, 1]
        )
        self.assertEqual(self.store.batch_is_exist([]), [])

    def test_registered_buffer_roundtrip_and_short_read(self):
        key = self.key("registered")
        value = b"registered\x00payload" * 17
        capacity = len(value) + 32
        with self.registered_buffer(capacity) as (buffer, pointer):
            ctypes.memmove(pointer, value, len(value))
            self.assertEqual(self.store.put_from(key, pointer, len(value)), 0)
            self.assertEqual(self.store.get(key), value)

            ctypes.memset(pointer, 0x5A, capacity)
            self.assertEqual(self.store.get_into(key, pointer, capacity), len(value))
            self.assertEqual(bytes(buffer[:len(value)]), value)
            self.assertEqual(bytes(buffer[len(value):]), b"Z" * 32)

            ctypes.memset(pointer, 0x5A, capacity)
            self.assertLess(self.store.get_into(key, pointer, len(value) - 1), 0)
            self.assertEqual(bytes(buffer), b"Z" * capacity)

    def test_batch_buffer_roundtrip_preserves_order(self):
        keys = [self.key("batch_first"), self.key("batch_second")]
        values = [b"first\x00value", b"a longer second value"]
        spacing = 4096
        with self.registered_buffer(2 * spacing) as (buffer, pointer):
            pointers = [pointer, pointer + spacing]
            sizes = [len(value) for value in values]
            for address, value in zip(pointers, values):
                ctypes.memmove(address, value, len(value))
            self.assertEqual(self.store.batch_put_from(keys, pointers, sizes), [0, 0])
            self.assertEqual(self.store.get_batch(list(reversed(keys))), list(reversed(values)))

            ctypes.memset(pointer, 0x5A, 2 * spacing)
            results = self.store.batch_get_into(list(reversed(keys)), pointers, [spacing] * 2)
            self.assertEqual(results, list(reversed(sizes)))
            for index, value in enumerate(reversed(values)):
                offset = index * spacing
                self.assertEqual(bytes(buffer[offset:offset + len(value)]), value)
                self.assertEqual(bytes(buffer[offset + len(value):offset + spacing]),
                                 b"Z" * (spacing - len(value)))

    def test_batch_read_keeps_successes_when_one_buffer_is_short(self):
        keys = [self.key("short"), self.key("fits")]
        values = [b"first value", b"second value"]
        self.assertEqual(self.store.put_batch(keys, values), 0)
        spacing = 4096
        with self.registered_buffer(2 * spacing) as (buffer, pointer):
            ctypes.memset(pointer, 0x5A, 2 * spacing)
            results = self.store.batch_get_into(
                keys, [pointer, pointer + spacing], [len(values[0]) - 1, spacing]
            )
            self.assertEqual(len(results), 2)
            self.assertLess(results[0], 0)
            self.assertEqual(results[1], len(values[1]))
            self.assertEqual(bytes(buffer[:spacing]), b"Z" * spacing)
            self.assertEqual(bytes(buffer[spacing:spacing + len(values[1])]), values[1])
            self.assertEqual(bytes(buffer[spacing + len(values[1]):]),
                             b"Z" * (spacing - len(values[1])))

    def test_batch_get_buffer_preserves_missing_positions(self):
        first, second, missing = [self.key(name) for name in ("first", "second", "missing")]
        self.assertEqual(self.store.put_batch([first, second], [b"first", b"second value"]), 0)
        results = self.store.batch_get_buffer([second, missing, first])
        self.assertEqual(len(results), 3)
        for index, value in ((0, b"second value"), (2, b"first")):
            self.assertIsNotNone(results[index])
            self.assertEqual(results[index].size(), len(value))
            self.assertEqual(bytes(results[index]), value)
        self.assertIsNone(results[1])
        self.assertEqual(self.store.batch_get_buffer([]), [])
        self.assertEqual(self.store.batch_get_buffer([missing]), [None])

    def test_mismatched_batch_arrays_have_no_side_effects(self):
        keys = [self.key("reject_first"), self.key("reject_second")]
        with self.registered_buffer(4096) as (buffer, pointer):
            ctypes.memset(pointer, 0x5A, len(buffer))
            results = self.store.batch_put_from(keys, [pointer], [16])
            self.assertEqual(len(results), len(keys))
            self.assertTrue(all(result < 0 for result in results), results)
            self.assertEqual(self.store.batch_is_exist(keys), [0, 0])

            self.assertEqual(self.store.put(keys[0], b"existing value"), 0)
            results = self.store.batch_get_into(keys, [pointer], [len(buffer)])
            self.assertEqual(len(results), len(keys))
            self.assertTrue(all(result < 0 for result in results), results)
            self.assertEqual(bytes(buffer), b"Z" * len(buffer))
            self.assertEqual(self.store.get(keys[0]), b"existing value")
            self.assertEqual(self.store.batch_put_from([], [], []), [])
            self.assertEqual(self.store.batch_get_into([], [], []), [])

    def test_concurrent_byte_roundtrips(self):
        num_threads = 8
        operations_per_thread = 100
        value_size = 1 << 20
        values = [os.urandom(value_size) for _ in range(num_threads)]

        def put_worker(worker_id):
            for index in range(operations_per_thread):
                key = self.key(f"thread_{worker_id}_{index}")
                self.assertEqual(self.store.put(key, values[worker_id]), 0)

        def get_worker(worker_id):
            for index in range(operations_per_thread):
                key = self.key(f"thread_{worker_id}_{index}")
                result = self.store.get(key)
                self.assertEqual(len(result), value_size)
                self.assertEqual(result, values[worker_id])

        # Finish every write before starting reads. The runner bounds execution;
        # the executor joins workers before fixture cleanup.
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(put_worker, index) for index in range(num_threads)]
            for future in futures:
                future.result()
            futures = [executor.submit(get_worker, index) for index in range(num_threads)]
            for future in futures:
                future.result()


class TestP2PDistributedObjectStoreMemcpy(TestP2PDistributedObjectStore):
    LOCAL_TRANSFER_MODE = "memcpy"


if __name__ == "__main__":
    unittest.main(verbosity=2, failfast=True)
