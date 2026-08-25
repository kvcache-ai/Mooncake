#!/usr/bin/env python3
"""E2E tests for standalone Mooncake Store (no external mooncake_master).

These tests start an in-process embedded master via enable_standalone /
MOONCAKE_ENABLE_STANDALONE. They must not launch the mooncake_master binary.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import time
import unittest

from mooncake.store import MooncakeDistributedStore

try:
    import torch
except Exception:  # pragma: no cover - optional in some local setups
    torch = None

SEGMENT_SIZE = 64 * 1024 * 1024
LOCAL_BUFFER_SIZE = 32 * 1024 * 1024


def _master_pids() -> list[str]:
    result = subprocess.run(
        ["pgrep", "-x", "mooncake_master"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [pid for pid in result.stdout.split() if pid]


def _protocol() -> str:
    return os.getenv("PROTOCOL", "tcp")


def _device_name() -> str:
    return os.getenv("DEVICE_NAME", "")


def setup_standalone(
    store: MooncakeDistributedStore,
    *,
    enable_standalone: bool = True,
    master_server_addr: str = "",
    metadata_server: str = "P2PHANDSHAKE",
    local_hostname: str = "localhost",
) -> int:
    return store.setup(
        local_hostname,
        metadata_server,
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        _protocol(),
        _device_name(),
        master_server_addr,
        enable_standalone=enable_standalone,
    )


class TestStandaloneStoreE2E(unittest.TestCase):
    """Put/get, batch, session-range, and tensor paths without mooncake_master."""

    store: MooncakeDistributedStore

    @classmethod
    def setUpClass(cls) -> None:
        cls.store = MooncakeDistributedStore()
        ret = setup_standalone(cls.store)
        if ret != 0:
            raise RuntimeError(f"standalone setup failed with return code {ret}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.store.close()

    def setUp(self) -> None:
        self.assertEqual(
            _master_pids(),
            [],
            "standalone e2e must not start a mooncake_master process",
        )

    def test_put_get_exist_without_external_master(self) -> None:
        key = f"standalone_put_get_{os.getpid()}_{time.time_ns()}"
        payload = b"hello-standalone-store-e2e"
        self.assertEqual(self.store.put(key, payload), 0)
        self.assertEqual(self.store.get_size(key), len(payload))
        self.assertEqual(self.store.get(key), payload)
        self.assertEqual(self.store.is_exist(key), 1)

    def test_batch_put_from_and_get(self) -> None:
        batch_size = 3
        payloads = [
            b"standalone-batch-1 " * 32,
            b"standalone-batch-2 " * 48,
            b"standalone-batch-3 " * 16,
        ]
        keys = [
            f"standalone_batch_{i}_{os.getpid()}_{time.time_ns()}"
            for i in range(batch_size)
        ]
        spacing = 64 * 1024
        total = spacing * batch_size
        buf = (ctypes.c_ubyte * total)()
        ptr = ctypes.addressof(buf)
        self.assertEqual(self.store.register_buffer(ptr, total), 0)
        try:
            ptrs = []
            sizes = []
            for i, data in enumerate(payloads):
                offset = i * spacing
                ctypes.memmove(ctypes.c_void_p(ptr + offset), data, len(data))
                ptrs.append(ptr + offset)
                sizes.append(len(data))
            results = self.store.batch_put_from(keys, ptrs, sizes)
            self.assertEqual(len(results), batch_size)
            for i, rc in enumerate(results):
                self.assertEqual(rc, 0, f"batch_put_from failed for {keys[i]}")
            for key, expected in zip(keys, payloads):
                self.assertEqual(self.store.get(key), expected)
        finally:
            self.assertEqual(self.store.unregister_buffer(ptr), 0)

    def test_session_ranges_put_get(self) -> None:
        num_layers = 4
        page_size = 4096
        num_keys = 3
        object_size = page_size * num_layers
        src = (ctypes.c_char * (object_size * num_keys))()
        dst = (ctypes.c_char * (object_size * num_keys))()
        for i in range(len(src)):
            src[i] = ord("a") + (i % 26)
            dst[i] = ord("B")

        src_ptr = ctypes.addressof(src)
        dst_ptr = ctypes.addressof(dst)
        self.assertEqual(self.store.register_buffer(src_ptr, len(src)), 0)
        self.assertEqual(self.store.register_buffer(dst_ptr, len(dst)), 0)
        keys = [
            f"standalone_session_{i}_{os.getpid()}_{time.time_ns()}"
            for i in range(num_keys)
        ]
        try:
            put_start = self.store.batch_put_session_start(
                keys, [object_size] * num_keys
            )
            self.assertTrue(all(rc == 0 for rc in put_start), put_start)
            for layer in range(num_layers):
                all_buffers = []
                all_sizes = []
                all_dst_offsets = []
                for i in range(num_keys):
                    offset = i * object_size + layer * page_size
                    all_buffers.append([src_ptr + offset])
                    all_sizes.append([page_size])
                    all_dst_offsets.append([layer * page_size])
                put_rcs = self.store.batch_put_from_multi_buffer_ranges(
                    keys, all_buffers, all_sizes, all_dst_offsets
                )
                self.assertTrue(
                    all(rc == page_size for rc in put_rcs),
                    f"layer={layer} put_rcs={put_rcs}",
                )
            put_end = self.store.batch_put_session_end(keys)
            self.assertTrue(all(rc == 0 for rc in put_end), put_end)

            get_start = self.store.batch_get_session_start(keys)
            self.assertTrue(all(rc == 0 for rc in get_start), get_start)
            for layer in range(num_layers):
                all_buffers = []
                all_sizes = []
                all_src_offsets = []
                for i in range(num_keys):
                    offset = i * object_size + layer * page_size
                    all_buffers.append([dst_ptr + offset])
                    all_sizes.append([page_size])
                    all_src_offsets.append([layer * page_size])
                get_rcs = self.store.batch_get_into_multi_buffer_ranges(
                    keys, all_buffers, all_sizes, all_src_offsets
                )
                self.assertTrue(
                    all(rc == page_size for rc in get_rcs),
                    f"layer={layer} get_rcs={get_rcs}",
                )
            get_end = self.store.batch_get_session_end(keys)
            self.assertEqual(get_end, 0)
            self.assertEqual(bytes(src), bytes(dst))
        finally:
            self.store.unregister_buffer(src_ptr)
            self.store.unregister_buffer(dst_ptr)

    @unittest.skipIf(torch is None, "torch is not installed")
    def test_put_get_tensor(self) -> None:
        key = f"standalone_tensor_{os.getpid()}_{time.time_ns()}"
        tensor = torch.tensor([1.0, 2.0, 3.5, 4.0], dtype=torch.float32)
        self.assertEqual(self.store.put_tensor(key, tensor), 0)
        retrieved = self.store.get_tensor(key)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.dtype, tensor.dtype)
        self.assertTrue(torch.allclose(tensor, retrieved))


class TestStandaloneConfigDictE2E(unittest.TestCase):
    def test_config_dict_omits_master_address(self) -> None:
        store = MooncakeDistributedStore()
        self.addCleanup(store.close)
        ret = store.setup(
            {
                "local_hostname": "localhost",
                "metadata_server": "P2PHANDSHAKE",
                "global_segment_size": "64MB",
                "local_buffer_size": "32MB",
                "protocol": _protocol(),
                "rdma_devices": _device_name(),
                "enable_standalone": True,
            }
        )
        self.assertEqual(ret, 0)
        key = f"standalone_dict_{os.getpid()}_{time.time_ns()}"
        payload = b"config-dict-standalone-e2e"
        self.assertEqual(store.put(key, payload), 0)
        self.assertEqual(store.get(key), payload)
        self.assertEqual(_master_pids(), [])


class TestStandaloneEnvVarE2E(unittest.TestCase):
    def tearDown(self) -> None:
        os.environ.pop("MOONCAKE_ENABLE_STANDALONE", None)

    def test_env_var_enables_standalone_without_kwarg(self) -> None:
        os.environ["MOONCAKE_ENABLE_STANDALONE"] = "true"
        store = MooncakeDistributedStore()
        self.addCleanup(store.close)
        # Omit enable_standalone so HiCache-style setup() still embeds master.
        ret = store.setup(
            "localhost",
            "P2PHANDSHAKE",
            SEGMENT_SIZE,
            LOCAL_BUFFER_SIZE,
            _protocol(),
            _device_name(),
            "127.0.0.1:50051",
        )
        self.assertEqual(ret, 0)
        key = f"standalone_env_{os.getpid()}_{time.time_ns()}"
        payload = b"hello-env-standalone-e2e"
        self.assertEqual(store.put(key, payload), 0)
        self.assertEqual(store.get(key), payload)
        self.assertEqual(_master_pids(), [])

    def test_rejects_empty_metadata_without_standalone(self) -> None:
        os.environ.pop("MOONCAKE_ENABLE_STANDALONE", None)
        store = MooncakeDistributedStore()
        self.addCleanup(store.close)
        ret = store.setup(
            {
                "local_hostname": "localhost",
                "global_segment_size": "16MB",
                "local_buffer_size": "16MB",
                "protocol": _protocol(),
            }
        )
        self.assertNotEqual(ret, 0)


if __name__ == "__main__":
    unittest.main()
