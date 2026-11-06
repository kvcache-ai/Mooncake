#!/usr/bin/env python3
"""Same-host SHM e2e for Python allocate_managed_buffer."""

from __future__ import annotations

import os
import unittest

try:
    from mooncake.engine import TransferEngine
except Exception as error:  # pragma: no cover - depends on built extension
    TransferEngine = None
    _ENGINE_IMPORT_ERROR = error
else:
    _ENGINE_IMPORT_ERROR = None

_SMALL_LEN = 4096
# Buddy class_id is < 0 only when length exceeds the 256 MiB max-class slab.
_LARGE_LEN = 256 * 1024 * 1024 + 4096


def _segment_name(engine, host="127.0.0.1"):
    return f"{host}:{engine.get_rpc_port()}"


def _shm_free_bytes():
    if not os.path.isdir("/dev/shm"):
        return 0
    st = os.statvfs("/dev/shm")
    return st.f_bavail * st.f_frsize


@unittest.skipIf(
    TransferEngine is None,
    f"mooncake.engine is unavailable: {_ENGINE_IMPORT_ERROR}",
)
@unittest.skipUnless(
    os.path.isdir("/dev/shm"),
    "POSIX shm requires /dev/shm",
)
class TransferEngineShmManagedBufferTest(unittest.TestCase):
    def setUp(self):
        # Two 256 MiB buddy slabs plus a little headroom for metadata.
        needed = 2 * 256 * 1024 * 1024 + 16 * 1024 * 1024
        free = _shm_free_bytes()
        if free < needed:
            self.skipTest(f"/dev/shm has {free} free bytes; need at least {needed}")

        self.initiator = TransferEngine()
        self.target = TransferEngine()
        ret = self.initiator.initialize("127.0.0.1:12347", "P2PHANDSHAKE", "shm", "")
        self.assertEqual(ret, 0, f"initiator initialize failed: {ret}")
        ret = self.target.initialize("127.0.0.1:12345", "P2PHANDSHAKE", "shm", "")
        self.assertEqual(ret, 0, f"target initialize failed: {ret}")
        self.initiator_name = _segment_name(self.initiator)
        self.target_name = _segment_name(self.target)
        self._to_free = []

    def tearDown(self):
        for engine, addr, length in reversed(self._to_free):
            if addr:
                engine.free_managed_buffer(addr, length)
        self._to_free.clear()
        self.initiator = None
        self.target = None

    def _alloc(self, engine, length):
        addr = engine.allocate_managed_buffer(length)
        self.assertNotEqual(addr, 0, f"allocate_managed_buffer({length}) failed")
        self._to_free.append((engine, addr, length))
        return addr

    def test_small_managed_buffer_write_read(self):
        src = self._alloc(self.initiator, _SMALL_LEN)
        dst = self._alloc(self.target, _SMALL_LEN)
        payload = b"shm-managed-buffer-payload"
        self.assertEqual(
            self.initiator.write_bytes_to_buffer(src, payload, len(payload)), 0
        )
        self.assertEqual(
            self.initiator.transfer_sync_write(
                self.target_name, src, dst, len(payload)
            ),
            0,
        )
        self.assertEqual(self.target.read_bytes_from_buffer(dst, len(payload)), payload)

        zeros = bytes(len(payload))
        self.assertEqual(
            self.initiator.write_bytes_to_buffer(src, zeros, len(payload)), 0
        )
        self.assertEqual(
            self.initiator.transfer_sync_read(self.target_name, src, dst, len(payload)),
            0,
        )
        self.assertEqual(
            self.initiator.read_bytes_from_buffer(src, len(payload)), payload
        )

    def test_free_large_managed_buffer_then_write_fails(self):
        needed = _LARGE_LEN + 256 * 1024 * 1024 + 16 * 1024 * 1024
        free = _shm_free_bytes()
        if free < needed:
            self.skipTest(
                f"/dev/shm has {free} free bytes; large free-after-write "
                f"needs about {needed}"
            )

        src = self._alloc(self.initiator, _SMALL_LEN)
        dst = self.target.allocate_managed_buffer(_LARGE_LEN)
        if dst == 0:
            self.skipTest("allocate_managed_buffer of 256MiB+ failed")
        self._to_free.append((self.target, dst, _LARGE_LEN))

        payload = b"before-free"
        self.assertEqual(
            self.initiator.write_bytes_to_buffer(src, payload, len(payload)), 0
        )
        self.assertEqual(
            self.initiator.transfer_sync_write(
                self.target_name, src, dst, len(payload)
            ),
            0,
        )
        self.assertEqual(self.target.read_bytes_from_buffer(dst, len(payload)), payload)

        self.assertEqual(self.target.free_managed_buffer(dst, _LARGE_LEN), 0)
        self._to_free = [item for item in self._to_free if item[1] != dst]

        ret = self.initiator.transfer_sync_write(
            self.target_name, src, dst, len(payload)
        )
        self.assertNotEqual(ret, 0, "WRITE after free_managed_buffer should fail")


if __name__ == "__main__":
    unittest.main()
