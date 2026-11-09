"""Completion eventfd coverage for the Python binding.

The round trip needs an RDMA device. The fallback case does not: slice
timeout is process-global, so it runs in a child before any engine exists.
"""

from __future__ import annotations

import os
import select
import subprocess
import sys
import unittest
from pathlib import Path

from mooncake.engine import (
    CompletionNotificationFallback,
    TransferEngine,
    TransferOpcode,
    TransferStatus,
)

_FALLBACK = r"""
import os
import sys

os.environ["MC_SLICE_TIMEOUT"] = "1"
from mooncake.engine import (
    CompletionNotificationFallback,
    TransferEngine,
    TransferOpcode,
)

host = os.environ.get("MC_TEST_LOCAL_IP") or "127.0.0.1"
source = TransferEngine()
target = TransferEngine()
if source.initialize(f"{host}:0", "P2PHANDSHAKE", "tcp", "") != 0:
    print("source initialize failed", file=sys.stderr)
    raise SystemExit(1)
if target.initialize(f"{host}:0", "P2PHANDSHAKE", "tcp", "") != 0:
    print("target initialize failed", file=sys.stderr)
    raise SystemExit(1)
peer = f"{host}:{target.get_rpc_port()}"
length = 64
local = source.allocate_managed_buffer(length)
remote = target.allocate_managed_buffer(length)
if local == 0 or remote == 0:
    print("allocate_managed_buffer failed", file=sys.stderr)
    raise SystemExit(1)
try:
    source.batch_transfer_async(
        peer,
        [local],
        [remote],
        [length],
        TransferOpcode.Write,
        enable_completion_notification=True,
    )
except CompletionNotificationFallback:
    raise SystemExit(0)
print("expected CompletionNotificationFallback", file=sys.stderr)
raise SystemExit(1)
"""


def _rdma_device() -> str | None:
    override = os.environ.get("MC_TEST_DEVICE_NAME")
    if override:
        return override
    root = Path("/sys/class/infiniband")
    if not root.is_dir():
        return None
    names = sorted(entry.name for entry in root.iterdir())
    return names[0] if names else None


class TestTransferEngineCompletionFd(unittest.TestCase):
    def test_slice_timeout_raises_fallback(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-c", _FALLBACK],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            completed.stdout + completed.stderr,
        )

    def test_rdma_round_trip(self) -> None:
        device = _rdma_device()
        if device is None:
            self.skipTest("no RDMA device")
        host = os.environ.get("MC_TEST_LOCAL_IP") or "127.0.0.1"
        source = TransferEngine()
        target = TransferEngine()
        self.assertEqual(
            source.initialize(f"{host}:0", "P2PHANDSHAKE", "rdma", device), 0
        )
        self.assertEqual(
            target.initialize(f"{host}:0", "P2PHANDSHAKE", "rdma", device), 0
        )
        peer = f"{host}:{target.get_rpc_port()}"
        length = 4096
        local = source.allocate_managed_buffer(length)
        remote = target.allocate_managed_buffer(length)
        self.assertNotEqual(local, 0)
        self.assertNotEqual(remote, 0)
        source.write_bytes_to_buffer(local, b"P" * length, length)
        try:
            batch_id = source.batch_transfer_async(
                peer,
                [local],
                [remote],
                [length],
                TransferOpcode.Write,
                enable_completion_notification=True,
            )
        except CompletionNotificationFallback as exc:
            self.fail(f"RDMA submit fell back to polling: {exc}")
        self.assertNotEqual(batch_id, 0)
        fd = source.completion_fd()
        self.assertGreaterEqual(fd, 0)
        ready, _, _ = select.select([fd], [], [], 10)
        self.assertTrue(ready, "completion fd was not readable")
        rows = [
            row
            for row in source.drain_completions()
            if row["batch_id"] == batch_id
        ]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], int(TransferStatus.COMPLETED))
        self.assertEqual(rows[0]["transferred_bytes"], length)
        self.assertEqual(target.read_bytes_from_buffer(remote, length), b"P" * length)
        self.assertIn(source.free_batch_id(batch_id), (0, 1))


if __name__ == "__main__":
    unittest.main()
