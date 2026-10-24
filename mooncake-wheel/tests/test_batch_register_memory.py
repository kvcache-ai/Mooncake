"""Regression tests for parallel arrays in the real TransferEngine binding."""

import ctypes
import os
import subprocess
import sys
import unittest

from mooncake.engine import TransferEngine


def probe_mismatch(address_count, capacity_count):
    # A broken native binding can segfault. Keep failures in a subprocess and
    # disable core dumps rather than terminating the installation test runner.
    if os.name == "posix":
        import resource

        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    engine = TransferEngine()
    assert engine.initialize("127.0.0.1:0", "P2PHANDSHAKE", "tcp", "") == 0
    buffers = [ctypes.create_string_buffer(4096) for _ in range(2)]
    addresses = [ctypes.addressof(buffer) for buffer in buffers]
    result = engine.batch_register_memory(
        addresses[:address_count], [4096] * capacity_count
    )
    assert result == -1, f"expected -1 for mismatched lengths, got {result}"
    # Reuse the same buffers: rejecting the request must not register a prefix.
    assert engine.batch_register_memory(addresses, [4096, 4096]) == 0
    assert engine.batch_unregister_memory(addresses) == 0


class BatchRegisterMemoryTest(unittest.TestCase):
    def test_mismatched_lengths(self):
        for address_count, capacity_count in ((1, 0), (2, 1), (1, 2), (0, 1)):
            with self.subTest(addresses=address_count, capacities=capacity_count):
                completed = subprocess.run(
                    [
                        sys.executable,
                        __file__,
                        "--probe-mismatch",
                        str(address_count),
                        str(capacity_count),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
                self.assertEqual(
                    completed.returncode, 0, completed.stdout + completed.stderr
                )

    def test_matching_lengths(self):
        engine = TransferEngine()
        self.assertEqual(engine.initialize("127.0.0.1:0", "P2PHANDSHAKE", "tcp", ""), 0)
        self.assertEqual(engine.batch_register_memory([], []), 0)
        buffers = [ctypes.create_string_buffer(4096) for _ in range(2)]
        addresses = [ctypes.addressof(buffer) for buffer in buffers]
        self.assertEqual(engine.batch_register_memory(addresses, [4096, 4096]), 0)
        self.assertEqual(engine.batch_unregister_memory(addresses), 0)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--probe-mismatch":
        probe_mismatch(int(sys.argv[2]), int(sys.argv[3]))
    else:
        unittest.main()
