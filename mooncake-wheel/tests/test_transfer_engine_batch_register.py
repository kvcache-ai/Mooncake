#!/usr/bin/env python3
"""Validate batch memory registration using CPU buffers and TCP."""

import ctypes
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

from mooncake.engine import TransferEngine


class TestTransferEngineBatchRegister(unittest.TestCase):
    def setUp(self) -> None:
        env = patch.dict("os.environ", MC_FORCE_TCP="1")
        env.start()
        self.addCleanup(env.stop)
        self.region_size = 4096
        self.region = ctypes.create_string_buffer(2 * self.region_size)
        base = ctypes.addressof(self.region)
        self.addresses = [base, base + self.region_size]
        self.capacities = [self.region_size] * 2
        self.registered: list[int] = []
        self.engine = TransferEngine()
        hostname = socket.gethostbyname(socket.gethostname())
        self.assertEqual(
            self.engine.initialize(f"{hostname}:0", "P2PHANDSHAKE", "tcp", ""), 0
        )

    def tearDown(self) -> None:
        try:
            if self.registered:
                self.assertEqual(
                    self.engine.batch_unregister_memory(self.registered), 0
                )
        finally:
            # Keep the backing buffers alive until the engine is destroyed.
            self.engine = None

    def assert_valid_registration(self) -> None:
        self.assertEqual(
            self.engine.batch_register_memory(self.addresses, self.capacities), 0
        )
        self.registered = self.addresses.copy()
        self.assertEqual(self.engine.batch_unregister_memory(self.addresses), 0)
        self.registered.clear()

    def check_short_capacities(self) -> None:
        # Explicitly selected by the subprocess below, not unittest discovery.
        for addresses, capacities in (
            (self.addresses[:1], []),
            (self.addresses, self.capacities[:1]),
        ):
            with self.subTest(addresses=len(addresses), capacities=len(capacities)):
                self.assertEqual(
                    self.engine.batch_register_memory(addresses, capacities), -1
                )
                self.assert_valid_registration()

    def test_rejects_short_capacities(self) -> None:
        # The unfixed binding can crash, so keep it out of the test runner.
        result = subprocess.run(
            [
                sys.executable,
                __file__,
                "TestTransferEngineBatchRegister.check_short_capacities",
                "-v",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_rejects_long_capacities_without_registration(self) -> None:
        for addresses, capacities in (
            (self.addresses[:1], self.capacities),
            ([], self.capacities[:1]),
        ):
            with self.subTest(addresses=len(addresses), capacities=len(capacities)):
                result = self.engine.batch_register_memory(addresses, capacities)
                if result == 0:
                    self.registered = addresses.copy()
                self.assertEqual(result, -1)
                self.assert_valid_registration()

    def test_valid_batch_registration(self) -> None:
        self.assert_valid_registration()

    def test_empty_batch_registration(self) -> None:
        self.assertEqual(self.engine.batch_register_memory([], []), 0)
        self.assert_valid_registration()


if __name__ == "__main__":
    unittest.main()
