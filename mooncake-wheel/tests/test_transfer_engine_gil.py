#!/usr/bin/env python3
"""Verify that single-buffer registration does not block other Python threads."""

import ctypes
import mmap
import socket
import sys
import threading
import unittest
from collections.abc import Callable

from mooncake.engine import TransferEngine


class _OneShotWorker:
    def __init__(self) -> None:
        self.arm = threading.Event()
        self.ran = threading.Event()
        self.ready = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        self.ready.set()
        while not self.stop.is_set():
            if not self.arm.wait(timeout=0.1):
                continue
            self.arm.clear()
            if not self.stop.is_set():
                self.ran.set()

    def start(self) -> None:
        self.thread.start()
        if not self.ready.wait(timeout=5):
            raise RuntimeError("worker did not start")

    def observe(self, call: Callable[[int], int], max_calls: int) -> tuple[bool, int]:
        self.ran.clear()
        self.arm.set()
        calls_attempted = 0
        for index in range(max_calls):
            result = call(index)
            if result != 0:
                raise RuntimeError(f"memory registration returned {result}")
            calls_attempted += 1
            if self.ran.is_set():
                break
        self.arm.clear()
        return self.ran.is_set(), calls_attempted

    def close(self) -> None:
        self.stop.set()
        self.arm.set()
        self.thread.join(timeout=5)


class TestTransferEngineGil(unittest.TestCase):
    def test_single_memory_registration_releases_gil(self) -> None:
        max_calls = 2048
        region_size = 4096
        region = mmap.mmap(-1, max_calls * region_size)
        base_address = ctypes.addressof(ctypes.c_char.from_buffer(region))
        ctypes.memset(base_address, 0, max_calls * region_size)
        addresses = [base_address + i * region_size for i in range(max_calls)]
        sizes = [region_size] * max_calls

        engine = TransferEngine()
        hostname = socket.gethostbyname(socket.gethostname())
        self.assertEqual(
            engine.initialize(f"{hostname}:0", "P2PHANDSHAKE", "tcp", ""), 0
        )

        worker = _OneShotWorker()
        worker.start()
        old_switch_interval = sys.getswitchinterval()
        sys.setswitchinterval(10.0)
        registered: set[int] = set()
        register_observed = False
        unregister_observed = False
        try:

            def register_one(index: int) -> int:
                result = engine.register_memory(addresses[index], region_size)
                if result == 0:
                    registered.add(addresses[index])
                return result

            register_observed, _ = worker.observe(register_one, max_calls)
            self.assertEqual(engine.batch_unregister_memory(list(registered)), 0)
            registered.clear()

            self.assertEqual(engine.batch_register_memory(addresses, sizes), 0)
            registered.update(addresses)

            def unregister_one(index: int) -> int:
                result = engine.unregister_memory(addresses[index])
                if result == 0:
                    registered.discard(addresses[index])
                return result

            unregister_observed, _ = worker.observe(unregister_one, max_calls)
            self.assertEqual(engine.batch_unregister_memory(list(registered)), 0)
            registered.clear()
        finally:
            if registered:
                engine.batch_unregister_memory(list(registered))
            sys.setswitchinterval(old_switch_interval)
            worker.close()
            region.close()

        self.assertTrue(register_observed, "register_memory retained the GIL")
        self.assertTrue(unregister_observed, "unregister_memory retained the GIL")


if __name__ == "__main__":
    unittest.main()
