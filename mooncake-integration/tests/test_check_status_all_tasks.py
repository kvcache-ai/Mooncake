import ctypes
import time
import unittest

import engine

TARGET_SIZE = 1 << 20
CHUNK = 4096


class CheckStatusAllTasksTest(unittest.TestCase):
    def setUp(self):
        self.target = engine.TransferEngine()
        self.initiator = engine.TransferEngine()
        self.assertEqual(self.target.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)
        self.assertEqual(self.initiator.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)
        self.target_name = f"127.0.0.1:{self.target.get_rpc_port()}"
        self.dst = ctypes.create_string_buffer(TARGET_SIZE)
        self.dst_addr = ctypes.addressof(self.dst)
        self.assertEqual(self.target.register_memory(self.dst_addr, TARGET_SIZE), 0)
        self.src = ctypes.create_string_buffer(b"x" * (2 * CHUNK), 2 * CHUNK)
        self.src_addr = ctypes.addressof(self.src)
        self.assertEqual(self.initiator.register_memory(self.src_addr, 2 * CHUNK), 0)

    def wait(self, batch_id):
        deadline = time.monotonic() + 30
        rc = 0
        while rc == 0 and time.monotonic() < deadline:
            rc = self.initiator.transfer_check_status(batch_id)
            time.sleep(0.005)
        return rc

    def submit(self, peer_addrs):
        batch_id = self.initiator.batch_transfer_async_write(
            self.target_name,
            [self.src_addr, self.src_addr + CHUNK],
            peer_addrs,
            [CHUNK, CHUNK],
        )
        self.assertNotEqual(batch_id, 0)
        return batch_id

    def test_failed_later_task_fails_batch(self):
        # The second task lands just past the registered target buffer, so
        # the target rejects it while the first task completes.
        past_end = self.dst_addr + TARGET_SIZE
        for peers in ([self.dst_addr, past_end], [past_end, self.dst_addr]):
            with self.subTest(peers=peers):
                self.assertEqual(self.wait(self.submit(peers)), -1)

    def test_all_tasks_complete(self):
        batch_id = self.submit([self.dst_addr, self.dst_addr + CHUNK])
        self.assertEqual(self.wait(batch_id), 1)
        self.assertEqual(self.dst.raw[: 2 * CHUNK], b"x" * (2 * CHUNK))


if __name__ == "__main__":
    unittest.main()
