import ctypes
import time
import unittest

import engine


class AsyncBatchTransferTest(unittest.TestCase):
    def test_multiple_batches_write_and_read(self):
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()
        for te in (target, initiator):
            self.assertEqual(te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)

        size = 4096
        payload = bytes(range(256)) * 16
        local = ctypes.create_string_buffer(payload, size)
        remote = ctypes.create_string_buffer(size)
        local_addr, remote_addr = ctypes.addressof(local), ctypes.addressof(remote)
        for te, buffer in ((initiator, local), (target, remote)):
            self.assertEqual(te.register_memory(ctypes.addressof(buffer), size), 0)
            self.addCleanup(
                lambda te=te, b=buffer: te.unregister_memory(ctypes.addressof(b))
            )

        name = f"127.0.0.1:{target.get_rpc_port()}"
        for operation in ("write", "read"):
            with self.subTest(operation=operation):
                if operation == "read":
                    ctypes.memset(local_addr, 0, size)
                submit = getattr(initiator, f"batch_transfer_async_{operation}")
                batches = []
                for offset in (0, 2048):
                    batch = submit(
                        name,
                        [local_addr + offset, local_addr + offset + 1024],
                        [remote_addr + offset, remote_addr + offset + 1024],
                        [1024, 1024],
                    )
                    self.assertNotEqual(batch, 0)
                    batches.append(batch)
                self.assertEqual(initiator.get_batch_transfer_status(batches), 0)
                self.assertEqual(remote.raw, payload)
                self.assertEqual(local.raw, payload)

        for poll_single in (False, True):
            with self.subTest(poll_single=poll_single):
                ctypes.memset(remote_addr, 0, size)
                batch = initiator.transfer_submit_write(
                    name, local_addr, remote_addr, size
                )
                self.assertNotEqual(batch, 0)
                if poll_single:
                    deadline = time.monotonic() + 5
                    status = 0
                    while status == 0 and time.monotonic() < deadline:
                        status = initiator.transfer_check_status(batch)
                        time.sleep(0.001)
                    self.assertEqual(status, 1)
                else:
                    self.assertEqual(initiator.get_batch_transfer_status([batch]), 0)
                self.assertEqual(remote.raw, payload)


if __name__ == "__main__":
    unittest.main()
