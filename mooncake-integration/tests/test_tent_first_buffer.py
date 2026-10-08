import ctypes
import unittest

import engine


class TentFirstBufferTest(unittest.TestCase):
    def test_local_and_remote_buffer(self):
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()
        for te in (target, initiator):
            self.assertEqual(te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)

        name = f"127.0.0.1:{target.get_rpc_port()}"
        self.assertEqual(target.get_first_buffer_address(name), 0)

        target_buffer = ctypes.create_string_buffer(128)
        source_buffer = ctypes.create_string_buffer(b"first buffer regression", 128)
        target_addr = ctypes.addressof(target_buffer)
        source_addr = ctypes.addressof(source_buffer)
        self.assertEqual(target.register_memory(target_addr, 128), 0)
        self.addCleanup(
            lambda: target.unregister_memory(ctypes.addressof(target_buffer))
        )
        self.assertEqual(initiator.register_memory(source_addr, 128), 0)
        self.addCleanup(
            lambda: initiator.unregister_memory(ctypes.addressof(source_buffer))
        )

        self.assertEqual(target.get_first_buffer_address(name), target_addr)
        for _ in range(2):
            # The second query must preserve the handle cached by the first write.
            self.assertEqual(initiator.get_first_buffer_address(name), target_addr)
            ctypes.memset(target_addr, 0, 128)
            self.assertEqual(
                initiator.transfer_sync_write(name, source_addr, target_addr, 128), 0
            )
            self.assertEqual(target_buffer.raw, source_buffer.raw)


if __name__ == "__main__":
    unittest.main()
