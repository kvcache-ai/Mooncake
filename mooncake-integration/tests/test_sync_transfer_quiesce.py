import ctypes
import time
import unittest

import engine

LENGTH = 16 << 20
REJECTED_LENGTH = 4096


class SyncTransferQuiesceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.target = engine.TransferEngine()
        cls.initiator = engine.TransferEngine()
        assert cls.target.initialize("", "P2PHANDSHAKE", "tcp", "") == 0
        assert cls.initiator.initialize("", "P2PHANDSHAKE", "tcp", "") == 0
        cls.target_name = f"127.0.0.1:{cls.target.get_rpc_port()}"
        cls.source = cls.initiator.allocate_managed_buffer(LENGTH + REJECTED_LENGTH)
        cls.destination = cls.target.allocate_managed_buffer(LENGTH)
        assert cls.source != 0 and cls.destination != 0
        # The target rejects writes outside its registered buffers.
        cls.unregistered = cls.destination + LENGTH + (64 << 20)

    def setUp(self):
        ctypes.memset(self.source, 0xAA, LENGTH + REJECTED_LENGTH)
        ctypes.memset(self.destination, 0, LENGTH)

    def test_successful_batch_write_returns_zero(self):
        ret = self.initiator.batch_transfer_sync_write(
            self.target_name, [self.source], [self.destination], [LENGTH]
        )
        self.assertEqual(ret, 0)
        written = ctypes.string_at(self.destination, LENGTH).count(b"\xaa")
        self.assertEqual(written, LENGTH)

    def test_failed_batch_write_does_not_read_source_after_return(self):
        """One rejected task fails the batch; the large sibling task must
        finish before the call returns, so the source is safe to reuse."""
        ret = self.initiator.batch_transfer_sync_write(
            self.target_name,
            [self.source + LENGTH, self.source],
            [self.unregistered, self.destination],
            [REJECTED_LENGTH, LENGTH],
        )
        self.assertEqual(ret, -1)

        ctypes.memset(self.source, 0xBB, LENGTH)
        time.sleep(0.5)  # give a late sibling write time to land
        late_bytes = ctypes.string_at(self.destination, LENGTH).count(b"\xbb")
        self.assertEqual(late_bytes, 0)


if __name__ == "__main__":
    unittest.main()
