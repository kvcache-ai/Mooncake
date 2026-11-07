"""CPU TCP coverage for the native session refresh binding."""

import ctypes
import unittest

from mooncake.engine import TransferEngine


class TestSessionRefresh(unittest.TestCase):
    def test_refresh_preserves_real_transfer(self):
        source, target = TransferEngine(), TransferEngine()
        for engine in (source, target):
            self.assertEqual(
                engine.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0
            )
        peer = f"127.0.0.1:{target.get_rpc_port()}"
        # Strict refresh does not implicitly open an uncached peer.
        self.assertNotEqual(source.refresh_session(peer), 0)
        payload = b"session-refresh-real-tcp"
        src = ctypes.create_string_buffer(payload)
        dst = ctypes.create_string_buffer(len(src))
        self.assertEqual(source.register_memory(ctypes.addressof(src), len(src)), 0)
        self.assertEqual(target.register_memory(ctypes.addressof(dst), len(dst)), 0)
        for refresh in (False, True):
            if refresh:
                old_dst = dst
                self.assertEqual(target.unregister_memory(ctypes.addressof(old_dst)), 0)
                dst = ctypes.create_string_buffer(len(src))
                self.assertEqual(
                    target.register_memory(ctypes.addressof(dst), len(dst)), 0
                )
                # The cached descriptor must change, not merely return success.
                self.assertEqual(source.refresh_session(peer), 0)
                self.assertEqual(
                    source.get_first_buffer_address(peer), ctypes.addressof(dst)
                )
                self.assertEqual(source.send_probe(peer), 0)
            ctypes.memset(ctypes.addressof(dst), 0, len(dst))
            self.assertEqual(
                source.transfer_sync_write(
                    peer, ctypes.addressof(src), ctypes.addressof(dst), len(src)
                ),
                0,
            )
            self.assertEqual(dst.raw, src.raw)
        self.assertEqual(source.refresh_session(peer), 0)  # unchanged metadata
        self.assertNotEqual(source.refresh_session(""), 0)
        self.assertNotEqual(source.refresh_session("127.0.0.1:1"), 0)
        self.assertEqual(source.unregister_memory(ctypes.addressof(src)), 0)
        self.assertEqual(target.unregister_memory(ctypes.addressof(dst)), 0)


if __name__ == "__main__":
    unittest.main()
