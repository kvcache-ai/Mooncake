import unittest

import engine


class TentSendProbeTest(unittest.TestCase):
    def test_probe_preserves_cached_peer_handle(self):
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()

        self.assertEqual(target.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)
        self.assertEqual(initiator.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)

        target_name = f"127.0.0.1:{target.get_rpc_port()}"
        length = 4096
        source = initiator.allocate_managed_buffer(length)
        destination = target.allocate_managed_buffer(length)
        self.assertNotEqual(source, 0)
        self.assertNotEqual(destination, 0)
        self.assertEqual(initiator.send_probe(target_name), 0)

        for payload in (b"before-probe", b"after-probe"):
            self.assertEqual(
                initiator.write_bytes_to_buffer(source, payload, len(payload)), 0
            )
            self.assertEqual(
                initiator.transfer_sync_write(
                    target_name, source, destination, len(payload)
                ),
                0,
            )
            self.assertEqual(
                target.read_bytes_from_buffer(destination, len(payload)), payload
            )
            if payload == b"before-probe":
                self.assertEqual(initiator.send_probe(target_name), 0)


if __name__ == "__main__":
    unittest.main()
