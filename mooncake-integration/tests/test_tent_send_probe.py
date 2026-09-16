import unittest

import engine


class TentSendProbeTest(unittest.TestCase):
    def test_reachable_peer(self):
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()

        self.assertEqual(target.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)
        self.assertEqual(initiator.initialize("", "P2PHANDSHAKE", "tcp", ""), 0)

        target_name = f"127.0.0.1:{target.get_rpc_port()}"
        self.assertEqual(initiator.send_probe(target_name), 0)


if __name__ == "__main__":
    unittest.main()
