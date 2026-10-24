import ctypes
import json
import os
import unittest
from unittest.mock import patch

import engine


class TentInitializationTest(unittest.TestCase):
    def test_reinitialization_preserves_registered_buffers_and_cached_peer(self):
        for method in ("initialize", "initialize_ext"):
            with self.subTest(method=method):
                target = engine.TransferEngine()
                sender = engine.TransferEngine()
                for te in (target, sender):
                    self.assertEqual(
                        te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0
                    )
                source = [
                    ctypes.create_string_buffer(bytes([i + 1]) * 32) for i in range(2)
                ]
                destination = [ctypes.create_string_buffer(len(buf)) for buf in source]
                source_addrs = [ctypes.addressof(buf) for buf in source]
                destination_addrs = [ctypes.addressof(buf) for buf in destination]
                lengths = [len(buf) for buf in source]
                endpoint = f"127.0.0.1:{target.get_rpc_port()}"
                self.assertEqual(sender.batch_register_memory(source_addrs, lengths), 0)
                self.assertEqual(
                    target.batch_register_memory(destination_addrs, lengths), 0
                )
                try:
                    self.assertEqual(
                        sender.batch_transfer_sync_write(
                            endpoint, source_addrs, destination_addrs, lengths
                        ),
                        0,
                    )
                    original_engine = sender.get_engine_ptr()
                    original_port = sender.get_rpc_port()
                    args = ("127.0.0.1", "P2PHANDSHAKE", "tcp", "")
                    if method == "initialize_ext":
                        args = ("127.0.0.1", "", "tcp", "", "p2p")
                    self.assertLess(getattr(sender, method)(*args), 0)
                    self.assertEqual(sender.get_engine_ptr(), original_engine)
                    self.assertEqual(sender.get_rpc_port(), original_port)
                    for buf in destination:
                        ctypes.memset(ctypes.addressof(buf), 0, len(buf))
                    self.assertEqual(
                        sender.batch_transfer_sync_write(
                            endpoint, source_addrs, destination_addrs, lengths
                        ),
                        0,
                    )
                    self.assertEqual(
                        [buf.raw for buf in destination], [buf.raw for buf in source]
                    )
                finally:
                    sender.batch_unregister_memory(source_addrs)
                    target.batch_unregister_memory(destination_addrs)

    def test_failed_initialization_does_not_publish_an_engine_and_can_retry(self):
        te = engine.TransferEngine()
        bad_config = json.dumps({"rpc_server_hostname": "256.256.256.256"})
        with patch.dict(os.environ, {"MC_TENT_CONF": bad_config}):
            os.environ.pop("MOONCAKE_LOCAL_HOSTNAME", None)
            self.assertLess(te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)
            self.assertEqual(te.get_engine_ptr(), 0)
        self.assertEqual(te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)
        self.assertGreater(te.get_rpc_port(), 0)

    def test_tcp_instance_does_not_bypass_another_instances_protocol_checks(self):
        tcp = engine.TransferEngine()
        self.assertEqual(tcp.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)
        protocols = {
            "hip": "SUPPORT_HIP",
            "musa": "SUPPORT_MUSA",
            "nvlink": "SUPPORT_MNNVL",
            "nvlink_intra": "SUPPORT_INTRA_NVLINK",
        }
        for protocol, capability in protocols.items():
            if getattr(engine, capability):
                continue
            with self.subTest(protocol=protocol):
                te = engine.TransferEngine()
                self.assertLess(
                    te.initialize("127.0.0.1", "P2PHANDSHAKE", protocol, ""), 0
                )
                self.assertEqual(te.get_engine_ptr(), 0)
                # Failed protocol selection must leave this instance retryable.
                self.assertEqual(
                    te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0
                )


if __name__ == "__main__":
    unittest.main()
