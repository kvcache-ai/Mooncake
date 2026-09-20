"""Check that guard exits return safely and preserve Python exceptions."""

import json
import tempfile
import unittest
from pathlib import Path

import tent


class PythonGuardTest(unittest.TestCase):
    def test_context_manager_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "tent.json"
            config.write_text(
                json.dumps(
                    {
                        "metadata_type": "p2p",
                        "rpc_server_hostname": "127.0.0.1",
                        "rpc_server_port": 0,
                        "transports": {
                            "tcp": {"enable": True},
                            "hp_tcp": {"enable": False},
                            "rdma": {"enable": False},
                            "shm": {"enable": False},
                        },
                    }
                )
            )
            engine = tent.TransferEngine(str(config))
            self.assertTrue(engine.available())
            for name, allocate in (
                ("memory", lambda: engine.allocate_memory_guard(4096)),
                ("batch", lambda: engine.allocate_batch_guard(1)),
            ):
                with self.subTest(guard=name, exit="normal"):
                    with allocate():
                        pass
                with self.subTest(guard=name, exit="exception"):
                    expected = ValueError("exception from the with body")
                    with self.assertRaises(ValueError) as raised:
                        with allocate():
                            raise expected
                    self.assertIs(raised.exception, expected)


if __name__ == "__main__":
    unittest.main()
