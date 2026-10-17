"""Check that guard exits return safely and preserve Python exceptions."""

import json
import tempfile
import unittest
from pathlib import Path

import tent


class PythonGuardTest(unittest.TestCase):
    @staticmethod
    def _tcp_engine(directory):
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
        return tent.TransferEngine(str(config))

    def test_context_manager_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._tcp_engine(directory)
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

    def test_task_congestion_query_lifecycle(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._tcp_engine(directory)
            self.assertTrue(engine.available())
            batch_id = engine.allocate_transfer_batch(1)
            try:
                for invalid_batch in (0, 2**64 - 1, batch_id):
                    with self.subTest(batch_id=invalid_batch):
                        with self.assertRaises(tent.InvalidArgumentError):
                            engine.get_task_congestion_state(invalid_batch, 0)
                        with self.assertRaises(tent.InvalidArgumentError):
                            engine.get_task_congestion_detail(invalid_batch, 0)
            finally:
                engine.free_transfer_batch(batch_id)
            with self.assertRaises(tent.InvalidArgumentError):
                engine.get_task_congestion_state(batch_id, 0)

    def test_tcp_task_congestion_detail_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._tcp_engine(directory)
            self.assertTrue(engine.available())
            with engine.allocate_memory_guard(4096) as address:
                engine.register_local_memory(address, 4096)
                try:
                    batch_id = engine.allocate_transfer_batch(1)
                    try:
                        request = tent.Request(
                            tent.OpCode.WRITE,
                            address,
                            tent.LOCAL_SEGMENT_ID,
                            address,
                            64,
                            transport_hint=tent.TransportType.TCP,
                        )
                        engine.submit_transfer(batch_id, [request])
                        state = engine.get_task_congestion_state(batch_id, 0)
                        detail = engine.get_task_congestion_detail(batch_id, 0)
                        self.assertEqual(state, tent.TaskCongestionState.UNKNOWN)
                        self.assertEqual(detail["state"], state)
                        self.assertEqual(
                            detail["attempt_kind"],
                            tent.TaskCongestionAttemptKind.OTHER,
                        )
                        self.assertIsNone(detail["reason"])
                        self.assertIsNone(detail["affected_path"])
                        self.assertIsNone(detail["window_bytes"])
                    finally:
                        engine.free_transfer_batch(batch_id)
                finally:
                    engine.unregister_local_memory(address, 4096)


if __name__ == "__main__":
    unittest.main()
