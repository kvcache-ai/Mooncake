import ctypes
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import tent

COMPLETED = int(tent.TransferStatusEnum.COMPLETED)
INVALID = int(tent.TransferStatusEnum.INVALID)


def write_config(directory, name):
    config = Path(directory) / name
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
    return str(config)


class ArrayEntryPointTest(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.engine = tent.TransferEngine(
            write_config(self._directory.name, "engine.json")
        )

    def tearDown(self):
        self._directory.cleanup()

    def test_mismatched_lengths_report_invalid_states(self):
        source = np.zeros(2, dtype=np.uint64)
        target = np.zeros(2, dtype=np.uint64)
        lengths = np.zeros(3, dtype=np.uint64)
        states = self.engine.transfer_sync_arrays(
            source, target, lengths, 0, tent.OpCode.WRITE
        )
        self.assertEqual(states.tolist(), [INVALID, INVALID])

    def test_two_dimensional_arrays_report_invalid_states(self):
        source = np.zeros((2, 2), dtype=np.uint64)
        target = np.zeros((2, 2), dtype=np.uint64)
        lengths = np.zeros((2, 2), dtype=np.uint64)
        states = self.engine.transfer_sync_arrays(
            source, target, lengths, 0, tent.OpCode.WRITE
        )
        self.assertEqual(states.tolist(), [INVALID] * 4)

    def test_non_uint64_input_reports_invalid_states(self):
        source = np.zeros(2, dtype=np.float64)
        target = np.zeros(2, dtype=np.float64)
        lengths = np.zeros(2, dtype=np.float64)
        states = self.engine.transfer_sync_arrays(
            source, target, lengths, 0, tent.OpCode.WRITE
        )
        self.assertEqual(states.tolist(), [INVALID, INVALID])

    def test_strided_view_reports_invalid_states(self):
        source = np.zeros(4, dtype=np.uint64)[::2]
        target = np.zeros(4, dtype=np.uint64)[::2]
        lengths = np.zeros(4, dtype=np.uint64)[::2]
        states = self.engine.transfer_sync_arrays(
            source, target, lengths, 0, tent.OpCode.WRITE
        )
        self.assertEqual(states.tolist(), [INVALID, INVALID])

    def test_empty_batch_returns_no_states(self):
        empty = np.zeros(0, dtype=np.uint64)
        states = self.engine.transfer_sync_arrays(
            empty, empty, empty, 0, tent.OpCode.WRITE
        )
        self.assertEqual(states.dtype, np.uint8)
        self.assertEqual(states.shape, (0,))

    def test_empty_non_uint64_input_raises(self):
        empty = np.zeros(0, dtype=np.float64)
        with self.assertRaises(tent.InvalidArgumentError):
            self.engine.transfer_sync_arrays(empty, empty, empty, 0, tent.OpCode.WRITE)

    def test_empty_two_dimensional_input_raises(self):
        empty = np.zeros((0, 0), dtype=np.uint64)
        with self.assertRaises(tent.InvalidArgumentError):
            self.engine.transfer_sync_arrays(empty, empty, empty, 0, tent.OpCode.WRITE)

    def test_empty_source_with_non_empty_target_raises(self):
        source = np.zeros(0, dtype=np.uint64)
        target = np.zeros(2, dtype=np.uint64)
        lengths = np.zeros(0, dtype=np.uint64)
        with self.assertRaises(tent.InvalidArgumentError):
            self.engine.transfer_sync_arrays(
                source, target, lengths, 0, tent.OpCode.WRITE
            )


class ArrayTransferTest(unittest.TestCase):
    def test_write_and_read_move_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            server = tent.TransferEngine(write_config(directory, "server.json"))
            client = tent.TransferEngine(write_config(directory, "client.json"))
            buffer_size = 4096
            pattern = bytes(range(64)) * 64
            server_addr = server.allocate_local_memory(buffer_size)
            client_addr = client.allocate_local_memory(buffer_size)
            server.register_local_memory(server_addr, buffer_size)
            client.register_local_memory(client_addr, buffer_size)
            segment = None
            try:
                ctypes.memmove(client_addr, pattern, buffer_size)
                segment = client.open_segment(server.get_segment_name())
                server_base = client.get_segment_info(segment).buffers[0].base
                source = np.array([client_addr], dtype=np.uint64)
                target = np.array([server_base], dtype=np.uint64)
                lengths = np.array([buffer_size], dtype=np.uint64)

                states = client.transfer_sync_arrays(
                    source, target, lengths, segment, tent.OpCode.WRITE
                )
                self.assertEqual(states.tolist(), [COMPLETED])
                self.assertEqual(ctypes.string_at(server_addr, buffer_size), pattern)

                ctypes.memset(client_addr, 0, buffer_size)
                states = client.transfer_sync_arrays(
                    source, target, lengths, segment, tent.OpCode.READ
                )
                self.assertEqual(states.tolist(), [COMPLETED])
                self.assertEqual(ctypes.string_at(client_addr, buffer_size), pattern)
            finally:
                if segment is not None:
                    client.close_segment(segment)
                client.unregister_local_memory(client_addr, buffer_size)
                server.unregister_local_memory(server_addr, buffer_size)
                client.free_local_memory(client_addr)
                server.free_local_memory(server_addr)


if __name__ == "__main__":
    unittest.main()
