import ctypes
import json
import tempfile
import unittest
from pathlib import Path

import tent


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


def submit_and_wait(engine, requests, timeout_ms):
    batch_id = engine.allocate_transfer_batch(len(requests))
    if not batch_id:
        raise RuntimeError("no free transfer batch")
    try:
        engine.submit_transfer(batch_id, requests)
        return engine.wait_transfer(batch_id, timeout_ms=timeout_ms)
    finally:
        engine.free_transfer_batch(batch_id)


class PythonWaitTransferTest(unittest.TestCase):
    def test_write_reports_per_request_status(self):
        with tempfile.TemporaryDirectory() as directory:
            server = tent.TransferEngine(write_config(directory, "server.json"))
            client = tent.TransferEngine(write_config(directory, "client.json"))
            buffer_size = 4096
            payload = b"wait-transfer-payload"
            server_addr = server.allocate_local_memory(buffer_size)
            client_addr = client.allocate_local_memory(buffer_size)
            server.register_local_memory(server_addr, buffer_size)
            client.register_local_memory(client_addr, buffer_size)
            segment = 0
            try:
                ctypes.memmove(client_addr, payload, len(payload))
                segment = client.open_segment(server.get_segment_name())
                server_base = client.get_segment_info(segment).buffers[0].base

                write_status = submit_and_wait(
                    client,
                    [
                        tent.Request(
                            opcode=tent.OpCode.WRITE,
                            source=client_addr,
                            target_id=segment,
                            target_offset=server_base,
                            length=len(payload),
                        )
                    ],
                    5000,
                )
                self.assertEqual(len(write_status), 1)
                self.assertEqual(
                    write_status[0].state, tent.TransferStatusEnum.COMPLETED
                )
                self.assertEqual(write_status[0].bytes, len(payload))
                self.assertEqual(ctypes.string_at(server_addr, len(payload)), payload)
            finally:
                if segment:
                    client.close_segment(segment)
                client.unregister_local_memory(client_addr, buffer_size)
                server.unregister_local_memory(server_addr, buffer_size)
                client.free_local_memory(client_addr)
                server.free_local_memory(server_addr)


if __name__ == "__main__":
    unittest.main()
