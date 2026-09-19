import ctypes
import json
import os
import socket
import struct
import threading
import unittest
from unittest.mock import patch

import engine


def receive_exact(connection, length):
    data = bytearray()
    while len(data) < length:
        chunk = connection.recv(length - len(data))
        if not chunk:
            raise EOFError("incomplete HP TCP frame")
        data.extend(chunk)
    return bytes(data)


class DropWriteAckProxy:
    """Forward real HP TCP writes, then lose the first committed write's ACK."""

    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.2", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.writes = 0
        self.error = None
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            while not self.stopped.is_set():
                try:
                    connection, _ = self.listener.accept()
                except TimeoutError:
                    continue
                with connection, socket.create_connection(
                    ("127.0.0.1", self.port), timeout=5
                ) as target:
                    connection.settimeout(5)
                    header = receive_exact(connection, 48)
                    if header[6] != 2:
                        raise AssertionError("expected an HP TCP WRITE")
                    length = struct.unpack_from("!Q", header, 32)[0]
                    target.sendall(header + receive_exact(connection, length))
                    response = receive_exact(target, 32)
                    status = struct.unpack_from("!H", response, 6)[0]
                    committed = struct.unpack_from("!Q", response, 16)[0]
                    if status != 0 or committed != length:
                        raise AssertionError("target did not commit the WRITE")
                    self.writes += 1
                    if self.writes > 1:
                        connection.sendall(response)
                    # Closing the first connection loses only the ACK, after
                    # the real target has already committed the payload.
        except Exception as error:
            self.error = error

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=6)
        self.listener.close()
        if self.thread.is_alive():
            raise AssertionError("proxy did not stop")
        if self.error:
            raise self.error


def initialize(hp_port=0, advertised_host="127.0.0.1"):
    config = {
        "transports": {
            "tcp": {"enable": False},
            "rdma": {"enable": False},
            "shm": {"enable": False},
            "gds": {"enable": False},
            "hp_tcp": {
                "enable": True,
                "bind_address": "127.0.0.1",
                "advertise_address": advertised_host,
                "port": hp_port,
                "worker_count": 1,
                "connections_per_peer": 1,
            },
        }
    }
    with patch.dict(os.environ, {"MC_TENT_CONF": json.dumps(config)}):
        te = engine.TransferEngine()
        if te.initialize("127.0.0.1", "P2PHANDSHAKE", "", "") != 0:
            raise AssertionError("TENT initialization failed")
    return te


class TentSyncNoReplayTest(unittest.TestCase):
    def check_write(self, batched):
        proxy = DropWriteAckProxy()
        self.addCleanup(proxy.close)
        target = initialize(proxy.port, "127.0.0.2")
        initiator = initialize()
        source = ctypes.create_string_buffer(b"committed before ACK loss", 4096)
        destination = ctypes.create_string_buffer(4096)
        source_address = ctypes.addressof(source)
        destination_address = ctypes.addressof(destination)
        self.assertEqual(initiator.register_memory(source_address, 4096), 0)
        self.assertEqual(target.register_memory(destination_address, 4096), 0)
        self.addCleanup(lambda: initiator.unregister_memory(ctypes.addressof(source)))
        self.addCleanup(lambda: target.unregister_memory(ctypes.addressof(destination)))
        name = f"127.0.0.1:{target.get_rpc_port()}"
        if batched:
            result = initiator.batch_transfer_sync_write(
                name, [source_address], [destination_address], [4096], "hp_tcp"
            )
        else:
            result = initiator.transfer_sync_write(
                name, source_address, destination_address, 4096, "hp_tcp"
            )
        self.assertEqual(destination.raw, source.raw)
        self.assertEqual(proxy.writes, 1, "Python replayed a committed WRITE")
        self.assertEqual(result, -1, "a lost ACK must report an uncertain failure")

    def test_single_write_is_not_replayed(self):
        self.check_write(batched=False)

    def test_batch_write_is_not_replayed(self):
        self.check_write(batched=True)


if __name__ == "__main__":
    unittest.main()
