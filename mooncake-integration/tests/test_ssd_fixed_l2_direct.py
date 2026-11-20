"""CPU Python binding test using a real master, SSD, TCP and lost RPC reply.

Run with PYTHONPATH=<build>/mooncake-integration and
MOONCAKE_TEST_MASTER=<build>/mooncake-store/src/mooncake_master.
"""

import ctypes
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import _store as store


def wait_for(predicate, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for test condition")


class LostReplyProxy:
    """Discard first terminal reply; forward recovery to the unchanged Owner."""

    def __init__(self, host, port):
        self.upstream = (host, port)
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.closed = threading.Event()
        self.lost = threading.Event()
        self.errors = []
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    @staticmethod
    def receive(sock, length):
        data = bytearray()
        while len(data) < length:
            block = sock.recv(length - len(data))
            if not block:
                raise EOFError("Short RPC frame")
            data.extend(block)
        return bytes(data)

    def run(self):
        first = True
        while not self.closed.is_set():
            try:
                incoming, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                with incoming, socket.create_connection(self.upstream, 5) as outgoing:
                    incoming.settimeout(5)
                    outgoing.settimeout(5)
                    head = self.receive(incoming, 20)
                    length = struct.unpack_from("<I", head, 12)[0]
                    if length > 2 * 1024 * 1024:
                        raise ValueError("Unexpected RPC body size")
                    outgoing.sendall(head + self.receive(incoming, length))
                    # The test replies fit in this recv. Closing downstream
                    # without sending bytes forces a lost terminal reply.
                    response = outgoing.recv(65536)
                    if first:
                        first = False
                        self.lost.set()
                    else:
                        incoming.sendall(response)
            except Exception as error:
                self.errors.append(str(error))

    def close(self):
        self.closed.set()
        self.listener.close()
        self.thread.join(6)


class FixedL2DirectPythonTest(unittest.TestCase):
    def test_api_after_successful_close_returns_unavailable(self):
        # A separate process makes any null-pointer dereference a clean RED.
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import _store as store; s=store.MooncakeDistributedStore(); "
                "assert s.close()==0; assert s.get_ssd_push_peer_info()=='{}'; "
                "assert s.configure_ssd_push_peers([])==-1; assert s.recover_ssd_push()==-1",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_native_ssd_terminal_reply_and_lost_reply_recovers_before_return(self):
        binary = os.environ["MOONCAKE_TEST_MASTER"]
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        with tempfile.TemporaryDirectory(prefix="mooncake_fixed_l2_python_") as folder:
            with open(Path(folder) / "master.log", "w") as log:
                master = subprocess.Popen(
                    [
                        binary,
                        f"--rpc_port={port}",
                        "--rpc_address=127.0.0.1",
                        "--enable_offload=true",
                        "--default_kv_lease_ttl=1s",
                        "--metrics_port=0",
                        "--http_metadata_server_port=0",
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                owner = store.MooncakeDistributedStore()
                requester = store.MooncakeDistributedStore()
                proxy = None
                # Keep this allocation alive until close is acknowledged.
                size = 3 * 1024 * 1024 + 123
                pool = (ctypes.c_ubyte * (size + 64))()
                pointer = ctypes.addressof(pool)
                try:

                    def ready():
                        if master.poll() is not None:
                            raise AssertionError(
                                (Path(folder) / "master.log").read_text()
                            )
                        try:
                            with socket.create_connection(("127.0.0.1", port), 0.2):
                                return True
                        except OSError:
                            return False

                    wait_for(ready)
                    os.environ.update(
                        {
                            "MOONCAKE_OFFLOAD_SSD_PUSH": "true",
                            "MOONCAKE_OFFLOAD_STREAM_USE_URING": os.environ.get(
                                "MOONCAKE_TEST_SSD_USE_URING", "true"
                            ),
                            "MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES": "524288",
                            "MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES": "65536",
                            "MOONCAKE_OFFLOAD_STREAM_SLOTS": "2",
                            "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS": "1",
                            "MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT": "500",
                            "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS": "1",
                        }
                    )
                    address = f"127.0.0.1:{port}"
                    Path(folder, "ssd").mkdir()
                    self.assertEqual(
                        owner.setup(
                            "localhost:17813",
                            "P2PHANDSHAKE",
                            16 * 1024 * 1024,
                            16 * 1024 * 1024,
                            "tcp",
                            "",
                            address,
                            None,
                            True,
                            folder + "/ssd",
                            "default",
                        ),
                        0,
                    )
                    self.assertEqual(
                        requester.setup(
                            "localhost:17814",
                            "P2PHANDSHAKE",
                            0,
                            1024 * 1024,
                            "tcp",
                            "",
                            address,
                        ),
                        0,
                    )
                    peer = json.loads(owner.get_ssd_push_peer_info())
                    self.assertTrue(peer["can_read_ssd"])
                    self.assertEqual(
                        owner.configure_ssd_push_peers(
                            [requester.get_ssd_push_peer_info()]
                        ),
                        0,
                    )
                    self.assertEqual(
                        requester.configure_ssd_push_peers([json.dumps(peer)]), 0
                    )
                    self.assertEqual(requester.register_buffer(pointer, size + 64), 0)
                    payload = (bytes(range(251)) * ((size + 250) // 251))[:size]
                    self.assertEqual(owner.put("fixed-l2-python", payload), 0)

                    def disk_ready():
                        replicas = owner.get_replica_desc("fixed-l2-python")
                        return any(r.is_local_disk_replica() for r in replicas)

                    wait_for(disk_ready)
                    wait_for(
                        lambda: bool(
                            owner.batch_replica_clear(
                                ["fixed-l2-python"], segment_name=owner.get_hostname()
                            )
                        )
                    )

                    def disk_only():
                        replicas = owner.get_replica_desc("fixed-l2-python")
                        return bool(replicas) and all(
                            r.is_local_disk_replica() for r in replicas
                        )

                    wait_for(disk_only)
                    ctypes.memset(pointer, 0xA5, size + 64)
                    self.assertEqual(
                        requester.batch_get_into(
                            ["fixed-l2-python"], [pointer + 32], [size]
                        ),
                        [size],
                    )
                    self.assertEqual(bytes(pool)[32:-32], payload)
                    self.assertEqual(bytes(pool)[:32], b"\xa5" * 32)
                    self.assertEqual(bytes(pool)[-32:], b"\xa5" * 32)
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 1
                    )

                    # Exercise the bounded whole-value batch path through the
                    # public API. Four adjacent CPU destinations fit within
                    # the two-slot staging budget and share one registration.
                    small_size = 8 * 1024
                    small_keys = [f"fixed-l2-small-{i}" for i in range(4)]
                    small_payloads = [
                        bytes([i + 1]) * small_size for i in range(len(small_keys))
                    ]
                    for small_key, small_payload in zip(
                        small_keys, small_payloads, strict=True
                    ):
                        self.assertEqual(owner.put(small_key, small_payload), 0)

                    def small_values_on_disk():
                        return all(
                            any(
                                r.is_local_disk_replica()
                                for r in owner.batch_get_replica_desc([k]).get(k, [])
                            )
                            for k in small_keys
                        )

                    wait_for(small_values_on_disk)
                    cleared = set()

                    def clear_small_memory_replicas():
                        cleared.update(
                            owner.batch_replica_clear(
                                small_keys, segment_name=owner.get_hostname()
                            )
                        )
                        return len(cleared) == len(small_keys)

                    wait_for(clear_small_memory_replicas)

                    def small_values_are_disk_only():
                        return all(
                            (replicas := owner.batch_get_replica_desc([k]).get(k, []))
                            and all(r.is_local_disk_replica() for r in replicas)
                            for k in small_keys
                        )

                    wait_for(small_values_are_disk_only)
                    ctypes.memset(pointer, 0xA5, size + 64)
                    small_ptrs = [pointer + 32 + i * small_size for i in range(4)]
                    self.assertEqual(
                        requester.batch_get_into(
                            small_keys, small_ptrs, [small_size] * len(small_keys)
                        ),
                        [small_size] * len(small_keys),
                    )
                    for i, small_payload in enumerate(small_payloads):
                        start = 32 + i * small_size
                        self.assertEqual(
                            bytes(pool[start : start + small_size]), small_payload
                        )
                    self.assertEqual(bytes(pool)[:32], b"\xa5" * 32)
                    self.assertEqual(
                        bytes(pool)[32 + len(small_keys) * small_size :],
                        b"\xa5" * (size + 32 - len(small_keys) * small_size),
                    )
                    self.assertEqual(bytes(pool)[-32:], b"\xa5" * 32)
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 2
                    )

                    # Many disjoint small TCP destinations cannot share data
                    # frames. Verify the pre-submit policy keeps this shape on
                    # the batched legacy path and preserves every gap.
                    ctypes.memset(pointer, 0xA5, size + 64)
                    gapped_ptrs = [
                        pointer + 32 + i * (small_size + 64)
                        for i in range(len(small_keys))
                    ]
                    self.assertEqual(
                        requester.batch_get_into(
                            small_keys, gapped_ptrs, [small_size] * len(small_keys)
                        ),
                        [small_size] * len(small_keys),
                    )
                    for i, small_payload in enumerate(small_payloads):
                        start = 32 + i * (small_size + 64)
                        self.assertEqual(
                            bytes(pool[start : start + small_size]), small_payload
                        )
                        self.assertEqual(
                            bytes(pool[start + small_size : start + small_size + 64]),
                            b"\xa5" * 64,
                        )
                    self.assertEqual(bytes(pool)[:32], b"\xa5" * 32)
                    self.assertEqual(
                        bytes(pool)[32 + len(small_keys) * (small_size + 64) :],
                        b"\xa5" * (size + 32 - len(small_keys) * (small_size + 64)),
                    )
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 2
                    )

                    overlapping = requester.batch_get_into(
                        small_keys,
                        [pointer + 32] * len(small_keys),
                        [small_size] * len(small_keys),
                    )
                    self.assertTrue(all(result < 0 for result in overlapping))
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 2
                    )

                    # Concurrent reads borrow distinct destinations in the
                    # same registered pool. Their attempts must retain their
                    # own request binding and drain before returning.
                    ctypes.memset(pointer, 0xA5, size + 64)
                    lane_bytes = len(small_keys) * small_size + 64

                    def read_small_lane(lane):
                        base = pointer + lane * lane_bytes + 32
                        addresses = [base + i * small_size for i in range(4)]
                        for _ in range(8):
                            self.assertEqual(
                                requester.batch_get_into(
                                    small_keys, addresses, [small_size] * 4
                                ),
                                [small_size] * 4,
                            )

                    with ThreadPoolExecutor(max_workers=4) as executor:
                        list(executor.map(read_small_lane, range(4)))
                    for lane in range(4):
                        start = lane * lane_bytes
                        self.assertEqual(bytes(pool[start : start + 32]), b"\xa5" * 32)
                        for i, small_payload in enumerate(small_payloads):
                            begin = start + 32 + i * small_size
                            self.assertEqual(
                                bytes(pool[begin : begin + small_size]), small_payload
                            )
                        self.assertEqual(
                            bytes(pool[start + lane_bytes - 32 : start + lane_bytes]),
                            b"\xa5" * 32,
                        )
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 34
                    )

                    proxy = LostReplyProxy(peer["control_host"], peer["control_port"])
                    peer["control_host"] = "127.0.0.1"
                    peer["control_port"] = proxy.port
                    self.assertEqual(
                        requester.configure_ssd_push_peers([json.dumps(peer)]), 0
                    )
                    result = requester.batch_get_into(
                        ["fixed-l2-python"], [pointer + 32], [size], 1000
                    )
                    self.assertLess(result[0], 0)
                    self.assertTrue(proxy.lost.is_set(), proxy.errors)
                    # Public Get owns exception recovery. FluentLLM treats
                    # this failed return as IODone and immediately reuses pages.
                    self.assertEqual(
                        json.loads(requester.get_ssd_push_peer_info())[
                            "pending_requests"
                        ],
                        0,
                    )
                    ctypes.memset(pointer, 0x52, size + 64)
                    time.sleep(0.05)
                    self.assertEqual(bytes(pool), b"\x52" * (size + 64))
                    self.assertEqual(requester.unregister_buffer(pointer), 0)
                    self.assertEqual(requester.close(), 0)
                    self.assertEqual(
                        json.loads(owner.get_ssd_push_peer_info())["read_requests"], 35
                    )
                    self.assertFalse(proxy.errors)
                finally:
                    if proxy and requester.health_check() != 1:
                        requester.recover_ssd_push()
                    requester.close()
                    if proxy:
                        proxy.close()
                    owner.close()
                    master.terminate()
                    try:
                        master.wait(5)
                    except subprocess.TimeoutExpired:
                        master.kill()
                        master.wait()


if __name__ == "__main__":
    unittest.main()
