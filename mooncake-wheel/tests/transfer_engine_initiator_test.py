import unittest
import json
import subprocess
import sys
import textwrap
import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from mooncake import engine as engine_module
import os
import ctypes
from mooncake.engine import TransferEngine


class TestVLLMAdaptorTransfer(unittest.TestCase):
    DEFAULT_BUFFER_CAPACITY = 64 * 1024 * 1024

    @classmethod
    def setUpClass(cls):
        cls.target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
        cls.initiator_server_name = os.getenv(
            "INITIATOR_SERVER_NAME", "127.0.0.1:12347"
        )
        cls.metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
        cls.protocol = os.getenv("PROTOCOL", "tcp")  # "rdma" or "tcp"
        cls.circle = int(os.getenv("CIRCLE", 1000))

        cls.adaptor = TransferEngine()
        ret = cls.adaptor.initialize(
            cls.initiator_server_name, cls.metadata_server, cls.protocol, ""
        )
        if ret != 0:
            raise RuntimeError(f"Initialization failed with code {ret}")

        if cls.metadata_server == "P2PHANDSHAKE":
            cls.initiator_server_name = (
                cls.initiator_server_name.rpartition(":")[0]
                + ":"
                + str(cls.adaptor.get_rpc_port())
            )

        cls._fallback_buffer_addr = None

    @classmethod
    def _ensure_buffer_available(cls):
        src_addr = cls.adaptor.get_first_buffer_address(cls.initiator_server_name)
        if src_addr == 0:
            cls._fallback_buffer_addr = cls.adaptor.allocate_managed_buffer(
                cls.DEFAULT_BUFFER_CAPACITY
            )
            if cls._fallback_buffer_addr == 0:
                raise RuntimeError("Failed to allocate fallback buffer")
            src_addr = cls._fallback_buffer_addr
        return src_addr

    @classmethod
    def tearDownClass(cls):
        if cls._fallback_buffer_addr is not None:
            cls.adaptor.free_managed_buffer(
                cls._fallback_buffer_addr, cls.DEFAULT_BUFFER_CAPACITY
            )
            cls._fallback_buffer_addr = None

    def test_random_write_circle_times(self):
        """Test circle times of random string write/read via buffer transfer."""
        import random
        import string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        circles = self.circle

        src_addr = self._ensure_buffer_available()
        dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(dst_addr, 0, "Target server has no registered buffers")

        for i in range(circles):
            str_len = random.randint(16, 256)
            src_data = generate_random_string(str_len).encode("utf-8")
            data_len = len(src_data)

            # Write to local buffer
            result = adaptor.write_bytes_to_buffer(src_addr, src_data, data_len)
            self.assertEqual(result, 0, f"[{i}] writeBytesToBuffer failed")

            # Write to the remote end
            result = adaptor.transfer_sync_write(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] WRITE transferSyncExt failed")

            # Clear the local buffer
            clear_data = bytes([0] * data_len)
            result = adaptor.write_bytes_to_buffer(src_addr, clear_data, data_len)
            self.assertEqual(result, 0, f"[{i}] Clear buffer failed")

            # Read it back from the remote end
            result = adaptor.transfer_sync_read(
                self.target_server_name, src_addr, dst_addr, data_len
            )
            self.assertEqual(result, 0, f"[{i}] READ transferSyncExt failed")

            # Verify data consistency
            read_back = adaptor.read_bytes_from_buffer(src_addr, data_len)
            self.assertEqual(read_back, src_data, f"[{i}] Data mismatch")

        print(f"[✓] {circles} iterations of random write-read passed successfully.")

    def test_batch_write_read(self):
        """Test batch_transfer_sync_write and batch_transfer_sync_read for batch write/read consistency."""
        import random
        import string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        batch_size = 100  # Adjust batch size if needed
        circles = max(2, self.circle // 100)  # Number of batch test rounds

        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 1KB offset between each buffer

        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # Generate multiple groups of random data
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode("utf-8")
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # Write to local buffers in batch
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], data_list[j], data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] writeBytesToBuffer failed")

            # Batch write to remote
            result = adaptor.batch_transfer_sync_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_write failed")

            # Clear local buffers
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], clear_data, data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] Clear buffer failed")

            # Batch read back from remote
            result = adaptor.batch_transfer_sync_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertEqual(result, 0, f"[{i}] batch_transfer_sync_read failed")

            # Verify data consistency
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(
                    src_addr_list[j], data_len_list[j]
                )
                self.assertEqual(
                    read_back, data_list[j], f"[{i}-{j}] Data mismatch in batch read"
                )

        print(
            f"[✓] {circles} rounds of batch_write_read passed, batch size {batch_size}."
        )

    def test_scatter_write_read(self):
        """Transfer allocation-bounded offset vectors through one operation."""

        adaptor = self.adaptor
        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        payloads = (b"scatter-range-a", b"scatter-range-b", b"scatter-range-c")
        write_local_offsets = ((0, 128), (1024,))
        remote_offsets = ((256, 512), (1536,))
        lengths = ((len(payloads[0]), len(payloads[1])), (len(payloads[2]),))
        for address, payload in zip(
            (
                base_src_addr + write_local_offsets[0][0],
                base_src_addr + write_local_offsets[0][1],
                base_src_addr + write_local_offsets[1][0],
            ),
            payloads,
            strict=True,
        ):
            self.assertEqual(
                adaptor.write_bytes_to_buffer(address, payload, len(payload)),
                0,
            )

        write_ticket = adaptor.scatter_transfer_sync_write_with_ticket(
            self.target_server_name,
            [base_src_addr, base_src_addr],
            [4096, 4096],
            [base_dst_addr, base_dst_addr],
            [4096, 4096],
            [list(offsets) for offsets in write_local_offsets],
            [list(offsets) for offsets in remote_offsets],
            [list(sizes) for sizes in lengths],
        )
        self.assertTrue(write_ticket.drained)
        self.assertIn("COMPLETED", str(write_ticket.status))

        read_local_offsets = ((2048, 2304), (3072,))
        read_ticket = adaptor.scatter_transfer_sync_read_with_ticket(
            self.target_server_name,
            [base_src_addr, base_src_addr],
            [4096, 4096],
            [base_dst_addr, base_dst_addr],
            [4096, 4096],
            [list(offsets) for offsets in read_local_offsets],
            [list(offsets) for offsets in remote_offsets],
            [list(sizes) for sizes in lengths],
        )
        self.assertTrue(read_ticket.drained)
        self.assertIn("COMPLETED", str(read_ticket.status))

        for address, payload in zip(
            (
                base_src_addr + read_local_offsets[0][0],
                base_src_addr + read_local_offsets[0][1],
                base_src_addr + read_local_offsets[1][0],
            ),
            payloads,
            strict=True,
        ):
            self.assertEqual(
                adaptor.read_bytes_from_buffer(address, len(payload)),
                payload,
            )

    def test_scatter_submit_write_read(self):
        """Submit scatter transfers without draining them in the binding."""

        adaptor = self.adaptor
        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        payload = b"scatter-submit"
        self.assertEqual(
            adaptor.write_bytes_to_buffer(base_src_addr, payload, len(payload)),
            0,
        )

        write_ticket = adaptor.submit_scatter_write(
            self.target_server_name,
            [base_src_addr],
            [4096],
            [base_dst_addr],
            [4096],
            [[0]],
            [[256]],
            [[len(payload)]],
        )
        self.assertFalse(write_ticket.drained)
        self.assertIn("COMPLETED", str(write_ticket.drain(30_000)))
        self.assertTrue(write_ticket.drained)

        self.assertEqual(
            adaptor.write_bytes_to_buffer(
                base_src_addr + 512, bytes(len(payload)), len(payload)
            ),
            0,
        )
        read_ticket = adaptor.submit_scatter_read(
            self.target_server_name,
            [base_src_addr],
            [4096],
            [base_dst_addr],
            [4096],
            [[512]],
            [[256]],
            [[len(payload)]],
        )
        self.assertFalse(read_ticket.drained)
        self.assertIn("COMPLETED", str(read_ticket.drain(30_000)))
        self.assertTrue(read_ticket.drained)
        self.assertEqual(
            adaptor.read_bytes_from_buffer(base_src_addr + 512, len(payload)),
            payload,
        )

    def test_scatter_rejects_mismatched_fragment_vectors(self):
        adaptor = self.adaptor
        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        ticket = adaptor.scatter_transfer_sync_write_with_ticket(
            self.target_server_name,
            [base_src_addr],
            [4096],
            [base_dst_addr],
            [4096],
            [[0, 128]],
            [[256]],
            [[16, 16]],
        )
        self.assertTrue(ticket.drained)
        self.assertIn("FAILED_DRAINED", str(ticket.status))

    def test_scatter_rejects_fragment_outside_allocation(self):
        adaptor = self.adaptor
        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        ticket = adaptor.scatter_transfer_sync_write_with_ticket(
            self.target_server_name,
            [base_src_addr],
            [4096],
            [base_dst_addr],
            [4096],
            [[4088]],
            [[0]],
            [[16]],
        )
        self.assertTrue(ticket.drained)
        self.assertIn("FAILED_DRAINED", str(ticket.status))

    def test_scatter_rejects_local_allocation_address_overflow(self):
        adaptor = self.adaptor
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")
        max_address = (1 << 64) - 1
        stderr_read, stderr_write = os.pipe()
        saved_stderr = os.dup(2)
        try:
            os.dup2(stderr_write, 2)
            for transfer in (
                adaptor.scatter_transfer_sync_write_with_ticket,
                adaptor.scatter_transfer_sync_read_with_ticket,
            ):
                ticket = transfer(
                    self.target_server_name,
                    [max_address],
                    [1],
                    [base_dst_addr],
                    [1],
                    [[0]],
                    [[0]],
                    [[1]],
                )
                self.assertTrue(ticket.drained)
                self.assertIn("FAILED_DRAINED", str(ticket.status))
        finally:
            os.dup2(saved_stderr, 2)
            os.close(saved_stderr)
            os.close(stderr_write)
        native_stderr = os.read(stderr_read, 64 * 1024)
        os.close(stderr_read)
        self.assertNotIn(hex(max_address).encode(), native_stderr)

    def test_scatter_rejects_local_address_range_overflow(self):
        max_local_address = (1 << (ctypes.sizeof(ctypes.c_void_p) * 8)) - 1

        for direction in ("write", "read"):
            with self.subTest(direction=direction):
                transfer = getattr(
                    self.adaptor,
                    f"scatter_transfer_sync_{direction}_with_ticket",
                )
                ticket = transfer(
                    self.target_server_name,
                    [max_local_address],
                    [2],
                    [0],
                    [0],
                    [[0]],
                    [[0]],
                    [[0]],
                )
                self.assertTrue(ticket.drained)
                self.assertIn("FAILED_DRAINED", str(ticket.status))

    def test_async_batch_write_read(self):
        """Test batch_transfer_async_write and batch_transfer_async_read for batch write/read consistency."""
        import random
        import string

        def generate_random_string(length):
            chars = string.ascii_letters + string.digits + string.punctuation
            return "".join(random.choices(chars, k=length))

        adaptor = self.adaptor
        batch_size = 100  # Adjust batch size if needed
        circles = max(2, self.circle // 100)  # Number of batch test rounds

        base_src_addr = self._ensure_buffer_available()
        base_dst_addr = adaptor.get_first_buffer_address(self.target_server_name)
        self.assertNotEqual(base_dst_addr, 0, "Target server has no registered buffers")

        src_addr_list = []
        dst_addr_list = []
        offset_size = 1024  # 1KB offset between each buffer

        for i in range(batch_size):
            src_addr_list.append(base_src_addr + i * offset_size)
            dst_addr_list.append(base_dst_addr + i * offset_size)

        for i in range(circles):
            # Generate multiple groups of random data
            data_list = []
            data_len_list = []
            for _ in range(batch_size):
                str_len = random.randint(32, min(128, offset_size))
                src_data = generate_random_string(str_len).encode("utf-8")
                data_list.append(src_data)
                data_len_list.append(len(src_data))

            # Write to local buffers in batch
            for j in range(batch_size):
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], data_list[j], data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] writeBytesToBuffer failed")

            # Batch write to remote
            batch_id = adaptor.batch_transfer_async_write(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(
                batch_id,
                0,
                f"[{i}] batch_transfer_async_write {batch_id} failed in submitting task(s)",
            )

            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(
                result, 0, f"[{i}] batch {batch_id} failed during transferring"
            )

            # Clear local buffers
            for j in range(batch_size):
                clear_data = bytes([0] * data_len_list[j])
                result = adaptor.write_bytes_to_buffer(
                    src_addr_list[j], clear_data, data_len_list[j]
                )
                self.assertEqual(result, 0, f"[{i}-{j}] Clear buffer failed")

            # Batch read back from remote
            batch_id = adaptor.batch_transfer_async_read(
                self.target_server_name, src_addr_list, dst_addr_list, data_len_list
            )
            self.assertNotEqual(
                batch_id,
                0,
                f"[{i}] batch_transfer_async_read {batch_id} failed in submitting task(s)",
            )

            result = adaptor.get_batch_transfer_status([batch_id])
            self.assertEqual(
                result, 0, f"[{i}] batch {batch_id} failed during transferring"
            )

            # Verify data consistency
            for j in range(batch_size):
                read_back = adaptor.read_bytes_from_buffer(
                    src_addr_list[j], data_len_list[j]
                )
                self.assertEqual(
                    read_back, data_list[j], f"[{i}-{j}] Data mismatch in batch read"
                )

        print(
            f"[✓] {circles} rounds of batch_write_async_read passed, batch size {batch_size}."
        )

    def test_async_submit_to_unknown_peer_returns_zero(self):
        """Async submit APIs return the documented failure sentinel 0.

        openSegment failure used to surface as -1 cast through the unsigned
        batch_id_t, i.e. 2**64 - 1, which passes the documented
        `batch_id != 0` failure check and crashes get_batch_transfer_status.
        """
        # "unreachable_peer:9" is not registered with the metadata server, so
        # openSegment fails before any buffer address is dereferenced.
        batch_id = self.adaptor.batch_transfer_async_write(
            "unreachable_peer:9", [0], [0], [16]
        )
        self.assertEqual(
            batch_id,
            0,
            f"batch_transfer_async_write to unknown peer returned {batch_id}",
        )

        batch_id = self.adaptor.transfer_submit_write("unreachable_peer:9", 0, 0, 16)
        self.assertEqual(
            batch_id,
            0,
            f"transfer_submit_write to unknown peer returned {batch_id}",
        )

    def test_send_probe_reachable_peer(self):
        """send_probe against a registered, reachable peer returns 0."""
        rc = self.adaptor.send_probe(self.target_server_name)
        self.assertEqual(
            rc,
            0,
            f"send_probe to reachable peer {self.target_server_name} returned {rc}",
        )

    def test_send_probe_unknown_peer(self):
        """send_probe against an unknown peer returns non-zero (does not crash)."""
        # "unreachable_peer:9" is not registered with the metadata server, so the
        # metadata lookup itself should fail and sendProbe returns ERR_METADATA.
        rc = self.adaptor.send_probe("unreachable_peer:9")
        self.assertNotEqual(
            rc,
            0,
            "send_probe to unknown peer unexpectedly succeeded",
        )


class TestTransferEngineInitialization(unittest.TestCase):
    @contextmanager
    def metadata_server(self, *, reject_writes=False):
        entries, deleted = {}, []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def respond(self, status, body=b"OK"):
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def key(self):
                return parse_qs(urlsplit(self.path).query).get("key", [""])[0]

            def do_GET(self):
                value = entries.get(self.key())
                self.respond(404 if value is None else 200, value or b"")

            def do_PUT(self):
                value = self.rfile.read(int(self.headers["Content-Length"]))
                if reject_writes:
                    self.respond(503)
                else:
                    entries[self.key()] = value
                    self.respond(200)

            def do_DELETE(self):
                key = self.key()
                deleted.append(key)
                entries.pop(key, None)
                self.respond(200)

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()  # The constructor has already bound/listened.
            try:
                yield (
                    f"http://127.0.0.1:{server.server_port}/metadata",
                    entries,
                    deleted,
                )
            finally:
                server.shutdown()
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive())

    def initialize(self, connections, *, selector=None):
        # Fatal native regressions must not kill the test runner.
        code = textwrap.dedent("""
            import json
            import os
            import resource
            import sys
            from mooncake import engine as module

            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            use_tent = any(os.getenv(name) == "1" for name in
                           ("MC_USE_TENT", "MC_USE_TEV1"))
            assert not use_tent or module.SUPPORT_TENT
            engine = module.TransferEngine()
            results, ports = [], []
            for connection in sys.argv[1:]:
                results.append(engine.initialize("127.0.0.1", connection, "tcp", ""))
                if use_tent:
                    ports.append(engine.get_rpc_port())
            del engine
            print(json.dumps([results, ports]), flush=True)
        """)
        environment = {**os.environ, "MC_FORCE_TCP": "1"}
        for name in ("MC_USE_TENT", "MC_USE_TEV1"):
            environment.pop(name, None)
        if selector:
            self.assertTrue(engine_module.SUPPORT_TENT)
            environment[selector] = "1"
        process = subprocess.run(
            [sys.executable, "-X", "faulthandler", "-c", code, *connections],
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        results, ports = json.loads(process.stdout.splitlines()[-1])
        if selector:
            for result, port in zip(results, ports):
                if result != 0:
                    self.assertEqual(
                        port, 0, "failed TENT engine retained an RPC service"
                    )
        return results

    def supports(self, backend, selector=None):
        prefix = (
            "SUPPORT_TENT_" if selector and backend in ("HTTP", "REDIS") else "SUPPORT_"
        )
        return getattr(engine_module, prefix + backend)

    def unavailable_connections(self, selector=None):
        connections = ["unsupported://127.0.0.1:1"]
        if not self.supports("ETCD", selector):
            connections += ["127.0.0.1:1", "etcd://127.0.0.1:1"]
        if not self.supports("REDIS", selector):
            connections.append("redis://127.0.0.1:1")
        return connections

    def selectors(self):
        return [None] + (
            ["MC_USE_TENT", "MC_USE_TEV1"] if engine_module.SUPPORT_TENT else []
        )

    def test_unavailable_metadata_returns_error(self):
        connections = self.unavailable_connections()
        self.assertEqual(self.initialize(connections), [-1] * len(connections))

    def test_tent_unavailable_metadata_returns_error(self):
        if not engine_module.SUPPORT_TENT:
            self.skipTest("TENT is not compiled into this binding")
        for selector in ("MC_USE_TENT", "MC_USE_TEV1"):
            connections = self.unavailable_connections(selector)
            with self.subTest(selector=selector):
                self.assertEqual(
                    self.initialize(connections, selector=selector),
                    [-1] * len(connections),
                )

    def test_can_initialize_p2p_after_rejected_configuration(self):
        connections = ["unsupported://127.0.0.1:1"] * 4 + ["P2PHANDSHAKE"]
        for selector in self.selectors():
            with self.subTest(selector=selector):
                self.assertEqual(
                    self.initialize(connections, selector=selector), [-1] * 4 + [0]
                )

    def test_http_metadata_initialization(self):
        selectors = [
            selector for selector in self.selectors() if self.supports("HTTP", selector)
        ]
        if not selectors:
            self.skipTest("HTTP metadata is not compiled into this binding")
        with self.metadata_server() as (connection, _entries, deleted):
            for selector in selectors:
                with self.subTest(selector=selector):
                    before = len(deleted)
                    self.assertEqual(
                        self.initialize([connection], selector=selector), [0]
                    )
                    self.assertGreater(len(deleted), before)

    def test_http_connection_failure(self):
        selectors = [
            selector for selector in self.selectors() if self.supports("HTTP", selector)
        ]
        if not selectors:
            self.skipTest("HTTP metadata is not compiled into this binding")
        # A bound socket without listen() guarantees a local refused connection.
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            connection = f"http://127.0.0.1:{reserved.getsockname()[1]}/metadata"
            for selector in selectors:
                with self.subTest(selector=selector):
                    self.assertEqual(
                        self.initialize(
                            [connection, "P2PHANDSHAKE"], selector=selector
                        ),
                        [-1, 0],
                    )

    def test_redis_connection_failure(self):
        selectors = [
            selector
            for selector in self.selectors()
            if self.supports("REDIS", selector)
        ]
        if not selectors:
            self.skipTest("Redis is not compiled into this binding")
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            connection = f"redis://127.0.0.1:{reserved.getsockname()[1]}"
            for selector in selectors:
                with self.subTest(selector=selector):
                    self.assertEqual(
                        self.initialize(
                            [connection, "P2PHANDSHAKE"], selector=selector
                        ),
                        [-1, 0],
                    )

    def test_tent_failed_publication_preserves_existing_metadata(self):
        if not (engine_module.SUPPORT_TENT and engine_module.SUPPORT_TENT_HTTP):
            self.skipTest("Requires TENT and HTTP metadata")
        with self.metadata_server(reject_writes=True) as (connection, entries, deleted):
            key = "mooncake/tent/127.0.0.1"
            entries[key] = b'{"owner":"existing"}'
            for selector in ("MC_USE_TENT", "MC_USE_TEV1"):
                with self.subTest(selector=selector):
                    self.assertEqual(
                        self.initialize(
                            [connection, "P2PHANDSHAKE"], selector=selector
                        ),
                        [-1, 0],
                    )
                    self.assertEqual(entries, {key: b'{"owner":"existing"}'})
                    self.assertEqual(deleted, [])


if __name__ == "__main__":
    unittest.main()
