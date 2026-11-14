"""Test deployment lifecycle in each architecture's matching wheel environment.

MOONCAKE_REAL_CLIENT_COMMAND configures the daemon; MOONCAKE_MASTER_COMMAND
starts an isolated master per case. MOONCAKE_DEPLOYMENT_PROTOCOL selects the
daemon transport; Dummy SHM checks do not exercise cross-endpoint RDMA.
"""

import ctypes
import json
import os
from pathlib import Path
import select
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from urllib.request import urlopen
import uuid


def emit(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def dummy_worker(address, namespace, local_buffer_size=4 << 20):
    # Keep native stdout logs off the JSON reply channel.
    sys.stdout.flush()
    reply_fd = os.dup(sys.stdout.fileno())
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = os.fdopen(reply_fd, "w", buffering=1)
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    if store.setup_dummy(4 << 20, local_buffer_size, address) != 0:
        raise RuntimeError("Dummy setup failed")
    if local_buffer_size == 0:
        # Delay the first registration to exercise the no-SHM heartbeat path.
        emit({"ready": True})
        for line in sys.stdin:
            action = json.loads(line)["action"]
            if action == "probe":
                emit({"exists": store.is_exist(namespace)})
            elif action == "close":
                emit({"closed": store.close()})
                return
            elif action == "allocate":
                break
            else:
                raise ValueError(f"Unknown unregistered worker action: {action}")
        else:
            store.close()
            return
    buffer = store.alloc_from_mem_pool(4096)
    if not buffer or store.register_buffer(buffer, 4096) != 0:
        raise RuntimeError("Dummy buffer registration failed")
    values = [(namespace + "\0first").encode(), (namespace + "\0second").encode()]
    pointers = [buffer, buffer + 1024]
    targets = [buffer + 2048, buffer + 3072]
    sizes = [len(value) for value in values]
    for pointer, value in zip(pointers, values):
        ctypes.memmove(pointer, value, len(value))

    def intact():
        return all(ctypes.string_at(pointer, len(value)) == value
                   for pointer, value in zip(pointers, values))

    emit({"ready": True, "pid": os.getpid(), "namespace": namespace})
    for counter, line in enumerate(sys.stdin):
        command = json.loads(line)
        action = command["action"]
        if action not in ("memory", "close", "roundtrip", "local_io", "down"):
            raise ValueError(f"Unknown deployment worker action: {action}")
        keys = [f"{namespace}_{counter}_a", f"{namespace}_{counter}_b"]
        try:
            if action == "memory":
                emit({"intact": intact()})
                continue
            if action == "close":
                emit({"closed": store.close()})
                return
            shape_ok = True
            if action == "roundtrip":
                bad_keys = [key + "_bad" for key in keys]
                bad_put = store.batch_put_from(bad_keys, pointers, sizes[:1])
                bad_get = store.batch_get_into(bad_keys, targets[:1], [1024, 1024])
                shape_ok = (len(bad_put) == len(bad_get) == 2 and
                            all(code < 0 for code in bad_put + bad_get) and
                            all(store.is_exist(key) == 0 for key in bad_keys))
            if command.get("announce"):
                # This marks the caller boundary, not native transfer progress.
                emit({"calling": True})
            writes = store.batch_put_from(keys, pointers, sizes)
            reads = []
            content_ok = False
            short_ok = False
            single_ok = False
            single_write = None
            if action in ("roundtrip", "local_io") and writes == [0, 0]:
                if action == "roundtrip":
                    for pointer in targets:
                        ctypes.memset(pointer, 0x5a, 1024)
                    short = store.batch_get_into(keys, targets, [sizes[0] - 1, 1024])
                    short_ok = (len(short) == 2 and short[0] < 0 and short[1] == sizes[1]
                                and ctypes.string_at(targets[0], 1024) == b"Z" * 1024
                                and ctypes.string_at(targets[1], sizes[1]) == values[1])
                for pointer in targets:
                    ctypes.memset(pointer, 0x5a, 1024)
                reads = store.batch_get_into(keys, targets, [1024, 1024])
                content_ok = (reads == sizes and
                              all(ctypes.string_at(pointer, len(value)) == value
                                  for pointer, value in zip(targets, values)))
                if action == "roundtrip":
                    single_key = keys[0] + "_bytes"
                    single_write = store.put(single_key, values[0])
                    single_ok = (single_write == 0 and
                                 store.get(single_key) == values[0])
            emit({"writes": writes, "reads": reads, "sizes": sizes,
                  "content_ok": content_ok, "short_ok": short_ok,
                  "single_write": single_write, "single_ok": single_ok,
                  "shape_ok": shape_ok, "intact": intact()})
        except Exception:
            error = traceback.format_exc()
            sys.stderr.write(error)
            sys.stderr.flush()
            emit({"error": error})
    store.close()


class Worker:
    def __init__(self, process, log):
        self.process = process
        self.log = log
        self.pending = b""

    def send(self, action, **fields):
        message = json.dumps({"action": action, **fields}).encode() + b"\n"
        with self.log.with_suffix(".jsonl").open("ab") as trace:
            trace.write(json.dumps({"request": json.loads(message)}).encode() + b"\n")
        self.process.stdin.write(message)
        self.process.stdin.flush()

    def receive(self, timeout=30):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"Dummy response timed out; log: {self.log}")
            ready, _, _ = select.select([self.process.stdout], [], [], remaining)
            if not ready:
                continue
            data = os.read(self.process.stdout.fileno(), 4096)
            if not data:
                raise AssertionError(f"Dummy exited ({self.process.poll()}); log: {self.log}")
            self.pending += data
        line, self.pending = self.pending.split(b"\n", 1)
        with self.log.with_suffix(".jsonl").open("ab") as trace:
            trace.write(line + b"\n")
        response = json.loads(line)
        if "error" in response:
            raise AssertionError(response["error"])
        return response

    def call(self, action, **fields):
        self.send(action, **fields)
        return self.receive()


class TestSetupConfiguration(unittest.TestCase):
    def test_setup_configuration_errors_return_codes(self):
        from mooncake.store import MooncakeDistributedStore

        store = MooncakeDistributedStore()
        self.addCleanup(store.close)
        values = {"local_hostname": "127.0.0.1", "metadata_server": "P2PHANDSHAKE"}
        if hasattr(store, "setup_p2p_real_client"):
            values["tiered_backend_config"] = "{invalid json"
            self.assertEqual(store.setup_p2p_real_client(values), -600)
            self.assertEqual(store.setup_p2p_real_client(
                "127.0.0.1", "P2PHANDSHAKE", "{invalid json", 0,
                "tcp", "", "127.0.0.1:50051"), -600)
        else:
            values["local_buffer_size"] = "invalid"
            self.assertEqual(store.setup(values), -600)
            values["local_buffer_size"] = "0"
            values["client_rpc_port"] = "65536"
            self.assertEqual(store.setup(values), -600)
            self.assertEqual(store.setup(
                "", "P2PHANDSHAKE", 0, 0, "tcp", "", "127.0.0.1:50051"), -600)


@unittest.skipUnless(sys.platform == "linux" and
                     os.getenv("MOONCAKE_REAL_CLIENT_COMMAND"),
                     "Requires Linux, a matching daemon command and live master")
class TestDeploymentLifecycle(unittest.TestCase):
    def setUp(self):
        from mooncake.store import MooncakeDistributedStore

        self.run_dir = Path(tempfile.mkdtemp(prefix="mooncake-deployment-"))
        print(f"Deployment logs: {self.run_dir}", file=sys.stderr)
        self.processes = []
        self.logs = []
        self.addCleanup(self.cleanup)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        protocol = os.getenv("MOONCAKE_DEPLOYMENT_PROTOCOL", "tcp")
        self.command = shlex.split(os.environ["MOONCAKE_REAL_CLIENT_COMMAND"])
        if os.getenv("MOONCAKE_MASTER_COMMAND"):
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                master_port = reservation.getsockname()[1]
            master_command = shlex.split(os.environ["MOONCAKE_MASTER_COMMAND"])
            master_command += ["--rpc_address=127.0.0.1",
                               f"--rpc_port={master_port}", "--metrics_port=0",
                               "--enable_metric_reporting=false", "--enable_ha=false"]
            master, log = self.spawn(master_command, "master")
            self.master = master
            deadline = time.monotonic() + 20
            while True:
                self.assertIsNone(master.poll(), f"Master startup failed; log: {log}")
                try:
                    with socket.create_connection(("127.0.0.1", master_port), timeout=0.2):
                        break
                except OSError as error:
                    if time.monotonic() >= deadline:
                        self.fail(f"Master did not become ready ({error}); log: {log}")
                    time.sleep(0.05)
            self.command += [f"--master_server_address=127.0.0.1:{master_port}"]
        self.command += [f"--port={self.port}", "--host=127.0.0.1",
                         "--metadata_server=P2PHANDSHAKE", f"--protocol={protocol}",
                         "--client_rpc_port=0"]
        if hasattr(MooncakeDistributedStore, "setup_p2p_real_client"):
            # Keep secondary listeners from masking the intended collision.
            self.command += ["--enable_http_server=false",
                             "--enable_client_metric_collection=false",
                             "--async_sender_thread_count=0",
                             "--te_async_poll_worker_num=2",
                             '--runtime_config={"write":{"remote_weight":0.0}}']
        self.ipc = f"\0@mooncake_client_{self.port}.sock"
        self.workers = []

    def start_workers(self, count=2):
        for index in range(count):
            namespace = f"deployment_{uuid.uuid4().hex}_{index}"
            process, log = self.spawn(
                [sys.executable, "-u", str(Path(__file__).resolve()),
                 "--dummy-worker", f"127.0.0.1:{self.port}", namespace],
                f"dummy_{index}", pipes=True)
            worker = Worker(process, log)
            self.assertTrue(worker.receive().get("ready"))
            self.workers.append(worker)
            self.wait_roundtrip(worker)

    def spawn(self, command, name, pipes=False):
        path = self.run_dir / f"{name}_{len(self.processes)}.log"
        log = path.open("wb")
        self.logs.append(log)
        environment = dict(os.environ, GLOG_logtostderr="1")
        process = subprocess.Popen(
            command, stdin=subprocess.PIPE if pipes else subprocess.DEVNULL,
            stdout=subprocess.PIPE if pipes else log, stderr=log,
            env=environment, start_new_session=True, bufsize=0)
        self.processes.append(process)
        return process, path

    @staticmethod
    def signal_owned(process, sig):
        if process.poll() is None:
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                print(f"Owned process {process.pid} exited before signal delivery",
                      file=sys.stderr)

    def cleanup(self):
        for process in reversed(self.processes):
            self.signal_owned(process, signal.SIGTERM)
        deadline = time.monotonic() + 10
        for process in reversed(self.processes):
            try:
                process.wait(timeout=max(0.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                print(f"Force-cleaning owned process {process.pid}; logs: {self.run_dir}",
                      file=sys.stderr)
                self.signal_owned(process, signal.SIGKILL)
                process.wait(timeout=5)
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()
        for log in self.logs:
            log.close()

    def start_real(self):
        self.real, log = self.spawn(self.command, "real")
        deadline = time.monotonic() + 30
        last_error = None
        while time.monotonic() < deadline:
            self.assertIsNone(self.real.poll(), f"Real startup failed; log: {log}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    pass
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as ipc:
                    ipc.settimeout(0.2)
                    ipc.connect(self.ipc)
                return
            except OSError as error:
                last_error = error
                time.sleep(0.05)
        self.fail(f"Real did not become ready ({last_error}); log: {log}")

    def assert_buffer_io(self, response):
        self.assertEqual(response["writes"], [0, 0])
        self.assertEqual(response["reads"], response["sizes"])
        self.assertTrue(response["content_ok"])
        self.assertTrue(response["intact"])

    def assert_roundtrip(self, response):
        self.assert_buffer_io(response)
        self.assertEqual(response["single_write"], 0)
        for field in ("short_ok", "single_ok", "shape_ok"):
            self.assertTrue(response[field], field)

    def wait_roundtrip(self, worker, *, expect_local_pool=True):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            response = worker.call("roundtrip")
            if (response["writes"] == [0, 0] and
                    response["reads"] == response["sizes"] and
                    (not expect_local_pool or response["single_write"] == 0)):
                if expect_local_pool:
                    self.assert_roundtrip(response)
                else:
                    self.assert_buffer_io(response)
                    # Explicit SHM must not create the ordinary put/get pool.
                    self.assertLess(response["single_write"], 0)
                return
            time.sleep(0.1)
        self.fail(f"Dummy did not complete readiness/reconnect data checks; log: {worker.log}")

    def crash_and_reconnect(self, phase):
        self.start_real()
        self.start_workers()
        if phase == "after_reply":
            self.assert_roundtrip(self.workers[0].call("roundtrip"))
        elif phase == "pending_reply":
            self.workers[0].send("roundtrip", announce=True)
            self.assertTrue(self.workers[0].receive().get("calling"))
        self.signal_owned(self.real, signal.SIGKILL)
        self.real.wait(timeout=5)
        if phase == "pending_reply":
            # Completion or a per-item error is legal at this caller boundary.
            response = self.workers[0].receive()
            self.assertEqual(len(response["writes"]), 2)
            self.assertTrue(all(code <= 0 for code in response["writes"]))
            self.assertTrue(response["intact"])
            if response["reads"] == response["sizes"]:
                self.assertTrue(response["content_ok"])
        for worker in self.workers:
            self.assertIsNone(worker.process.poll())
            response = worker.call("down")
            self.assertEqual(len(response["writes"]), 2)
            self.assertTrue(all(code < 0 for code in response["writes"]))
            self.assertTrue(response["intact"])
        self.start_real()
        for worker in self.workers:
            self.assertIsNone(worker.process.poll())
            self.wait_roundtrip(worker)
            self.assertTrue(worker.call("memory")["intact"])

    def test_multi_dummy_real_crash_before_call(self):
        self.crash_and_reconnect("before_call")

    def test_multi_dummy_real_crash_before_reply_is_read(self):
        self.crash_and_reconnect("pending_reply")

    def test_multi_dummy_real_crash_after_reply(self):
        self.crash_and_reconnect("after_reply")

    def test_p2p_master_loss_preserves_dummy_local_io(self):
        from mooncake.store import MooncakeDistributedStore

        if not hasattr(MooncakeDistributedStore, "setup_p2p_real_client"):
            self.skipTest("P2P local-service behavior")
        if not hasattr(self, "master"):
            self.skipTest("Requires a test-owned master")
        # Keep the HTTP probe port distinct from the pending Dummy RPC listener.
        with socket.socket() as rpc_reservation, socket.socket() as reservation:
            rpc_reservation.bind(("127.0.0.1", self.port))
            reservation.bind(("127.0.0.1", 0))
            http_port = reservation.getsockname()[1]
        self.command += ["--enable_http_server=true", f"--http_port={http_port}"]
        self.start_real()
        self.start_workers(count=1)
        worker = self.workers[0]

        def health():
            with urlopen(f"http://127.0.0.1:{http_port}/health", timeout=1) as reply:
                return reply.read().decode().strip()

        self.assertEqual(health(), "ONLINE")
        self.signal_owned(self.master, signal.SIGKILL)
        self.master.wait(timeout=5)
        deadline = time.monotonic() + 30
        while health() != "DEGRADED":
            self.assertIsNone(self.real.poll())
            if time.monotonic() >= deadline:
                self.fail(f"P2P did not enter DEGRADED; logs: {self.run_dir}")
            time.sleep(0.1)
        # Exercise Dummy forwarding across several nominal heartbeat periods.
        # Native DEGRADED must still produce a HEALTH response for Dummy.
        deadline = time.monotonic() + 5
        while True:
            self.assertIsNone(self.real.poll())
            self.assert_buffer_io(worker.call("local_io"))
            if time.monotonic() >= deadline:
                break
            time.sleep(0.1)

    def start_zero_buffer_worker(self):
        namespace = f"zero_buffer_{uuid.uuid4().hex}"
        process, log = self.spawn(
            [sys.executable, "-u", str(Path(__file__).resolve()),
             "--zero-buffer-worker", f"127.0.0.1:{self.port}", namespace],
            "zero_buffer", pipes=True)
        worker = Worker(process, log)
        self.assertTrue(worker.receive().get("ready"))
        return worker

    def test_zero_buffer_close_without_mapping(self):
        self.start_real()
        worker = self.start_zero_buffer_worker()
        self.assertEqual(worker.call("probe")["exists"], 0)
        self.assertEqual(worker.call("close")["closed"], 0)
        self.assertEqual(worker.process.wait(timeout=10), 0)

    def test_zero_buffer_reconnect_then_explicit_io(self):
        self.start_real()
        worker = self.start_zero_buffer_worker()
        self.signal_owned(self.real, signal.SIGKILL)
        self.real.wait(timeout=5)
        self.assertLess(worker.call("probe")["exists"], 0)
        self.start_real()
        deadline = time.monotonic() + 30
        while worker.call("probe")["exists"] != 0:
            if time.monotonic() >= deadline:
                self.fail(f"Zero-buffer Dummy did not reconnect; log: {worker.log}")
            time.sleep(0.1)
        self.assertTrue(worker.call("allocate").get("ready"))
        # RPC reconnection can precede eviction of the killed Real's segments.
        self.wait_roundtrip(worker, expect_local_pool=False)
        self.assertEqual(worker.call("close")["closed"], 0)
        self.assertEqual(worker.process.wait(timeout=10), 0)

    def assert_signal_exit(self, order):
        self.start_real()
        self.start_workers()
        processes = [worker.process for worker in self.workers]
        if order == "real_first":
            processes.insert(0, self.real)
        else:
            processes.append(self.real)
        for process in processes:
            self.signal_owned(process, signal.SIGTERM)
        deadline = time.monotonic() + 20
        for process in processes:
            try:
                code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                self.fail(f"Signal shutdown timed out for {process.pid}; logs: {self.run_dir}")
            self.assertIn(code, (0, -signal.SIGTERM))
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as ipc:
            ipc.bind(self.ipc)
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", self.port))

    def test_signal_exit_dummy_signaled_first(self):
        self.assert_signal_exit("dummy_first")

    def test_signal_exit_real_signaled_first(self):
        self.assert_signal_exit("real_first")


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] in (
            "--dummy-worker", "--zero-buffer-worker"):
        try:
            size = 0 if sys.argv[1] == "--zero-buffer-worker" else 4 << 20
            dummy_worker(sys.argv[2], sys.argv[3], size)
        except Exception:
            traceback.print_exc()
            sys.exit(1)
    else:
        unittest.main(verbosity=2)
