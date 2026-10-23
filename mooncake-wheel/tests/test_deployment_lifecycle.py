"""Test deployment lifecycle in each architecture's matching wheel environment.

MOONCAKE_REAL_CLIENT_COMMAND configures the daemon; MOONCAKE_MASTER_COMMAND
starts an isolated master per case. MOONCAKE_DEPLOYMENT_PROTOCOL selects the
daemon transport; Dummy SHM checks do not exercise cross-endpoint RDMA.
"""

import array
import ctypes
import json
import os
from pathlib import Path
import select
import shlex
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
import uuid


def emit(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def dummy_worker(address, namespace):
    # Keep native stdout logs off the JSON reply channel.
    sys.stdout.flush()
    reply_fd = os.dup(sys.stdout.fileno())
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = os.fdopen(reply_fd, "w", buffering=1)
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    if store.setup_dummy(4 << 20, 4 << 20, address) != 0:
        raise RuntimeError("Dummy setup failed")
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
        if action not in ("memory", "close", "roundtrip", "down"):
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
            if action == "roundtrip" and writes == [0, 0]:
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
        self.start_real()
        self.workers = []
        for index in range(2):
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

    def assert_roundtrip(self, response):
        self.assertEqual(response["writes"], [0, 0])
        self.assertEqual(response["reads"], response["sizes"])
        self.assertEqual(response["single_write"], 0)
        for field in ("content_ok", "short_ok", "single_ok", "shape_ok", "intact"):
            self.assertTrue(response[field], field)

    def wait_roundtrip(self, worker):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            response = worker.call("roundtrip")
            if (response["writes"] == [0, 0] and
                    response["reads"] == response["sizes"] and
                    response["single_write"] == 0):
                self.assert_roundtrip(response)
                return
            time.sleep(0.1)
        self.fail(f"Dummy did not complete readiness/reconnect data checks; log: {worker.log}")

    def crash_and_reconnect(self, phase):
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

    def assert_signal_exit(self, order):
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

    def test_rpc_collision_does_not_damage_running_deployment(self):
        duplicate, log = self.spawn(self.command, "collision")
        try:
            code = duplicate.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.fail(f"Occupied RPC port did not fail startup; log: {log}")
        self.assertNotEqual(code, 0)
        self.assertNotIn(code, (-signal.SIGSEGV, -signal.SIGBUS, -signal.SIGILL,
                               -signal.SIGKILL))
        self.assertIsNone(self.real.poll())
        for worker in self.workers:
            self.assert_roundtrip(worker.call("roundtrip"))

    def test_ipc_collision_rejects_dummy_and_preserves_other_deployment(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        ipc_name = f"\0@mooncake_client_{port}.sock"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as occupied:
            occupied.bind(ipc_name)
            occupied.listen(5)
            occupied.settimeout(15)
            duplicate, log = self.spawn(self.command + [f"--port={port}"], "ipc_collision")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if duplicate.poll() is not None:
                    self.assertNotEqual(duplicate.returncode, 0)
                    self.assertIsNone(self.real.poll())
                    for worker in self.workers:
                        self.assert_roundtrip(worker.call("roundtrip"))
                    return  # Synchronous startup rejection is also valid.
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                self.fail(f"Occupied IPC startup did not fail or expose RPC; log: {log}")
            probe, probe_log = self.spawn(
                [sys.executable, "-u", str(Path(__file__).resolve()), "--dummy-worker",
                 f"127.0.0.1:{port}", f"rejected_{uuid.uuid4().hex}"],
                "rejected_dummy", pipes=True)
            # The existing owner rejects the normal header+FD registration.
            with occupied.accept()[0] as connection:
                connection.settimeout(5)
                payload, ancillary, flags, _ = connection.recvmsg(128, socket.CMSG_SPACE(4))
                received_fds = []
                for level, kind, data in ancillary:
                    if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                        descriptors = array.array("i")
                        descriptors.frombytes(data[:len(data) - len(data) % descriptors.itemsize])
                        for descriptor in descriptors:
                            received_fds.append(descriptor)
                            os.close(descriptor)
                self.assertTrue(payload)
                self.assertEqual(len(received_fds), 1)
                self.assertFalse(flags & socket.MSG_CTRUNC)
                connection.sendall(struct.pack("i", -1))
            try:
                code = probe.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.fail(f"Dummy did not reject occupied IPC; log: {probe_log}")
            self.assertEqual(code, 1)
            self.assertIn("RuntimeError: Dummy setup failed",
                          probe_log.read_text(errors="replace"))
            self.assertIsNone(self.real.poll())
            for worker in self.workers:
                self.assert_roundtrip(worker.call("roundtrip"))


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--dummy-worker":
        try:
            dummy_worker(sys.argv[2], sys.argv[3])
        except Exception:
            traceback.print_exc()
            sys.exit(1)
    else:
        unittest.main(verbosity=2)
