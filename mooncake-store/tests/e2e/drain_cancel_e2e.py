#!/usr/bin/env python3
"""Exercise running DrainJob cancellation through the Python and C APIs.

Example, after building the Python module, master and shared C library:
    python3 drain_cancel_e2e.py /path/to/mooncake_master /path/to/libmooncake_store_c.so
"""

import ctypes
import json
import signal
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

MOONCAKE_STORE_DRAIN_RUNNING = 2
MOONCAKE_STORE_DRAIN_SUCCEEDED = 3
MOONCAKE_STORE_DRAIN_CANCELED = 5
MOONCAKE_STORE_DRAIN_CANCELING = 6
MOONCAKE_STORE_DRAIN_TIMED_OUT = 7


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("timed out waiting for drain cancellation")


def setup(master, metadata, segment_size):
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    assert (
        store.setup(address(), metadata, segment_size, 16 << 20, "tcp", "", master) == 0
    )
    return store


def worker(master, metadata):
    from mooncake.store import ReplicateConfig

    store = setup(master, metadata, 64 << 20)
    print(store.get_hostname(), flush=True)
    for key in sys.stdin:
        key = key.strip()
        config = ReplicateConfig()
        config.preferred_segments = [store.get_hostname()]
        print(store.put(key, b"data" * 8192, config), flush=True)
    store.close()


class Uuid(ctypes.Structure):
    _fields_ = [("high", ctypes.c_uint64), ("low", ctypes.c_uint64)]


class DrainRequest(ctypes.Structure):
    _fields_ = [
        ("segments", ctypes.POINTER(ctypes.c_char_p)),
        ("segment_count", ctypes.c_size_t),
        ("targets", ctypes.POINTER(ctypes.c_char_p)),
        ("target_count", ctypes.c_size_t),
        ("max_concurrency", ctypes.c_uint32),
    ]


class DrainInfo(ctypes.Structure):
    _fields_ = [
        ("id", Uuid),
        ("status", ctypes.c_int32),
        ("created", ctypes.c_int64),
        ("updated", ctypes.c_int64),
        ("segments", ctypes.POINTER(ctypes.c_char_p)),
        ("segment_count", ctypes.c_size_t),
        ("succeeded", ctypes.c_uint64),
        ("failed", ctypes.c_uint64),
        ("blocked", ctypes.c_uint64),
        ("active", ctypes.c_uint64),
        ("migrated_bytes", ctypes.c_uint64),
        ("message", ctypes.c_char_p),
    ]


class DrainFilter(ctypes.Structure):
    _fields_ = [
        ("has_status", ctypes.c_int32),
        ("status", ctypes.c_int32),
        ("segment", ctypes.c_char_p),
        ("page_token", ctypes.c_char_p),
        ("limit", ctypes.c_uint32),
    ]


class DrainPage(ctypes.Structure):
    _fields_ = [
        ("jobs", ctypes.POINTER(DrainInfo)),
        ("count", ctypes.c_size_t),
        ("next_page_token", ctypes.c_char_p),
    ]


class CStore:
    def __init__(self, library, master, metadata):
        self.lib = ctypes.CDLL(library)
        self.libc = ctypes.CDLL(None)
        self.libc.free.argtypes = [ctypes.c_void_p]
        self.lib.mooncake_store_create.restype = ctypes.c_void_p
        self.lib.mooncake_store_destroy.argtypes = [ctypes.c_void_p]
        self.lib.mooncake_store_setup.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self.lib.mooncake_store_create_drain_job.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(DrainRequest),
            ctypes.POINTER(Uuid),
        ]
        self.lib.mooncake_store_create_timed_drain_job.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(DrainRequest),
            ctypes.c_uint64,
            ctypes.POINTER(Uuid),
        ]
        self.lib.mooncake_store_query_drain_job.argtypes = [
            ctypes.c_void_p,
            Uuid,
            ctypes.POINTER(ctypes.POINTER(DrainInfo)),
        ]
        self.lib.mooncake_store_cancel_drain_job.argtypes = [ctypes.c_void_p, Uuid]
        self.lib.mooncake_store_list_drain_jobs.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(DrainFilter),
            ctypes.POINTER(ctypes.POINTER(DrainPage)),
        ]
        self.store = self.lib.mooncake_store_create()
        assert self.store
        assert (
            self.lib.mooncake_store_setup(
                self.store,
                address().encode(),
                metadata.encode(),
                0,
                16 << 20,
                b"tcp",
                b"",
                master.encode(),
            )
            == 0
        )

    def close(self):
        self.lib.mooncake_store_destroy(self.store)

    def create(self, source, target, timeout_ms=None):
        sources = (ctypes.c_char_p * 1)(source.encode())
        targets = (ctypes.c_char_p * 1)(target.encode())
        request = DrainRequest(sources, 1, targets, 1, 1)
        job_id = Uuid()
        if timeout_ms is not None:
            result = self.lib.mooncake_store_create_timed_drain_job(
                self.store, ctypes.byref(request), timeout_ms, ctypes.byref(job_id)
            )
        else:
            result = self.lib.mooncake_store_create_drain_job(
                self.store, ctypes.byref(request), ctypes.byref(job_id)
            )
        assert result == 0
        return job_id

    def status(self, job_id):
        info = ctypes.POINTER(DrainInfo)()
        assert (
            self.lib.mooncake_store_query_drain_job(
                self.store, job_id, ctypes.byref(info)
            )
            == 0
        )
        try:
            return info.contents.status
        finally:
            self.libc.free(info)

    def assert_invalid_timeout(self, source, target):
        sources = (ctypes.c_char_p * 1)(source.encode())
        targets = (ctypes.c_char_p * 1)(target.encode())
        request = DrainRequest(sources, 1, targets, 1, 1)
        job_id = Uuid(1, 1)
        assert (
            self.lib.mooncake_store_create_timed_drain_job(
                self.store, ctypes.byref(request), (1 << 64) - 1, ctypes.byref(job_id)
            )
            == -600
        )
        assert job_id.high == 0 and job_id.low == 0

    def cancel(self, job_id):
        assert self.lib.mooncake_store_cancel_drain_job(self.store, job_id) == 0

    def assert_status_is_listed(self, job_id, source, status):
        filt = DrainFilter(1, status, source.encode(), None, 100)
        page = ctypes.POINTER(DrainPage)()
        assert (
            self.lib.mooncake_store_list_drain_jobs(
                self.store, ctypes.byref(filt), ctypes.byref(page)
            )
            == 0
        )
        try:
            assert any(
                entry.id.high == job_id.high and entry.id.low == job_id.low
                for entry in page.contents.jobs[: page.contents.count]
            )
        finally:
            self.libc.free(page)


def c_worker(library, master, metadata):
    # The C shared library and Python extension both link gflags; load them in
    # separate processes to avoid duplicate global flag registration.
    store = CStore(library, master, metadata)
    print("ready", flush=True)
    try:
        for line in sys.stdin:
            command = json.loads(line)
            operation = command[0]
            if operation == "create":
                job_id = store.create(command[1], command[2])
                result = [job_id.high, job_id.low]
            elif operation == "create_timed":
                job_id = store.create(command[1], command[2], command[3])
                result = [job_id.high, job_id.low]
            elif operation == "invalid_timed":
                store.assert_invalid_timeout(command[1], command[2])
                result = True
            elif operation == "status":
                result = store.status(Uuid(*command[1]))
            elif operation == "cancel":
                store.cancel(Uuid(*command[1]))
                result = True
            elif operation == "list":
                store.assert_status_is_listed(Uuid(*command[1]), command[2], command[3])
                result = True
            else:
                raise ValueError(operation)
            print(json.dumps(result), flush=True)
    finally:
        store.close()


class CStoreProxy:
    def __init__(self, process):
        self.process = process
        assert process.stdout.readline().strip() == "ready"

    def call(self, *command):
        self.process.stdin.write(json.dumps(command) + "\n")
        self.process.stdin.flush()
        response = self.process.stdout.readline()
        assert response, f"C API worker exited during {command[0]}"
        return json.loads(response)


def main(master_binary, c_library):
    from mooncake.store import JobStatus, TaskStatus

    master, metadata, admin = address(), address(), address()
    metadata_url = f"http://{metadata}/metadata"
    with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:

        def start(args, name):
            log = Path(directory) / name
            output = stack.enter_context(log.open("w"))
            proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=output,
                text=True,
            )

            def cleanup():
                if proc.poll() is None:
                    proc.send_signal(signal.SIGCONT)
                    proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                proc.stdin.close()
                proc.stdout.close()

            stack.callback(cleanup)
            return proc, log

        master_process, master_log = start(
            [
                master_binary,
                f"--rpc_port={master.split(':')[-1]}",
                "--rpc_address=127.0.0.1",
                "--rpc_thread_num=2",
                "--enable_metric_reporting=false",
                f"--metrics_port={admin.split(':')[-1]}",
                "--metrics_host=127.0.0.1",
                "--enable_http_metadata_server=true",
                f"--http_metadata_server_port={metadata.split(':')[-1]}",
                "--http_metadata_server_host=127.0.0.1",
                "--default_kv_lease_ttl=500ms",
            ],
            "master.log",
        )

        def listening(endpoint):
            try:
                with socket.create_connection(
                    ("127.0.0.1", int(endpoint.split(":")[-1])), 0.1
                ):
                    return True
            except OSError:
                return False

        def master_ready():
            if master_process.poll() is not None:
                raise RuntimeError(f"Master exited: {master_log.read_text()}")
            return listening(master) and listening(metadata) and listening(admin)

        wait_until(master_ready)
        source, source_log = start(
            [sys.executable, __file__, "worker", master, metadata_url], "source.log"
        )
        source_name = source.stdout.readline().strip()
        target, _ = start(
            [sys.executable, __file__, "worker", master, metadata_url], "target.log"
        )
        target_name = target.stdout.readline().strip()
        assert source_name.startswith("127.0.0.1:") and target_name.startswith(
            "127.0.0.1:"
        )
        store = setup(master, metadata_url, 0)
        stack.callback(store.close)
        c_process, _ = start(
            [sys.executable, __file__, "c-worker", c_library, master, metadata_url],
            "c-worker.log",
        )
        c_store = CStoreProxy(c_process)

        def put(key):
            source.stdin.write(key + "\n")
            source.stdin.flush()
            assert source.stdout.readline().strip() == "0"
            time.sleep(0.6)  # let the write lease expire

        def assert_value_and_remove(key):
            assert store.get(key) == b"data" * 8192
            replicas = store.get_replica_desc(key)
            assert len(replicas) == 1
            assert (
                replicas[0].get_memory_descriptor().buffer_descriptor.transport_endpoint
                == source_name
            )
            assert store.remove(key, True) == 0

        put("drain-pending")
        source.send_signal(signal.SIGSTOP)
        job_id, error = store.create_drain_job([source_name], [target_name], 1)
        assert error == 0
        wait_until(lambda: store.query_drain_job(job_id)[0].active_units == 1)
        assert store.cancel_drain_job(job_id) == 0
        assert store.query_drain_job(job_id)[0].status == JobStatus.CANCELED
        source.send_signal(signal.SIGCONT)
        assert_value_and_remove("drain-pending")

        for use_c_api, key in [(False, "drain-python"), (True, "drain-c")]:
            put(key)
            target.send_signal(signal.SIGSTOP)
            if use_c_api:
                job_id = c_store.call("create", source_name, target_name)
            else:
                job_id, error = store.create_drain_job([source_name], [target_name], 1)
                assert error == 0

            def processing():
                page, error = store.list_tasks(key=key, status=TaskStatus.PROCESSING)
                assert error == 0
                return page.tasks[0] if page.tasks else None

            task = wait_until(processing)
            wait_until(
                lambda: f"action=replica_move_start, key={key}"
                in source_log.read_text()
            )
            if use_c_api:
                assert c_store.call("cancel", job_id)
                assert c_store.call("status", job_id) == MOONCAKE_STORE_DRAIN_CANCELING
                assert c_store.call(
                    "list", job_id, source_name, MOONCAKE_STORE_DRAIN_CANCELING
                )
            else:
                assert store.cancel_drain_job(job_id) == 0
                assert store.query_drain_job(job_id)[0].status == JobStatus.CANCELING
                page, error = store.list_drain_jobs(
                    status=JobStatus.CANCELING, segment=source_name
                )
                assert error == 0 and any(entry.id == job_id for entry in page.jobs)
            target.send_signal(signal.SIGCONT)
            if use_c_api:
                wait_until(
                    lambda: c_store.call("status", job_id)
                    == MOONCAKE_STORE_DRAIN_CANCELED
                )
            else:
                wait_until(
                    lambda: store.query_drain_job(job_id)[0].status
                    == JobStatus.CANCELED
                )
            assert store.query_task(task.id)[0].status == TaskStatus.CANCELED
            assert_value_and_remove(key)

        assert (
            store.create_drain_job([source_name], [target_name], timeout_ms=-1)[1]
            == -600
        )
        assert (
            store.create_drain_job(
                [source_name], [target_name], timeout_ms=(1 << 63) - 1
            )[1]
            == -600
        )
        assert c_store.call("invalid_timed", source_name, target_name)
        put("timeout-zero")
        source.send_signal(signal.SIGSTOP)
        no_timeout = c_store.call("create_timed", source_name, target_name, 0)
        wait_until(
            lambda: c_store.call("status", no_timeout) == MOONCAKE_STORE_DRAIN_RUNNING
        )
        time.sleep(1.1)
        assert c_store.call("status", no_timeout) == MOONCAKE_STORE_DRAIN_RUNNING
        assert c_store.call("cancel", no_timeout)
        assert c_store.call("status", no_timeout) == MOONCAKE_STORE_DRAIN_CANCELED
        source.send_signal(signal.SIGCONT)
        assert_value_and_remove("timeout-zero")
        put("timeout-python")
        source.send_signal(signal.SIGSTOP)
        job_id, error = store.create_drain_job(
            [source_name], [target_name], 1, timeout_ms=1000
        )
        assert error == 0
        wait_until(
            lambda: store.query_drain_job(job_id)[0].status == JobStatus.TIMED_OUT
        )
        page, error = store.list_drain_jobs(
            status=JobStatus.TIMED_OUT, segment=source_name
        )
        assert error == 0 and any(entry.id == job_id for entry in page.jobs)
        # A replacement job can claim the still-draining segment. Canceling
        # that new job restores the segment for the next scenario.
        retry_id, error = store.create_drain_job([source_name], [target_name], 1)
        assert error == 0 and retry_id != job_id
        wait_until(lambda: store.query_drain_job(retry_id)[0].active_units == 1)
        assert store.cancel_drain_job(retry_id) == 0
        assert store.query_drain_job(retry_id)[0].status == JobStatus.CANCELED
        source.send_signal(signal.SIGCONT)
        assert_value_and_remove("timeout-python")

        put("timeout-c")
        target.send_signal(signal.SIGSTOP)
        c_job = c_store.call("create_timed", source_name, target_name, 5000)

        def processing_timeout_task():
            page, error = store.list_tasks(
                key="timeout-c", status=TaskStatus.PROCESSING
            )
            assert error == 0
            return page.tasks[0] if page.tasks else None

        task = wait_until(processing_timeout_task)
        wait_until(
            lambda: "action=replica_move_start, key=timeout-c" in source_log.read_text()
        )
        wait_until(
            lambda: c_store.call("status", c_job) == MOONCAKE_STORE_DRAIN_CANCELING
        )
        assert store.create_drain_job([source_name], [target_name], 1)[1] == -1010
        target.send_signal(signal.SIGCONT)
        wait_until(
            lambda: c_store.call("status", c_job) == MOONCAKE_STORE_DRAIN_TIMED_OUT
        )
        assert c_store.call("list", c_job, source_name, MOONCAKE_STORE_DRAIN_TIMED_OUT)
        assert store.query_task(task.id)[0].status == TaskStatus.CANCELED
        assert store.get("timeout-c") == b"data" * 8192
        replicas = store.get_replica_desc("timeout-c")
        assert len(replicas) == 1
        assert (
            replicas[0].get_memory_descriptor().buffer_descriptor.transport_endpoint
            == source_name
        )

        retry_c = c_store.call("create", source_name, target_name)
        assert retry_c != c_job
        wait_until(
            lambda: c_store.call("status", retry_c) == MOONCAKE_STORE_DRAIN_SUCCEEDED
        )
        assert store.get("timeout-c") == b"data" * 8192
        replicas = store.get_replica_desc("timeout-c")
        assert len(replicas) == 1
        assert (
            replicas[0].get_memory_descriptor().buffer_descriptor.transport_endpoint
            == target_name
        )
        assert store.remove("timeout-c", True) == 0
        print("Python and C DrainJob cancellation and timeout passed")


if __name__ == "__main__":
    if sys.argv[1] == "worker":
        worker(*sys.argv[2:])
    elif sys.argv[1] == "c-worker":
        c_worker(*sys.argv[2:])
    else:
        main(*sys.argv[1:])
