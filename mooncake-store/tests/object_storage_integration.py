"""Isolated OSS HTTP + etcd + real Master/Python client integration test.

Usage: python object_storage_integration.py --build DIR --etcd /path/to/etcd
The HTTP fixture implements the OSS operations used here; it does not test OSS
credentials/signing or the cloud service itself. All processes and data are
owned by this test, with ephemeral ports and no bucket listing.
"""

import argparse
import concurrent.futures
import ctypes
import http.server
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out: {description}")


def listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


class Objects(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    objects = {}
    lock = threading.Lock()
    failures = 0
    corrupt_reads = False
    requests = []

    def log_message(self, *args):
        pass

    def reply(self, status, data=b"", headers=None):
        self.send_response(status)
        self.send_header("Content-Length", str(len(data)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def do_PUT(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        with self.lock:
            self.requests.append(("PUT", self.path))
            if type(self).failures:
                type(self).failures -= 1
                fail = True
            else:
                self.objects[self.path] = data
                fail = False
        self.reply(503 if fail else 200)

    def do_GET(self):
        with self.lock:
            self.requests.append(("GET", self.path))
            data = self.objects.get(self.path)
        if data is None:
            self.reply(404)
            return
        if type(self).corrupt_reads and data:
            data = bytes([data[0] ^ 1]) + data[1:]
        selected = self.headers.get("Range")
        if selected:
            start, end = map(int, selected.removeprefix("bytes=").split("-"))
            if start > end or end >= len(data):
                self.reply(416)
                return
            self.reply(
                206,
                data[start : end + 1],
                {"Content-Range": f"bytes {start}-{end}/{len(data)}"},
            )
        else:
            self.reply(200, data)

    def do_HEAD(self):
        with self.lock:
            data = self.objects.get(self.path)
        self.reply(404 if data is None else 200, data or b"")

    def do_DELETE(self):
        with self.lock:
            self.requests.append(("DELETE", self.path))
            self.objects.pop(self.path, None)
        self.reply(204)


def run(args):
    sys.path.insert(0, str(args.build / "mooncake-integration"))
    os.environ["MOONCAKE_STORE_CHECKSUM"] = "1"
    import store

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Objects)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    children = []
    clients = []
    logs = []
    gpu_allocations = []
    cuda = None
    if args.gpu:
        cuda = ctypes.CDLL("libcudart.so")
        cuda.cudaMalloc.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t]
        cuda.cudaMemcpy.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]
        cuda.cudaFree.argtypes = [ctypes.c_void_p]
        assert cuda.cudaSetDevice(0) == 0
    with tempfile.TemporaryDirectory(prefix="mooncake-object-it-") as temporary:
        root = Path(temporary)
        try:
            etcd_port, peer_port = free_port(), free_port()
            log = open(root / "etcd.log", "wb")
            logs.append(log)
            etcd = subprocess.Popen(
                [
                    str(args.etcd),
                    "--name=object-it",
                    f"--data-dir={root / 'etcd'}",
                    f"--listen-client-urls=http://127.0.0.1:{etcd_port}",
                    f"--advertise-client-urls=http://127.0.0.1:{etcd_port}",
                    f"--listen-peer-urls=http://127.0.0.1:{peer_port}",
                    f"--initial-advertise-peer-urls=http://127.0.0.1:{peer_port}",
                    f"--initial-cluster=object-it=http://127.0.0.1:{peer_port}",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            children.append(etcd)
            wait_for(lambda: listening(etcd_port), "isolated etcd")

            def start_master(
                mode,
                cluster,
                port,
                *,
                ha=False,
                wait_ready=True,
                extra_args=(),
                env=None,
            ):
                log = open(root / f"master-{mode}-{len(logs)}.log", "wb")
                logs.append(log)
                process = subprocess.Popen(
                    [
                        str(args.build / "mooncake-store/src/mooncake_master"),
                        f"--port={port}",
                        "--rpc_address=127.0.0.1",
                        "--rpc_thread_num=4",
                        *(["--enable_ha=true", "--enable_oplog=true"] if ha else []),
                        "--enable_metric_reporting=false",
                        f"--metrics_port={free_port()}",
                        "--root_fs_dir=",
                        f"--cluster_id={cluster}",
                        f"--ha_backend_connstring=http://127.0.0.1:{etcd_port}",
                        "--object_storage_enabled=true",
                        "--object_storage_anonymous=true",
                        "--object_storage_path_style=true",
                        "--object_storage_bucket=test",
                        "--object_storage_region=test",
                        f"--object_storage_prefix={cluster}/",
                        f"--object_storage_endpoint=http://127.0.0.1:{server.server_port}",
                        f"--object_storage_write_mode={mode}",
                        "--object_storage_request_timeout_ms=3000",
                        "--default_kv_lease_ttl=5000ms",
                        *extra_args,
                    ],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                children.append(process)
                if wait_ready:
                    wait_for(
                        lambda: listening(port) or process.poll() is not None,
                        "master startup",
                    )
                assert process.poll() is None, f"Master startup failed: {log.name}"
                return process

            def client(port, memory=0):
                result = store.MooncakeDistributedStore()
                status = result.setup(
                    f"127.0.0.1:{free_port()}",
                    "P2PHANDSHAKE",
                    memory,
                    16 << 20,
                    "tcp",
                    "",
                    port if isinstance(port, str) else f"127.0.0.1:{port}",
                )
                assert status == 0, status
                clients.append(result)
                return result

            config = store.ReplicateConfig()
            config.os_replica_num = 1
            if args.completion_only:
                cluster = f"object-completion-it-{uuid.uuid4().hex}"
                os.environ["MC_STORE_CLUSTER_ID"] = cluster
                failpoints = root / "completion-failpoints"
                failpoints.mkdir()
                master_port = free_port()
                master = start_master(
                    "sync",
                    cluster,
                    master_port,
                    extra_args=("--object_storage_max_pending_objects=1",),
                    env=dict(os.environ, MOONCAKE_TEST_FAILPOINT_DIR=str(failpoints)),
                )
                # Only the provider owns a segment. The writer has neither
                # mounted memory nor a local-disk / DFS backend.
                provider = client(master_port, 32 << 20)
                writer = client(master_port)
                value = b"completion retry from a storage-less writer" * 4096
                name = "object_storage_finish_rpc_failure"
                (failpoints / f"{name}.arm").touch()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    put = executor.submit(writer.put, "failed", value, config)
                    try:
                        wait_for(
                            lambda: (failpoints / f"{name}.hit").exists() or put.done(),
                            "Finish RPC failpoint (requires MOONCAKE_ENABLE_TEST_FAILPOINTS)",
                        )
                        assert (
                            failpoints / f"{name}.hit"
                        ).exists(), "Finish RPC failpoint was not enabled or reached"
                        with Objects.lock:
                            uploads = [
                                path
                                for method, path in Objects.requests
                                if method == "PUT" and f"/{cluster}/" in path
                            ]
                        assert len(uploads) == 1, uploads
                    finally:
                        (failpoints / f"{name}.release").touch()
                    assert put.result(timeout=10) != 0
                assert writer.get("failed") in (None, b"")
                # Known completion must be retried, allowing GC to release the
                # sole pending slot. An UNKNOWN tombstone alone cannot free it.
                wait_for(
                    lambda: writer.put("after-retry", value, config) == 0,
                    "completion retry releases pending admission",
                    timeout=45,
                )
                assert writer.get("after-retry") == value
                with Objects.lock:
                    assert (
                        sum(
                            method == "PUT" and path == uploads[0]
                            for method, path in Objects.requests
                        )
                        == 1
                    )
                assert writer.remove("after-retry", True) == 0
                writer.close()
                provider.close()
                clients.remove(writer)
                clients.remove(provider)
                master.terminate()
                master.wait(timeout=10)
                print(
                    "PASS completion retry: storage-less writer, failed Finish RPC, single PUT, pending slot reclaimed",
                    flush=True,
                )
                return
            for mode in (
                () if args.graceful_only else ("sync", "async", "offload_on_evict")
            ):
                cluster, port = f"object-it-{uuid.uuid4().hex}", free_port()
                master = start_master(mode, cluster, port)
                owner = client(port, 32 << 20)
                writer = client(port)
                reader = client(port)
                value = bytes(range(256)) * 4096 + b"tail"
                if mode == "sync":
                    Objects.failures = 1
                    assert writer.put("failed", value, config) != 0
                    assert writer.get("failed") in (None, b"")
                    assert writer.put("failed", value, config) == 0
                    assert writer.remove("failed", True) == 0
                if mode == "sync":
                    batch_values = [b"first-batch-value", b"second-batch-value"]
                    buffers = [ctypes.create_string_buffer(v) for v in batch_values]
                    addresses = [ctypes.addressof(b) for b in buffers]
                    sizes = [len(v) for v in batch_values]
                    for address, size in zip(addresses, sizes):
                        assert writer.register_buffer(address, size) == 0
                    try:
                        Objects.failures = 1
                        results = writer.batch_put_from(
                            ["batch-a", "batch-b"], addresses, sizes, config
                        )
                        assert (
                            len(results) == 2 and results[0] != 0 and results[1] == 0
                        ), results
                        assert writer.get("batch-a") in (None, b"")
                        assert writer.batch_upsert_from(
                            ["batch-a", "batch-b"], addresses, sizes, config
                        ) == [0, 0]
                    finally:
                        for address in addresses:
                            assert writer.unregister_buffer(address) == 0
                if args.gpu and mode == "sync":
                    gpu_value = bytes(range(256)) * 1024
                    host_source = ctypes.create_string_buffer(gpu_value)
                    gpu_source, gpu_destination = ctypes.c_void_p(), ctypes.c_void_p()
                    for pointer in (gpu_source, gpu_destination):
                        assert (
                            cuda.cudaMalloc(ctypes.byref(pointer), len(gpu_value)) == 0
                        )
                        gpu_allocations.append(pointer)
                    assert (
                        cuda.cudaMemcpy(
                            gpu_source, ctypes.addressof(host_source), len(gpu_value), 1
                        )
                        == 0
                    )
                    assert writer.register_buffer(gpu_source.value, len(gpu_value)) == 0
                    try:
                        assert (
                            writer.put_from(
                                "gpu-key", gpu_source.value, len(gpu_value), config
                            )
                            == 0
                        )
                        assert writer.batch_upsert_from(
                            ["gpu-key"], [gpu_source.value], [len(gpu_value)], config
                        ) == [0]
                    finally:
                        assert writer.unregister_buffer(gpu_source.value) == 0
                if mode == "async":
                    Objects.failures = 1
                with Objects.lock:
                    first_request = len(Objects.requests)
                assert writer.put("key", value, config) == 0

                def pressure():
                    if mode == "offload_on_evict":
                        memory_only = store.ReplicateConfig()
                        for index in range(48):
                            writer.put(
                                f"pressure-{uuid.uuid4().hex}-{index}",
                                value,
                                memory_only,
                            )

                pressure()

                def committed():
                    return any(
                        r.is_object_storage_replica()
                        and r.status == store.ReplicaStatus.COMPLETE
                        for r in writer.get_replica_desc("key")
                    )

                wait_for(committed, f"{mode} object commit")
                if mode == "offload_on_evict":
                    # Pressure has proved that upload precedes eviction. Free
                    # the fixture's filler keys before the independent Upsert
                    # check, which requires a new MEMORY allocation.
                    assert writer.remove_by_regex("^pressure-.*$", True) >= 0
                if mode == "async":
                    with Objects.lock:
                        puts = [
                            path
                            for method, path in Objects.requests[first_request:]
                            if method == "PUT"
                        ]
                    assert len(puts) >= 2 and len(puts) == len(set(puts)), puts
                with Objects.lock:
                    prior_names = {
                        name for name, data in Objects.objects.items() if data == value
                    }
                assert writer.upsert("key", value[::-1], config) == 0
                with Objects.lock:
                    assert all(
                        Objects.objects.get(name) == value for name in prior_names
                    )
                pressure()
                wait_for(committed, f"{mode} upsert commit")
                value = value[::-1]
                # Closing the only memory owner must leave direct object reads usable.
                owner.close()
                clients.remove(owner)
                assert reader.get("key") == value
                Objects.corrupt_reads = True
                try:
                    assert reader.get("key") in (None, b"")
                finally:
                    Objects.corrupt_reads = False
                assert reader.get("key") == value
                if mode == "sync":
                    assert [
                        bytes(v)
                        for v in reader.batch_get_buffer(["batch-a", "batch-b"])
                    ] == batch_values
                if args.gpu and mode == "sync":
                    assert reader.get_into(
                        "gpu-key", gpu_destination.value, len(gpu_value)
                    ) == len(gpu_value)
                    host_result = ctypes.create_string_buffer(len(gpu_value))
                    assert (
                        cuda.cudaMemcpy(
                            ctypes.addressof(host_result),
                            gpu_destination,
                            len(gpu_value),
                            2,
                        )
                        == 0
                    )
                    assert host_result.raw == gpu_value
                    assert reader.batch_get_session_start(["gpu-key"]) == [0]
                    assert reader.batch_get_into_multi_buffer_ranges(
                        ["gpu-key"], [[gpu_destination.value]], [[257]], [[17]]
                    ) == [257]
                    reader.batch_get_session_end(["gpu-key"])
                    assert (
                        cuda.cudaMemcpy(
                            ctypes.addressof(host_result), gpu_destination, 257, 2
                        )
                        == 0
                    )
                    assert host_result.raw[:257] == gpu_value[17:274]
                    print(
                        "PASS GPU: device-source Put/batch Upsert, owner-independent device Get and session range",
                        flush=True,
                    )
                destination = ctypes.create_string_buffer(len(value) + 16)
                assert reader.get_into(
                    "key", ctypes.addressof(destination), len(destination)
                ) == len(value)
                assert destination.raw[: len(value)] == value
                assert reader.batch_get_session_start(["key"]) == [0]
                ranges = [
                    ctypes.create_string_buffer(19),
                    ctypes.create_string_buffer(7),
                ]
                counts = reader.batch_get_into_multi_buffer_ranges(
                    ["key"],
                    [[ctypes.addressof(b) for b in ranges]],
                    [[19, 7]],
                    [[13, len(value) - 7]],
                )
                assert counts == [26], counts
                assert ranges[0].raw == value[13:32]
                assert ranges[1].raw == value[-7:]
                reader.batch_get_session_end(["key"])
                assert bytes(reader.batch_get_buffer(["key"])[0]) == value
                # An abrupt Master death must recover committed objects from etcd.
                master.kill()
                master.wait(timeout=10)
                writer.close()
                reader.close()
                clients.remove(writer)
                clients.remove(reader)
                port = free_port()
                master = start_master(mode, cluster, port)
                recovered = client(port)
                assert recovered.get("key") == value
                assert recovered.remove("key", True) == 0
                assert recovered.get("key") in (None, b"")
                recovered.close()
                clients.remove(recovered)
                master.kill()
                master.wait(timeout=10)
                port = free_port()
                master = start_master(mode, cluster, port)
                after_delete = client(port)
                assert after_delete.get("key") in (None, b"")
                after_delete.close()
                clients.remove(after_delete)
                master.terminate()
                master.wait(timeout=10)
                print(
                    f"PASS {mode}: distinct owner, direct read, upsert, buffer/batch/session ranges, crash recovery, remove",
                    flush=True,
                )
            cluster, port = f"object-grace-it-{uuid.uuid4().hex}", free_port()
            master = start_master("offload_on_evict", cluster, port)
            owner, writer, reader = client(port), client(port), client(port)
            mounted = owner.allocate_and_mount_segment(8 << 20, "tcp", "cpu:0")
            assert mounted["ret"] == 0 and mounted["segment_ids"], mounted
            values = {
                f"grace-{i}": (f"graceful-source-{i}".encode() * 65536)
                for i in range(3)
            }
            # Queue durable policies before unmount. With no pressure these
            # remain DEFERRED; no poll/upload/unmount timing race is needed.
            for key, value in values.items():
                assert writer.put(key, value, config) == 0
            with Objects.lock:
                assert not any(
                    value in Objects.objects.values() for value in values.values()
                )
            assert owner.unmount_and_free_segment(mounted["segment_ids"], 20) == 0
            filler_owner = client(port, 32 << 20)
            filler = b"grace-pressure" * 80000
            for index in range(48):
                writer.put(f"grace-pressure-{index}", filler, store.ReplicateConfig())

            def all_grace_uploads_complete():
                return all(
                    any(
                        replica.is_object_storage_replica()
                        and replica.status == store.ReplicaStatus.COMPLETE
                        for replica in writer.get_replica_desc(key)
                    )
                    for key in values
                )

            wait_for(
                all_grace_uploads_complete,
                "uploads from gracefully unmounting source",
                timeout=20,
            )
            # Graceful unmount keeps MEMORY descriptors readable until its
            # deadline. Wait for Master to retire them before closing their
            # owner, then prove these reads really use the shared object.
            wait_for(
                lambda: all(
                    not any(
                        replica.is_memory_replica()
                        for replica in writer.get_replica_desc(key)
                    )
                    for key in values
                ),
                "graceful MEMORY source retired",
                timeout=35,
            )
            owner.close()
            clients.remove(owner)
            filler_owner.close()
            clients.remove(filler_owner)
            for key, value in values.items():
                assert reader.get(key) == value
            writer.close()
            reader.close()
            clients.remove(writer)
            clients.remove(reader)
            master.terminate()
            master.wait(timeout=10)
            print(
                "PASS graceful unmount: queued policies upload during grace, queue progresses, owner-independent bytes",
                flush=True,
            )
            if args.graceful_only:
                return
            cluster = f"object-ha-it-{uuid.uuid4().hex}"
            primary_port, standby_port = free_port(), free_port()
            os.environ["MC_STORE_CLUSTER_ID"] = cluster
            address = f"etcd://127.0.0.1:{etcd_port}"
            primary = start_master("sync", cluster, primary_port, ha=True)
            standby = start_master(
                "sync", cluster, standby_port, ha=True, wait_ready=False
            )
            owner = client(address, 32 << 20)
            writer = client(address)
            reader = client(address)
            value = b"HA object survives owner and leader" * 4096
            assert writer.put("ha-key", value, config) == 0
            owner.close()
            clients.remove(owner)
            assert reader.get("ha-key") == value
            primary.kill()
            primary.wait(timeout=10)
            wait_for(lambda: listening(standby_port), "standby promotion", timeout=45)
            wait_for(
                lambda: reader.get("ha-key") == value,
                "existing reader rediscovers leader",
                timeout=45,
            )
            assert reader.remove("ha-key", True) == 0
            writer.close()
            reader.close()
            clients.remove(writer)
            clients.remove(reader)
            standby.kill()
            standby.wait(timeout=10)
            replacement = start_master("sync", cluster, free_port(), ha=True)
            recovered = client(address)
            assert recovered.get("ha-key") in (None, b"")
            recovered.close()
            clients.remove(recovered)
            replacement.terminate()
            replacement.wait(timeout=10)
            print(
                "PASS HA: two Masters, SIGKILL promotion, existing client rediscovery, durable deletion",
                flush=True,
            )
        except BaseException:
            for log in logs:
                log.flush()
                print(f"--- {log.name} ---", file=sys.stderr)
                print(
                    Path(log.name).read_text(errors="replace")[-12000:], file=sys.stderr
                )
            raise
        finally:
            for item in clients:
                item.close()
            for process in reversed(children):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for pointer in gpu_allocations:
                assert cuda.cudaFree(pointer) == 0
            for log in logs:
                log.close()
            server.shutdown()
            server.server_close()
            server_thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--etcd", type=Path, required=True)
    parser.add_argument(
        "--gpu",
        action="store_true",
        help="Also test CUDA device buffers; requires a CUDA-enabled build",
    )
    parser.add_argument(
        "--graceful-only",
        action="store_true",
        help="Run only the graceful-unmount regression",
    )
    parser.add_argument(
        "--completion-only",
        action="store_true",
        help="Test completion RPC retry without local storage; requires test failpoints",
    )
    run(parser.parse_args())
