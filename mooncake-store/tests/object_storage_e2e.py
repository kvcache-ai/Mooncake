"""Manual E2E tests: actual Master, Python SDK, etcd and S3-compatible storage.

Requires the built store extension, etcd and boto3. Each run owns its etcd,
Masters and a unique object prefix; it never lists or deletes another prefix.
The optional HTTP proxy injects failures while forwarding data to real MinIO.
Evidence remains under --output, including on failure.
"""

import argparse
import base64
import concurrent.futures
import ctypes
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(predicate, description, timeout=60):
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


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def handle_request(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        control = self.server.control
        with control["lock"]:
            control["events"].append((time.monotonic(), self.command, self.path))
            if self.command == "PUT" and control["hold_put"]:
                control["hold_put"] = False
                control["pending"].append((self.path, dict(self.headers), body))
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            fail = self.command == "PUT" and control["fail_put"] > 0
            if fail:
                control["fail_put"] -= 1
            elif (
                self.command == "DELETE"
                and control["fail_delete"] > 0
                and self.path == control["fail_delete_path"]
            ):
                control["fail_delete"] -= 1
                fail = True
        if self.command == "PUT" and control["pause_put"]:
            control["pause_put"] = False
            control["put_paused"].set()
            assert control["put_release"].wait(30), "Timed out waiting to release PUT"
        if self.command == "DELETE" and self.path == control["pause_delete_path"]:
            control["pause_delete_path"] = None
            control["delete_paused"].set()
            assert control["delete_release"].wait(
                30
            ), "Timed out waiting to release DELETE"
        if fail:
            # A non-retryable DELETE response isolates the Master's retry
            # scheduling from the adapter's own transient HTTP retries.
            status, headers, data = (400 if self.command == "DELETE" else 503), {}, b""
        else:
            status, headers, data = forward(
                control["backend"], self.command, self.path, dict(self.headers), body
            )
            with control["lock"]:
                control["forwarded"].append(
                    (time.monotonic(), self.command, self.path, status)
                )
        self.send_response(status)
        for name, value in headers.items():
            if name.lower() not in (
                "connection",
                "transfer-encoding",
                "content-length",
            ):
                self.send_header(name, value)
        self.send_header(
            "Content-Length",
            headers.get("Content-Length", "0")
            if self.command == "HEAD"
            else str(len(data)),
        )
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    do_GET = do_HEAD = do_PUT = do_DELETE = handle_request


def forward(endpoint, method, path, headers, body):
    target = urllib.parse.urlsplit(endpoint)
    connection_type = (
        http.client.HTTPSConnection
        if target.scheme == "https"
        else http.client.HTTPConnection
    )
    connection = connection_type(target.hostname, target.port, timeout=30)
    try:
        # Preserve the signed Host header when forwarding to MinIO.
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


class Environment:
    def __init__(self, args):
        self.args = args
        self.output = args.output
        self.output.mkdir(parents=True, exist_ok=False)
        self.children = []
        self.clients = []
        self.logs = []
        self.results = []
        self.cluster = "unit3-" + uuid.uuid4().hex
        self.prefix = self.cluster + "/"
        self.failpoints = self.output / "failpoints"
        self.failpoints.mkdir()
        os.environ["MOONCAKE_TEST_FAILPOINT_DIR"] = str(self.failpoints)
        self.control = {
            "lock": threading.Lock(),
            "events": [],
            "forwarded": [],
            "pending": [],
            "hold_put": False,
            "fail_put": 0,
            "fail_delete": 0,
            "fail_delete_path": None,
            "pause_delete_path": None,
            "delete_paused": threading.Event(),
            "delete_release": threading.Event(),
            "backend": args.endpoint,
            "pause_put": False,
            "put_paused": threading.Event(),
            "put_release": threading.Event(),
        }
        self.proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        self.proxy.daemon_threads = True
        self.proxy.control = self.control
        threading.Thread(target=self.proxy.serve_forever, daemon=True).start()
        self.endpoint = f"http://127.0.0.1:{self.proxy.server_port}"
        self.etcd_port, self.peer_port = free_port(), free_port()
        self.address = f"etcd://127.0.0.1:{self.etcd_port}"
        os.environ["MC_STORE_CLUSTER_ID"] = self.cluster
        os.environ["MOONCAKE_STORE_CHECKSUM"] = "1"
        sys.path.insert(0, str(args.build / "mooncake-integration"))
        import _store as store

        self.store = store
        import boto3
        from botocore.config import Config

        self.s3 = boto3.client(
            "s3",
            endpoint_url=args.endpoint,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )
        self.s3.head_bucket(Bucket=args.bucket)
        self.spawn(
            "etcd",
            [
                str(args.etcd),
                "--name=unit3",
                f"--data-dir={self.output / 'etcd'}",
                f"--listen-client-urls=http://127.0.0.1:{self.etcd_port}",
                f"--advertise-client-urls=http://127.0.0.1:{self.etcd_port}",
                f"--listen-peer-urls=http://127.0.0.1:{self.peer_port}",
                f"--initial-advertise-peer-urls=http://127.0.0.1:{self.peer_port}",
                f"--initial-cluster=unit3=http://127.0.0.1:{self.peer_port}",
            ],
        )
        wait_for(lambda: listening(self.etcd_port), "etcd startup")

    def spawn(self, name, command, env=None):
        log = open(self.output / f"{name}-{len(self.logs)}.log", "wb")
        self.logs.append(log)
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.children.append(child)
        return child

    def master(
        self,
        ready=True,
        max_pending_records=1000,
        enabled=True,
        maintenance_batch_size=32,
        cleanup_interval_ms=200,
    ):
        port = free_port()
        child = self.spawn(
            "master",
            [
                str(self.args.build / "mooncake-store/src/mooncake_master"),
                f"--port={port}",
                "--rpc_address=127.0.0.1",
                "--rpc_thread_num=4",
                "--handler_worker_thread_num=8",
                "--enable_ha=true",
                "--enable_oplog=true",
                "--enable_metric_reporting=false",
                f"--metrics_port={free_port()}",
                f"--enable_oplog_snapshot={str(self.args.scenario == 'snapshot').lower()}",
                "--snapshot_interval_seconds=1",
                "--snapshot_object_store_type=local",
                "--weight_management_oplog_capability_confirmed=true",
                f"--cluster_id={self.cluster}",
                f"--ha_backend_connstring=http://127.0.0.1:{self.etcd_port}",
                f"--enable_object_storage={str(enabled).lower()}",
                "--object_storage_provider=s3",
                f"--object_storage_endpoint={self.endpoint}",
                f"--object_storage_bucket={self.args.bucket}",
                "--object_storage_region=us-east-1",
                "--object_storage_path_style=true",
                f"--object_storage_prefix={self.prefix}",
                f"--object_storage_max_pending_records={max_pending_records}",
                f"--object_storage_cleanup_interval_ms={cleanup_interval_ms}",
                f"--object_storage_maintenance_batch_size={maintenance_batch_size}",
                f"--default_kv_lease_ttl={10000 if self.args.scenario == 'gc' else 3000}ms",
                "--ha_leader_lease_ttl_sec=5",
                f"--put_start_discard_timeout_sec={2 if self.args.scenario == 'import' else 30}",
                f"--put_start_release_timeout_sec={5 if self.args.scenario == 'import' else 600}",
            ],
            dict(
                os.environ,
                MOONCAKE_SNAPSHOT_LOCAL_PATH=str(self.output / "snapshots"),
            ),
        )
        if ready:
            self.serving_port = port
            wait_for(
                lambda: listening(port) or child.poll() is not None, "Master startup"
            )
            assert child.poll() is None, "Master failed; inspect output logs"
        return child, port

    def client(self, memory=0, ha=False):
        if self.args.scenario == "object-only":
            memory = 0
        client = self.store.MooncakeDistributedStore()
        status = client.setup(
            f"127.0.0.1:{free_port()}",
            "P2PHANDSHAKE",
            memory,
            32 << 20,
            "tcp",
            "",
            self.address if ha else f"127.0.0.1:{self.serving_port}",
        )
        assert status == 0, f"Client setup failed: {status}"
        self.clients.append(client)
        return client

    def names(self):
        response = self.s3.list_objects_v2(Bucket=self.args.bucket, Prefix=self.prefix)
        assert not response.get(
            "IsTruncated"
        ), "Test namespace unexpectedly exceeds one page"
        return {entry["Key"] for entry in response.get("Contents", [])}

    def report(self, name):
        self.results.append({"scenario": name, "result": "PASS"})
        print("PASS", name, flush=True)

    def close(self):
        for client in reversed(self.clients):
            client.close()
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
        for child in reversed(self.children):
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        self.proxy.shutdown()
        for log in self.logs:
            log.close()
        (self.output / "results.json").write_text(
            json.dumps(
                {
                    "cluster": self.cluster,
                    "prefix": self.prefix,
                    "results": self.results,
                    "requests": self.control["events"],
                    "forwarded": self.control["forwarded"],
                },
                indent=2,
            )
        )


def run_basic(args):
    env = Environment(args)
    try:
        primary, _ = env.master()
        owner = env.client(128 << 20)
        writer, reader = env.client(ha=True), env.client(ha=True)
        config = env.store.ReplicateConfig()
        if args.scenario == "object-only":
            config.replica_num = 0
            assert writer.put("no-replicas", b"invalid", config) != 0
        config.os_replica_num = 1
        value = bytes(range(256)) * 4096
        assert writer.put("value", value, config) == 0
        if args.scenario == "object-only":
            replicas = reader.get_replica_desc("value")
            assert len(replicas) == 1 and replicas[0].is_object_storage_replica()
            mixed_config = env.store.ReplicateConfig()
            mixed_config.os_replica_num = 1
            assert writer.put("missing-memory", value, mixed_config) != 0
            assert reader.get("missing-memory") in (None, b"")
        assert reader.get("value") == value
        old_objects = env.names()
        assert len(old_objects) == 1, old_objects
        replacement = value[::-1]
        assert writer.upsert("value", replacement, config) == 0
        assert reader.get("value") == replacement
        assert len(env.names() - old_objects) == 1
        env.report("sync put and immutable-name upsert")

        values = [b"batch-A" * 16384, b"batch-B" * 16384]
        buffers = [ctypes.create_string_buffer(value) for value in values]
        addresses = [ctypes.addressof(buffer) for buffer in buffers]
        sizes = list(map(len, values))
        for address, size in zip(addresses, sizes):
            assert writer.register_buffer(address, size) == 0
        try:
            with env.control["lock"]:
                env.control["fail_put"] = 1
            results = writer.batch_put_from(
                ["batch-a", "batch-b"], addresses, sizes, config
            )
            assert results[0] != 0 and results[1] == 0, results
            assert reader.get("batch-a") in (None, b"")
            assert reader.get("batch-b") == values[1]
            assert writer.batch_upsert_from(
                ["batch-a", "batch-b"], addresses, sizes, config
            ) == [0, 0]
        finally:
            for address in addresses:
                assert writer.unregister_buffer(address) == 0
        env.report("batch partial failure and upsert")
        assert writer.put("leased", value, config) == 0

        owner.close()
        env.clients.remove(owner)
        wait_for(
            lambda: all(
                not replica.is_memory_replica()
                for replica in reader.get_replica_desc("value")
            ),
            "original owner replicas removed",
        )
        assert reader.get("value") == replacement
        target = ctypes.create_string_buffer(len(replacement) + 16)
        assert reader.get_into("value", ctypes.addressof(target), len(target)) == len(
            replacement
        )
        assert target.raw[: len(replacement)] == replacement
        assert bytes(reader.get_buffer("value")) == replacement
        assert reader.batch_get_session_start(["value"]) == [0]
        ranges = [ctypes.create_string_buffer(19), ctypes.create_string_buffer(7)]
        assert reader.batch_get_into_multi_buffer_ranges(
            ["value"],
            [[ctypes.addressof(buffer) for buffer in ranges]],
            [[19, 7]],
            [[13, len(replacement) - 7]],
        ) == [26]
        assert ranges[0].raw == replacement[13:32] and ranges[1].raw == replacement[-7:]
        assert reader.batch_get_session_end(["value"]) == 0
        assert [
            bytes(buffer) for buffer in reader.batch_get_buffer(["batch-a", "batch-b"])
        ] == values
        env.report("owner-independent ordinary, batch, buffer and session range reads")
        lease_owner = env.client(64 << 20)
        assert reader.batch_get_session_start(["leased"]) == [0]
        leased_object = next(
            replica.get_object_storage_descriptor().object_key
            for replica in reader.get_replica_desc("leased")
            if replica.is_object_storage_replica()
        )
        leased_name = next(name for name in env.names() if name.endswith(leased_object))
        assert writer.upsert("leased", replacement, config) == 0
        leased_buffer = ctypes.create_string_buffer(32)
        assert reader.batch_get_into_multi_buffer_ranges(
            ["leased"], [[ctypes.addressof(leased_buffer)]], [[32]], [[0]]
        ) == [32]
        assert leased_buffer.raw == value[:32]
        assert (
            leased_name in env.names()
        ), "Replacement removed a leased physical object"
        assert reader.batch_get_session_end(["leased"]) == 0
        assert reader.get("leased") == replacement
        env.report("old read lease survives immutable replacement")
        lease_owner.close()
        env.clients.remove(lease_owner)

        standby, standby_port = env.master(ready=False)
        primary.kill()
        primary.wait(timeout=15)
        wait_for(lambda: listening(standby_port), "HA promotion", timeout=90)
        wait_for(
            lambda: reader.get("value") == replacement,
            "existing reader rediscovers leader",
        )
        assert reader.remove("value", True) == 0
        assert reader.get("value") in (None, b"")
        standby.kill()
        standby.wait(timeout=15)
        env.master()
        recovered = env.client()
        assert recovered.get("value") in (None, b"")
        assert recovered.get("batch-a") == values[0]
        env.report("HA promotion, durable removal and restart")

        new_owner = env.client(64 << 20)
        wait_for(
            lambda: writer.get("batch-a") == values[0],
            "writer rediscovers restarted Master",
        )
        before = len(env.control["events"])
        with env.control["lock"]:
            env.control["hold_put"] = True
        assert writer.put("late", b"late upload" * 4096, config) != 0
        with env.control["lock"]:
            path, headers, payload = env.control["pending"].pop()
        wait_for(
            lambda: any(
                method == "DELETE" and requested == path
                for _, method, requested in env.control["events"][before:]
            ),
            "cleanup of exact ambiguous upload before delayed PUT",
        )
        status, _, _ = forward(args.endpoint, "PUT", path, headers, payload)
        assert status == 200, status
        physical = urllib.parse.unquote(path.split("/", 2)[2])
        assert physical in env.names()
        wait_for(lambda: physical not in env.names(), "late PUT cleaned again")
        assert reader.get("late") in (None, b"")
        assert (
            sum(
                method == "PUT" and requested == path
                for _, method, requested in env.control["events"]
            )
            == 1
        )
        env.report("DELETE-before-late-PUT, repeated cleanup, no logical resurrection")
        new_owner.close()
        env.clients.remove(new_owner)
    finally:
        env.close()


def run_budget(args):
    env = Environment(args)
    try:
        _, port = env.master(max_pending_records=1)
        owner = env.client(64 << 20)
        writer = env.client()
        object_config = env.store.ReplicateConfig()
        object_config.os_replica_num = 1
        memory_config = env.store.ReplicateConfig()
        old, new = b"old!" * 1024, b"new!" * 1024
        assert writer.put("budget", old, object_config) == 0
        assert writer.put("stable", old, object_config) == 0
        assert len(env.names()) == 2
        env.report("committed objects exceed the pending-work budget")
        driver_log = open(env.output / "protocol.log", "wb")
        env.logs.append(driver_log)
        driver = subprocess.Popen(
            [str(args.build / "mooncake-store/tests/object_storage_protocol_client")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=driver_log,
            text=True,
        )
        env.children.append(driver)

        def command(text):
            driver.stdin.write(text + "\n")
            driver.stdin.flush()
            response = driver.stdout.readline().split()
            assert response, "Protocol driver exited"
            return response

        assert command(f"connect 127.0.0.1:{port}") == ["0"]
        started = command("start")
        assert started[0] == "0", started
        env.s3.put_object(Bucket=args.bucket, Key=env.prefix + started[2], Body=old)
        assert command("uploaded") == ["0"]
        assert writer.get("reused-attempt") in (None, b"")
        assert writer.upsert("budget", new, object_config) != 0
        assert writer.get("budget") == old
        env.report("uploaded but uncommitted object retains admission slot")
        assert command("revoke") == ["0"]
        assert writer.remove("stable", True) == 0
        env.report("revoke and delete remain available above the pending budget")
        assert writer.upsert("budget", new, memory_config) == 0
        deadline = time.monotonic() + 15
        while env.names() and time.monotonic() < deadline:
            assert writer.upsert("budget", new, memory_config) == 0
            time.sleep(0.1)
        assert (
            not env.names()
        ), "Hot-key updates must not postpone retired object cleanup"
        assert writer.upsert("budget", old, memory_config) == 0
        assert writer.get("budget") == old
        wait_for(
            lambda: writer.put("budget-next", new, object_config) == 0,
            "cleanup frees admission budget",
        )
        env.report("cross-tier replacement, hot-key cleanup and post-GC memory upsert")
        # A plain memory write session can inherit an exact attempt while the
        # old object is pending cleanup. It must reject and revoke cleanly.
        assert writer.remove("budget-next", True) == 0
        session_results = writer.batch_put_session_start(
            ["budget-next"], [len(new)], memory_config
        )
        assert session_results == [-600], session_results
        assert writer.put("budget-next", b"session-recreated", memory_config) == 0
        assert writer.get("budget-next") == b"session-recreated"
        env.report("tracked memory write session rejected and exact allocation revoked")
        assert owner is not None
    finally:
        env.close()


def run_cleanup(args):
    env = Environment(args)
    try:
        primary, port = env.master(
            max_pending_records=2,
            maintenance_batch_size=2,
            cleanup_interval_ms=60000,
        )
        owner = env.client()
        writer = env.client()
        config = env.store.ReplicateConfig()
        config.replica_num = 0
        config.os_replica_num = 1
        driver_log = open(env.output / "protocol.log", "wb")
        env.logs.append(driver_log)
        driver = subprocess.Popen(
            [str(args.build / "mooncake-store/tests/object_storage_protocol_client")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=driver_log,
            text=True,
        )
        env.children.append(driver)

        def command(text):
            driver.stdin.write(text + "\n")
            driver.stdin.flush()
            response = driver.stdout.readline().split()
            assert response, "Protocol driver exited"
            return response

        assert command(f"connect 127.0.0.1:{port}") == ["0"]
        started = command("start_object")
        assert started[0] == "0", started
        physical = env.prefix + started[2]
        env.s3.put_object(Bucket=args.bucket, Key=physical, Body=b"X" * 4096)
        assert command("uploaded") == ["0"]
        assert command("end") == ["0"]
        assert writer.get("reused-attempt") == b"X" * 4096
        keys = [f"cleanup-{i}" for i in range(8)]
        for key in keys:
            assert writer.put(key, key.encode() * 4096, config) == 0
        assert len(env.names()) == len(keys) + 1
        failed_path = f"/{args.bucket}/{physical}"
        with env.control["lock"]:
            env.control["fail_delete"] = 1
            env.control["fail_delete_path"] = failed_path
            env.control["pause_delete_path"] = failed_path
        assert writer.remove("reused-attempt", True) == 0
        for key in keys:
            assert writer.remove(key, True) == 0
        try:
            assert env.control["delete_paused"].wait(15), "DELETE did not reach barrier"
            assert command("uploaded") == ["0"]
        finally:
            env.control["delete_release"].set()
        wait_for(
            lambda: env.names() == {physical},
            "healthy backlog drains across batches despite one DELETE failure",
            timeout=30,
        )
        assert (
            sum(
                method == "DELETE" and path == failed_path
                for _, method, path in env.control["events"]
            )
            == 1
        )
        env.report(
            "continuous batches, failed DELETE backoff and duplicate notification isolation"
        )
        # Restoring the retired records must rebuild scheduling and admission.
        # Failed DELETEs may be retried after failover; their in-memory retry
        # deadlines are not a durability contract.
        primary.kill()
        primary.wait(timeout=15)
        env.master(
            max_pending_records=2, maintenance_batch_size=2, cleanup_interval_ms=10000
        )
        recovered = env.client()
        wait_for(lambda: not env.names(), "recovered cleanup backlog", timeout=30)
        assert recovered.put("after-cleanup", b"new", config) == 0
        assert recovered.get("after-cleanup") == b"new"
        env.report("restart rebuilds pending work and resumes admission")
        assert owner is not None
    finally:
        env.close()


def run_faults(args):
    env = Environment(args)
    try:
        primary, _ = env.master()
        owner = env.client(64 << 20, ha=True)
        writer, reader = env.client(ha=True), env.client(ha=True)
        config = env.store.ReplicateConfig()
        config.os_replica_num = 1
        for index, point in enumerate(
            (
                "object_before_commit_append",
                "object_commit_durable_before_publish",
                "object_commit_after_publish_before_reply",
            )
        ):
            wait_for(
                lambda: owner.upsert("fault-ready", b"ready") == 0, "owner remount"
            )
            wait_for(
                lambda: writer.is_exist("fault-ready") == 1
                and reader.is_exist("fault-ready") == 1,
                "writer and reader rediscover restarted Master",
            )
            key = f"crash-{index}"
            value = bytes([index + 1]) * 65536
            (env.failpoints / f"{point}.arm").touch()
            before = len(env.control["events"])
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                put = executor.submit(writer.put, key, value, config)
                wait_for(
                    lambda: (env.failpoints / f"{point}.hit").exists() or put.done(),
                    point,
                )
                assert (
                    env.failpoints / f"{point}.hit"
                ).exists(), "Failpoint not enabled/reached"
                assert not put.done(), "Write acknowledged before the armed boundary"
                if index < 2:
                    assert reader.get(key) in (
                        None,
                        b"",
                    ), "Undurable/unpublished value became visible"
                primary.kill()
                primary.wait(timeout=15)
                primary, _ = env.master()
                put.result(
                    timeout=90
                )  # A lost reply can return an error or recover through retry.
            fresh = env.client()
            if index == 0:
                assert fresh.get(key) in (None, b"")
            else:
                wait_for(lambda: fresh.get(key) == value, "durable value recovered")
            with env.control["lock"]:
                uploads = [
                    path
                    for _, method, path in env.control["events"][before:]
                    if method == "PUT"
                ]
            assert len(uploads) == 1, uploads
            env.report(point + ": crash/recovery and exactly one physical PUT")
        assert owner is not None
    finally:
        env.close()


def run_stale(args):
    env = Environment(args)
    driver = None
    try:
        primary, port = env.master()
        owner = env.client(64 << 20, ha=True)
        driver_log = open(env.output / "protocol.log", "wb")
        env.logs.append(driver_log)
        driver = subprocess.Popen(
            [str(args.build / "mooncake-store/tests/object_storage_protocol_client")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=driver_log,
            text=True,
        )
        env.children.append(driver)

        def command(text):
            driver.stdin.write(text + "\n")
            driver.stdin.flush()
            response = driver.stdout.readline().split()
            assert response, "Protocol driver exited; inspect protocol.log"
            return list(map(int, response[:1])) + response[1:]

        assert command(f"connect 127.0.0.1:{port}") == [0]
        first = command("start")
        assert first[0] == 0, first
        (env.failpoints / "object_cleanup_forgotten.arm").touch()
        assert command("not_started") == [0]
        assert command("revoke") == [0]
        wait_for(
            lambda: (env.failpoints / "object_cleanup_forgotten.hit").exists(),
            "old record durably forgotten",
        )
        primary.kill()
        primary.wait(timeout=15)
        _, port = env.master()
        # Remount completion comes from the real owner heartbeat, without writing
        # another key (which would consume replica IDs before the assertion).
        assert command(f"connect 127.0.0.1:{port}") == [0]
        second = []

        def start():
            nonlocal second
            second = command("start")
            return second[0] == 0

        wait_for(start, "owner remount and replacement authorization")
        assert first[1] == second[1], (
            "Test must actually reuse the replica ID",
            first,
            second,
        )
        assert first[2] != second[2]
        assert command("old_end")[0] != 0
        assert command("old_revoke")[0] != 0
        assert command("old_finished") == [0]
        assert owner.get("reused-attempt") in (
            None,
            b"",
        ), "Old completion published new write"
        assert command("not_started") == [0]
        assert command("revoke") == [0], "Old revoke destroyed the new attempt"
        env.report(
            "post-GC restart reuses ID: stale completion/revoke rejected, old notification idempotent"
        )
    finally:
        if driver:
            driver.stdin.close()
            driver.wait(timeout=15)
        env.close()


def run_import(args):
    env = Environment(args)
    driver = None
    try:
        _, port = env.master()
        owner = env.client(64 << 20)
        writer = env.client()
        log = open(env.output / "protocol.log", "wb")
        env.logs.append(log)
        driver = subprocess.Popen(
            [str(args.build / "mooncake-store/tests/object_storage_protocol_client")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        env.children.append(driver)

        def command(text):
            driver.stdin.write(text + "\n")
            driver.stdin.flush()
            response = driver.stdout.readline().split()
            assert response and response[0] == "0", response
            return response[1:]

        command(f"connect 127.0.0.1:{port}")
        (group,) = command("begin_import")
        config = env.store.ReplicateConfig()
        config.os_replica_num = 1
        config.group_ids = [group]
        config.residency_affinity_ids = ["unit3-affinity"]
        config.with_hard_pin = True
        config.data_type = env.store.ObjectDataType.WEIGHT
        env.control["pause_put"] = True
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            put = executor.submit(writer.put, "import-aborted", b"data" * 1024, config)
            wait_for(
                env.control["put_paused"].is_set, "PUT in flight during import abort"
            )
            command("abort_import")
            env.control["put_release"].set()
            assert put.result(timeout=30) != 0
        assert writer.get("import-aborted") in (None, b"")
        assert (
            sum(
                method == "PUT" and status == 200
                for _, method, _, status in env.control["forwarded"]
            )
            == 1
        ), "Real MinIO PUT must have succeeded"
        wait_for(lambda: not env.names(), "aborted import upload reclaimed")
        plain = env.store.ReplicateConfig()
        wait_for(
            lambda: writer.put("import-aborted", b"recreated", plain) == 0
            and writer.get("import-aborted") == b"recreated",
            "recreate key after rejected commit",
        )
        assert writer.get("import-aborted") == b"recreated"
        env.report(
            "abort before admission: real PUT reclaimed, same key writable again"
        )

        (group,) = command("begin_import")
        config.group_ids = [group]
        point = "object_after_import_admission"
        (env.failpoints / f"{point}.arm").touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            put = executor.submit(writer.put, "import-admitted", b"data" * 1024, config)
            wait_for(
                lambda: (env.failpoints / f"{point}.hit").exists() or put.done(), point
            )
            assert (env.failpoints / f"{point}.hit").exists()
            try:
                command("abort_import")
            finally:
                (env.failpoints / f"{point}.release").touch()
            assert (
                put.result(timeout=30) == 0
            ), "Admitted write must publish despite a later abort"
        wait_for(
            lambda: writer.put("import-admitted", b"recreated", plain) == 0
            and writer.get("import-admitted") == b"recreated",
            "recreate key after admitted write and import deletion",
        )
        assert writer.get("import-admitted") == b"recreated"
        wait_for(
            lambda: not env.names(), "admitted then aborted import upload reclaimed"
        )
        assert (
            sum(
                method == "PUT" and status == 200
                for _, method, _, status in env.control["forwarded"]
            )
            == 2
        )
        assert owner is not None
        env.report(
            "abort after admission: committed write reconciled and reclaimed, same key writable again"
        )
    finally:
        env.control["put_release"].set()
        if driver:
            driver.stdin.close()
            driver.wait(timeout=15)
        env.close()


def run_snapshot(args):
    env = Environment(args)
    try:
        primary, _ = env.master()
        owner = env.client(64 << 20)
        writer = env.client()
        config = env.store.ReplicateConfig()
        config.os_replica_num = 1
        value = b"snapshot-object" * 4096
        assert writer.put("snapshot-live", value, config) == 0
        assert writer.put("snapshot-removed", value, config) == 0
        assert writer.remove("snapshot-removed", True) == 0
        standby, _ = env.master(ready=False)

        def read_etcd(key, end=None):
            body = {"key": base64.b64encode(key.encode()).decode()}
            if end is not None:
                body["range_end"] = base64.b64encode(end.encode()).decode()
            data = json.dumps(body).encode()
            request = urllib.request.Request(
                f"http://127.0.0.1:{env.etcd_port}/v3/kv/range",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                values = json.load(response).get("kvs", [])
            return values

        def control(name):
            values = read_etcd(f"/oplog/{env.cluster}/snapshot/{name}")
            return base64.b64decode(values[0]["value"]) if values else None

        wait_for(lambda: control("latest") is not None, "standby snapshot publication")
        # A second advancing snapshot is needed for safe log pruning, whose
        # floor is the fallback snapshot rather than the newest one.
        assert writer.put("snapshot-tail", value[::-1], config) == 0
        wait_for(
            lambda: control("compaction_floor") is not None, "snapshot log pruning"
        )
        floor = int(control("compaction_floor"))
        assert floor > 0
        batch_prefix = f"/oplog/{env.cluster}/batches/"
        wait_for(
            lambda: not read_etcd(
                batch_prefix + f"{0:020d}", batch_prefix + f"{floor + 1:020d}"
            ),
            "actual deletion of pre-snapshot log batches",
        )
        (env.output / "pruned-through.txt").write_text(str(floor))
        descriptor = json.loads(control("latest"))
        assert descriptor["schema_version"] == 3, descriptor
        (env.output / "snapshot-descriptor.json").write_text(
            json.dumps(descriptor, indent=2)
        )
        primary.kill()
        standby.kill()
        primary.wait(timeout=15)
        standby.wait(timeout=15)
        env.master()
        reader = env.client()
        assert reader.get("snapshot-live") == value
        assert reader.get("snapshot-tail") == value[::-1]
        assert reader.get("snapshot-removed") in (None, b"")
        assert owner is not None
        env.report(
            "v3 snapshot plus pruned-log recovery: live values retained, deletion retained"
        )
    finally:
        env.close()


def run_gc(args):
    env = Environment(args)
    driver = None
    try:
        _, port = env.master()
        owner = env.client(64 << 20)
        writer = env.client()
        object_config, plain = env.store.ReplicateConfig(), env.store.ReplicateConfig()
        object_config.os_replica_num = 1

        def seed_retirement(key):
            assert writer.put(key, b"old-object", object_config) == 0
            (env.failpoints / "object_cleanup_forgotten.arm").touch()
            assert writer.remove(key, True) == 0

        seed_retirement("gc-memory")
        point = "object_memory_before_end"
        (env.failpoints / f"{point}.arm").touch()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            put = executor.submit(writer.put, "gc-memory", b"new-memory", plain)
            wait_for(
                lambda: (env.failpoints / f"{point}.hit").exists() or put.done(), point
            )
            assert (
                env.failpoints / f"{point}.hit"
            ).exists(), "Memory Start must see old record"
            try:
                wait_for(
                    lambda: (env.failpoints / "object_cleanup_forgotten.hit").exists(),
                    "GC between Start and End",
                )
            finally:
                (env.failpoints / "object_cleanup_forgotten.release").touch()
                (env.failpoints / f"{point}.release").touch()
            assert put.result(timeout=30) == 0
        assert writer.get("gc-memory") == b"new-memory"
        env.report(
            "actual memory transfer, GC of last old record, then successful durable End"
        )
        wait_for(
            lambda: not (env.failpoints / "object_cleanup_forgotten.hit").exists(),
            "cleanup barrier released",
        )

        log = open(env.output / "protocol.log", "wb")
        env.logs.append(log)
        driver = subprocess.Popen(
            [str(args.build / "mooncake-store/tests/object_storage_protocol_client")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        env.children.append(driver)

        def command(text):
            driver.stdin.write(text + "\n")
            driver.stdin.flush()
            response = driver.stdout.readline().split()
            assert response, "Protocol driver exited"
            return int(response[0])

        assert command(f"connect 127.0.0.1:{port}") == 0
        seed_retirement("reused-attempt")
        assert command("start_memory") == 0
        assert command("partial_end") != 0, "Tracked attempts require ALL completion"
        try:
            wait_for(
                lambda: (env.failpoints / "object_cleanup_forgotten.hit").exists(),
                "GC between Start and Revoke",
            )
        finally:
            (env.failpoints / "object_cleanup_forgotten.release").touch()
        assert command("revoke") == 0
        assert writer.put("reused-attempt", b"recreated", plain) == 0
        assert writer.get("reused-attempt") == b"recreated"
        assert owner is not None
        env.report(
            "GC of last old record before exact Revoke; partial End rejected; same key writable"
        )
    finally:
        if driver:
            driver.stdin.close()
            driver.wait(timeout=15)
        env.close()


def run(args):
    functions = {
        "basic": run_basic,
        "object-only": run_basic,
        "budget": run_budget,
        "cleanup": run_cleanup,
        "faults": run_faults,
        "stale": run_stale,
        "snapshot": run_snapshot,
        "import": run_import,
        "gc": run_gc,
    }
    functions[args.scenario](args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=(
            "basic",
            "object-only",
            "budget",
            "cleanup",
            "faults",
            "stale",
            "snapshot",
            "import",
            "gc",
        ),
        default="basic",
    )
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--etcd", type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())
