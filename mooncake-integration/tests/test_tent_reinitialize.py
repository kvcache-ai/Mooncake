"""Failed Python reinitialization must preserve the working TENT engine."""

import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


entries = {}
deleted = []
fail_next_put = threading.Event()
delay_next_get = threading.Event()
get_started = threading.Event()
release_get = threading.Event()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def key(self):
        return parse_qs(urlsplit(self.path).query).get("key", [""])[0]

    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        action = parse_qs(urlsplit(self.path).query).get("action", [""])[0]
        if urlsplit(self.path).path == "/control":
            if action == "get-started":
                self.reply(200, b"1" if get_started.is_set() else b"0")
            else:
                self.reply(400)
            return
        if delay_next_get.is_set():
            delay_next_get.clear()
            get_started.set()
            if not release_get.wait(10):
                self.reply(503, b"timed out waiting for probe release")
                return
        value = entries.get(self.key())
        self.reply(404 if value is None else 200, value or b"")

    def do_PUT(self):
        if fail_next_put.is_set():
            fail_next_put.clear()
            self.reply(500, b"injected metadata publication failure")
            return
        entries[self.key()] = self.rfile.read(int(self.headers["Content-Length"]))
        self.reply(200)

    def do_POST(self):
        action = parse_qs(urlsplit(self.path).query).get("action", [""])[0]
        if action == "fail-put":
            fail_next_put.set()
        elif action == "delay-get":
            release_get.clear()
            get_started.clear()
            delay_next_get.set()
        elif action == "release-get":
            release_get.set()
        else:
            self.reply(400)
            return
        self.reply(200, b"1")

    def do_DELETE(self):
        key = self.key()
        deleted.append(key)
        entries.pop(key, None)
        self.reply(200)


client = r"""
import ctypes
import socket
import sys
import threading
import time
import urllib.request
import engine

control_url = sys.argv[1].replace("/metadata", "/control")

def control(action, method="POST"):
    request = urllib.request.Request(
        control_url + "?action=" + action,
        data=b"" if method == "POST" else None,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.read()

failed = engine.TransferEngine()
assert failed.initialize("127.0.0.1", "http://127.0.0.1:1/metadata", "tcp", "") == -1
assert failed.get_rpc_port() == 0
del failed

engine = engine.TransferEngine()
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
identity = engine.get_engine_ptr()
port = engine.get_rpc_port()
assert port > 0
with socket.create_connection(("127.0.0.1", port), timeout=2):
    pass
assert engine.send_probe("127.0.0.1") == 0

assert engine.initialize("127.0.0.1", "http://127.0.0.1:1/metadata", "tcp", "") == -1
assert engine.get_engine_ptr() == identity
assert engine.get_rpc_port() == port
with socket.create_connection(("127.0.0.1", port), timeout=2):
    pass
assert engine.send_probe("127.0.0.1") == 0
with urllib.request.urlopen(sys.argv[1] + "?key=mooncake/tent/127.0.0.1", timeout=2) as response:
    assert response.status == 200
    assert response.read()

registered = ctypes.create_string_buffer(4096)
address = ctypes.addressof(registered)
assert engine.register_memory(address, len(registered)) == 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == -1
assert engine.get_engine_ptr() == identity
assert engine.send_probe("127.0.0.1") == 0
assert engine.unregister_memory(address) == 0

assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
second_identity = engine.get_engine_ptr()
assert second_identity != identity
assert engine.send_probe("127.0.0.1") == 0
managed = engine.allocate_managed_buffer(4096)
assert managed != 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == -1
assert engine.get_engine_ptr() == second_identity
assert engine.send_probe("127.0.0.1") == 0
assert engine.free_managed_buffer(managed, 4096) == 0

# A failed publication after registration must not make the live buffer
# disappear from the replacement gate. Conversely, a failed publication after
# removal must not leave a stale registration blocking replacement forever.
single = ctypes.create_string_buffer(4096)
single_address = ctypes.addressof(single)
assert control("fail-put") == b"1"
assert engine.register_memory(single_address, len(single)) != 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == -1
assert engine.get_engine_ptr() == second_identity
assert control("fail-put") == b"1"
assert engine.unregister_memory(single_address) != 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
third_identity = engine.get_engine_ptr()
assert third_identity != second_identity
reused = engine.allocate_managed_buffer(4096)
assert reused != 0
assert engine.free_managed_buffer(reused, 4096) == 0

batch_buffers = [ctypes.create_string_buffer(4096) for _ in range(2)]
batch_addresses = [ctypes.addressof(buffer) for buffer in batch_buffers]
assert control("fail-put") == b"1"
assert engine.batch_register_memory(batch_addresses, [4096, 4096]) != 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == -1
assert engine.get_engine_ptr() == third_identity
assert control("fail-put") == b"1"
assert engine.batch_unregister_memory(batch_addresses) != 0
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
fourth_identity = engine.get_engine_ptr()
assert fourth_identity != third_identity

# This is the first remote probe on the fourth engine, so its segment lookup
# cannot be served from the earlier cache. A second loopback engine gives the
# probe a real metadata read and a live RPC endpoint.
remote = type(engine)()
assert remote.initialize("127.0.0.2", sys.argv[1], "tcp", "") == 0
assert remote.get_rpc_port() > 0
assert engine.send_probe("127.0.0.2") == 0
# Rebuild once more so the delayed probe below cannot reuse the baseline's
# remote segment cache.
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
fourth_identity = engine.get_engine_ptr()
assert fourth_identity != third_identity
assert control("delay-get") == b"1"
results = {}

def probe():
    try:
        results["probe"] = engine.send_probe("127.0.0.2")
    except BaseException as error:
        results["probe"] = error

probe_thread = threading.Thread(target=probe)
probe_thread.start()
deadline = time.monotonic() + 5
while control("get-started", "GET") != b"1" and time.monotonic() < deadline:
    time.sleep(0.01)
if control("get-started", "GET") != b"1":
    control("release-get")
    probe_thread.join(timeout=10)
    raise AssertionError("probe never reached the delayed metadata read")


def reinitialize():
    try:
        results["reinitialize"] = engine.initialize("127.0.0.1", sys.argv[1], "tcp", "")
    except BaseException as error:
        results["reinitialize"] = error

reinitialize_thread = threading.Thread(target=reinitialize)
reinitialize_thread.start()
time.sleep(0.1)
assert control("release-get") == b"1"
probe_thread.join(timeout=10)
reinitialize_thread.join(timeout=10)
assert not probe_thread.is_alive()
assert not reinitialize_thread.is_alive()
assert results["probe"] == 0, results
assert results["reinitialize"] == 0, results
assert engine.get_engine_ptr() != fourth_identity
assert engine.send_probe("127.0.0.2") == 0
del engine
del remote
"""


def main():
    env = dict(
        os.environ,
        MC_USE_TENT="1",
        MC_FORCE_TCP="1",
        MC_TENT_CONF='{"rpc_server_hostname":"127.0.0.1","rpc_server_port":0}',
    )
    env.pop("MC_USE_TEV1", None)
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}/metadata"
            result = subprocess.run(
                [sys.executable, "-c", client, endpoint],
                env=env,
                capture_output=True,
                text=True,
                timeout=45,
            )
            if result.returncode:
                print(result.stdout, end="")
                print(result.stderr, end="", file=sys.stderr)
                raise AssertionError(f"child failed with {result.returncode}")
            assert deleted == [
                "mooncake/tent/127.0.0.1",
                "mooncake/tent/127.0.0.2",
            ], deleted
            assert not entries, entries
        finally:
            server.shutdown()
            worker.join()


if __name__ == "__main__":
    main()
