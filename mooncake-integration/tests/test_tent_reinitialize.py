"""Failed Python reinitialization must preserve the working TENT engine."""

import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


entries = {}
deleted = []


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
        value = entries.get(self.key())
        self.reply(404 if value is None else 200, value or b"")

    def do_PUT(self):
        entries[self.key()] = self.rfile.read(int(self.headers["Content-Length"]))
        self.reply(200)

    def do_DELETE(self):
        key = self.key()
        deleted.append(key)
        entries.pop(key, None)
        self.reply(200)


client = r"""
import socket
import sys
import urllib.request
import engine

engine = engine.TransferEngine()
assert engine.initialize("127.0.0.1", sys.argv[1], "tcp", "") == 0
port = engine.get_rpc_port()
with socket.create_connection(("127.0.0.1", port), timeout=2):
    pass
assert engine.initialize("127.0.0.1", "http://127.0.0.1:1/metadata", "tcp", "") == -1
with socket.create_connection(("127.0.0.1", port), timeout=2):
    pass
with urllib.request.urlopen(sys.argv[1] + "?key=mooncake/tent/127.0.0.1", timeout=2) as response:
    assert response.status == 200
    assert response.read()
del engine
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
            assert deleted == ["mooncake/tent/127.0.0.1"], deleted
            assert not entries, entries
        finally:
            server.shutdown()
            worker.join()


if __name__ == "__main__":
    main()
