"""Python client e2e against a real conductor process.

Spins up a real ``mooncake_conductor`` subprocess (a temporary JSON config
pins free ports) and asserts through the pybind client over the RPC channel.
Run from the repo root:

    CONDUCTOR_BINARY=mooncake-conductor/build/mooncake_conductor \
      python3.12 -m pytest mooncake-conductor/tests/python/test_client_e2e.py -v

Requires ``build/mooncake-integration/_conductor.*.so`` to be built
(``-DWITH_CONDUCTOR=ON``); the interpreter must match that .so's cp tag.
"""

from __future__ import annotations

import ctypes
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
_MODULE_DIR = REPO_ROOT / "build" / "mooncake-integration"
_LIBASIO = REPO_ROOT / "build" / "mooncake-common" / "libasio.so"

# `_conductor` has DT_NEEDED on libasio.so (SONAME) and the build tree sets
# no RPATH; preloading libasio with RTLD_GLOBAL lets the dlopen of
# `_conductor` resolve against the already-loaded library, so callers need
# not set LD_LIBRARY_PATH. An import failure raises explicitly at collection
# time instead of being silently skipped.
ctypes.CDLL(str(_LIBASIO), mode=ctypes.RTLD_GLOBAL)
sys.path.insert(0, str(_MODULE_DIR))
from _conductor import ConductorClient  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _log_tail(log_path: str, max_bytes: int = 4000) -> str:
    with open(log_path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - max_bytes))
        return f.read().decode(errors="replace")


@pytest.fixture
def conductor_server():
    http_port, rpc_port = _free_port(), _free_port()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(
            {
                "http_server_port": http_port,
                "rpc_server_port": rpc_port,
                "services": [],
            },
            f,
        )
        path = f.name
    env = dict(os.environ, CONDUCTOR_CONFIG_PATH=path)
    binary = os.environ["CONDUCTOR_BINARY"]  # points at the built binary
    # Log to a temp file instead of a PIPE (a full pipe buffer would block
    # the subprocess); the tail is attached when the process fails to start.
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    log_path = log.name
    try:
        proc = subprocess.Popen([binary], env=env, stdout=log, stderr=log)
        deadline = time.time() + 10
        try:
            while time.time() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", rpc_port), 0.2):
                        break
                except OSError:
                    if proc.poll() is not None:
                        pytest.fail(
                            f"conductor exited early rc={proc.returncode}:\n"
                            f"{_log_tail(log_path)}"
                        )
                    time.sleep(0.1)
            else:
                proc.kill()
                pytest.fail(f"conductor did not start:\n{_log_tail(log_path)}")
            yield rpc_port
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
    finally:
        log.close()
        os.unlink(path)
        os.unlink(log_path)


def test_query_roundtrip(conductor_server):
    client = ConductorClient()
    assert client.setup(f"127.0.0.1:{conductor_server}") == 0
    assert client.health_check() == 0
    result = client.query(model_name="m", block_size=16, token_ids=[1, 2, 3])
    assert result["ret"] == 0
    assert result["hits"] == {}
    assert client.list_services()["ret"] == 0
    assert client.close() == 0


def test_unavailable_codes(conductor_server):
    client = ConductorClient()
    # Before setup(): ret is CONDUCTOR_UNAVAILABLE (-2000)
    assert client.query(model_name="m", block_size=16, token_ids=[])["ret"] == -2000
    assert client.health_check() == 1
