"""Python client e2e against a real conductor process.

Spins up a real ``mooncake_conductor`` subprocess (a temporary JSON config
pins free ports) and asserts through the pybind client over the RPC channel.
Run from the repo root:

    CONDUCTOR_BINARY=mooncake-conductor/build/mooncake_conductor \
      python3.12 -m pytest mooncake-conductor/tests/python/test_client_e2e.py -v

Requires an installed wheel with Conductor support, pytest, msgpack and pyzmq.
The client always uses the installed public API, including its native extension.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen

import msgpack
import pytest
import zmq

from mooncake.conductor import ConductorClient

REPO_ROOT = Path(__file__).resolve().parents[3]


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
def conductor_endpoints():
    http_port, rpc_port = _free_port(), _free_port()
    while rpc_port == http_port:
        rpc_port = _free_port()
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
    server_library_path = env.pop("CONDUCTOR_SERVER_LD_LIBRARY_PATH", None)
    if server_library_path is not None:
        env["LD_LIBRARY_PATH"] = server_library_path
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
            yield rpc_port, http_port
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            # Pytest retains this output on failure, including failures after
            # startup (e.g. event ingestion or RPC regressions).
            print(f"Conductor server log:\n{_log_tail(log_path)}")
    finally:
        log.close()
        os.unlink(path)
        os.unlink(log_path)


@pytest.fixture
def conductor_server(conductor_endpoints):
    return conductor_endpoints[0]


def test_query_roundtrip(conductor_server):
    with ConductorClient() as client:
        assert client.setup(f"127.0.0.1:{conductor_server}") == 0
        assert client.health_check() == 0
        result = client.query(model_name="m", block_size=16, token_ids=[1, 2, 3])
        assert result["ret"] == 0
        assert result["hits"] == {}
        assert client.list_services() == {"ret": 0, "count": 0, "services": []}
    assert client.health_check() == 1
    assert client.close() == 0


def test_unavailable_codes(conductor_server):
    client = ConductorClient()
    # Before setup(): ret is CONDUCTOR_UNAVAILABLE (-2000)
    assert client.query(model_name="m", block_size=16, token_ids=[])["ret"] == -2000
    assert client.health_check() == 1


@pytest.mark.parametrize(
    "bad_addr, expected", [("invalid", -600), ("127.0.0.1:1", -900)]
)
def test_failed_setup_replaces_connection(conductor_server, bad_addr, expected):
    client = ConductorClient()
    addr = f"127.0.0.1:{conductor_server}"
    try:
        assert client.setup(addr) == 0
        assert client.health_check() == 0
        assert client.setup(bad_addr, 200, 200) == expected
        assert client.health_check() == 1
        assert client.list_services()["ret"] == -2000
        assert client.query(model_name="m", block_size=4, token_ids=[])["ret"] == -2000
        assert client.setup(addr) == 0
        assert client.health_check() == 0
        # Replacing an active connection with another successful setup works.
        assert client.setup(addr) == 0
        assert client.health_check() == 0
        assert client.close() == client.close() == 0
        assert client.health_check() == 1
        assert client.setup(bad_addr, 200, 200) == expected
        assert client.health_check() == 1
        assert client.setup(addr) == 0
        assert client.health_check() == 0
    finally:
        client.close()


@pytest.mark.parametrize("rank", [0, 12])
def test_query_rank_keys_match_http(conductor_endpoints, rank):
    rpc_port, http_port = conductor_endpoints
    vectors = json.loads(
        (
            REPO_ROOT
            / "mooncake-conductor/tests/fixtures/hash_golden_vectors_sha256.json"
        ).read_text()
    )
    case = vectors["cases"][0]
    profile = {k: v for k, v in vectors["profile"].items() if k != "root_digest"}
    tokens = case["token_ids"]
    expected = len(case["expected"]) * case["block_size"]
    client = ConductorClient()
    with zmq.Context() as context, context.socket(zmq.XPUB) as publisher:
        publisher.setsockopt(zmq.LINGER, 0)
        port = publisher.bind_to_random_port("tcp://127.0.0.1")
        try:
            assert client.setup(f"127.0.0.1:{rpc_port}") == 0
            assert (
                client.register(
                    {
                        "instance_id": "rank-test",
                        "endpoint": f"tcp://127.0.0.1:{port}",
                        "model_name": "rank-model",
                        "block_size": case["block_size"],
                        "dp_rank": rank,
                        "hash_profile": profile,
                    }
                )
                == 0
            )
            assert publisher.poll(3000), "No ZMQ subscription"
            assert publisher.recv().startswith(b"\x01")
            event = {
                "type": "BlockStored",
                "block_hashes": [int(h["projected_decimal"]) for h in case["expected"]],
                "parent_block_hash": None,
                "token_ids": tokens,
                "block_size": case["block_size"],
                "lora_id": None,
                "lora_name": None,
                "medium": "GPU",
                "cache_salt": None,
            }
            publisher.send_multipart(
                [
                    b"",
                    (0).to_bytes(8, "big"),
                    msgpack.packb([time.time(), [event], rank]),
                ]
            )
            deadline = time.monotonic() + 5
            while True:
                result = client.query(
                    model_name="rank-model",
                    block_size=case["block_size"],
                    token_ids=tokens,
                )
                assert result["ret"] == 0
                hit = result["hits"].get("rank-test", {})
                if hit.get("longest_matched") == expected:
                    break
                assert time.monotonic() < deadline, result
                time.sleep(0.02)
            assert hit["dp"] == {str(rank): expected}
            assert hit["device"] == expected
            assert "gpu" not in hit
            assert "npu" not in hit
            assert set(hit["rank_matches"]) == {str(rank)}
            assert hit["rank_matches"][str(rank)]["device"] == expected
            assert "gpu" not in hit["rank_matches"][str(rank)]
            assert "npu" not in hit["rank_matches"][str(rank)]
            request = Request(
                f"http://127.0.0.1:{http_port}/query",
                data=msgpack.packb(
                    {
                        "model": "rank-model",
                        "block_size": case["block_size"],
                        "token_ids": tokens,
                    }
                ),
                headers={"Content-Type": "application/msgpack"},
            )
            with urlopen(request, timeout=3) as response:
                http_result = msgpack.unpackb(response.read(), raw=False)
            # Both channels expose the same tier names, rank keys and token counts.
            assert result["hits"] == http_result["instances"]
            services = client.list_services()
            assert services["ret"] == 0
            assert services["count"] == 1
            assert services["services"][0]["InstanceID"] == "rank-test"
            assert services["services"][0]["DPRank"] == rank
            view = client.get_global_view()
            assert view["ret"] == 0
            assert view["context_count"] == 1
            assert view["contexts"][0]["instances"] == {"rank-test": {rank}}
            assert view["contexts"][0]["prefix_count"] == len(case["expected"])
            assert client.unregister("rank-test", dp_rank=rank) == 0
            assert client.list_services() == {"ret": 0, "count": 0, "services": []}
            assert client.unregister("rank-test", dp_rank=rank) == -2001
        finally:
            client.close()
