"""Exercise the already-built, non-HA master on IPv4 loopback."""

import argparse
import json
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import closing, contextmanager
from http.client import HTTPConnection, HTTPException
from pathlib import Path

HOST = "127.0.0.1"
TIMEOUT = 15


@contextmanager
def run_master(command, directory):
    log = directory / "master.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            command, cwd=directory, stdout=output, stderr=subprocess.STDOUT
        )
        try:
            yield process, log
        except Exception:
            print(log.read_text(errors="replace"), file=sys.stderr)
            raise
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=TIMEOUT)


def wait_until_ready(process, http_port, rpc_port):
    deadline = time.monotonic() + TIMEOUT
    last_error = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"master exited before readiness ({process.returncode})")
        probe = f"HTTP /health on {http_port}"
        try:
            with closing(HTTPConnection(HOST, http_port, timeout=1)) as http:
                http.request("GET", "/health")
                response = http.getresponse()
                if response.status != 200:
                    raise ValueError(f"HTTP {response.status}")
                health = json.load(response)
                if not (
                    isinstance(health, dict)
                    and health.get("status") == "ok"
                    and health.get("service_ready") is True
                ):
                    raise ValueError(f"master not serving: {health!r}")
            probe = f"RPC port {rpc_port}"
            with socket.create_connection((HOST, rpc_port), timeout=1):
                return
        except (OSError, HTTPException, ValueError) as error:
            last_error = f"{probe}: {error}"
            time.sleep(0.1)
    raise RuntimeError(f"master did not become ready: {last_error}")


def expect_startup_failure(command, directory, scenario, diagnostic):
    with run_master(command, directory) as (process, log):
        code = process.wait(timeout=TIMEOUT)
        if code <= 0 or diagnostic not in log.read_text(errors="replace"):
            raise RuntimeError(f"{scenario}: expected '{diagnostic}', got exit {code}")
    print(f"PASS: {scenario}")


def smoke(binary, directory):
    # Reserve both ports together so the OS chooses distinct endpoints.
    with socket.create_server((HOST, 0)) as http, socket.create_server(
        (HOST, 0)
    ) as rpc:
        http_port = http.getsockname()[1]
        rpc_port = rpc.getsockname()[1]
    command = [
        binary,
        f"--metrics_host={HOST}",
        f"--metrics_port={http_port}",
        f"--rpc_address={HOST}",
        f"--rpc_port={rpc_port}",
        "--rpc_thread_num=1",
        "--logtostderr",
    ]
    # A failed RPC bind need not emit a diagnostic on stderr. Check that the
    # admin listener started, then retry the same command with both ports free.
    for port, scenario, diagnostic in (
        (
            http_port,
            "occupied HTTP port",
            f"Failed to start master admin server on {HOST}:{http_port}",
        ),
        (
            rpc_port,
            "occupied RPC port",
            f"Master admin server started on port {http_port}",
        ),
    ):
        with socket.create_server((HOST, port)):
            expect_startup_failure(command, directory, scenario, diagnostic)

    with run_master(command, directory) as (process, _):
        wait_until_ready(process, http_port, rpc_port)
        if process.poll() is not None:
            raise RuntimeError(f"master exited before SIGTERM ({process.returncode})")
        process.terminate()  # POSIX SIGTERM
        code = process.wait(timeout=TIMEOUT)
        if code != 0:
            raise RuntimeError(f"master exited with {code} after SIGTERM")
    # A new listener on each address proves both ports are immediately reusable.
    with socket.create_server((HOST, http_port)), socket.create_server(
        (HOST, rpc_port)
    ):
        pass
    print("PASS: HTTP health, RPC readiness, SIGTERM exit 0 and both ports reusable")

    config = directory / "invalid.json"
    config.write_text('{"client_active_ttl_sec": 0}\n')
    expect_startup_failure(
        [*command, f"--config_path={config}"],
        directory,
        "invalid client liveness configuration",
        "Client liveness TTLs must be positive",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path, help="path to mooncake_master")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as workdir:
        smoke(args.binary.resolve(strict=True), Path(workdir))
