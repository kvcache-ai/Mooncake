"""Isolated real-etcd correctness checks on the latest W02 asynchronous API."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--etcd", default=shutil.which("etcd"))
    parser.add_argument("--wrapper-dir", type=Path, required=True)
    args = parser.parse_args()
    if not args.etcd:
        parser.error("etcd is required")
    metadata = json.loads((args.build_dir / "build_metadata.json").read_text())
    args.output_dir.mkdir(parents=True, exist_ok=True)
    data = tempfile.mkdtemp(prefix="p01-w02-etcd-")
    client, peer = free_port(), free_port()
    while peer == client:
        peer = free_port()
    endpoint = f"http://127.0.0.1:{client}"
    peer_url = f"http://127.0.0.1:{peer}"
    env = dict(os.environ, P01_ETCD_ENDPOINT=endpoint)
    for variable in ("DYLD_LIBRARY_PATH", "LD_LIBRARY_PATH"):
        env[variable] = (
            str(args.wrapper_dir.resolve()) + os.pathsep + env.get(variable, "")
        )
    command = [
        args.etcd,
        "--name=p01",
        f"--data-dir={data}",
        f"--listen-client-urls={endpoint}",
        f"--advertise-client-urls={endpoint}",
        f"--listen-peer-urls={peer_url}",
        f"--initial-advertise-peer-urls={peer_url}",
        f"--initial-cluster=p01={peer_url}",
    ]
    server = None
    cases = []

    def stop():
        if server is not None and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)

    with (args.output_dir / "etcd-server.log").open("w") as log:

        def start():
            nonlocal server
            server = subprocess.Popen(
                command, env=env, stdout=log, stderr=subprocess.STDOUT
            )
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError("etcd exited; see etcd-server.log")
                try:
                    with urllib.request.urlopen(
                        endpoint + "/health", timeout=0.5
                    ) as response:
                        if response.status == 200:
                            return
                except (OSError, ValueError):
                    time.sleep(0.1)
            raise RuntimeError("etcd health timeout")

        try:
            start()
            for phase in ("persist", "restore-fence"):
                if phase == "restore-fence":
                    stop()
                    print("Restarting etcd on the same on-disk data", flush=True)
                    start()
                for codec in ("json", "json-control", "cbor", "msgpack"):
                    # A fresh process for every case: no in-memory writer/cache
                    # can hide missing bytes after the server restart.
                    result = subprocess.run(
                        [
                            str(args.build_dir.resolve() / "oplog_codec_etcd_e2e"),
                            codec,
                            phase,
                        ],
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        timeout=45,
                    )
                    (args.output_dir / f"{codec}-{phase}.log").write_text(result.stdout)
                    print(result.stdout, end="", flush=True)
                    cases.append(
                        {
                            "codec": codec,
                            "phase": phase,
                            "passed": result.returncode == 0,
                        }
                    )
                    result.check_returncode()
        finally:
            stop()
            (args.output_dir / "e2e-report.json").write_text(
                json.dumps(
                    {
                        "build": metadata,
                        "endpoint": endpoint,
                        "data_directory": data,
                        "cases": cases,
                        "workload": "typed_replay_trace",
                        "entries_per_case": 11,
                        "checks": [
                            "pending asynchronous durable future before transaction",
                            "reentrant durable continuation",
                            "all persisted batch bytes and acknowledged entries",
                            "ready restored-prefix future after server restart",
                            "stale-producer fencing without batch/prefix visibility",
                            "corrupted stored record rejection",
                            "production applier object/tenant/Segment/replica identity",
                            "duplicate replay idempotence and cursor",
                        ],
                        "all_passed": len(cases) == 8
                        and all(case["passed"] for case in cases),
                        "scope": "real-etcd storage and production applier semantic replay, not RPC/HA service E2E or performance",
                    },
                    indent=2,
                )
                + "\n"
            )


if __name__ == "__main__":
    main()
