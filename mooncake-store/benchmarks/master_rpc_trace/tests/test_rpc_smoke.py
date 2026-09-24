# Copyright 2026 Alibaba Cloud and its affiliates
# Licensed under the Apache License, Version 2.0.
"""Opt-in integration checks against a fresh, local master for each test."""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recorder import RpcTraceWriter  # noqa: E402


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RpcSmokeTest(unittest.TestCase):
    master_binary = None
    replayer_binary = None

    def setUp(self):
        if self.master_binary is None or self.replayer_binary is None:
            self.skipTest("opt-in test: supply --master and --replayer")
        directory = tempfile.TemporaryDirectory(prefix="master-trace-test-")
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.env = dict(os.environ, MC_STORE_RPC_CLIENT_IO_THREADS="2")
        self.env.pop("MOONCAKE_CONFIG_PATH", None)
        self.port = unused_port()
        metrics_port = unused_port()
        while metrics_port == self.port:
            metrics_port = unused_port()
        log = open(self.directory / "master.log", "w+")
        self.addCleanup(log.close)
        self.master = subprocess.Popen(
            [
                str(self.master_binary),
                f"--rpc_port={self.port}",
                "--rpc_address=127.0.0.1",
                "--rpc_thread_num=2",
                f"--metrics_port={metrics_port}",
                "--metrics_host=127.0.0.1",
                "--enable_metric_reporting=false",
                "--default_kv_lease_ttl=0ms",
                "--logtostderr=1",
            ],
            cwd=self.directory,
            env=self.env,
            stdout=log,
            stderr=log,
        )
        self.addCleanup(self.stop_master)
        deadline = time.monotonic() + 20
        while self.master.poll() is None and time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.05)
        log.seek(0)
        self.fail("master did not start:\n" + log.read())

    def stop_master(self):
        self.master.terminate()
        try:
            self.master.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.master.kill()
            self.master.wait()

    def replay(self, trace, *, prefill=None, expected_code=0):
        summary = self.directory / "result.json"
        samples = self.directory / "samples.jsonl"
        command = [
            str(self.replayer_binary),
            f"--trace={trace}",
            f"--master_server=127.0.0.1:{self.port}",
            "--workers=4",
            f"--output={summary}",
            f"--samples={samples}",
            "--logtostderr=1",
        ]
        if prefill:
            command.append(f"--prefill_trace={prefill}")
        completed = subprocess.run(
            command,
            cwd=self.directory,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, expected_code, completed.stderr)
        result = json.loads(summary.read_text())
        rows = [json.loads(line) for line in samples.read_text().splitlines()]
        self.assertTrue(result["healthy_heartbeats"])
        for row in rows:
            self.assertLessEqual(row["scheduled_us"], row["start_us"])
            self.assertLessEqual(row["start_us"], row["finish_us"])
        return result, rows

    def test_example_cross_client_reuse(self):
        example = Path(__file__).resolve().parents[1] / "example.jsonl"
        events = [json.loads(line) for line in example.read_text().splitlines()]
        events[-1]["timestamp_us"] = 1_100_000  # Include a heartbeat sweep.
        trace = self.directory / "reuse.jsonl"
        trace.write_text("\n".join(json.dumps(event) for event in events) + "\n")
        result, rows = self.replay(trace)
        self.assertFalse(result["has_errors"])
        self.assertEqual(result["logical_clients"], 2)
        self.assertEqual(
            result["operations"]["BatchGetReplicaList"]["key_status"]["ok"], 2
        )
        self.assertEqual(result["operations"]["BatchExistKey"]["key_status"]["miss"], 1)
        self.assertEqual(len(rows), 4)
        self.assertGreaterEqual(result["heartbeat_calls_during_replay"], 2)

    def test_partial_put_after_prefill(self):
        prefill = self.directory / "prefill.jsonl"
        with RpcTraceWriter(prefill) as writer:
            start = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutStart",
                keys=["existing"],
                value_sizes=[4096],
            )
            writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutEnd",
                keys=["existing"],
                put_start=start,
            )
        trace = self.directory / "partial.jsonl"
        with RpcTraceWriter(trace) as writer:
            start = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutStart",
                keys=["existing", "new"],
                value_sizes=[4096, 4096],
            )
            end = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutEnd",
                keys=["existing", "new"],
                put_start=start,
            )
            writer.record(
                timestamp_us=0,
                client_id="b",
                op="BatchGetReplicaList",
                keys=["existing", "new"],
                depends_on=[end],
            )
        result, rows = self.replay(trace, prefill=prefill)
        self.assertFalse(result["has_errors"])
        self.assertEqual(rows[0]["key_status"]["already_exists"], 1)
        self.assertEqual(rows[1]["key_status"]["skipped"], 1)
        self.assertEqual(rows[1]["key_status"]["ok"], 1)
        self.assertEqual(rows[2]["key_status"]["ok"], 2)

    def test_revoke_and_remove(self):
        trace = self.directory / "remove.jsonl"
        with RpcTraceWriter(trace) as writer:
            start = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutStart",
                keys=["revoked"],
                value_sizes=[4096],
            )
            revoked = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutRevoke",
                keys=["revoked"],
                put_start=start,
            )
            start = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutStart",
                keys=["removed"],
                value_sizes=[4096],
            )
            end = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchPutEnd",
                keys=["removed"],
                put_start=start,
            )
            removed = writer.record(
                timestamp_us=0,
                client_id="a",
                op="BatchRemove",
                keys=["removed"],
                depends_on=[end],
            )
            writer.record(
                timestamp_us=0,
                client_id="b",
                op="BatchExistKey",
                keys=["revoked", "removed"],
                depends_on=[revoked, removed],
            )
        result, rows = self.replay(trace)
        self.assertFalse(result["has_errors"])
        self.assertEqual(rows[-1]["key_status"]["miss"], 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", type=Path, required=True)
    parser.add_argument("--replayer", type=Path, required=True)
    args = parser.parse_args()
    RpcSmokeTest.master_binary = args.master.resolve(strict=True)
    RpcSmokeTest.replayer_binary = args.replayer.resolve(strict=True)
    unittest.main(argv=[sys.argv[0]], verbosity=2)
