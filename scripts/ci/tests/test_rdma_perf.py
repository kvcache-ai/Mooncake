"""Protocol/fixture tests: these do not measure RDMA hardware performance."""

import argparse
import copy
from contextlib import redirect_stdout
import importlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

DIRECTORY = Path(__file__).resolve().parents[1] / "performance"
sys.path.insert(0, str(DIRECTORY))
gate = importlib.import_module("perf_gate")
collector = importlib.import_module("rdma_collect")
worker = importlib.import_module("rdma_worker")
tone = importlib.import_module("tone_rdma")

BASE, HEAD, HARNESS = "a" * 40, "b" * 40, "c" * 40
HOSTS = [
    {
        "ssh": f"benchmark@192.0.2.{i}",
        "address": f"192.0.2.{i}",
        "device": "erdma_0",
        "port": 1,
        "gid_index": 0,
        "numa": 0,
    }
    for i in (10, 11)
]


def native(case, target, bandwidth=20, latency=10):
    size = 100 * case["block_size"] * case["batch_size"]
    return {
        "schema_version": 1,
        "record_type": "target_metrics",
        "backend": "tent",
        "op_type": case["op_type"],
        "block_size": case["block_size"],
        "batch_size": case["batch_size"],
        "num_threads": case["threads"],
        "aggregate_operations": 100,
        "aggregate_transferred_bytes": size,
        "aggregate_throughput_gbps": bandwidth,
        "targets": [
            {
                "segment_name": target,
                "threads": case["threads"],
                "operations": 100,
                "transferred_bytes": size,
                "total_duration_us": 10e6,
                "p99_us": latency,
            }
        ],
    }


class FixtureRemote:
    calls = []
    failure = None
    bandwidth_ratio = 1

    def __init__(self, host, benchmark, output, index):
        self.index = index

    def request(self, op, **args):
        self.calls.append((self.index, op, args))
        if op == self.failure:
            raise RuntimeError("injected " + op + " failure")
        environment = {
            "hostname": f"host-{self.index}",
            "boot_id": f"boot-{self.index}",
        }
        if op == "prepare":
            self.policy = args["policy"]
            return {
                "environment": environment,
                "builds": {
                    side: {
                        "revision": args[side + "_sha"],
                        "binary_sha256": "d" * 64,
                        "build_config_sha256": "f" * 64,
                        "libraries": {"/lib/provider.so": "e" * 64},
                    }
                    for side in ("baseline", "candidate")
                },
            }
        if op == "probe":
            return environment
        if op == "launch":
            if "target" not in args:
                return "192.0.2.11:12345"
            ratio = self.bandwidth_ratio if args["side"] == "candidate" else 1
            return (
                json.dumps(
                    native(
                        self.policy["cases"][args["case"]],
                        args["target"],
                        bandwidth=20 * ratio,
                    )
                )
                + "\n"
            )
        return None

    def close(self):
        self.calls.append((self.index, "close", {}))
        if self.failure == "close":
            raise RuntimeError("injected cleanup failure")


class RDMAPerformanceTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.policy = gate.load_policy(DIRECTORY / "rdma-policy.json")
        self.hosts = self.root / "hosts.json"
        self.hosts.write_text(json.dumps(HOSTS))
        self.benchmark = self.root / "benchmark"
        self.benchmark.mkdir()
        (self.benchmark / "fixture.cpp").write_text("fixture")
        self.args = argparse.Namespace(
            policy=DIRECTORY / "rdma-policy.json",
            hosts=self.hosts,
            benchmark=self.benchmark,
            baseline_sha=BASE,
            candidate_sha=HEAD,
            harness_sha=HARNESS,
            run_id="123-1",
            output=self.root / "results",
            summary=self.root / "summary.md",
        )
        FixtureRemote.calls = []
        FixtureRemote.failure = None
        FixtureRemote.bandwidth_ratio = 1

    def collect(self):
        with redirect_stdout(io.StringIO()):
            return collector.collect(self.args, FixtureRemote)

    def bundle(self):
        self.assertEqual(self.collect(), 0)
        return json.loads((self.args.output / "rdma-results.log").read_text())

    def client_args(self):
        return argparse.Namespace(
            **{
                **vars(self.args),
                "output": self.root / "github",
                "workspace": "test",
                "project": "test",
                "template": "mooncake-rdma-perf",
                "result_origin": "https://logs.example.com",
                "timeout": 60,
            }
        )

    def run_client(self, bundle, state="success", **changes):
        client = Mock()
        client.api.side_effect = [{"id": 42}, {"job_state": state}]
        client.result.return_value = bundle
        args = self.client_args()
        for key, value in changes.items():
            setattr(args, key, value)
        with redirect_stdout(io.StringIO()):
            code = tone.run(args, client)
        return code, args, client

    def test_paired_collection_orders_versions_checks_integrity_and_preserves_evidence(
        self,
    ):
        self.assertEqual(self.collect(), 0)
        calls = FixtureRemote.calls
        launches = [
            args
            for index, op, args in calls
            if index == 0 and op == "launch" and not args.get("verify")
        ]
        self.assertEqual(
            [item["side"] for item in launches[:6]],
            ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"],
        )
        self.assertEqual(len(launches), 40)
        self.assertEqual(
            sum(args.get("verify", False) for _, op, args in calls if op == "launch"), 8
        )
        self.assertEqual(sum(op == "stop" for _, op, _ in calls), 40)
        self.assertEqual(sum(op == "close" for _, op, _ in calls), 2)
        summary = json.loads((self.args.output / "comparison.json").read_text())
        self.assertEqual(len(summary["rows"]), 4)
        self.assertEqual(summary["status"], "PASSED")
        self.assertEqual(len(list(self.args.output.glob("*.jsonl"))), 40)
        self.assertIn("GB/s", self.args.summary.read_text())

    def test_regression_fails_locally_and_in_github_with_a_table(self):
        FixtureRemote.bandwidth_ratio = 0.94
        self.assertEqual(self.collect(), 1)
        bundle = json.loads((self.args.output / "rdma-results.log").read_text())
        code, args, _ = self.run_client(bundle, state="fail")
        self.assertEqual(code, 1)
        summary = (args.output / "summary.md").read_text()
        self.assertIn("+6.00%", summary)
        self.assertIn("FAILED", summary)

    def test_collection_failures_close_both_hosts_and_write_error_reports(self):
        for failure in ("prepare", "launch", "stop", "probe", "close"):
            with self.subTest(failure=failure):
                FixtureRemote.failure = failure
                FixtureRemote.calls = []
                self.args.output = self.root / failure
                self.assertEqual(self.collect(), 2)
                self.assertEqual(
                    sum(op == "close" for _, op, _ in FixtureRemote.calls), 2
                )
                self.assertIn("ERROR", (self.args.output / "summary.md").read_text())
                self.assertTrue((self.args.output / "rdma-results.log").exists())

    def test_native_parser_rejects_wrong_backend_workload_target_truncated_or_invalid_data(
        self,
    ):
        case = self.policy["cases"]["read-1m"]
        good = native(case, "peer")
        edits = [
            lambda x: x.update(backend="classic"),
            lambda x: x.update(block_size=4096),
            lambda x: x.update(num_threads=1),
            lambda x: x.update(op_type="write"),
            lambda x: x.update(aggregate_transferred_bytes=1),
            lambda x: x.update(aggregate_operations=0),
            lambda x: x.update(aggregate_throughput_gbps=float("nan")),
            lambda x: x["targets"][0].update(segment_name="wrong"),
            lambda x: x["targets"][0].update(total_duration_us=1),
            lambda x: x.update(targets=[]),
        ]
        for edit in edits:
            bad = copy.deepcopy(good)
            edit(bad)
            with self.assertRaises((ValueError, KeyError)):
                collector.parse_sample(json.dumps(bad), case, "peer", 10)
        with self.assertRaises(ValueError):
            collector.parse_sample(
                json.dumps(good) + "\n" + json.dumps(good), case, "peer", 10
            )
        self.assertEqual(
            collector.parse_sample(json.dumps(good), case, "peer", 10),
            {"bandwidth_gbs": 20},
        )

    def test_hosts_validate_distinct_ips_and_reject_shell_injection(self):
        self.assertEqual(collector.validate_hosts(HOSTS), HOSTS)
        for key, value in (
            ("ssh", "-oProxyCommand=bad"),
            ("address", "not-an-ip"),
            ("device", "erdma_0;bad"),
            ("numa", -1),
            ("port", 0),
            ("gid_index", True),
        ):
            bad = copy.deepcopy(HOSTS)
            bad[0][key] = value
            with self.assertRaises(ValueError):
                collector.validate_hosts(bad)
        with self.assertRaises(ValueError):
            collector.validate_hosts([HOSTS[0], HOSTS[0]])

    def test_github_recomputes_verdict_and_rejects_stale_or_mismatched_provenance(self):
        bundle = self.bundle()
        code, _, client = self.run_client(bundle)
        self.assertEqual(code, 0)
        create = client.api.call_args_list[0]
        self.assertIn("HARNESS_SHA=" + HARNESS, create.kwargs["env_info"])
        edits = [
            lambda x: x.update(run_id="old"),
            lambda x: x["candidate"].update(revision=BASE),
            lambda x: x["candidate"]["environment"].update(harness_sha=HEAD),
            lambda x: x["candidate"].update(builds=[]),
            lambda x: x["candidate"]["builds"][0].update(build_config_sha256="0" * 64),
            lambda x: x["candidate"]["builds"][0].pop("build_config_sha256"),
            lambda x: x["baseline"].update(errors="malformed errors"),
            lambda x: x["baseline"].update(errors=[{}]),
            lambda x: x["candidate"].update(policy_sha256="wrong"),
            lambda x: x["candidate"]["environment"].update(
                hosts=[{"boot_id": "same"}] * 2
            ),
            lambda x: x["candidate"]["cases"].pop("write-1m"),
        ]
        for index, edit in enumerate(edits):
            bad = copy.deepcopy(bundle)
            edit(bad)
            code, args, _ = self.run_client(bad, output=self.root / f"bad-{index}")
            self.assertEqual(code, 2)
            self.assertIn("ERROR", (args.output / "summary.md").read_text())
            self.assertEqual(
                json.loads((args.output / "comparison.json").read_text())["status"],
                "ERROR",
            )

    def test_infrastructure_failure_cannot_be_masked_by_passing_measurements(self):
        code, args, _ = self.run_client(self.bundle(), state="fail")
        self.assertEqual(code, 2)
        self.assertIn("execution failed", (args.output / "summary.md").read_text())

    def test_timeout_and_query_error_cancel_the_remote_job(self):
        for index, timeout in enumerate((0, 60)):
            args = self.client_args()
            args.timeout, args.output = timeout, self.root / f"timeout-{index}"
            client = Mock()
            client.api.side_effect = (
                [{"id": 42}, None]
                if timeout == 0
                else [{"id": 42}, RuntimeError("query failed"), None]
            )
            with redirect_stdout(io.StringIO()):
                self.assertEqual(tone.run(args, client), 2)
            self.assertEqual(client.api.call_args.args, ("stop",))
            self.assertEqual(client.api.call_args.kwargs, {"job_id": 42})
            self.assertTrue((args.output / "summary.md").exists())

    def test_missing_credentials_or_unknown_job_state_fail_closed(self):
        args = self.client_args()
        with patch.dict(
            "os.environ", {"TONE_USER_NAME": "", "TONE_USER_TOKEN": ""}
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(tone.run(args), 2)
        self.assertIn("credentials", (args.output / "summary.md").read_text())
        client = Mock()
        client.api.side_effect = [{"id": 42}, {"job_state": "unknown"}, None]
        args.output = self.root / "unknown"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(tone.run(args, client), 2)
        self.assertEqual(client.api.call_args.args, ("stop",))

    def test_log_download_contract_rejects_missing_duplicates_and_other_origins(self):
        client = tone.Tone.__new__(tone.Tone)
        client.api = Mock()
        client.read = Mock(return_value={"run_id": "123-1"})
        record = {
            "test_suite": "mooncake-rdma-perf",
            "logs": ["https://logs.example.com/job/rdma-results.log"],
        }
        client.api.return_value = [record]
        self.assertEqual(
            client.result(42, "https://logs.example.com"), {"run_id": "123-1"}
        )
        self.assertNotIn("Authorization", client.read.call_args.args[0].headers)
        for urls in (
            [],
            ["https://other.example.com/rdma-results.log"],
            [
                "https://logs.example.com/one/rdma-results.log",
                "https://logs.example.com/two/rdma-results.log",
            ],
        ):
            client.api.return_value = [{**record, "logs": urls}]
            with self.assertRaises(ValueError):
                client.result(42, "https://logs.example.com")

    def test_tone_parse_reports_missing_results_as_fail(self):
        suite = DIRECTORY / "tone/mooncake-rdma-perf/run.sh"
        result = subprocess.run(
            ["bash", "-c", 'source "$1"; parse', "bash", str(suite)],
            env={
                "PATH": "/usr/bin:/bin",
                "TONE_BM_RUN_DIR": str(self.root),
                "RDMA_RUN_ID": "absent",
                "TONE_CURRENT_RESULT_DIR": str(self.root),
            },
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("rdma_perf: Fail", result.stdout)

    def test_worker_process_group_is_stopped(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
        )
        self.addCleanup(worker.stop, process)
        worker.stop(process)
        self.assertIsNotNone(process.returncode)

    def test_process_group_cleanup_reaches_descendants(self):
        marker = self.root / "stopped"
        ready = self.root / "ready"
        child = f"""import signal, time
from pathlib import Path
def terminate(signum, frame):
    Path({str(marker)!r}).write_text("stopped")
    raise SystemExit(0)
signal.signal(signal.SIGTERM, terminate)
Path({str(ready)!r}).write_text("ready")
while True:
    time.sleep(1)
"""
        parent = f"""import signal, subprocess, sys
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c", {child!r}])
child.wait()
"""
        process = subprocess.Popen(
            [sys.executable, "-c", parent], start_new_session=True
        )
        self.addCleanup(worker.stop, process)
        for _ in range(100):
            if ready.exists():
                break
            time.sleep(0.01)
        self.assertTrue(ready.exists(), "child did not become ready")
        worker.stop(process)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(marker.read_text(), "stopped")

    def test_worker_launch_pins_rdma_and_numa_and_manages_target_lifetime(self):
        remote = self.root / "remote"
        remote.mkdir()
        with patch.dict(
            "os.environ",
            {"TONE_USER_TOKEN": "private-fixture-token", "MC_FORCE_TCP": "1"},
        ):
            instance = worker.Worker(remote)
        instance.host, instance.policy = HOSTS[0], self.policy
        shim = self.root / "bin"
        shim.mkdir()
        numactl = shim / "numactl"
        numactl.write_text('#!/bin/sh\nshift\nshift\nexec "$@"\n')
        numactl.chmod(0o755)
        timeout = shim / "timeout"
        timeout.write_text(
            '#!/bin/sh\nwhile [ "${1#-}" != "$1" ]; do shift; done\nshift\nexec "$@"\n'
        )
        timeout.chmod(0o755)
        instance.env["PATH"] = str(shim) + ":/usr/bin:/bin"
        binary = remote / "baseline/build/mooncake-transfer-engine/benchmark/tebench"
        binary.parent.mkdir(parents=True)
        binary.write_text(
            f"#!{sys.executable}\n"
            + """
import json, os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
flags = dict(arg[2:].split("=", 1) for arg in sys.argv[1:])
if "target_seg_name" not in flags:
    print("./tebench --target_seg_name=192.0.2.10:12345 --seg_type=DRAM", flush=True)
    time.sleep(60)
else:
    pathlib.Path(flags["result_output_jsonl"]).write_text(json.dumps({"flags": flags, "env": dict(os.environ)}))
"""
        )
        binary.chmod(0o755)
        self.addCleanup(worker.stop, instance.target)
        request = {"side": "baseline", "tag": "fixture", "case": "read-4k"}
        try:
            target = instance.launch(request)
            self.assertEqual(target, "192.0.2.10:12345")
            result = json.loads(
                instance.launch({**request, "tag": "client", "target": target})
            )
            self.assertEqual(result["flags"]["xport_type"], "rdma")
            self.assertEqual(result["flags"]["tent_transport_hint"], "rdma")
            self.assertEqual(result["flags"]["backend"], "tent")
            self.assertEqual(result["flags"]["dram_numa_node"], "0")
            self.assertEqual(result["flags"]["start_block_size"], "4096")
            self.assertEqual(result["flags"]["max_block_size"], "4096")
            self.assertEqual(result["env"]["MC_TE_FILTERS"], "erdma_0")
            self.assertEqual(
                json.loads(result["env"]["MC_TENT_CONF"]),
                {"rpc_server_hostname": "192.0.2.10"},
            )
            self.assertNotIn("TONE_USER_TOKEN", result["env"])
            self.assertNotIn("MC_FORCE_TCP", result["env"])
            pid = instance.target
            instance.handle({"op": "stop"})
            self.assertIsNotNone(pid.returncode)
            self.assertIsNone(instance.target)
        finally:
            worker.stop(instance.target)

    def test_worker_lease_excludes_another_job_and_releases_on_eof(self):
        import os

        first_root, second_root = self.root / "lease-first", self.root / "lease-second"
        first_root.mkdir()
        second_root.mkdir()
        env = {**os.environ, "HOME": str(self.root)}
        command = [sys.executable, str(DIRECTORY / "rdma_worker.py")]
        first = subprocess.Popen(
            command + [str(first_root)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        try:
            deadline = time.monotonic() + 5
            while not (first_root / "logs").exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue((first_root / "logs").exists())
            second = subprocess.run(
                command + [str(second_root)],
                input="",
                text=True,
                capture_output=True,
                env=env,
                timeout=5,
            )
            self.assertNotEqual(second.returncode, 0)
            first.communicate(timeout=5)
            self.assertEqual(first.returncode, 0)
            released = subprocess.run(
                command + [str(second_root)],
                input="",
                text=True,
                capture_output=True,
                env=env,
                timeout=5,
            )
            self.assertEqual(released.returncode, 0, released.stderr)
        finally:
            if first.poll() is None:
                first.terminate()
                first.communicate(timeout=5)

    def test_tone_retry_cannot_republish_a_stale_passing_envelope(self):
        import os

        result_dir, publish_dir = self.root / "results-retry", self.root / "publish"
        result_dir.mkdir()
        publish_dir.mkdir()
        (result_dir / "comparison.json").write_text('{"status":"PASSED"}')
        (result_dir / "rdma-results.log").write_text('{"run_id":"retry"}')
        (self.root / "rdma-exit-retry").write_text("2")
        result = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; parse',
                "bash",
                str(DIRECTORY / "tone/mooncake-rdma-perf/run.sh"),
            ],
            env={
                **os.environ,
                "TONE_BM_RUN_DIR": str(self.root),
                "RDMA_RUN_ID": "retry",
                "TONE_CURRENT_RESULT_DIR": str(publish_dir),
            },
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("rdma_perf: Fail", result.stdout)
        self.assertFalse((publish_dir / "rdma-results.log").exists())

    def test_http_error_redacts_signature_and_disallows_redirects(self):
        client = tone.Tone.__new__(tone.Tone)
        client.username, client.token = "fixture", "do-not-print-this"
        client.opener = Mock()
        client.opener.open.side_effect = tone.urllib.error.URLError(
            "url with do-not-print-this"
        )
        with self.assertRaisesRegex(RuntimeError, "^T-One HTTP request failed$"):
            client.api("stop", job_id=42)
        request = client.opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertIn("signature=", request.full_url)
        self.assertIsNone(
            tone.NoRedirect().redirect_request(
                None, None, 302, None, None, "https://other.example.com"
            )
        )


if __name__ == "__main__":
    unittest.main()
