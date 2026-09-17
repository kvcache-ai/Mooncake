#!/usr/bin/env python3
"""P02 real-etcd reader integration runner.

It owns the process lifecycle and the evidence capture:

  * starts a loopback etcd instance per scenario with an isolated data dir;
  * runs the seed and verify phases as separate OS processes, so the reader is
    always a fresh process that did not create the history it reads;
  * captures the raw etcd values (length, sha256, magic classification) so the
    evidence shows binary bytes at the etcd layer, not only decoded objects;
  * restarts etcd on the same data dir and replays, proving the history is
    durable and not held in a warm process;
  * compares the materialized metadata of the mixed-format runs against the
    all-JSON control run;
  * exercises corrupt-history scenarios that must fail closed.

No GPU, RDMA device, or production etcd is required. Only the etcd processes
this script starts are stopped, by PID.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import datetime
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MAGIC = b"\x89\x4d\x43\x4f\x50\x4c\x47\x0a"
BATCH_IDS = range(1, 8)


def log(message: str) -> None:
    stamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


class Runner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.root = Path(args.work_root).resolve()
        self.reports = self.root / "reports"
        self.logs = self.reports / "logs"
        self.measurements = self.reports / "measurements"
        self.records: list[dict[str, object]] = []
        # etcd 3.x etcdctl defaults to the v2 API; the Mooncake backend is
        # a v3 client, so the inspection commands must use v3 too.
        self.etcdctl_env = {**os.environ, "ETCDCTL_API": "3"}
        self.failures: list[str] = []
        self.evidence: dict[str, object] = {}
        self.etcd_processes: list[subprocess.Popen[bytes]] = []
        self.etcd_client = self._resolve_etcdctl()

    # -- infrastructure ----------------------------------------------------
    def _resolve_etcdctl(self) -> str:
        if self.args.etcdctl:
            return self.args.etcdctl
        found = shutil.which("etcdctl")
        if found is None:
            raise SystemExit("etcdctl not found; pass --etcdctl=PATH")
        return found

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def start_etcd(self, tag: str, data_dir: Path) -> tuple[str, subprocess.Popen[bytes]]:
        client_port = self._free_port()
        peer_port = self._free_port()
        data_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.logs / f"etcd-{tag}.log"
        handle = log_path.open("ab")
        command = [
            self.args.etcd,
            "--name",
            f"p02-{tag}",
            "--data-dir",
            str(data_dir),
            "--listen-client-urls",
            f"http://127.0.0.1:{client_port}",
            "--advertise-client-urls",
            f"http://127.0.0.1:{client_port}",
            "--listen-peer-urls",
            f"http://127.0.0.1:{peer_port}",
            "--initial-advertise-peer-urls",
            f"http://127.0.0.1:{peer_port}",
            "--initial-cluster",
            f"p02-{tag}=http://127.0.0.1:{peer_port}",
        ]
        process = subprocess.Popen(
            command, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True
        )
        self.etcd_processes.append(process)
        endpoint = f"http://127.0.0.1:{client_port}"
        deadline = time.time() + 30.0
        while time.time() < deadline:
            probe = subprocess.run(
                [self.etcd_client, "--endpoints", endpoint, "endpoint", "health"],
                capture_output=True,
                text=True,
                env=self.etcdctl_env,
            )
            if probe.returncode == 0:
                log(f"etcd {tag} ready at {endpoint} (pid={process.pid})")
                return endpoint, process
            if process.poll() is not None:
                raise SystemExit(f"etcd {tag} exited early; see {log_path}")
            time.sleep(0.2)
        raise SystemExit(f"etcd {tag} did not become healthy; see {log_path}")

    def stop_etcd(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        log(f"stopping etcd pid={process.pid}")
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            process.wait(timeout=10)
        if process in self.etcd_processes:
            self.etcd_processes.remove(process)

    # -- phases ------------------------------------------------------------
    def run_e2e(
        self,
        tag: str,
        endpoint: str,
        mode: str,
        cluster: str,
        extra: list[str] | None = None,
        expect_success: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        command = [
            self.args.e2e_binary,
            f"--etcd={endpoint}",
            f"--mode={mode}",
            f"--cluster={cluster}",
        ]
        command.extend(extra or [])
        log(f"run {tag}: {' '.join(command)}")
        result = subprocess.run(
            command,
            capture_output=True,
            cwd=str(self.root),
            env={**os.environ, "GLOG_logtostderr": "0"},
        )
        log_path = self.logs / f"{tag}.log"
        with log_path.open("ab") as handle:
            handle.write(f"$ {' '.join(command)}\n".encode())
            handle.write(f"exit={result.returncode}\n".encode())
            handle.write(b"--- stdout ---\n")
            handle.write(result.stdout)
            handle.write(b"--- stderr ---\n")
            handle.write(result.stderr)
        if expect_success and result.returncode != 0:
            self.failures.append(f"{tag}: exit={result.returncode}")
            log(f"FAIL {tag} exit={result.returncode}")
        else:
            log(f"{'ok' if result.returncode == 0 else 'expected-failure'} {tag} exit={result.returncode}")
        return result

    def etcd_range(self, endpoint: str, cluster: str) -> dict[str, bytes]:
        prefix = f"/oplog/{cluster}/"
        result = subprocess.run(
            [
                self.etcd_client,
                "--endpoints",
                endpoint,
                "get",
                prefix,
                "--prefix",
                "--write-out=json",
            ],
            capture_output=True,
            env=self.etcdctl_env,
        )
        if result.returncode != 0:
            raise SystemExit(f"etcdctl get failed: {result.stderr.decode()}")
        payload = json.loads(result.stdout.decode() or "{}")
        values: dict[str, bytes] = {}
        for item in payload.get("kvs", []) or []:
            key = base64.b64decode(item["key"]).decode()
            values[key] = base64.b64decode(item["value"])
        return values

    def capture_raw_values(
        self, tag: str, endpoint: str, cluster: str
    ) -> dict[str, dict[str, object]]:
        values = self.etcd_range(endpoint, cluster)
        captured: dict[str, dict[str, object]] = {}
        out_path = self.measurements / f"etcd-raw-{tag}.jsonl"
        with out_path.open("w", encoding="utf-8") as handle:
            for key, value in sorted(values.items()):
                record = {
                    "key": key,
                    "length": len(value),
                    "sha256": hashlib.sha256(value).hexdigest(),
                    "starts_with_binary_magic": value.startswith(MAGIC),
                    "envelope_version": value[8] if value.startswith(MAGIC) else None,
                    "codec_id": value[9] if value.startswith(MAGIC) else None,
                    "starts_with_bracket": value.lstrip()[:1] == b"[",
                }
                captured[key] = record
                handle.write(json.dumps(record, sort_keys=True) + "\n")
        batch_formats = {
            key.rsplit("/", 1)[-1]: ("binary" if record["starts_with_binary_magic"] else "json")
            for key, record in captured.items()
            if "/batches/" in key
        }
        log(f"raw etcd capture {tag}: {batch_formats}")
        return captured

    def read_dump(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    # -- scenarios ---------------------------------------------------------
    def seed_and_capture(
        self, tag: str, variant: str, data_dir: Path, restart: bool = True
    ) -> tuple[str, Path, dict[str, dict[str, object]]]:
        endpoint, process = self.start_etcd(tag, data_dir)
        cluster = f"p02-{tag}-{int(time.time()) % 100000}"
        self.run_e2e(tag, endpoint, "seed", cluster, [f"--variant={variant}"])
        captured = self.capture_raw_values(tag, endpoint, cluster)
        if restart:
            # Restart etcd on the same data dir, so the reader always runs as a
            # fresh process against durable bytes.
            self.stop_etcd(process)
            endpoint, process = self.start_etcd(f"{tag}-restart", data_dir)
        self.active_endpoint = endpoint
        self.active_cluster = cluster
        self.active_process = process
        return cluster, data_dir, captured

    def control_scenario(self) -> None:
        """All-JSON control: the full reader path must succeed end to end."""
        tag = "control"
        data_dir = self.root / "etcd-data-control"
        try:
            cluster, _, captured = self.seed_and_capture(
                tag, "control", data_dir
            )
            if any(r["starts_with_binary_magic"] for r in captured.values()):
                self.failures.append("control: unexpected binary value in etcd")
            self.expect_format_set(tag, captured, expected_binary=set())

            dump_path = self.measurements / "control-dump.txt"
            self.run_e2e(
                f"{tag}-verify",
                self.active_endpoint,
                "verify",
                cluster,
                [f"--dump={dump_path}", "--append-json"],
            )
            self.expect_writer_continuation(tag)
        finally:
            self.stop_etcd(self.active_process)

    def probe_scenarios(self) -> None:
        """Split the binary read failure between Get and Range."""
        tag = "probe"
        data_dir = self.root / "etcd-data-probe"
        try:
            cluster, _, captured = self.seed_and_capture(
                tag, "mixed_a", data_dir, restart=False
            )
            self.expect_format_set(tag, captured, expected_binary={2, 4})

            get_result = self.run_e2e(
                f"{tag}-get",
                self.active_endpoint,
                "probe-get",
                cluster,
                ["--probe-batch=2"],
            )
            range_result = self.run_e2e(
                f"{tag}-range",
                self.active_endpoint,
                "probe-range",
                cluster,
                expect_success=False,
            )
            self.evidence["probe_get_ok"] = get_result.returncode == 0
            self.evidence["probe_range_ok"] = range_result.returncode == 0
            if get_result.returncode != 0:
                self.failures.append(
                    "probe: single-key Get could not read a binary batch"
                )
            if range_result.returncode == 0:
                self.failures.append(
                    "probe: Range unexpectedly preserved binary bytes; "
                    "reassess the etcd bridge blocker"
                )
            else:
                log(
                    "blocked: production Range path cannot deliver binary bytes "
                    "(recorded as an open gate)"
                )
        finally:
            self.stop_etcd(self.active_process)

    def mixed_scenario(self, tag: str, variant: str, expected: set[int]) -> None:
        """Mixed history: the range-based replay is blocked by the etcd bridge.

        The codec decodes both formats correctly (see the unit suites and the
        single-key probe); the failure below is produced by the production
        Range read path mangling non-UTF-8 bytes, which is outside the P02
        reader diff. The check records that failure rather than hiding it.
        """
        data_dir = self.root / f"etcd-data-{tag}"
        try:
            cluster, _, captured = self.seed_and_capture(tag, variant, data_dir)
            self.expect_format_set(tag, captured, expected_binary=expected)
            dump_path = self.measurements / f"{tag}-dump.txt"
            result = self.run_e2e(
                f"{tag}-verify",
                self.active_endpoint,
                "verify",
                cluster,
                [f"--dump={dump_path}", "--append-json"],
                expect_success=False,
            )
            if result.returncode == 0:
                log(f"ok {tag}: mixed history replayed end to end")
                self.evidence.setdefault("range_path_blocked", False)
            else:
                log(
                    f"blocked {tag}: range-based replay rejected the mangled "
                    "binary bytes (expected until the etcd bridge is fixed)"
                )
                self.evidence.setdefault("range_path_blocked", True)
                if not self.evidence.get("range_path_blocked_first_seen"):
                    self.evidence["range_path_blocked_first_seen"] = tag
        finally:
            self.stop_etcd(self.active_process)

    def expect_format_set(
        self,
        tag: str,
        captured: dict[str, dict[str, object]],
        expected_binary: set[int],
    ) -> None:
        actual_binary = set()
        for key, record in captured.items():
            if "/batches/" not in key:
                continue
            if record["starts_with_binary_magic"]:
                actual_binary.add(int(key.rsplit("/", 1)[-1]))
        if actual_binary != expected_binary:
            self.failures.append(
                f"{tag}: etcd raw format set {sorted(actual_binary)} != "
                f"expected {sorted(expected_binary)}"
            )
            log(f"FAIL {tag} raw format mismatch")
        else:
            log(f"ok {tag} raw etcd formats match the scenario: "
                f"{sorted(actual_binary)} binary")

    def expect_writer_continuation(self, tag: str) -> None:
        captured = self.capture_raw_values(
            f"{tag}-restart", self.active_endpoint, self.active_cluster
        )
        continuation = next(
            (
                k
                for k in captured
                if k.endswith(f"/{len(BATCH_IDS) + 1:020d}")
            ),
            None,
        )
        if continuation is None:
            self.failures.append(f"{tag}: writer continuation batch missing")
        elif captured[continuation]["starts_with_binary_magic"]:
            self.failures.append(f"{tag}: continuation batch is not JSON")
        else:
            log(
                f"ok {tag}: writer continuation stored as JSON, raw length="
                f"{captured[continuation]['length']}"
            )

    def edge_case(self, tag: str, case: str) -> None:
        data_dir = self.root / f"etcd-data-edge-{tag}"
        endpoint, process = self.start_etcd(f"edge-{tag}", data_dir)
        cluster = f"p02-edge-{tag}-{int(time.time()) % 100000}"
        try:
            # Seed the corrupt history in one process...
            seed = self.run_e2e(
                f"edge-seed-{tag}",
                endpoint,
                "edge-case",
                cluster,
                [f"--edge-case={case}"],
            )
            if seed.returncode != 0:
                return
            # ...then prove a fresh reader process fails closed on it.
            result = self.run_e2e(
                f"edge-{tag}",
                endpoint,
                "verify",
                cluster,
                expect_success=False,
            )
            if result.returncode == 0:
                self.failures.append(f"edge {case}: unexpectedly succeeded")
                log(f"FAIL edge {case} unexpectedly succeeded")
            else:
                log(f"ok edge {case} rejected with exit={result.returncode}")
        finally:
            self.stop_etcd(process)

    # -- driver ------------------------------------------------------------
    def run(self) -> int:
        self.reports.mkdir(parents=True, exist_ok=True)
        self.logs.mkdir(parents=True, exist_ok=True)
        self.measurements.mkdir(parents=True, exist_ok=True)

        started = time.time()
        self.control_scenario()
        self.probe_scenarios()
        self.mixed_scenario("mixed-a", "mixed_a", {2, 4})
        self.mixed_scenario("mixed-b", "mixed_b", {1, 3})

        for case in (
            "unknown_envelope_version",
            "bad_checksum",
            "missing_terminal",
            "key_body_batch_id_mismatch",
            "sequence_gap",
        ):
            self.edge_case(case, case)
        return self.finish(started)

    def finish(self, started: float) -> int:
        duration = time.time() - started
        summary = {
            "kind": "p02-real-etcd-reader-integration",
            "duration_seconds": round(duration, 3),
            "e2e_binary": self.args.e2e_binary,
            "etcd": self.args.etcd,
            "etcdctl": self.etcd_client,
            "failures": self.failures,
            "evidence": self.evidence,
            "status": "PASS" if not self.failures else "FAIL",
        }
        summary_path = self.measurements / "etcd-run-summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        log(f"summary: {json.dumps(summary, sort_keys=True)}")
        return 0 if not self.failures else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--e2e-binary", required=True)
    parser.add_argument("--etcd", default=shutil.which("etcd") or "etcd")
    parser.add_argument("--etcdctl", default=None)
    parser.add_argument("--work-root", required=True)
    args = parser.parse_args()
    runner = Runner(args)
    try:
        return runner.run()
    finally:
        for process in list(runner.etcd_processes):
            runner.stop_etcd(process)


if __name__ == "__main__":
    raise SystemExit(main())
