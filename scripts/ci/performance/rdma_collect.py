#!/usr/bin/env python3
"""Collect paired TENT DRAM measurements on an exclusively leased RDMA pair."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import shlex
import signal
import subprocess
import sys

from perf_gate import load_policy, number, policy_digest, report, require, write_json


def benchmark_digest(directory):
    fingerprint = hashlib.sha256()
    paths = sorted(Path(directory).rglob("*"))
    require(any(path.is_file() for path in paths), "missing benchmark source")
    for path in paths:
        if path.is_file():
            fingerprint.update(
                str(path.relative_to(directory)).encode() + b"\0" + path.read_bytes()
            )
    return fingerprint.hexdigest()


def validate_hosts(hosts):
    require(
        isinstance(hosts, list) and len(hosts) == 2, "exactly two hosts are required"
    )
    for host in hosts:
        require(
            re.fullmatch(
                r"[a-zA-Z0-9_][a-zA-Z0-9_.-]*@[a-zA-Z0-9][a-zA-Z0-9.-]*", host["ssh"]
            ),
            "invalid SSH destination",
        )
        ipaddress.IPv4Address(host["address"])
        require(re.fullmatch(r"[a-zA-Z0-9_]+", host["device"]), "invalid RDMA device")
        for key, minimum in (("numa", 0), ("port", 1), ("gid_index", 0)):
            require(type(host[key]) is int and host[key] >= minimum, f"invalid {key}")
    require(
        hosts[0]["address"] != hosts[1]["address"]
        and hosts[0]["ssh"] != hosts[1]["ssh"],
        "hosts must differ",
    )
    return hosts


def parse_sample(text, case, target, duration):
    records = [json.loads(line) for line in text.splitlines() if line.strip()]
    require(len(records) == 1, "expected one tebench JSONL record")
    row = records[0]
    expected = {
        "schema_version": 1,
        "record_type": "target_metrics",
        "backend": "tent",
        "op_type": case["op_type"],
        "block_size": case["block_size"],
        "batch_size": case["batch_size"],
        "num_threads": case["threads"],
    }
    require(
        all(row[key] == value for key, value in expected.items()),
        "benchmark workload mismatch",
    )
    require(len(row["targets"]) == 1, "expected exactly one RDMA target")
    peer = row["targets"][0]
    require(
        peer["segment_name"] == target and peer["threads"] == case["threads"],
        "target mismatch",
    )
    count = row["aggregate_operations"]
    require(
        type(count) is int and count > 0 and peer["operations"] == count,
        "invalid operation count",
    )
    require(
        row["aggregate_transferred_bytes"]
        == peer["transferred_bytes"]
        == count * case["block_size"] * case["batch_size"],
        "incomplete transferred bytes",
    )
    # target_metrics reports the average wall duration across its threads.
    require(
        number(peer["total_duration_us"])
        and peer["total_duration_us"] >= duration * 1e6,
        "measurement ended before the requested duration",
    )
    # tebench's *_gbps fields are decimal GB/s, not gigabits/s.
    sample = {
        "bandwidth_gbs": row["aggregate_throughput_gbps"],
        "p99_us": peer["p99_us"],
    }
    require(all(number(value) for value in sample.values()), "invalid RDMA measurement")
    return {key: sample[key] for key in case["metrics"]}


class Remote:
    def __init__(self, host, benchmark, output, index):
        self.destination = host["ssh"]
        self.options = [
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "ConnectTimeout=15",
        ]
        self.output = output / f"host-{index}"
        self.output.mkdir()
        self.root = None
        self.process = None
        self.stderr = (self.output / "ssh.log").open("w")
        try:
            self.root = self.ssh(
                ["mktemp", "-d", "/tmp/mooncake-rdma-perf.XXXXXXXX"]
            ).strip()
            require(
                re.fullmatch(r"/tmp/mooncake-rdma-perf\.[a-zA-Z0-9]+", self.root),
                "invalid remote work directory",
            )
            files = [
                str(Path(__file__).with_name(name))
                for name in ("rdma_worker.py", "perf_gate.py", "freeze_build.py")
            ]
            subprocess.run(
                [
                    "scp",
                    *self.options,
                    "-r",
                    *files,
                    str(benchmark),
                    f"{self.destination}:{self.root}/",
                ],
                check=True,
                timeout=120,
                stdout=self.stderr,
                stderr=self.stderr,
            )
            command = [
                "timeout",
                "--kill-after=15s",
                "2700s",
                "python3",
                "-u",
                self.root + "/rdma_worker.py",
                self.root,
            ]
            self.process = subprocess.Popen(
                [
                    "ssh",
                    *self.options,
                    "-o",
                    "ServerAliveInterval=15",
                    "-o",
                    "ServerAliveCountMax=2",
                    self.destination,
                    shlex.join(command),
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.stderr,
                text=True,
            )
        except BaseException:
            self.close()
            raise

    def ssh(self, command):
        return subprocess.check_output(
            ["ssh", *self.options, self.destination, shlex.join(command)],
            text=True,
            timeout=30,
            stderr=self.stderr,
        )

    def request(self, op, **args):
        self.process.stdin.write(json.dumps({"op": op, **args}) + "\n")
        self.process.stdin.flush()
        ready, _, _ = select.select(
            [self.process.stdout], [], [], 2400 if op == "prepare" else 90
        )
        require(ready, "SSH worker response timed out")
        response = json.loads(self.process.stdout.readline())
        require("error" not in response, response.get("error", "worker error"))
        return response["value"]

    def close(self):
        errors = []
        try:
            if self.process:
                self.process.stdin.close()
                try:
                    self.process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    self.process.terminate()
                    self.process.wait(timeout=10)
                    raise RuntimeError(
                        "SSH worker did not release its lease; remote watchdog bounds cleanup"
                    )
                if self.process.returncode != 0:
                    errors.append("SSH worker failed; see ssh.log")
            if self.root:
                try:
                    subprocess.run(
                        [
                            "scp",
                            *self.options,
                            "-r",
                            f"{self.destination}:{self.root}/logs",
                            str(self.output),
                        ],
                        check=True,
                        timeout=120,
                        stdout=self.stderr,
                        stderr=self.stderr,
                    )
                except (OSError, subprocess.SubprocessError):
                    errors.append("could not retrieve all remote logs")
                finally:
                    self.ssh(["rm", "-rf", "--", self.root])
        finally:
            self.stderr.close()
        require(not errors, "; ".join(errors))


def collect(args, remote_class=Remote):
    args.artifact_name = "T-One job"
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    policy = load_policy(args.policy)
    require(policy["suite"] == "rdma", "expected RDMA policy")
    hosts = validate_hosts(json.loads(Path(args.hosts).read_text()))
    for sha in (args.baseline_sha, args.candidate_sha, args.harness_sha):
        require(re.fullmatch(r"[0-9a-f]{40}", sha), "expected full commit SHA")
    benchmark = Path(args.benchmark).resolve()
    environment = {
        "hosts": [],
        "harness_sha": args.harness_sha,
        "benchmark_sha256": benchmark_digest(benchmark),
        "run_id": args.run_id,
        "host_config": hosts,
    }
    results = {
        side: {
            "schema_version": 1,
            "revision": getattr(args, side + "_sha"),
            "environment": environment,
            "policy_sha256": policy_digest(policy),
            "builds": [],
            "cases": {},
            "errors": [],
        }
        for side in ("baseline", "candidate")
    }
    remotes = []
    try:
        for index, host in enumerate(hosts):
            remotes.append(remote_class(host, benchmark, output, index))
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    remote.request,
                    "prepare",
                    host=host,
                    policy=policy,
                    baseline_sha=args.baseline_sha,
                    candidate_sha=args.candidate_sha,
                )
                for remote, host in zip(remotes, hosts)
            ]
            prepared = [future.result() for future in futures]
        environment["hosts"] = [item["environment"] for item in prepared]
        require(
            len({item["boot_id"] for item in environment["hosts"]}) == 2,
            "both SSH destinations resolve to one host",
        )
        for side in results:
            results[side]["builds"] = [item["builds"][side] for item in prepared]
        for name, case in policy["cases"].items():
            for repeat in range(policy["repetitions"]):
                for side in (
                    ("baseline", "candidate")
                    if repeat % 2 == 0
                    else ("candidate", "baseline")
                ):
                    tag = f"{name}-{repeat}-{side}"
                    print(tag, flush=True)
                    target = remotes[1].request("launch", side=side, tag=tag, case=name)
                    try:
                        if repeat == 0:
                            remotes[0].request(
                                "launch",
                                side=side,
                                tag=tag + "-verify",
                                case=name,
                                target=target,
                                verify=True,
                            )
                        text = remotes[0].request(
                            "launch", side=side, tag=tag, case=name, target=target
                        )
                        (output / (tag + ".jsonl")).write_text(text)
                        sample = parse_sample(
                            text, case, target, policy["arguments"]["duration"]
                        )
                        values = results[side]["cases"].setdefault(
                            name,
                            {
                                key: {
                                    "unit": policy["metrics"][key]["unit"],
                                    "samples": [],
                                }
                                for key in case["metrics"]
                            },
                        )
                        for key, value in sample.items():
                            values[key]["samples"].append(value)
                    finally:
                        remotes[1].request("stop")
        for index, remote in enumerate(remotes):
            require(
                remote.request("probe") == environment["hosts"][index],
                "RDMA environment changed during measurements",
            )
    except Exception as exc:
        for result in results.values():
            result["errors"].append(str(exc))
    finally:
        for remote in reversed(remotes):
            try:
                remote.close()
            except Exception as exc:
                for result in results.values():
                    result["errors"].append(f"cleanup: {exc}")
        for side, result in results.items():
            write_json(output / (side + ".json"), result)
        # T-One indexes .log files; this file contains JSON, never console text.
        write_json(output / "rdma-results.log", {"run_id": args.run_id, **results})
    args.baseline, args.candidate = (
        str(output / "baseline.json"),
        str(output / "candidate.json"),
    )
    return report(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "hosts",
        "benchmark",
        "baseline-sha",
        "candidate-sha",
        "harness-sha",
        "run-id",
        "output",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument(
        "--policy", default=str(Path(__file__).with_name("rdma-policy.json"))
    )
    parser.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY"))

    def interrupted(signum, frame):
        raise RuntimeError(f"interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    sys.exit(collect(parser.parse_args()))
