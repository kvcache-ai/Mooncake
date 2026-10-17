#!/usr/bin/env python3
"""Bounded SSH worker. One lease owns one host for the entire paired run."""

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import subprocess
import sys
import time

from perf_gate import require


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stop(process):
    if process is not None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        # Reap descendants even if their parent exited before handling SIGTERM.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


class Worker:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.logs = self.root / "logs"
        self.logs.mkdir()
        self.target = None
        self.active = None
        # Do not pass T-One credentials or inherited transport overrides to PR code.
        self.env = {
            key: os.environ[key]
            for key in ("PATH", "HOME", "LANG")
            if key in os.environ
        }

    def command(self, argv, log, timeout=900, cwd=None, capture=False, env=None):
        with (self.logs / log).open("a") as output:
            self.active = subprocess.Popen(
                ["timeout", "--kill-after=10s", f"{timeout}s", *argv],
                cwd=cwd,
                env=env or self.env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE if capture else output,
                stderr=output,
                text=True,
                start_new_session=True,
            )
            try:
                stdout, _ = self.active.communicate(timeout=timeout + 15)
                require(self.active.returncode == 0, f"command failed; see {log}")
                return stdout
            finally:
                stop(self.active)
                self.active = None

    def probe(self, host, policy):
        device = Path("/sys/class/infiniband") / host["device"]
        require(device.exists(), "configured RDMA device is absent")
        require(
            "/virtual/" not in str(device.resolve()),
            "software RDMA is not a performance runner",
        )
        port = device / "ports" / str(host["port"])
        require(
            (port / "state").read_text().strip().startswith("4:"),
            "RDMA port is not ACTIVE",
        )
        node = Path(f"/sys/devices/system/node/node{host['numa']}")
        cpus = set()
        for item in (node / "cpulist").read_text().strip().split(","):
            ends = [int(x) for x in item.split("-")]
            cpus.update(range(ends[0], ends[-1] + 1))
        cpus &= os.sched_getaffinity(0)
        require(
            len(cpus) >= policy["cpu_count"], "NUMA node has too few available CPUs"
        )
        soft, _ = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        require(
            soft == resource.RLIM_INFINITY
            or soft >= 2 * policy["arguments"]["total_buffer_size"],
            "memlock limit is too small",
        )
        info = self.command(
            ["ibv_devinfo", "-d", host["device"], "-i", str(host["port"])],
            "preflight.log",
            timeout=15,
            capture=True,
        )
        cpuinfo = {
            key.strip(): value.strip()
            for line in Path("/proc/cpuinfo").read_text().split("\n\n")[0].splitlines()
            if ":" in line
            for key, value in [line.split(":", 1)]
            if key.strip() in ("model name", "vendor_id", "microcode", "flags")
        }
        gid = (port / "gids" / str(host["gid_index"])).read_text().strip()
        require(gid.replace(":", "").strip("0"), "configured GID is empty")
        return {
            "hostname": platform.node(),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "platform": platform.platform(),
            "cpuinfo": cpuinfo,
            "cpus": sorted(cpus),
            "numa": host["numa"],
            "device": host["device"],
            "device_path": str(device.resolve()),
            "port": host["port"],
            "gid_index": host["gid_index"],
            "gid": gid,
            "rate": (port / "rate").read_text().strip(),
            "ibv_devinfo": info,
            "compiler": self.command(
                ["c++", "--version"], "preflight.log", capture=True
            ).splitlines()[0],
        }

    def prepare(self, request):
        self.host, self.policy = request["host"], request["policy"]
        before = self.probe(self.host, self.policy)
        builds = {}
        for side in ("baseline", "candidate"):
            sha = request[side + "_sha"]
            require(re.fullmatch(r"[0-9a-f]{40}", sha), "invalid revision")
            source = self.root / side
            source.mkdir()
            log = side + "-build.log"
            for argv in (
                ["git", "init", "-q"],
                [
                    "git",
                    "remote",
                    "add",
                    "origin",
                    "https://github.com/kvcache-ai/Mooncake.git",
                ],
                ["git", "fetch", "--depth=1", "origin", sha],
                ["git", "checkout", "--detach", "FETCH_HEAD"],
                ["git", "submodule", "update", "--init", "--recursive", "--depth=1"],
            ):
                self.command(argv, log, cwd=source)
            actual = self.command(
                ["git", "rev-parse", "HEAD"], log, cwd=source, capture=True
            ).strip()
            require(actual == sha, "checkout SHA mismatch")
            # Both revisions use the benchmark from the reviewed harness bundle.
            benchmark = source / "mooncake-transfer-engine/benchmark"
            shutil.rmtree(benchmark)
            shutil.copytree(self.root / "benchmark", benchmark)
            self.command(
                [
                    "cmake",
                    "-S",
                    str(source),
                    "-B",
                    str(source / "build"),
                    "-G",
                    "Ninja",
                    "-DCMAKE_BUILD_TYPE=Release",
                    "-DCMAKE_CXX_COMPILER=c++",
                    "-DUSE_TENT=ON",
                    "-DUSE_CUDA=OFF",
                    "-DUSE_ETCD=OFF",
                    "-DWITH_STORE=OFF",
                    "-DWITH_STORE_RUST=OFF",
                    "-DBUILD_UNIT_TESTS=OFF",
                    "-DBUILD_EXAMPLES=OFF",
                    "-DBUILD_BENCHMARK=ON",
                ],
                log,
            )
            self.command(
                [
                    "cmake",
                    "--build",
                    str(source / "build"),
                    "--parallel",
                    str(self.policy["cpu_count"]),
                    "--target",
                    "tebench",
                ],
                log,
            )
            binary = source / "build/mooncake-transfer-engine/benchmark/tebench"
            linked = self.command(["ldd", str(binary)], log, capture=True)
            require("not found" not in linked, "missing benchmark shared library")
            libraries = {
                path: digest(path)
                for path in re.findall(r"(?:=>\s+|^\s*)(/\S+)", linked, re.M)
            }
            builds[side] = {
                "revision": sha,
                "binary_sha256": digest(binary),
                "libraries": libraries,
            }
        require(
            before == self.probe(self.host, self.policy), "host changed during build"
        )
        self.builds = builds
        return {"environment": before, "builds": builds}

    def launch(self, request):
        side, tag = request["side"], request["tag"]
        require(
            side in ("baseline", "candidate") and re.fullmatch(r"[a-z0-9_-]+", tag),
            "invalid invocation",
        )
        case = self.policy["cases"][request["case"]]
        host = self.host
        flags = {
            "backend": "tent",
            "xport_type": "rdma",
            "tent_transport_hint": "rdma",
            "seg_type": "DRAM",
            "dram_numa_node": host["numa"],
            "metadata_type": "p2p",
            "seg_name": host["address"],
            "op_type": case["op_type"],
            **self.policy["arguments"],
        }
        for key in ("block_size", "batch_size"):
            flags["start_" + key] = flags["max_" + key] = case[key]
        flags["start_num_threads"] = flags["max_num_threads"] = case["threads"]
        env = {
            **self.env,
            "MC_TENT_CONF": json.dumps({"rpc_server_hostname": host["address"]}),
            "MC_TE_FILTERS": host["device"],
            "MC_IB_PORT": str(host["port"]),
            "MC_GID_INDEX": str(host["gid_index"]),
        }
        # A separate integrity smoke is excluded from performance samples.
        if request.get("verify"):
            flags.update(check_consistency="true", duration=1)
        output = self.logs / (tag + ".jsonl")
        require(not output.exists(), "refusing stale benchmark output")
        if request.get("target"):
            flags.update(
                target_seg_name=request["target"], result_output_jsonl=str(output)
            )
        argv = [
            "numactl",
            f"--cpunodebind={host['numa']}",
            f"--membind={host['numa']}",
            str(self.root / side / "build/mooncake-transfer-engine/benchmark/tebench"),
        ]
        argv += [f"--{key}={value}" for key, value in flags.items()]
        log = tag + ".log"
        if request.get("target"):
            self.command(argv, log, timeout=self.policy["timeout_seconds"], env=env)
            return output.read_text()
        require(self.target is None, "a target is already running")
        with (self.logs / log).open("w") as stream:
            self.target = subprocess.Popen(
                ["timeout", "--preserve-status", "--kill-after=10s", "180s", *argv],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stream,
                stderr=stream,
                start_new_session=True,
            )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            require(self.target.poll() is None, "RDMA target exited before readiness")
            text = (self.logs / log).read_text()
            match = re.search(r"--target_seg_name=([^\s\x1b]+)", text)
            if match:
                require(
                    re.fullmatch(re.escape(host["address"]) + r":[0-9]+", match[1]),
                    "target advertised a different RPC address",
                )
                return match[1]
            time.sleep(0.1)
        raise TimeoutError("RDMA target readiness timed out")

    def handle(self, request):
        if request["op"] == "prepare":
            return self.prepare(request)
        if request["op"] == "launch":
            return self.launch(request)
        if request["op"] == "probe":
            for side, build in self.builds.items():
                binary = (
                    self.root
                    / side
                    / "build/mooncake-transfer-engine/benchmark/tebench"
                )
                require(
                    digest(binary) == build["binary_sha256"],
                    "benchmark binary changed during run",
                )
                require(
                    all(
                        digest(path) == value
                        for path, value in build["libraries"].items()
                    ),
                    "linked library changed during run",
                )
            return self.probe(self.host, self.policy)
        if request["op"] == "stop":
            require(
                self.target is not None and self.target.poll() is None,
                "RDMA target died during sample",
            )
            stop(self.target)
            require(
                self.target.returncode == 0, "RDMA target did not shut down cleanly"
            )
            self.target = None
            return None
        raise ValueError("unknown worker operation")


def serve(root):
    import fcntl

    worker = None
    # Stable path across runs. All templates using this pair must use the same account.
    lock = Path.home() / ".mooncake-rdma-perf.lock"
    with lock.open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        worker = Worker(root)
        try:
            for line in sys.stdin:
                try:
                    result = {"value": worker.handle(json.loads(line))}
                except Exception as exc:
                    result = {"error": str(exc)}
                print(json.dumps(result, allow_nan=False), flush=True)
                if "error" in result:
                    break
        finally:
            stop(worker.active)
            stop(worker.target)


if __name__ == "__main__":

    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    serve(sys.argv[1])
