# SPDX-License-Identifier: Apache-2.0
"""External process launcher for the fixed seven NIXL elastic plans.

This launcher owns only worker placement, rank replacement, and test-only
phase barriers. It does not add a runtime recovery protocol.
"""

import argparse
import json
import os
import shlex
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import rank_server
import store_group

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
DEV = REPO.parent
CONTROL_ENV = "MC_EP_ELASTIC_CONTROL_ADDR"
CONTROL_TIMEOUT_ENV = "MC_EP_ELASTIC_CONTROL_TIMEOUT_S"
TCP_STORE_PORT = 9999
RANK_SERVER_PORT = 10000
HCA_ALLOWLIST = "mlx5_1,mlx5_2,mlx5_3,mlx5_4"
PLAN_FILES = {
    "single_rank.json",
    "no_expansion.json",
    "single_expansion.json",
    "double_expansion.json",
    "single_expansion_16_ranks.json",
    "expansion_contraction.json",
    "expansion_fault_contraction.json",
}


@dataclass
class Worker:
    rank: int
    phase_started: int
    host: str
    process: subprocess.Popen
    log_path: Path
    pid_file: Optional[Path] = None
    is_local: bool = False
    mp_ordinal: Optional[int] = None
    remote_pid: Optional[int] = None
    task_pgid: Optional[int] = None
    hostname: Optional[str] = None
    local_rank: Optional[int] = None
    gpu_id: Optional[str] = None


def _is_local_target(host: str) -> bool:
    return host in {
        "local",
        "localhost",
        "127.0.0.1",
        socket.gethostname(),
        os.uname().nodename,
    }


def _parse_gpu_lists(spec: str, inherited: str, host_count: int):
    if spec.strip():
        host_specs = [part.strip() for part in spec.split(";")]
        if len(host_specs) != host_count:
            raise ValueError(
                f"--gpu-lists needs one semicolon-separated list for each of {host_count} hosts"
            )
    elif inherited.strip():
        host_specs = [inherited.strip()] * host_count
    else:
        raise ValueError(
            "set CUDA_VISIBLE_DEVICES or pass explicit --gpu-lists before starting workers"
        )

    result = []
    for host_index, host_spec in enumerate(host_specs):
        gpu_ids = [item.strip() for item in host_spec.split(",") if item.strip()]
        if not gpu_ids:
            raise ValueError(f"GPU list for host index {host_index} is empty")
        if len(set(gpu_ids)) != len(gpu_ids):
            raise ValueError(f"GPU list for host index {host_index} has duplicates")
        result.append(tuple(gpu_ids))
    return tuple(result)


class RunCoordinator:
    def __init__(self, phases):
        self.condition = threading.Condition()
        self.barriers = {}
        self.planned_faults = {
            (abs(rank), phase_index)
            for phase_index, phase in enumerate(phases)
            for rank in phase
            if rank < 0
        }
        self.expected_faults = set()
        self.fault_done = set()
        self.expected_fault_callback = None
        self.aborted = None

    def configure(self, kind, phase, ranks):
        key = (kind, phase)
        with self.condition:
            if key in self.barriers:
                raise RuntimeError(f"duplicate barrier configuration: {key}")
            self.barriers[key] = {
                "expected": set(ranks),
                "arrived": set(),
                "released": False,
            }
            self.condition.notify_all()

    def _state(self, kind, phase):
        key = (kind, phase)
        state = self.barriers.get(key)
        if state is None:
            raise RuntimeError(f"unknown phase barrier: {key}")
        return state

    def arrive_and_wait(self, kind, phase, rank):
        key = (kind, phase)
        with self.condition:
            state = self._state(kind, phase)
            if rank not in state["expected"]:
                raise RuntimeError(f"rank {rank} is not expected at {key}")
            if rank in state["arrived"]:
                raise RuntimeError(f"rank {rank} arrived twice at {key}")
            state["arrived"].add(rank)
            self.condition.notify_all()
            while not state["released"] and self.aborted is None:
                self.condition.wait()
            if self.aborted is not None:
                raise RuntimeError(self.aborted)

    def arrive_only(self, kind, phase, rank):
        key = (kind, phase)
        with self.condition:
            state = self._state(kind, phase)
            if rank not in state["expected"]:
                raise RuntimeError(f"rank {rank} is not expected at {key}")
            if rank in state["arrived"]:
                raise RuntimeError(f"rank {rank} arrived twice at {key}")
            state["arrived"].add(rank)
            self.condition.notify_all()

    def mark_expected_fault(self, rank, phase):
        key = (rank, phase)
        with self.condition:
            if key not in self.planned_faults:
                raise RuntimeError(f"unplanned SIGTERM fault marker: {key}")
            self.expected_faults.add(key)
            callback = self.expected_fault_callback
            if callback is not None:
                callback(rank, phase)
            self.condition.notify_all()

    def mark_fault_done(self, rank, phase):
        key = (rank, phase)
        with self.condition:
            if key not in self.expected_faults:
                raise RuntimeError(f"SIGTERM without a planned fault marker: {key}")
            self.arrive_only("done", phase, rank)
            self.fault_done.add(key)
            self.condition.notify_all()

    def wait_arrivals(self, kind, phase, timeout_s):
        key = (kind, phase)
        deadline = time.monotonic() + timeout_s
        with self.condition:
            state = self._state(kind, phase)
            while not state["expected"].issubset(state["arrived"]):
                if self.aborted is not None:
                    raise RuntimeError(self.aborted)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    missing = sorted(state["expected"] - state["arrived"])
                    raise TimeoutError(
                        f"phase barrier {key} timed out; missing ranks={missing}"
                    )
                self.condition.wait(remaining)

    def release(self, kind, phase):
        with self.condition:
            state = self._state(kind, phase)
            state["released"] = True
            self.condition.notify_all()

    def expected_faults_in_phase(self, phase):
        with self.condition:
            return sorted(
                rank
                for rank, fault_phase in self.expected_faults
                if fault_phase == phase
            )

    def abort(self, message):
        with self.condition:
            self.aborted = message
            self.condition.notify_all()


class ControlHandler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            request = json.loads(self.rfile.readline())
            op = request.get("op")
            coordinator = self.server.coordinator
            if op == "barrier":
                coordinator.arrive_and_wait(
                    request["barrier"], int(request["phase"]), int(request["rank"])
                )
            elif op == "expected_fault":
                coordinator.mark_expected_fault(
                    int(request["rank"]), int(request["phase"])
                )
            elif op == "fault_done":
                coordinator.mark_fault_done(int(request["rank"]), int(request["phase"]))
            else:
                raise ValueError(f"unknown control operation: {op}")
            self.wfile.write(b'{"ok":true}\n')
        except Exception as exc:
            payload = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
            self.wfile.write(payload + b"\n")


class ControlServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, coordinator):
        self.coordinator = coordinator
        super().__init__(address, ControlHandler)


class ElasticLauncher:
    def __init__(self, args, plan_path, phases, run_dir, coordinator):
        self.args = args
        self.plan_path = plan_path
        self.phases = phases
        self.run_dir = run_dir
        self.coordinator = coordinator
        self.workers = []
        self.current_by_rank = {}
        self.expected_signal_workers = set()
        coordinator.expected_fault_callback = self._record_expected_fault

    @staticmethod
    def phase_members(phase):
        # Negative IDs denote a worker expected to fault during this phase.
        return {abs(rank) for rank in phase}

    def _record_expected_fault(self, rank, phase):
        worker = self.current_by_rank.get(rank)
        if worker is None:
            raise RuntimeError(
                f"fault marker has no worker process: rank={rank}, phase={phase}"
            )
        self.expected_signal_workers.add(id(worker))

    def _host_index_for_rank(self, rank):
        # Stable round-robin placement spreads even four-rank plans over two hosts.
        return rank % len(self.args.worker_hosts)

    def _host_for_rank(self, rank):
        return self.args.worker_hosts[self._host_index_for_rank(rank)]

    def _gpu_ids_for_rank(self, rank):
        return self.args.gpu_lists[self._host_index_for_rank(rank)]

    def _worker_python_path(self):
        # Keep the caller's explicit PYTHONPATH first. Extra source paths are
        # opt-in, so installed runtimes remain the default.
        paths = [
            os.environ.get("PYTHONPATH", ""),
            self.args.worker_pythonpath,
        ]
        if self.args.backend == "nixl":
            paths.append(self.args.nixl_pythonpath)
        return os.pathsep.join(path for path in paths if path)

    def _worker_args(self):
        command = [
            str(HERE / "elastic.py"),
            "--plan",
            str(self.plan_path),
            "--backend",
            self.args.backend,
            "--format-set",
            self.args.format_set,
            "--num-processes",
            "1",
            "--num-tokens",
            str(self.args.num_tokens),
            "--num-experts-per-rank",
            str(self.args.num_experts_per_rank),
            "--hidden-dim",
            str(self.args.hidden_dim),
            "--num-topk",
            str(self.args.num_topk),
            "--timeout-ms",
            str(self.args.timeout_ms),
            "--tcp-server",
            self.args.advertise_address,
            "--validate-phase-failures",
        ]
        if self.args.kineto:
            command.append("--kineto")
        if self.args.disable_ll_nvlink:
            command.append("--disable-ll-nvlink")
        return command

    def _remote_command(self, rank, pid_file):
        command = self._worker_args()
        gpu_list = ",".join(self._gpu_ids_for_rank(rank))
        exports = [
            f"export {CONTROL_ENV}={shlex.quote(self.args.advertise_address + ':' + str(self.args.control_port))}",
            f"export {CONTROL_TIMEOUT_ENV}={shlex.quote(str(self.args.phase_timeout_s))}",
            f"export CUDA_VISIBLE_DEVICES={shlex.quote(gpu_list)}",
        ]
        python_path = self._worker_python_path()
        if python_path:
            exports.append(f"export PYTHONPATH={shlex.quote(python_path)}")
        if self.args.backend == "mooncake":
            exports.append(f"export MOONCAKE_EP_DEVICE_FILTER={HCA_ALLOWLIST}")
        else:
            # Forward only the NIXL runtime knobs; keep the HCA allowlist fixed below.
            for variable in (
                "NIXL_EP_DEVICE_MODE",
                "NIXL_EP_NUM_CHANNELS",
                "UCX_TLS",
                "NIXL_PLUGIN_DIR",
                "UCX_MEMTYPE_CACHE",
                "UCX_LOG_LEVEL",
            ):
                if variable in os.environ:
                    exports.append(
                        f"export {variable}={shlex.quote(os.environ[variable])}"
                    )
            exports.append("export UCX_NET_DEVICES=mlx5_1:1,mlx5_2:1,mlx5_3:1,mlx5_4:1")
        if self.args.native_lib_path:
            exports.append(
                "export LD_LIBRARY_PATH="
                + shlex.quote(self.args.native_lib_path)
                + ":${LD_LIBRARY_PATH:-}"
            )

        # The child shell starts a new remote session, writes its own PID on
        # shared storage, then execs Python without changing that PID.
        child = (
            f'echo "$$" > {shlex.quote(str(pid_file))}; '
            f'exec "$CONDA_PREFIX/bin/python" {shlex.join(command)}'
        )
        remote_lines = [
            f"source {shlex.quote(self.args.conda_sh)}",
            "conda activate myenv",
            *exports,
            f"setsid --wait bash -c {shlex.quote(child)}",
        ]
        return f"bash -lc {shlex.quote('; '.join(remote_lines))}"

    def _spawn(self, rank, phase):
        host = self._host_for_rank(rank)
        gpu_list = self._gpu_ids_for_rank(rank)
        worker_index = len(self.workers)
        log_path = self.run_dir / f"rank-{rank}-phase-{phase}-{worker_index}.log"
        pid_file = self.run_dir / f"rank-{rank}-phase-{phase}-{worker_index}.pid"
        command = self._worker_args()
        is_local = _is_local_target(host)
        if is_local:
            env = os.environ.copy()
            env[CONTROL_ENV] = f"{self.args.advertise_address}:{self.args.control_port}"
            env[CONTROL_TIMEOUT_ENV] = str(self.args.phase_timeout_s)
            env["CUDA_VISIBLE_DEVICES"] = ",".join(gpu_list)
            python_path = self._worker_python_path()
            if python_path:
                env["PYTHONPATH"] = python_path
            if self.args.backend == "mooncake":
                env["MOONCAKE_EP_DEVICE_FILTER"] = HCA_ALLOWLIST
            else:
                env["UCX_NET_DEVICES"] = "mlx5_1:1,mlx5_2:1,mlx5_3:1,mlx5_4:1"
            if self.args.native_lib_path:
                env["LD_LIBRARY_PATH"] = (
                    self.args.native_lib_path
                    + os.pathsep
                    + env.get("LD_LIBRARY_PATH", "")
                )
            argv = [sys.executable, *command]
        else:
            argv = [self.args.ssh, host, self._remote_command(rank, pid_file)]
            env = None

        with log_path.open("wb") as log:
            process = subprocess.Popen(
                argv,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        worker = Worker(
            rank=rank,
            phase_started=phase,
            host=host,
            process=process,
            log_path=log_path,
            pid_file=pid_file,
            is_local=is_local,
        )
        self.workers.append(worker)
        self.current_by_rank[rank] = worker
        if is_local:
            pid_file.write_text(f"{process.pid}\n")
        self._wait_registered(worker)
        print(
            f"P1_LAUNCH_RANK global_rank={rank} target_host={host} "
            f"worker_os_pid={worker.remote_pid} task_pgid={worker.task_pgid} "
            f"hostname={worker.hostname} local_rank={worker.local_rank} "
            f"gpu={worker.gpu_id} "
            f"launcher_pid={process.pid} log={log_path}",
            flush=True,
        )
        return worker

    def _wait_registered(self, worker):
        deadline = time.monotonic() + self.args.start_timeout_s
        prefix = "P1_WORKER_READY "
        while time.monotonic() < deadline:
            code = worker.process.poll()
            try:
                output = worker.log_path.read_text(errors="replace")
            except FileNotFoundError:
                output = ""
            ready = None
            for line in output.splitlines():
                if line.startswith(prefix):
                    ready = json.loads(line[len(prefix) :])
                    break
            if ready is not None:
                actual_rank = int(ready["global_rank"])
                if actual_rank != worker.rank:
                    raise RuntimeError(
                        f"rank server assigned {actual_rank}, requested {worker.rank}; log={worker.log_path}"
                    )
                worker.mp_ordinal = int(ready["mp_ordinal"])
                worker.remote_pid = int(ready["os_pid"])
                worker.hostname = str(ready["hostname"])
                worker.local_rank = int(ready["local_rank"])
                worker.gpu_id = str(ready["selected_gpu"])
                expected_gpu_ids = self._gpu_ids_for_rank(worker.rank)
                if ready["visible_devices"] != list(expected_gpu_ids):
                    raise RuntimeError(
                        f"rank {worker.rank} visible GPU map differs from launcher: "
                        f"worker={ready['visible_devices']} launcher={list(expected_gpu_ids)}"
                    )
                if worker.local_rank >= len(expected_gpu_ids):
                    raise RuntimeError(
                        f"rank {worker.rank} local_rank={worker.local_rank} exceeds visible GPU list"
                    )
                if worker.gpu_id != expected_gpu_ids[worker.local_rank]:
                    raise RuntimeError(
                        f"rank {worker.rank} selected GPU {worker.gpu_id}, expected "
                        f"{expected_gpu_ids[worker.local_rank]}"
                    )
                if worker.pid_file is None or not worker.pid_file.exists():
                    raise RuntimeError(
                        f"worker PID file is not visible at {worker.pid_file}; "
                        "multi-host artifact storage must be shared"
                    )
                try:
                    recorded_pid = int(worker.pid_file.read_text().strip())
                except (OSError, ValueError) as exc:
                    raise RuntimeError(
                        f"invalid worker PID file {worker.pid_file}"
                    ) from exc
                process_group_id = int(ready["process_group_id"])
                if recorded_pid != process_group_id:
                    raise RuntimeError(
                        f"task process-group PID file says {recorded_pid}, "
                        f"worker reports process group {process_group_id}"
                    )
                if worker.remote_pid != recorded_pid:
                    raise RuntimeError(
                        f"worker PID {worker.remote_pid} is not its task-group leader {recorded_pid}"
                    )
                worker.task_pgid = recorded_pid
                return
            if code is not None:
                raise RuntimeError(
                    f"worker rank {worker.rank} exited before registration: code={code}; log={worker.log_path}"
                )
            time.sleep(0.05)
        raise TimeoutError(
            f"worker rank {worker.rank} did not register within {self.args.start_timeout_s}s; log={worker.log_path}"
        )

    def _task_pgid(self, worker):
        if worker.task_pgid is not None:
            return worker.task_pgid
        if worker.pid_file is None:
            return None
        try:
            pid = int(worker.pid_file.read_text().strip())
        except (OSError, ValueError):
            return None
        return pid if pid > 1 else None

    def _signal_launcher_group(self, worker, signum):
        try:
            os.killpg(worker.process.pid, signum)
        except ProcessLookupError:
            pass

    def _signal_task_group(self, worker, signum):
        pid = self._task_pgid(worker)
        if worker.is_local:
            pid = pid or worker.process.pid
            try:
                os.killpg(pid, signum)
            except ProcessLookupError:
                pass
            return

        if pid is None:
            # Before the remote PID file exists, only the local SSH session is known.
            self._signal_launcher_group(worker, signum)
            return

        signal_name = signal.Signals(signum).name
        command = f"kill -{signal_name} -- -{pid} 2>/dev/null || true"
        try:
            subprocess.run(
                [self.args.ssh, worker.host, command],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=8.0,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(
                f"bounded remote cleanup failed for rank={worker.rank} "
                f"host={worker.host} pid={pid}: {type(exc).__name__}",
                file=sys.stderr,
                flush=True,
            )

    @staticmethod
    def _local_task_group_alive(pgid):
        try:
            entries = list(Path("/proc").iterdir())
        except OSError:
            return None
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                state = fields[0]
                process_group = int(fields[2])
            except (OSError, IndexError, ValueError):
                continue
            if process_group == pgid and state != "Z":
                return True
        return False

    def _remote_task_group_alive(self, worker):
        pid = self._task_pgid(worker)
        if pid is None:
            return False
        try:
            result = subprocess.run(
                [self.args.ssh, worker.host, f"kill -0 -- -{pid} 2>/dev/null"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=8.0,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(
                f"bounded remote liveness check failed for rank={worker.rank} "
                f"host={worker.host} pid={pid}: {type(exc).__name__}",
                file=sys.stderr,
                flush=True,
            )
            return None
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        print(
            f"remote liveness check inconclusive for rank={worker.rank} "
            f"host={worker.host} pid={pid}: ssh_exit={result.returncode}",
            file=sys.stderr,
            flush=True,
        )
        return None

    def _task_group_alive(self, worker):
        pid = self._task_pgid(worker)
        if pid is None:
            return False
        if worker.is_local:
            return self._local_task_group_alive(pid)
        return self._remote_task_group_alive(worker)

    def cleanup_workers(self):
        # Always signal a recorded task group, even if its SSH leader exited.
        # Planned 100us faults remain SIGTERM; SIGKILL is bounded cleanup only.
        for worker in self.workers:
            if self._task_pgid(worker) is not None:
                self._signal_task_group(worker, signal.SIGTERM)
            elif worker.process.poll() is None:
                self._signal_launcher_group(worker, signal.SIGTERM)

        deadline = time.monotonic() + 5.0
        for worker in self.workers:
            if worker.process.poll() is None:
                try:
                    worker.process.wait(timeout=max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass

        for worker in self.workers:
            alive = self._task_group_alive(worker)
            if alive is True:
                self._signal_task_group(worker, signal.SIGKILL)
            if worker.process.poll() is None:
                # The task group has had its targeted signal; close a stuck
                # local SSH client only after that bounded cleanup attempt.
                self._signal_launcher_group(worker, signal.SIGKILL)

        deadline = time.monotonic() + 5.0
        for worker in self.workers:
            if worker.process.poll() is None:
                try:
                    worker.process.wait(timeout=max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    pass

        remaining = []
        for worker in self.workers:
            group_alive = self._task_group_alive(worker)
            if worker.process.poll() is None or group_alive is not False:
                remaining.append(worker)
        return remaining

    def _is_expected_signal_exit(self, worker, code):
        if id(worker) not in self.expected_signal_workers:
            return False
        if worker.is_local:
            return code in (-signal.SIGTERM, 128 + signal.SIGTERM)
        # The fixed remote wrapper uses `setsid --wait`, which reports the
        # terminating signal number as a positive SSH exit status.
        return code == signal.SIGTERM

    def _wait_removed(self, ranks):
        deadline = time.monotonic() + self.args.phase_timeout_s
        for rank in sorted(ranks):
            worker = self.current_by_rank.get(rank)
            if worker is None:
                continue
            while worker.process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            code = worker.process.poll()
            if self._is_expected_signal_exit(worker, code):
                continue
            if code is None:
                raise TimeoutError(f"cleanly removed rank {rank} did not exit")
            if code != 0:
                raise RuntimeError(
                    f"cleanly removed rank {rank} exited with code {code}; log={worker.log_path}"
                )

    def _wait_expected_faults(self, phase):
        deadline = time.monotonic() + self.args.phase_timeout_s
        for rank in self.coordinator.expected_faults_in_phase(phase):
            worker = self.current_by_rank[rank]
            while worker.process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            code = worker.process.poll()
            if not self._is_expected_signal_exit(worker, code):
                raise RuntimeError(
                    f"planned fault rank {rank} phase {phase} returned {code}; log={worker.log_path}"
                )

    def _wait_all(self):
        failures = []
        for worker in self.workers:
            code = worker.process.wait(timeout=self.args.phase_timeout_s)
            if code == 0 or self._is_expected_signal_exit(worker, code):
                continue
            failures.append((worker.rank, code, str(worker.log_path)))
        if failures:
            raise RuntimeError(f"unexpected worker exits: {failures}")

    def run(self):
        members = [self.phase_members(phase) for phase in self.phases]
        for phase_index, phase_members in enumerate(members):
            self.coordinator.configure("ready", phase_index, phase_members)
            self.coordinator.configure("done", phase_index, phase_members)

        for rank in sorted(members[0]):
            self._spawn(rank, 0)
        self.coordinator.wait_arrivals("ready", 0, self.args.phase_timeout_s)
        self.coordinator.release("ready", 0)

        for phase in range(len(members)):
            self.coordinator.wait_arrivals("done", phase, self.args.phase_timeout_s)
            if phase == len(members) - 1:
                self.coordinator.release("done", phase)
                break

            self._wait_expected_faults(phase)
            target = members[phase + 1]
            alive = {
                rank
                for rank, worker in self.current_by_rank.items()
                if worker.process.poll() is None
            }
            for rank in sorted(target - alive):
                self._spawn(rank, phase + 1)
            self.coordinator.release("done", phase)

            removed = members[phase] - target
            self._wait_removed(removed)
            self.coordinator.wait_arrivals(
                "ready", phase + 1, self.args.phase_timeout_s
            )
            self.coordinator.release("ready", phase + 1)

        self._wait_all()
        print(
            f"P1_SUITE_STATUS=PASS backend={self.args.backend} plan={self.plan_path} "
            f"workers={len(self.workers)} run_dir={self.run_dir}",
            flush=True,
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run one fixed elastic EP plan")
    parser.add_argument("--plan", choices=sorted(PLAN_FILES), required=True)
    parser.add_argument("--backend", choices=("mooncake", "nixl"), default="mooncake")
    parser.add_argument(
        "--format-set",
        choices=("common", "nixl-full"),
        default="common",
        help="common parity cases, or NIXL-only FP8 special formats",
    )
    parser.add_argument(
        "--worker-hosts",
        default="local",
        help="SSH targets in rank-placement order; global rank r maps to hosts[r %% host_count]",
    )
    parser.add_argument(
        "--gpu-lists",
        default=os.environ.get("ELASTIC_EP_GPU_LISTS", ""),
        help="Per-host visible GPU IDs; semicolon separates hosts, comma separates GPUs",
    )
    parser.add_argument("--bind-address", default="0.0.0.0")
    parser.add_argument("--advertise-address", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, default=10001)
    parser.add_argument("--ssh", default="ssh")
    parser.add_argument(
        "--conda-sh",
        default=os.environ.get("ELASTIC_EP_CONDA_SH", ""),
        help="Remote conda.sh path; required for SSH worker hosts",
    )
    parser.add_argument(
        "--worker-pythonpath",
        default=os.environ.get("ELASTIC_EP_WORKER_PYTHONPATH", ""),
        help="Optional extra Python source path; existing PYTHONPATH stays first",
    )
    parser.add_argument(
        "--nixl-pythonpath",
        default=os.environ.get("NIXL_EP_PYTHONPATH", ""),
        help="Optional installed/built NIXL Python path; no reference source is injected",
    )
    parser.add_argument(
        "--artifact-dir",
        default=os.environ.get("ELASTIC_EP_ARTIFACT_DIR", ""),
        help="Shared output root; defaults to ../artifacts/p1/launch-runs",
    )
    parser.add_argument("--native-lib-path", default="")
    parser.add_argument("--num-tokens", type=int, default=128)
    parser.add_argument("--num-experts-per-rank", type=int, default=8)
    parser.add_argument("--hidden-dim", type=int, default=7168)
    parser.add_argument("--num-topk", type=int, default=8)
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--phase-timeout-s", type=float, default=1800.0)
    parser.add_argument("--start-timeout-s", type=float, default=120.0)
    parser.add_argument("--kineto", action="store_true")
    parser.add_argument("--disable-ll-nvlink", action="store_true")
    args = parser.parse_args(argv)
    args.worker_hosts = [
        host.strip() for host in args.worker_hosts.split(",") if host.strip()
    ]
    if args.backend == "mooncake" and args.format_set == "nixl-full":
        parser.error("nixl-full formats are NIXL-only; use --backend nixl")
    if not args.worker_hosts:
        parser.error("at least one worker host is required")
    if len(set(args.worker_hosts)) != len(args.worker_hosts):
        parser.error("worker-hosts entries must be unique")
    if len(args.worker_hosts) > 1 and args.advertise_address in {
        "127.0.0.1",
        "localhost",
    }:
        parser.error(
            "multi-host runs require an internal advertise-address reachable from every node"
        )
    if (
        any(not _is_local_target(host) for host in args.worker_hosts)
        and not args.conda_sh
    ):
        parser.error("SSH worker hosts require --conda-sh or ELASTIC_EP_CONDA_SH")
    try:
        args.gpu_lists = _parse_gpu_lists(
            args.gpu_lists,
            os.environ.get("CUDA_VISIBLE_DEVICES", ""),
            len(args.worker_hosts),
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _ensure_port_free(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError as exc:
            raise RuntimeError(
                f"required port {port} is unavailable on {host}: {exc}"
            ) from exc


def _raise_on_controller_sigterm(signum, frame):
    raise InterruptedError(f"elastic launcher received SIGTERM ({signum})")


def _run_with_cleanup(launcher, coordinator, run_dir):
    previous_sigterm = signal.signal(signal.SIGTERM, _raise_on_controller_sigterm)
    try:
        launcher.run()
    except (Exception, KeyboardInterrupt) as exc:
        coordinator.abort(str(exc))
        remaining = launcher.cleanup_workers()
        details = [
            f"{type(exc).__name__}: {exc}",
            f"cleanup_remaining={[(w.rank, w.hostname, w.remote_pid, str(w.log_path)) for w in remaining]}",
        ]
        (run_dir / "failure.txt").write_text("\n".join(details) + "\n")
        if remaining:
            print(
                f"workers remain after bounded targeted cleanup: "
                f"{[(w.rank, w.hostname, w.remote_pid) for w in remaining]}",
                file=sys.stderr,
                flush=True,
            )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


def main():
    args = parse_args()
    if os.environ.get("CONDA_DEFAULT_ENV") != "myenv":
        raise SystemExit("activate conda myenv before launching elastic tests")
    if (
        Path(sys.executable).resolve()
        != (Path(os.environ["CONDA_PREFIX"]) / "bin/python").resolve()
    ):
        raise SystemExit("launcher Python is not from activated myenv")

    plan_path = HERE / args.plan
    phases = json.loads(plan_path.read_text())
    max_rank = max(abs(int(rank)) for phase in phases for rank in phase)
    rank_capacity = max_rank + 1
    host_count = len(args.worker_hosts)
    required_slots = [
        sum(rank % host_count == host_index for rank in range(rank_capacity))
        for host_index in range(host_count)
    ]
    for host_index, (host, gpu_ids, slots) in enumerate(
        zip(args.worker_hosts, args.gpu_lists, required_slots)
    ):
        if len(gpu_ids) < slots:
            raise SystemExit(
                f"host index {host_index} ({host}) needs {slots} visible GPUs "
                f"for this plan, got {len(gpu_ids)}"
            )
    run_root = (
        Path(args.artifact_dir).expanduser().resolve()
        if args.artifact_dir
        else DEV / "artifacts/p1/launch-runs"
    )
    run_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = run_root / f"{args.plan.removesuffix('.json')}-{args.backend}-{stamp}"
    run_dir.mkdir()

    for port in (TCP_STORE_PORT, RANK_SERVER_PORT, args.control_port):
        _ensure_port_free(args.bind_address, port)

    rank_host_map = ", ".join(
        f"{rank}->{args.worker_hosts[rank % host_count]}"
        for rank in range(rank_capacity)
    )
    print(
        f"P1_PLACEMENT hosts={args.worker_hosts} gpu_lists={args.gpu_lists} "
        f"rank_map={rank_host_map}",
        flush=True,
    )
    coordinator = RunCoordinator(phases)
    launcher = ElasticLauncher(args, plan_path, phases, run_dir, coordinator)
    control_server = ControlServer((args.bind_address, args.control_port), coordinator)
    control_thread = threading.Thread(target=control_server.serve_forever, daemon=True)
    control_thread.start()
    rank_thread = threading.Thread(
        target=rank_server.start_server, kwargs={"port": RANK_SERVER_PORT}, daemon=True
    )
    rank_thread.start()
    master_store = store_group.create_master_store(
        port=TCP_STORE_PORT, timeout_sec=args.phase_timeout_s
    )

    try:
        _run_with_cleanup(launcher, coordinator, run_dir)
    finally:
        del master_store
        control_server.shutdown()
        control_server.server_close()


if __name__ == "__main__":
    main()
