#!/usr/bin/env python3
"""Two-node TE chaos runner for Mooncake RDMA transport.

The runner starts the dedicated rdma_transport_chaos_test target process on one
host and the initiator process on another host. It is intentionally based on a
TransferEngine test binary, not tebench.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


DEFAULT_DEVICES = ("mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4")
DEFAULT_NETDEVS = ("eth1", "eth2", "eth3", "eth4")
DEFAULT_EXCLUDED_DEVICES = ("mlx5_0",)
DEFAULT_EXCLUDED_NETDEVS = ("eth0",)
DEFAULT_FAULT_KINDS = (
    "mixed-netem",
    "delay",
    "loss",
    "reorder",
    "corrupt",
    "duplicate",
    "rate-limit",
    "link-down",
)


def now_s() -> float:
    return time.time()


def timestamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.localtime())


def default_repo_root() -> str:
    return str(Path(__file__).resolve().parents[3])


def sh_join(items: Iterable[str]) -> str:
    return " ".join(shlex.quote(str(item)) for item in items)


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def local_hostnames() -> set[str]:
    names = {"localhost", "127.0.0.1", socket.gethostname()}
    try:
        names.add(socket.getfqdn())
    except Exception:
        pass
    return names


@dataclass
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class Host:
    def __init__(
        self,
        name: str,
        repo: Path,
        ssh_options: list[str],
        dry_run: bool = False,
    ) -> None:
        self.name = name
        self.repo = repo
        self.ssh_options = ssh_options
        self.dry_run = dry_run
        self.is_local = name in local_hostnames()

    def shell_prefix(self) -> list[str]:
        if self.is_local:
            return []
        return ["ssh", *self.ssh_options, self.name]

    def shell_command(self, script: str) -> list[str]:
        if self.is_local:
            return ["bash", "-lc", script]
        return [*self.shell_prefix(), f"bash -lc {shlex.quote(script)}"]

    def run(
        self,
        script: str,
        *,
        check: bool = False,
        timeout: Optional[int] = None,
    ) -> CommandResult:
        if self.dry_run:
            print(f"[dry-run:{self.name}] {script}")
            return CommandResult(0, "", "")
        proc = subprocess.run(
            self.shell_command(script),
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if check and proc.returncode != 0:
            raise RuntimeError(
                f"{self.name}: command failed with {proc.returncode}: "
                f"{script}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return CommandResult(proc.returncode, proc.stdout, proc.stderr)

    def popen(self, script: str, log_path: Path) -> subprocess.Popen[str]:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            print(f"[dry-run:{self.name}] popen {script} > {log_path}")
            return subprocess.Popen(
                ["bash", "-lc", "sleep 0.1"],
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        if not self.is_local:
            pid_path = f"{log_path}.pid"
            wrapped = (
                f"echo $$ > {shlex.quote(pid_path)}; "
                f"trap 'rm -f {shlex.quote(pid_path)}' EXIT; "
                f"{script}"
            )
            remote_script = (
                f"mkdir -p {shlex.quote(str(log_path.parent))} && "
                f"setsid bash -lc {shlex.quote(wrapped)} "
                f"> {shlex.quote(str(log_path))} 2>&1"
            )
            return subprocess.Popen(
                self.shell_command(remote_script),
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        cmd = self.shell_command(script)
        log_file = log_path.open("w", encoding="utf-8")
        return subprocess.Popen(
            cmd,
            text=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )


@dataclass
class ManagedProcess:
    name: str
    host: Host
    proc: subprocess.Popen[str]
    log_path: Path
    remote_pid_path: Optional[str] = None
    remote_kill_pattern: Optional[str] = None

    def poll(self) -> Optional[int]:
        return self.proc.poll()

    def terminate(self, grace_s: float = 10.0) -> None:
        if not self.host.is_local:
            self.terminate_remote("TERM")
            time.sleep(min(2.0, grace_s))
            self.terminate_remote("KILL")
            if self.proc.poll() is None:
                self.proc.terminate()
            return
        if self.proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
            self.proc.wait(timeout=grace_s)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            self.proc.wait(timeout=5)
        except ProcessLookupError:
            pass

    def terminate_remote(self, sig: str) -> None:
        commands: list[str] = []
        if self.remote_pid_path:
            pid_file = shlex.quote(self.remote_pid_path)
            commands.append(
                "if test -s "
                f"{pid_file}; then pid=$(cat {pid_file}); "
                "pgid=$(ps -o pgid= -p $pid 2>/dev/null | tr -d ' '); "
                f"test -n \"$pgid\" && kill -{sig} -- -$pgid 2>/dev/null || "
                f"kill -{sig} $pid 2>/dev/null || true; fi"
            )
        if self.remote_kill_pattern:
            commands.append(
                f"pkill -{sig} -f {shlex.quote(self.remote_kill_pattern)} "
                "2>/dev/null || true"
            )
        if commands:
            self.host.run("; ".join(commands))


@dataclass
class FaultAction:
    name: str
    host: Host
    netdev: str
    command: str
    revert_command: str


@dataclass
class RunConfig:
    initiator: Host
    target: Host
    run_dir: Path
    te_test: str
    metadata_server: str
    protocol: str
    devices: list[str]
    excluded_devices: list[str]
    netdevs: list[str]
    excluded_netdevs: list[str]
    target_port: int
    initiator_port: int
    startup_wait_s: float
    timeout_s: float
    iterations: int
    sudo: str
    dry_run: bool
    extra_te_args: list[str]
    inject_faults: bool
    fault_mode: str
    fault_kinds: list[str]
    max_concurrent_faults: int
    fault_hold_s: float
    fault_gap_s: float
    fault_duration_min_s: float
    fault_duration_max_s: float
    fault_interval_min_s: float
    fault_interval_max_s: float
    fault_randomize_params: bool
    fault_burst_probability: float
    fault_burst_max: int
    fault_host_scope: str
    fault_seed: int

    @property
    def device_arg(self) -> str:
        return ",".join(self.devices)

    @property
    def target_segment(self) -> str:
        return f"{self.target.name}:{self.target_port}"

    @property
    def initiator_segment(self) -> str:
        return f"{self.initiator.name}:{self.initiator_port}"


class EventLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **fields: object) -> None:
        record = {"ts": now_s(), "event": event, **fields}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
        print(json.dumps(record, sort_keys=True))


def discover_p2p_segment(log_path: Path, timeout_s: float) -> str:
    deadline = now_s() + timeout_s
    patterns = [
        re.compile(r"listening on ([^\s]+:\d+)"),
        re.compile(r"--target_seg_name=([^\s]+)"),
        re.compile(r"Transfer Engine ([^\s]+:\d+) started successfully"),
    ]
    while now_s() < deadline:
        if log_path.exists():
            text = log_path.read_text(encoding="utf-8", errors="replace")
            for pattern in patterns:
                match = pattern.search(text)
                if match:
                    return match.group(1)
        time.sleep(0.2)
    raise RuntimeError(f"could not discover P2P target segment from {log_path}")


class TERunner:
    def __init__(self, cfg: RunConfig, events: EventLog) -> None:
        self.cfg = cfg
        self.events = events
        self.target_segment_name = cfg.target_segment

    def sudo_cmd(self, command: str) -> str:
        if not self.cfg.sudo:
            return command
        return f"{self.cfg.sudo} {command}"

    def cleanup_network(self) -> None:
        for host in (self.cfg.initiator, self.cfg.target):
            for netdev in self.cfg.netdevs:
                if netdev in self.cfg.excluded_netdevs:
                    continue
                quoted = shlex.quote(netdev)
                script = (
                    f"{self.sudo_cmd(f'tc qdisc del dev {quoted} root')} "
                    "2>/dev/null || true; "
                    f"{self.sudo_cmd(f'ip link set dev {quoted} up')} "
                    "2>/dev/null || true"
                )
                result = host.run(script)
                self.events.write(
                    "cleanup-netdev",
                    host=host.name,
                    netdev=netdev,
                    rc=result.returncode,
                )

    def random_percent(
        self,
        rnd: random.Random,
        low: float,
        high: float,
        digits: int = 3,
    ) -> str:
        value = rnd.uniform(low, high)
        return f"{value:.{digits}f}".rstrip("0").rstrip(".")

    def make_fault_action(
        self,
        kind: str,
        host: Host,
        netdev: str,
        rnd: Optional[random.Random] = None,
    ) -> FaultAction:
        quoted = shlex.quote(netdev)
        randomize = self.cfg.fault_randomize_params and rnd is not None
        if kind == "mixed-netem":
            if randomize:
                delay_ms = rnd.randint(10, 200)
                jitter_ms = rnd.randint(1, max(1, delay_ms // 2))
                loss_pct = self.random_percent(rnd, 0.05, 3.0)
                duplicate_pct = self.random_percent(rnd, 0.05, 1.0)
                corrupt_pct = self.random_percent(rnd, 0.005, 0.1)
                params = (
                    f"delay {delay_ms}ms {jitter_ms}ms distribution normal "
                    f"loss {loss_pct}% duplicate {duplicate_pct}% "
                    f"corrupt {corrupt_pct}%"
                )
            else:
                params = (
                    "delay 50ms 10ms distribution normal "
                    "loss 0.5% duplicate 0.2% corrupt 0.02%"
                )
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem {params}"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "delay":
            if randomize:
                delay_ms = rnd.randint(5, 250)
                jitter_ms = rnd.randint(1, min(50, max(1, delay_ms // 2)))
                params = f"delay {delay_ms}ms {jitter_ms}ms distribution normal"
            else:
                params = "delay 100ms 20ms distribution normal"
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem {params}"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "loss":
            loss_pct = (
                self.random_percent(rnd, 0.05, 5.0)
                if randomize
                else "1"
            )
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem loss {loss_pct}%"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "reorder":
            if randomize:
                delay_ms = rnd.randint(5, 80)
                reorder_pct = self.random_percent(rnd, 5.0, 50.0, digits=2)
                correlation_pct = self.random_percent(rnd, 10.0, 80.0, digits=2)
                params = (
                    f"delay {delay_ms}ms reorder {reorder_pct}% "
                    f"{correlation_pct}%"
                )
            else:
                params = "delay 20ms reorder 25% 50%"
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem {params}"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "corrupt":
            corrupt_pct = (
                self.random_percent(rnd, 0.005, 0.2)
                if randomize
                else "0.05"
            )
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem corrupt {corrupt_pct}%"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "duplicate":
            duplicate_pct = (
                self.random_percent(rnd, 0.05, 2.0)
                if randomize
                else "0.5"
            )
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root netem duplicate {duplicate_pct}%"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "rate-limit":
            if randomize:
                rate = rnd.choice(("500mbit", "1gbit", "2gbit", "5gbit", "10gbit"))
                burst_mb = rnd.randint(8, 64)
                latency_ms = rnd.randint(100, 800)
                params = (
                    f"rate {rate} burst {burst_mb}mb latency {latency_ms}ms"
                )
            else:
                params = "rate 5gbit burst 32mb latency 400ms"
            command = self.sudo_cmd(
                f"tc qdisc replace dev {quoted} root tbf {params}"
            )
            revert = self.sudo_cmd(f"tc qdisc del dev {quoted} root") + (
                " 2>/dev/null || true"
            )
        elif kind == "link-down":
            command = self.sudo_cmd(f"ip link set dev {quoted} down")
            revert = self.sudo_cmd(f"ip link set dev {quoted} up")
        else:
            raise ValueError(f"unsupported fault kind: {kind}")
        return FaultAction(
            name=kind,
            host=host,
            netdev=netdev,
            command=command,
            revert_command=revert,
        )

    def make_fault_group(self, round_index: int) -> list[FaultAction]:
        rnd = random.Random(self.cfg.fault_seed + round_index)
        scope = self.choose_fault_host_scope(rnd)
        pairs = self.fault_pairs_for_scope(scope)
        rnd.shuffle(pairs)
        if not pairs:
            return []
        count = min(self.cfg.max_concurrent_faults, len(pairs))
        actions = []
        for i in range(count):
            kind = self.cfg.fault_kinds[
                (round_index + i) % len(self.cfg.fault_kinds)
            ]
            host, netdev = pairs[i]
            actions.append(self.make_fault_action(kind, host, netdev, rnd))
        return actions

    def apply_fault_group(self, round_index: int) -> list[FaultAction]:
        actions = self.make_fault_group(round_index)
        self.events.write(
            "fault-round-apply",
            round_index=round_index,
            faults=[
                {"kind": action.name, "host": action.host.name, "netdev": action.netdev}
                for action in actions
            ],
        )
        applied: list[FaultAction] = []
        for action in actions:
            if not self.apply_fault_action(action, round_index=round_index):
                self.revert_fault_group(applied, round_index)
                raise RuntimeError(
                    f"failed to apply {action.name} on "
                    f"{action.host.name}:{action.netdev}"
                )
            applied.append(action)
        return applied

    def revert_fault_group(
        self, actions: list[FaultAction], round_index: int
    ) -> None:
        for action in reversed(actions):
            self.revert_fault_action(action, round_index=round_index)

    def apply_fault_action(
        self,
        action: FaultAction,
        *,
        round_index: Optional[int] = None,
        action_id: Optional[int] = None,
        planned_duration_s: Optional[float] = None,
    ) -> bool:
        result = action.host.run(action.command)
        self.events.write(
            "fault-applied",
            round_index=round_index,
            action_id=action_id,
            kind=action.name,
            host=action.host.name,
            netdev=action.netdev,
            command=action.command,
            planned_duration_s=planned_duration_s,
            rc=result.returncode,
            stdout=result.stdout[-2000:],
            stderr=result.stderr[-2000:],
        )
        return result.returncode == 0

    def revert_fault_action(
        self,
        action: FaultAction,
        *,
        round_index: Optional[int] = None,
        action_id: Optional[int] = None,
    ) -> None:
        result = action.host.run(action.revert_command)
        self.events.write(
            "fault-reverted",
            round_index=round_index,
            action_id=action_id,
            kind=action.name,
            host=action.host.name,
            netdev=action.netdev,
            command=action.revert_command,
            rc=result.returncode,
            stdout=result.stdout[-2000:],
            stderr=result.stderr[-2000:],
        )

    def te_args(
        self,
        *,
        mode: str,
        local_server_name: str,
        segment_id: Optional[str] = None,
    ) -> list[str]:
        args = [
            self.cfg.te_test,
            f"--mode={mode}",
            f"--metadata_server={self.cfg.metadata_server}",
            f"--protocol={self.cfg.protocol}",
            f"--device_name={self.cfg.device_arg}",
            f"--local_server_name={local_server_name}",
            "--logtostderr=true",
        ]
        if segment_id:
            args.append(f"--segment_id={segment_id}")
        args.extend(self.cfg.extra_te_args)
        return args

    def te_env(self) -> str:
        env = {
            "MC_TE_FILTERS": ",".join(self.cfg.devices),
            "MC_TE_FILTERS_EXCLUDE": ",".join(self.cfg.excluded_devices),
        }
        return " ".join(f"{key}={shlex.quote(value)}" for key, value in env.items())

    def start_target(self, run_id: str) -> ManagedProcess:
        args = self.te_args(
            mode="target",
            local_server_name=self.cfg.target_segment,
        )
        cmd = (
            f"cd {shlex.quote(str(self.cfg.target.repo))} && "
            f"{self.te_env()} {sh_join(args)}"
        )
        log = self.cfg.run_dir / f"{run_id}-target.log"
        proc = ManagedProcess(
            "target",
            self.cfg.target,
            self.cfg.target.popen(cmd, log),
            log,
            None if self.cfg.target.is_local else f"{log}.pid",
            None if self.cfg.target.is_local else run_id,
        )
        self.events.write(
            "target-started",
            host=self.cfg.target.name,
            segment=self.cfg.target_segment,
            log=str(log),
            command=cmd,
        )
        time.sleep(self.cfg.startup_wait_s)
        if not self.cfg.dry_run and proc.poll() is not None:
            raise RuntimeError(f"target exited during startup; see {log}")
        if self.cfg.metadata_server == "P2PHANDSHAKE" and not self.cfg.dry_run:
            self.target_segment_name = discover_p2p_segment(log, 30.0)
            self.events.write(
                "target-segment-discovered",
                host=self.cfg.target.name,
                target_segment=self.target_segment_name,
            )
        return proc

    def start_initiator(self, run_id: str) -> ManagedProcess:
        args = self.te_args(
            mode="initiator",
            local_server_name=self.cfg.initiator_segment,
            segment_id=self.target_segment_name,
        )
        cmd = (
            f"cd {shlex.quote(str(self.cfg.initiator.repo))} && "
            f"{self.te_env()} {sh_join(args)}"
        )
        log = self.cfg.run_dir / f"{run_id}-initiator.log"
        proc = ManagedProcess(
            "initiator",
            self.cfg.initiator,
            self.cfg.initiator.popen(cmd, log),
            log,
            None if self.cfg.initiator.is_local else f"{log}.pid",
            None if self.cfg.initiator.is_local else run_id,
        )
        self.events.write(
            "initiator-started",
            host=self.cfg.initiator.name,
            segment=self.cfg.initiator_segment,
            target_segment=self.target_segment_name,
            log=str(log),
            command=cmd,
        )
        return proc

    def run_once(self, run_id: str) -> int:
        target: Optional[ManagedProcess] = None
        initiator: Optional[ManagedProcess] = None
        rc = 1
        try:
            target = self.start_target(run_id)
            initiator = self.start_initiator(run_id)
            deadline = now_s() + self.cfg.timeout_s
            if self.cfg.inject_faults:
                self.run_faults_until_done(initiator, target, deadline)
            else:
                self.wait_until_done(initiator, target, deadline)
            if initiator.poll() is None:
                self.events.write("initiator-timeout", run_id=run_id)
                initiator.terminate(grace_s=5)
                rc = 124
            else:
                rc = initiator.poll() or 0
            self.events.write("initiator-exited", run_id=run_id, rc=rc)
        except Exception as exc:
            self.events.write("run-error", run_id=run_id, error=str(exc))
            rc = 1
        finally:
            if initiator is not None:
                initiator.terminate(grace_s=5)
            if target is not None:
                target.terminate(grace_s=10)
        return rc

    def wait_until_done(
        self,
        initiator: ManagedProcess,
        target: ManagedProcess,
        deadline: float,
    ) -> None:
        while initiator.poll() is None and now_s() < deadline:
            target_rc = target.poll()
            if target_rc is not None:
                raise RuntimeError(f"target exited early with {target_rc}")
            time.sleep(1)

    def sleep_or_until_done(
        self,
        seconds: float,
        initiator: ManagedProcess,
        target: ManagedProcess,
    ) -> None:
        end = now_s() + seconds
        while now_s() < end:
            if initiator.poll() is not None:
                return
            target_rc = target.poll()
            if target_rc is not None:
                raise RuntimeError(f"target exited early with {target_rc}")
            time.sleep(min(1.0, end - now_s()))

    def run_faults_until_done(
        self,
        initiator: ManagedProcess,
        target: ManagedProcess,
        deadline: float,
    ) -> None:
        if self.cfg.fault_mode == "async":
            self.run_async_faults_until_done(initiator, target, deadline)
            return

        round_index = 0
        while initiator.poll() is None and now_s() < deadline:
            actions = self.apply_fault_group(round_index)
            try:
                self.sleep_or_until_done(
                    self.cfg.fault_hold_s, initiator, target
                )
            finally:
                self.revert_fault_group(actions, round_index)
            if self.cfg.fault_gap_s > 0:
                self.sleep_or_until_done(
                    self.cfg.fault_gap_s, initiator, target
                )
            round_index += 1

    def fault_pairs(self) -> list[tuple[Host, str]]:
        return [
            (host, netdev)
            for host in (self.cfg.initiator, self.cfg.target)
            for netdev in self.cfg.netdevs
            if netdev not in self.cfg.excluded_netdevs
        ]

    def choose_fault_host_scope(self, rnd: random.Random) -> str:
        if self.cfg.fault_host_scope == "random":
            return rnd.choice(("initiator", "target", "both"))
        return self.cfg.fault_host_scope

    def fault_pairs_for_scope(
        self,
        scope: str,
        pairs: Optional[list[tuple[Host, str]]] = None,
    ) -> list[tuple[Host, str]]:
        source = pairs if pairs is not None else self.fault_pairs()
        if scope == "initiator":
            return [pair for pair in source if pair[0].name == self.cfg.initiator.name]
        if scope == "target":
            return [pair for pair in source if pair[0].name == self.cfg.target.name]
        return list(source)

    def choose_async_burst_pairs(
        self,
        rnd: random.Random,
        available: list[tuple[Host, str]],
        scope: str,
        count: int,
    ) -> list[tuple[Host, str]]:
        scoped = self.fault_pairs_for_scope(scope, available)
        if not scoped:
            return []
        if scope != "both" or count < 2:
            rnd.shuffle(scoped)
            return scoped[:count]

        initiator_pairs = self.fault_pairs_for_scope("initiator", scoped)
        target_pairs = self.fault_pairs_for_scope("target", scoped)
        selected: list[tuple[Host, str]] = []
        if initiator_pairs and target_pairs:
            selected.append(rnd.choice(initiator_pairs))
            selected.append(rnd.choice(target_pairs))
        remaining = [pair for pair in scoped if pair not in selected]
        rnd.shuffle(remaining)
        selected.extend(remaining[: max(0, count - len(selected))])
        return selected[:count]

    def run_async_faults_until_done(
        self,
        initiator: ManagedProcess,
        target: ManagedProcess,
        deadline: float,
    ) -> None:
        rnd = random.Random(self.cfg.fault_seed)
        active: list[tuple[int, FaultAction, float]] = []
        action_id = 0
        next_apply = now_s()

        def revert_due() -> None:
            nonlocal active
            current = now_s()
            keep: list[tuple[int, FaultAction, float]] = []
            for item_action_id, action, end_time in active:
                if current >= end_time:
                    self.events.write(
                        "fault-async-expire",
                        action_id=item_action_id,
                        kind=action.name,
                        host=action.host.name,
                        netdev=action.netdev,
                    )
                    self.revert_fault_action(action, action_id=item_action_id)
                else:
                    keep.append((item_action_id, action, end_time))
            active = keep

        try:
            while initiator.poll() is None and now_s() < deadline:
                target_rc = target.poll()
                if target_rc is not None:
                    raise RuntimeError(f"target exited early with {target_rc}")

                revert_due()

                current = now_s()
                if current >= next_apply and len(active) < self.cfg.max_concurrent_faults:
                    occupied = {
                        (action.host.name, action.netdev)
                        for _, action, _ in active
                    }
                    available = [
                        pair
                        for pair in self.fault_pairs()
                        if (pair[0].name, pair[1]) not in occupied
                    ]
                    if available:
                        scope = self.choose_fault_host_scope(rnd)
                        scoped_available = self.fault_pairs_for_scope(
                            scope, available
                        )
                        if not scoped_available:
                            next_apply = current + rnd.uniform(
                                self.cfg.fault_interval_min_s,
                                self.cfg.fault_interval_max_s,
                            )
                            continue
                        free_slots = self.cfg.max_concurrent_faults - len(active)
                        burst_count = 1
                        if (
                            self.cfg.fault_burst_max > 1
                            and rnd.random() < self.cfg.fault_burst_probability
                        ):
                            burst_count = rnd.randint(2, self.cfg.fault_burst_max)
                        if scope == "both" and free_slots >= 2:
                            burst_count = max(2, burst_count)
                        burst_count = min(
                            burst_count, free_slots, len(scoped_available)
                        )
                        selected_pairs = self.choose_async_burst_pairs(
                            rnd, available, scope, burst_count
                        )

                        for burst_index, (host, netdev) in enumerate(selected_pairs):
                            kind = rnd.choice(self.cfg.fault_kinds)
                            duration_s = rnd.uniform(
                                self.cfg.fault_duration_min_s,
                                self.cfg.fault_duration_max_s,
                            )
                            action = self.make_fault_action(kind, host, netdev, rnd)
                            self.events.write(
                                "fault-async-schedule",
                                action_id=action_id,
                                kind=kind,
                                host=host.name,
                                netdev=netdev,
                                duration_s=duration_s,
                                active_faults=len(active) + 1,
                                burst_index=burst_index,
                                burst_count=len(selected_pairs),
                                host_scope=scope,
                                command=action.command,
                            )
                            if not self.apply_fault_action(
                                action,
                                action_id=action_id,
                                planned_duration_s=duration_s,
                            ):
                                raise RuntimeError(
                                    f"failed to apply {kind} on {host.name}:{netdev}"
                                )
                            active.append((action_id, action, current + duration_s))
                            action_id += 1
                    next_apply = current + rnd.uniform(
                        self.cfg.fault_interval_min_s,
                        self.cfg.fault_interval_max_s,
                    )

                sleep_until = min(deadline, next_apply)
                if active:
                    sleep_until = min(sleep_until, min(end for _, _, end in active))
                time.sleep(max(0.05, min(0.5, sleep_until - now_s())))
        finally:
            for item_action_id, action, _ in reversed(active):
                self.revert_fault_action(action, action_id=item_action_id)


def run_doctor(cfg: RunConfig) -> int:
    checks = []
    for host in (cfg.initiator, cfg.target):
        checks.append((host, "hostname", "hostname"))
        checks.append(
            (
                host,
                "te-test",
                f"cd {shlex.quote(str(host.repo))} && "
                f"test -x {shlex.quote(cfg.te_test)}",
            )
        )
        checks.append((host, "rdma", "command -v rdma >/dev/null && rdma link show"))
        checks.append((host, "ip", "ip -brief link show"))
        for device in cfg.devices:
            checks.append(
                (
                    host,
                    f"device-{device}",
                    f"test -d /sys/class/infiniband/{shlex.quote(device)}",
                )
            )
    ok = True
    for host, name, script in checks:
        result = host.run(script)
        status = "OK" if result.returncode == 0 else "FAIL"
        print(f"[{status}] {host.name}:{name}")
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        ok = ok and result.returncode == 0
    banned_devices = set(cfg.excluded_devices)
    active_devices = set(cfg.devices)
    banned_netdevs = set(cfg.excluded_netdevs)
    active_netdevs = set(cfg.netdevs)
    if banned_devices & active_devices:
        print(f"[FAIL] device selection includes excluded devices: {banned_devices & active_devices}")
        ok = False
    if banned_netdevs & active_netdevs:
        print(f"[FAIL] netdev selection includes excluded devices: {banned_netdevs & active_netdevs}")
        ok = False
    print(f"selected RDMA devices: {','.join(cfg.devices)}")
    print(f"selected netdevs: {','.join(cfg.netdevs)}")
    print(f"metadata server: {cfg.metadata_server}")
    print(f"TE test binary: {cfg.te_test}")
    return 0 if ok else 1


def run_cleanup(cfg: RunConfig) -> int:
    events = EventLog(cfg.run_dir / "cleanup-events.jsonl")
    TERunner(cfg, events).cleanup_network()
    return 0


def run_smoke(cfg: RunConfig) -> int:
    events = EventLog(cfg.run_dir / "events.jsonl")
    events.write(
        "run-start",
        suite="smoke",
        run_dir=str(cfg.run_dir),
        initiator=cfg.initiator.name,
        target=cfg.target.name,
        metadata_server=cfg.metadata_server,
        devices=cfg.devices,
        network_faults=cfg.inject_faults,
        fault_mode=cfg.fault_mode if cfg.inject_faults else None,
        fault_kinds=cfg.fault_kinds if cfg.inject_faults else [],
        max_concurrent_faults=cfg.max_concurrent_faults
        if cfg.inject_faults
        else 0,
        fault_duration_range_s=[
            cfg.fault_duration_min_s,
            cfg.fault_duration_max_s,
        ]
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        fault_interval_range_s=[
            cfg.fault_interval_min_s,
            cfg.fault_interval_max_s,
        ]
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        fault_randomize_params=cfg.fault_randomize_params
        if cfg.inject_faults
        else False,
        fault_burst_probability=cfg.fault_burst_probability
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        fault_burst_max=cfg.fault_burst_max
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        fault_host_scope=cfg.fault_host_scope if cfg.inject_faults else None,
        fault_seed=cfg.fault_seed if cfg.inject_faults else None,
    )
    runner = TERunner(cfg, events)
    if cfg.inject_faults:
        runner.cleanup_network()
    results = []
    try:
        for index in range(cfg.iterations):
            run_id = f"{timestamp()}-{index}-rdma-transport-chaos-test"
            events.write("iteration-start", run_id=run_id)
            rc = runner.run_once(run_id)
            events.write("iteration-finish", run_id=run_id, rc=rc)
            results.append({"run_id": run_id, "rc": rc})
            if rc != 0:
                break
    finally:
        if cfg.inject_faults:
            runner.cleanup_network()
    summary = {
        "results": results,
        "run_dir": str(cfg.run_dir),
        "network_faults": cfg.inject_faults,
        "fault_mode": cfg.fault_mode if cfg.inject_faults else None,
        "fault_kinds": cfg.fault_kinds if cfg.inject_faults else [],
        "max_concurrent_faults": cfg.max_concurrent_faults
        if cfg.inject_faults
        else 0,
        "fault_duration_range_s": [
            cfg.fault_duration_min_s,
            cfg.fault_duration_max_s,
        ]
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        "fault_interval_range_s": [
            cfg.fault_interval_min_s,
            cfg.fault_interval_max_s,
        ]
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        "fault_randomize_params": cfg.fault_randomize_params
        if cfg.inject_faults
        else False,
        "fault_burst_probability": cfg.fault_burst_probability
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        "fault_burst_max": cfg.fault_burst_max
        if cfg.inject_faults and cfg.fault_mode == "async"
        else None,
        "fault_host_scope": cfg.fault_host_scope if cfg.inject_faults else None,
        "fault_seed": cfg.fault_seed if cfg.inject_faults else None,
        "te_test": cfg.te_test,
        "devices": cfg.devices,
    }
    (cfg.run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    events.write("run-finish", summary=summary)
    return 0 if all(item["rc"] == 0 for item in results) else 1


def build_config(args: argparse.Namespace) -> RunConfig:
    ssh_options = ["-o", "BatchMode=yes", "-o", f"ConnectTimeout={args.ssh_timeout}"]
    repo = Path(args.repo).resolve()
    run_dir = Path(args.run_dir).resolve() if args.run_dir else (
        repo / "build" / "chaos-runs" / timestamp()
    )
    dry_run = bool(getattr(args, "dry_run", False))
    initiator = Host(args.initiator, repo, ssh_options, dry_run=dry_run)
    target = Host(args.target, Path(args.remote_repo or args.repo), ssh_options, dry_run=dry_run)
    devices = parse_csv(args.devices)
    netdevs = parse_csv(args.netdevs)
    excluded_devices = parse_csv(args.exclude_devices)
    excluded_netdevs = parse_csv(args.exclude_netdevs)
    fault_kinds = parse_csv(args.fault_kinds)
    if "mlx5_0" in devices or "eth0" in netdevs:
        raise ValueError("refusing to include mlx5_0/eth0 in the TE test target set")
    if not devices:
        raise ValueError("at least one RDMA device must be selected")
    unknown_fault_kinds = sorted(set(fault_kinds) - set(DEFAULT_FAULT_KINDS))
    if unknown_fault_kinds:
        raise ValueError(f"unsupported fault kinds: {','.join(unknown_fault_kinds)}")
    if args.inject_faults and not fault_kinds:
        raise ValueError("at least one fault kind is required with --inject-faults")
    if args.inject_faults and args.max_concurrent_faults <= 0:
        raise ValueError("--max-concurrent-faults must be positive")
    if args.inject_faults and args.fault_duration_min <= 0:
        raise ValueError("--fault-duration-min must be positive")
    if args.inject_faults and args.fault_duration_max < args.fault_duration_min:
        raise ValueError("--fault-duration-max must be >= --fault-duration-min")
    if args.inject_faults and args.fault_interval_min < 0:
        raise ValueError("--fault-interval-min must be non-negative")
    if args.inject_faults and args.fault_interval_max < args.fault_interval_min:
        raise ValueError("--fault-interval-max must be >= --fault-interval-min")
    if args.inject_faults and not 0 <= args.fault_burst_probability <= 1:
        raise ValueError("--fault-burst-probability must be between 0 and 1")
    if args.inject_faults and args.fault_burst_max < 1:
        raise ValueError("--fault-burst-max must be >= 1")
    return RunConfig(
        initiator=initiator,
        target=target,
        run_dir=run_dir,
        te_test=args.te_test,
        metadata_server=args.metadata_server,
        protocol=args.protocol,
        devices=devices,
        excluded_devices=excluded_devices,
        netdevs=netdevs,
        excluded_netdevs=excluded_netdevs,
        target_port=args.target_port,
        initiator_port=args.initiator_port,
        startup_wait_s=args.startup_wait,
        timeout_s=args.timeout,
        iterations=args.iterations,
        sudo=args.sudo,
        dry_run=dry_run,
        extra_te_args=args.extra_te_arg,
        inject_faults=args.inject_faults,
        fault_mode=args.fault_mode,
        fault_kinds=fault_kinds,
        max_concurrent_faults=args.max_concurrent_faults,
        fault_hold_s=args.fault_hold,
        fault_gap_s=args.fault_gap,
        fault_duration_min_s=args.fault_duration_min,
        fault_duration_max_s=args.fault_duration_max,
        fault_interval_min_s=args.fault_interval_min,
        fault_interval_max_s=args.fault_interval_max,
        fault_randomize_params=args.fault_randomize_params,
        fault_burst_probability=args.fault_burst_probability,
        fault_burst_max=args.fault_burst_max,
        fault_host_scope=args.fault_host_scope,
        fault_seed=args.fault_seed,
    )


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--initiator", default=socket.gethostname())
    parser.add_argument("--target", default="qjh001")
    parser.add_argument("--repo", default=default_repo_root())
    parser.add_argument("--remote-repo", default=None)
    parser.add_argument("--run-dir", default="")
    parser.add_argument(
        "--te-test",
        default="build/mooncake-transfer-engine/tests/rdma_transport_chaos_test",
        help=(
            "Path to the TE chaos test binary, relative to --repo unless "
            "absolute."
        ),
    )
    parser.add_argument("--metadata-server", default="P2PHANDSHAKE")
    parser.add_argument("--protocol", choices=["rdma", "tcp", "nvmeof"], default="rdma")
    parser.add_argument("--devices", default=",".join(DEFAULT_DEVICES))
    parser.add_argument("--exclude-devices", default=",".join(DEFAULT_EXCLUDED_DEVICES))
    parser.add_argument("--netdevs", default=",".join(DEFAULT_NETDEVS))
    parser.add_argument("--exclude-netdevs", default=",".join(DEFAULT_EXCLUDED_NETDEVS))
    parser.add_argument("--target-port", type=int, default=12345)
    parser.add_argument("--initiator-port", type=int, default=12346)
    parser.add_argument("--ssh-timeout", type=int, default=8)
    parser.add_argument("--startup-wait", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--sudo", default="sudo -n")
    parser.add_argument("--inject-faults", action="store_true")
    parser.add_argument(
        "--fault-mode",
        choices=["grouped", "async"],
        default="grouped",
        help=(
            "grouped applies/reverts fault rounds together; async starts and "
            "ends individual random faults independently."
        ),
    )
    parser.add_argument(
        "--fault-kinds",
        default=",".join(DEFAULT_FAULT_KINDS),
        help=(
            "Comma-separated fault kinds: mixed-netem,delay,loss,reorder,"
            "corrupt,duplicate,rate-limit,link-down"
        ),
    )
    parser.add_argument("--max-concurrent-faults", type=int, default=3)
    parser.add_argument("--fault-hold", type=float, default=6.0)
    parser.add_argument("--fault-gap", type=float, default=1.0)
    parser.add_argument("--fault-duration-min", type=float, default=2.0)
    parser.add_argument("--fault-duration-max", type=float, default=8.0)
    parser.add_argument("--fault-interval-min", type=float, default=0.5)
    parser.add_argument("--fault-interval-max", type=float, default=2.0)
    parser.add_argument(
        "--fault-randomize-params",
        action="store_true",
        help="Randomize tc/netem/tbf intensity parameters for each fault action.",
    )
    parser.add_argument(
        "--fault-burst-probability",
        type=float,
        default=0.0,
        help="Async mode probability that one scheduling point starts a burst.",
    )
    parser.add_argument(
        "--fault-burst-max",
        type=int,
        default=1,
        help="Async mode maximum number of faults in one burst.",
    )
    parser.add_argument(
        "--fault-host-scope",
        choices=["random", "initiator", "target", "both"],
        default="random",
        help=(
            "Fault side selection. random chooses initiator, target, or both "
            "per scheduling point."
        ),
    )
    parser.add_argument("--fault-seed", type=int, default=1)
    parser.add_argument(
        "--extra-te-arg",
        action="append",
        default=[],
        help="Additional raw rdma_transport_chaos_test flag; may be repeated.",
    )
    parser.add_argument("--dry-run", action="store_true")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Check hosts, devices and TE test binary")
    add_common_args(doctor)

    cleanup = sub.add_parser("cleanup", help="Remove qdisc faults and bring rails up")
    add_common_args(cleanup)

    run = sub.add_parser("run", help="Run the TE RDMA chaos test")
    add_common_args(run)

    args = parser.parse_args(argv)
    cfg = build_config(args)
    cfg.run_dir.mkdir(parents=True, exist_ok=True)

    if args.command == "doctor":
        return run_doctor(cfg)
    if args.command == "cleanup":
        return run_cleanup(cfg)
    return run_smoke(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
