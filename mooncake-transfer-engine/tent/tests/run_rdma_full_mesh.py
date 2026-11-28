#!/usr/bin/env python3
"""Run the opt-in TENT RDMA full-mesh bootstrap regression test."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass


TEST_FILTER = "RdmaTransportIntegrationTest.FullMeshBootstrapStorm"
REMOTE_COMMAND_PREFIX = ""


@dataclass(frozen=True)
class Peer:
    index: int
    ssh_target: str
    segment: str


def csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def remote(
    target: str, command: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    if REMOTE_COMMAND_PREFIX:
        command = f"{REMOTE_COMMAND_PREFIX} {shlex.quote(command)}"
    return subprocess.run(
        ["ssh", target, command],
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def quote_environment(values: dict[str, str]) -> str:
    return " ".join(f"{name}={shlex.quote(value)}" for name, value in values.items())


def wait_for_count(
    targets: list[str], run_dir: str, prefix: str, expected: int, timeout: int
) -> bool:
    deadline = time.monotonic() + timeout
    pattern = shlex.quote(f"{prefix}.*")
    while time.monotonic() < deadline:
        total = 0
        for target in targets:
            result = remote(
                target,
                f"find {shlex.quote(run_dir)} -maxdepth 1 -name {pattern} "
                "-type f -print | wc -l",
                check=False,
            )
            if result.returncode == 0:
                total += int(result.stdout.strip() or "0")
        if total == expected:
            return True
        time.sleep(0.2)
    return False


def cleanup(peers: list[Peer], run_dir: str) -> None:
    for target in sorted({peer.ssh_target for peer in peers}):
        command = (
            f"for f in {shlex.quote(run_dir)}/pid.*; do "
            'test -f "$f" || continue; '
            'pid=$(cat "$f"); kill "$pid" 2>/dev/null || true; done'
        )
        remote(target, command, check=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Launch synchronized TENT RDMA peers across multiple hosts. "
            "Every peer writes 4 KiB to every other peer with four workers."
        )
    )
    parser.add_argument(
        "--ssh-targets",
        required=True,
        help="Comma-separated SSH destinations, for example node-a,node-b,node-c",
    )
    parser.add_argument(
        "--node-addresses",
        required=True,
        help="Comma-separated addresses advertised by TENT, one per SSH target",
    )
    parser.add_argument("--test-binary", required=True)
    parser.add_argument("--library-path", default="")
    parser.add_argument(
        "--remote-command-prefix",
        default="",
        help=(
            "Optional prefix for commands on each SSH destination, for example "
            "'docker exec rdma-test bash -lc'"
        ),
    )
    parser.add_argument("--peers-per-node", type=int, default=20)
    parser.add_argument("--rpc-base-port", type=int, default=10300)
    parser.add_argument("--rpc-threads", type=int, default=16)
    parser.add_argument(
        "--path-mtu",
        type=int,
        choices=(256, 512, 1024, 2048, 4096),
        default=1024,
        help="Common RDMA path MTU used by every participant.",
    )
    parser.add_argument(
        "--iterations-per-worker",
        type=int,
        default=0,
        help=(
            "Transfers issued by each worker after warmup. "
            "0 uses one full pass over all remote peers."
        ),
    )
    parser.add_argument("--warmup-seconds", type=int, default=1)
    parser.add_argument("--transfer-timeout-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--run-dir",
        default=f"/tmp/mooncake-rdma-full-mesh-{int(time.time())}",
    )
    return parser.parse_args()


def main() -> int:
    global REMOTE_COMMAND_PREFIX
    args = parse_args()
    REMOTE_COMMAND_PREFIX = args.remote_command_prefix
    ssh_targets = csv(args.ssh_targets)
    node_addresses = csv(args.node_addresses)
    if not ssh_targets or len(ssh_targets) != len(node_addresses):
        raise SystemExit("--ssh-targets and --node-addresses must have equal lengths")
    if (
        args.peers_per_node <= 0
        or args.rpc_threads <= 0
        or args.iterations_per_worker < 0
        or args.warmup_seconds <= 0
        or args.transfer_timeout_seconds <= 0
        or args.timeout_seconds <= 0
    ):
        raise SystemExit("peer and thread counts must be positive")

    peers: list[Peer] = []
    for node_index, (ssh_target, address) in enumerate(
        zip(ssh_targets, node_addresses)
    ):
        for local_index in range(args.peers_per_node):
            index = node_index * args.peers_per_node + local_index
            peers.append(
                Peer(
                    index=index,
                    ssh_target=ssh_target,
                    segment=f"{address}:{args.rpc_base_port + index}",
                )
            )
    peer_list = ",".join(peer.segment for peer in peers)
    iterations_per_worker = args.iterations_per_worker or max(1, len(peers) - 1)

    for target in ssh_targets:
        remote(target, f"mkdir -p {shlex.quote(args.run_dir)}")

    try:
        for peer in peers:
            env = {
                "MOONCAKE_RDMA_FULL_MESH_LOCAL_SEGMENT": peer.segment,
                "MOONCAKE_RDMA_FULL_MESH_PEERS": peer_list,
                "MOONCAKE_RDMA_FULL_MESH_PEER_INDEX": str(peer.index),
                "MOONCAKE_RDMA_FULL_MESH_READY_FILE": (
                    f"{args.run_dir}/ready.{peer.index}"
                ),
                "MOONCAKE_RDMA_FULL_MESH_START_FILE": f"{args.run_dir}/start",
                "MOONCAKE_RDMA_FULL_MESH_FINISH_READY_FILE": (
                    f"{args.run_dir}/finish-ready.{peer.index}"
                ),
                "MOONCAKE_RDMA_FULL_MESH_FINISH_RELEASE_FILE": (
                    f"{args.run_dir}/finish-release"
                ),
                "MOONCAKE_RDMA_FULL_MESH_RPC_THREADS": str(args.rpc_threads),
                "MOONCAKE_RDMA_FULL_MESH_PATH_MTU": str(args.path_mtu),
                "MOONCAKE_RDMA_FULL_MESH_ITERATIONS_PER_WORKER": str(
                    iterations_per_worker
                ),
                "MOONCAKE_RDMA_FULL_MESH_WARMUP_SECONDS": str(args.warmup_seconds),
                "MOONCAKE_RDMA_FULL_MESH_TRANSFER_TIMEOUT_SECONDS": str(
                    args.transfer_timeout_seconds
                ),
                "MOONCAKE_RDMA_FULL_MESH_TIMEOUT_SECONDS": str(args.timeout_seconds),
            }
            if args.library_path:
                env["LD_LIBRARY_PATH"] = args.library_path
            log = f"{args.run_dir}/peer.{peer.index}.log"
            rc = f"{args.run_dir}/rc.{peer.index}"
            pid = f"{args.run_dir}/pid.{peer.index}"
            launch = (
                f"({quote_environment(env)} {shlex.quote(args.test_binary)} "
                f"--gtest_filter={shlex.quote(TEST_FILTER)} "
                f">{shlex.quote(log)} 2>&1; echo $? >{shlex.quote(rc)}) "
                f"& echo $! >{shlex.quote(pid)}"
            )
            remote(peer.ssh_target, launch)

        if not wait_for_count(
            ssh_targets, args.run_dir, "ready", len(peers), args.timeout_seconds
        ):
            raise RuntimeError("timed out waiting for every peer to become ready")
        for target in ssh_targets:
            remote(target, f"touch {shlex.quote(args.run_dir + '/start')}")

        if not wait_for_count(
            ssh_targets,
            args.run_dir,
            "finish-ready",
            len(peers),
            args.timeout_seconds,
        ):
            raise RuntimeError("timed out waiting for every peer to finish transfers")
        for target in ssh_targets:
            remote(target, f"touch {shlex.quote(args.run_dir + '/finish-release')}")

        if not wait_for_count(
            ssh_targets, args.run_dir, "rc", len(peers), args.timeout_seconds
        ):
            raise RuntimeError("timed out waiting for peer exit codes")

        failures: list[str] = []
        for peer in peers:
            result = remote(
                peer.ssh_target,
                f"cat {shlex.quote(args.run_dir + f'/rc.{peer.index}')}",
                check=False,
            )
            if result.returncode != 0 or result.stdout.strip() != "0":
                failures.append(
                    f"peer {peer.index} ({peer.segment}) log="
                    f"{args.run_dir}/peer.{peer.index}.log"
                )
        if failures:
            print("Full-mesh regression failed:", file=sys.stderr)
            print("\n".join(failures), file=sys.stderr)
            return 1
        print(f"Full-mesh regression passed for {len(peers)} peers")
        return 0
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Full-mesh regression aborted: {error}", file=sys.stderr)
        cleanup(peers, args.run_dir)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
