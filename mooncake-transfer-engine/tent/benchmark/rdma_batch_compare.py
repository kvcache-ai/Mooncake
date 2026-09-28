#!/usr/bin/env python3
"""Compare RDMA batch policies using tebench or tent_rdma_workload (host DRAM).

The default steady mode retains tebench. Burst/biased_backlog select the native
arrival-driven entry; --binary must name tent_rdma_workload for those modes.
Run only on approved hosts/ports; it does not reserve or discover resources.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import shlex
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["server", "client"])
    parser.add_argument("--run-config", help="JSON argument defaults; '-' reads stdin")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--host", help="Routable local RPC IP")
    parser.add_argument("--port", type=int, default=19001)
    parser.add_argument("--peer", help="Server IP:port, required for client")
    parser.add_argument("--rails", help="Comma-separated RDMA NICs")
    parser.add_argument(
        "--base-config", type=Path, help="Host-specific GID/rail mapping etc."
    )
    parser.add_argument("--output", type=Path, default=Path("rdma-batch-run"))
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--numa", type=int, default=0)
    parser.add_argument("--trace-interval", type=int, default=0)
    parser.add_argument(
        "--workload", choices=["steady", "burst", "biased_backlog"], default="steady"
    )
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--burst-size", type=int, default=4)
    parser.add_argument("--interval-us", type=int, default=5000)
    parser.add_argument("--background-mib", type=int, default=32)
    parser.add_argument("--background-gap-us", type=int, default=0)
    parser.add_argument("--busy-rail", type=int, choices=[0, 1], default=0)
    parser.add_argument("--timeout-ms", type=int, default=10000)
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="Native workload allocation tracing; separate from performance runs",
    )
    parser.add_argument("--baselines", action="store_true")
    parser.add_argument(
        "--static-baseline",
        action="store_true",
        help="Compare all three policies without alpha tuning",
    )
    parser.add_argument("--ssh-host", help="SSH host/alias; omit to execute locally")
    parser.add_argument("--ssh-config", type=Path)
    parser.add_argument("--ssh-port", type=int)
    parser.add_argument("--ssh-user")
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--jump-host")
    parser.add_argument(
        "--remote-root", help="Checkout directory on the remote Linux host"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write configs/commands without starting traffic",
    )
    parser.add_argument(
        "--capacity-gbps", help="Frozen calibration by local NIC ID, e.g. 95,94"
    )
    preliminary, _ = parser.parse_known_args()
    if preliminary.run_config:
        spec = (
            json.load(sys.stdin)
            if preliminary.run_config == "-"
            else json.loads(Path(preliminary.run_config).read_text(encoding="utf-8"))
        )
        parser.set_defaults(**spec)
    args = parser.parse_args()
    if not args.binary or not args.host or not args.rails:
        parser.error(
            "--binary, --host and --rails are required, directly or in --run-config"
        )
    if args.role == "client" and not args.peer:
        parser.error("client requires --peer")
    if (args.baselines or args.static_baseline) and not args.capacity_gbps:
        parser.error("static baseline requires explicit --capacity-gbps calibration")
    if args.ssh_host:
        if not args.remote_root:
            parser.error("--ssh-host requires --remote-root")
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
        for flag, value in [
            ("-F", args.ssh_config),
            ("-p", args.ssh_port),
            ("-l", args.ssh_user),
            ("-i", args.identity_file),
            ("-J", args.jump_host),
        ]:
            if value is not None:
                ssh += [flag, str(value)]
        remote = {
            key: value.as_posix() if isinstance(value, Path) else value
            for key, value in vars(args).items()
            if key
            not in {
                "role",
                "run_config",
                "ssh_host",
                "ssh_config",
                "ssh_port",
                "ssh_user",
                "identity_file",
                "jump_host",
                "remote_root",
            }
        }
        remote_command = (
            "cd "
            + shlex.quote(args.remote_root)
            + " && "
            + shlex.join(
                [
                    "python3",
                    "mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py",
                    args.role,
                    "--run-config",
                    "-",
                ]
            )
        )
        ssh += [args.ssh_host, remote_command]
        if args.dry_run:
            print(json.dumps({"ssh_command": ssh, "remote_config": remote}, indent=2))
            return
        raise SystemExit(
            subprocess.run(
                ssh, input=json.dumps(remote), text=True, check=False
            ).returncode
        )
    args.output.mkdir(parents=True, exist_ok=True)
    base = (
        json.loads(args.base_config.read_text(encoding="utf-8"))
        if args.base_config
        else {}
    )
    base.update(
        local_segment_name=f"{args.host}:{args.port}",
        metadata_type="p2p",
        rpc_server_hostname=args.host,
        rpc_server_port=args.port,
    )
    base.setdefault("topology", {})["rdma_whitelist"] = args.rails.split(",")
    transports = base.setdefault("transports", {})
    for name in ["tcp", "shm", "nvlink", "mnnvl", "gds", "io_uring"]:
        transports.setdefault(name, {})["enable"] = False
    rdma = transports.setdefault("rdma", {})
    rdma.update(
        enable=True,
        enable_smart_scheduling=True,
        batch_trace_interval=args.trace_interval,
    )
    rdma.setdefault("num_lanes", 6)
    rdma.setdefault("workers", {})["block_size"] = 2 * 1024 * 1024
    native = args.workload != "steady"

    def native_spec(output, policy):
        spec = dict(
            role=args.role,
            engine=base,
            peer=args.peer,
            output=str(output),
            mode=args.workload,
            count=args.requests,
            burst_size=args.burst_size,
            interval_us=args.interval_us,
            background_mib=args.background_mib,
            background_gap_us=args.background_gap_us,
            busy_rail=args.busy_rail,
            rails=args.rails.split(","),
            policy=policy,
            diagnostic=args.diagnostic,
            numa=args.numa,
            timeout_ms=args.timeout_ms,
        )
        if args.capacity_gbps:
            spec["capacity_gbps"] = [float(v) for v in args.capacity_gbps.split(",")]
        return spec

    def native_command(config):
        return [
            "numactl",
            f"--cpunodebind={args.numa}",
            f"--membind={args.numa}",
            str(args.binary.resolve()),
            "--config",
            str(config),
        ]

    if native and args.role == "server":
        path = args.output / "server-workload.json"
        path.write_text(
            json.dumps(
                native_spec(args.output / "native-server", "inverse_score"), indent=2
            )
            + "\n"
        )
        command = native_command(path)
        if args.dry_run:
            print(json.dumps(command))
            return
        os.execvp(command[0], command)
    command = [
        "numactl",
        f"--cpunodebind={args.numa}",
        f"--membind={args.numa}",
        str(args.binary.resolve()),
        "--backend=tent",
        "--xport_type=rdma",
        "--tent_transport_hint=rdma",
        "--seg_type=DRAM",
        "--metadata_type=p2p",
        f"--seg_name={args.host}:{args.port}",
        f"--rpc_server_port={args.port}",
        "--total_buffer_size=1073741824",
    ]
    if args.role == "server":
        env = dict(os.environ, MC_TENT_CONF=json.dumps(base))
        (args.output / "server-config.json").write_text(
            json.dumps(base, indent=2) + "\n"
        )
        if args.dry_run:
            print(json.dumps(command))
            return
        os.execvpe(command[0], command, env)

    command += [
        f"--target_seg_name={args.peer}",
        "--op_type=write",
        "--start_block_size=67108864",
        "--max_block_size=67108864",
        "--start_batch_size=1",
        "--max_batch_size=1",
        f"--start_num_threads={args.threads}",
        f"--max_num_threads={args.threads}",
        f"--duration={args.seconds}",
        "--request_interval_us=0",
    ]
    policies = [("inverse_score", 0.01), ("virtual_load", 0.01)]
    if args.baselines or args.static_baseline:
        rdma["batch_capacity_gbps"] = [float(v) for v in args.capacity_gbps.split(",")]
        policies.append(("static_capacity", 0.01))
    if args.baselines:
        policies += [
            ("inverse_score", 0.5),
            ("inverse_score", 0.9),
        ]
    for repeat in range(args.repeats):
        order = policies if repeat % 2 == 0 else list(reversed(policies))
        for position, (policy, alpha) in enumerate(order, 1):
            name = f"r{repeat + 1}-p{position}-{policy}-a{alpha}"
            run = args.output / name
            run.mkdir()  # Keep prior measurements intact.
            rdma.update(batch_allocation_policy=policy, bandwidth_learning_rate=alpha)
            (run / "config.json").write_text(json.dumps(base, indent=2) + "\n")
            env = dict(os.environ, MC_TENT_CONF=json.dumps(base))
            invocation = ["/usr/bin/time", "-v", "-o", str(run / "cpu.txt")]
            if native:
                path = run / "workload.json"
                path.write_text(
                    json.dumps(native_spec(run / "native", policy), indent=2) + "\n"
                )
                invocation += native_command(path)
            else:
                invocation += command + [
                    f"--result_output_jsonl={run / 'metrics.jsonl'}"
                ]
            if args.dry_run:
                (run / "command.json").write_text(
                    json.dumps(invocation, indent=2) + "\n"
                )
                print(name, flush=True)
                continue
            started = time.time()
            with (run / "output.log").open("w") as output:
                result = subprocess.run(
                    invocation,
                    env=env,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    check=False,
                )
            record = dict(
                repeat=repeat + 1,
                position=position,
                policy=policy,
                alpha=alpha,
                command=invocation,
                exit_code=result.returncode,
                started_unix=started,
                wall_seconds=time.time() - started,
                workload=args.workload if native else "closed_loop_64MiB_write",
                directory=str(run),
            )
            if native and (run / "native" / "summary.json").exists():
                record["result"] = json.loads(
                    (run / "native" / "summary.json").read_text(encoding="utf-8")
                )
            with (args.output / "runs.jsonl").open("a") as manifest:
                manifest.write(json.dumps(record) + "\n")
            print(json.dumps(record), flush=True)
            if result.returncode:
                raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
