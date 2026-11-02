#!/usr/bin/env python3
"""Supervise passive, per-physical-GPU MNNVL EGM Store owners.

Run on the target Linux/CUDA host with an accessible IMEX channel:
  python scripts/launch_egm_owners.py --host 10.0.0.1 --rack rack1 \
    --fabric-domain fabric1 --master 10.0.0.2:50051 --gpus 0,1,2,3

Workers use the same MOONCAKE_FABRIC_DOMAIN/MC_RACK/MC_TRAY and their own
MC_PHYSICAL_GPU, or ReplicateConfig.requester_self_id + fabric_domain.
The parent imports no CUDA libraries. Each fresh child gets a single-device
CUDA_VISIBLE_DEVICES before importing Mooncake. Owners do not perform DMA or
serve retrieve RPCs; workers copy directly through TENT MNNVL.
"""

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time


def parse_gpus(value):
    try:
        gpus = [int(item) for item in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("GPU IDs must be physical integers") from exc
    if not gpus or any(gpu < 0 for gpu in gpus) or len(set(gpus)) != len(gpus):
        raise argparse.ArgumentTypeError("GPU IDs must be unique and nonnegative")
    return gpus


def owner_environment(args, gpu):
    env = os.environ.copy()
    # Readiness consumes native INFO markers from each child's captured stderr.
    # Inherited MC_LOG_DIR would redirect those markers into separate files.
    env.pop("MC_LOG_DIR", None)
    env.update(
        CUDA_DEVICE_ORDER="PCI_BUS_ID",
        CUDA_VISIBLE_DEVICES=str(gpu),
        MC_PHYSICAL_GPU=str(gpu),
        MC_RACK=args.rack,
        MC_TRAY=args.host,
        MOONCAKE_HOST_ID=args.host,
        MOONCAKE_FABRIC_DOMAIN=args.fabric_domain,
        MC_USE_TENT="1",
        MC_ENABLE_MNNVL="1",
        MC_MNNVL_EGM="1",
        MOONCAKE_STORE_BACKEND="cpp",
        GLOG_logtostderr="1",
        GLOG_minloglevel="0",
        MC_LOG_LEVEL="INFO",
        PYTHONUNBUFFERED="1",
    )
    return env


def run_owner(args):
    # This is reached only in a new, single-GPU child process.
    gpu = args.gpus[0]
    if os.environ.get("CUDA_VISIBLE_DEVICES") != str(gpu):
        raise RuntimeError("owner must be launched with CVD pinned before imports")
    from mooncake.store import MooncakeDistributedStore

    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    store = MooncakeDistributedStore()
    try:
        rc = store.setup(
            local_hostname=f"{args.host}:{args.base_port}",
            metadata_server=args.metadata,
            global_segment_size=int(args.segment_gib * 1024**3),
            local_buffer_size=0,
            protocol="mnnvl",
            rdma_devices="",
            master_server_addr=args.master,
        )
        if rc != 0:
            raise RuntimeError(f"Store setup failed: {rc}")
        # setup succeeds only after required export, synchronous TE metadata
        # publication, and master mount of every allocated segment.
        if store.health_check() != 0:
            raise RuntimeError("master is unavailable after mount")
        print(f"OWNER_READY gpu={gpu} domain={args.fabric_domain}", flush=True)
        while not stopped.wait(2):
            if store.health_check() != 0:
                raise RuntimeError("owner lost master connectivity")
    finally:
        rc = store.close()
        if rc != 0:
            raise RuntimeError(f"owner cleanup failed: {rc}")


def wait_ready(children, deadline, stopped):
    pending = list(children)
    while pending:
        if stopped.is_set():
            return False
        for process, log_path in list(pending):
            if process.poll() is not None:
                raise RuntimeError(f"owner exited before readiness; see {log_path}")
            content = log_path.read_text(errors="replace")
            if "MNNVL_SEGMENT_READY" in content and "OWNER_READY" in content:
                pending.remove((process, log_path))
        if pending and time.monotonic() >= deadline:
            raise TimeoutError(f"owners not ready: {[str(p) for _, p in pending]}")
        if pending:
            stopped.wait(0.25)
    return not stopped.is_set()


def supervise(args):
    if not Path("/dev/nvidia-caps-imex-channels/channel0").exists():
        raise RuntimeError("IMEX channel0 is missing; provision it before launching")
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    args.log_dir.mkdir(parents=True, exist_ok=True)
    children = []
    try:
        for index, gpu in enumerate(args.gpus):
            if stopped.is_set():
                return
            log_path = args.log_dir / f"owner_gpu{gpu}.log"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--owner",
                "--host",
                args.host,
                "--rack",
                args.rack,
                "--fabric-domain",
                args.fabric_domain,
                "--master",
                args.master,
                "--metadata",
                args.metadata,
                "--gpus",
                str(gpu),
                "--base-port",
                str(args.base_port + index),
                "--segment-gib",
                str(args.segment_gib),
            ]
            with log_path.open("w") as output:
                process = subprocess.Popen(
                    command,
                    env=owner_environment(args, gpu),
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
            children.append((process, log_path))
        if not wait_ready(children, time.monotonic() + args.ready_timeout, stopped):
            return
        print(f"All {len(children)} owners ready; logs: {args.log_dir}", flush=True)
        while not stopped.wait(1):
            for process, log_path in children:
                if process.poll() is not None:
                    raise RuntimeError(f"owner exited; see {log_path}")
    finally:
        # Terminate only children owned by this supervisor. Do not use pkill,
        # persistent PID files, or force-kill owners with live fabric mappings.
        for process, _ in children:
            if process.poll() is None:
                process.terminate()
        for process, log_path in children:
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                print(
                    f"Owner cleanup still running (pid {process.pid}); see {log_path}",
                    file=sys.stderr,
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="routable local IP or hostname")
    parser.add_argument("--rack", required=True)
    parser.add_argument(
        "--fabric-domain",
        required=True,
        help="operator-verified reachable MNNVL fabric domain",
    )
    parser.add_argument("--master", required=True)
    parser.add_argument("--metadata", default="P2PHANDSHAKE")
    parser.add_argument("--gpus", type=parse_gpus, default=parse_gpus("0,1,2,3"))
    parser.add_argument("--base-port", type=int, default=18060)
    parser.add_argument("--segment-gib", type=float, default=32)
    parser.add_argument("--ready-timeout", type=float, default=120)
    parser.add_argument(
        "--log-dir", type=Path, default=Path("/tmp/mooncake-egm-owners")
    )
    parser.add_argument("--owner", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not (0 < args.segment_gib < float("inf")) or not (
        0 < args.ready_timeout < float("inf")
    ):
        parser.error("segment size and readiness timeout must be positive")
    if not 0 < args.base_port <= 65535 - len(args.gpus) + 1:
        parser.error("owner ports must fit within 1..65535")
    if args.owner and len(args.gpus) != 1:
        parser.error("an owner requires exactly one GPU")
    (run_owner if args.owner else supervise)(args)


if __name__ == "__main__":
    main()
