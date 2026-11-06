#!/usr/bin/env python3
"""Reserve Ascend Store memory before model loading and admit zero-size clients."""

import argparse
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

GIB = 1 << 30
STOP = threading.Event()


def process_token(pid):
    """Linux process birth token; a recycled PID must not satisfy admission."""
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    if fields[0] == "Z":
        raise RuntimeError(f"memory owner {pid} is a zombie")
    return fields[19]


def write_state(path, state):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def owned_capacities(text, names):
    """Read only this reservation's segment series, never master-wide totals."""
    result = dict.fromkeys(names, 0)
    seen = set()
    for line in text.splitlines():
        match = re.fullmatch(
            r"segment_total_capacity_bytes\{(.*)\}\s+(\S+)(?:\s+\d+)?", line
        )
        if not match:
            continue
        label = re.search(r'(?:^|,)\s*segment="((?:\\.|[^"\\])*)"(?:,|$)', match[1])
        if not label:
            continue
        name = json.loads('"' + label[1] + '"')
        if name not in result:
            continue
        if name in seen:
            raise ValueError(f"duplicate capacity series for {name}")
        value = Decimal(match[2])
        if not value.is_finite() or value < 0 or value != int(value):
            raise ValueError(f"invalid byte capacity for {name}")
        result[name] = int(value)
        seen.add(name)
    return result


def fetch_metrics(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


def require_capacity(state, text, minimum=0):
    names = [owner["segment"] for owner in state["owners"]]
    if len(names) != len(set(names)) or len(names) != state["devices"]:
        raise RuntimeError("reservation has missing or duplicate owners")
    capacities = owned_capacities(text, names)
    floor = state["per_device_bytes"]
    if any(value < floor for value in capacities.values()):
        raise RuntimeError(
            f"reservation is short: {capacities}; each owner requires {floor} bytes"
        )
    actual = sum(capacities.values())
    if actual < minimum:
        raise RuntimeError(f"owned capacity {actual} is below required {minimum} bytes")
    return actual


def refresh(state, path):
    try:
        actual = require_capacity(state, fetch_metrics(state["metrics_url"]))
        state.update(ready=True, capacity_bytes=actual, error=None)
    except Exception as exc:
        # Observability loss closes admission, but must not deallocate the pool.
        state.update(ready=False, error=str(exc))
    write_state(path, state)


def monitor(
    state, path, processes, interval, startup_deadline, release_after_ready=False
):
    admitted = False
    while not STOP.is_set():
        if any(process.poll() is not None for process in processes):
            raise RuntimeError("a memory owner exited")
        refresh(state, path)
        admitted = admitted or state["ready"]
        if release_after_ready and state["ready"]:
            return
        if not admitted and time.monotonic() >= startup_deadline:
            raise TimeoutError(f"full reservation not ready: {state['error']}")
        STOP.wait(interval)


def confirm_released(state, processes, timeout=60):
    if any(process.poll() != 0 for process in processes):
        raise RuntimeError("a warmup owner did not exit cleanly")
    names = [owner["segment"] for owner in state["owners"]]
    deadline = time.monotonic() + timeout
    while True:
        try:
            capacities = owned_capacities(fetch_metrics(state["metrics_url"]), names)
            if all(value == 0 for value in capacities.values()):
                return
        except Exception as exc:
            error = str(exc)
        else:
            error = f"warmup segments remain mounted: {capacities}"
        if time.monotonic() >= deadline:
            raise RuntimeError(f"warmup release not confirmed: {error}")
        time.sleep(1)


def contribute(args):
    import acl

    store = None
    initialized = False
    selected = False
    try:
        if acl.init() != 0:
            raise RuntimeError("acl.init failed")
        initialized = True
        if acl.rt.set_device(args.device) != 0:
            raise RuntimeError(f"cannot select device {args.device}")
        selected = True
        os.environ["ASCEND_ENABLE_USE_FABRIC_MEM"] = "1"
        from mooncake.store import MooncakeDistributedStore

        store = MooncakeDistributedStore()
        config = json.loads(Path(args.config).read_text())
        result = store.setup(**config)
        if result != 0:
            raise RuntimeError(f"Store setup failed: {result}")
        if hasattr(signal, "pthread_sigmask"):
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM})
        if not STOP.is_set():
            owner = dict(
                pid=os.getpid(),
                token=process_token(os.getpid()),
                segment=store.get_hostname(),
            )
            os.write(args.notify_fd, (json.dumps(owner) + "\n").encode())
            os.close(args.notify_fd)
            STOP.wait()
    finally:
        if store is not None:
            store.close()
        if selected:
            acl.rt.reset_device(args.device)
        if initialized:
            acl.finalize()


def reserve(args):
    directory = Path(args.state_dir).resolve()
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    path = directory / "reservation.json"
    state = dict(
        ready=False,
        controller_pid=os.getpid(),
        controller_token=process_token(os.getpid()),
        devices=len(args.devices),
        per_device_bytes=args.per_device_gib * GIB,
        owners=[],
        metrics_url=args.metrics_url,
    )
    config = dict(
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        global_segment_size=state["per_device_bytes"],
        local_buffer_size=0,
        protocol="ascend",
        rdma_devices="",
        master_server_addr=args.master,
    )
    write_state(directory / "contributor.json", config)
    consumer = dict(
        local_hostname=args.local_hostname,
        metadata_server=args.metadata_server,
        master_server_address=args.master,
        global_segment_size=0,
        local_buffer_size=0,
        protocol="ascend",
        device_name="",
        preferred_segment=False,
        prefer_alloc_in_same_node=True,
    )
    write_state(directory / "consumer.json", consumer)
    state["consumer_config"] = consumer
    processes, logs = [], []
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + args.startup_timeout
    warmed = False
    write_state(path, state)
    try:
        for device in args.devices:
            if STOP.is_set():
                return
            reader, writer = os.pipe()
            log = (directory / f"device-{device}.log").open("w")
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "_contribute",
                    "--config",
                    str(directory / "contributor.json"),
                    "--device",
                    str(device),
                    "--notify-fd",
                    str(writer),
                ],
                pass_fds=(writer,),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            os.close(writer)
            selector.register(reader, selectors.EVENT_READ)
            processes.append(process)
            logs.append(log)
        while selector.get_map() and not STOP.is_set():
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("memory owner exited before reporting its segment")
            if time.monotonic() >= deadline:
                raise TimeoutError("physical allocation deadline exceeded")
            for key, _ in selector.select(timeout=1):
                message = os.read(key.fd, 65536)
                if not message:
                    raise RuntimeError("memory owner closed its startup channel")
                state["owners"].append(json.loads(message))
                selector.unregister(key.fd)
                os.close(key.fd)
        if not STOP.is_set():
            monitor(
                state,
                path,
                processes,
                args.poll_interval,
                deadline,
                args.release_after_ready,
            )
            warmed = state["ready"] if args.release_after_ready else False
    finally:
        state.update(ready=False, stopping=True)
        write_state(path, state)
        for process in processes:
            if process.poll() is None:
                process.terminate()
        end = time.monotonic() + 30
        for process in processes:
            try:
                process.wait(timeout=max(0.1, end - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
        end = time.monotonic() + 10
        for process in processes:
            try:
                process.wait(timeout=max(0.1, end - time.monotonic()))
            except subprocess.TimeoutExpired:
                print(
                    f"owner {process.pid} remains in a kernel call; keep application stopped",
                    file=sys.stderr,
                )
        for key in list(selector.get_map().values()):
            os.close(key.fd)
        selector.close()
        for log in logs:
            log.close()
    if args.release_after_ready:
        if not warmed:
            raise RuntimeError("warmup stopped before the full capacity was mounted")
        confirm_released(state, processes)
        write_state(
            directory / "warmup.json",
            dict(status="PASS", held_bytes=state["capacity_bytes"], released=True),
        )


def check_admission(directory, minimum):
    state = json.loads((directory / "reservation.json").read_text())
    if not state["ready"] or state.get("stopping"):
        raise RuntimeError(f"reservation not ready: {state.get('error')}")
    consumer = json.loads((directory / "consumer.json").read_text())
    if consumer != state["consumer_config"]:
        raise RuntimeError("consumer configuration differs from the reservation")
    for owner in [
        dict(pid=state["controller_pid"], token=state["controller_token"]),
        *state["owners"],
    ]:
        if process_token(owner["pid"]) != owner["token"]:
            raise RuntimeError("memory owner identity changed")
    return require_capacity(state, fetch_metrics(state["metrics_url"]), minimum)


def admit(args):
    directory = Path(args.state_dir).resolve()
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            actual = check_admission(directory, args.minimum_gib * GIB)
            break
        except Exception as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"capacity admission denied: {exc}") from exc
            time.sleep(1)
    consumer = json.loads((directory / "consumer.json").read_text())
    master = consumer["master_server_address"]
    if os.environ.get("MOONCAKE_MASTER", master) != master:
        raise RuntimeError("inherited MOONCAKE_MASTER conflicts with the reservation")
    print(f"owned capacity admitted: {actual} bytes", flush=True)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if command:
        env = dict(
            os.environ,
            MOONCAKE_CONFIG_PATH=str(directory / "consumer.json"),
            MOONCAKE_GLOBAL_SEGMENT_SIZE="0",
            MOONCAKE_MASTER=master,
        )
        os.execvpe(command[0], command, env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    reservation = commands.add_parser("reserve")
    reservation.add_argument("--state-dir", required=True)
    reservation.add_argument("--local-hostname", required=True)
    reservation.add_argument("--master", required=True)
    reservation.add_argument("--metadata-server", required=True)
    reservation.add_argument("--metrics-url", required=True)
    reservation.add_argument(
        "--devices",
        required=True,
        type=lambda value: [int(x) for x in value.split(",")],
    )
    reservation.add_argument("--per-device-gib", type=int, default=30)
    reservation.add_argument("--startup-timeout", type=float, default=600)
    reservation.add_argument("--poll-interval", type=float, default=5)
    reservation.add_argument(
        "--release-after-ready",
        action="store_true",
        help="release full warmup capacity",
    )
    admission = commands.add_parser("admit")
    admission.add_argument("--state-dir", required=True)
    admission.add_argument("--minimum-gib", type=int, default=0)
    admission.add_argument("--timeout", type=float, default=0)
    admission.add_argument("command", nargs=argparse.REMAINDER)
    worker = commands.add_parser("_contribute", help=argparse.SUPPRESS)
    worker.add_argument("--config", required=True)
    worker.add_argument("--device", type=int, required=True)
    worker.add_argument("--notify-fd", type=int, required=True)
    args = parser.parse_args()
    if args.action == "reserve":
        if (
            not args.devices
            or len(set(args.devices)) != len(args.devices)
            or min(args.devices) < 0
            or args.per_device_gib <= 0
            or args.startup_timeout <= 0
            or args.poll_interval <= 0
        ):
            parser.error(
                "devices must be unique/nonnegative and sizes/timeouts positive"
            )
        reserve(args)
    elif args.action == "admit":
        if args.minimum_gib < 0 or args.timeout < 0:
            parser.error("capacity and timeout must be nonnegative")
        admit(args)
    else:
        contribute(args)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    main()
