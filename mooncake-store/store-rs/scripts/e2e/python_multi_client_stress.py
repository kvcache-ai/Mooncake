#!/usr/bin/env python3
from __future__ import annotations

import multiprocessing as mp
import os
import queue
import signal
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON_ROOT = REPO_ROOT / "python"
if str(PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_ROOT))

from mooncake.store import MooncakeDistributedStore, ReplicateConfig  # noqa: E402


LEASE_MS = 30_000


@dataclass
class StressConfig:
    local_hostname: str
    redis_url: str
    storage_clients: int
    writer_clients: int
    storage_bytes: int
    storage_scratch_bytes: int
    writer_storage_bytes: int
    writer_scratch_bytes: int
    phase_pause_ms: int
    membership_wait_ms: int
    visibility_wait_ms: int
    ready_timeout_ms: int
    child_timeout_ms: int
    storage_heartbeat_ms: int
    value_size: int
    batch_size: int
    single_iters: int
    batch_iters: int
    warmup_iters: int
    replica_count: int
    tenant: str
    pool: str
    keyspace: str
    route_control: str
    phases: list[str]

    @classmethod
    def from_env(cls) -> "StressConfig":
        stamp = int(time.time() * 1000)
        keyspace = os.environ.get(
            "MC_STORE_RS_STRESS_KEYSPACE", f"mc/store-rs/stress/{stamp}"
        )
        phases_raw = os.environ.get(
            "MC_STORE_RS_STRESS_PHASES", "put,get,batch-put,batch-get"
        )
        phases = [phase.strip() for phase in phases_raw.split(",") if phase.strip()]
        if not phases:
            phases = ["put", "get", "batch-put", "batch-get"]
        return cls(
            local_hostname=os.environ.get(
                "MC_STORE_RS_STRESS_LOCAL_HOSTNAME", "127.0.0.1"
            ),
            redis_url=os.environ.get(
                "MC_STORE_RS_REDIS_URL", "redis://127.0.0.1:6380/0"
            ),
            storage_clients=_env_int("MC_STORE_RS_STRESS_STORAGE_CLIENTS", 4),
            writer_clients=_env_int("MC_STORE_RS_STRESS_WRITER_CLIENTS", 8),
            storage_bytes=_env_int(
                "MC_STORE_RS_STRESS_STORAGE_BYTES", 256 * 1024 * 1024
            ),
            storage_scratch_bytes=_env_int(
                "MC_STORE_RS_STRESS_STORAGE_SCRATCH_BYTES", 16 * 1024 * 1024
            ),
            writer_storage_bytes=_env_int("MC_STORE_RS_STRESS_WRITER_STORAGE_BYTES", 0),
            writer_scratch_bytes=_env_int(
                "MC_STORE_RS_STRESS_WRITER_SCRATCH_BYTES", 16 * 1024 * 1024
            ),
            phase_pause_ms=_env_int("MC_STORE_RS_STRESS_PHASE_PAUSE_MS", 1_000),
            membership_wait_ms=_env_int("MC_STORE_RS_STRESS_MEMBERSHIP_WAIT_MS", 500),
            visibility_wait_ms=_env_int("MC_STORE_RS_STRESS_VISIBILITY_WAIT_MS", 200),
            ready_timeout_ms=_env_int("MC_STORE_RS_STRESS_READY_TIMEOUT_MS", 30_000),
            child_timeout_ms=_env_int("MC_STORE_RS_STRESS_CHILD_TIMEOUT_MS", 180_000),
            storage_heartbeat_ms=_env_int("MC_STORE_RS_STRESS_HEARTBEAT_MS", 30_000),
            value_size=_env_int("MC_STORE_RS_STRESS_VALUE_SIZE", 4096),
            batch_size=_env_int("MC_STORE_RS_STRESS_BATCH_SIZE", 32),
            single_iters=_env_int("MC_STORE_RS_STRESS_SINGLE_ITERS", 256),
            batch_iters=_env_int("MC_STORE_RS_STRESS_BATCH_ITERS", 128),
            warmup_iters=_env_int("MC_STORE_RS_STRESS_WARMUP_ITERS", 16),
            replica_count=_env_int("MC_STORE_RS_STRESS_REPLICA_COUNT", 1),
            tenant=os.environ.get("MC_STORE_RS_STRESS_TENANT", "default"),
            pool=os.environ.get("MC_STORE_RS_STRESS_POOL", "stress-pool"),
            keyspace=keyspace,
            route_control=os.environ.get(
                "MC_STORE_RS_STRESS_ROUTE_CONTROL", "metadata_only"
            ),
            phases=phases,
        )


def main() -> int:
    mp.freeze_support()
    ctx = mp.get_context("spawn")
    config = StressConfig.from_env()
    print_config(config)

    storage_cluster = StorageCluster.start(ctx, config)
    results: list[dict[str, Any]] = []
    try:
        sleep_ms(config.membership_wait_ms)
        remaining = list(config.phases)
        while remaining:
            phase = remaining.pop(0)
            result = run_phase(ctx, config, phase)
            results.append(result)
            print_phase(config, phase, result)
            if remaining:
                sleep_ms(config.phase_pause_ms)
    finally:
        storage_cluster.stop()
    print_bandwidth_summary(results)
    return 0


class StorageCluster:
    def __init__(
        self, processes: list[mp.Process], stop_events: list[mp.Event]
    ) -> None:
        self.processes = processes
        self.stop_events = stop_events

    @classmethod
    def start(cls, ctx: Any, config: StressConfig) -> "StorageCluster":
        ready_queue = ctx.Queue()
        processes: list[mp.Process] = []
        stop_events: list[mp.Event] = []
        for index in range(config.storage_clients):
            stop_event = ctx.Event()
            process = ctx.Process(
                target=storage_process_main,
                args=(config, index, ready_queue, stop_event),
                name=f"stress-storage-{index}",
            )
            process.start()
            processes.append(process)
            stop_events.append(stop_event)
        wait_ready(
            processes, ready_queue, config.ready_timeout_ms, config.storage_clients
        )
        return cls(processes, stop_events)

    def stop(self) -> None:
        for event in self.stop_events:
            event.set()
        for process in self.processes:
            process.join(timeout=2)
            if process.is_alive():
                process.terminate()
        for process in self.processes:
            process.join(timeout=2)


def run_phase(ctx: Any, config: StressConfig, phase: str) -> dict[str, Any]:
    if phase == "put":
        run = run_workers(
            ctx, config, phase, "put-worker", put_worker_main, measured=True
        )
        return finalize_phase(phase, config, run)
    if phase == "get":
        run = run_workers(
            ctx, config, phase, "rw-worker", get_worker_main, measured=True
        )
        return finalize_phase(phase, config, run)
    if phase == "batch-put":
        run = run_workers(
            ctx, config, phase, "batch-put-worker", batch_put_worker_main, measured=True
        )
        return finalize_phase(phase, config, run)
    if phase == "batch-get":
        run = run_workers(
            ctx, config, phase, "rw-worker", batch_get_worker_main, measured=True
        )
        return finalize_phase(phase, config, run)
    raise ValueError(f"unsupported phase {phase}")


def run_workers(
    ctx: Any,
    config: StressConfig,
    phase: str,
    role: str,
    target,
    *,
    measured: bool,
) -> dict[str, Any]:
    message_queue = ctx.Queue()
    start_event = ctx.Event()
    processes: list[mp.Process] = []
    phase_start = time.perf_counter()
    for worker_index in range(config.writer_clients):
        process = ctx.Process(
            target=worker_entry,
            args=(
                target,
                config,
                phase,
                role,
                worker_index,
                message_queue,
                start_event,
                measured,
            ),
            name=f"{role}-{worker_index}",
        )
        process.start()
        processes.append(process)

    ready: dict[int, dict[str, Any]] = {}
    result_map: dict[int, dict[str, Any]] = {}
    deadline = time.time() + config.child_timeout_ms / 1000.0
    while measured and len(ready) < config.writer_clients:
        remaining = deadline - time.time()
        if remaining <= 0:
            terminate_processes(processes)
            raise RuntimeError(
                f"{role} readiness timed out after {config.child_timeout_ms} ms"
            )
        try:
            message = message_queue.get(timeout=min(remaining, 0.2))
        except queue.Empty:
            raise_if_process_failed(processes, role)
            continue
        if "error" in message:
            terminate_processes(processes)
            raise RuntimeError(message["error"])
        kind = message.get("kind")
        if kind == "ready":
            ready[int(message["worker_index"])] = message
            continue
        if kind == "result":
            result_map[int(message["worker_index"])] = message["result"]
            continue

    measured_start = time.perf_counter()
    start_event.set()

    while measured and len(result_map) < config.writer_clients:
        remaining = deadline - time.time()
        if remaining <= 0:
            terminate_processes(processes)
            raise RuntimeError(f"{role} timed out after {config.child_timeout_ms} ms")
        try:
            message = message_queue.get(timeout=min(remaining, 0.2))
        except queue.Empty:
            raise_if_process_failed(processes, role)
            continue
        if "error" in message:
            terminate_processes(processes)
            raise RuntimeError(message["error"])
        kind = message.get("kind")
        if kind == "ready":
            ready[int(message["worker_index"])] = message
            continue
        if kind == "result":
            result_map[int(message["worker_index"])] = message["result"]

    ordered_results: list[dict[str, Any]] = []
    for worker_index in range(config.writer_clients):
        result = result_map.get(worker_index)
        if result is None and measured:
            raise RuntimeError(
                f"{role} expected result for worker={worker_index}, got {sorted(result_map)}"
            )
        if result is not None:
            ordered_results.append(result)
    wait_processes(processes, config.child_timeout_ms, role)
    phase_end = time.perf_counter()
    return {
        "results": ordered_results,
        "phase_wall_s": phase_end - phase_start,
        "prepare_wall_s": measured_start - phase_start,
        "measured_wall_s": phase_end - measured_start,
        "ready": [ready[index] for index in sorted(ready)],
    }


def worker_entry(
    target,
    config: StressConfig,
    phase: str,
    role: str,
    worker_index: int,
    message_queue,
    start_event,
    measured: bool,
) -> None:
    try:
        result = target(
            config,
            phase,
            role,
            worker_index,
            message_queue,
            start_event,
            measured,
        )
        if measured:
            message_queue.put(
                {
                    "kind": "result",
                    "worker_index": worker_index,
                    "result": result,
                }
            )
    except Exception:
        message_queue.put(
            {"error": "".join(traceback.format_exception(*sys.exc_info()))}
        )
        raise


def storage_process_main(
    config: StressConfig, index: int, ready_queue, stop_event: mp.Event
) -> None:
    store = None
    try:
        store = build_store(
            config,
            stable_id=f"stress-storage-{index}",
            storage_bytes=config.storage_bytes,
            scratch_bytes=config.storage_scratch_bytes,
            labels={"pool": config.pool, "role": "storage", "storage": "true"},
            routed_writes=False,
        )
        ready_queue.put({"status": "ready", "index": index})
        next_heartbeat = time.time() + config.storage_heartbeat_ms / 1000.0
        while not stop_event.is_set():
            now = time.time()
            if now >= next_heartbeat:
                expires_at_ms = int(now * 1000) + LEASE_MS
                status = int(store.heartbeat(expires_at_ms))
                if status != 0:
                    raise RuntimeError(
                        f"storage heartbeat failed index={index} status={status}"
                    )
                next_heartbeat = now + config.storage_heartbeat_ms / 1000.0
                continue
            stop_event.wait(timeout=min(next_heartbeat - now, 0.1))
    except Exception:
        ready_queue.put(
            {"status": "error", "index": index, "traceback": traceback.format_exc()}
        )
        raise


def put_worker_main(
    config: StressConfig,
    phase: str,
    role: str,
    worker_index: int,
    message_queue,
    start_event,
    measured: bool,
) -> dict[str, Any]:
    worker_start = time.perf_counter()
    setup_start = worker_start
    store = build_store(
        config,
        stable_id=f"stress-{phase}-{role}-{worker_index}",
        storage_bytes=rw_storage_bytes(config),
        scratch_bytes=config.writer_scratch_bytes,
        labels={"pool": config.pool, "role": role, "storage": "false"},
        routed_writes=True,
    )
    setup_s = time.perf_counter() - setup_start
    value = payload(f"{phase}-worker-{worker_index}", config.value_size)
    replicate = remote_only_replication(config)
    latencies: list[int] = []
    prepare_start = time.perf_counter()
    for index in range(config.warmup_iters):
        key = f"{phase}/warmup/{worker_index}/{index}"
        ensure_status(store.put(key, value, config=replicate), f"put warmup {key}")
    prepare_s = time.perf_counter() - prepare_start
    if measured:
        message_queue.put(
            {
                "kind": "ready",
                "worker_index": worker_index,
                "setup_s": setup_s,
                "prepare_s": prepare_s,
            }
        )
        start_event.wait()
    measured_start = time.perf_counter()
    for index in range(config.single_iters):
        key = f"{phase}/bench/{worker_index}/{index}"
        request_start = time.perf_counter_ns()
        ensure_status(store.put(key, value, config=replicate), f"put {key}")
        latencies.append((time.perf_counter_ns() - request_start) // 1_000)
    measured_s = time.perf_counter() - measured_start
    return worker_result(
        latencies,
        config.single_iters,
        config.single_iters,
        config.single_iters * config.value_size,
        setup_s=setup_s,
        prepare_s=prepare_s,
        measured_s=measured_s,
        worker_wall_s=time.perf_counter() - worker_start,
    )


def get_worker_main(
    config: StressConfig,
    phase: str,
    role: str,
    worker_index: int,
    message_queue,
    start_event,
    measured: bool,
) -> dict[str, Any]:
    worker_start = time.perf_counter()
    setup_start = worker_start
    store = build_store(
        config,
        stable_id=f"stress-{phase}-{role}-{worker_index}",
        storage_bytes=rw_storage_bytes(config),
        scratch_bytes=config.writer_scratch_bytes,
        labels={"pool": config.pool, "role": role, "storage": "false"},
        routed_writes=True,
    )
    setup_s = time.perf_counter() - setup_start
    value = payload(f"{phase}-worker-{worker_index}", config.value_size)
    replicate = remote_only_replication(config)
    keys = build_single_keys(
        phase, worker_index, config.warmup_iters + config.single_iters
    )
    prepare_start = time.perf_counter()
    for key in keys:
        ensure_status(store.put(key, value, config=replicate), f"get preload {key}")
    sleep_ms(config.visibility_wait_ms)
    for key in keys[: config.warmup_iters]:
        ensure_length(get_with_retry(store, key), config.value_size, key)
    prepare_s = time.perf_counter() - prepare_start
    if measured:
        message_queue.put(
            {
                "kind": "ready",
                "worker_index": worker_index,
                "setup_s": setup_s,
                "prepare_s": prepare_s,
            }
        )
        start_event.wait()
    measured_start = time.perf_counter()
    latencies: list[int] = []
    for key in keys[config.warmup_iters :]:
        request_start = time.perf_counter_ns()
        value = get_with_retry(store, key)
        latencies.append((time.perf_counter_ns() - request_start) // 1_000)
        ensure_length(value, config.value_size, key)
    measured_s = time.perf_counter() - measured_start
    return worker_result(
        latencies,
        config.single_iters,
        config.single_iters,
        config.single_iters * config.value_size,
        setup_s=setup_s,
        prepare_s=prepare_s,
        measured_s=measured_s,
        worker_wall_s=time.perf_counter() - worker_start,
    )


def batch_put_worker_main(
    config: StressConfig,
    phase: str,
    role: str,
    worker_index: int,
    message_queue,
    start_event,
    measured: bool,
) -> dict[str, Any]:
    worker_start = time.perf_counter()
    setup_start = worker_start
    store = build_store(
        config,
        stable_id=f"stress-{phase}-{role}-{worker_index}",
        storage_bytes=rw_storage_bytes(config),
        scratch_bytes=config.writer_scratch_bytes,
        labels={"pool": config.pool, "role": role, "storage": "false"},
        routed_writes=True,
    )
    setup_s = time.perf_counter() - setup_start
    value = payload(f"{phase}-worker-{worker_index}", config.value_size)
    replicate = remote_only_replication(config)
    prepare_start = time.perf_counter()
    for batch_keys in build_batch_keys(
        phase, worker_index, config.warmup_iters, config.batch_size
    ):
        items = [(key, value) for key in batch_keys]
        ensure_status(
            store.batch_put(items, config=replicate),
            f"batch-put warmup worker={worker_index}",
        )
    prepare_s = time.perf_counter() - prepare_start
    if measured:
        message_queue.put(
            {
                "kind": "ready",
                "worker_index": worker_index,
                "setup_s": setup_s,
                "prepare_s": prepare_s,
            }
        )
        start_event.wait()
    measured_start = time.perf_counter()
    latencies: list[int] = []
    for batch_keys in build_batch_keys(
        phase, worker_index, config.batch_iters, config.batch_size
    ):
        items = [(key, value) for key in batch_keys]
        request_start = time.perf_counter_ns()
        ensure_status(
            store.batch_put(items, config=replicate), f"batch-put worker={worker_index}"
        )
        latencies.append((time.perf_counter_ns() - request_start) // 1_000)
    objects = config.batch_iters * config.batch_size
    total_bytes = objects * config.value_size
    measured_s = time.perf_counter() - measured_start
    return worker_result(
        latencies,
        config.batch_iters,
        objects,
        total_bytes,
        setup_s=setup_s,
        prepare_s=prepare_s,
        measured_s=measured_s,
        worker_wall_s=time.perf_counter() - worker_start,
    )


def batch_get_worker_main(
    config: StressConfig,
    phase: str,
    role: str,
    worker_index: int,
    message_queue,
    start_event,
    measured: bool,
) -> dict[str, Any]:
    worker_start = time.perf_counter()
    setup_start = worker_start
    store = build_store(
        config,
        stable_id=f"stress-{phase}-{role}-{worker_index}",
        storage_bytes=rw_storage_bytes(config),
        scratch_bytes=config.writer_scratch_bytes,
        labels={"pool": config.pool, "role": role, "storage": "false"},
        routed_writes=True,
    )
    setup_s = time.perf_counter() - setup_start
    value = payload(f"{phase}-worker-{worker_index}", config.value_size)
    replicate = remote_only_replication(config)
    batches = build_batch_keys(
        phase,
        worker_index,
        config.warmup_iters + config.batch_iters,
        config.batch_size,
    )
    prepare_start = time.perf_counter()
    for batch_keys in batches:
        items = [(key, value) for key in batch_keys]
        ensure_status(
            store.batch_put(items, config=replicate),
            f"batch-get preload worker={worker_index}",
        )
    sleep_ms(config.visibility_wait_ms)
    for batch_keys in batches[: config.warmup_iters]:
        ensure_batch_lengths(
            batch_get_with_retry(store, batch_keys),
            config.value_size,
            len(batch_keys),
            batch_keys,
        )
    prepare_s = time.perf_counter() - prepare_start
    if measured:
        message_queue.put(
            {
                "kind": "ready",
                "worker_index": worker_index,
                "setup_s": setup_s,
                "prepare_s": prepare_s,
            }
        )
        start_event.wait()
    measured_start = time.perf_counter()
    latencies: list[int] = []
    for batch_keys in batches[config.warmup_iters :]:
        request_start = time.perf_counter_ns()
        values = batch_get_with_retry(store, batch_keys)
        latencies.append((time.perf_counter_ns() - request_start) // 1_000)
        ensure_batch_lengths(values, config.value_size, len(batch_keys), batch_keys)
    objects = config.batch_iters * config.batch_size
    total_bytes = objects * config.value_size
    measured_s = time.perf_counter() - measured_start
    return worker_result(
        latencies,
        config.batch_iters,
        objects,
        total_bytes,
        setup_s=setup_s,
        prepare_s=prepare_s,
        measured_s=measured_s,
        worker_wall_s=time.perf_counter() - worker_start,
    )


def build_store(
    config: StressConfig,
    *,
    stable_id: str,
    storage_bytes: int,
    scratch_bytes: int,
    labels: dict[str, str],
    routed_writes: bool,
) -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    status = int(
        store.setup(
            config.local_hostname,
            config.redis_url,
            storage_bytes,
            scratch_bytes,
            "tcp",
            "",
            "",
            stable_id=stable_id,
            tenant=config.tenant,
            labels=labels,
            routed_writes=routed_writes,
            replica_count=config.replica_count,
            keyspace=config.keyspace,
            expires_at_ms=int(time.time() * 1000) + LEASE_MS,
            route_control=config.route_control,
        )
    )
    if status != 0:
        store.close()
        raise RuntimeError(f"setup failed stable_id={stable_id} status={status}")
    return store


def remote_only_replication(config: StressConfig) -> ReplicateConfig:
    return ReplicateConfig(
        replica_num=config.replica_count,
        prefer_local=False,
    )


def rw_storage_bytes(config: StressConfig) -> int:
    return config.writer_storage_bytes


def worker_result(
    latencies: list[int],
    request_count: int,
    object_count: int,
    total_bytes: int,
    *,
    setup_s: float,
    prepare_s: float,
    measured_s: float,
    worker_wall_s: float,
) -> dict[str, Any]:
    return {
        "request_latencies_us": latencies,
        "request_count": request_count,
        "object_count": object_count,
        "total_bytes": total_bytes,
        "setup_s": setup_s,
        "prepare_s": prepare_s,
        "measured_s": measured_s,
        "worker_wall_s": worker_wall_s,
    }


def get_with_retry(store: MooncakeDistributedStore, key: str) -> bytes:
    last_error: Exception | None = None
    for _ in range(8):
        try:
            return store.get(key)
        except Exception as exc:
            last_error = exc
            sleep_ms(25)
    raise RuntimeError(f"get_with_retry exhausted for key={key}: {last_error}")


def batch_get_with_retry(
    store: MooncakeDistributedStore, keys: list[str]
) -> list[bytes]:
    last_error: Exception | None = None
    for _ in range(8):
        try:
            return store.batch_get(keys)
        except Exception as exc:
            last_error = exc
            sleep_ms(25)
    raise RuntimeError(f"batch_get_with_retry exhausted keys={keys!r}: {last_error}")


def wait_ready(
    processes: list[mp.Process], ready_queue, timeout_ms: int, expected: int
) -> None:
    deadline = time.time() + timeout_ms / 1000.0
    ready = 0
    while ready < expected:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise RuntimeError(
                f"storage clients did not become ready after {timeout_ms} ms"
            )
        try:
            message = ready_queue.get(timeout=min(remaining, 0.2))
        except queue.Empty:
            for process in processes:
                if process.exitcode not in (None, 0):
                    raise RuntimeError(
                        f"storage process {process.name} exited with code {process.exitcode}"
                    )
            continue
        if message["status"] == "ready":
            ready += 1
            continue
        raise RuntimeError(message["traceback"])


def terminate_processes(processes: list[mp.Process]) -> None:
    for process in processes:
        if process.is_alive():
            process.terminate()


def raise_if_process_failed(processes: list[mp.Process], role: str) -> None:
    for process in processes:
        if process.exitcode not in (None, 0):
            raise RuntimeError(
                f"{role} process {process.name} exited with code {process.exitcode}"
            )


def wait_processes(processes: list[mp.Process], timeout_ms: int, role: str) -> None:
    deadline = time.time() + timeout_ms / 1000.0
    alive = set(range(len(processes)))
    while alive:
        now = time.time()
        if now >= deadline:
            for index in alive:
                processes[index].terminate()
            raise RuntimeError(f"{role} timed out after {timeout_ms} ms")
        finished = []
        for index in alive:
            process = processes[index]
            process.join(timeout=0.05)
            if process.exitcode is None:
                continue
            if process.exitcode != 0:
                raise RuntimeError(
                    f"{role} process {process.name} exited with code {process.exitcode}"
                )
            finished.append(index)
        for index in finished:
            alive.remove(index)


def finalize_phase(
    phase: str, config: StressConfig, run: dict[str, Any]
) -> dict[str, Any]:
    workers = list(run["results"])
    latencies = sorted(
        latency for worker in workers for latency in worker["request_latencies_us"]
    )
    request_count = sum(int(worker["request_count"]) for worker in workers)
    object_count = sum(int(worker["object_count"]) for worker in workers)
    total_bytes = sum(int(worker["total_bytes"]) for worker in workers)
    setup_values = [float(worker["setup_s"]) for worker in workers]
    prepare_values = [float(worker["prepare_s"]) for worker in workers]
    measured_values = [float(worker["measured_s"]) for worker in workers]
    worker_wall_values = [float(worker["worker_wall_s"]) for worker in workers]
    avg_request_us = 0.0 if not latencies else sum(latencies) / len(latencies)
    return {
        "phase": phase,
        "request_count": request_count,
        "object_count": object_count,
        "total_bytes": total_bytes,
        "phase_wall_s": float(run["phase_wall_s"]),
        "prepare_wall_s": float(run["prepare_wall_s"]),
        "measured_wall_s": float(run["measured_wall_s"]),
        "worker_setup_avg_ms": average_seconds_to_ms(setup_values),
        "worker_prepare_avg_ms": average_seconds_to_ms(prepare_values),
        "worker_prepare_p95_ms": percentile_float_seconds_to_ms(prepare_values, 95),
        "worker_measured_avg_ms": average_seconds_to_ms(measured_values),
        "worker_measured_p95_ms": percentile_float_seconds_to_ms(measured_values, 95),
        "worker_wall_avg_ms": average_seconds_to_ms(worker_wall_values),
        "avg_request_us": avg_request_us,
        "p50_request_us": percentile(latencies, 50),
        "p95_request_us": percentile(latencies, 95),
        "p99_request_us": percentile(latencies, 99),
        "max_request_us": latencies[-1] if latencies else 0,
    }


def print_config(config: StressConfig) -> None:
    print(
        "stress config: "
        f"model=python-compat-multiprocess "
        f"storage_clients={config.storage_clients} "
        f"writer_clients={config.writer_clients} "
        f"value_size={config.value_size} "
        f"single_iters={config.single_iters} "
        f"batch_iters={config.batch_iters} "
        f"batch_size={config.batch_size} "
        f"warmup_iters={config.warmup_iters} "
        f"replica_count={config.replica_count} "
        f"route_control={config.route_control} "
        f"storage_bytes={config.storage_bytes} "
        f"rw_storage_bytes={rw_storage_bytes(config)} "
        f"rw_scratch_bytes={config.writer_scratch_bytes} "
        f"keyspace={config.keyspace}"
    )


def print_phase(config: StressConfig, phase: str, result: dict[str, Any]) -> None:
    phase_wall_s = float(result["phase_wall_s"])
    prepare_wall_s = float(result["prepare_wall_s"])
    measured_wall_s = float(result["measured_wall_s"])
    request_count = int(result["request_count"])
    object_count = int(result["object_count"])
    total_bytes = int(result["total_bytes"])
    phase_request_rate = 0.0 if phase_wall_s == 0 else request_count / phase_wall_s
    phase_object_rate = 0.0 if phase_wall_s == 0 else object_count / phase_wall_s
    phase_throughput_mib_s = (
        0.0 if phase_wall_s == 0 else total_bytes / phase_wall_s / (1024 * 1024)
    )
    measured_request_rate = (
        0.0 if measured_wall_s == 0 else request_count / measured_wall_s
    )
    measured_object_rate = (
        0.0 if measured_wall_s == 0 else object_count / measured_wall_s
    )
    measured_throughput_mib_s = (
        0.0 if measured_wall_s == 0 else total_bytes / measured_wall_s / (1024 * 1024)
    )
    batch_size = 1 if phase in ("put", "get") else config.batch_size
    print(
        "stress "
        f"phase={phase} "
        f"model=python-compat-multiprocess "
        f"storage_clients={config.storage_clients} "
        f"writer_clients={config.writer_clients} "
        f"batch_size={batch_size} "
        f"requests={request_count} "
        f"objects={object_count} "
        f"value_size={config.value_size} "
        f"phase_wall_s={phase_wall_s:.3f} "
        f"prepare_wall_s={prepare_wall_s:.3f} "
        f"measured_wall_s={measured_wall_s:.3f} "
        f"phase_request_rate={phase_request_rate:.2f} "
        f"phase_object_rate={phase_object_rate:.2f} "
        f"phase_throughput_mib_s={phase_throughput_mib_s:.2f} "
        f"measured_request_rate={measured_request_rate:.2f} "
        f"measured_object_rate={measured_object_rate:.2f} "
        f"measured_throughput_mib_s={measured_throughput_mib_s:.2f} "
        f"avg_request_us={result['avg_request_us']:.2f} "
        f"p50_request_us={result['p50_request_us']} "
        f"p95_request_us={result['p95_request_us']} "
        f"p99_request_us={result['p99_request_us']} "
        f"max_request_us={result['max_request_us']} "
        f"worker_setup_avg_ms={result['worker_setup_avg_ms']:.2f} "
        f"worker_prepare_avg_ms={result['worker_prepare_avg_ms']:.2f} "
        f"worker_prepare_p95_ms={result['worker_prepare_p95_ms']:.2f} "
        f"worker_measured_avg_ms={result['worker_measured_avg_ms']:.2f} "
        f"worker_measured_p95_ms={result['worker_measured_p95_ms']:.2f} "
        f"worker_wall_avg_ms={result['worker_wall_avg_ms']:.2f}"
    )


def print_bandwidth_summary(results: list[dict[str, Any]]) -> None:
    if not results:
        return
    print("steady-state bandwidth:")
    for result in results:
        phase = str(result["phase"])
        throughput_mib_s = measured_throughput_mib_s(result)
        print(f"- {phase}: {throughput_mib_s:.2f} MiB/s")


def measured_throughput_mib_s(result: dict[str, Any]) -> float:
    measured_wall_s = float(result["measured_wall_s"])
    total_bytes = int(result["total_bytes"])
    if measured_wall_s == 0.0:
        return 0.0
    return total_bytes / measured_wall_s / (1024 * 1024)


def build_single_keys(phase: str, worker_index: int, count: int) -> list[str]:
    return [f"{phase}/worker-{worker_index}/key-{index}" for index in range(count)]


def build_batch_keys(
    phase: str, worker_index: int, batches: int, batch_size: int
) -> list[list[str]]:
    return [
        [
            f"{phase}/worker-{worker_index}/batch-{batch_index}/item-{slot}"
            for slot in range(batch_size)
        ]
        for batch_index in range(batches)
    ]


def payload(seed: str, size: int) -> bytes:
    state = 0
    for byte in seed.encode():
        state = (state * 131 + byte + 17) & ((1 << 64) - 1)
    output = bytearray(size)
    for index in range(size):
        state = (state * 6364136223846793005 + index + 1) & ((1 << 64) - 1)
        output[index] = (state >> 24) % 251
    return bytes(output)


def ensure_status(status: Any, context: str) -> None:
    if int(status) != 0:
        raise RuntimeError(f"{context} failed with status={status}")


def ensure_length(value: bytes, expected: int, key: str) -> None:
    if len(value) != expected:
        raise RuntimeError(
            f"length mismatch key={key} actual={len(value)} expected={expected}"
        )


def ensure_batch_lengths(
    values: list[bytes], expected_size: int, expected_items: int, keys: list[str]
) -> None:
    if len(values) != expected_items:
        raise RuntimeError(
            f"batch item mismatch keys={keys!r} actual={len(values)} expected={expected_items}"
        )
    for key, value in zip(keys, values):
        ensure_length(value, expected_size, key)


def percentile(values: list[int], percentile_value: int) -> int:
    if not values:
        return 0
    rank = max(((len(values) * percentile_value + 99) // 100) - 1, 0)
    return values[min(rank, len(values) - 1)]


def average_seconds_to_ms(values: list[float]) -> float:
    if not values:
        return 0.0
    return (sum(values) / len(values)) * 1000.0


def percentile_float_seconds_to_ms(values: list[float], percentile_value: int) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    rank = max(((len(sorted_values) * percentile_value + 99) // 100) - 1, 0)
    return sorted_values[min(rank, len(sorted_values) - 1)] * 1000.0


def sleep_ms(value: int) -> None:
    if value > 0:
        time.sleep(value / 1000.0)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


if __name__ == "__main__":
    raise SystemExit(main())
