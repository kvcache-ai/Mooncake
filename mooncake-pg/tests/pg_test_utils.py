import os
import signal
import socket
import time
import unittest
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Sequence

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import pg


DEVICE_FILTER_ENV_VAR = "MOONCAKE_PGTEST_DEVICE_FILTERS"
MASTER_ADDR_ENV_VAR = "MOONCAKE_PGTEST_MASTER_ADDR"
MASTER_PORT_ENV_VAR = "MOONCAKE_PGTEST_MASTER_PORT"
DEFAULT_MASTER_ADDR = "127.0.0.1"
DEFAULT_WAIT_TIMEOUT_S = 30.0
DEFAULT_SPAWN_TIMEOUT_S = 30.0


def parse_device_filters(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    filters = [item.strip() for item in raw.split(",") if item.strip()]
    return filters or None


def resolve_device_filters(
    device_filters: Sequence[str] | None = None,
) -> list[str] | None:
    if device_filters is not None:
        resolved = [item.strip() for item in device_filters if item.strip()]
        return resolved or None
    return parse_device_filters(os.getenv(DEVICE_FILTER_ENV_VAR))


def configure_mooncake_device_filter(
    device_filters: Sequence[str] | None = None,
) -> list[str] | None:
    resolved = resolve_device_filters(device_filters)
    if resolved is not None:
        pg.set_device_filter(resolved)
    return resolved


def resolve_env_value(env_var: str, default: str) -> str:
    value = os.getenv(env_var)
    if value:
        return value
    return default


def find_free_local_port() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((DEFAULT_MASTER_ADDR, 0))
        return str(sock.getsockname()[1])


@contextmanager
def temporary_env(updates: dict[str, str]):
    previous = {key: os.environ.get(key) for key in updates}
    os.environ.update(updates)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def cuda_runtime_available(min_devices: int = 1) -> bool:
    if not torch.cuda.is_available():
        return False
    return torch.cuda.device_count() >= min_devices


def require_test_device(rank: int, device_type: str) -> torch.device:
    if device_type == "cuda":
        device_count = torch.cuda.device_count()
        if device_count <= 0:
            raise RuntimeError(
                "CUDA backend requested but no CUDA devices are available"
            )
        if rank >= device_count:
            raise RuntimeError(
                f"rank {rank} requires a dedicated CUDA device but only {device_count} are visible"
            )
        torch.cuda.set_device(rank)
        return torch.device("cuda", rank)
    if device_type != "cpu":
        raise ValueError(f"unsupported device_type: {device_type}")
    return torch.device("cpu")


def mooncake_backend_options(
    world_size: int,
    device_type: str,
    *,
    active_value: int = 0,
    is_extension: bool = False,
) -> pg.MooncakeBackendOptions:
    device = torch.device(device_type)
    active_ranks = torch.full(
        (world_size,),
        int(active_value),
        dtype=torch.int32,
        device=device,
    )
    if is_extension:
        return pg.MooncakeBackendOptions(active_ranks, True)
    return pg.MooncakeBackendOptions(active_ranks)


def mooncake_cpu_options(world_size: int) -> pg.MooncakeBackendOptions:
    return mooncake_backend_options(world_size, "cpu", active_value=0)


def mooncake_extension_cpu_options(world_size: int) -> pg.MooncakeBackendOptions:
    return mooncake_backend_options(
        world_size,
        "cpu",
        active_value=1,
        is_extension=True,
    )


def init_mooncake_group(
    rank: int,
    world_size: int,
    *,
    backend_name: str,
    device_type: str,
    device_filters: Sequence[str] | None = None,
    use_pg_options: bool = True,
    is_extension: bool = False,
    active_value: int | None = None,
) -> torch.device:
    device = require_test_device(rank, device_type)
    configure_mooncake_device_filter(device_filters)
    kwargs = {
        "backend": backend_name,
        "rank": rank,
        "world_size": world_size,
    }
    if use_pg_options:
        resolved_active_value = (
            1 if is_extension else 0 if active_value is None else active_value
        )
        kwargs["pg_options"] = mooncake_backend_options(
            world_size,
            device_type,
            active_value=resolved_active_value,
            is_extension=is_extension,
        )
    dist.init_process_group(**kwargs)
    return device


def init_mooncake_cpu_group(
    rank: int,
    world_size: int,
    *,
    device_filters: Sequence[str] | None = None,
    use_pg_options: bool = True,
) -> None:
    init_mooncake_group(
        rank,
        world_size,
        backend_name="mooncake-cpu",
        device_type="cpu",
        device_filters=device_filters,
        use_pg_options=use_pg_options,
    )


def get_mooncake_backend(group=None, device_type: str = "cpu"):
    if group is None:
        group = dist.group.WORLD
    return group


@dataclass(slots=True)
class MooncakePGWorkerContext:
    proc_rank: int
    world_size: int
    result_map: object
    device_filters: Sequence[str] | None
    backend_name: str
    device_type: str
    _device: torch.device | None = None

    @property
    def rank(self) -> int:
        return self.proc_rank

    @property
    def device(self) -> torch.device:
        if self._device is None:
            raise RuntimeError("worker device is unavailable before init_group()")
        return self._device

    def init_group(
        self,
        *,
        rank: int | None = None,
        world_size: int | None = None,
        device_filters: Sequence[str] | None = None,
        use_pg_options: bool = True,
        is_extension: bool = False,
        active_value: int | None = None,
    ) -> torch.device:
        self._device = init_mooncake_group(
            self.proc_rank if rank is None else rank,
            self.world_size if world_size is None else world_size,
            backend_name=self.backend_name,
            device_type=self.device_type,
            device_filters=self.device_filters
            if device_filters is None
            else device_filters,
            use_pg_options=use_pg_options,
            is_extension=is_extension,
            active_value=active_value,
        )
        return self._device

    def get_backend(self, group=None):
        return get_mooncake_backend(group=group, device_type=self.device_type)

    def record_result(self, payload: dict) -> None:
        record_rank_result(
            self.result_map,
            self.proc_rank,
            {"ok": True, "rank": self.proc_rank, **payload},
        )

    def record_error(self, exc: Exception) -> None:
        record_rank_error(self.result_map, self.proc_rank, exc)

    def synchronize(self) -> None:
        if self.device_type == "cuda" and self._device is not None:
            torch.cuda.synchronize(self._device)


def destroy_process_groups(*groups) -> None:
    for group in reversed([item for item in groups if item is not None]):
        try:
            dist.destroy_process_group(group)
        except Exception:
            pass
    if dist.is_available() and dist.is_initialized():
        try:
            dist.destroy_process_group()
        except Exception:
            pass


def record_rank_result(result_map, rank: int, payload: dict) -> None:
    result_map[rank] = payload


def record_rank_error(result_map, rank: int, exc: Exception) -> None:
    record_rank_result(
        result_map,
        rank,
        {
            "ok": False,
            "rank": rank,
            "error_type": type(exc).__name__,
            "error": str(exc),
        },
    )


def collect_rank_results(result_map, count: int) -> list[dict]:
    results = []
    for rank in range(count):
        if rank in result_map:
            results.append(result_map[rank])
        else:
            results.append(
                {
                    "ok": False,
                    "rank": rank,
                    "error_type": "MissingResult",
                    "error": f"Rank {rank} did not report a result (process may have crashed or timed out)",
                }
            )
    return results


def finalize_worker_results(result_map, count: int, *groups) -> None:
    # Clean up process groups; result collection and timeouts are handled by the parent process.
    destroy_process_groups(*groups)


def _run_worker_with_finalizer(
    rank: int,
    world_size: int,
    result_map,
    device_filters,
    worker,
    args: tuple,
) -> None:
    try:
        worker(rank, world_size, result_map, device_filters, *args)
    except Exception as exc:  # noqa: BLE001
        record_rank_error(result_map, rank, exc)
    finally:
        finalize_worker_results(result_map, world_size)


def _run_backend_worker_with_finalizer(
    rank: int,
    world_size: int,
    result_map,
    device_filters,
    result_count: int,
    backend_name: str,
    device_type: str,
    worker,
    args: tuple,
) -> None:
    ctx = MooncakePGWorkerContext(
        proc_rank=rank,
        world_size=world_size,
        result_map=result_map,
        device_filters=device_filters,
        backend_name=backend_name,
        device_type=device_type,
    )
    try:
        worker(ctx, *args)
    except Exception as exc:  # noqa: BLE001
        record_rank_error(result_map, rank, exc)
    finally:
        finalize_worker_results(result_map, result_count)


def wait_until(
    predicate,
    *,
    timeout_s: float = DEFAULT_WAIT_TIMEOUT_S,
    poll_interval_s: float = 0.05,
    description: str = "condition",
):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(poll_interval_s)
    raise TimeoutError(f"timed out waiting for {description}")


def wait_for_spawn_context(ctx, timeout_s: float) -> None:
    """Wait for spawn context with timeout; force kill if hung."""
    deadline = time.monotonic() + timeout_s

    # Phase 1: Normal wait
    while time.monotonic() < deadline:
        if not any(p.is_alive() for p in ctx.processes):
            # All processes exited (success or failure)
            return
        time.sleep(0.1)

    # Phase 2: Timeout - try graceful termination
    for process in ctx.processes:
        if process.is_alive():
            try:
                process.terminate()
            except Exception:
                pass

    # Phase 3: Wait for terminations with hard timeout
    term_deadline = time.monotonic() + 5.0
    while time.monotonic() < term_deadline:
        if not any(p.is_alive() for p in ctx.processes):
            break
        time.sleep(0.1)

    # Phase 4: SIGKILL any survivors
    for process in ctx.processes:
        if process.is_alive():
            try:
                os.kill(process.pid, signal.SIGKILL)
            except Exception:
                pass

    # Phase 5: Final wait (don't block forever)
    final_deadline = time.monotonic() + 3.0
    while time.monotonic() < final_deadline:
        if not any(p.is_alive() for p in ctx.processes):
            break
        time.sleep(0.1)

    raise AssertionError(f"Spawn timed out after {timeout_s} seconds")


def spawn_and_collect(
    worker,
    world_size: int,
    *args,
    device_filters: Sequence[str] | None = None,
    nprocs: int | None = None,
    timeout_s: float | None = None,
) -> list[dict]:
    resolved_filters = resolve_device_filters(device_filters)
    master_addr = resolve_env_value(MASTER_ADDR_ENV_VAR, DEFAULT_MASTER_ADDR)
    default_master_port = find_free_local_port()
    master_port = resolve_env_value(MASTER_PORT_ENV_VAR, default_master_port)
    actual_nprocs = world_size if nprocs is None else nprocs
    spawn_ctx = mp.get_context("spawn")

    with spawn_ctx.Manager() as manager:
        result_map = manager.dict()

        with temporary_env(
            {
                "MASTER_ADDR": master_addr,
                "MASTER_PORT": master_port,
            }
        ):
            spawn_args = (
                world_size,
                result_map,
                resolved_filters,
                worker,
                args,
            )
            if timeout_s is None:
                mp.spawn(
                    _run_worker_with_finalizer,
                    args=spawn_args,
                    nprocs=actual_nprocs,
                    join=True,
                )
            else:
                ctx = mp.spawn(
                    _run_worker_with_finalizer,
                    args=spawn_args,
                    nprocs=actual_nprocs,
                    join=False,
                )
                wait_for_spawn_context(ctx, timeout_s)

        return collect_rank_results(result_map, actual_nprocs)


class MultiProcessTestCase(unittest.TestCase):
    world_size = 4
    device_filters = None
    spawn_timeout_s = DEFAULT_SPAWN_TIMEOUT_S

    def spawn_and_collect(
        self,
        worker,
        *args,
        device_filters=None,
        nprocs: int | None = None,
        timeout_s: float | None = None,
    ) -> list[dict]:
        resolved_filters = (
            self.device_filters if device_filters is None else device_filters
        )
        return spawn_and_collect(
            worker,
            self.world_size,
            *args,
            device_filters=resolved_filters,
            nprocs=nprocs,
            timeout_s=self.spawn_timeout_s if timeout_s is None else timeout_s,
        )

    def assert_all_ok(self, rows: list[dict]) -> None:
        for row in rows:
            if not row.get("ok", False):
                self.fail(
                    f"rank {row.get('rank', '?')} failed with "
                    f"{row.get('error_type', 'UnknownError')}: {row.get('error', '')}"
                )


class BackendMultiProcessTestCase(MultiProcessTestCase):
    backend_name: str | None = None
    device_type: str | None = None

    @classmethod
    def configure_for_cuda_device_count(cls, device_count: int) -> None:
        del device_count

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if cls.backend_name is None or cls.device_type is None:
            raise RuntimeError(
                f"{cls.__name__} must inherit a concrete Mooncake PG backend test base class"
            )
        if cls.device_type == "cuda":
            device_count = torch.cuda.device_count() if torch.cuda.is_available() else 0
            cls.configure_for_cuda_device_count(device_count)
        if cls.device_type == "cuda" and not cuda_runtime_available(cls.world_size):
            raise unittest.SkipTest(
                f"{cls.__name__} requires {cls.world_size} visible CUDA devices"
            )

    def spawn_backend_and_collect(
        self,
        worker,
        *args,
        device_filters=None,
        nprocs: int | None = None,
        timeout_s: float | None = None,
        world_size: int | None = None,
    ) -> list[dict]:
        if self.backend_name is None or self.device_type is None:
            raise RuntimeError(
                f"{type(self).__name__} must inherit a concrete Mooncake PG backend test base class"
            )

        resolved_filters = (
            self.device_filters if device_filters is None else device_filters
        )
        resolved_world_size = (
            self.world_size if world_size is None else world_size
        )
        master_addr = resolve_env_value(MASTER_ADDR_ENV_VAR, DEFAULT_MASTER_ADDR)
        default_master_port = find_free_local_port()
        master_port = resolve_env_value(MASTER_PORT_ENV_VAR, default_master_port)
        actual_nprocs = resolved_world_size if nprocs is None else nprocs
        spawn_ctx = mp.get_context("spawn")

        with spawn_ctx.Manager() as manager:
            result_map = manager.dict()

            with temporary_env(
                {
                    "MASTER_ADDR": master_addr,
                    "MASTER_PORT": master_port,
                }
            ):
                spawn_args = (
                    resolved_world_size,
                    result_map,
                    resolved_filters,
                    actual_nprocs,
                    self.backend_name,
                    self.device_type,
                    worker,
                    args,
                )
                # Always use non-blocking spawn with timeout to avoid hangs
                resolved_timeout = self.spawn_timeout_s if timeout_s is None else timeout_s
                ctx = mp.spawn(
                    _run_backend_worker_with_finalizer,
                    args=spawn_args,
                    nprocs=actual_nprocs,
                    join=False,
                )
                wait_for_spawn_context(ctx, resolved_timeout)

            return collect_rank_results(result_map, actual_nprocs)


class MooncakePGCPUBackendTestCase(BackendMultiProcessTestCase):
    backend_name = "mooncake-cpu"
    device_type = "cpu"


class MooncakePGCUDABackendTestCase(BackendMultiProcessTestCase):
    backend_name = "mooncake"
    device_type = "cuda"
