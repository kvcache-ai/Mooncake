from __future__ import annotations

import ctypes
import json
import math
import os
import socket
import time
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, replace
from statistics import fmean, pstdev
from typing import Callable, Mapping, Protocol
from uuid import uuid4

import pytest

from mooncake.weight_transfer import (
    MemoryRegistrationLease,
    MooncakeTransferEngineSink,
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    TensorDescriptor,
    WeightStore,
    plan_runtime_transfer,
)


@dataclass(frozen=True)
class PerfConfig:
    total_bytes: int
    warmups: int
    iterations: int
    max_cv: float
    max_p95_p50: float
    min_store_gbps: float
    min_te_gbps: float
    topologies: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class PerfSummary:
    phase: str
    logical_bytes: int
    iterations: int
    mean_seconds: float
    p50_seconds: float
    p95_seconds: float
    p50_gbps: float | None
    cv: float
    p95_p50_ratio: float


def _parse_int(
    environ: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
) -> int:
    raw = environ.get(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an integer") from error
    if str(value) != raw.strip() or value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _parse_float(
    environ: Mapping[str, str],
    name: str,
    default: float,
    *,
    minimum: float,
    inclusive: bool,
) -> float:
    raw = environ.get(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be numeric") from error
    below_minimum = value < minimum if inclusive else value <= minimum
    if not math.isfinite(value) or below_minimum:
        comparator = "at least" if inclusive else "greater than"
        raise ValueError(f"{name} must be {comparator} {minimum}")
    return value


def _parse_topologies(raw: str) -> tuple[tuple[int, int], ...]:
    name = "MOONCAKE_WEIGHT_PERF_TOPOLOGIES"
    result = []
    try:
        for item in raw.split(","):
            source, target = item.split(":", maxsplit=1)
            source_tp = int(source)
            target_tp = int(target)
            if source_tp <= 0 or target_tp <= 0:
                raise ValueError
            result.append((source_tp, target_tp))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must contain positive source:target pairs") from error
    if not result:
        raise ValueError(f"{name} must not be empty")
    return tuple(result)


def _parse_cuda_devices(
    environ: Mapping[str, str],
    name: str,
    *,
    default: str,
) -> tuple[int, ...]:
    raw = environ.get(name, default)
    if raw != raw.strip():
        raise ValueError(f"{name} must contain unique non-negative device IDs")
    devices = []
    try:
        for item in raw.split(","):
            device = int(item)
            if str(device) != item or device < 0 or device in devices:
                raise ValueError
            devices.append(device)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{name} must contain unique non-negative device IDs"
        ) from error
    if not devices:
        raise ValueError(f"{name} must not be empty")
    return tuple(devices)


def _cuda_rank_devices(devices: tuple[int, ...], ranks: int) -> tuple[int, ...]:
    if not devices or ranks <= 0 or any(device < 0 for device in devices):
        raise ValueError("CUDA rank placement is invalid")
    return tuple(devices[rank % len(devices)] for rank in range(ranks))


def _perf_config_from_environ(environ: Mapping[str, str]) -> PerfConfig:
    return PerfConfig(
        total_bytes=_parse_int(
            environ,
            "MOONCAKE_WEIGHT_PERF_BYTES",
            256 * 1024 * 1024,
            minimum=1,
        ),
        warmups=_parse_int(
            environ,
            "MOONCAKE_WEIGHT_PERF_WARMUPS",
            3,
            minimum=0,
        ),
        iterations=_parse_int(
            environ,
            "MOONCAKE_WEIGHT_PERF_ITERATIONS",
            30,
            minimum=1,
        ),
        max_cv=_parse_float(
            environ,
            "MOONCAKE_WEIGHT_PERF_MAX_CV",
            0.20,
            minimum=0.0,
            inclusive=False,
        ),
        max_p95_p50=_parse_float(
            environ,
            "MOONCAKE_WEIGHT_PERF_MAX_P95_P50",
            1.50,
            minimum=1.0,
            inclusive=True,
        ),
        min_store_gbps=_parse_float(
            environ,
            "MOONCAKE_WEIGHT_PERF_MIN_STORE_GBPS",
            0.0,
            minimum=0.0,
            inclusive=True,
        ),
        min_te_gbps=_parse_float(
            environ,
            "MOONCAKE_WEIGHT_PERF_MIN_TE_GBPS",
            0.0,
            minimum=0.0,
            inclusive=True,
        ),
        topologies=_parse_topologies(
            environ.get("MOONCAKE_WEIGHT_PERF_TOPOLOGIES", "4:2,2:4")
        ),
    )


def _percentile(samples: tuple[float, ...], percentile: float) -> float:
    ordered = tuple(sorted(samples))
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _summarize_perf_samples(
    *,
    phase: str,
    logical_bytes: int,
    samples: tuple[float, ...],
) -> PerfSummary:
    if not phase or logical_bytes < 0 or not samples:
        raise ValueError("performance summary inputs must not be empty")
    if any(not math.isfinite(sample) or sample <= 0 for sample in samples):
        raise ValueError("performance samples must be finite and positive")
    mean_seconds = fmean(samples)
    p50_seconds = _percentile(samples, 0.50)
    p95_seconds = _percentile(samples, 0.95)
    return PerfSummary(
        phase=phase,
        logical_bytes=logical_bytes,
        iterations=len(samples),
        mean_seconds=mean_seconds,
        p50_seconds=p50_seconds,
        p95_seconds=p95_seconds,
        p50_gbps=(
            (logical_bytes / 1_000_000_000) / p50_seconds if logical_bytes else None
        ),
        cv=pstdev(samples) / mean_seconds,
        p95_p50_ratio=p95_seconds / p50_seconds,
    )


def _expected_tp_shard(
    *,
    total_bytes: int,
    source_tp: int,
    target_tp: int,
    target_rank: int,
) -> bytes:
    return b"".join(
        bytes([value]) * nbytes
        for _, nbytes, value in _expected_tp_segments(
            total_bytes=total_bytes,
            source_tp=source_tp,
            target_tp=target_tp,
            target_rank=target_rank,
        )
    )


def _expected_tp_segments(
    *,
    total_bytes: int,
    source_tp: int,
    target_tp: int,
    target_rank: int,
) -> tuple[tuple[int, int, int], ...]:
    if (
        total_bytes <= 0
        or source_tp <= 0
        or source_tp > 255
        or target_tp <= 0
        or not 0 <= target_rank < target_tp
        or total_bytes % source_tp
        or total_bytes % target_tp
    ):
        raise ValueError("invalid TP shard geometry")
    source_extent = total_bytes // source_tp
    target_extent = total_bytes // target_tp
    target_begin = target_rank * target_extent
    target_end = target_begin + target_extent
    result = []
    target_offset = 0
    for source_rank in range(source_tp):
        source_begin = source_rank * source_extent
        source_end = source_begin + source_extent
        overlap = max(
            0,
            min(source_end, target_end) - max(source_begin, target_begin),
        )
        if overlap:
            result.append((target_offset, overlap, source_rank + 1))
            target_offset += overlap
    if target_offset != target_extent:
        raise AssertionError("TP shard expectation is incomplete")
    return tuple(result)


def _collect_perf_samples(
    *,
    warmups: int,
    iterations: int,
    run_once: Callable[[int], Mapping[str, float]],
) -> dict[str, tuple[float, ...]]:
    if warmups < 0 or iterations <= 0:
        raise ValueError("benchmark iteration counts are invalid")
    samples: dict[str, list[float]] = {}
    phases: tuple[str, ...] | None = None
    for iteration in range(warmups + iterations):
        current = run_once(iteration)
        current_phases = tuple(sorted(current))
        if not current_phases or (phases is not None and current_phases != phases):
            raise ValueError("benchmark phases changed between iterations")
        phases = current_phases
        for phase in current_phases:
            duration = current[phase]
            if not math.isfinite(duration) or duration <= 0:
                raise ValueError(f"benchmark phase is invalid: {phase}")
            if iteration >= warmups:
                samples.setdefault(phase, []).append(duration)
    return {phase: tuple(values) for phase, values in samples.items()}


def _assert_perf_gate(
    summary: PerfSummary,
    *,
    max_cv: float,
    max_p95_p50: float,
    min_gbps: float,
) -> None:
    if summary.cv > max_cv:
        raise AssertionError(
            f"{summary.phase} CV {summary.cv:.4f} exceeds {max_cv:.4f}"
        )
    if summary.p95_p50_ratio > max_p95_p50:
        raise AssertionError(
            f"{summary.phase} p95/p50 {summary.p95_p50_ratio:.4f} "
            f"exceeds {max_p95_p50:.4f}"
        )
    if summary.p50_gbps is None:
        if min_gbps == 0:
            return
        raise AssertionError(f"{summary.phase} has no GB/s measurement")
    if summary.p50_gbps < min_gbps:
        raise AssertionError(
            f"{summary.phase} {summary.p50_gbps:.4f} GB/s is below {min_gbps:.4f} GB/s"
        )


def _perf_result_payload(
    *,
    backend: str,
    total_bytes: int,
    source_tp: int,
    target_tp: int,
    warmups: int,
    iterations: int,
    samples: Mapping[str, tuple[float, ...]],
    logical_bytes: Mapping[str, int],
    placement: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if set(samples) != set(logical_bytes):
        raise ValueError("performance phase byte accounting is incomplete")
    summaries = {
        phase: _summarize_perf_samples(
            phase=phase,
            logical_bytes=logical_bytes[phase],
            samples=durations,
        )
        for phase, durations in sorted(samples.items())
    }
    payload = {
        "schema_version": 1,
        "backend": backend,
        "total_bytes": total_bytes,
        "topology": {"source_tp": source_tp, "target_tp": target_tp},
        "warmups": warmups,
        "iterations": iterations,
        "phases": {phase: asdict(summary) for phase, summary in summaries.items()},
    }
    if placement is not None:
        payload["placement"] = dict(placement)
    return payload


def _emit_perf_result(payload: Mapping[str, object]) -> None:
    print("WEIGHT_TRANSFER_PERF=" + json.dumps(payload, sort_keys=True))


class TransferBuffer(Protocol):
    size: int

    @property
    def pointer(self) -> int: ...

    def activate(self) -> None: ...

    def fill(self, value: int) -> None: ...

    def read_range(self, offset: int, nbytes: int) -> bytes: ...


@contextmanager
def _registered_store_buffers(store, buffers: list[TransferBuffer]):
    buffers_by_address = {}
    for buffer in buffers:
        current = buffers_by_address.get(buffer.pointer)
        if current is None or buffer.size > current.size:
            buffers_by_address[buffer.pointer] = buffer
    registered = []
    primary_error = None
    try:
        for buffer in buffers_by_address.values():
            buffer.activate()
            result = store.register_buffer(buffer.pointer, buffer.size)
            if result != 0:
                raise RuntimeError(
                    f"benchmark register_buffer failed for {buffer.pointer}: {result}"
                )
            registered.append(buffer)
        yield
    except BaseException as error:
        primary_error = error

    failures = []
    for buffer in reversed(registered):
        try:
            buffer.activate()
            result = store.unregister_buffer(buffer.pointer)
        except Exception as error:
            failures.append((buffer.pointer, repr(error)))
            continue
        if result != 0:
            failures.append((buffer.pointer, result))
    if failures:
        detail = f"benchmark unregister_buffer failed: {failures}"
        if primary_error is not None:
            raise RuntimeError(f"{primary_error}; {detail}") from primary_error
        raise RuntimeError(detail)
    if primary_error is not None:
        raise primary_error


def _verify_tp_buffers(
    buffers: list[TransferBuffer],
    *,
    total_bytes: int,
    source_tp: int,
    target_tp: int,
    chunk_bytes: int = 8 * 1024 * 1024,
) -> None:
    if len(buffers) != target_tp or chunk_bytes <= 0:
        raise ValueError("target buffer geometry is invalid")
    expected_size = total_bytes // target_tp
    for rank, buffer in enumerate(buffers):
        if buffer.size != expected_size:
            raise ValueError(f"target TP rank {rank} has an invalid buffer size")
        for offset, nbytes, value in _expected_tp_segments(
            total_bytes=total_bytes,
            source_tp=source_tp,
            target_tp=target_tp,
            target_rank=rank,
        ):
            for chunk_offset in range(0, nbytes, chunk_bytes):
                current_size = min(chunk_bytes, nbytes - chunk_offset)
                actual = buffer.read_range(offset + chunk_offset, current_size)
                if actual != bytes([value]) * current_size:
                    raise AssertionError(
                        f"target TP rank {rank} differs at byte {offset + chunk_offset}"
                    )


class CudaBuffer:
    def __init__(self, runtime, size: int) -> None:
        self._runtime = runtime
        self.size = size
        self.address = ctypes.c_void_p()
        self.activate()
        self._runtime.check(
            self._runtime.library.cudaMalloc(
                ctypes.byref(self.address),
                self.size,
            )
        )

    @property
    def pointer(self) -> int:
        return self.address.value

    def activate(self) -> None:
        self._runtime.activate()

    def write(self, data: bytes) -> None:
        if len(data) != self.size:
            raise ValueError("CUDA buffer write size mismatch")
        source = ctypes.create_string_buffer(data)
        self.activate()
        self._runtime.check(
            self._runtime.library.cudaMemcpy(
                self.address,
                ctypes.cast(source, ctypes.c_void_p),
                self.size,
                1,
            )
        )

    def fill(self, value: int) -> None:
        if not 0 <= value <= 255:
            raise ValueError("CUDA fill value must fit in one byte")
        self.activate()
        self._runtime.check(
            self._runtime.library.cudaMemset(self.address, value, self.size)
        )

    def read_range(self, offset: int, nbytes: int) -> bytes:
        if offset < 0 or nbytes < 0 or offset + nbytes > self.size:
            raise ValueError("CUDA buffer read range is invalid")
        target = ctypes.create_string_buffer(nbytes)
        self.activate()
        self._runtime.check(
            self._runtime.library.cudaMemcpy(
                ctypes.cast(target, ctypes.c_void_p),
                ctypes.c_void_p(self.pointer + offset),
                nbytes,
                2,
            )
        )
        return target.raw

    def read(self) -> bytes:
        return self.read_range(0, self.size)

    def zero(self) -> None:
        self.fill(0)

    def close(self) -> None:
        if self.address.value is not None:
            self.activate()
            self._runtime.check(self._runtime.library.cudaFree(self.address))
            self.address = ctypes.c_void_p()

    def __enter__(self) -> CudaBuffer:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class ManagedBuffer:
    def __init__(self, engine, size: int) -> None:
        self._engine = engine
        self.size = size
        self.address = engine.allocate_managed_buffer(size)
        if self.address == 0:
            raise RuntimeError("failed to allocate managed transfer buffer")

    @property
    def pointer(self) -> int:
        return self.address

    def activate(self) -> None:
        return None

    def write(self, data: bytes) -> None:
        if len(data) != self.size:
            raise ValueError("managed buffer write size mismatch")
        result = self._engine.write_bytes_to_buffer(
            self.address,
            data,
            self.size,
        )
        if result != 0:
            raise RuntimeError(f"managed buffer write failed: {result}")

    def read(self) -> bytes:
        return self.read_range(0, self.size)

    def fill(self, value: int) -> None:
        if not 0 <= value <= 255:
            raise ValueError("managed fill value must fit in one byte")
        ctypes.memset(self.address, value, self.size)

    def read_range(self, offset: int, nbytes: int) -> bytes:
        if offset < 0 or nbytes < 0 or offset + nbytes > self.size:
            raise ValueError("managed buffer read range is invalid")
        return ctypes.string_at(self.address + offset, nbytes)

    def zero(self) -> None:
        self.fill(0)

    def close(self) -> None:
        if self.address != 0:
            self._engine.free_managed_buffer(self.address, self.size)
            self.address = 0

    def __enter__(self) -> ManagedBuffer:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class CudaRuntime:
    def __init__(self, device: int = 0) -> None:
        if device < 0:
            raise ValueError("CUDA device must be non-negative")
        self.device = device
        self.library = ctypes.CDLL("libcudart.so")
        self.library.cudaMalloc.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_size_t,
        ]
        self.library.cudaFree.argtypes = [ctypes.c_void_p]
        self.library.cudaMemcpy.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]
        self.library.cudaMemset.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_size_t,
        ]
        self.library.cudaGetErrorString.argtypes = [ctypes.c_int]
        self.library.cudaGetErrorString.restype = ctypes.c_char_p
        self.library.cudaSetDevice.argtypes = [ctypes.c_int]
        self.activate()

    def activate(self) -> None:
        self.check(self.library.cudaSetDevice(self.device))

    def check(self, result: int) -> None:
        if result == 0:
            return
        message = self.library.cudaGetErrorString(result).decode()
        raise RuntimeError(f"CUDA call failed: {result}: {message}")


def _cuda_rank_buffers(
    stack: ExitStack,
    runtimes: Mapping[int, CudaRuntime],
    devices: tuple[int, ...],
    *,
    ranks: int,
    size: int,
) -> tuple[list[CudaBuffer], tuple[int, ...]]:
    rank_devices = _cuda_rank_devices(devices, ranks)
    buffers = [
        stack.enter_context(CudaBuffer(runtimes[device], size))
        for device in rank_devices
    ]
    return buffers, rank_devices


def _manifest(
    *,
    tensor: TensorDescriptor,
    revision: str,
    instance_id: str,
    tp_rank: int,
    tp_size: int,
    buffer: TransferBuffer,
    endpoint: str | None = None,
) -> RuntimeManifest:
    extent = tensor.global_shape[0] // tp_size
    return RuntimeManifest(
        model_id="weight-store-gpu-e2e",
        revision=revision,
        instance_id=instance_id,
        tensors=(tensor,),
        fragments=(
            RuntimeFragment(
                fragment_id=f"{instance_id}-weight",
                tensor_id=tensor.tensor_id,
                global_offset=(tp_rank * extent,),
                local_shape=(extent,),
                address=buffer.pointer,
                nbytes=buffer.size,
                worker_id=instance_id,
                endpoint=endpoint or f"{instance_id}:12345",
                rank=ParallelRank(tp=tp_rank),
                lease_generation=1,
                owner=buffer,
            ),
        ),
    )


def _tensor(total_bytes: int) -> TensorDescriptor:
    return TensorDescriptor(
        tensor_id="layers.0.mlp.gate_proj.weight",
        global_shape=(total_bytes,),
        dtype="uint8",
        itemsize=1,
        partition_dim=0,
        layer_id=0,
        layout_fingerprint="e2e:contiguous:uint8:v1",
    )


def _rank_manifests(
    *,
    tensor: TensorDescriptor,
    revision: str,
    prefix: str,
    buffers: list[TransferBuffer],
    endpoint: str | None = None,
) -> tuple[RuntimeManifest, ...]:
    return tuple(
        _manifest(
            tensor=tensor,
            revision=revision,
            instance_id=f"{prefix}-tp{rank}",
            tp_rank=rank,
            tp_size=len(buffers),
            buffer=buffer,
            endpoint=endpoint,
        )
        for rank, buffer in enumerate(buffers)
    )


def _combined_manifest(
    manifests: tuple[RuntimeManifest, ...],
    *,
    instance_id: str,
) -> RuntimeManifest:
    if not manifests:
        raise ValueError("manifests must not be empty")
    return RuntimeManifest(
        model_id=manifests[0].model_id,
        revision=manifests[0].revision,
        instance_id=instance_id,
        tensors=manifests[0].tensors,
        fragments=tuple(
            replace(fragment, worker_id=instance_id)
            for manifest in manifests
            for fragment in manifest.fragments
        ),
    )


def _cleanup_store_upload(store, upload_plan) -> None:
    keys = [
        upload_plan.manifest.manifest_key,
        *(operation.target.object_key for operation in upload_plan.operations),
        upload_plan.control_key,
    ]
    for key in dict.fromkeys(keys):
        result = store.remove(key, force=True)
        if result not in (0, -704):
            raise AssertionError(f"failed to remove benchmark object {key}: {result}")
        if store.is_exist(key) != 0:
            raise AssertionError(f"benchmark object remains after cleanup: {key}")


def _run_store_iteration(
    *,
    store,
    weight_store: WeightStore,
    tensor: TensorDescriptor,
    source_buffers: list[TransferBuffer],
    target_buffers: list[TransferBuffer],
    namespace: str,
    pre_registered: bool = False,
) -> dict[str, float]:
    source_tp = len(source_buffers)
    target_tp = len(target_buffers)
    total_bytes = tensor.global_shape[0]
    for buffer in target_buffers:
        buffer.zero()

    revision = uuid4().hex
    sources = _rank_manifests(
        tensor=tensor,
        revision=revision,
        prefix="source",
        buffers=source_buffers,
    )
    target_ranks = _rank_manifests(
        tensor=tensor,
        revision=revision,
        prefix="target",
        buffers=target_buffers,
    )
    target = _combined_manifest(target_ranks, instance_id="target-worker")
    upload_plan = None
    durations = {}
    e2e_start = time.perf_counter()
    try:
        started = time.perf_counter()
        upload_plan = weight_store.prepare_upload(sources, namespace=namespace)
        durations["prepare"] = time.perf_counter() - started

        started = time.perf_counter()
        receipts = tuple(
            receipt
            for source in sources
            for receipt in weight_store.upload(
                upload_plan,
                source,
                pre_registered=pre_registered,
            )
        )
        durations["upload"] = time.perf_counter() - started

        started = time.perf_counter()
        manifest = weight_store.commit(upload_plan, receipts)
        durations["commit"] = time.perf_counter() - started

        started = time.perf_counter()
        loaded = weight_store.load_manifest(manifest.manifest_key)
        durations["manifest_get"] = time.perf_counter() - started

        started = time.perf_counter()
        load_plan = weight_store.plan_load(loaded, (target,))
        durations["plan_load"] = time.perf_counter() - started

        started = time.perf_counter()
        weight_store.load(
            load_plan,
            target,
            pre_registered=pre_registered,
        )
        durations["load"] = time.perf_counter() - started
        durations["e2e"] = time.perf_counter() - e2e_start

        _verify_tp_buffers(
            target_buffers,
            total_bytes=total_bytes,
            source_tp=source_tp,
            target_tp=target_tp,
        )
        return durations
    finally:
        if upload_plan is not None:
            _cleanup_store_upload(store, upload_plan)


def _run_te_iteration(
    *,
    source_engine,
    target_endpoint: str,
    tensor: TensorDescriptor,
    source_buffers: list[TransferBuffer],
    target_buffers: list[TransferBuffer],
) -> dict[str, float]:
    source_tp = len(source_buffers)
    target_tp = len(target_buffers)
    total_bytes = tensor.global_shape[0]
    for buffer in target_buffers:
        buffer.zero()

    revision = uuid4().hex
    sources = _rank_manifests(
        tensor=tensor,
        revision=revision,
        prefix="source",
        buffers=source_buffers,
    )
    targets = _rank_manifests(
        tensor=tensor,
        revision=revision,
        prefix="target",
        buffers=target_buffers,
        endpoint=target_endpoint,
    )
    e2e_start = time.perf_counter()
    started = time.perf_counter()
    plan = plan_runtime_transfer(sources, targets)
    plan_seconds = time.perf_counter() - started

    sink = MooncakeTransferEngineSink(source_engine)
    source_registrations = tuple(
        MemoryRegistrationLease.from_fragment(fragment)
        for source in sources
        for fragment in source.fragments
    )
    target_registrations = tuple(
        MemoryRegistrationLease.from_fragment(fragment)
        for target in targets
        for fragment in target.fragments
    )
    started = time.perf_counter()
    receipts = tuple(
        receipt
        for source in sources
        for receipt in sink.execute(
            plan,
            source,
            targets,
            target_registrations=target_registrations,
            source_pre_registered=True,
            source_registrations=source_registrations,
        )
    )
    transfer_seconds = time.perf_counter() - started
    e2e_seconds = time.perf_counter() - e2e_start

    if sum(receipt.nbytes for receipt in receipts) != total_bytes:
        raise AssertionError("Transfer Engine receipt bytes are incomplete")
    _verify_tp_buffers(
        target_buffers,
        total_bytes=total_bytes,
        source_tp=source_tp,
        target_tp=target_tp,
    )
    return {
        "plan": plan_seconds,
        "transfer": transfer_seconds,
        "e2e": e2e_seconds,
    }


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_GPU_STORE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_GPU_STORE_E2E=1 to run the CUDA Store test",
)
def test_gpu_store_round_trip_reshards_tp_split_and_merge() -> None:
    from mooncake.store import MooncakeDistributedStore

    source_devices = _parse_cuda_devices(
        os.environ,
        "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES",
        default="0",
    )
    target_devices = _parse_cuda_devices(
        os.environ,
        "MOONCAKE_WEIGHT_TARGET_CUDA_DEVICES",
        default="0",
    )
    runtimes = {
        device: CudaRuntime(device)
        for device in sorted(set(source_devices) | set(target_devices))
    }
    total_bytes = 8 * 1024 * 1024
    store = MooncakeDistributedStore()
    result = store.setup(
        os.getenv(
            "MOONCAKE_WEIGHT_LOCAL_HOSTNAME", socket.gethostbyname(socket.gethostname())
        ),
        os.getenv(
            "MOONCAKE_WEIGHT_METADATA_SERVER",
            "http://127.0.0.1:8080/metadata",
        ),
        128 * 1024 * 1024,
        64 * 1024 * 1024,
        os.getenv("MOONCAKE_WEIGHT_PROTOCOL", "tcp"),
        os.getenv("MOONCAKE_WEIGHT_DEVICE", "eth0"),
        os.getenv("MOONCAKE_WEIGHT_MASTER", "127.0.0.1:50051"),
    )
    assert result == 0
    try:
        for source_tp, target_tp in ((4, 2), (2, 4)):
            with ExitStack() as stack:
                source_buffers, source_rank_devices = _cuda_rank_buffers(
                    stack,
                    runtimes,
                    source_devices,
                    ranks=source_tp,
                    size=total_bytes // source_tp,
                )
                target_buffers, target_rank_devices = _cuda_rank_buffers(
                    stack,
                    runtimes,
                    target_devices,
                    ranks=target_tp,
                    size=total_bytes // target_tp,
                )
                pre_registered = (
                    len(set(source_rank_devices) | set(target_rank_devices)) > 1
                )
                if pre_registered:
                    stack.enter_context(
                        _registered_store_buffers(
                            store,
                            [*source_buffers, *target_buffers],
                        )
                    )
                for rank, buffer in enumerate(source_buffers):
                    buffer.fill(rank + 1)

                durations = _run_store_iteration(
                    store=store,
                    weight_store=WeightStore(store),
                    tensor=_tensor(total_bytes),
                    source_buffers=source_buffers,
                    target_buffers=target_buffers,
                    namespace="gpu-e2e",
                    pre_registered=pre_registered,
                )
                assert set(durations) == {
                    "prepare",
                    "upload",
                    "commit",
                    "manifest_get",
                    "plan_load",
                    "load",
                    "e2e",
                }
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            assert close() == 0


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_PERF_STORE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_PERF_STORE_E2E=1 to run Store performance E2E",
)
def test_gpu_store_heterogeneous_tp_performance() -> None:
    from mooncake.store import MooncakeDistributedStore

    config = _perf_config_from_environ(os.environ)
    source_devices = _parse_cuda_devices(
        os.environ,
        "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES",
        default="0",
    )
    target_devices = _parse_cuda_devices(
        os.environ,
        "MOONCAKE_WEIGHT_TARGET_CUDA_DEVICES",
        default="0",
    )
    runtimes = {
        device: CudaRuntime(device)
        for device in sorted(set(source_devices) | set(target_devices))
    }
    store = MooncakeDistributedStore()
    result = store.setup(
        os.getenv(
            "MOONCAKE_WEIGHT_LOCAL_HOSTNAME", socket.gethostbyname(socket.gethostname())
        ),
        os.getenv(
            "MOONCAKE_WEIGHT_METADATA_SERVER",
            "http://127.0.0.1:8080/metadata",
        ),
        max(128 * 1024 * 1024, config.total_bytes * 2),
        64 * 1024 * 1024,
        os.getenv("MOONCAKE_WEIGHT_PROTOCOL", "tcp"),
        os.getenv("MOONCAKE_WEIGHT_DEVICE", "eth0"),
        os.getenv("MOONCAKE_WEIGHT_MASTER", "127.0.0.1:50051"),
    )
    assert result == 0
    try:
        for source_tp, target_tp in config.topologies:
            if config.total_bytes % source_tp or config.total_bytes % target_tp:
                raise ValueError("performance bytes must divide every TP topology")
            with ExitStack() as stack:
                source_buffers, source_rank_devices = _cuda_rank_buffers(
                    stack,
                    runtimes,
                    source_devices,
                    ranks=source_tp,
                    size=config.total_bytes // source_tp,
                )
                target_buffers, target_rank_devices = _cuda_rank_buffers(
                    stack,
                    runtimes,
                    target_devices,
                    ranks=target_tp,
                    size=config.total_bytes // target_tp,
                )
                stack.enter_context(
                    _registered_store_buffers(
                        store,
                        [*source_buffers, *target_buffers],
                    )
                )
                for rank, buffer in enumerate(source_buffers):
                    buffer.fill(rank + 1)

                tensor = _tensor(config.total_bytes)
                samples = _collect_perf_samples(
                    warmups=config.warmups,
                    iterations=config.iterations,
                    run_once=lambda _: _run_store_iteration(
                        store=store,
                        weight_store=WeightStore(store),
                        tensor=tensor,
                        source_buffers=source_buffers,
                        target_buffers=target_buffers,
                        namespace=f"perf-tp{source_tp}-to-tp{target_tp}",
                        pre_registered=True,
                    ),
                )
                logical_bytes = {phase: 0 for phase in samples}
                logical_bytes.update(
                    {
                        "upload": config.total_bytes,
                        "load": config.total_bytes,
                        "e2e": config.total_bytes * 2,
                    }
                )
                payload = _perf_result_payload(
                    backend=(
                        "store-cuda-tcp-preregistered-multi-gpu"
                        if len(set(source_rank_devices) | set(target_rank_devices)) > 1
                        else "store-cuda-tcp-preregistered"
                    ),
                    total_bytes=config.total_bytes,
                    source_tp=source_tp,
                    target_tp=target_tp,
                    warmups=config.warmups,
                    iterations=config.iterations,
                    samples=samples,
                    logical_bytes=logical_bytes,
                    placement={
                        "source_cuda_devices": list(source_rank_devices),
                        "target_cuda_devices": list(target_rank_devices),
                    },
                )
                _emit_perf_result(payload)
                for phase in ("upload", "load"):
                    _assert_perf_gate(
                        _summarize_perf_samples(
                            phase=phase,
                            logical_bytes=logical_bytes[phase],
                            samples=samples[phase],
                        ),
                        max_cv=config.max_cv,
                        max_p95_p50=config.max_p95_p50,
                        min_gbps=config.min_store_gbps,
                    )
                _assert_perf_gate(
                    _summarize_perf_samples(
                        phase="e2e",
                        logical_bytes=logical_bytes["e2e"],
                        samples=samples["e2e"],
                    ),
                    max_cv=float("inf"),
                    max_p95_p50=float("inf"),
                    min_gbps=config.min_store_gbps,
                )
    finally:
        close = getattr(store, "close", None)
        if callable(close):
            assert close() == 0


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_TE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_TE_E2E=1 to run the Transfer Engine test",
)
def test_te_tcp_round_trip_reshards_tp2_to_tp4() -> None:
    from mooncake.engine import TransferEngine

    total_bytes = 8 * 1024 * 1024
    source_engine = TransferEngine()
    target_engine = TransferEngine()
    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
        socket.gethostbyname(socket.gethostname()),
    )
    assert source_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert target_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert source_engine.get_rpc_port() != target_engine.get_rpc_port()
    target_endpoint = f"{local_hostname}:{target_engine.get_rpc_port()}"

    for source_tp, target_tp in ((2, 4), (4, 2)):
        with ExitStack() as stack:
            source_buffers = [
                stack.enter_context(
                    ManagedBuffer(source_engine, total_bytes // source_tp)
                )
                for _ in range(source_tp)
            ]
            target_buffers = [
                stack.enter_context(
                    ManagedBuffer(target_engine, total_bytes // target_tp)
                )
                for _ in range(target_tp)
            ]
            for rank, buffer in enumerate(source_buffers):
                buffer.fill(rank + 1)

            durations = _run_te_iteration(
                source_engine=source_engine,
                target_endpoint=target_endpoint,
                tensor=_tensor(total_bytes),
                source_buffers=source_buffers,
                target_buffers=target_buffers,
            )
            assert set(durations) == {"plan", "transfer", "e2e"}


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_PERF_TE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_PERF_TE_E2E=1 to run TE performance E2E",
)
def test_te_tcp_heterogeneous_tp_performance() -> None:
    from mooncake.engine import TransferEngine

    config = _perf_config_from_environ(os.environ)
    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
        socket.gethostbyname(socket.gethostname()),
    )
    source_engine = TransferEngine()
    target_engine = TransferEngine()
    assert source_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert target_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert source_engine.get_rpc_port() != target_engine.get_rpc_port()
    target_endpoint = f"{local_hostname}:{target_engine.get_rpc_port()}"

    for source_tp, target_tp in config.topologies:
        if config.total_bytes % source_tp or config.total_bytes % target_tp:
            raise ValueError("performance bytes must divide every TP topology")
        with ExitStack() as stack:
            source_buffers = [
                stack.enter_context(
                    ManagedBuffer(source_engine, config.total_bytes // source_tp)
                )
                for _ in range(source_tp)
            ]
            target_buffers = [
                stack.enter_context(
                    ManagedBuffer(target_engine, config.total_bytes // target_tp)
                )
                for _ in range(target_tp)
            ]
            for rank, buffer in enumerate(source_buffers):
                buffer.fill(rank + 1)

            tensor = _tensor(config.total_bytes)
            samples = _collect_perf_samples(
                warmups=config.warmups,
                iterations=config.iterations,
                run_once=lambda _: _run_te_iteration(
                    source_engine=source_engine,
                    target_endpoint=target_endpoint,
                    tensor=tensor,
                    source_buffers=source_buffers,
                    target_buffers=target_buffers,
                ),
            )
            logical_bytes = {"plan": 0, "transfer": config.total_bytes}
            logical_bytes["e2e"] = config.total_bytes
            payload = _perf_result_payload(
                backend="te-tcp-managed",
                total_bytes=config.total_bytes,
                source_tp=source_tp,
                target_tp=target_tp,
                warmups=config.warmups,
                iterations=config.iterations,
                samples=samples,
                logical_bytes=logical_bytes,
            )
            _emit_perf_result(payload)
            _assert_perf_gate(
                _summarize_perf_samples(
                    phase="transfer",
                    logical_bytes=config.total_bytes,
                    samples=samples["transfer"],
                ),
                max_cv=config.max_cv,
                max_p95_p50=config.max_p95_p50,
                min_gbps=config.min_te_gbps,
            )


def test_perf_metrics_report_percentiles_bandwidth_and_variation() -> None:
    summary = _summarize_perf_samples(
        phase="load",
        logical_bytes=1_000_000_000,
        samples=(1.0, 2.0, 3.0, 4.0),
    )

    assert summary.phase == "load"
    assert summary.iterations == 4
    assert summary.mean_seconds == pytest.approx(2.5)
    assert summary.p50_seconds == pytest.approx(2.5)
    assert summary.p95_seconds == pytest.approx(3.85)
    assert summary.p50_gbps == pytest.approx(0.4)
    assert summary.cv == pytest.approx(0.4472135955)
    assert summary.p95_p50_ratio == pytest.approx(1.54)


def test_perf_metrics_allow_latency_only_phase() -> None:
    summary = _summarize_perf_samples(
        phase="commit",
        logical_bytes=0,
        samples=(0.001, 0.002),
    )

    assert summary.logical_bytes == 0
    assert summary.p50_gbps is None


def test_perf_result_payload_is_machine_readable() -> None:
    payload = _perf_result_payload(
        backend="te-tcp-managed",
        total_bytes=1_000,
        source_tp=2,
        target_tp=4,
        warmups=1,
        iterations=2,
        samples={"plan": (0.001, 0.002), "transfer": (0.1, 0.2)},
        logical_bytes={"plan": 0, "transfer": 1_000},
        placement={"source_cuda_devices": [0, 1], "target_cuda_devices": [2, 3]},
    )

    assert payload["schema_version"] == 1
    assert payload["topology"] == {"source_tp": 2, "target_tp": 4}
    assert payload["phases"]["plan"]["p50_gbps"] is None
    assert payload["phases"]["transfer"]["p50_gbps"] == pytest.approx(0.0000066666667)
    assert payload["placement"] == {
        "source_cuda_devices": [0, 1],
        "target_cuda_devices": [2, 3],
    }


def test_perf_config_parses_explicit_environment() -> None:
    config = _perf_config_from_environ(
        {
            "MOONCAKE_WEIGHT_PERF_BYTES": "268435456",
            "MOONCAKE_WEIGHT_PERF_WARMUPS": "2",
            "MOONCAKE_WEIGHT_PERF_ITERATIONS": "10",
            "MOONCAKE_WEIGHT_PERF_MAX_CV": "0.10",
            "MOONCAKE_WEIGHT_PERF_MAX_P95_P50": "1.25",
            "MOONCAKE_WEIGHT_PERF_MIN_STORE_GBPS": "3.5",
            "MOONCAKE_WEIGHT_PERF_MIN_TE_GBPS": "5.25",
            "MOONCAKE_WEIGHT_PERF_TOPOLOGIES": "4:2,2:4,4:8",
        }
    )

    assert config.total_bytes == 256 * 1024 * 1024
    assert config.warmups == 2
    assert config.iterations == 10
    assert config.max_cv == pytest.approx(0.10)
    assert config.max_p95_p50 == pytest.approx(1.25)
    assert config.min_store_gbps == pytest.approx(3.5)
    assert config.min_te_gbps == pytest.approx(5.25)
    assert config.topologies == ((4, 2), (2, 4), (4, 8))


def test_perf_config_defaults_match_remote_stability_gate() -> None:
    config = _perf_config_from_environ({})

    assert config.warmups == 3
    assert config.iterations == 30
    assert config.max_cv == pytest.approx(0.20)
    assert config.max_p95_p50 == pytest.approx(1.50)


@pytest.mark.parametrize(
    "name,value",
    [
        ("MOONCAKE_WEIGHT_PERF_BYTES", "0"),
        ("MOONCAKE_WEIGHT_PERF_WARMUPS", "-1"),
        ("MOONCAKE_WEIGHT_PERF_ITERATIONS", "false"),
        ("MOONCAKE_WEIGHT_PERF_MAX_CV", "0"),
        ("MOONCAKE_WEIGHT_PERF_MAX_P95_P50", "nan"),
        ("MOONCAKE_WEIGHT_PERF_MIN_STORE_GBPS", "-0.1"),
        ("MOONCAKE_WEIGHT_PERF_TOPOLOGIES", "4:0"),
    ],
)
def test_perf_config_rejects_invalid_environment(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name):
        _perf_config_from_environ({name: value})


@pytest.mark.parametrize(
    "source_tp,target_tp,target_rank,expected",
    [
        (4, 2, 0, bytes([1]) * 4 + bytes([2]) * 4),
        (4, 2, 1, bytes([3]) * 4 + bytes([4]) * 4),
        (2, 4, 0, bytes([1]) * 4),
        (2, 4, 1, bytes([1]) * 4),
        (2, 4, 2, bytes([2]) * 4),
        (2, 4, 3, bytes([2]) * 4),
    ],
)
def test_expected_tp_shard_handles_split_and_merge(
    source_tp: int,
    target_tp: int,
    target_rank: int,
    expected: bytes,
) -> None:
    assert (
        _expected_tp_shard(
            total_bytes=16,
            source_tp=source_tp,
            target_tp=target_tp,
            target_rank=target_rank,
        )
        == expected
    )


@pytest.mark.parametrize(
    "source_tp,target_tp,target_rank,expected",
    [
        (4, 2, 0, ((0, 4, 1), (4, 4, 2))),
        (4, 2, 1, ((0, 4, 3), (4, 4, 4))),
        (2, 4, 0, ((0, 4, 1),)),
        (2, 4, 1, ((0, 4, 1),)),
        (2, 4, 2, ((0, 4, 2),)),
        (2, 4, 3, ((0, 4, 2),)),
    ],
)
def test_expected_tp_segments_handle_split_and_merge(
    source_tp: int,
    target_tp: int,
    target_rank: int,
    expected: tuple[tuple[int, int, int], ...],
) -> None:
    assert (
        _expected_tp_segments(
            total_bytes=16,
            source_tp=source_tp,
            target_tp=target_tp,
            target_rank=target_rank,
        )
        == expected
    )


def test_collect_perf_samples_discards_warmups() -> None:
    calls = []

    def run_once(iteration: int) -> dict[str, float]:
        calls.append(iteration)
        return {"transfer": float(iteration + 1)}

    samples = _collect_perf_samples(warmups=2, iterations=3, run_once=run_once)

    assert calls == [0, 1, 2, 3, 4]
    assert samples == {"transfer": (3.0, 4.0, 5.0)}


def test_verify_tp_buffers_checks_every_chunk() -> None:
    class FakeBuffer:
        def __init__(self, data: bytes) -> None:
            self.data = data
            self.size = len(data)
            self.reads = []

        def read_range(self, offset: int, nbytes: int) -> bytes:
            self.reads.append((offset, nbytes))
            return self.data[offset : offset + nbytes]

    buffers = [
        FakeBuffer(bytes([1]) * 4 + bytes([2]) * 4),
        FakeBuffer(bytes([3]) * 4 + bytes([4]) * 4),
    ]

    _verify_tp_buffers(
        buffers,
        total_bytes=16,
        source_tp=4,
        target_tp=2,
        chunk_bytes=3,
    )

    assert buffers[0].reads == [(0, 3), (3, 1), (4, 3), (7, 1)]
    assert buffers[1].reads == [(0, 3), (3, 1), (4, 3), (7, 1)]


def test_verify_tp_buffers_rejects_corruption() -> None:
    class FakeBuffer:
        size = 8

        def read_range(self, offset: int, nbytes: int) -> bytes:
            data = bytes([1]) * 4 + bytes([2]) * 4
            if offset <= 3 < offset + nbytes:
                data = data[:3] + b"\xff" + data[4:]
            return data[offset : offset + nbytes]

    with pytest.raises(AssertionError, match="target TP rank 0"):
        _verify_tp_buffers(
            [FakeBuffer(), FakeBuffer()],
            total_bytes=16,
            source_tp=4,
            target_tp=2,
            chunk_bytes=4,
        )


def test_registered_store_buffers_register_once_and_cleanup_in_reverse() -> None:
    class FakeStore:
        def __init__(self) -> None:
            self.calls = []

        def register_buffer(self, address: int, nbytes: int) -> int:
            self.calls.append(("register", address, nbytes))
            return 0

        def unregister_buffer(self, address: int) -> int:
            self.calls.append(("unregister", address))
            return 0

    @dataclass
    class FakeBuffer:
        pointer: int
        size: int

        def activate(self) -> None:
            store.calls.append(("activate", self.pointer))

    store = FakeStore()
    buffers = [FakeBuffer(100, 8), FakeBuffer(200, 16), FakeBuffer(100, 4)]

    with _registered_store_buffers(store, buffers):
        assert store.calls == [
            ("activate", 100),
            ("register", 100, 8),
            ("activate", 200),
            ("register", 200, 16),
        ]

    assert store.calls[-4:] == [
        ("activate", 200),
        ("unregister", 200),
        ("activate", 100),
        ("unregister", 100),
    ]


def test_registered_store_buffers_continue_cleanup_after_activation_failure() -> None:
    class FakeStore:
        def __init__(self) -> None:
            self.calls = []

        def register_buffer(self, address: int, nbytes: int) -> int:
            self.calls.append(("register", address, nbytes))
            return 0

        def unregister_buffer(self, address: int) -> int:
            self.calls.append(("unregister", address))
            return 0

    @dataclass
    class FakeBuffer:
        pointer: int
        size: int
        fail_cleanup_activation: bool = False
        activations: int = 0

        def activate(self) -> None:
            self.activations += 1
            if self.fail_cleanup_activation and self.activations == 2:
                raise RuntimeError("activate failed")

    store = FakeStore()
    buffers = [
        FakeBuffer(100, 8),
        FakeBuffer(200, 16, fail_cleanup_activation=True),
    ]

    with pytest.raises(RuntimeError, match="activate failed"):
        with _registered_store_buffers(store, buffers):
            pass

    assert ("unregister", 100) in store.calls


def test_cuda_devices_parse_and_assign_round_robin() -> None:
    devices = _parse_cuda_devices(
        {"MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES": "0,2,3"},
        "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES",
        default="0",
    )

    assert devices == (0, 2, 3)
    assert _cuda_rank_devices(devices, 5) == (0, 2, 3, 0, 2)


@pytest.mark.parametrize("raw", ["", "0,", "-1", "0,0", "gpu0", " 0, 1 "])
def test_cuda_devices_reject_invalid_or_duplicate_values(raw: str) -> None:
    with pytest.raises(ValueError, match="MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES"):
        _parse_cuda_devices(
            {"MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES": raw},
            "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES",
            default="0",
        )


def test_cuda_runtime_activates_configured_device(monkeypatch) -> None:
    calls = []

    class FakeFunction:
        def __init__(self, name: str, result=0) -> None:
            self.name = name
            self.result = result
            self.argtypes = None
            self.restype = None

        def __call__(self, *args):
            calls.append((self.name, *args))
            return self.result

    class FakeLibrary:
        cudaMalloc = FakeFunction("malloc")
        cudaFree = FakeFunction("free")
        cudaMemcpy = FakeFunction("memcpy")
        cudaMemset = FakeFunction("memset")
        cudaGetErrorString = FakeFunction("error_string", b"error")
        cudaSetDevice = FakeFunction("set_device")

    monkeypatch.setattr(ctypes, "CDLL", lambda _: FakeLibrary())

    runtime = CudaRuntime(device=3)
    runtime.activate()

    assert calls == [("set_device", 3), ("set_device", 3)]


def test_cuda_buffer_activates_its_runtime_before_alloc_and_free() -> None:
    events = []

    class FakeLibrary:
        @staticmethod
        def cudaMalloc(address, size: int) -> int:
            events.append(("malloc", size))
            ctypes.cast(address, ctypes.POINTER(ctypes.c_void_p))[0] = ctypes.c_void_p(
                1234
            )
            return 0

        @staticmethod
        def cudaFree(address) -> int:
            events.append(("free", address.value))
            return 0

    class FakeRuntime:
        library = FakeLibrary()

        @staticmethod
        def activate() -> None:
            events.append(("activate",))

        @staticmethod
        def check(result: int) -> None:
            assert result == 0

    buffer = CudaBuffer(FakeRuntime(), 16)
    buffer.close()

    assert events == [
        ("activate",),
        ("malloc", 16),
        ("activate",),
        ("free", 1234),
    ]


def test_perf_gate_accepts_stable_samples_above_minimum_bandwidth() -> None:
    summary = _summarize_perf_samples(
        phase="transfer",
        logical_bytes=1_000_000_000,
        samples=(0.99, 1.0, 1.01),
    )

    _assert_perf_gate(
        summary,
        max_cv=0.02,
        max_p95_p50=1.02,
        min_gbps=0.9,
    )


@pytest.mark.parametrize(
    "samples,max_cv,max_ratio,min_gbps,message",
    [
        ((0.5, 1.0, 1.5), 0.10, 2.0, 0.0, "CV"),
        ((0.5, 0.5, 1.5), 1.0, 1.25, 0.0, "p95/p50"),
        ((1.0, 1.0, 1.0), 0.1, 1.1, 1.1, "GB/s"),
    ],
)
def test_perf_gate_rejects_unstable_or_slow_samples(
    samples: tuple[float, ...],
    max_cv: float,
    max_ratio: float,
    min_gbps: float,
    message: str,
) -> None:
    summary = _summarize_perf_samples(
        phase="transfer",
        logical_bytes=1_000_000_000,
        samples=samples,
    )

    with pytest.raises(AssertionError, match=message):
        _assert_perf_gate(
            summary,
            max_cv=max_cv,
            max_p95_p50=max_ratio,
            min_gbps=min_gbps,
        )
