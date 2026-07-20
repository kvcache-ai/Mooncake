from __future__ import annotations

import ctypes
import multiprocessing
import os
import socket
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Callable, Iterator
from uuid import uuid4

import pytest

from mooncake.weight_transfer import (
    DirectReadReceipt,
    MemoryRegistrationLease,
    MooncakeTransferEngineReader,
    MooncakeTransferEngineSink,
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    TensorDescriptor,
    TransferPlan,
    WeightStore,
    plan_runtime_transfer,
    plan_runtime_transfer_to_local_target,
)
from test_weight_store_gpu_e2e import (
    CudaBuffer,
    CudaRuntime,
    ManagedBuffer,
    TransferBuffer,
    _cleanup_store_upload,
    _parse_cuda_devices,
    _registered_store_buffers,
)


_TENSOR_BYTES = 64 * 1024
_READER_BUFFER_BYTES = 256
_READER_SOURCE_GUARD = 0x3C
_READER_TARGET_SENTINEL = 0xD7
_READER_VIEW_OFFSETS = (64, 128, 192)


@dataclass(frozen=True)
class LogicalWeight:
    tensor_id: str
    layer_id: int
    expert_id: int | None
    source_pp: int
    source_ep: int
    target_pp: int
    target_ep: int
    pattern: int

    def descriptor(self) -> TensorDescriptor:
        return TensorDescriptor(
            tensor_id=self.tensor_id,
            global_shape=(_TENSOR_BYTES,),
            dtype="uint8",
            itemsize=1,
            partition_dim=0,
            layer_id=self.layer_id,
            expert_id=self.expert_id,
            layout_fingerprint="all-axes:contiguous:uint8:v1",
        )


@dataclass
class AllAxesFixture:
    sources: tuple[RuntimeManifest, ...]
    targets: tuple[RuntimeManifest, ...]
    source_buffers: dict[tuple[str, int, int], TransferBuffer]
    target_buffers: dict[tuple[str, int, int], TransferBuffer]
    weights: tuple[LogicalWeight, ...]
    source_tp: int
    target_tp: int

    def verify(self) -> None:
        source_extent = _TENSOR_BYTES // self.source_tp
        target_extent = _TENSOR_BYTES // self.target_tp
        patterns = {weight.tensor_id: weight.pattern for weight in self.weights}
        for (tensor_id, _dp_rank, tp_rank), buffer in self.target_buffers.items():
            expected = bytearray()
            target_begin = tp_rank * target_extent
            for offset in range(target_extent):
                source_rank = (target_begin + offset) // source_extent
                expected.append(patterns[tensor_id] + source_rank)
            assert buffer.read_range(0, buffer.size) == bytes(expected)


@dataclass
class PackedReaderFixture:
    source: RuntimeManifest
    targets: tuple[RuntimeManifest, ...]
    source_buffer: TransferBuffer
    target_buffers: tuple[TransferBuffer, ...]
    source_expected: bytes
    target_expected: tuple[bytes, ...]

    def verify(self) -> None:
        assert self.source_buffer.read_range(0, _READER_BUFFER_BYTES) == (
            self.source_expected
        )
        for buffer, expected in zip(
            self.target_buffers, self.target_expected, strict=True
        ):
            assert buffer.read_range(0, _READER_BUFFER_BYTES) == expected


class HostBuffer:
    def __init__(self, size: int) -> None:
        self.size = size
        self._storage = ctypes.create_string_buffer(size)

    @property
    def pointer(self) -> int:
        return ctypes.addressof(self._storage)

    def activate(self) -> None:
        return None

    def fill(self, value: int) -> None:
        ctypes.memset(self.pointer, value, self.size)

    def write(self, data: bytes) -> None:
        if len(data) != self.size:
            raise ValueError("host buffer write size mismatch")
        ctypes.memmove(self.pointer, data, self.size)

    def zero(self) -> None:
        self.fill(0)

    def read_range(self, offset: int, nbytes: int) -> bytes:
        return ctypes.string_at(self.pointer + offset, nbytes)


class HostTransferEngine:
    def register_memory(self, address: int, nbytes: int) -> int:
        del address, nbytes
        return 0

    def unregister_memory(self, address: int) -> int:
        del address
        return 0

    def batch_transfer_sync_write(
        self, endpoint, source_addresses, target_addresses, sizes
    ) -> int:
        del endpoint
        for source, target, size in zip(
            source_addresses, target_addresses, sizes, strict=True
        ):
            ctypes.memmove(target, source, size)
        return 0

    def batch_transfer_sync_read(
        self, endpoint, target_addresses, source_addresses, sizes
    ) -> int:
        del endpoint
        for target, source, size in zip(
            target_addresses, source_addresses, sizes, strict=True
        ):
            ctypes.memmove(target, source, size)
        return 0


class RemoteCudaBuffer:
    def __init__(
        self,
        connection: Connection,
        index: int,
        pointer: int,
        size: int,
    ) -> None:
        self._connection = connection
        self._index = index
        self._pointer = pointer
        self.size = size

    @property
    def pointer(self) -> int:
        return self._pointer

    def activate(self) -> None:
        return None

    def zero(self) -> None:
        return None

    def write(self, data: bytes) -> None:
        if len(data) != self.size:
            raise ValueError("remote CUDA buffer write size mismatch")
        self._connection.send(("write", self._index, data))
        response = self._connection.recv()
        if response[0] == "error":
            raise RuntimeError(response[1])
        assert response[0] == "written"

    def read_range(self, offset: int, nbytes: int) -> bytes:
        self._connection.send(("read", self._index, offset, nbytes))
        response = self._connection.recv()
        if response[0] == "error":
            raise RuntimeError(response[1])
        assert response[0] == "data"
        return response[1]


def _cuda_target_worker(
    connection: Connection,
    local_hostname: str,
    protocol: str,
    device: str,
    target_devices: tuple[int, ...],
) -> None:
    from mooncake.engine import TransferEngine

    engine = TransferEngine()
    buffers = []
    runtimes = {target: CudaRuntime(target) for target in target_devices}
    try:
        result = engine.initialize(
            local_hostname,
            "P2PHANDSHAKE",
            protocol,
            device,
        )
        if result != 0:
            raise RuntimeError(f"target TransferEngine initialize failed: {result}")
        connection.send(("ready", f"{local_hostname}:{engine.get_rpc_port()}"))
        while True:
            command = connection.recv()
            if command[0] == "allocate":
                index = len(buffers)
                runtime = runtimes[target_devices[index % len(target_devices)]]
                buffer = CudaBuffer(runtime, command[1])
                buffer.zero()
                buffer.activate()
                result = engine.register_memory(buffer.pointer, buffer.size)
                if result != 0:
                    buffer.close()
                    raise RuntimeError(f"target register_memory failed: {result}")
                buffers.append(buffer)
                connection.send(("allocated", index, buffer.pointer, buffer.size))
            elif command[0] == "write":
                _, index, data = command
                buffers[index].write(data)
                connection.send(("written",))
            elif command[0] == "read":
                _, index, offset, nbytes = command
                connection.send(("data", buffers[index].read_range(offset, nbytes)))
            elif command[0] == "execute_reader":
                _, plan, sources, target = command
                receipts = MooncakeTransferEngineReader(engine).execute(
                    plan,
                    sources,
                    target,
                    source_registrations=tuple(
                        MemoryRegistrationLease.from_fragment(
                            fragment,
                            runtime_lease_id=source.lease_id,
                        )
                        for source in sources
                        for fragment in source.fragments
                    ),
                    target_pre_registered=True,
                    target_registrations=tuple(
                        MemoryRegistrationLease.from_fragment(fragment)
                        for fragment in target.fragments
                    ),
                )
                connection.send(("executed", receipts))
            elif command[0] == "shutdown":
                break
            else:
                raise RuntimeError(f"unknown target command: {command[0]}")

        for buffer in reversed(buffers):
            buffer.activate()
            result = engine.unregister_memory(buffer.pointer)
            if result != 0:
                raise RuntimeError(f"target unregister_memory failed: {result}")
        for buffer in reversed(buffers):
            buffer.close()
        connection.send(("stopped",))
    except BaseException as exc:
        connection.send(("error", repr(exc)))
    finally:
        connection.close()


@contextmanager
def _remote_cuda_target(
    *,
    local_hostname: str,
    protocol: str,
    device: str,
    target_devices: tuple[int, ...],
) -> Iterator[
    tuple[
        str,
        Callable[[int], RemoteCudaBuffer],
        Callable[
            [TransferPlan, tuple[RuntimeManifest, ...], RuntimeManifest],
            tuple[DirectReadReceipt, ...],
        ],
    ]
]:
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe()
    process = context.Process(
        target=_cuda_target_worker,
        args=(
            child_connection,
            local_hostname,
            protocol,
            device,
            target_devices,
        ),
    )
    process.start()
    child_connection.close()
    response = parent_connection.recv()
    if response[0] == "error":
        process.join(timeout=10)
        raise RuntimeError(response[1])
    assert response[0] == "ready"
    target_endpoint = response[1]

    def allocate(size: int) -> RemoteCudaBuffer:
        parent_connection.send(("allocate", size))
        allocation = parent_connection.recv()
        if allocation[0] == "error":
            raise RuntimeError(allocation[1])
        assert allocation[0] == "allocated"
        return RemoteCudaBuffer(
            parent_connection,
            allocation[1],
            allocation[2],
            allocation[3],
        )

    def execute_reader(
        plan: TransferPlan,
        sources: tuple[RuntimeManifest, ...],
        target: RuntimeManifest,
    ) -> tuple[DirectReadReceipt, ...]:
        parent_connection.send(("execute_reader", plan, sources, target))
        response = parent_connection.recv()
        if response[0] == "error":
            raise RuntimeError(response[1])
        assert response[0] == "executed"
        return response[1]

    try:
        yield target_endpoint, allocate, execute_reader
    finally:
        if process.is_alive():
            parent_connection.send(("shutdown",))
            stopped = parent_connection.recv()
            if stopped[0] == "error":
                raise RuntimeError(stopped[1])
            assert stopped[0] == "stopped"
        parent_connection.close()
        process.join(timeout=20)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
            raise RuntimeError("target TransferEngine process did not stop")
        if process.exitcode != 0:
            raise RuntimeError(
                f"target TransferEngine process exited with {process.exitcode}"
            )


def _weights() -> tuple[LogicalWeight, ...]:
    return (
        LogicalWeight(
            tensor_id="layers.0.self_attn.q_proj.weight",
            layer_id=0,
            expert_id=None,
            source_pp=0,
            source_ep=0,
            target_pp=0,
            target_ep=0,
            pattern=10,
        ),
        LogicalWeight(
            tensor_id="layers.3.mlp.gate_proj.weight",
            layer_id=3,
            expert_id=None,
            source_pp=1,
            source_ep=0,
            target_pp=3,
            target_ep=0,
            pattern=30,
        ),
        LogicalWeight(
            tensor_id="layers.1.mlp.experts.0.gate_proj.weight",
            layer_id=1,
            expert_id=0,
            source_pp=0,
            source_ep=0,
            target_pp=1,
            target_ep=0,
            pattern=50,
        ),
        LogicalWeight(
            tensor_id="layers.2.mlp.experts.1.down_proj.weight",
            layer_id=2,
            expert_id=1,
            source_pp=1,
            source_ep=1,
            target_pp=2,
            target_ep=0,
            pattern=70,
        ),
    )


def _build_fixture(
    *,
    revision: str,
    source_tp: int,
    target_tp: int,
    allocate_source: Callable[[int], TransferBuffer],
    allocate_target: Callable[[int], TransferBuffer],
    target_endpoint: str,
) -> AllAxesFixture:
    source_dp = 2
    target_dp = 3
    weights = _weights()
    source_fragments: dict[
        tuple[int, int, int, int], list[tuple[TensorDescriptor, RuntimeFragment]]
    ] = {}
    target_fragments: dict[
        tuple[int, int, int, int], list[tuple[TensorDescriptor, RuntimeFragment]]
    ] = {}
    source_buffers = {}
    target_buffers = {}

    source_extent = _TENSOR_BYTES // source_tp
    target_extent = _TENSOR_BYTES // target_tp
    for weight in weights:
        descriptor = weight.descriptor()
        for dp_rank in range(source_dp):
            for tp_rank in range(source_tp):
                buffer = allocate_source(source_extent)
                buffer.fill(weight.pattern + tp_rank)
                source_buffers[(weight.tensor_id, dp_rank, tp_rank)] = buffer
                rank = ParallelRank(
                    dp=dp_rank,
                    tp=tp_rank,
                    pp=weight.source_pp,
                    ep=weight.source_ep,
                )
                worker_id = (
                    f"source-d{dp_rank}-t{tp_rank}-"
                    f"p{weight.source_pp}-e{weight.source_ep}"
                )
                source_fragments.setdefault(
                    (dp_rank, tp_rank, weight.source_pp, weight.source_ep), []
                ).append(
                    (
                        descriptor,
                        RuntimeFragment(
                            fragment_id=f"{worker_id}:{weight.tensor_id}",
                            tensor_id=weight.tensor_id,
                            global_offset=(tp_rank * source_extent,),
                            local_shape=(source_extent,),
                            address=buffer.pointer,
                            nbytes=source_extent,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            rank=rank,
                            lease_generation=1,
                            owner=buffer,
                        ),
                    )
                )

        for dp_rank in range(target_dp):
            for tp_rank in range(target_tp):
                buffer = allocate_target(target_extent)
                buffer.zero()
                target_buffers[(weight.tensor_id, dp_rank, tp_rank)] = buffer
                rank = ParallelRank(
                    dp=dp_rank,
                    tp=tp_rank,
                    pp=weight.target_pp,
                    ep=weight.target_ep,
                )
                worker_id = (
                    f"target-d{dp_rank}-t{tp_rank}-"
                    f"p{weight.target_pp}-e{weight.target_ep}"
                )
                target_fragments.setdefault(
                    (dp_rank, tp_rank, weight.target_pp, weight.target_ep), []
                ).append(
                    (
                        descriptor,
                        RuntimeFragment(
                            fragment_id=f"{worker_id}:{weight.tensor_id}",
                            tensor_id=weight.tensor_id,
                            global_offset=(tp_rank * target_extent,),
                            local_shape=(target_extent,),
                            address=buffer.pointer,
                            nbytes=target_extent,
                            worker_id=worker_id,
                            endpoint=target_endpoint,
                            rank=rank,
                            lease_generation=1,
                            owner=buffer,
                        ),
                    )
                )

    def manifests(groups, prefix):
        result = []
        for rank, entries in sorted(groups.items()):
            descriptors = {
                descriptor.tensor_id: descriptor for descriptor, _ in entries
            }
            worker_id = entries[0][1].worker_id
            result.append(
                RuntimeManifest(
                    model_id="all-axes-native-e2e",
                    revision=revision,
                    instance_id=worker_id,
                    tensors=tuple(
                        descriptors[tensor_id] for tensor_id in sorted(descriptors)
                    ),
                    fragments=tuple(fragment for _, fragment in entries),
                    lease_id=f"{prefix}-lease-d{rank[0]}-t{rank[1]}-p{rank[2]}-e{rank[3]}",
                )
            )
        return tuple(result)

    return AllAxesFixture(
        sources=manifests(source_fragments, "source"),
        targets=manifests(target_fragments, "target"),
        source_buffers=source_buffers,
        target_buffers=target_buffers,
        weights=weights,
        source_tp=source_tp,
        target_tp=target_tp,
    )


def _build_packed_reader_fixture(
    *,
    revision: str,
    source_endpoint: str,
    target_endpoint: str,
    allocate_source: Callable[[int], TransferBuffer],
    allocate_target: Callable[[int], TransferBuffer],
) -> PackedReaderFixture:
    descriptors = (
        TensorDescriptor(
            tensor_id="layers.0.axis0.weight",
            global_shape=(8, 5),
            dtype="uint8",
            itemsize=1,
            partition_dim=0,
            layer_id=0,
            layout_fingerprint="native-reader:packed:uint8:v1",
        ),
        TensorDescriptor(
            tensor_id="layers.1.axis1.weight",
            global_shape=(4, 8),
            dtype="uint8",
            itemsize=1,
            partition_dim=1,
            layer_id=1,
            layout_fingerprint="native-reader:packed:uint8:v1",
        ),
        TensorDescriptor(
            tensor_id="model.norm.weight",
            global_shape=(16,),
            dtype="uint8",
            itemsize=1,
            partition_dim=None,
            layout_fingerprint="native-reader:packed:uint8:v1",
        ),
    )
    payloads = (
        bytes(range(1, 41)),
        bytes(range(65, 97)),
        bytes(range(129, 145)),
    )

    source_buffer = allocate_source(_READER_BUFFER_BYTES)
    source_expected = bytearray([_READER_SOURCE_GUARD] * _READER_BUFFER_BYTES)
    source_fragments = []
    source_worker = "reader-source-t0"
    for descriptor, payload, view_offset in zip(
        descriptors, payloads, _READER_VIEW_OFFSETS, strict=True
    ):
        source_expected[view_offset : view_offset + len(payload)] = payload
        source_fragments.append(
            RuntimeFragment(
                fragment_id=f"{source_worker}:{descriptor.tensor_id}",
                tensor_id=descriptor.tensor_id,
                global_offset=(0,) * len(descriptor.global_shape),
                local_shape=descriptor.global_shape,
                address=source_buffer.pointer + view_offset,
                nbytes=len(payload),
                worker_id=source_worker,
                endpoint=source_endpoint,
                rank=ParallelRank(tp=0),
                lease_generation=1,
            )
        )
    source_buffer.write(bytes(source_expected))
    source = RuntimeManifest(
        model_id="native-reader-e2e",
        revision=revision,
        instance_id=source_worker,
        tensors=descriptors,
        fragments=tuple(source_fragments),
        lease_id="reader-source-lease",
    )

    targets = []
    target_buffers = []
    target_expected = []
    for tp_rank in range(2):
        target_buffer = allocate_target(_READER_BUFFER_BYTES)
        expected = bytearray([_READER_TARGET_SENTINEL] * _READER_BUFFER_BYTES)
        target_buffer.write(bytes(expected))
        target_worker = f"reader-target-t{tp_rank}"
        target_fragments = []
        for descriptor, payload, view_offset in zip(
            descriptors, payloads, _READER_VIEW_OFFSETS, strict=True
        ):
            if descriptor.partition_dim == 0:
                local_shape = (4, 5)
                global_offset = (tp_rank * 4, 0)
                local_payload = payload[tp_rank * 20 : (tp_rank + 1) * 20]
            elif descriptor.partition_dim == 1:
                local_shape = (4, 4)
                global_offset = (0, tp_rank * 4)
                local_payload = b"".join(
                    payload[row * 8 + tp_rank * 4 : row * 8 + (tp_rank + 1) * 4]
                    for row in range(4)
                )
            else:
                local_shape = descriptor.global_shape
                global_offset = (0,) * len(descriptor.global_shape)
                local_payload = payload
            expected[view_offset : view_offset + len(local_payload)] = local_payload
            target_fragments.append(
                RuntimeFragment(
                    fragment_id=f"{target_worker}:{descriptor.tensor_id}",
                    tensor_id=descriptor.tensor_id,
                    global_offset=global_offset,
                    local_shape=local_shape,
                    address=target_buffer.pointer + view_offset,
                    nbytes=len(local_payload),
                    worker_id=target_worker,
                    endpoint=target_endpoint,
                    rank=ParallelRank(tp=tp_rank),
                    lease_generation=1,
                )
            )
        targets.append(
            RuntimeManifest(
                model_id="native-reader-e2e",
                revision=revision,
                instance_id=target_worker,
                tensors=descriptors,
                fragments=tuple(target_fragments),
                lease_id=f"reader-target-lease-t{tp_rank}",
            )
        )
        target_buffers.append(target_buffer)
        target_expected.append(bytes(expected))

    return PackedReaderFixture(
        source=source,
        targets=tuple(targets),
        source_buffer=source_buffer,
        target_buffers=tuple(target_buffers),
        source_expected=bytes(source_expected),
        target_expected=tuple(target_expected),
    )


def _execute_reader_fixture(
    fixture: PackedReaderFixture,
    execute_reader: Callable[
        [TransferPlan, tuple[RuntimeManifest, ...], RuntimeManifest],
        tuple[DirectReadReceipt, ...],
    ],
) -> None:
    sources = (fixture.source,)
    total_bytes = 0
    for target in fixture.targets:
        plan = plan_runtime_transfer_to_local_target(sources, target)
        axis1 = next(
            operation
            for operation in plan.operations
            if operation.tensor_id == "layers.1.axis1.weight"
        )
        assert (axis1.nbytes, axis1.repeat) == (4, 4)
        assert (axis1.source_stride, axis1.target_stride) == (8, 4)

        receipts = execute_reader(plan, sources, target)
        assert len(receipts) == 1
        assert receipts[0].source_endpoint == fixture.source.fragments[0].endpoint
        assert receipts[0].target_worker_id == target.fragments[0].worker_id
        assert receipts[0].operation_count == 6
        assert receipts[0].nbytes == 52
        total_bytes += receipts[0].nbytes

    assert total_bytes == 104
    fixture.verify()


@contextmanager
def _registered_engine_buffers(engine, buffers) -> Iterator[None]:
    registered = []
    try:
        for buffer in buffers:
            buffer.activate()
            result = engine.register_memory(buffer.pointer, buffer.size)
            if result != 0:
                raise RuntimeError(f"register_memory failed: {result}")
            registered.append(buffer)
        yield
    finally:
        failures = []
        for buffer in reversed(registered):
            buffer.activate()
            result = engine.unregister_memory(buffer.pointer)
            if result != 0:
                failures.append((buffer.pointer, result))
        if failures:
            raise RuntimeError(f"unregister_memory failed: {failures}")


def _execute_te_fixture(source_engine, fixture: AllAxesFixture) -> None:
    plan = plan_runtime_transfer(fixture.sources, fixture.targets)
    assert {operation.source.rank.dp for operation in plan.operations} == {0, 1}
    assert {operation.target.rank.dp for operation in plan.operations} == {0, 1, 2}
    assert {operation.source.rank.pp for operation in plan.operations} == {0, 1}
    assert {operation.target.rank.pp for operation in plan.operations} == {0, 1, 2, 3}
    assert {operation.source.rank.ep for operation in plan.operations} == {0, 1}
    assert {operation.target.rank.ep for operation in plan.operations} == {0}

    sink = MooncakeTransferEngineSink(source_engine)
    source_registrations = tuple(
        MemoryRegistrationLease.from_fragment(fragment)
        for manifest in fixture.sources
        for fragment in manifest.fragments
    )
    target_registrations = tuple(
        MemoryRegistrationLease.from_fragment(fragment)
        for manifest in fixture.targets
        for fragment in manifest.fragments
    )
    receipts = tuple(
        receipt
        for source in fixture.sources
        for receipt in sink.execute(
            plan,
            source,
            fixture.targets,
            target_registrations=target_registrations,
            source_pre_registered=True,
            source_registrations=source_registrations,
        )
    )
    assert sum(receipt.nbytes for receipt in receipts) == (
        len(fixture.weights) * _TENSOR_BYTES * 3
    )
    fixture.verify()


def test_all_axes_fixture_executes_one_composite_plan_without_native_runtime() -> None:
    engine = HostTransferEngine()
    fixture = _build_fixture(
        revision="unit-test",
        source_tp=2,
        target_tp=4,
        allocate_source=HostBuffer,
        allocate_target=HostBuffer,
        target_endpoint="target:12345",
    )
    with (
        _registered_engine_buffers(engine, fixture.source_buffers.values()),
        _registered_engine_buffers(engine, fixture.target_buffers.values()),
    ):
        _execute_te_fixture(engine, fixture)


def test_reader_fixture_pulls_packed_tp1_weights_into_tp2_targets() -> None:
    engine = HostTransferEngine()
    fixture = _build_packed_reader_fixture(
        revision="unit-test",
        source_endpoint="reader-source:12345",
        target_endpoint="reader-target:12345",
        allocate_source=HostBuffer,
        allocate_target=HostBuffer,
    )

    def execute_reader(plan, sources, target):
        return MooncakeTransferEngineReader(engine).execute(
            plan,
            sources,
            target,
            source_registrations=tuple(
                MemoryRegistrationLease.from_fragment(
                    fragment,
                    runtime_lease_id=source.lease_id,
                )
                for source in sources
                for fragment in source.fragments
            ),
            target_pre_registered=True,
            target_registrations=tuple(
                MemoryRegistrationLease.from_fragment(fragment)
                for fragment in target.fragments
            ),
        )

    with (
        _registered_engine_buffers(engine, (fixture.source_buffer,)),
        _registered_engine_buffers(engine, fixture.target_buffers),
    ):
        _execute_reader_fixture(fixture, execute_reader)


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_GPU_STORE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_GPU_STORE_E2E=1 to run the CUDA Store test",
)
def test_gpu_store_moves_weights_across_dp_tp_pp_ep_together() -> None:
    from mooncake.store import MooncakeDistributedStore

    source_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES", default="0"
    )
    target_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_TARGET_CUDA_DEVICES", default="0"
    )
    runtimes = {
        cuda_device: CudaRuntime(cuda_device)
        for cuda_device in sorted(set(source_devices) | set(target_devices))
    }
    source_index = 0
    target_index = 0
    with ExitStack() as stack:

        def allocate_source(size: int):
            nonlocal source_index
            device = source_devices[source_index % len(source_devices)]
            source_index += 1
            return stack.enter_context(CudaBuffer(runtimes[device], size))

        def allocate_target(size: int):
            nonlocal target_index
            device = target_devices[target_index % len(target_devices)]
            target_index += 1
            return stack.enter_context(CudaBuffer(runtimes[device], size))

        fixture = _build_fixture(
            revision=uuid4().hex,
            source_tp=2,
            target_tp=4,
            allocate_source=allocate_source,
            allocate_target=allocate_target,
            target_endpoint="store-target:12345",
        )
        store = MooncakeDistributedStore()
        result = store.setup(
            os.getenv(
                "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
                socket.gethostbyname(socket.gethostname()),
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
        upload_plan = None
        try:
            all_buffers = [
                *fixture.source_buffers.values(),
                *fixture.target_buffers.values(),
            ]
            with _registered_store_buffers(store, all_buffers):
                weight_store = WeightStore(store)
                upload_plan = weight_store.prepare_upload(
                    fixture.sources, namespace="all-axes-native-e2e"
                )
                receipts = tuple(
                    receipt
                    for source in fixture.sources
                    for receipt in weight_store.upload(
                        upload_plan, source, pre_registered=True
                    )
                )
                persisted = weight_store.commit(upload_plan, receipts)
                weight_store.finalize_upload_session(upload_plan)
                loaded = weight_store.load_manifest(persisted.manifest_key)
                load_plan = weight_store.plan_load(loaded, fixture.targets)
                for target in fixture.targets:
                    weight_store.load(load_plan, target, pre_registered=True)
                fixture.verify()
        finally:
            if upload_plan is not None:
                _cleanup_store_upload(store, upload_plan)
            close = getattr(store, "close", None)
            if callable(close):
                assert close() == 0


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_TE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_TE_E2E=1 to run the Transfer Engine test",
)
def test_te_tcp_moves_weights_across_dp_tp_pp_ep_together() -> None:
    from mooncake.engine import TransferEngine

    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
        socket.gethostbyname(socket.gethostname()),
    )
    source_engine = TransferEngine()
    target_engine = TransferEngine()
    assert source_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert target_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    target_endpoint = f"{local_hostname}:{target_engine.get_rpc_port()}"

    with ExitStack() as stack:
        fixture = _build_fixture(
            revision=uuid4().hex,
            source_tp=2,
            target_tp=4,
            allocate_source=lambda size: stack.enter_context(
                ManagedBuffer(source_engine, size)
            ),
            allocate_target=lambda size: stack.enter_context(
                ManagedBuffer(target_engine, size)
            ),
            target_endpoint=target_endpoint,
        )
        _execute_te_fixture(source_engine, fixture)


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_TE_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_TE_E2E=1 to run the Transfer Engine test",
)
def test_te_reader_tcp_pulls_packed_tp1_weights_into_tp2_targets() -> None:
    from mooncake.engine import TransferEngine

    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
        socket.gethostbyname(socket.gethostname()),
    )
    source_engine = TransferEngine()
    target_engine = TransferEngine()
    assert source_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    assert target_engine.initialize(local_hostname, "P2PHANDSHAKE", "tcp", "") == 0
    source_endpoint = f"{local_hostname}:{source_engine.get_rpc_port()}"
    target_endpoint = f"{local_hostname}:{target_engine.get_rpc_port()}"

    with ExitStack() as stack:
        fixture = _build_packed_reader_fixture(
            revision=uuid4().hex,
            source_endpoint=source_endpoint,
            target_endpoint=target_endpoint,
            allocate_source=lambda size: stack.enter_context(
                ManagedBuffer(source_engine, size)
            ),
            allocate_target=lambda size: stack.enter_context(
                ManagedBuffer(target_engine, size)
            ),
        )

        def execute_reader(plan, sources, target):
            return MooncakeTransferEngineReader(target_engine).execute(
                plan,
                sources,
                target,
                source_registrations=tuple(
                    MemoryRegistrationLease.from_fragment(
                        fragment,
                        runtime_lease_id=source.lease_id,
                    )
                    for source in sources
                    for fragment in source.fragments
                ),
                target_pre_registered=True,
                target_registrations=tuple(
                    MemoryRegistrationLease.from_fragment(fragment)
                    for fragment in target.fragments
                ),
            )

        _execute_reader_fixture(fixture, execute_reader)


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_TE_GPU_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_TE_GPU_E2E=1 to run the CUDA TE test",
)
def test_te_cuda_moves_weights_across_dp_tp_pp_ep_together() -> None:
    from mooncake.engine import TransferEngine

    source_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES", default="0"
    )
    target_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_TARGET_CUDA_DEVICES", default="0"
    )
    runtimes = {
        device: CudaRuntime(device)
        for device in sorted(set(source_devices) | set(target_devices))
    }
    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_TE_HOSTNAME",
        os.getenv(
            "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
            socket.gethostbyname(socket.gethostname()),
        ),
    )
    protocol = os.getenv("MOONCAKE_WEIGHT_TE_PROTOCOL", "rdma")
    transport_device = os.getenv("MOONCAKE_WEIGHT_TE_DEVICE", "")
    source_engine = TransferEngine()
    assert (
        source_engine.initialize(
            local_hostname,
            "P2PHANDSHAKE",
            protocol,
            transport_device,
        )
        == 0
    )

    source_index = 0
    with (
        ExitStack() as stack,
        _remote_cuda_target(
            local_hostname=local_hostname,
            protocol=protocol,
            device=transport_device,
            target_devices=target_devices,
        ) as (target_endpoint, allocate_target, _execute_reader),
    ):

        def allocate_source(size: int):
            nonlocal source_index
            cuda_device = source_devices[source_index % len(source_devices)]
            source_index += 1
            return stack.enter_context(CudaBuffer(runtimes[cuda_device], size))

        fixture = _build_fixture(
            revision=uuid4().hex,
            source_tp=2,
            target_tp=4,
            allocate_source=allocate_source,
            allocate_target=allocate_target,
            target_endpoint=target_endpoint,
        )
        with _registered_engine_buffers(source_engine, fixture.source_buffers.values()):
            _execute_te_fixture(source_engine, fixture)


@pytest.mark.skipif(
    os.getenv("MOONCAKE_WEIGHT_TE_GPU_E2E") != "1",
    reason="set MOONCAKE_WEIGHT_TE_GPU_E2E=1 to run the CUDA TE test",
)
def test_te_reader_cuda_pulls_packed_tp1_weights_into_tp2_targets() -> None:
    from mooncake.engine import TransferEngine

    source_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_SOURCE_CUDA_DEVICES", default="0"
    )
    target_devices = _parse_cuda_devices(
        os.environ, "MOONCAKE_WEIGHT_TARGET_CUDA_DEVICES", default="0"
    )
    runtimes = {device: CudaRuntime(device) for device in sorted(set(source_devices))}
    local_hostname = os.getenv(
        "MOONCAKE_WEIGHT_TE_HOSTNAME",
        os.getenv(
            "MOONCAKE_WEIGHT_LOCAL_HOSTNAME",
            socket.gethostbyname(socket.gethostname()),
        ),
    )
    protocol = os.getenv("MOONCAKE_WEIGHT_TE_PROTOCOL", "rdma")
    transport_device = os.getenv("MOONCAKE_WEIGHT_TE_DEVICE", "")
    source_engine = TransferEngine()
    assert (
        source_engine.initialize(
            local_hostname,
            "P2PHANDSHAKE",
            protocol,
            transport_device,
        )
        == 0
    )
    source_endpoint = f"{local_hostname}:{source_engine.get_rpc_port()}"

    source_index = 0
    with (
        ExitStack() as stack,
        _remote_cuda_target(
            local_hostname=local_hostname,
            protocol=protocol,
            device=transport_device,
            target_devices=target_devices,
        ) as (target_endpoint, allocate_target, execute_reader),
    ):

        def allocate_source(size: int):
            nonlocal source_index
            cuda_device = source_devices[source_index % len(source_devices)]
            source_index += 1
            return stack.enter_context(CudaBuffer(runtimes[cuda_device], size))

        fixture = _build_packed_reader_fixture(
            revision=uuid4().hex,
            source_endpoint=source_endpoint,
            target_endpoint=target_endpoint,
            allocate_source=allocate_source,
            allocate_target=allocate_target,
        )
        with _registered_engine_buffers(source_engine, (fixture.source_buffer,)):
            _execute_reader_fixture(fixture, execute_reader)
