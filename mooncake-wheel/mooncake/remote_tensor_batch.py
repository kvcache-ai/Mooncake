from __future__ import annotations

import ctypes
import threading
import time
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
import math
from math import prod
from typing import Any, Deque, Optional, Sequence, Union

try:
    import torch
except ImportError:  # pragma: no cover - torch is optional for metadata helpers.
    torch = None


MAX_TENSOR_DIM_SELECTION_OUTPUT_BYTES = 64 * 1024 * 1024 * 1024

DTYPE_BYTE_SIZES = {
    "bool": 1,
    "uint8": 1,
    "int8": 1,
    "float8_e4m3fn": 1,
    "float8_e5m2": 1,
    "int16": 2,
    "uint16": 2,
    "float16": 2,
    "bfloat16": 2,
    "int32": 4,
    "uint32": 4,
    "float32": 4,
    "int64": 8,
    "uint64": 8,
    "float64": 8,
}


@dataclass(frozen=True)
class TensorFieldRef:
    """Metadata-only reference to one fixed-shape tensor stored in Mooncake."""

    key: str
    shape: tuple[int, ...]
    dtype: Union[str, Any]
    data_offset: int = 0

    @classmethod
    def from_tensor(
        cls, key: str, tensor: Any, data_offset: int = 0
    ) -> "TensorFieldRef":
        """Create a field reference from a tensor-like object with shape and dtype."""
        return cls(
            key=key,
            shape=tuple(int(dim) for dim in tensor.shape),
            dtype=normalize_dtype_name(tensor.dtype),
            data_offset=data_offset,
        )

    def nbytes(self) -> int:
        """Return the byte length of the full stored tensor payload."""
        return tensor_nbytes(self.shape, self.dtype)


@dataclass(frozen=True)
class TensorDimSelection:
    """Lazy selection operation applied to one tensor dimension."""

    op: str
    value: Any

    @staticmethod
    def select_idxs(index: Any) -> "TensorDimSelection":
        """Select by integer indices or a boolean mask."""
        return TensorDimSelection("select_idxs", normalize_indices(index))

    @staticmethod
    def slice(
        start: Optional[int] = None,
        end: Optional[int] = None,
        step: Optional[int] = None,
    ) -> "TensorDimSelection":
        """Select a Python slice on the selected tensor dimension."""
        return TensorDimSelection("slice", (start, end, step))

    @staticmethod
    def repeat(times: int = 2, interleave: bool = True) -> "TensorDimSelection":
        """Repeat the selected dimension, matching DataProto repeat semantics."""
        return TensorDimSelection("repeat", (times, interleave))

    @staticmethod
    def cat(
        selection_groups: Sequence[Sequence["TensorDimSelection"]],
    ) -> "TensorDimSelection":
        """Concatenate independently selected views of the same tensor dimension."""
        return TensorDimSelection(
            "cat", tuple(tuple(group) for group in selection_groups)
        )

    def as_tuple(self) -> tuple[str, Any]:
        """Return the pybind-friendly representation accepted by Mooncake store APIs."""
        if self.op == "cat":
            return self.op, tuple(
                tuple(selection.as_tuple() for selection in group)
                for group in self.value
            )
        return self.op, self.value


@dataclass(frozen=True)
class TensorReadRequest:
    """Materialization request for one remote tensor field."""

    name: str
    ref: TensorFieldRef
    dim: int = 0
    selections: tuple[TensorDimSelection, ...] = ()
    device: Optional[Any] = None

    def output_shape(self) -> tuple[int, ...]:
        """Return the shape after applying lazy dimension selections."""
        return selection_output_shape(self.ref.shape, self.selections, self.dim)

    def output_nbytes(self) -> int:
        """Return the number of bytes needed to materialize the request."""
        return tensor_nbytes(self.output_shape(), self.ref.dtype)

    def store_selections(self) -> list[tuple[str, Any]]:
        """Return pybind-friendly selection tuples for store.get_tensor_dim_selection_into."""
        return [selection.as_tuple() for selection in self.selections]


@dataclass(frozen=True)
class RemoteTensorBatch:
    """Generic metadata-only batch of Mooncake-backed tensor fields."""

    fields: Mapping[str, TensorFieldRef]
    batch_size: int
    dim: int = 0
    selections: tuple[TensorDimSelection, ...] = ()
    device: Optional[Any] = None

    def __len__(self) -> int:
        return self.batch_size

    def keys(self) -> list[str]:
        """Return field names in deterministic insertion order."""
        return list(self.fields.keys())

    def clone(self) -> "RemoteTensorBatch":
        """Return an immutable metadata clone."""
        return replace(
            self, fields=dict(self.fields), selections=tuple(self.selections)
        )

    def to(self, device: Any) -> "RemoteTensorBatch":
        """Set the preferred materialization device in metadata."""
        return replace(self, device=device)

    def select_fields(self, fields: Optional[Sequence[str]]) -> "RemoteTensorBatch":
        """Keep only the requested tensor fields."""
        if fields is None:
            return self.clone()
        selected = {field: self.fields[field] for field in fields}
        return replace(self, fields=selected)

    def select_indices(self, index: Any) -> "RemoteTensorBatch":
        """Record an index or boolean-mask selection on the batch dimension."""
        selection = TensorDimSelection.select_idxs(index)
        batch_size = _selection_length(self.batch_size, selection)
        return self._with_selection(selection, batch_size)

    def slice(
        self, start: Optional[int], end: Optional[int], step: Optional[int]
    ) -> "RemoteTensorBatch":
        """Record a slice selection on the batch dimension."""
        selection = TensorDimSelection.slice(start, end, step)
        batch_size = _selection_length(self.batch_size, selection)
        return self._with_selection(selection, batch_size)

    def repeat(
        self, repeat_times: int = 2, interleave: bool = True
    ) -> "RemoteTensorBatch":
        """Record a repeat operation on the batch dimension."""
        if repeat_times < 0:
            raise ValueError("repeat_times must be non-negative")
        selection = TensorDimSelection.repeat(repeat_times, interleave)
        return self._with_selection(selection, self.batch_size * repeat_times)

    def union(self, rhs: "RemoteTensorBatch") -> "RemoteTensorBatch":
        """Merge disjoint field sets with identical batch selection state."""
        if (
            self.batch_size != rhs.batch_size
            or self.selections != rhs.selections
            or self.dim != rhs.dim
        ):
            raise ValueError("RemoteTensorBatch union requires matching batch metadata")
        overlap = set(self.fields).intersection(rhs.fields)
        if overlap:
            raise ValueError(
                f"RemoteTensorBatch union has duplicate fields: {sorted(overlap)}"
            )
        fields = dict(self.fields)
        fields.update(rhs.fields)
        return replace(self, fields=fields)

    @staticmethod
    def cat(batches: Sequence["RemoteTensorBatch"]) -> "RemoteTensorBatch":
        """Concatenate compatible remote batches along the selected dimension."""
        if not batches:
            raise ValueError("RemoteTensorBatch.cat requires non-empty batches")
        first = batches[0]
        for batch in batches:
            if (
                batch.fields != first.fields
                or batch.dim != first.dim
                or batch.device != first.device
            ):
                raise ValueError(
                    "RemoteTensorBatch.cat requires matching fields, dim, and device"
                )
        selection = TensorDimSelection.cat([batch.selections for batch in batches])
        return replace(
            first,
            batch_size=sum(batch.batch_size for batch in batches),
            selections=(selection,),
        )

    def read_requests(
        self, fields: Optional[Sequence[str]] = None
    ) -> list[TensorReadRequest]:
        """Build materialization requests for selected fields."""
        selected_fields = self.keys() if fields is None else list(fields)
        return [
            TensorReadRequest(
                name=field,
                ref=self.fields[field],
                dim=self.dim,
                selections=self.selections,
                device=self.device,
            )
            for field in selected_fields
        ]

    def _with_selection(
        self, selection: TensorDimSelection, batch_size: int
    ) -> "RemoteTensorBatch":
        return replace(
            self, batch_size=batch_size, selections=(*self.selections, selection)
        )


class RemoteTensorBatchMaterializer:
    """Materialize RemoteTensorBatch requests through Mooncake registered buffers."""

    def __init__(self, store: Any, buffer_allocator: Optional[Any] = None):
        self.store = store
        self.buffer_allocator = buffer_allocator or BytearrayBufferAllocator()

    def materialize_buffers(
        self,
        remote_batch: RemoteTensorBatch,
        fields: Optional[Sequence[str]] = None,
    ) -> dict[str, tuple[Any, tuple[int, ...], Union[str, Any]]]:
        """Fetch selected fields into allocated buffers and return buffers with shape and dtype metadata.

        Allocator regions must keep their backing ``buffer`` valid after ``close()``; closing a region releases only
        temporary exported views or registration handles owned by the allocator.
        """
        requests = remote_batch.read_requests(fields)
        allocated = {
            request.name: self.buffer_allocator.allocate(
                _checked_output_nbytes(request)
            )
            for request in requests
        }
        try:
            for request in requests:
                self.materialize_request_into_region(request, allocated[request.name])
            return {
                request.name: (
                    allocated[request.name].buffer,
                    request.output_shape(),
                    request.ref.dtype,
                )
                for request in requests
            }
        finally:
            for region in allocated.values():
                region.close()

    def materialize_tensors(
        self,
        remote_batch: RemoteTensorBatch,
        fields: Optional[Sequence[str]] = None,
    ) -> dict[str, Any]:
        """Fetch selected fields and return torch tensors backed by the materialized buffers."""
        if torch is None:
            raise ImportError("torch is required to materialize remote tensors")
        requests = remote_batch.read_requests(fields)
        allocated = {
            request.name: self.buffer_allocator.allocate(_checked_output_nbytes(request))
            for request in requests
        }
        tensors = {}
        try:
            for request in requests:
                region = allocated[request.name]
                self.materialize_request_into_region(request, region)
                tensor = torch.frombuffer(
                    region.buffer,
                    dtype=_torch_dtype(request.ref.dtype),
                    count=int(prod(request.output_shape())),
                ).reshape(request.output_shape())
                if request.device is not None:
                    tensor = tensor.to(request.device)
                tensors[request.name] = tensor
            return tensors
        finally:
            for region in allocated.values():
                region.close()

    def materialize_requests_into(
        self, requests: Sequence[TensorReadRequest], buffers: Mapping[str, Any]
    ) -> dict[str, int]:
        """Fetch multiple requests into caller-provided writable buffers."""
        return {
            request.name: self.materialize_request_into(request, buffers[request.name])
            for request in requests
        }

    def materialize_request_into(self, request: TensorReadRequest, buffer: Any) -> int:
        """Fetch one request into a caller-provided writable buffer object."""
        with writable_buffer_region(buffer) as region:
            return self.materialize_request_into_region(request, region)

    def materialize_request_into_region(
        self, request: TensorReadRequest, region: "WritableBufferRegion"
    ) -> int:
        """Fetch one request into a preallocated writable buffer region."""
        required_size = _checked_output_nbytes(request)
        if region.size < required_size:
            raise ValueError(
                f"buffer too small: need {required_size} bytes, got {region.size}"
            )
        if required_size == 0:
            return 0
        should_unregister = not region.registered
        if should_unregister:
            register_ret = self.store.register_buffer(region.ptr, region.size)
            if register_ret != 0:
                raise RuntimeError(f"register_buffer failed with retcode {register_ret}")
        transfer_error: Optional[BaseException] = None
        try:
            ret = self.store.get_tensor_dim_selection_into(
                request.ref.key,
                region.ptr,
                required_size,
                list(request.ref.shape),
                normalize_dtype_name(request.ref.dtype),
                request.dim,
                request.store_selections(),
                request.ref.data_offset,
            )
            if ret < 0:
                raise RuntimeError(
                    f"get_tensor_dim_selection_into failed with retcode {ret}"
                )
            return int(ret)
        except BaseException as exc:
            transfer_error = exc
            raise
        finally:
            if should_unregister:
                unregister_ret = self.store.unregister_buffer(region.ptr)
                if unregister_ret != 0:
                    cleanup_error = RuntimeError(
                        f"unregister_buffer failed with retcode {unregister_ret}"
                    )
                    if transfer_error is not None:
                        raise cleanup_error from transfer_error
                    raise cleanup_error


@dataclass
class ReusableRegisteredBufferAllocator:
    """Keep allocated writable buffers registered with Mooncake until close()."""

    store: Any
    _regions: list["WritableBufferRegion"] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def allocate(self, size: int) -> "WritableBufferRegion":
        with self._lock:
            return self._add_region(size).view_region(size)

    def close(self) -> None:
        with self._lock:
            errors = []
            remaining = []
            for region in self._regions:
                ret = self.store.unregister_buffer(region.ptr)
                if ret != 0:
                    errors.append(ret)
                    remaining.append(region)
                    continue
                region.registered = False
                region.close()
            self._regions = remaining
            if errors:
                raise RuntimeError(f"unregister_buffer failed with retcodes {errors}")

    def __enter__(self) -> "ReusableRegisteredBufferAllocator":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _add_region(self, size: int) -> "WritableBufferRegion":
        buffer = bytearray(size)
        region = writable_buffer_region(buffer, registered=True)
        ret = self.store.register_buffer(region.ptr, region.size)
        if ret != 0:
            region.close()
            raise RuntimeError(f"register_buffer failed with retcode {ret}")
        self._regions.append(region)
        return region


@dataclass(frozen=True)
class BatchReadItem:
    """One whole-key read into a destination pointer."""

    key: str
    ptr: int
    size: int
    registered: bool = False


class BatchReadPlanner:
    """Plan and execute whole-key reads through Mooncake batch_get_into."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self._items: list[BatchReadItem] = []

    def add(self, key: str, ptr: int, size: int, registered: bool = False) -> None:
        """Add one whole-key read into a destination pointer."""
        if size < 0:
            raise ValueError("size must be non-negative")
        self._items.append(BatchReadItem(key=key, ptr=ptr, size=size, registered=registered))

    def execute(self) -> None:
        """Execute planned reads and verify every result size."""
        items = [item for item in self._items if item.size > 0]
        if not items:
            return
        registered_ptrs: list[int] = []
        transfer_error: Optional[BaseException] = None
        try:
            for item in items:
                if item.registered:
                    continue
                ret = self.store.register_buffer(item.ptr, item.size)
                if ret != 0:
                    raise RuntimeError(f"register_buffer failed for {item.key}: {ret}")
                registered_ptrs.append(item.ptr)
            keys = [item.key for item in items]
            ptrs = [item.ptr for item in items]
            sizes = [item.size for item in items]
            read_results = self.store.batch_get_into(keys, ptrs, sizes)
            if len(read_results) != len(items):
                raise RuntimeError(f"batch_get_into returned {len(read_results)} results for {len(items)} reads")
            for item, read_size in zip(items, read_results):
                if read_size != item.size:
                    raise RuntimeError(f"batch_get_into failed for {item.key}: expected {item.size}, got {read_size}")
        except BaseException as exc:
            transfer_error = exc
            raise
        finally:
            cleanup_errors = []
            for ptr in reversed(registered_ptrs):
                ret = self.store.unregister_buffer(ptr)
                if ret != 0:
                    cleanup_errors.append(ret)
            if cleanup_errors:
                cleanup_error = RuntimeError(f"unregister_buffer failed with retcodes {cleanup_errors}")
                if transfer_error is not None:
                    raise cleanup_error from transfer_error
                raise cleanup_error


class RangeReadPlanner:
    """Plan and execute ranged reads into one registered destination buffer."""

    def __init__(self, store: Any, ptr: int, capacity: Optional[int] = None) -> None:
        if capacity is not None and capacity < 0:
            raise ValueError("capacity must be non-negative")
        self.store = store
        self.ptr = ptr
        self.capacity = None if capacity is None else int(capacity)
        self._keys: list[str] = []
        self._dst_offsets: list[list[int]] = []
        self._src_offsets: list[list[int]] = []
        self._sizes: list[list[int]] = []

    def add_whole_key(self, key: str, dst_offset: int, size: int) -> None:
        """Read a whole key into ``ptr + dst_offset``."""
        if dst_offset < 0 or size < 0:
            raise ValueError("dst_offset and size must be non-negative")
        if self.capacity is not None and dst_offset + size > self.capacity:
            raise ValueError(f"range read exceeds destination capacity: {dst_offset + size} > {self.capacity}")
        if size == 0:
            return
        self._keys.append(key)
        self._dst_offsets.append([dst_offset])
        self._src_offsets.append([0])
        self._sizes.append([size])

    def execute(self) -> None:
        """Execute planned range reads and verify every result size."""
        if not self._keys:
            return
        results = self.store.get_into_ranges(
            [self.ptr],
            [self._keys],
            [self._dst_offsets],
            [self._src_offsets],
            [self._sizes],
        )
        if len(results) != 1:
            raise RuntimeError(f"get_into_ranges returned {len(results)} buffer results for one destination")
        if len(results[0]) != len(self._keys):
            raise RuntimeError(f"get_into_ranges returned {len(results[0])} key results for {len(self._keys)} keys")
        for key, expected_sizes, key_results in zip(self._keys, self._sizes, results[0]):
            if len(key_results) != len(expected_sizes):
                raise RuntimeError(
                    f"get_into_ranges returned {len(key_results)} ranges for {key}, expected {len(expected_sizes)}"
                )
            for expected_size, read_size in zip(expected_sizes, key_results):
                if read_size != expected_size:
                    raise RuntimeError(
                        f"get_into_ranges failed for {key}: expected {expected_size}, got {read_size}"
                    )


@dataclass(frozen=True)
class RegisteredBufferPoolConfig:
    """Configuration for a reusable Mooncake registered scratch buffer pool."""

    max_bytes: int
    min_size_class: int = 64 * 1024
    max_size_class: Optional[int] = None
    alignment: int = 8 * 1024 * 1024
    block_on_exhaustion: bool = True
    default_timeout: Optional[float] = None
    max_regions: Optional[int] = None
    prewarm_size: Optional[int] = None
    prewarm_count: int = 0


@dataclass(frozen=True)
class RegisteredBufferPoolStats:
    """Point-in-time counters for a registered scratch buffer pool."""

    size_classes: dict[int, int]
    free_regions: int
    in_use_regions: int
    total_regions: int
    total_bytes: int
    free_bytes: int
    in_use_bytes: int
    acquire_count: int
    reuse_count: int
    allocate_count: int
    oversize_allocate_count: int
    wait_count: int
    release_count: int
    register_s: float
    unregister_s: float


@dataclass
class RegisteredBufferLease:
    """Context-managed lease for a registered scratch buffer."""

    pool: "RegisteredBufferPool"
    region: "WritableBufferRegion"
    requested_size: int
    closed: bool = False

    @property
    def buffer(self) -> Any:
        return self.region.buffer

    @property
    def ptr(self) -> int:
        return self.region.ptr

    @property
    def size(self) -> int:
        return self.requested_size

    def view_region(self) -> "WritableBufferRegion":
        return self.region.view_region(self.requested_size)

    def release(self) -> None:
        if self.closed:
            raise RuntimeError("registered buffer lease released twice")
        self.pool.release(self)
        self.closed = True

    def __enter__(self) -> "RegisteredBufferLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


@dataclass
class ExternalRegisteredBufferLease:
    """Context-managed registration for caller-owned destination memory."""

    pool: "RegisteredBufferPool"
    ptr: int
    size: int
    closed: bool = False

    def release(self) -> None:
        self.pool.unregister_external(self)

    def __enter__(self) -> "ExternalRegisteredBufferLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


class RegisteredBufferPool:
    """Reusable pool of Mooncake-registered scratch buffers.

    The pool is intended for temporary transfer buffers whose contents are copied or consumed before release.
    Do not use it for materialized tensors or returned buffers, because released regions may be reused later.
    """

    def __init__(
        self,
        store: Any,
        max_bytes: int,
        *,
        min_size_class: int = 64 * 1024,
        max_size_class: Optional[int] = None,
        alignment: int = 8 * 1024 * 1024,
        block_on_exhaustion: bool = True,
        default_timeout: Optional[float] = None,
        max_regions: Optional[int] = None,
        prewarm_size: Optional[int] = None,
        prewarm_count: int = 0,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if min_size_class <= 0 or alignment <= 0:
            raise ValueError("min_size_class and alignment must be positive")
        self.store = store
        self.max_bytes = int(max_bytes)
        self.min_size_class = int(min_size_class)
        self.max_size_class = min(int(max_size_class or max_bytes), self.max_bytes)
        self.alignment = int(alignment)
        self.block_on_exhaustion = block_on_exhaustion
        self.default_timeout = default_timeout
        self.max_regions = max_regions
        self._free: dict[int, Deque[WritableBufferRegion]] = defaultdict(deque)
        self._in_use: set[int] = set()
        self._external_in_use: set[int] = set()
        self._regions: dict[int, WritableBufferRegion] = {}
        self._region_sizes: dict[int, int] = {}
        self._closed = False
        self._closing = False
        self._total_bytes = 0
        self._reserved_bytes = 0
        self._acquire_count = 0
        self._reuse_count = 0
        self._allocate_count = 0
        self._oversize_allocate_count = 0
        self._wait_count = 0
        self._release_count = 0
        self._register_s = 0.0
        self._unregister_s = 0.0
        self._condition = threading.Condition(threading.Lock())
        if prewarm_count:
            if prewarm_size is None:
                raise ValueError("prewarm_size is required when prewarm_count is positive")
            self.prewarm(prewarm_size, prewarm_count)

    @classmethod
    def from_config(cls, store: Any, config: RegisteredBufferPoolConfig) -> "RegisteredBufferPool":
        """Create and optionally prewarm a pool from configuration."""
        return cls(
            store,
            max_bytes=config.max_bytes,
            min_size_class=config.min_size_class,
            max_size_class=config.max_size_class,
            alignment=config.alignment,
            block_on_exhaustion=config.block_on_exhaustion,
            default_timeout=config.default_timeout,
            max_regions=config.max_regions,
            prewarm_size=config.prewarm_size,
            prewarm_count=config.prewarm_count,
        )

    def acquire(
        self,
        size: int,
        *,
        block: Optional[bool] = None,
        timeout: Optional[float] = None,
    ) -> RegisteredBufferLease:
        """Acquire a registered buffer lease of at least ``size`` bytes."""
        if size < 0:
            raise ValueError("size must be non-negative")
        should_block = self.block_on_exhaustion if block is None else block
        timeout_s = self.default_timeout if timeout is None else timeout
        end_time = None if timeout_s is None else time.monotonic() + timeout_s
        self._raise_if_impossible(size)
        with self._condition:
            while True:
                self._raise_if_closed()
                lease = self._try_acquire_locked(size)
                if lease is not None:
                    return lease
                if not should_block:
                    raise RuntimeError("registered buffer pool is exhausted")
                remaining = None if end_time is None else end_time - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("timed out waiting for registered buffer")
                self._wait_count += 1
                self._condition.wait(remaining)

    def try_acquire(self, size: int) -> RegisteredBufferLease:
        """Acquire a lease without waiting for capacity."""
        return self.acquire(size, block=False)

    def buffer(self, size: int, *, timeout: Optional[float] = None) -> RegisteredBufferLease:
        """Return a context-managed registered buffer lease."""
        return self.acquire(size, timeout=timeout)

    def try_register_external(self, ptr: int, size: int) -> Optional[ExternalRegisteredBufferLease]:
        """Best-effort registration for caller-owned destination memory."""
        if size < 0:
            raise ValueError("size must be non-negative")
        with self._condition:
            self._raise_if_closed()
            if size == 0:
                return ExternalRegisteredBufferLease(self, ptr, size)
            if ptr in self._external_in_use:
                raise RuntimeError("external buffer is already registered through this pool")
        register_start = time.perf_counter()
        ret = self.store.register_buffer(ptr, size)
        register_s = time.perf_counter() - register_start
        with self._condition:
            self._register_s += register_s
            if ret != 0:
                return None
            try:
                self._raise_if_closed()
            except RuntimeError:
                pass
            else:
                self._external_in_use.add(ptr)
                self._condition.notify_all()
                return ExternalRegisteredBufferLease(self, ptr, size)
        unregister_start = time.perf_counter()
        unregister_ret = self.store.unregister_buffer(ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
        if unregister_ret != 0:
            raise RuntimeError(f"unregister_buffer failed with retcode {unregister_ret}")
        raise RuntimeError("registered buffer pool is closed")

    def unregister_external(self, lease: ExternalRegisteredBufferLease) -> None:
        """Unregister caller-owned destination memory previously registered by the pool."""
        with self._condition:
            if lease.closed:
                raise RuntimeError("external registered buffer lease released twice")
            lease.closed = True
            if lease.size == 0:
                return
            if lease.ptr not in self._external_in_use:
                lease.closed = False
                raise RuntimeError("external registered buffer lease does not belong to this pool or is not active")
            self._external_in_use.remove(lease.ptr)
        unregister_start = time.perf_counter()
        ret = self.store.unregister_buffer(lease.ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
            if ret != 0:
                self._external_in_use.add(lease.ptr)
                lease.closed = False
                self._condition.notify_all()
                raise RuntimeError(f"unregister_buffer failed with retcode {ret}")
            self._condition.notify_all()

    def iter_transfer_groups(self, sizes: Sequence[int]) -> list[list[int]]:
        """Group transfer item indexes by the pool's scratch-buffer sizing policy."""
        groups: list[list[int]] = []
        current_group: list[int] = []
        current_bytes = 0
        for index, size in enumerate(sizes):
            item_size = int(size)
            if item_size < 0:
                raise ValueError("transfer sizes must be non-negative")
            if item_size == 0:
                continue
            if current_group and current_bytes + item_size > self.max_size_class:
                groups.append(current_group)
                current_group = []
                current_bytes = 0
            current_group.append(index)
            current_bytes += item_size
        if current_group:
            groups.append(current_group)
        return groups

    def release(self, lease: RegisteredBufferLease) -> None:
        """Return a lease to the pool."""
        region = lease.region
        with self._condition:
            if region.ptr not in self._in_use:
                raise RuntimeError("registered buffer lease does not belong to this pool or is not active")
            self._in_use.remove(region.ptr)
            region_size = self._region_sizes[region.ptr]
            should_unregister = self._closed or region_size > self.max_size_class
        if should_unregister:
            try:
                self._unregister_region(region)
            except RuntimeError:
                with self._condition:
                    self._in_use.add(region.ptr)
                    self._condition.notify_all()
                raise
            with self._condition:
                self._release_count += 1
                self._condition.notify_all()
            return
        with self._condition:
            self._free[region_size].append(region)
            self._release_count += 1
            self._condition.notify_all()

    def prewarm(self, size: int, count: int) -> None:
        """Register ``count`` reusable buffers for a size class ahead of the transfer path."""
        if count < 0:
            raise ValueError("count must be non-negative")
        size_class = self._size_class(size)
        for _ in range(count):
            self._append_prewarmed_region(size_class)

    def ensure_prewarmed(self, size: int, count: int) -> None:
        """Ensure at least ``count`` free reusable buffers exist for the request size class."""
        if count < 0:
            raise ValueError("count must be non-negative")
        size_class = self._size_class(size)
        while True:
            with self._condition:
                missing_count = max(0, count - len(self._free[size_class]))
            if missing_count == 0:
                return
            self._append_prewarmed_region(size_class)

    def stats(self) -> RegisteredBufferPoolStats:
        """Return current pool counters."""
        with self._condition:
            free_by_class = {size: len(regions) for size, regions in self._free.items() if regions}
            free_bytes = sum(size * count for size, count in free_by_class.items())
            in_use_bytes = sum(self._region_sizes[ptr] for ptr in self._in_use)
            return RegisteredBufferPoolStats(
                size_classes=free_by_class,
                free_regions=sum(free_by_class.values()),
                in_use_regions=len(self._in_use) + len(self._external_in_use),
                total_regions=len(self._regions),
                total_bytes=self._total_bytes,
                free_bytes=free_bytes,
                in_use_bytes=in_use_bytes,
                acquire_count=self._acquire_count,
                reuse_count=self._reuse_count,
                allocate_count=self._allocate_count,
                oversize_allocate_count=self._oversize_allocate_count,
                wait_count=self._wait_count,
                release_count=self._release_count,
                register_s=self._register_s,
                unregister_s=self._unregister_s,
            )

    def close(self) -> None:
        """Unregister all free regions and mark the pool closed."""
        with self._condition:
            if self._closed:
                return
            while self._reserved_bytes:
                self._condition.wait()
            self._closing = True
            active_count = len(self._in_use) + len(self._external_in_use)
            if active_count:
                self._closing = False
                raise RuntimeError(f"cannot close registered buffer pool with {active_count} active leases")
            free_regions = []
            for regions in self._free.values():
                while regions:
                    free_regions.append(regions.pop())
        errors = []
        retained = []
        for region in free_regions:
            try:
                self._unregister_region(region)
            except RuntimeError as exc:
                errors.append(exc)
                retained.append(region)
        with self._condition:
            if retained:
                for region in retained:
                    self._free[self._region_sizes[region.ptr]].append(region)
            else:
                self._free.clear()
            if errors:
                self._closing = False
                self._condition.notify_all()
                raise RuntimeError(f"unregister_buffer failed for {len(errors)} pooled regions") from errors[0]
            self._closed = True
            self._closing = False
            self._condition.notify_all()

    def __enter__(self) -> "RegisteredBufferPool":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def _try_acquire_locked(self, requested_size: int) -> Optional[RegisteredBufferLease]:
        oversize_class = self._oversize_size_class(requested_size)
        if oversize_class is not None:
            return self._allocate_acquire_unlocked(oversize_class, requested_size, oversize=True)
        size_class = self._size_class(requested_size)
        if self._free[size_class]:
            region = self._free[size_class].pop()
            self._in_use.add(region.ptr)
            self._acquire_count += 1
            self._reuse_count += 1
            return RegisteredBufferLease(self, region, requested_size)
        if self._has_capacity_for(size_class):
            return self._allocate_acquire_unlocked(size_class, requested_size, oversize=False)
        return None

    def _has_capacity_for(self, size_class: int) -> bool:
        if size_class > self.max_bytes:
            return False
        if self._total_bytes + self._reserved_bytes + size_class > self.max_bytes:
            return False
        if self.max_regions is None:
            return True
        reserved_regions = math.ceil(self._reserved_bytes / size_class) if size_class else 0
        return len(self._regions) + reserved_regions < self.max_regions

    def _raise_if_impossible(self, size: int) -> None:
        size_class = self._oversize_size_class_for_empty_pool(size) or self._size_class(size)
        if size_class > self.max_bytes:
            raise ValueError(f"requested buffer size exceeds pool capacity: {size} > {self.max_bytes}")
        if self.max_regions == 0:
            raise ValueError("registered buffer pool max_regions is zero")

    def _oversize_size_class_for_empty_pool(self, size: int) -> Optional[int]:
        if size <= self.max_size_class:
            return None
        return ((int(size) + self.alignment - 1) // self.alignment) * self.alignment

    def _allocate_acquire_unlocked(
        self,
        size_class: int,
        requested_size: int,
        *,
        oversize: bool,
    ) -> RegisteredBufferLease:
        self._reserve_region(size_class)
        self._condition.release()
        try:
            region = self._register_region(size_class)
        except BaseException:
            self._condition.acquire()
            self._reserved_bytes -= size_class
            self._condition.notify_all()
            raise
        self._condition.acquire()
        self._reserved_bytes -= size_class
        self._condition.notify_all()
        try:
            self._raise_if_closed()
        except RuntimeError:
            self._condition.release()
            try:
                self._unregister_region(region)
            finally:
                self._condition.acquire()
            raise
        self._in_use.add(region.ptr)
        self._acquire_count += 1
        if oversize:
            self._oversize_allocate_count += 1
        else:
            self._allocate_count += 1
        return RegisteredBufferLease(self, region, requested_size)

    def _reserve_region(self, size_class: int) -> None:
        self._raise_if_closed()
        if self._closing:
            raise RuntimeError("registered buffer pool is closing")
        if not self._has_capacity_for(size_class):
            raise RuntimeError("registered buffer pool capacity exceeded")
        self._reserved_bytes += size_class

    def _append_prewarmed_region(self, size_class: int) -> None:
        region = self._allocate_registered_region(size_class)
        with self._condition:
            try:
                self._raise_if_closed()
                if self._closing:
                    raise RuntimeError("registered buffer pool is closing")
                self._free[size_class].append(region)
                self._condition.notify_all()
                return
            except BaseException:
                pass
        self._unregister_region(region)
        raise RuntimeError("registered buffer pool is closed")

    def _allocate_registered_region(self, size_class: int) -> "WritableBufferRegion":
        with self._condition:
            self._reserve_region(size_class)
        try:
            region = self._register_region(size_class)
        except BaseException:
            with self._condition:
                self._reserved_bytes -= size_class
                self._condition.notify_all()
            raise
        with self._condition:
            self._reserved_bytes -= size_class
            self._condition.notify_all()
        return region

    def _register_region(self, size: int) -> "WritableBufferRegion":
        buffer = bytearray(size)
        region = writable_buffer_region(buffer, registered=True)
        register_start = time.perf_counter()
        ret = self.store.register_buffer(region.ptr, region.size)
        register_s = time.perf_counter() - register_start
        if ret != 0:
            region.close()
            with self._condition:
                self._register_s += register_s
            raise RuntimeError(f"register_buffer failed with retcode {ret}")
        with self._condition:
            self._register_s += register_s
            self._regions[region.ptr] = region
            self._region_sizes[region.ptr] = region.size
            self._total_bytes += region.size
        return region

    def _unregister_region(self, region: "WritableBufferRegion") -> None:
        unregister_start = time.perf_counter()
        ret = self.store.unregister_buffer(region.ptr)
        unregister_s = time.perf_counter() - unregister_start
        with self._condition:
            self._unregister_s += unregister_s
        if ret != 0:
            raise RuntimeError(f"unregister_buffer failed with retcode {ret}")
        with self._condition:
            self._regions.pop(region.ptr, None)
            self._total_bytes -= self._region_sizes.pop(region.ptr, region.size)
        region.registered = False
        region.close()

    def _size_class(self, size: int) -> int:
        size = max(int(size), 1)
        capped_size = min(size, self.max_size_class)
        if capped_size <= self.min_size_class:
            return self.min_size_class
        if capped_size <= self.alignment:
            return 1 << (capped_size - 1).bit_length()
        return ((capped_size + self.alignment - 1) // self.alignment) * self.alignment

    def _oversize_size_class(self, size: int) -> Optional[int]:
        if size <= self.max_size_class:
            return None
        rounded_size = ((int(size) + self.alignment - 1) // self.alignment) * self.alignment
        if self._total_bytes + self._reserved_bytes + rounded_size > self.max_bytes:
            return None
        if self.max_regions is not None:
            reserved_regions = 1 if self._reserved_bytes else 0
            if len(self._regions) + reserved_regions >= self.max_regions:
                return None
        return rounded_size

    def _raise_if_closed(self) -> None:
        if self._closed:
            raise RuntimeError("registered buffer pool is closed")


@dataclass
class WritableBufferRegion:
    """Writable buffer pointer that keeps the Python buffer export alive."""

    buffer: Any
    view: memoryview
    c_buffer: Any
    ptr: int
    size: int
    registered: bool = False
    owns_view: bool = True
    closed: bool = False

    def view_region(self, size: int) -> "WritableBufferRegion":
        if self.closed:
            raise ValueError("buffer region is closed")
        if size > self.size:
            raise ValueError(f"buffer too small: need {size} bytes, got {self.size}")
        return WritableBufferRegion(
            buffer=self.buffer,
            view=self.view,
            c_buffer=self.c_buffer,
            ptr=self.ptr,
            size=size,
            registered=self.registered,
            owns_view=False,
        )

    def close(self) -> None:
        if self.closed:
            return
        self.c_buffer = None
        if self.owns_view:
            self.view.release()
        self.closed = True

    def __enter__(self) -> "WritableBufferRegion":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


class BytearrayBufferAllocator:
    """Allocate writable bytearray buffers for remote tensor materialization."""

    def allocate(self, size: int) -> WritableBufferRegion:
        return writable_buffer_region(bytearray(size))


def normalize_dtype_name(dtype: Union[str, Any]) -> str:
    """Normalize a dtype object or name to the string accepted by Mooncake store APIs."""
    return str(dtype).removeprefix("torch.").lower()


def _torch_dtype(dtype: Union[str, Any]) -> Any:
    if torch is None:
        raise ImportError("torch is required for dtype conversion")
    if isinstance(dtype, torch.dtype):
        return dtype
    dtype_name = normalize_dtype_name(dtype)
    if dtype_name == "float8_e4m3fn":
        return torch.float8_e4m3fn
    if dtype_name == "float8_e5m2":
        return torch.float8_e5m2
    return getattr(torch, dtype_name)


def dtype_byte_size(dtype: Union[str, Any]) -> int:
    """Return the byte size for a dtype name or torch dtype."""
    if torch is not None and isinstance(dtype, torch.dtype):
        return torch.empty((), dtype=dtype).element_size()
    dtype_name = normalize_dtype_name(dtype)
    if dtype_name not in DTYPE_BYTE_SIZES:
        raise ValueError(f"Unsupported dtype for remote tensor batch: {dtype}")
    return DTYPE_BYTE_SIZES[dtype_name]


def tensor_nbytes(shape: Sequence[int], dtype: Union[str, Any]) -> int:
    """Return the number of bytes for a fixed-shape tensor."""
    if any(dim < 0 for dim in shape):
        raise ValueError("shape dimensions must be non-negative")
    return int(prod(shape)) * dtype_byte_size(dtype)


def normalize_indices(index: Any) -> list[Union[int, bool]]:
    """Normalize list, numpy array, or torch tensor indices to a plain Python list."""
    if torch is not None and isinstance(index, torch.Tensor):
        index = index.detach().cpu().tolist()
    elif hasattr(index, "tolist"):
        index = index.tolist()
    if isinstance(index, (int, bool)):
        return [index]
    return list(index)


def selection_output_shape(
    shape: Sequence[int],
    selections: Sequence[Union[TensorDimSelection, tuple[str, Any], Mapping[str, Any]]],
    dim: int = 0,
) -> tuple[int, ...]:
    """Return output shape after applying selections to one tensor dimension."""
    normalized_shape = tuple(int(extent) for extent in shape)
    if not normalized_shape:
        raise ValueError("shape must have at least one dimension")
    normalized_dim = dim + len(normalized_shape) if dim < 0 else dim
    if normalized_dim < 0 or normalized_dim >= len(normalized_shape):
        raise ValueError("selection dim out of range")
    dim_size = normalized_shape[normalized_dim]
    for selection in selections:
        dim_size = _selection_length(dim_size, selection)
    return (
        *normalized_shape[:normalized_dim],
        dim_size,
        *normalized_shape[normalized_dim + 1 :],
    )


def _checked_output_nbytes(request: TensorReadRequest) -> int:
    output_nbytes = request.output_nbytes()
    if output_nbytes > MAX_TENSOR_DIM_SELECTION_OUTPUT_BYTES:
        raise ValueError(
            f"selected tensor output exceeds {MAX_TENSOR_DIM_SELECTION_OUTPUT_BYTES} bytes: {output_nbytes}"
        )
    return output_nbytes


def writable_buffer_region(buffer: Any, registered: bool = False) -> WritableBufferRegion:
    """Return a writable buffer region while keeping the Python buffer export alive."""
    view = memoryview(buffer)
    if view.readonly:
        view.release()
        raise ValueError("buffer must be writable")
    if view.format != "B":
        try:
            contiguous = view.cast("B")
        except BaseException:
            view.release()
            raise
        view.release()
    else:
        contiguous = view
    try:
        c_buffer = (ctypes.c_ubyte * contiguous.nbytes).from_buffer(contiguous)
    except BaseException:
        contiguous.release()
        raise
    return WritableBufferRegion(
        buffer=buffer,
        view=contiguous,
        c_buffer=c_buffer,
        ptr=ctypes.addressof(c_buffer),
        size=contiguous.nbytes,
        registered=registered,
    )


def _selection_length(
    length: int,
    selection: Union[TensorDimSelection, tuple[str, Any], Mapping[str, Any]],
) -> int:
    op, value = _selection_op_value(selection)
    if op == "select_idxs":
        indices = normalize_indices(value)
        if all(isinstance(index, bool) for index in indices):
            if len(indices) != length:
                raise IndexError("bool mask length must match current dimension")
            return sum(1 for index in indices if index)
        return len(indices)
    if op == "slice":
        return len(range(length)[slice(*value)])
    if op == "repeat":
        repeat_times, _ = value
        if repeat_times < 0:
            raise ValueError("repeat_times must be non-negative")
        return length * repeat_times
    if op == "cat":
        return sum(_selection_group_length(length, group) for group in value)
    raise ValueError(f"Unsupported tensor dimension selection op: {op}")


def _selection_group_length(
    length: int,
    selections: Sequence[Union[TensorDimSelection, tuple[str, Any], Mapping[str, Any]]],
) -> int:
    for selection in selections:
        length = _selection_length(length, selection)
    return length


def _selection_op_value(
    selection: Union[TensorDimSelection, tuple[str, Any], Mapping[str, Any]],
) -> tuple[str, Any]:
    if isinstance(selection, TensorDimSelection):
        return selection.op, selection.value
    if isinstance(selection, Mapping):
        return str(selection["op"]), selection["value"]
    return selection
