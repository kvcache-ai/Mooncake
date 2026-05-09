from __future__ import annotations

import ctypes
import threading
from dataclasses import dataclass, field, replace
from math import prod
from typing import Any, Optional, Sequence, Union
from collections.abc import Mapping

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
