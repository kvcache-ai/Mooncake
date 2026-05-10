from __future__ import annotations

import ctypes
import json
import math
import os
import pickle
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

import numpy as np
import torch

try:
    from mooncake.remote_tensor_batch import (
        BatchReadPlanner,
        RangeReadPlanner,
        RegisteredBufferPool,
        RegisteredBufferPoolConfig,
    )
except ImportError:  # pragma: no cover - optional helper for local tests
    BatchReadPlanner = None  # type: ignore[assignment]
    RangeReadPlanner = None  # type: ignore[assignment]
    RegisteredBufferPool = None  # type: ignore[assignment]
    RegisteredBufferPoolConfig = None  # type: ignore[assignment]


MAX_STRUCT_KEYS = 64
MAX_EXPAND_LIST_LEN = 8
MAX_SAMPLE_ROWS_FOR_INFERENCE = 128
MAX_JSON_INLINE_BYTES = 1 << 20
RAGGED_TENSOR_PART_CHUNK_BYTES = int(os.environ.get("MOONCAKE_DATAPROTO_RAGGED_PART_BYTES", str(512 * 1024 ** 2)))
RAGGED_BYTES_PART_CHUNK_BYTES = int(os.environ.get("MOONCAKE_DATAPROTO_BYTES_PART_BYTES", str(512 * 1024 ** 2)))
REGISTERED_BUFFER_POOL_BYTES = int(os.environ.get("MOONCAKE_REGISTERED_BUFFER_POOL_BYTES", str(2 * 1024 ** 3)))
REGISTERED_BUFFER_POOL_MAX_BUFFER_BYTES = int(
    os.environ.get("MOONCAKE_REGISTERED_BUFFER_POOL_MAX_BUFFER_BYTES", str(512 * 1024 ** 2))
)
REGISTERED_BUFFER_POOL_PREWARM_BYTES = int(os.environ.get("MOONCAKE_REGISTERED_BUFFER_POOL_PREWARM_BYTES", "0"))
REGISTERED_BUFFER_POOL_PREWARM_COUNT = int(os.environ.get("MOONCAKE_REGISTERED_BUFFER_POOL_PREWARM_COUNT", "0"))
_STORE_PROFILES: Dict[int, Dict[str, Any]] = {}
_REGISTERED_BUFFER_POOLS: Dict[int, Any] = {}
_TRANSFER_ADAPTIVE_STATES: Dict[int, "DataProtoTransferAdaptiveState"] = {}
_STORE_STATE_LOCK = threading.RLock()


@dataclass
class CodecDecision:
    accepted: bool
    codec: str
    reason: str
    confidence: str
    normalized_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EncodedLeaf:
    path: str
    codec: str
    rows: int
    bytes: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    encode_s: float = 0.0


@dataclass
class RemoteDataProtoRef:
    object_id: str
    row_count: int
    manifest_key: str
    manifest: Dict[str, Any]


@dataclass
class RemoteDataProtoShardedRef:
    object_id: str
    row_count: int
    manifest_key: str
    manifest: Dict[str, Any]


@dataclass
class DataProtoShardPolicy:
    enabled: bool = False
    adaptive: bool = False
    num_shards: Optional[int] = None
    target_shard_bytes: int = 2 * 1024 ** 3
    max_shards: int = 16
    max_inflight_get: int = 4
    small_payload_threshold: int = 256 * 1024 ** 2
    row_oriented_ratio_threshold: float = 0.5
    dense_ratio_threshold: float = 0.8
    byte_balanced: bool = True


@dataclass
class DataProtoTransferProfile:
    total_bytes: int
    dense_tensor_bytes: int
    row_oriented_bytes: int
    row_bytes_p50: int
    row_bytes_p95: int
    row_bytes_max: int
    row_skew_ratio: float
    row_skewed: bool


@dataclass
class DataProtoTransferDecision:
    shard_count: int
    byte_balanced: bool
    target_shard_bytes: int
    recommended_max_inflight_get: int
    profile: DataProtoTransferProfile


@dataclass
class DataProtoTransferAdaptiveState:
    pressure_level: str = "normal"
    last_max_inflight_get: Optional[int] = None
    ewma_parallel_efficiency: float = 0.0
    ewma_pool_wait_count: float = 0.0
    ewma_direct_range_fallback_count: float = 0.0
    update_count: int = 0


@dataclass
class DataProtoMaterializePolicy:
    max_inflight_get: Optional[int] = None
    pool_prewarm: bool = True
    pool_prewarm_size: Optional[int] = None
    pool_prewarm_count: Optional[int] = None
    adaptive_feedback: bool = True


@dataclass
class PayloadParts:
    parts: List[Any]
    kind: str
    dtype: Optional[str]
    shape: List[int]
    chunk_offsets: Optional[List[int]] = None


def chunk_offsets_by_size(sizes: List[int], target_bytes: int) -> List[int]:
    offsets = [0]
    chunk_bytes = 0
    for index, size in enumerate(sizes):
        if index > offsets[-1] and chunk_bytes + size > target_bytes:
            offsets.append(index)
            chunk_bytes = 0
        chunk_bytes += size
    offsets.append(len(sizes))
    return offsets


class KeyValueStore(Protocol):
    def put(self, key: str, value: Any) -> int:
        ...

    def get(self, key: str) -> bytes:
        ...

    def remove(self, key: str, force: bool = False) -> int:
        ...

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        ...

    def unregister_buffer(self, buffer_ptr: int) -> int:
        ...

    def put_from(self, key: str, buffer_ptr: int, size: int, config: Any = None) -> int:
        ...

    def get_into(self, key: str, buffer_ptr: int, size: int) -> int:
        ...

    def batch_put_from(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int], config: Any = None) -> List[int]:
        ...

    def batch_get_into(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int]) -> List[int]:
        ...


def create_mooncake_store_from_env() -> Any:
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    setup_start = now()
    rc = store.setup(
        os.environ.get("LOCAL_HOSTNAME", "localhost"),
        os.environ.get("MC_METADATA_SERVER", "127.0.0.1:2379"),
        int(os.environ.get("MOONCAKE_GLOBAL_SEGMENT_SIZE", str(3200 * 1024 * 1024))),
        int(os.environ.get("MOONCAKE_LOCAL_BUFFER_SIZE", str(512 * 1024 * 1024))),
        os.environ.get("PROTOCOL", "tcp"),
        os.environ.get("DEVICE_NAME", "ibp6s0"),
        os.environ.get("MASTER_SERVER", "127.0.0.1:50051"),
    )
    setup_s = now() - setup_start
    _STORE_PROFILES[id(store)] = {
        "register_count": 0,
        "unregister_count": 0,
        "register_s": 0.0,
        "unregister_s": 0.0,
        "registered_bytes": 0,
        "setup_s": setup_s,
        "setup_segment_bytes": int(os.environ.get("MOONCAKE_LOCAL_BUFFER_SIZE", str(512 * 1024 * 1024))),
    }
    if rc != 0:
        raise RuntimeError(f"failed to setup MooncakeDistributedStore: {rc}")
    return store


class InMemoryMooncakeStore:
    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}

    def put(self, key: str, value: Any) -> int:
        self.objects[key] = bytes(value)
        return 0

    def get(self, key: str) -> bytes:
        return self.objects[key]

    def remove(self, key: str, force: bool = False) -> int:
        self.objects.pop(key, None)
        return 0

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        return 0

    def unregister_buffer(self, buffer_ptr: int) -> int:
        return 0

    def put_from(self, key: str, buffer_ptr: int, size: int, config: Any = None) -> int:
        self.objects[key] = ctypes.string_at(buffer_ptr, size)
        return 0

    def get_into(self, key: str, buffer_ptr: int, size: int) -> int:
        data = self.objects[key]
        if len(data) > size:
            return -1
        ctypes.memmove(buffer_ptr, data, len(data))
        return len(data)

    def batch_put_from(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int], config: Any = None) -> List[int]:
        return [self.put_from(key, buffer_ptr, size, config) for key, buffer_ptr, size in zip(keys, buffer_ptrs, sizes)]

    def batch_get_into(self, keys: List[str], buffer_ptrs: List[int], sizes: List[int]) -> List[int]:
        return [self.get_into(key, buffer_ptr, size) for key, buffer_ptr, size in zip(keys, buffer_ptrs, sizes)]

    def get_into_ranges(self, buffers, all_keys, all_dst_offsets, all_src_offsets, all_sizes):
        results = []
        for ptr, keys, key_dst_offsets, key_src_offsets, key_sizes in zip(
            buffers,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        ):
            buffer_results = []
            for key, dst_offsets, src_offsets, sizes in zip(keys, key_dst_offsets, key_src_offsets, key_sizes):
                data = self.objects[key]
                key_results = []
                for dst_offset, src_offset, size in zip(dst_offsets, src_offsets, sizes):
                    chunk = data[src_offset : src_offset + size]
                    ctypes.memmove(ptr + dst_offset, chunk, len(chunk))
                    key_results.append(len(chunk) if len(chunk) == size else -1)
                buffer_results.append(key_results)
            results.append(buffer_results)
        return results


def now() -> float:
    return time.perf_counter()


def tensor_nbytes(tensor: torch.Tensor) -> int:
    return tensor.numel() * tensor.element_size()


def numpy_nbytes(array: np.ndarray) -> int:
    return int(array.nbytes)


def all_not_none(values: List[Any]) -> List[Any]:
    return [value for value in values if value is not None]


def accept(codec: str, normalized_type: str, reason: str, metadata: Optional[Dict[str, Any]] = None) -> CodecDecision:
    return CodecDecision(True, codec, reason, "exact", normalized_type, metadata or {})


def reject(codec: str, normalized_type: str, reason: str) -> CodecDecision:
    return CodecDecision(False, codec, reason, "exact", normalized_type)


def is_pil_image(value: Any) -> bool:
    return value.__class__.__module__.startswith("PIL.") and hasattr(value, "save")


def is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (bool, int, float, str, bytes, bytearray, memoryview))


def can_tensor(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("ragged_tensor_v1", "torch.Tensor", "all rows are null")
    if not all(isinstance(value, torch.Tensor) for value in non_null):
        return reject("ragged_tensor_v1", "torch.Tensor", "at least one non-null row is not torch.Tensor")
    dtypes = sorted({str(value.dtype) for value in non_null})
    if len(dtypes) != 1:
        return reject("ragged_tensor_v1", "torch.Tensor", f"mixed tensor dtypes: {dtypes}")
    return accept(
        "ragged_tensor_v1",
        "torch.Tensor",
        "all non-null rows are torch.Tensor with one dtype",
        {"source_type": "torch.Tensor", "dtype": dtypes[0]},
    )


def is_bytes_like(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview))


def is_media_list(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and all(is_pil_image(item) or is_bytes_like(item) for item in value)


def can_media_list(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("media_list_ragged_v1", "media list", "all rows are null")
    if not all(is_media_list(value) for value in non_null):
        return reject("media_list_ragged_v1", "media list", "non-null row is not a supported media list")
    return accept(
        "media_list_ragged_v1",
        "media list",
        "all non-null rows are lists of supported media objects",
        {"source_types": sorted({type(item).__name__ for value in non_null for item in value})},
    )


def can_numeric_sequence(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("typed_ragged_v1", "numeric sequence", "all rows are null")
    arrays = []
    for value in non_null:
        if isinstance(value, np.ndarray):
            array = value
        elif isinstance(value, (list, tuple)):
            array = np.asarray(value)
        else:
            return reject("typed_ragged_v1", "numeric sequence", "non-null row is not ndarray/list/tuple")
        if array.dtype == object:
            return reject("typed_ragged_v1", "numeric sequence", "row converts to object dtype")
        if not np.issubdtype(array.dtype, np.number):
            return reject("typed_ragged_v1", "numeric sequence", f"non-numeric dtype observed: {array.dtype}")
        arrays.append(array)
    dtype = np.result_type(*arrays)
    return accept(
        "typed_ragged_v1",
        "numeric sequence",
        "all non-null rows promote to a common numeric dtype",
        {"source_dtypes": sorted({str(array.dtype) for array in arrays}), "dtype": str(dtype)},
    )


def can_bytes(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("bytes_ragged_v1", "bytes-like", "all rows are null")
    if not all(is_bytes_like(value) for value in non_null):
        return reject("bytes_ragged_v1", "bytes-like", "non-null row is not bytes-like")
    return accept("bytes_ragged_v1", "bytes-like", "all non-null rows are bytes-like")


def can_media(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("media_bytes_ragged_v1", "media", "all rows are null")
    if not all(is_pil_image(value) for value in non_null):
        return reject("media_bytes_ragged_v1", "media", "non-null row is not a supported media object")
    return accept(
        "media_bytes_ragged_v1",
        "media",
        "all non-null rows implement supported media encoding",
        {"source_types": sorted({type(value).__name__ for value in non_null})},
    )


def can_text(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("utf8_ragged_v1", "str", "all rows are null")
    if not all(isinstance(value, str) for value in non_null):
        return reject("utf8_ragged_v1", "str", "non-null row is not str")
    return accept("utf8_ragged_v1", "str", "all non-null rows are str", {"encoding": "utf-8"})


def can_json(values: List[Any]) -> CodecDecision:
    non_null = all_not_none(values)
    if not non_null:
        return reject("json_ragged_v1", "json", "all rows are null")
    source_types = set()
    total_sample_bytes = 0
    for value in non_null[:MAX_SAMPLE_ROWS_FOR_INFERENCE]:
        try:
            encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            return reject("json_ragged_v1", "json", f"JSON serialization failed: {exc}")
        total_sample_bytes += len(encoded)
        source_types.add(type(value).__name__)
    if total_sample_bytes > MAX_JSON_INLINE_BYTES:
        return reject("json_ragged_v1", "json", "sampled JSON payload is too large")
    return accept("json_ragged_v1", "json", "sampled rows pass actual JSON serialization", {"source_types": sorted(source_types)})


def choose_leaf_codec(values: List[Any]) -> CodecDecision:
    rejected = []
    for predicate in (can_tensor, can_media_list, can_numeric_sequence, can_bytes, can_media, can_text, can_json):
        decision = predicate(values)
        if decision.accepted:
            return decision
        rejected.append({"codec": decision.codec, "reason": decision.reason})
    return CodecDecision(
        False,
        "pickle_ragged_fallback_v1",
        "no optimized codec accepted the observed values",
        "fallback",
        "python object",
        {"rejected_codecs": rejected, "sample_types": sorted({type(value).__name__ for value in all_not_none(values)})},
    )


def value_to_media_bytes(value: Any) -> Tuple[bytes, Optional[str], Dict[str, Any]]:
    if value is None:
        return b"", None, {"kind": "null"}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value), None, {"kind": "bytes"}
    if is_pil_image(value):
        image = value.convert(value.mode) if getattr(value, "readonly", False) else value
        return image.tobytes(), "image/raw", {
            "kind": "pil_raw",
            "mode": image.mode,
            "size": list(image.size),
            "format": getattr(value, "format", None),
        }
    return bytes(value), None, {"kind": "bytes"}


def encode_ragged_tensor(path: str, values: List[Any]) -> EncodedLeaf:
    start = now()
    tensors = []
    dtype = None
    max_ndim = 0
    for value in values:
        if value is None:
            tensors.append(None)
            continue
        tensor = value.detach().cpu().contiguous()
        dtype = tensor.dtype if dtype is None else dtype
        if tensor.dtype != dtype:
            raise ValueError(f"mixed tensor dtype for {path}: {dtype} vs {tensor.dtype}")
        max_ndim = max(max_ndim, tensor.dim())
        tensors.append(tensor)

    offsets = torch.zeros(len(tensors) + 1, dtype=torch.int64)
    ndims = torch.zeros(len(tensors), dtype=torch.int16)
    shapes = torch.zeros((len(tensors), max(max_ndim, 1)), dtype=torch.int64)
    nulls = torch.zeros(len(tensors), dtype=torch.bool)
    flats = []
    offset = 0
    for row, tensor in enumerate(tensors):
        if tensor is None:
            nulls[row] = True
            offsets[row + 1] = offset
            continue
        flat = tensor.reshape(-1)
        flats.append(flat)
        offset += flat.numel()
        offsets[row + 1] = offset
        ndims[row] = tensor.dim()
        if tensor.dim() > 0:
            shapes[row, : tensor.dim()] = torch.tensor(list(tensor.shape), dtype=torch.int64)
    data_numel = int(offsets[-1].item())
    data_dtype = dtype or torch.float32
    element_size = torch.empty((), dtype=data_dtype).element_size()
    chunk_offsets = chunk_offsets_by_size([tensor_nbytes(flat) for flat in flats], RAGGED_TENSOR_PART_CHUNK_BYTES)
    data = PayloadParts(flats, "tensor", torch_dtype_name(data_dtype), [data_numel], chunk_offsets)
    data_bytes = data_numel * element_size
    return EncodedLeaf(
        path=path,
        codec="ragged_tensor_v1",
        rows=len(values),
        bytes=data_bytes + tensor_nbytes(offsets) + tensor_nbytes(shapes) + tensor_nbytes(ndims) + tensor_nbytes(nulls),
        payload={"data": data, "offsets": offsets, "shapes": shapes, "ndims": ndims, "nulls": nulls},
        metadata={"dtype": str(data_dtype), "max_ndim": int(max_ndim), "shape_policy": "ragged"},
        encode_s=now() - start,
    )


def decode_ragged_tensor(payload: Dict[str, Any], rows: int) -> List[Any]:
    data = payload["data"]
    offsets = payload["offsets"]
    shapes = payload["shapes"]
    ndims = payload["ndims"]
    nulls = payload["nulls"]
    out = []
    for row in range(rows):
        if bool(nulls[row].item()):
            out.append(None)
            continue
        begin = int(offsets[row].item())
        end = int(offsets[row + 1].item())
        ndim = int(ndims[row].item())
        shape = tuple(int(v) for v in shapes[row, :ndim].tolist())
        out.append(data[begin:end].reshape(shape))
    return out


def encode_typed_ragged(path: str, values: List[Any]) -> EncodedLeaf:
    start = now()
    source_arrays = [np.asarray(value) for value in values if value is not None]
    dtype = np.result_type(*source_arrays) if source_arrays else np.dtype(np.int64)
    arrays = []
    max_ndim = 0
    for value in values:
        array = np.asarray([], dtype=dtype) if value is None else np.asarray(value, dtype=dtype)
        arrays.append(np.ascontiguousarray(array))
        max_ndim = max(max_ndim, array.ndim)
    offsets = np.zeros(len(arrays) + 1, dtype=np.int64)
    ndims = np.zeros(len(arrays), dtype=np.int16)
    shapes = np.zeros((len(arrays), max(max_ndim, 1)), dtype=np.int64)
    nulls = np.zeros(len(arrays), dtype=np.bool_)
    flat_arrays = []
    offset = 0
    for row, array in enumerate(arrays):
        if values[row] is None:
            nulls[row] = True
        flat = array.reshape(-1)
        flat_arrays.append(flat)
        offset += flat.size
        offsets[row + 1] = offset
        ndims[row] = array.ndim
        if array.ndim > 0:
            shapes[row, : array.ndim] = array.shape
    data = np.concatenate(flat_arrays) if flat_arrays else np.asarray([], dtype=dtype or np.int64)
    return EncodedLeaf(
        path=path,
        codec="typed_ragged_v1",
        rows=len(values),
        bytes=numpy_nbytes(data) + numpy_nbytes(offsets) + numpy_nbytes(shapes) + numpy_nbytes(ndims) + numpy_nbytes(nulls),
        payload={"data": data, "offsets": offsets, "shapes": shapes, "ndims": ndims, "nulls": nulls},
        metadata={"dtype": str(data.dtype), "max_ndim": int(max_ndim), "shape_policy": "ragged"},
        encode_s=now() - start,
    )


def decode_typed_ragged(payload: Dict[str, Any], rows: int) -> List[Any]:
    data = payload["data"]
    offsets = payload["offsets"]
    shapes = payload["shapes"]
    ndims = payload["ndims"]
    nulls = payload["nulls"]
    out = []
    for row in range(rows):
        if bool(nulls[row]):
            out.append(None)
            continue
        begin = int(offsets[row])
        end = int(offsets[row + 1])
        ndim = int(ndims[row])
        shape = tuple(int(v) for v in shapes[row, :ndim].tolist())
        out.append(data[begin:end].reshape(shape).tolist())
    return out


def encode_bytes_like(path: str, values: List[Any], codec: str) -> EncodedLeaf:
    start = now()
    offsets = [0]
    parts = []
    media_types = []
    encodings = []
    for value in values:
        data, media_type, encoding = value_to_media_bytes(value)
        parts.append(data)
        media_types.append(media_type)
        encodings.append(encoding)
        offsets.append(offsets[-1] + len(data))
    payload_bytes = sum(len(part) for part in parts)
    payload = PayloadParts(
        parts,
        "bytes",
        None,
        [payload_bytes],
        chunk_offsets_by_size([len(part) for part in parts], RAGGED_BYTES_PART_CHUNK_BYTES),
    )
    offsets_array = np.asarray(offsets, dtype=np.int64)
    nulls = np.asarray([value is None for value in values], dtype=np.bool_)
    metadata: Dict[str, Any] = {}
    if any(encoding.get("kind") == "pil_raw" for encoding in encodings):
        metadata["media_encodings"] = encodings
    non_null_media_types = sorted({media_type for media_type in media_types if media_type})
    if non_null_media_types:
        metadata["media_types"] = non_null_media_types
        metadata["encode_source"] = "raw_pixels_from_object"
    return EncodedLeaf(
        path=path,
        codec=codec,
        rows=len(values),
        bytes=payload_bytes + numpy_nbytes(offsets_array) + numpy_nbytes(nulls),
        payload={"data": payload, "offsets": offsets_array, "nulls": nulls},
        metadata=metadata,
        encode_s=now() - start,
    )


def encode_media_list(path: str, values: List[Any]) -> EncodedLeaf:
    start = now()
    row_offsets = [0]
    byte_offsets = [0]
    parts = []
    media_types = []
    encodings = []
    for value in values:
        items = [] if value is None else list(value)
        for item in items:
            data, media_type, encoding = value_to_media_bytes(item)
            parts.append(data)
            media_types.append(media_type)
            encodings.append(encoding)
            byte_offsets.append(byte_offsets[-1] + len(data))
        row_offsets.append(row_offsets[-1] + len(items))
    payload_bytes = sum(len(part) for part in parts)
    payload = PayloadParts(
        parts,
        "bytes",
        None,
        [payload_bytes],
        chunk_offsets_by_size([len(part) for part in parts], RAGGED_BYTES_PART_CHUNK_BYTES),
    )
    row_offsets_array = np.asarray(row_offsets, dtype=np.int64)
    byte_offsets_array = np.asarray(byte_offsets, dtype=np.int64)
    nulls = np.asarray([value is None for value in values], dtype=np.bool_)
    metadata: Dict[str, Any] = {"encode_source": "raw_pixels_from_object", "media_encodings": encodings}
    non_null_media_types = sorted({media_type for media_type in media_types if media_type})
    if non_null_media_types:
        metadata["media_types"] = non_null_media_types
    return EncodedLeaf(
        path=path,
        codec="media_list_ragged_v1",
        rows=len(values),
        bytes=payload_bytes + numpy_nbytes(row_offsets_array) + numpy_nbytes(byte_offsets_array) + numpy_nbytes(nulls),
        payload={"data": payload, "row_offsets": row_offsets_array, "byte_offsets": byte_offsets_array, "nulls": nulls},
        metadata=metadata,
        encode_s=now() - start,
    )


def decode_media_bytes(data: Any, encoding: Optional[Dict[str, Any]]) -> Any:
    if not encoding or encoding.get("kind") != "pil_raw":
        return bytes(data)
    try:
        from PIL import Image
    except ImportError:
        return bytes(data)
    image = Image.frombuffer(encoding["mode"], tuple(encoding["size"]), data, "raw", encoding["mode"], 0, 1)
    image.format = encoding.get("format")
    return image


def decode_media_list(payload: Dict[str, Any], rows: int, metadata: Optional[Dict[str, Any]] = None) -> List[Any]:
    data = payload["data"]
    row_offsets = payload["row_offsets"]
    byte_offsets = payload["byte_offsets"]
    nulls = payload["nulls"]
    encodings = (metadata or {}).get("media_encodings", [])
    out = []
    for row in range(rows):
        if bool(nulls[row]):
            out.append(None)
            continue
        items = []
        for item_index in range(int(row_offsets[row]), int(row_offsets[row + 1])):
            item = memoryview(data)[int(byte_offsets[item_index]) : int(byte_offsets[item_index + 1])]
            encoding = encodings[item_index] if item_index < len(encodings) else None
            items.append(decode_media_bytes(item, encoding))
        out.append(items)
    return out


def decode_media_list_fast(payload: Dict[str, Any], rows: int, metadata: Optional[Dict[str, Any]] = None) -> List[Any]:
    data = memoryview(payload["data"])
    row_offsets = payload["row_offsets"]
    byte_offsets = payload["byte_offsets"]
    nulls = payload["nulls"]
    encodings = (metadata or {}).get("media_encodings", [])
    media_types = (metadata or {}).get("media_types", [])
    media_type = media_types[0] if len(media_types) == 1 else None
    out = []
    for row in range(rows):
        if bool(nulls[row]):
            out.append(None)
            continue
        items = []
        for item_index in range(int(row_offsets[row]), int(row_offsets[row + 1])):
            begin = int(byte_offsets[item_index])
            end = int(byte_offsets[item_index + 1])
            items.append(
                {
                    "data": data[begin:end],
                    "media_type": media_type,
                    "encoding": encodings[item_index] if item_index < len(encodings) else None,
                }
            )
        out.append(items)
    return out


def decode_bytes_like(payload: Dict[str, Any], rows: int, metadata: Optional[Dict[str, Any]] = None) -> List[Any]:
    data = payload["data"]
    offsets = payload["offsets"]
    nulls = payload["nulls"]
    encodings = (metadata or {}).get("media_encodings", [])
    out = []
    for row in range(rows):
        if bool(nulls[row]):
            out.append(None)
            continue
        item = memoryview(data)[int(offsets[row]) : int(offsets[row + 1])]
        encoding = encodings[row] if row < len(encodings) else None
        out.append(decode_media_bytes(item, encoding))
    return out


def encode_utf8(path: str, values: List[Any]) -> EncodedLeaf:
    encoded = [None if value is None else value.encode("utf-8") for value in values]
    return encode_bytes_like(path, encoded, "utf8_ragged_v1")


def decode_utf8(payload: Dict[str, Any], rows: int) -> List[Any]:
    return [None if value is None else value.decode("utf-8") for value in decode_bytes_like(payload, rows)]


def encode_json(path: str, values: List[Any]) -> EncodedLeaf:
    encoded = [None if value is None else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") for value in values]
    return encode_bytes_like(path, encoded, "json_ragged_v1")


def decode_json(payload: Dict[str, Any], rows: int) -> List[Any]:
    return [None if value is None else json.loads(value.decode("utf-8")) for value in decode_bytes_like(payload, rows)]


def encode_pickle(path: str, values: List[Any]) -> EncodedLeaf:
    encoded = [None if value is None else pickle.dumps(value, protocol=5) for value in values]
    return encode_bytes_like(path, encoded, "pickle_ragged_fallback_v1")


def decode_pickle(payload: Dict[str, Any], rows: int) -> List[Any]:
    return [None if value is None else pickle.loads(value) for value in decode_bytes_like(payload, rows)]


def encode_leaf(path: str, values: List[Any], decision: CodecDecision) -> EncodedLeaf:
    if decision.codec == "ragged_tensor_v1":
        leaf = encode_ragged_tensor(path, values)
    elif decision.codec == "typed_ragged_v1":
        leaf = encode_typed_ragged(path, values)
    elif decision.codec == "media_list_ragged_v1":
        leaf = encode_media_list(path, values)
    elif decision.codec == "bytes_ragged_v1":
        leaf = encode_bytes_like(path, values, "bytes_ragged_v1")
    elif decision.codec == "media_bytes_ragged_v1":
        leaf = encode_bytes_like(path, values, "media_bytes_ragged_v1")
    elif decision.codec == "utf8_ragged_v1":
        leaf = encode_utf8(path, values)
    elif decision.codec == "json_ragged_v1":
        leaf = encode_json(path, values)
    else:
        leaf = encode_pickle(path, values)
    leaf.metadata.update(
        {
            "schema_source": "inferred_from_runtime_values" if decision.accepted else "fallback",
            "decision_reason": decision.reason,
            "decision_confidence": decision.confidence,
            "normalized_type": decision.normalized_type,
            "fallback": decision.codec == "pickle_ragged_fallback_v1" and not decision.accepted,
            **decision.metadata,
        }
    )
    return leaf


def decode_leaf(
    codec: str, payload: Dict[str, Any], rows: int, metadata: Optional[Dict[str, Any]] = None, fast_media: bool = False
) -> List[Any]:
    if codec == "ragged_tensor_v1":
        return decode_ragged_tensor(payload, rows)
    if codec == "typed_ragged_v1":
        return decode_typed_ragged(payload, rows)
    if codec == "media_list_ragged_v1":
        if fast_media:
            return decode_media_list_fast(payload, rows, metadata)
        return decode_media_list(payload, rows, metadata)
    if codec == "media_bytes_ragged_v1":
        return decode_bytes_like(payload, rows, metadata)
    if codec == "bytes_ragged_v1":
        return decode_bytes_like(payload, rows)
    if codec == "utf8_ragged_v1":
        return decode_utf8(payload, rows)
    if codec == "json_ragged_v1":
        return decode_json(payload, rows)
    if codec == "pickle_ragged_fallback_v1":
        return decode_pickle(payload, rows)
    raise ValueError(f"unknown codec: {codec}")


def can_expand_dict(values: List[Any]) -> bool:
    non_null = all_not_none(values)
    if not non_null or not all(isinstance(value, dict) for value in non_null):
        return False
    keys = {key for value in non_null for key in value.keys()}
    return all(isinstance(key, str) for key in keys) and len(keys) <= MAX_STRUCT_KEYS


def can_expand_list(values: List[Any]) -> bool:
    non_null = all_not_none(values)
    if not non_null or not all(isinstance(value, (list, tuple)) for value in non_null):
        return False
    max_len = max(len(value) for value in non_null)
    if max_len > MAX_EXPAND_LIST_LEN:
        return False
    for value in non_null:
        for item in value:
            if not isinstance(item, (dict, list, tuple)):
                return False
    return True


def infer_and_encode(path: str, values: List[Any], leaves: List[EncodedLeaf], nodes: List[Dict[str, Any]]) -> None:
    decision = choose_leaf_codec(values)
    if decision.accepted:
        leaves.append(encode_leaf(path, values, decision))
        return
    if can_expand_dict(values):
        keys = sorted({key for value in all_not_none(values) for key in value.keys()})
        nodes.append({"path": path, "node_type": "dict", "children": keys})
        for key in keys:
            child_values = [value.get(key) if isinstance(value, dict) else None for value in values]
            infer_and_encode(f"{path}.{key}", child_values, leaves, nodes)
        return
    if can_expand_list(values):
        max_len = max((len(value) for value in all_not_none(values)), default=0)
        lengths = [len(value) if isinstance(value, (list, tuple)) else 0 for value in values]
        nodes.append({"path": path, "node_type": "list", "lengths": lengths, "children": list(range(max_len))})
        for index in range(max_len):
            child_values = [value[index] if isinstance(value, (list, tuple)) and index < len(value) else None for value in values]
            infer_and_encode(f"{path}[{index}]", child_values, leaves, nodes)
        return
    leaves.append(encode_leaf(path, values, decision))


def path_field(path: str) -> str:
    return path.removeprefix("non_tensor_batch.").split(".", 1)[0].split("[", 1)[0]


def torch_dtype_name(dtype: torch.dtype) -> str:
    return str(dtype).removeprefix("torch.")


def torch_dtype_from_name(name: str) -> torch.dtype:
    return getattr(torch, name.removeprefix("torch."))


def tensor_to_numpy(tensor: torch.Tensor) -> np.ndarray:
    return tensor.detach().cpu().contiguous().numpy()


def numpy_as_bytes(array: np.ndarray) -> memoryview:
    return memoryview(np.ascontiguousarray(array)).cast("B")


def create_local_pin_config(store: Any) -> Any:
    try:
        from mooncake.store import ReplicateConfig
    except ImportError:
        return None
    if not hasattr(store, "get_hostname"):
        return None
    config = ReplicateConfig()
    config.preferred_segments = [store.get_hostname()]
    config.with_hard_pin = True
    return config


def payload_to_buffer(value: Any) -> Tuple[memoryview, Any, Dict[str, Any]]:
    if isinstance(value, torch.Tensor):
        array = tensor_to_numpy(value)
        return numpy_as_bytes(array), array, {"kind": "tensor", "dtype": torch_dtype_name(value.dtype), "shape": list(value.shape)}
    if isinstance(value, np.ndarray):
        array = np.ascontiguousarray(value)
        return numpy_as_bytes(array), array, {"kind": "ndarray", "dtype": str(array.dtype), "shape": list(array.shape)}
    if isinstance(value, bytes):
        buffer = bytearray(value)
        return memoryview(buffer), buffer, {"kind": "bytes"}
    encoded = bytearray(pickle.dumps(value, protocol=5))
    return memoryview(encoded), encoded, {"kind": "pickle"}


def payload_parts_to_buffers(value: PayloadParts) -> Tuple[List[List[memoryview]], List[Any], Dict[str, Any]]:
    owners = []
    buffers = []
    for part in value.parts:
        if isinstance(part, torch.Tensor):
            array = tensor_to_numpy(part)
            owners.append(array)
            buffers.append(numpy_as_bytes(array))
        elif isinstance(part, np.ndarray):
            array = np.ascontiguousarray(part)
            owners.append(array)
            buffers.append(numpy_as_bytes(array))
        else:
            buffer, owner, _ = payload_to_buffer(part)
            owners.append(owner)
            buffers.append(buffer)
    chunk_offsets = value.chunk_offsets or [0, len(buffers)]
    chunks = [buffers[chunk_offsets[index] : chunk_offsets[index + 1]] for index in range(len(chunk_offsets) - 1)]
    chunk_bytes = [sum(len(buffer) for buffer in chunk) for chunk in chunks]
    spec = {
        "kind": value.kind,
        "dtype": value.dtype,
        "shape": value.shape,
        "bytes": sum(chunk_bytes),
        "chunks": [{"bytes": size} for size in chunk_bytes],
    }
    return chunks, owners, spec


def put_parts_buffer(store: KeyValueStore, key: str, buffers: List[memoryview]) -> None:
    config = create_local_pin_config(store)
    if hasattr(store, "put_parts"):
        rc = store.put_parts(key, *buffers, config=config)
        if rc != 0:
            cleanup_written_keys(store, [key])
            raise RuntimeError(f"put_parts failed for {key}: {rc}")
        return
    joined = bytearray(sum(len(buffer) for buffer in buffers))
    offset = 0
    for buffer in buffers:
        joined[offset : offset + len(buffer)] = buffer
        offset += len(buffer)
    put_buffers(store, [(key, memoryview(joined), joined)])


def put_buffers(store: KeyValueStore, buffers: List[Tuple[str, memoryview, Any]]) -> List[Dict[str, Any]]:
    keys = [key for key, _, _ in buffers]
    values = [buffer for _, buffer, _ in buffers]
    sizes = [len(buffer) for buffer in values]
    written = []
    if keys:
        config = create_local_pin_config(store)
        ptrs = [ctypes.addressof(ctypes.c_char.from_buffer(buffer)) if len(buffer) else 0 for buffer in values]
        results = store.batch_put_from(keys, ptrs, sizes, config)
        if len(results) != len(keys):
            cleanup_written_keys(store, keys)
            raise RuntimeError(f"batch_put_from returned {len(results)} results for {len(keys)} buffers")
        for key, rc in zip(keys, results):
            if rc != 0:
                cleanup_written_keys(store, keys)
                raise RuntimeError(f"batch_put_from failed for {key}: {rc}")
            written.append(key)
    return [{"key": key, "bytes": size} for key, size in zip(keys, sizes)]


def cleanup_written_keys(store: KeyValueStore, keys: Iterable[str]) -> None:
    errors = []
    for key in keys:
        try:
            rc = store.remove(key, True)
            if rc not in (None, 0):
                errors.append((key, rc))
        except KeyError:
            continue
    if errors:
        raise RuntimeError(f"failed to remove {len(errors)} Mooncake keys: {errors[:3]}")


def ensure_store_profile(store: Any) -> Dict[str, Any]:
    with _STORE_STATE_LOCK:
        return _STORE_PROFILES.setdefault(
            id(store),
            {
                "register_count": 0,
                "unregister_count": 0,
                "register_s": 0.0,
                "unregister_s": 0.0,
                "registered_bytes": 0,
                "setup_s": None,
                "setup_segment_bytes": None,
            },
        )


def profile_counter(store: Any, name: str, value: float = 1.0) -> None:
    with _STORE_STATE_LOCK:
        ensure_store_profile(store)[name] += value


def get_register_profile(store: Any) -> Dict[str, Any]:
    with _STORE_STATE_LOCK:
        profile = dict(ensure_store_profile(store))
        pool = _REGISTERED_BUFFER_POOLS.get(id(store))
    if pool is not None:
        profile["registered_buffer_pool"] = pool.stats().__dict__
    return profile


def get_registered_buffer_pool(store: KeyValueStore) -> Any:
    if RegisteredBufferPool is None or RegisteredBufferPoolConfig is None:
        raise ImportError("mooncake.remote_tensor_batch.RegisteredBufferPool is required")
    with _STORE_STATE_LOCK:
        pool = _REGISTERED_BUFFER_POOLS.get(id(store))
        if pool is not None:
            return pool
        pool = RegisteredBufferPool.from_config(
            store,
            RegisteredBufferPoolConfig(
                max_bytes=REGISTERED_BUFFER_POOL_BYTES,
                max_size_class=REGISTERED_BUFFER_POOL_MAX_BUFFER_BYTES,
                prewarm_size=REGISTERED_BUFFER_POOL_PREWARM_BYTES or None,
                prewarm_count=REGISTERED_BUFFER_POOL_PREWARM_COUNT,
            ),
        )
        _REGISTERED_BUFFER_POOLS[id(store)] = pool
        return pool


def close_registered_buffer_pool(store: KeyValueStore) -> None:
    with _STORE_STATE_LOCK:
        pool = _REGISTERED_BUFFER_POOLS.pop(id(store), None)
    if pool is None:
        return
    pool.close()


def register_pointer(store: KeyValueStore, key: str, ptr: int, size: int) -> None:
    if size == 0:
        return
    register_start = now()
    rc = store.register_buffer(ptr, size)
    profile_counter(store, "register_s", now() - register_start)
    profile_counter(store, "register_count")
    profile_counter(store, "registered_bytes", size)
    if rc != 0:
        raise RuntimeError(f"register_buffer failed for {key}: {rc}")


def unregister_pointer(store: KeyValueStore, ptr: int) -> None:
    unregister_start = now()
    rc = store.unregister_buffer(ptr)
    profile_counter(store, "unregister_s", now() - unregister_start)
    profile_counter(store, "unregister_count")
    if rc != 0:
        raise RuntimeError(f"unregister_buffer failed: {rc}")


def new_batch_read_planner(store: KeyValueStore) -> Any:
    if BatchReadPlanner is None:
        raise ImportError("mooncake.remote_tensor_batch.BatchReadPlanner is required")
    return BatchReadPlanner(store)


def new_range_read_planner(store: KeyValueStore, ptr: int, capacity: int) -> Any:
    if RangeReadPlanner is None:
        raise ImportError("mooncake.remote_tensor_batch.RangeReadPlanner is required")
    return RangeReadPlanner(store, ptr, capacity)


def batch_read_into_pointers(store: KeyValueStore, keys: List[str], ptrs: List[int], sizes: List[int]) -> None:
    if not (len(keys) == len(ptrs) == len(sizes)):
        raise ValueError("keys, ptrs, and sizes must have the same length")
    planner = new_batch_read_planner(store)
    for key, ptr, size in zip(keys, ptrs, sizes):
        planner.add(key, ptr, size)
    planner.execute()


def prepare_tensor_payloads(
    store: KeyValueStore,
    entries: List[Dict[str, Any]],
    planner: Any,
    registered_ptrs: List[int],
) -> Dict[str, torch.Tensor]:
    tensors = {}
    try:
        for entry in entries:
            dtype = torch_dtype_from_name(entry["payload"]["dtype"])
            tensor = torch.empty(entry["shape"], dtype=dtype)
            tensors[entry["path"].removeprefix("batch.")] = tensor
            size = tensor_nbytes(tensor)
            register_pointer(store, entry["key"], tensor.data_ptr(), size)
            registered_ptrs.append(tensor.data_ptr())
            planner.add(entry["key"], tensor.data_ptr(), size, registered=True)
        return tensors
    except BaseException:
        while registered_ptrs:
            unregister_pointer(store, registered_ptrs.pop())
        raise


def prepare_whole_key_payload_reads(
    store: KeyValueStore,
    entries: List[Dict[str, Any]],
    planner: Any,
) -> Dict[str, Dict[str, Any]]:
    prepared = {}
    for entry in entries:
        entry_payload = {}
        for name, spec in entry["payload"].items():
            kind = spec["kind"]
            if kind == "tensor" and "chunks" in spec and spec["chunks"] and "key" in spec["chunks"][0]:
                continue
            if "chunks" in spec and spec["chunks"] and "key" in spec["chunks"][0]:
                chunks = spec["chunks"]
                data = bytearray(int(spec["bytes"]))
                offset = 0
                buffers = []
                for chunk in chunks:
                    chunk_buffer = bytearray(int(chunk["bytes"]))
                    buffers.append(chunk_buffer)
                    chunk_ptr = ctypes.addressof(ctypes.c_char.from_buffer(chunk_buffer))
                    planner.add(chunk["key"], chunk_ptr, len(chunk_buffer))
                entry_payload[name] = (kind, data, buffers)
                continue
            if kind == "tensor":
                dtype = torch_dtype_from_name(spec["dtype"])
                tensor = torch.empty(spec["shape"], dtype=dtype)
                planner.add(spec["key"], tensor.data_ptr(), tensor_nbytes(tensor))
                entry_payload[name] = tensor
                continue
            if kind == "ndarray":
                array = np.empty(spec["shape"], dtype=np.dtype(spec["dtype"]))
                planner.add(spec["key"], int(array.ctypes.data), numpy_nbytes(array))
                entry_payload[name] = array
                continue
            if kind in ("bytes", "pickle"):
                data = bytearray(int(spec["bytes"]))
                if data:
                    data_ptr = ctypes.addressof(ctypes.c_char.from_buffer(data))
                    planner.add(spec["key"], data_ptr, len(data))
                entry_payload[name] = data
        if entry_payload:
            prepared[entry["path"]] = entry_payload
    return prepared


def validate_tensor_chunks(chunks: List[Dict[str, Any]], tensor: torch.Tensor) -> List[int]:
    element_size = tensor.element_size()
    tensor_bytes = tensor_nbytes(tensor)
    chunk_sizes = [int(chunk["bytes"]) for chunk in chunks]
    if any(chunk_size < 0 for chunk_size in chunk_sizes):
        raise ValueError("tensor chunk sizes must be non-negative")
    if any(chunk_size % element_size != 0 for chunk_size in chunk_sizes):
        raise ValueError("tensor chunk sizes must be divisible by tensor element size")
    total_chunk_bytes = sum(chunk_sizes)
    if total_chunk_bytes != tensor_bytes:
        raise ValueError(f"tensor chunks total {total_chunk_bytes} bytes, expected {tensor_bytes}")
    return chunk_sizes


def read_chunked_tensor_direct(
    store: KeyValueStore,
    pool: Any,
    chunks: List[Dict[str, Any]],
    chunk_sizes: List[int],
    tensor: torch.Tensor,
    profile: Dict[str, Any],
) -> bool:
    tensor_bytes = tensor_nbytes(tensor)
    lease = pool.try_register_external(tensor.data_ptr(), tensor_bytes)
    if lease is None:
        return False
    try:
        range_planner = new_range_read_planner(store, tensor.data_ptr(), tensor_bytes)
        dst_offset = 0
        for chunk, chunk_bytes in zip(chunks, chunk_sizes):
            range_planner.add_whole_key(chunk["key"], dst_offset, chunk_bytes)
            dst_offset += chunk_bytes
        range_start = now()
        range_planner.execute()
        profile["direct_range_read_s"] = profile.get("direct_range_read_s", 0.0) + now() - range_start
        profile["direct_range_group_count"] = profile.get("direct_range_group_count", 0) + 1
        profile.setdefault("direct_range_group_sizes", []).append(len(chunks))
        return True
    finally:
        lease.release()


def deserialize_payload(
    store: KeyValueStore,
    entry: Dict[str, Any],
    registered_buffer_pool: Any = None,
    prepared_payload: Optional[Dict[str, Any]] = None,
    profile: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(prepared_payload or {})
    profile = profile if profile is not None else {}
    for name, spec in entry["payload"].items():
        kind = spec["kind"]
        if name in payload:
            prepared_value = payload[name]
            if isinstance(prepared_value, tuple):
                _prepared_kind, data, buffers = prepared_value
                offset = 0
                for buffer in buffers:
                    data[offset : offset + len(buffer)] = buffer
                    offset += len(buffer)
                prepared_value = data
            if kind == "pickle":
                payload[name] = pickle.loads(prepared_value)
            else:
                payload[name] = prepared_value
            continue
        if kind == "tensor" and "chunks" in spec and spec["chunks"] and "key" in spec["chunks"][0]:
            dtype = torch_dtype_from_name(spec["dtype"])
            tensor = torch.empty(spec["shape"], dtype=dtype)
            chunks = spec["chunks"]
            chunk_sizes = validate_tensor_chunks(chunks, tensor)
            pool = registered_buffer_pool or get_registered_buffer_pool(store)
            if read_chunked_tensor_direct(store, pool, chunks, chunk_sizes, tensor, profile):
                payload[name] = tensor
                continue
            profile["direct_range_fallback_count"] = profile.get("direct_range_fallback_count", 0) + 1
            offset_elements = 0
            tensor_flat = tensor.view(-1)
            range_group_count = 0
            range_read_s = 0.0
            scratch_copy_s = 0.0
            range_group_sizes = []
            for group in pool.iter_transfer_groups(chunk_sizes):
                batch_chunks = [chunks[index] for index in group]
                batch_bytes = sum(chunk_sizes[index] for index in group)
                if batch_bytes % tensor.element_size() != 0:
                    raise ValueError("range-read batch bytes must be divisible by tensor element size")
                with pool.buffer(batch_bytes) as lease:
                    range_planner = new_range_read_planner(store, lease.ptr, lease.size)
                    dst_offset = 0
                    for chunk, chunk_bytes in zip(batch_chunks, (chunk_sizes[index] for index in group)):
                        range_planner.add_whole_key(chunk["key"], dst_offset, chunk_bytes)
                        dst_offset += chunk_bytes
                    range_start = now()
                    range_planner.execute()
                    range_read_s += now() - range_start
                    range_group_count += 1
                    range_group_sizes.append(len(batch_chunks))
                    scratch = torch.frombuffer(lease.buffer, dtype=dtype, count=batch_bytes // tensor.element_size())
                    chunk_elements = batch_bytes // tensor.element_size()
                    copy_start = now()
                    tensor_flat.narrow(0, offset_elements, chunk_elements).copy_(scratch.narrow(0, 0, chunk_elements))
                    scratch_copy_s += now() - copy_start
                    offset_elements += chunk_elements
            if offset_elements != tensor.numel():
                raise RuntimeError(f"materialized {offset_elements} tensor elements, expected {tensor.numel()}")
            profile.setdefault("range_read_s", 0.0)
            profile.setdefault("scratch_copy_s", 0.0)
            profile.setdefault("range_group_count", 0)
            profile.setdefault("range_group_sizes", [])
            profile["range_read_s"] += range_read_s
            profile["scratch_copy_s"] += scratch_copy_s
            profile["range_group_count"] += range_group_count
            profile["range_group_sizes"].extend(range_group_sizes)
            payload[name] = tensor
            continue
        if "chunks" in spec and spec["chunks"] and "key" in spec["chunks"][0]:
            chunks = spec["chunks"]
            data = bytearray(int(spec["bytes"]))
            offset = 0
            chunk_buffers = [bytearray(int(chunk["bytes"])) for chunk in chunks]
            batch_read_into_pointers(
                store,
                [chunk["key"] for chunk in chunks],
                [ctypes.addressof(ctypes.c_char.from_buffer(chunk_buffer)) for chunk_buffer in chunk_buffers],
                [len(chunk_buffer) for chunk_buffer in chunk_buffers],
            )
            for chunk_buffer in chunk_buffers:
                data[offset : offset + len(chunk_buffer)] = chunk_buffer
                offset += len(chunk_buffer)
        elif kind in ("tensor", "ndarray"):
            raise RuntimeError(f"{kind} payload for {entry['path']}.{name} was not prepared")
        else:
            data = bytearray(int(spec["bytes"]))
            if data:
                planner = new_batch_read_planner(store)
                planner.add(spec["key"], ctypes.addressof(ctypes.c_char.from_buffer(data)), len(data))
                planner.execute()
        if kind == "bytes":
            payload[name] = data
        elif kind == "pickle":
            payload[name] = pickle.loads(data)
        else:
            raise ValueError(f"unknown payload kind: {kind}")
    return payload


class MooncakeDataProtoTransferBackend:
    def __init__(self, store: KeyValueStore, key_prefix: str = "dataproto", dataproto_cls: Optional[Any] = None) -> None:
        self.store = store
        self.key_prefix = key_prefix.rstrip("/")
        self.dataproto_cls = dataproto_cls

    def put_dataproto(
        self,
        data: Any,
        partition: str = "default",
        shard_policy: Optional[DataProtoShardPolicy] = None,
    ) -> RemoteDataProtoRef | RemoteDataProtoShardedRef:
        if shard_policy is not None and self._should_shard(data, shard_policy):
            return self.put_dataproto_sharded(data, partition=partition, shard_policy=shard_policy)
        return self._put_dataproto_single(data, partition=partition)

    def _resolve_dataproto_cls(self) -> Any:
        if self.dataproto_cls is not None:
            return self.dataproto_cls
        try:
            from roll.distributed.scheduler.protocol import DataProto
        except ImportError as exc:
            raise ImportError("dataproto_cls is required when ROLL DataProto is not installed") from exc
        return DataProto

    def _put_dataproto_single(self, data: Any, partition: str = "default") -> RemoteDataProtoRef:
        object_id = f"{partition}/{uuid.uuid4().hex}"
        base_key = f"{self.key_prefix}/{object_id}"
        written_keys: List[str] = []
        profile_start = now()
        row_count = len(data)
        leaves: List[EncodedLeaf] = []
        nodes: List[Dict[str, Any]] = []
        profile: Dict[str, Any] = {
            "batch_put_s": 0.0,
            "infer_encode_s": 0.0,
            "leaf_put_s": 0.0,
            "meta_put_s": 0.0,
            "manifest_put_s": 0.0,
            "batch": [],
            "leaves": [],
        }

        batch_entries = []
        batch_buffers = []
        if data.batch is not None:
            for name, tensor in data.batch.items():
                tensor_key = f"{base_key}/batch/{name}"
                buffer, owner, payload_spec = payload_to_buffer(tensor)
                batch_buffers.append((tensor_key, buffer, owner))
                batch_entries.append(
                    {
                        "path": f"batch.{name}",
                        "key": tensor_key,
                        "codec": "dense_tensor_v1",
                        "dtype": str(tensor.dtype),
                        "shape": list(tensor.shape),
                        "bytes": tensor_nbytes(tensor),
                        "payload": payload_spec,
                    }
                )
            batch_put_start = now()
            try:
                put_buffers(self.store, batch_buffers)
            except BaseException:
                self._remove_keys(written_keys)
                raise
            written_keys.extend(key for key, _buffer, _owner in batch_buffers)
            profile["batch_put_s"] = now() - batch_put_start
            profile["batch"] = [{"path": entry["path"], "bytes": entry["bytes"]} for entry in batch_entries]

        infer_start = now()
        for field, array in data.non_tensor_batch.items():
            if not isinstance(array, np.ndarray) or array.dtype != object:
                raise TypeError(f"non_tensor_batch.{field} must be np.ndarray(dtype=object)")
            infer_and_encode(f"non_tensor_batch.{field}", list(array), leaves, nodes)
        profile["infer_encode_s"] = now() - infer_start

        leaf_entries = []
        for leaf in leaves:
            payload_entries = {}
            payload_buffers = []
            part_payloads = []
            for payload_name, payload_value in leaf.payload.items():
                key = f"{base_key}/leaf/{len(leaf_entries)}/{payload_name}"
                if isinstance(payload_value, PayloadParts):
                    chunks, owners, payload_spec = payload_parts_to_buffers(payload_value)
                    part_payloads.append((key, chunks, owners))
                    if len(chunks) == 1:
                        payload_entries[payload_name] = {"key": key, **payload_spec}
                        payload_entries[payload_name].pop("chunks", None)
                    else:
                        chunk_entries = [
                            {"key": f"{key}/chunk/{index}", "bytes": chunk["bytes"]}
                            for index, chunk in enumerate(payload_spec["chunks"])
                        ]
                        payload_entries[payload_name] = {"key": key, **payload_spec}
                        payload_entries[payload_name]["chunks"] = chunk_entries
                    continue
                buffer, owner, payload_spec = payload_to_buffer(payload_value)
                payload_buffers.append((key, buffer, owner))
                payload_entries[payload_name] = {"key": key, "bytes": len(buffer), **payload_spec}
            leaf_put_start = now()
            try:
                for key, chunks, _owners in part_payloads:
                    for index, buffers in enumerate(chunks):
                        chunk_key = key if len(chunks) == 1 else f"{key}/chunk/{index}"
                        put_parts_buffer(self.store, chunk_key, buffers)
                        written_keys.append(chunk_key)
                put_buffers(self.store, payload_buffers)
            except BaseException:
                self._remove_keys(written_keys)
                raise
            written_keys.extend(key for key, _buffer, _owner in payload_buffers)
            leaf_put_s = now() - leaf_put_start
            profile["leaf_put_s"] += leaf_put_s
            profile["leaves"].append(
                {
                    "path": leaf.path,
                    "codec": leaf.codec,
                    "bytes": leaf.bytes,
                    "encode_s": leaf.encode_s,
                    "put_s": leaf_put_s,
                    "payloads": [
                        {"name": name, "bytes": spec["bytes"], "kind": spec["kind"]}
                        for name, spec in payload_entries.items()
                    ],
                }
            )
            leaf_entries.append(
                {
                    "path": leaf.path,
                    "codec": leaf.codec,
                    "rows": leaf.rows,
                    "bytes": leaf.bytes,
                    "metadata": leaf.metadata,
                    "payload": payload_entries,
                }
            )

        meta_start = now()
        meta_info = pickle.dumps(data.meta_info, protocol=5)
        meta_key = f"{base_key}/meta_info"
        rc = self.store.put(meta_key, meta_info)
        profile["meta_put_s"] = now() - meta_start
        if rc != 0:
            self._remove_keys(written_keys)
            raise RuntimeError(f"put failed for {meta_key}: {rc}")
        written_keys.append(meta_key)

        manifest = {
            "version": 1,
            "object_id": object_id,
            "row_count": row_count,
            "batch": batch_entries,
            "nodes": nodes,
            "leaves": leaf_entries,
            "meta_info_key": meta_key,
        }
        profile["total_s"] = now() - profile_start
        manifest["profile"] = profile
        manifest_key = f"{base_key}/manifest"
        manifest_start = now()
        encoded_manifest = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
        profile["manifest_put_s"] = now() - manifest_start
        encoded_manifest = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
        rc = self.store.put(manifest_key, encoded_manifest)
        if rc != 0:
            self._remove_keys(written_keys)
            raise RuntimeError(f"put failed for {manifest_key}: {rc}")
        return RemoteDataProtoRef(object_id=object_id, row_count=row_count, manifest_key=manifest_key, manifest=manifest)

    def put_dataproto_sharded(
        self,
        data: Any,
        partition: str = "default",
        shard_policy: Optional[DataProtoShardPolicy] = None,
    ) -> RemoteDataProtoShardedRef:
        policy = shard_policy or DataProtoShardPolicy(enabled=True)
        row_count = len(data)
        if row_count == 0:
            return self._put_dataproto_single(data, partition=partition)  # type: ignore[return-value]
        decision = self._select_transfer_decision(data, policy)
        shard_count = decision.shard_count
        if shard_count <= 1:
            return self._put_dataproto_single(data, partition=partition)  # type: ignore[return-value]

        object_id = f"{partition}/{uuid.uuid4().hex}"
        base_key = f"{self.key_prefix}/{object_id}"
        shard_ranges = self._select_shard_ranges(data, shard_count, decision.byte_balanced)
        shards: List[Dict[str, Any]] = []
        shard_refs: List[RemoteDataProtoRef] = []
        put_start = now()
        for shard_id, (row_start, row_end) in enumerate(shard_ranges):
            chunk_size = row_end - row_start
            shard_data = data.slice(row_start, row_end)
            try:
                shard_ref = self._put_dataproto_single(shard_data, partition=f"{object_id}/shard_{shard_id:04d}")
            except BaseException:
                for ref in reversed(shard_refs):
                    self.remove_dataproto(ref)
                raise
            shard_refs.append(shard_ref)
            shard_bytes = self._manifest_logical_bytes(shard_ref.manifest)
            shards.append(
                {
                    "shard_id": shard_id,
                    "row_start": row_start,
                    "row_count": chunk_size,
                    "logical_bytes": shard_bytes,
                    "object_id": shard_ref.object_id,
                    "manifest_key": shard_ref.manifest_key,
                    "manifest": shard_ref.manifest,
                }
            )

        manifest = {
            "version": 2,
            "layout": "row_sharded_v1",
            "object_id": object_id,
            "row_count": row_count,
            "shards": shards,
            "policy": self._policy_to_dict(policy, decision),
            "profile": {
                "put_s": now() - put_start,
                "shard_count": shard_count,
                "shard_ranges": shard_ranges,
                "rows_per_shard": [row_end - row_start for row_start, row_end in shard_ranges],
                "bytes_per_shard": [int(shard["logical_bytes"]) for shard in shards],
                "transfer_profile": self._transfer_profile_to_dict(decision.profile),
            },
        }
        manifest_key = f"{base_key}/manifest"
        rc = self.store.put(manifest_key, json.dumps(manifest, ensure_ascii=False).encode("utf-8"))
        if rc != 0:
            for ref in reversed(shard_refs):
                self.remove_dataproto(ref)
            raise RuntimeError(f"put failed for {manifest_key}: {rc}")
        return RemoteDataProtoShardedRef(
            object_id=object_id,
            row_count=row_count,
            manifest_key=manifest_key,
            manifest=manifest,
        )

    def materialize_dataproto(
        self,
        ref: RemoteDataProtoRef | RemoteDataProtoShardedRef,
        fast_media: bool = False,
        max_inflight_get: Optional[int] = None,
        materialize_policy: Optional[DataProtoMaterializePolicy] = None,
    ) -> Any:
        manifest = ref.manifest or json.loads(self.store.get(ref.manifest_key).decode("utf-8"))
        policy = materialize_policy or DataProtoMaterializePolicy(max_inflight_get=max_inflight_get)
        if manifest.get("layout") == "row_sharded_v1":
            return self._materialize_sharded_dataproto(ref, manifest, fast_media=fast_media, materialize_policy=policy)
        self._prepare_materialize_pool([manifest], policy)
        return self._materialize_single_dataproto(ref, manifest=manifest, fast_media=fast_media)

    def _materialize_single_dataproto(
        self,
        ref: RemoteDataProtoRef,
        manifest: Dict[str, Any],
        fast_media: bool = False,
    ) -> Any:
        dataproto_cls = self._resolve_dataproto_cls()
        materialize_profile = {
            "fast_media": fast_media,
            "manifest_s": 0.0,
            "batch_get_s": 0.0,
            "batch_planner_s": 0.0,
            "leaf_payload_get_s": 0.0,
            "range_read_s": 0.0,
            "scratch_copy_s": 0.0,
            "range_group_count": 0,
            "range_group_sizes": [],
            "direct_range_read_s": 0.0,
            "direct_range_group_count": 0,
            "direct_range_group_sizes": [],
            "direct_range_fallback_count": 0,
            "leaf_decode_s": 0.0,
            "reconstruct_s": 0.0,
            "meta_info_s": 0.0,
            "from_dict_s": 0.0,
            "leaves": [],
        }
        registered_buffer_pool = get_registered_buffer_pool(self.store)
        pool_stats_before = registered_buffer_pool.stats()
        planner = new_batch_read_planner(self.store)
        tensor_ptrs = []
        batch_start = now()
        tensors = prepare_tensor_payloads(self.store, manifest["batch"], planner, tensor_ptrs)
        cleanup_errors = []
        try:
            prepared_payloads = prepare_whole_key_payload_reads(self.store, manifest["leaves"], planner)
            planner_start = now()
            planner.execute()
            materialize_profile["batch_planner_s"] = now() - planner_start
        finally:
            for ptr in tensor_ptrs:
                try:
                    unregister_pointer(self.store, ptr)
                except RuntimeError as exc:
                    cleanup_errors.append(exc)
        if cleanup_errors:
            raise RuntimeError(f"failed to unregister {len(cleanup_errors)} dense tensor buffers") from cleanup_errors[0]
        materialize_profile["batch_get_s"] = now() - batch_start

        leaves_by_field: Dict[str, List[Tuple[Dict[str, Any], List[Any]]]] = {}
        for entry in manifest["leaves"]:
            payload_start = now()
            leaf_profile: Dict[str, Any] = {}
            payload = deserialize_payload(
                self.store,
                entry,
                registered_buffer_pool=registered_buffer_pool,
                prepared_payload=prepared_payloads.get(entry["path"]),
                profile=leaf_profile,
            )
            payload_s = now() - payload_start
            decode_start = now()
            values = decode_leaf(
                entry["codec"], payload, entry["rows"], entry.get("metadata"), fast_media=fast_media
            )
            decode_s = now() - decode_start
            materialize_profile["leaf_payload_get_s"] += payload_s
            materialize_profile["leaf_decode_s"] += decode_s
            materialize_profile["range_read_s"] += leaf_profile.get("range_read_s", 0.0)
            materialize_profile["scratch_copy_s"] += leaf_profile.get("scratch_copy_s", 0.0)
            materialize_profile["range_group_count"] += leaf_profile.get("range_group_count", 0)
            materialize_profile["range_group_sizes"].extend(leaf_profile.get("range_group_sizes", []))
            materialize_profile["direct_range_read_s"] += leaf_profile.get("direct_range_read_s", 0.0)
            materialize_profile["direct_range_group_count"] += leaf_profile.get("direct_range_group_count", 0)
            materialize_profile["direct_range_group_sizes"].extend(leaf_profile.get("direct_range_group_sizes", []))
            materialize_profile["direct_range_fallback_count"] += leaf_profile.get("direct_range_fallback_count", 0)
            materialize_profile["leaves"].append(
                {
                    "path": entry["path"],
                    "codec": entry["codec"],
                    "payload_get_s": payload_s,
                    "decode_s": decode_s,
                    "range_read_s": leaf_profile.get("range_read_s", 0.0),
                    "scratch_copy_s": leaf_profile.get("scratch_copy_s", 0.0),
                    "range_group_count": leaf_profile.get("range_group_count", 0),
                    "range_group_sizes": leaf_profile.get("range_group_sizes", []),
                    "direct_range_read_s": leaf_profile.get("direct_range_read_s", 0.0),
                    "direct_range_group_count": leaf_profile.get("direct_range_group_count", 0),
                    "direct_range_group_sizes": leaf_profile.get("direct_range_group_sizes", []),
                    "direct_range_fallback_count": leaf_profile.get("direct_range_fallback_count", 0),
                }
            )
            if entry["path"].startswith("non_tensor_batch."):
                leaves_by_field.setdefault(path_field(entry["path"]), []).append((entry, values))

        reconstruct_start = now()
        non_tensors = {}
        nodes_by_path = {node["path"]: node for node in manifest["nodes"]}
        for field, field_leaves in leaves_by_field.items():
            root_path = f"non_tensor_batch.{field}"
            if root_path not in nodes_by_path and len(field_leaves) == 1 and field_leaves[0][0]["path"] == root_path:
                values = field_leaves[0][1]
            else:
                values = self._reconstruct_struct(root_path, field_leaves, nodes_by_path, manifest["row_count"])
            array = np.empty(len(values), dtype=object)
            array[:] = values
            non_tensors[field] = array
        materialize_profile["reconstruct_s"] = now() - reconstruct_start

        meta_start = now()
        meta_info = pickle.loads(self.store.get(manifest["meta_info_key"]))
        materialize_profile["meta_info_s"] = now() - meta_start
        pool_stats_after = registered_buffer_pool.stats()
        materialize_profile["registered_buffer_pool"] = {
            "size_classes": pool_stats_after.size_classes,
            "free_regions": pool_stats_after.free_regions,
            "in_use_regions": pool_stats_after.in_use_regions,
            "total_regions": pool_stats_after.total_regions,
            "total_bytes": pool_stats_after.total_bytes,
            "free_bytes": pool_stats_after.free_bytes,
            "acquire_count_delta": pool_stats_after.acquire_count - pool_stats_before.acquire_count,
            "reuse_count_delta": pool_stats_after.reuse_count - pool_stats_before.reuse_count,
            "allocate_count_delta": pool_stats_after.allocate_count - pool_stats_before.allocate_count,
            "oversize_allocate_count_delta": (
                pool_stats_after.oversize_allocate_count - pool_stats_before.oversize_allocate_count
            ),
            "wait_count_delta": pool_stats_after.wait_count - pool_stats_before.wait_count,
            "register_s_total": pool_stats_after.register_s,
            "unregister_s_total": pool_stats_after.unregister_s,
        }
        meta_info["mooncake_materialize_profile"] = materialize_profile
        from_dict_start = now()
        if tensors:
            proto = dataproto_cls.from_dict(tensors=tensors, non_tensors=non_tensors, meta_info=meta_info)
            materialize_profile["from_dict_s"] = now() - from_dict_start
            return proto
        proto = dataproto_cls(batch=None, non_tensor_batch={}, meta_info=meta_info)
        proto.non_tensor_batch = non_tensors
        materialize_profile["from_dict_s"] = now() - from_dict_start
        return proto

    def _materialize_sharded_dataproto(
        self,
        ref: RemoteDataProtoRef | RemoteDataProtoShardedRef,
        manifest: Dict[str, Any],
        fast_media: bool = False,
        materialize_policy: Optional[DataProtoMaterializePolicy] = None,
    ) -> Any:
        dataproto_cls = self._resolve_dataproto_cls()
        shards = sorted(manifest["shards"], key=lambda shard: int(shard["row_start"]))
        if not shards:
            raise ValueError("sharded RemoteDataProtoRef has no shards")
        parent_policy = manifest.get("policy", {})
        policy = materialize_policy or DataProtoMaterializePolicy()
        max_workers = self._select_materialize_inflight(parent_policy, policy, len(shards))
        self._prepare_materialize_pool([shard.get("manifest", {}) for shard in shards], policy, max_workers)
        materialize_start = now()

        def materialize_shard(shard: Dict[str, Any]) -> Tuple[int, Any, float]:
            shard_ref = RemoteDataProtoRef(
                object_id=shard["object_id"],
                row_count=int(shard["row_count"]),
                manifest_key=shard["manifest_key"],
                manifest=shard.get("manifest", {}),
            )
            shard_start = now()
            proto = self._materialize_single_dataproto(shard_ref, shard_ref.manifest, fast_media)
            return int(shard["shard_id"]), proto, now() - shard_start

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(materialize_shard, shards))
        results.sort(key=lambda item: item[0])
        materialized = [item[1] for item in results]
        concat_start = now()
        proto = dataproto_cls.concat(materialized)
        concat_s = now() - concat_start
        total_s = now() - materialize_start
        shard_materialize_s = {str(shard_id): elapsed for shard_id, _proto, elapsed in results}
        shard_profiles = [item[1].meta_info.get("mooncake_materialize_profile", {}) for item in results]
        feedback = self._build_sharded_feedback(total_s, shard_materialize_s, shard_profiles, max_workers)
        self._update_adaptive_state(feedback, policy)
        materialize_profile = {
            "layout": "row_sharded_v1",
            "fast_media": fast_media,
            "shard_count": len(shards),
            "max_inflight_get": max_workers,
            "shard_materialize_s": shard_materialize_s,
            "concat_s": concat_s,
            "total_s": total_s,
            "parallel_feedback": feedback,
            "adaptive_state": self._adaptive_state_to_dict(),
            "parent_policy": parent_policy,
            "materialize_policy": {
                "max_inflight_get": policy.max_inflight_get,
                "pool_prewarm": policy.pool_prewarm,
                "pool_prewarm_size": policy.pool_prewarm_size,
                "pool_prewarm_count": policy.pool_prewarm_count,
                "adaptive_feedback": policy.adaptive_feedback,
            },
        }
        proto.meta_info["mooncake_materialize_profile"] = materialize_profile
        return proto

    def remove_dataproto(self, ref: RemoteDataProtoRef | RemoteDataProtoShardedRef) -> None:
        manifest = ref.manifest or json.loads(self.store.get(ref.manifest_key).decode("utf-8"))
        if manifest.get("layout") == "row_sharded_v1":
            errors = []
            for shard in manifest["shards"]:
                shard_ref = RemoteDataProtoRef(
                    object_id=shard["object_id"],
                    row_count=int(shard["row_count"]),
                    manifest_key=shard["manifest_key"],
                    manifest=shard.get("manifest", {}),
                )
                try:
                    self.remove_dataproto(shard_ref)
                except KeyError:
                    continue
                except RuntimeError as exc:
                    errors.append(exc)
            if errors:
                raise RuntimeError(f"failed to remove {len(errors)} Mooncake shards") from errors[0]
            self._remove_keys([ref.manifest_key])
            return
        keys = [manifest["meta_info_key"]]
        keys.extend(entry["key"] for entry in manifest["batch"])
        for leaf in manifest["leaves"]:
            for spec in leaf["payload"].values():
                if "chunks" in spec and spec["chunks"] and "key" in spec["chunks"][0]:
                    keys.extend(chunk["key"] for chunk in spec["chunks"])
                else:
                    keys.append(spec["key"])
        keys.append(ref.manifest_key)
        self._remove_keys(keys)

    def _remove_keys(self, keys: Iterable[str]) -> None:
        errors = []
        for key in keys:
            try:
                rc = self.store.remove(key, True)
                if rc not in (None, 0):
                    errors.append((key, rc))
            except KeyError:
                continue
        if errors:
            raise RuntimeError(f"failed to remove {len(errors)} Mooncake keys: {errors[:3]}")

    def _should_shard(self, data: Any, policy: DataProtoShardPolicy) -> bool:
        if not policy.enabled and not policy.adaptive and policy.num_shards is None:
            return False
        row_count = len(data)
        if row_count <= 1:
            return False
        if policy.num_shards is not None:
            return policy.num_shards > 1
        return self._select_transfer_decision(data, policy).shard_count > 1

    def _select_transfer_decision(self, data: Any, policy: DataProtoShardPolicy) -> DataProtoTransferDecision:
        row_count = len(data)
        profile = self._estimate_dataproto_profile(data)
        if row_count <= 1:
            shard_count = 1
        elif policy.num_shards is not None:
            shard_count = max(1, min(row_count, policy.num_shards))
        elif profile.total_bytes < policy.small_payload_threshold:
            shard_count = 1
        elif profile.dense_tensor_bytes / max(profile.total_bytes, 1) >= policy.dense_ratio_threshold:
            shard_count = 1
        elif profile.row_oriented_bytes / max(profile.total_bytes, 1) < policy.row_oriented_ratio_threshold:
            shard_count = 1
        else:
            by_bytes = max(1, math.ceil(profile.total_bytes / max(policy.target_shard_bytes, 1)))
            shard_count = max(1, min(row_count, policy.max_shards, by_bytes))

        byte_balanced = bool(policy.byte_balanced and (profile.row_skewed or not policy.adaptive or policy.num_shards is not None))
        recommended_max_inflight_get = self._recommend_materialize_inflight(shard_count, profile, policy)
        return DataProtoTransferDecision(
            shard_count=shard_count,
            byte_balanced=byte_balanced,
            target_shard_bytes=policy.target_shard_bytes,
            recommended_max_inflight_get=recommended_max_inflight_get,
            profile=profile,
        )

    def _recommend_materialize_inflight(
        self,
        shard_count: int,
        profile: DataProtoTransferProfile,
        policy: DataProtoShardPolicy,
    ) -> int:
        if shard_count <= 1:
            return 1
        if profile.total_bytes < 2 * policy.small_payload_threshold:
            return min(shard_count, 2, policy.max_inflight_get)
        return min(shard_count, 4, policy.max_inflight_get)

    def _select_shard_ranges(self, data: Any, shard_count: int, byte_balanced: bool) -> List[Tuple[int, int]]:
        if shard_count <= 1:
            return [(0, len(data))]
        if not byte_balanced:
            ranges = []
            row_start = 0
            for chunk_size in self._equal_chunk_sizes(len(data), shard_count):
                ranges.append((row_start, row_start + chunk_size))
                row_start += chunk_size
            return ranges
        row_bytes = self._estimate_row_bytes(data)
        if not row_bytes or sum(row_bytes) == 0:
            ranges = []
            row_start = 0
            for chunk_size in self._equal_chunk_sizes(len(data), shard_count):
                ranges.append((row_start, row_start + chunk_size))
                row_start += chunk_size
            return ranges
        return self._byte_balanced_ranges(row_bytes, shard_count)

    def _estimate_dataproto_profile(self, data: Any) -> DataProtoTransferProfile:
        dense_tensor_bytes = 0
        if data.batch is not None:
            dense_tensor_bytes = sum(tensor_nbytes(tensor) for _name, tensor in data.batch.items())
        row_oriented_bytes = 0
        if getattr(data, "non_tensor_batch", None):
            for array in data.non_tensor_batch.values():
                if isinstance(array, np.ndarray):
                    row_oriented_bytes += int(array.nbytes)
                    for value in array:
                        row_oriented_bytes += self._estimate_value_bytes(value)
        row_bytes = self._estimate_row_bytes(data)
        total_bytes = dense_tensor_bytes + row_oriented_bytes
        row_bytes_sorted = sorted(row_bytes)
        row_bytes_p50 = self._percentile(row_bytes_sorted, 0.50)
        row_bytes_p95 = self._percentile(row_bytes_sorted, 0.95)
        row_bytes_max = max(row_bytes_sorted, default=0)
        row_skew_ratio = float(row_bytes_max) / max(float(row_bytes_p50), 1.0)
        row_skewed = row_skew_ratio >= 3.0 and row_bytes_max >= 64 * 1024 ** 2
        return DataProtoTransferProfile(
            total_bytes=int(total_bytes),
            dense_tensor_bytes=int(dense_tensor_bytes),
            row_oriented_bytes=int(row_oriented_bytes),
            row_bytes_p50=int(row_bytes_p50),
            row_bytes_p95=int(row_bytes_p95),
            row_bytes_max=int(row_bytes_max),
            row_skew_ratio=row_skew_ratio,
            row_skewed=row_skewed,
        )

    def _estimate_row_bytes(self, data: Any) -> List[int]:
        row_count = len(data)
        row_bytes = [0 for _ in range(row_count)]
        if data.batch is not None:
            for _name, tensor in data.batch.items():
                if tensor.shape and int(tensor.shape[0]) == row_count:
                    per_row = tensor_nbytes(tensor) // max(row_count, 1)
                    for index in range(row_count):
                        row_bytes[index] += per_row
        if getattr(data, "non_tensor_batch", None):
            for array in data.non_tensor_batch.values():
                if isinstance(array, np.ndarray):
                    for index, value in enumerate(array[:row_count]):
                        row_bytes[index] += self._estimate_value_bytes(value)
        return row_bytes

    def _byte_balanced_ranges(self, row_bytes: List[int], shard_count: int) -> List[Tuple[int, int]]:
        row_count = len(row_bytes)
        remaining_bytes = sum(row_bytes)
        ranges = []
        row_start = 0
        for shard_index in range(shard_count):
            remaining_shards = shard_count - shard_index
            if remaining_shards == 1:
                ranges.append((row_start, row_count))
                break
            target_bytes = remaining_bytes / remaining_shards
            row_end = row_start
            shard_bytes = 0
            max_end = row_count - remaining_shards + 1
            while row_end < max_end:
                next_bytes = shard_bytes + row_bytes[row_end]
                if (
                    row_end > row_start
                    and next_bytes > target_bytes
                    and abs(target_bytes - shard_bytes) <= abs(next_bytes - target_bytes)
                ):
                    break
                shard_bytes = next_bytes
                row_end += 1
            if row_end == row_start:
                row_end += 1
                shard_bytes = row_bytes[row_start]
            ranges.append((row_start, row_end))
            row_start = row_end
            remaining_bytes -= shard_bytes
        return ranges

    def _estimate_value_bytes(self, value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, torch.Tensor):
            return tensor_nbytes(value)
        if isinstance(value, np.ndarray):
            return int(value.nbytes)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return len(value)
        if isinstance(value, str):
            return len(value.encode("utf-8"))
        if isinstance(value, dict):
            return sum(self._estimate_value_bytes(child) for child in value.values())
        if isinstance(value, (list, tuple)):
            return sum(self._estimate_value_bytes(child) for child in value)
        if is_pil_image(value):
            width, height = getattr(value, "size", (0, 0))
            bands = len(getattr(value, "getbands", lambda: ())()) or 3
            return int(width) * int(height) * bands
        return 0

    def _percentile(self, sorted_values: List[int], quantile: float) -> int:
        if not sorted_values:
            return 0
        index = min(len(sorted_values) - 1, max(0, math.ceil(len(sorted_values) * quantile) - 1))
        return int(sorted_values[index])

    def _equal_chunk_sizes(self, row_count: int, shard_count: int) -> List[int]:
        base = row_count // shard_count
        remainder = row_count % shard_count
        return [base + (1 if index < remainder else 0) for index in range(shard_count)]

    def _manifest_logical_bytes(self, manifest: Dict[str, Any]) -> int:
        batch_bytes = sum(int(entry.get("bytes", 0)) for entry in manifest.get("batch", []))
        leaf_bytes = sum(int(entry.get("bytes", 0)) for entry in manifest.get("leaves", []))
        return batch_bytes + leaf_bytes

    def _policy_to_dict(
        self,
        policy: DataProtoShardPolicy,
        decision: DataProtoTransferDecision,
    ) -> Dict[str, Any]:
        return {
            "enabled": policy.enabled,
            "adaptive": policy.adaptive,
            "num_shards": policy.num_shards,
            "selected_shards": decision.shard_count,
            "target_shard_bytes": policy.target_shard_bytes,
            "max_shards": policy.max_shards,
            "max_inflight_get": decision.recommended_max_inflight_get,
            "configured_max_inflight_get": policy.max_inflight_get,
            "small_payload_threshold": policy.small_payload_threshold,
            "row_oriented_ratio_threshold": policy.row_oriented_ratio_threshold,
            "dense_ratio_threshold": policy.dense_ratio_threshold,
            "byte_balanced": decision.byte_balanced,
            "configured_byte_balanced": policy.byte_balanced,
        }

    def _transfer_profile_to_dict(self, profile: DataProtoTransferProfile) -> Dict[str, Any]:
        return {
            "total_bytes": profile.total_bytes,
            "dense_tensor_bytes": profile.dense_tensor_bytes,
            "row_oriented_bytes": profile.row_oriented_bytes,
            "row_bytes_p50": profile.row_bytes_p50,
            "row_bytes_p95": profile.row_bytes_p95,
            "row_bytes_max": profile.row_bytes_max,
            "row_skew_ratio": profile.row_skew_ratio,
            "row_skewed": profile.row_skewed,
        }

    def _select_materialize_inflight(
        self,
        parent_policy: Dict[str, Any],
        policy: DataProtoMaterializePolicy,
        shard_count: int,
    ) -> int:
        requested = policy.max_inflight_get or parent_policy.get("max_inflight_get", 8)
        selected = max(1, min(shard_count, int(requested)))
        if not policy.adaptive_feedback:
            return selected
        state = self._get_adaptive_state()
        if state.pressure_level == "throttle":
            selected = max(1, min(selected, max(1, int(math.ceil(selected / 2)))))
        elif state.pressure_level == "elevated":
            selected = max(1, min(selected, state.last_max_inflight_get or selected))
        return selected

    def _build_sharded_feedback(
        self,
        total_s: float,
        shard_materialize_s: Dict[str, float],
        shard_profiles: List[Dict[str, Any]],
        max_workers: int,
    ) -> Dict[str, Any]:
        sum_shard_s = sum(shard_materialize_s.values())
        parallel_efficiency = sum_shard_s / max(total_s, 1e-9)
        pool_wait_count = 0
        direct_range_fallback_count = 0
        for profile in shard_profiles:
            pool_stats = profile.get("registered_buffer_pool", {})
            pool_wait_count += int(pool_stats.get("wait_count_delta", 0))
            direct_range_fallback_count += int(profile.get("direct_range_fallback_count", 0))
        pressure_level = "normal"
        if pool_wait_count > 0 or direct_range_fallback_count > 0:
            pressure_level = "throttle"
        elif max_workers > 1 and parallel_efficiency < 1.2:
            pressure_level = "elevated"
        return {
            "sum_shard_materialize_s": sum_shard_s,
            "parallel_efficiency": parallel_efficiency,
            "pool_wait_count": pool_wait_count,
            "direct_range_fallback_count": direct_range_fallback_count,
            "pressure_level": pressure_level,
            "max_inflight_get": max_workers,
        }

    def _get_adaptive_state(self) -> DataProtoTransferAdaptiveState:
        with _STORE_STATE_LOCK:
            state = _TRANSFER_ADAPTIVE_STATES.get(id(self.store))
            if state is None:
                state = DataProtoTransferAdaptiveState()
                _TRANSFER_ADAPTIVE_STATES[id(self.store)] = state
            return state

    def _update_adaptive_state(self, feedback: Dict[str, Any], policy: DataProtoMaterializePolicy) -> None:
        if not policy.adaptive_feedback:
            return
        with _STORE_STATE_LOCK:
            state = self._get_adaptive_state()
            alpha = 0.25
            if state.update_count == 0:
                state.ewma_parallel_efficiency = float(feedback["parallel_efficiency"])
                state.ewma_pool_wait_count = float(feedback["pool_wait_count"])
                state.ewma_direct_range_fallback_count = float(feedback["direct_range_fallback_count"])
            else:
                state.ewma_parallel_efficiency = self._ewma(
                    state.ewma_parallel_efficiency,
                    float(feedback["parallel_efficiency"]),
                    alpha,
                )
                state.ewma_pool_wait_count = self._ewma(
                    state.ewma_pool_wait_count,
                    float(feedback["pool_wait_count"]),
                    alpha,
                )
                state.ewma_direct_range_fallback_count = self._ewma(
                    state.ewma_direct_range_fallback_count,
                    float(feedback["direct_range_fallback_count"]),
                    alpha,
                )
            state.pressure_level = str(feedback["pressure_level"])
            state.last_max_inflight_get = int(feedback.get("max_inflight_get", 0)) or state.last_max_inflight_get
            state.update_count += 1

    def _ewma(self, previous: float, current: float, alpha: float) -> float:
        return previous * (1.0 - alpha) + current * alpha

    def _adaptive_state_to_dict(self) -> Dict[str, Any]:
        with _STORE_STATE_LOCK:
            state = self._get_adaptive_state()
            return {
                "pressure_level": state.pressure_level,
                "last_max_inflight_get": state.last_max_inflight_get,
                "ewma_parallel_efficiency": state.ewma_parallel_efficiency,
                "ewma_pool_wait_count": state.ewma_pool_wait_count,
                "ewma_direct_range_fallback_count": state.ewma_direct_range_fallback_count,
                "update_count": state.update_count,
            }

    def _prepare_materialize_pool(
        self,
        manifests: List[Dict[str, Any]],
        policy: DataProtoMaterializePolicy,
        max_inflight_get: int = 1,
    ) -> None:
        if not policy.pool_prewarm:
            return
        prewarm_size = policy.pool_prewarm_size or self._max_chunk_payload_bytes(manifests)
        if prewarm_size <= 0:
            return
        prewarm_count = policy.pool_prewarm_count or max_inflight_get
        pool = get_registered_buffer_pool(self.store)
        pool.ensure_prewarmed(prewarm_size, prewarm_count)

    def _max_chunk_payload_bytes(self, manifests: List[Dict[str, Any]]) -> int:
        max_bytes = 0
        for manifest in manifests:
            for leaf in manifest.get("leaves", []):
                for spec in leaf.get("payload", {}).values():
                    chunks = spec.get("chunks") or []
                    if chunks and "key" in chunks[0]:
                        max_bytes = max(max_bytes, *(int(chunk.get("bytes", 0)) for chunk in chunks))
        return max_bytes

    def _reconstruct_struct(
        self,
        root_path: str,
        field_leaves: List[Tuple[Dict[str, Any], List[Any]]],
        nodes_by_path: Dict[str, Dict[str, Any]],
        row_count: int,
    ) -> List[Any]:
        leaf_values = {entry["path"]: values for entry, values in field_leaves}

        def build(path: str, row: int) -> Any:
            if path in leaf_values:
                return leaf_values[path][row]
            node = nodes_by_path.get(path)
            if node is None:
                return None
            if node["node_type"] == "dict":
                item = {}
                for key in node["children"]:
                    value = build(f"{path}.{key}", row)
                    if value is not None:
                        item[key] = value
                return item
            if node["node_type"] == "list":
                length = int(node["lengths"][row])
                return [build(f"{path}[{index}]", row) for index in range(length)]
            raise ValueError(f"unknown node type: {node['node_type']}")

        return [build(root_path, row) for row in range(row_count)]
