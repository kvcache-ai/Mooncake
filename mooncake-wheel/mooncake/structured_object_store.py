from __future__ import annotations

import ctypes
import io
import json
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Literal, Mapping, Optional, Protocol, Sequence

import numpy as np

try:
    import mooncake.store as _mooncake_store
except Exception:  # pragma: no cover - depends on built extension
    _mooncake_store = None  # type: ignore[assignment]

import msgpack as _msgpack

DEFAULT_BUNDLE_CHUNK_BYTES = 512 * 1024**2
AUTO_PARALLEL_MIN_BYTES = 4 * 1024**3
AUTO_PARALLEL_MIN_CHUNKS = 8
MISSING_OBJECT_ERROR = (
    -704
)  # Mooncake remove returns -704 for an already-missing object.
STRUCTURED_FIELD_SPECS_KEY = "__mooncake_structured_fields__"


class BundleStore(Protocol):
    def put(self, key: str, value: Any) -> int: ...

    def get(self, key: str) -> bytes: ...

    def remove(self, key: str, force: bool = False) -> int: ...


@dataclass(frozen=True)
class BundleTransferPolicy:
    """Controls generic bundle transfer parallelism."""

    max_inflight_put: int = 1
    put_mode: Literal["auto", "batch", "parallel"] = "auto"
    copy_mode: Literal["auto", "zero_copy", "copy"] = "auto"


@dataclass
class RemoteBundleRef:
    """Reference to a generic named-buffer bundle stored in Mooncake."""

    manifest_key: str
    manifest: dict[str, Any]


@dataclass(frozen=True, init=False)
class StructuredObjectPayload:
    """Structured object encoded as metadata plus named buffers."""

    metadata: Mapping[str, Any]
    buffers: Mapping[str, Any]

    def __init__(
        self,
        metadata: Optional[Mapping[str, Any]] = None,
        buffers: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if buffers is None:
            raise TypeError("StructuredObjectPayload requires buffers")
        object.__setattr__(self, "metadata", {} if metadata is None else metadata)
        object.__setattr__(self, "buffers", buffers)


@dataclass
class StructuredObjectResult:
    """Materialized structured object data fetched from Mooncake."""

    metadata: dict[str, Any]
    objects: dict[str, Any]


@dataclass(frozen=True)
class StructuredFieldLocation:
    """Location of a DataProto field inside stage-level structured objects."""

    stage: str
    member: str
    section: Literal["batch", "non_tensor_batch"]


@dataclass
class MooncakeDataProtoRef:
    """Lightweight DataProto handle backed by structured object refs."""

    batch_size: int
    stage_refs: dict[str, RemoteBundleRef]
    field_index: dict[str, StructuredFieldLocation]
    meta_info: dict[str, Any]
    namespace: str = "default"
    partition: str = "default"
    global_indexes: list[int] | None = None
    encoded_non_tensor: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _TensorPayload:
    tensor: Any


@dataclass(frozen=True)
class _TensorObjectBufferPayload:
    ptr: int
    size: int
    owner: Any
    batch_size: int | None = None

    def __len__(self) -> int:
        if self.batch_size is None:
            raise TypeError(
                "tensor object buffer requires batch_size for DataProto use"
            )
        return self.batch_size


@dataclass(frozen=True)
class _RawDestinationBuffer:
    ptr: int
    size: int
    owner: Any
    pre_registered: bool = False


@dataclass(frozen=True)
class StructuredMemberSlice:
    """Slice description for one structured member."""

    axis: int
    start: int
    end: int
    step: int = 1


@dataclass(frozen=True)
class StructuredObjectReadSpec:
    """Lazy read plan for structured object materialization."""

    ref: RemoteBundleRef | Mapping[str, Any]
    member_names: tuple[str, ...] | None = None
    member_slices: tuple[tuple[str, StructuredMemberSlice], ...] = ()

    def select_members(
        self, names: Optional[Sequence[str]]
    ) -> "StructuredObjectReadSpec":
        """Return a new spec that materializes only the selected members."""
        return StructuredObjectReadSpec(
            ref=self.ref,
            member_names=None if names is None else tuple(names),
            member_slices=self.member_slices,
        )

    def slice_member(
        self,
        name: str,
        axis: int,
        start: int,
        end: int,
        step: int = 1,
    ) -> "StructuredObjectReadSpec":
        """Return a new spec with a slice applied to one member."""
        slice_spec = StructuredMemberSlice(axis=axis, start=start, end=end, step=step)
        member_slices = dict(self.member_slices)
        member_slices[name] = slice_spec
        return StructuredObjectReadSpec(
            ref=self.ref,
            member_names=self.member_names,
            member_slices=tuple(member_slices.items()),
        )

    def member_slice(self, name: str) -> StructuredMemberSlice | None:
        """Return the configured slice for a member, if any."""
        return dict(self.member_slices).get(name)


@dataclass(frozen=True)
class _NdarrayReadPlan:
    dtype: np.dtype[Any]
    full_shape: tuple[int, ...]
    output_shape: tuple[int, ...]
    byte_offset: int
    byte_length: int
    step: int
    cover_row_count: int


@dataclass(frozen=True)
class _DataProtoRowSelection:
    count: int
    member_slice: StructuredMemberSlice | None = None
    indices: tuple[int, ...] | None = None


DATAPROTO_REF_HANDLE_TYPE = "mooncake_dataproto_ref"
DATAPROTO_REF_HANDLE_VERSION = 1
DataProtoRefLike = MooncakeDataProtoRef | Mapping[str, Any]


def is_dataproto_ref_handle(value: Any) -> bool:
    return isinstance(value, Mapping) and value.get("type") == DATAPROTO_REF_HANDLE_TYPE


def export_dataproto_ref(ref: MooncakeDataProtoRef) -> dict[str, Any]:
    """Export a DataProto ref as a JSON-safe transport handle."""
    if not isinstance(ref, MooncakeDataProtoRef):
        raise TypeError(f"expected MooncakeDataProtoRef, got {type(ref).__name__}")
    handle = {
        "type": DATAPROTO_REF_HANDLE_TYPE,
        "version": DATAPROTO_REF_HANDLE_VERSION,
        "kind": "bundle_stages",
        "batch_size": int(ref.batch_size),
        "namespace": ref.namespace,
        "partition": ref.partition,
        "global_indexes": _json_safe_value(ref.global_indexes),
        "stage_refs": {
            stage: {"manifest_key": stage_ref.manifest_key}
            for stage, stage_ref in ref.stage_refs.items()
        },
        "field_index": {
            name: {
                "stage": location.stage,
                "member": location.member,
                "section": location.section,
            }
            for name, location in ref.field_index.items()
        },
        "meta_info": _json_safe_value(ref.meta_info),
        "encoded_non_tensor": _json_safe_value(ref.encoded_non_tensor),
    }
    json.dumps(handle, ensure_ascii=False)
    return handle


def import_dataproto_ref(handle: Mapping[str, Any]) -> MooncakeDataProtoRef:
    """Import a JSON-safe DataProto transport handle into a lazy ref."""
    if not is_dataproto_ref_handle(handle):
        raise ValueError("not a Mooncake DataProto ref handle")
    if int(handle.get("version", -1)) != DATAPROTO_REF_HANDLE_VERSION:
        raise ValueError(
            f"unsupported DataProto ref handle version: {handle.get('version')!r}"
        )
    if handle.get("kind") != "bundle_stages":
        raise ValueError(
            f"unsupported DataProto ref handle kind: {handle.get('kind')!r}"
        )
    stage_refs: dict[str, RemoteBundleRef] = {}
    for stage, stage_ref in _require_mapping(
        handle.get("stage_refs"), "stage_refs"
    ).items():
        manifest_key = _require_mapping(stage_ref, f"stage_refs[{stage!r}]").get(
            "manifest_key"
        )
        if not isinstance(stage, str) or not isinstance(manifest_key, str):
            raise ValueError(
                "DataProto ref handle stage refs must contain string manifest_key values"
            )
        stage_refs[stage] = RemoteBundleRef(manifest_key=manifest_key, manifest={})
    field_index: dict[str, StructuredFieldLocation] = {}
    for name, raw_location in _require_mapping(
        handle.get("field_index"), "field_index"
    ).items():
        location = _require_mapping(raw_location, f"field_index[{name!r}]")
        section = location.get("section")
        if section not in {"batch", "non_tensor_batch"}:
            raise ValueError(f"invalid DataProto field section: {section!r}")
        stage = location.get("stage")
        member = location.get("member")
        if (
            not isinstance(name, str)
            or not isinstance(stage, str)
            or not isinstance(member, str)
        ):
            raise ValueError("DataProto ref handle field locations must be strings")
        if stage not in stage_refs:
            raise ValueError(
                f"DataProto ref handle field {name!r} references unknown stage {stage!r}"
            )
        field_index[name] = StructuredFieldLocation(
            stage=stage, member=member, section=section
        )
    return MooncakeDataProtoRef(
        batch_size=int(handle["batch_size"]),
        stage_refs=stage_refs,
        field_index=field_index,
        meta_info=dict(_require_mapping(handle.get("meta_info", {}), "meta_info")),
        namespace=str(handle.get("namespace", "default")),
        partition=str(handle.get("partition", "default")),
        global_indexes=_import_global_indexes(handle.get("global_indexes")),
        encoded_non_tensor=dict(
            _require_mapping(handle.get("encoded_non_tensor", {}), "encoded_non_tensor")
        ),
    )


def _resolve_dataproto_ref(ref: DataProtoRefLike) -> MooncakeDataProtoRef:
    if isinstance(ref, MooncakeDataProtoRef):
        return ref
    if is_dataproto_ref_handle(ref):
        return import_dataproto_ref(ref)
    raise TypeError(
        f"expected MooncakeDataProtoRef or DataProto handle, got {type(ref).__name__}"
    )


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"DataProto ref handle {name} must be a mapping")
    return value


def _import_global_indexes(value: Any) -> list[int] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("DataProto ref handle global_indexes must be a list or null")
    indexes: list[int] = []
    for index in value:
        if isinstance(index, bool) or not isinstance(index, int):
            raise ValueError(
                "DataProto ref handle global_indexes must contain integers"
            )
        indexes.append(index)
    return indexes


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return _json_safe_value(value.tolist())
    if isinstance(value, Mapping):
        return {str(key): _json_safe_value(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    raise TypeError(
        f"DataProto ref handles support only JSON-safe values, got {type(value).__name__}"
    )


class MooncakeBundleTransfer:
    """Transfer structured objects through Mooncake, with a low-level bundle fallback."""

    def __init__(
        self,
        store: BundleStore,
        key_prefix: str = "bundle",
        default_chunk_bytes: int = DEFAULT_BUNDLE_CHUNK_BYTES,
        buffer_pool: Any = None,
    ) -> None:
        """Initialize a bundle transfer helper with a configurable default chunk size."""
        self.store = store
        self.key_prefix = _normalize_key_prefix(key_prefix)
        self.default_chunk_bytes = _validate_chunk_bytes(default_chunk_bytes)
        self._transport = _MooncakePayloadTransport(store, buffer_pool=buffer_pool)
        self._bundle_store = _BundleManifestStore(
            store=store,
            transport=self._transport,
            key_prefix=self.key_prefix,
            default_chunk_bytes=self.default_chunk_bytes,
        )
        self._structured_store = _StructuredObjectLayer(self._bundle_store)

    def put_bundle(
        self,
        meta: bytes | bytearray | memoryview,
        buffers: Mapping[str, Any],
        partition: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        max_inflight_put: Optional[int] = None,
        pre_registered_buffers: Optional[Mapping[str, bool]] = None,
    ) -> RemoteBundleRef:
        """Store raw metadata bytes plus named buffers as a low-level bundle."""
        return self._bundle_store.put_bundle(
            meta=meta,
            buffers=buffers,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
            pre_registered_buffers=pre_registered_buffers,
        )

    def remove_bundle(self, ref: RemoteBundleRef | Mapping[str, Any]) -> None:
        """Remove all Mooncake objects that belong to a stored bundle."""
        self._bundle_store.remove_bundle(ref)

    def put_structured_object(
        self,
        payload: StructuredObjectPayload,
        partition: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        max_inflight_put: Optional[int] = None,
        pre_registered_buffers: Optional[Mapping[str, bool]] = None,
    ) -> RemoteBundleRef:
        """Store a structured object described by JSON metadata plus named members."""
        return self._structured_store.put_structured_object(
            payload=payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
            pre_registered_buffers=pre_registered_buffers,
        )

    def put_object(
        self,
        obj: Any,
        partition: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        max_inflight_put: Optional[int] = None,
    ) -> RemoteBundleRef:
        """Store a mapping object or a single tensor/array value."""
        if isinstance(obj, Mapping):
            payload = StructuredObjectPayload(metadata={}, buffers=obj)
        else:
            payload = StructuredObjectPayload(
                metadata={"__mooncake_wrapped_object__": True},
                buffers={"value": obj},
            )
        return self.put_structured_object(
            payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
        )

    def get_object(self, ref: RemoteBundleRef | Mapping[str, Any]) -> Any:
        """Materialize an object stored by put_object()."""
        result = self.materialize(self.read_spec(ref))
        if result.metadata.get("__mooncake_wrapped_object__"):
            return result.objects["value"]
        return result.objects

    def read_spec(
        self, ref: RemoteBundleRef | Mapping[str, Any]
    ) -> StructuredObjectReadSpec:
        """Create a lazy read spec for a structured object reference."""
        return self._structured_store.read_spec(ref)

    def materialize(self, spec: StructuredObjectReadSpec) -> StructuredObjectResult:
        """Materialize a structured object read spec."""
        return self._structured_store.materialize(spec)

    def materialize_into(
        self,
        spec: StructuredObjectReadSpec,
        destinations: Optional[Mapping[str, Any]],
    ) -> StructuredObjectResult:
        """Materialize a structured object read spec into caller-provided destinations when possible."""
        return self._structured_store.materialize_into(spec, destinations)

    def put_dataproto(
        self,
        data: Any,
        *,
        namespace: str = "default",
        partition: str = "default",
        stage: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
    ) -> MooncakeDataProtoRef:
        """Store a DataProto-like object as a stage-level structured object."""
        return self._put_dataproto_stage(
            None,
            data,
            namespace=namespace,
            partition=partition,
            stage=stage,
            chunk_bytes=chunk_bytes,
            policy=policy,
            overwrite=False,
        )

    def append_dataproto_fields(
        self,
        ref: DataProtoRefLike,
        data: Any,
        *,
        stage: str,
        overwrite: bool = False,
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
    ) -> MooncakeDataProtoRef:
        """Append fields from a later DataProto stage without rewriting previous stages."""
        ref = _resolve_dataproto_ref(ref)
        return self._put_dataproto_stage(
            ref,
            data,
            namespace=ref.namespace,
            partition=ref.partition,
            stage=stage,
            chunk_bytes=chunk_bytes,
            policy=policy,
            overwrite=overwrite,
        )

    def dataproto_manifest_view(self, ref: DataProtoRefLike) -> dict[str, Any]:
        """Return a DataProto field view derived from stage structured-object manifests."""
        return _dataproto_manifest_view(self, _resolve_dataproto_ref(ref))

    def get_dataproto(
        self,
        ref: DataProtoRefLike,
        *,
        fields: Optional[Sequence[str]] = None,
        batch_fields: Optional[Sequence[str]] = None,
        non_tensor_fields: Optional[Sequence[str]] = None,
        meta_info_keys: Optional[Sequence[str]] = None,
        data_cls: Optional[Any] = None,
        destinations: Optional[Mapping[str, Any]] = None,
        rows: slice | StructuredMemberSlice | Sequence[int] | None = None,
    ) -> Any:
        """Materialize selected DataProto fields from structured object refs."""
        ref = _resolve_dataproto_ref(ref)
        row_selection = _coerce_dataproto_row_selection(rows, ref.batch_size)
        row_slice = None if row_selection is None else row_selection.member_slice
        row_indices = None if row_selection is None else row_selection.indices
        output_rows = ref.batch_size if row_selection is None else row_selection.count
        batch_names, non_tensor_names = _resolve_dataproto_field_selection(
            ref, fields, batch_fields, non_tensor_fields
        )
        batch: dict[str, Any] = {}
        non_tensor_batch: dict[str, Any] = {}
        destination_map = destinations or {}
        requested = [
            *[("batch", name) for name in batch_names],
            *[("non_tensor_batch", name) for name in non_tensor_names],
        ]
        by_stage: dict[str, list[tuple[str, StructuredFieldLocation]]] = {}
        encoded_requests: list[tuple[str, StructuredFieldLocation]] = []
        for _section, name in requested:
            location = ref.field_index[name]
            if (
                location.section == "non_tensor_batch"
                and name in ref.encoded_non_tensor
            ):
                encoded_requests.append((name, location))
                continue
            by_stage.setdefault(location.stage, []).append((name, location))
        for stage, entries in by_stage.items():
            stage_ref = ref.stage_refs[stage]
            members = [location.member for _name, location in entries]
            stage_destinations = {
                location.member: destination_map[name]
                for name, location in entries
                if name in destination_map
            }
            if row_indices is None:
                spec = self.read_spec(stage_ref).select_members(members)
                if row_slice is not None:
                    for member in members:
                        spec = spec.slice_member(
                            member,
                            axis=row_slice.axis,
                            start=row_slice.start,
                            end=row_slice.end,
                            step=row_slice.step,
                        )
                result = self.materialize_into(spec, stage_destinations)
                for name, location in entries:
                    value = result.objects[location.member]
                    if location.section == "batch":
                        batch[name] = value
                    else:
                        non_tensor_batch[name] = value
                continue
            for name, location in entries:
                value = self._read_dataproto_member_indices(
                    stage_ref,
                    location.member,
                    row_indices,
                    destination_map.get(name),
                )
                if location.section == "batch":
                    batch[name] = value
                else:
                    non_tensor_batch[name] = value
        for name, location in encoded_requests:
            stage_ref = ref.stage_refs[location.stage]
            encoded = ref.encoded_non_tensor[name]
            if row_selection is None:
                members = list(encoded["payload_members"].values())
                result = self.materialize(
                    self.read_spec(stage_ref).select_members(members)
                )
                payload = {
                    payload_name: result.objects[member]
                    for payload_name, member in encoded["payload_members"].items()
                }
                values = _decode_structured_non_tensor_encoded(
                    encoded, payload, ref.batch_size, encoded.get("metadata")
                )
            elif row_indices is not None:
                payload, metadata = self._read_structured_non_tensor_payload_indices(
                    stage_ref,
                    encoded,
                    row_indices,
                )
                values = _decode_structured_non_tensor_encoded(
                    encoded, payload, output_rows, metadata
                )
            else:
                payload, metadata = self._read_structured_non_tensor_payload_slice(
                    stage_ref,
                    encoded,
                    row_slice,
                    ref.batch_size,
                )
                values = _decode_structured_non_tensor_encoded(
                    encoded, payload, output_rows, metadata
                )
            array = np.empty(output_rows, dtype=object)
            array[:] = values
            non_tensor_batch[name] = array
        meta_info = _select_mapping(ref.meta_info, meta_info_keys)
        return _build_dataproto_like_result(
            batch, non_tensor_batch, meta_info, data_cls
        )

    def _read_dataproto_member_indices(
        self,
        stage_ref: RemoteBundleRef,
        member: str,
        indices: Sequence[int],
        destination: Any,
    ) -> Any:
        manifest = self._bundle_store.resolve_manifest(stage_ref)
        metadata = _decode_structured_metadata(
            self._bundle_store.read_payload(manifest["meta"])
        )
        field_spec = _structured_field_specs(metadata).get(member, {"encoding": "bytes"})
        payload_spec = manifest["buffers"][member]
        encoding = field_spec.get("encoding", "bytes")
        if encoding == "ndarray":
            return self._read_ndarray_member_indices(
                member, payload_spec, field_spec, indices, destination
            )
        if encoding == "torch_tensor":
            return self._read_torch_tensor_member_indices(
                member, payload_spec, field_spec, indices, destination
            )
        raise ValueError(f"structured member {member} does not support indexed rows")

    def _read_ndarray_member_indices(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        indices: Sequence[int],
        destination: Any,
    ) -> np.ndarray:
        dtype = field_spec.get("dtype")
        shape = field_spec.get("shape")
        if not isinstance(dtype, str) or not isinstance(shape, list):
            raise ValueError(
                f"structured ndarray field {name} is missing dtype or shape"
            )
        dtype_obj = np.dtype(dtype)
        full_shape = tuple(int(dim) for dim in shape)
        if not full_shape:
            raise ValueError("structured ndarray indexed read requires at least one dimension")
        output_shape = (len(indices), *full_shape[1:])
        if isinstance(destination, _RawDestinationBuffer):
            nbytes = int(np.prod(output_shape, dtype=np.int64)) * dtype_obj.itemsize
            if destination.size < nbytes:
                raise ValueError(
                    f"raw destination has {destination.size} bytes, expected at least {nbytes}"
                )
            target = np.ctypeslib.as_array(
                (ctypes.c_uint8 * nbytes).from_address(destination.ptr)
            ).view(dtype_obj).reshape(output_shape)
            if not indices:
                return target
            row_width = dtype_obj.itemsize * int(np.prod(full_shape[1:], dtype=np.int64))
            ranges = [
                (row * row_width, out * row_width, row_width)
                for out, row in enumerate(indices)
            ]
            if destination.pre_registered:
                self._bundle_store.read_payload_ranges_into_raw_destination(
                    payload_spec, destination.ptr, ranges
                )
            else:
                data = bytearray(nbytes)
                self._bundle_store.read_payload_ranges_into_bytearray(
                    payload_spec, data, ranges
                )
                ctypes.memmove(destination.ptr, bytes(data), nbytes)
            _ = destination.owner
            return target
        target = _resolve_ndarray_destination(name, destination, dtype_obj, output_shape)
        if not indices:
            return target
        row_width = dtype_obj.itemsize * int(np.prod(full_shape[1:], dtype=np.int64))
        self._bundle_store.read_payload_ranges_into_array(
            payload_spec,
            target.view(np.uint8).reshape(-1),
            [
                (row * row_width, out * row_width, row_width)
                for out, row in enumerate(indices)
            ],
        )
        return target

    def _read_torch_tensor_member_indices(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        indices: Sequence[int],
        destination: Any,
    ) -> Any:
        metadata_bytes = int(payload_spec.get("metadata_bytes", -1))
        shape = field_spec.get("shape")
        element_size = int(field_spec.get("element_size", 0))
        if metadata_bytes < 0 or not isinstance(shape, list) or element_size <= 0:
            raise ValueError(
                f"structured tensor field {name} is missing slice metadata"
            )
        if not shape:
            raise ValueError("structured tensor indexed read requires at least one dimension")
        row_width = element_size * int(np.prod(shape[1:], dtype=np.int64))
        data_length = len(indices) * row_width
        metadata = self._bundle_store.read_payload_range(payload_spec, 0, metadata_bytes)
        sliced_metadata = _slice_tensor_metadata(
            metadata, (len(indices), *shape[1:]), data_length
        )
        if destination is not None:
            if not isinstance(destination, (_TensorObjectBufferPayload, _RawDestinationBuffer)):
                raise ValueError(
                    f"structured tensor member {name} only supports tensor_object_buffer or raw_destination destinations"
                )
            expected_bytes = metadata_bytes + data_length
            if destination.size < expected_bytes:
                raise ValueError(
                    f"tensor destination has {destination.size} bytes, expected at least {expected_bytes}"
                )
            ctypes.memmove(destination.ptr, sliced_metadata, metadata_bytes)
            if data_length:
                ranges = [
                    (
                        metadata_bytes + row * row_width,
                        metadata_bytes + out * row_width,
                        row_width,
                    )
                    for out, row in enumerate(indices)
                ]
                if isinstance(destination, _RawDestinationBuffer) and destination.pre_registered:
                    self._bundle_store.read_payload_ranges_into_raw_destination(
                        payload_spec, destination.ptr, ranges
                    )
                else:
                    data = bytearray(data_length)
                    self._bundle_store.read_payload_ranges_into_bytearray(
                        payload_spec,
                        data,
                        [
                            (source_offset, destination_offset - metadata_bytes, size)
                            for source_offset, destination_offset, size in ranges
                        ],
                    )
                    ctypes.memmove(
                        destination.ptr + metadata_bytes, bytes(data), data_length
                    )
            _ = destination.owner
            return destination
        data = bytearray(data_length)
        if data_length:
            self._bundle_store.read_payload_ranges_into_bytearray(
                payload_spec,
                data,
                [(metadata_bytes + row * row_width, out * row_width, row_width) for out, row in enumerate(indices)],
            )
        return _deserialize_tensor_payload(sliced_metadata + data)

    def _read_structured_non_tensor_payload_indices(
        self,
        stage_ref: RemoteBundleRef,
        encoded: Mapping[str, Any],
        indices: Sequence[int],
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        codec = encoded["codec"]
        payload_members = encoded["payload_members"]
        manifest = self._bundle_store.resolve_manifest(stage_ref)
        stage_metadata = _decode_structured_metadata(
            self._bundle_store.read_payload(manifest["meta"])
        )
        field_specs = _structured_field_specs(stage_metadata)
        metadata = dict(encoded.get("metadata") or {})

        def member(payload_name: str) -> str:
            return payload_members[payload_name]

        def member_dtype(payload_name: str) -> np.dtype[Any]:
            spec = field_specs.get(member(payload_name), {})
            dtype = spec.get("dtype")
            if not isinstance(dtype, str):
                raise ValueError(
                    f"structured non-tensor payload {member(payload_name)} is missing dtype"
                )
            return np.dtype(dtype)

        def read_member_indices(payload_name: str, member_indices: Sequence[int]) -> Any:
            return self._read_dataproto_member_indices(
                stage_ref, member(payload_name), member_indices, None
            )

        def read_data_ranges(
            payload_name: str,
            ranges: Sequence[tuple[int, int, int]],
            dtype: np.dtype[Any] | None = None,
        ) -> Any:
            total_bytes = sum(byte_length for _src, _dst, byte_length in ranges)
            payload_spec = manifest["buffers"][member(payload_name)]
            if dtype is None:
                data = bytearray(total_bytes)
                self._bundle_store.read_payload_ranges_into_bytearray(
                    payload_spec, data, ranges
                )
                return bytes(data)
            array = np.empty(total_bytes // dtype.itemsize, dtype=dtype)
            self._bundle_store.read_payload_ranges_into_array(
                payload_spec, array.view(np.uint8).reshape(-1), ranges
            )
            return array

        if _is_recursive_encoded_non_tensor(encoded):
            metadata = _copy_recursive_metadata_for_leaf_updates(metadata)
            recursive_payload: dict[str, Any] = {}
            for node in metadata.get("nodes", []):
                for key in ("missing_payload", "row_mask_payload", "lengths_payload"):
                    payload_name = node.get(key)
                    if payload_name is not None:
                        recursive_payload[payload_name] = read_member_indices(
                            payload_name, indices
                        )
            for leaf in metadata.get("leaves", []):
                leaf_payload_members = leaf["payload_members"]
                flat_payload_members = {
                    name: payload_members[global_name]
                    for name, global_name in leaf_payload_members.items()
                    if name != "missing"
                }
                leaf_payload, _leaf_metadata = self._read_structured_non_tensor_payload_indices(
                    stage_ref,
                    {
                        "codec": leaf["codec"],
                        "metadata": leaf.get("metadata") or {},
                        "payload_members": flat_payload_members,
                    },
                    indices,
                )
                leaf["metadata"] = _leaf_metadata
                for name, value in leaf_payload.items():
                    recursive_payload[leaf_payload_members[name]] = value
                recursive_payload[leaf_payload_members["missing"]] = read_member_indices(
                    leaf_payload_members["missing"], indices
                )
            return recursive_payload, metadata

        if codec == "ndarray":
            return {
                "data": read_member_indices("data", indices),
                "nulls": read_member_indices("nulls", indices),
            }, metadata

        if codec in {"ragged_tensor", "typed_ragged"}:
            offsets = read_member_indices(
                "offsets", [index for row in indices for index in (row, row + 1)]
            )
            begins = offsets[0::2]
            ends = offsets[1::2]
            item_counts = [int(end) - int(begin) for begin, end in zip(begins, ends)]
            gathered_offsets = np.empty(len(indices) + 1, dtype=offsets.dtype)
            gathered_offsets[0] = 0
            for index, count in enumerate(item_counts):
                gathered_offsets[index + 1] = int(gathered_offsets[index]) + count
            dtype = member_dtype("data")
            ranges = []
            destination_item = 0
            for begin, count in zip(begins, item_counts):
                if count:
                    ranges.append(
                        (
                            int(begin) * dtype.itemsize,
                            destination_item * dtype.itemsize,
                            count * dtype.itemsize,
                        )
                    )
                destination_item += count
            return {
                "data": read_data_ranges("data", ranges, dtype),
                "offsets": gathered_offsets,
                "shapes": read_member_indices("shapes", indices),
                "ndims": read_member_indices("ndims", indices),
                "nulls": read_member_indices("nulls", indices),
            }, metadata

        if codec in {"media_bytes", "bytes_ragged", "utf8_ragged", "msgpack_ragged", "json_ragged"}:
            offsets = read_member_indices(
                "offsets", [index for row in indices for index in (row, row + 1)]
            )
            begins = offsets[0::2]
            ends = offsets[1::2]
            byte_counts = [int(end) - int(begin) for begin, end in zip(begins, ends)]
            gathered_offsets = np.empty(len(indices) + 1, dtype=offsets.dtype)
            gathered_offsets[0] = 0
            ranges = []
            destination_offset = 0
            for index, (begin, count) in enumerate(zip(begins, byte_counts)):
                gathered_offsets[index + 1] = destination_offset + count
                if count:
                    ranges.append((int(begin), destination_offset, count))
                destination_offset += count
            if "media_encodings" in metadata:
                metadata["media_encodings"] = [
                    metadata["media_encodings"][index] for index in indices
                ]
            return {
                "data": read_data_ranges("data", ranges),
                "offsets": gathered_offsets,
                "nulls": read_member_indices("nulls", indices),
            }, metadata

        if codec == "media_list_ragged":
            row_offsets = read_member_indices(
                "row_offsets", [index for row in indices for index in (row, row + 1)]
            )
            item_begins = row_offsets[0::2]
            item_ends = row_offsets[1::2]
            item_counts = [
                int(end) - int(begin) for begin, end in zip(item_begins, item_ends)
            ]
            gathered_row_offsets = np.empty(len(indices) + 1, dtype=row_offsets.dtype)
            gathered_row_offsets[0] = 0
            for index, count in enumerate(item_counts):
                gathered_row_offsets[index + 1] = int(gathered_row_offsets[index]) + count
            boundary_indices = [
                boundary
                for begin, end in zip(item_begins, item_ends)
                for boundary in range(int(begin), int(end) + 1)
            ]
            byte_offsets = read_member_indices("byte_offsets", boundary_indices)
            ranges = []
            gathered_byte_offsets = np.empty(
                int(gathered_row_offsets[-1]) + 1, dtype=byte_offsets.dtype
            )
            gathered_byte_offsets[0] = 0
            destination_offset = 0
            source_index = 0
            destination_boundary = 1
            media_encodings = []
            for begin, count in zip(item_begins, item_counts):
                for item in range(count):
                    byte_begin = int(byte_offsets[source_index + item])
                    byte_end = int(byte_offsets[source_index + item + 1])
                    byte_count = byte_end - byte_begin
                    if byte_count:
                        ranges.append((byte_begin, destination_offset, byte_count))
                    destination_offset += byte_count
                    gathered_byte_offsets[destination_boundary] = destination_offset
                    destination_boundary += 1
                if "media_encodings" in metadata:
                    media_encodings.extend(
                        metadata["media_encodings"][int(begin) : int(begin) + count]
                    )
                source_index += count + 1
            if "media_encodings" in metadata:
                metadata["media_encodings"] = media_encodings
            return {
                "data": read_data_ranges("data", ranges),
                "row_offsets": gathered_row_offsets,
                "byte_offsets": gathered_byte_offsets,
                "nulls": read_member_indices("nulls", indices),
            }, metadata

        raise ValueError(f"unknown structured non-tensor codec: {codec}")

    def _read_structured_non_tensor_payload_slice(
        self,
        stage_ref: RemoteBundleRef,
        encoded: Mapping[str, Any],
        row_slice: StructuredMemberSlice,
        total_rows: int,
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        start, end, step = _normalized_member_slice(row_slice, total_rows)
        if step != 1:
            raise ValueError(
                "DataProto structured non-tensor slicing currently requires step=1"
            )
        codec = encoded["codec"]
        payload_members = encoded["payload_members"]
        manifest = self._bundle_store.resolve_manifest(stage_ref)
        metadata = dict(encoded.get("metadata") or {})

        def read_member(payload_name: str, row_start: int, row_end: int) -> Any:
            member = payload_members[payload_name]
            spec = (
                self.read_spec(stage_ref)
                .select_members([member])
                .slice_member(member, axis=0, start=row_start, end=row_end)
            )
            return self.materialize(spec).objects[member]

        def read_bytes(payload_name: str, byte_start: int, byte_end: int) -> bytes:
            member = payload_members[payload_name]
            payload_spec = manifest["buffers"][member]
            return self._bundle_store.read_payload_range(
                payload_spec, byte_start, byte_end - byte_start
            )

        if _is_recursive_encoded_non_tensor(encoded):
            metadata = _copy_recursive_metadata_for_leaf_updates(metadata)
            recursive_payload: dict[str, Any] = {}
            for node in metadata.get("nodes", []):
                for key in ("missing_payload", "row_mask_payload", "lengths_payload"):
                    payload_name = node.get(key)
                    if payload_name is not None:
                        recursive_payload[payload_name] = read_member(payload_name, start, end)
            for leaf in metadata.get("leaves", []):
                leaf_payload_members = leaf["payload_members"]
                flat_payload_members = {
                    name: payload_members[global_name]
                    for name, global_name in leaf_payload_members.items()
                    if name != "missing"
                }
                leaf_payload, _leaf_metadata = self._read_structured_non_tensor_payload_slice(
                    stage_ref,
                    {
                        "codec": leaf["codec"],
                        "metadata": leaf.get("metadata") or {},
                        "payload_members": flat_payload_members,
                    },
                    row_slice,
                    total_rows,
                )
                leaf["metadata"] = _leaf_metadata
                for name, value in leaf_payload.items():
                    recursive_payload[leaf_payload_members[name]] = value
                recursive_payload[leaf_payload_members["missing"]] = read_member(
                    leaf_payload_members["missing"], start, end
                )
            return recursive_payload, metadata

        if codec == "ndarray":
            return {
                "data": read_member("data", start, end),
                "nulls": read_member("nulls", start, end),
            }, metadata

        if codec in {"ragged_tensor", "typed_ragged"}:
            offsets = read_member("offsets", start, end + 1)
            base = int(offsets[0])
            limit = int(offsets[-1])
            offsets = offsets - base
            return {
                "data": read_member("data", base, limit),
                "offsets": offsets,
                "shapes": read_member("shapes", start, end),
                "ndims": read_member("ndims", start, end),
                "nulls": read_member("nulls", start, end),
            }, metadata

        if codec in {"media_bytes", "bytes_ragged", "utf8_ragged", "msgpack_ragged", "json_ragged"}:
            offsets = read_member("offsets", start, end + 1)
            base = int(offsets[0])
            limit = int(offsets[-1])
            offsets = offsets - base
            if "media_encodings" in metadata:
                metadata["media_encodings"] = metadata["media_encodings"][start:end]
            return {
                "data": read_bytes("data", base, limit),
                "offsets": offsets,
                "nulls": read_member("nulls", start, end),
            }, metadata

        if codec == "media_list_ragged":
            row_offsets = read_member("row_offsets", start, end + 1)
            item_start = int(row_offsets[0])
            item_end = int(row_offsets[-1])
            row_offsets = row_offsets - item_start
            byte_offsets = read_member("byte_offsets", item_start, item_end + 1)
            byte_start = int(byte_offsets[0])
            byte_end = int(byte_offsets[-1])
            byte_offsets = byte_offsets - byte_start
            if "media_encodings" in metadata:
                metadata["media_encodings"] = metadata["media_encodings"][
                    item_start:item_end
                ]
            return {
                "data": read_bytes("data", byte_start, byte_end),
                "row_offsets": row_offsets,
                "byte_offsets": byte_offsets,
                "nulls": read_member("nulls", start, end),
            }, metadata

        raise ValueError(f"unknown structured non-tensor codec: {codec}")

    def cleanup_dataproto(self, ref: DataProtoRefLike) -> None:
        """Remove all structured object stages referenced by a DataProto handle."""
        ref = _resolve_dataproto_ref(ref)
        seen: set[str] = set()
        for stage_ref in ref.stage_refs.values():
            if stage_ref.manifest_key in seen:
                continue
            seen.add(stage_ref.manifest_key)
            self.remove_bundle(stage_ref)

    def _append_dataproto_stage_manifest(
        self,
        old_stage_ref: RemoteBundleRef,
        payload: StructuredObjectPayload,
        *,
        partition: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
    ) -> RemoteBundleRef:
        new_stage_ref = self.put_structured_object(
            payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
        )
        try:
            old_manifest = self._bundle_store.resolve_manifest(old_stage_ref)
            new_manifest = self._bundle_store.resolve_manifest(new_stage_ref)
            old_metadata = _decode_structured_metadata(
                self._bundle_store.read_payload(old_manifest["meta"])
            )
            new_metadata = _decode_structured_metadata(
                self._bundle_store.read_payload(new_manifest["meta"])
            )
            merged_metadata = _merge_structured_stage_metadata(
                old_metadata, new_metadata
            )
            merged_buffers = dict(old_manifest["buffers"])
            collisions = sorted(set(merged_buffers) & set(new_manifest["buffers"]))
            if collisions:
                raise ValueError(f"structured members already exist: {collisions}")
            merged_buffers.update(new_manifest["buffers"])
            merged_ref = self._bundle_store.put_bundle_manifest(
                _encode_structured_metadata(merged_metadata),
                merged_buffers,
                partition=partition,
                chunk_bytes=chunk_bytes,
                policy=policy,
                cleanup_keys=[
                    self._bundle_store.manifest_key(old_stage_ref),
                    *self._bundle_store.payload_keys(old_manifest["meta"]),
                ],
            )
        except Exception:
            self.remove_bundle(new_stage_ref)
            raise
        obsolete_keys = [
            self._bundle_store.manifest_key(new_stage_ref),
            *self._bundle_store.payload_keys(new_manifest["meta"]),
        ]
        self._bundle_store.remove_keys(obsolete_keys, strict=False)
        return merged_ref

    def _put_dataproto_stage(
        self,
        ref: MooncakeDataProtoRef | None,
        data: Any,
        *,
        namespace: str,
        partition: str,
        stage: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
        overwrite: bool,
    ) -> MooncakeDataProtoRef:
        batch, non_tensor_batch, meta_info = _split_dataproto_like(data)
        batch_size = _dataproto_batch_size(batch, non_tensor_batch)
        if ref is not None and batch_size != ref.batch_size:
            raise ValueError(
                f"DataProto append batch size {batch_size} does not match ref batch size {ref.batch_size}"
            )
        cross_section_fields = sorted(set(batch) & set(non_tensor_batch))
        if cross_section_fields:
            raise ValueError(
                f"DataProto batch and non_tensor_batch fields overlap: {cross_section_fields}"
            )
        buffers: dict[str, Any] = {}
        field_updates: dict[str, StructuredFieldLocation] = {}
        for name, value in batch.items():
            member = f"batch.{name}"
            buffers[member] = value
            field_updates[name] = StructuredFieldLocation(stage, member, "batch")
        encoded_updates: dict[str, Any] = {}
        for name, value in non_tensor_batch.items():
            if _should_encode_non_tensor_field(value):
                encoded = _encode_structured_non_tensor_field(
                    f"non_tensor_batch.{name}", value
                )
                payload_members: dict[str, str] = {}
                for payload_name, payload_value in encoded.payload.items():
                    member = f"non_tensor_batch.{name}.{payload_name}"
                    buffers[member] = payload_value
                    payload_members[payload_name] = member
                field_updates[name] = StructuredFieldLocation(
                    stage, f"non_tensor_batch.{name}", "non_tensor_batch"
                )
                encoded_updates[name] = {
                    "codec": encoded.codec,
                    "rows": encoded.rows,
                    "metadata": encoded.metadata,
                    "payload_members": payload_members,
                }
                continue
            member = f"non_tensor_batch.{name}"
            buffers[member] = value
            field_updates[name] = StructuredFieldLocation(
                stage, member, "non_tensor_batch"
            )
        if not buffers:
            if ref is not None:
                merged_meta_info = dict(ref.meta_info)
                merged_meta_info.update(meta_info)
                return MooncakeDataProtoRef(
                    batch_size=ref.batch_size,
                    stage_refs=dict(ref.stage_refs),
                    field_index=dict(ref.field_index),
                    meta_info=merged_meta_info,
                    namespace=ref.namespace,
                    partition=ref.partition,
                    global_indexes=ref.global_indexes,
                    encoded_non_tensor=dict(ref.encoded_non_tensor),
                )
            return MooncakeDataProtoRef(
                batch_size=batch_size,
                stage_refs={},
                field_index={},
                meta_info=dict(meta_info),
                namespace=namespace,
                partition=partition,
                encoded_non_tensor={},
            )
        duplicates = (
            sorted(set(ref.field_index) & set(field_updates)) if ref is not None else []
        )
        if duplicates and not overwrite:
            raise ValueError(f"DataProto fields already exist: {duplicates}")
        if ref is not None and overwrite:
            dangling = [
                name
                for name, location in ref.field_index.items()
                if location.stage == stage and name not in field_updates
            ]
            if dangling:
                raise ValueError(
                    f"DataProto overwrite for stage {stage!r} must include existing fields: {sorted(dangling)}"
                )
        payload = StructuredObjectPayload(
            metadata={
                "layout": "dataproto_stage",
                "dataproto": {
                    "version": 1,
                    "namespace": namespace,
                    "partition": partition,
                    "stage": stage,
                    "batch_size": batch_size,
                },
            },
            buffers=buffers,
        )
        existing_stage_ref = ref.stage_refs.get(stage) if ref is not None else None
        if existing_stage_ref is not None and not overwrite:
            stage_ref = self._append_dataproto_stage_manifest(
                existing_stage_ref,
                payload,
                partition=partition,
                chunk_bytes=chunk_bytes,
                policy=policy,
            )
        else:
            stage_ref = self.put_structured_object(
                payload,
                partition=partition,
                chunk_bytes=chunk_bytes,
                policy=policy,
            )
            if (
                existing_stage_ref is not None
                and overwrite
                and existing_stage_ref.manifest_key != stage_ref.manifest_key
            ):
                self.remove_bundle(existing_stage_ref)
        if ref is None:
            stage_refs = {stage: stage_ref}
            field_index = dict(field_updates)
            merged_meta_info = dict(meta_info)
            encoded_non_tensor = dict(encoded_updates)
        else:
            stage_refs = dict(ref.stage_refs)
            stage_refs[stage] = stage_ref
            field_index = dict(ref.field_index)
            field_index.update(field_updates)
            merged_meta_info = dict(ref.meta_info)
            merged_meta_info.update(meta_info)
            encoded_non_tensor = dict(ref.encoded_non_tensor)
            for name in field_updates:
                encoded_non_tensor.pop(name, None)
            encoded_non_tensor.update(encoded_updates)
        return MooncakeDataProtoRef(
            batch_size=batch_size,
            stage_refs=stage_refs,
            field_index=field_index,
            meta_info=merged_meta_info,
            namespace=namespace,
            partition=partition,
            global_indexes=None if ref is None else ref.global_indexes,
            encoded_non_tensor=encoded_non_tensor,
        )


def _copy_transfer_policy(policy: BundleTransferPolicy) -> BundleTransferPolicy:
    if policy.copy_mode == "copy":
        return policy
    return BundleTransferPolicy(
        max_inflight_put=policy.max_inflight_put,
        put_mode=policy.put_mode,
        copy_mode="copy",
    )


def _dataproto_manifest_view(
    transfer: MooncakeBundleTransfer, ref: MooncakeDataProtoRef
) -> dict[str, Any]:
    stage_manifests = {
        stage: transfer._bundle_store.resolve_manifest(stage_ref)
        for stage, stage_ref in ref.stage_refs.items()
    }
    stage_metadata = {
        stage: _decode_structured_metadata(
            transfer._bundle_store.read_payload(stage_manifest["meta"])
        )
        for stage, stage_manifest in stage_manifests.items()
    }
    fields: dict[str, dict[str, Any]] = {}
    for name, location in ref.field_index.items():
        metadata = stage_metadata[location.stage]
        field_specs = _structured_field_specs(metadata)
        member_spec = dict(field_specs.get(location.member, {"encoding": "bytes"}))
        encoded = ref.encoded_non_tensor.get(name)
        if encoded is not None:
            member_spec = {
                "encoding": "structured_non_tensor",
                "codec": encoded["codec"],
                "metadata": encoded.get("metadata"),
                "payload_members": dict(encoded["payload_members"]),
                "payload_specs": {
                    payload_name: dict(field_specs.get(member, {"encoding": "bytes"}))
                    for payload_name, member in encoded["payload_members"].items()
                },
            }
        fields[name] = {
            "section": location.section,
            "stage": location.stage,
            "member": location.member,
            "spec": member_spec,
        }
    return {
        "namespace": ref.namespace,
        "partition": ref.partition,
        "batch_size": ref.batch_size,
        "batch_fields": {
            name: info for name, info in fields.items() if info["section"] == "batch"
        },
        "non_tensor_fields": {
            name: info
            for name, info in fields.items()
            if info["section"] == "non_tensor_batch"
        },
        "meta_info_keys": list(ref.meta_info),
        "stages": {
            stage: {
                "manifest_key": stage_ref.manifest_key,
                "dataproto": stage_metadata[stage].get("dataproto", {}),
            }
            for stage, stage_ref in ref.stage_refs.items()
        },
    }


def _build_dataproto_like_result(
    batch: dict[str, Any],
    non_tensor_batch: dict[str, Any],
    meta_info: dict[str, Any],
    data_cls: Optional[Any],
) -> Any:
    payload = {
        "batch": batch,
        "non_tensor_batch": non_tensor_batch,
        "meta_info": meta_info,
    }
    if data_cls is None or data_cls is dict:
        return payload
    from_dict = getattr(data_cls, "from_dict", None)
    try:
        if callable(from_dict):
            return from_dict(batch, non_tensor_batch, meta_info=meta_info)
        return data_cls(
            batch=batch, non_tensor_batch=non_tensor_batch, meta_info=meta_info
        )
    except TypeError as error:
        raise TypeError(
            f"{getattr(data_cls, '__name__', data_cls)!r} cannot be constructed from "
            "batch, non_tensor_batch, and meta_info"
        ) from error


def _split_dataproto_like(
    data: Any,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if isinstance(data, Mapping):
        if _is_dataproto_envelope_mapping(data):
            return (
                _mapping_to_dict(data.get("batch")),
                _mapping_to_dict(data.get("non_tensor_batch")),
                _mapping_to_dict(data.get("meta_info")),
            )
        return dict(data), {}, {}
    batch = _mapping_to_dict(getattr(data, "batch", None))
    non_tensor_batch = _mapping_to_dict(getattr(data, "non_tensor_batch", None))
    meta_info = _mapping_to_dict(getattr(data, "meta_info", None))
    return batch, non_tensor_batch, meta_info


def _is_dataproto_envelope_mapping(data: Mapping[str, Any]) -> bool:
    envelope_keys = {"batch", "non_tensor_batch", "meta_info"}
    if not data or not set(data).issubset(envelope_keys):
        return False
    return all(_is_mapping_like_or_none(value) for value in data.values())


def _is_mapping_like_or_none(value: Any) -> bool:
    return (
        value is None
        or isinstance(value, Mapping)
        or callable(getattr(value, "items", None))
    )


def _mapping_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    items = getattr(value, "items", None)
    if callable(items):
        return dict(items())
    raise TypeError(f"expected mapping-like value, got {type(value).__name__}")


def _dataproto_batch_size(
    batch: Mapping[str, Any], non_tensor_batch: Mapping[str, Any]
) -> int:
    sizes = {
        len(value) for values in (batch, non_tensor_batch) for value in values.values()
    }
    if not sizes:
        return 0
    if len(sizes) != 1:
        raise ValueError(
            f"DataProto fields have inconsistent batch sizes: {sorted(sizes)}"
        )
    return sizes.pop()


def _resolve_dataproto_field_selection(
    ref: MooncakeDataProtoRef,
    fields: Optional[Sequence[str]],
    batch_fields: Optional[Sequence[str]],
    non_tensor_fields: Optional[Sequence[str]],
) -> tuple[list[str], list[str]]:
    if fields is not None and (
        batch_fields is not None or non_tensor_fields is not None
    ):
        raise ValueError(
            "fields cannot be combined with batch_fields or non_tensor_fields"
        )
    if fields is not None:
        requested = list(fields)
        _validate_dataproto_fields_exist(ref, requested)
        return (
            [name for name in requested if ref.field_index[name].section == "batch"],
            [
                name
                for name in requested
                if ref.field_index[name].section == "non_tensor_batch"
            ],
        )
    if batch_fields is None and non_tensor_fields is None:
        batch_names = [
            name
            for name, location in ref.field_index.items()
            if location.section == "batch"
        ]
        non_tensor_names = [
            name
            for name, location in ref.field_index.items()
            if location.section == "non_tensor_batch"
        ]
    else:
        batch_names = [] if batch_fields is None else list(batch_fields)
        non_tensor_names = (
            [] if non_tensor_fields is None else list(non_tensor_fields)
        )
    _validate_dataproto_fields_exist(ref, [*batch_names, *non_tensor_names])
    return batch_names, non_tensor_names


def _validate_dataproto_fields_exist(
    ref: MooncakeDataProtoRef, names: Sequence[str]
) -> None:
    missing = [name for name in names if name not in ref.field_index]
    if missing:
        raise KeyError(f"unknown DataProto fields: {missing}")


def _coerce_dataproto_row_selection(
    rows: slice | StructuredMemberSlice | Sequence[int] | None, total_rows: int
) -> _DataProtoRowSelection | None:
    if rows is None:
        return None
    if isinstance(rows, StructuredMemberSlice):
        if rows.axis != 0:
            raise ValueError("DataProto row slicing currently supports axis=0 only")
        _normalized_member_slice(rows, total_rows)
        return _DataProtoRowSelection(
            count=_slice_length(rows, total_rows), member_slice=rows
        )
    if isinstance(rows, slice):
        start, end, step = rows.indices(total_rows)
        if step <= 0:
            raise ValueError("DataProto row slicing step must be positive")
        member_slice = StructuredMemberSlice(axis=0, start=start, end=end, step=step)
        return _DataProtoRowSelection(
            count=_slice_length(member_slice, total_rows), member_slice=member_slice
        )
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes, bytearray)):
        indices = tuple(_normalize_dataproto_row_index(index, total_rows) for index in rows)
        return _DataProtoRowSelection(count=len(indices), indices=indices)
    raise TypeError("DataProto rows must be a slice, StructuredMemberSlice, or row index sequence")


def _normalize_dataproto_row_index(index: Any, total_rows: int) -> int:
    if not isinstance(index, (int, np.integer)):
        raise TypeError("DataProto row indices must be integers")
    normalized = int(index)
    if normalized < 0:
        normalized += total_rows
    if normalized < 0 or normalized >= total_rows:
        raise IndexError(f"DataProto row index {index} out of range for {total_rows} rows")
    return normalized


def _slice_length(member_slice: StructuredMemberSlice, total_rows: int) -> int:
    start, end, step = _normalized_member_slice(member_slice, total_rows)
    if start >= end:
        return 0
    return 1 + (end - 1 - start) // step


def _select_mapping(
    value: Mapping[str, Any], keys: Optional[Sequence[str]]
) -> dict[str, Any]:
    if keys is None:
        return dict(value)
    return {key: value[key] for key in keys if key in value}


def _should_encode_non_tensor_field(value: Any) -> bool:
    return isinstance(value, np.ndarray) and value.dtype == object


def _encode_structured_non_tensor_field(
    path: str, value: np.ndarray
) -> _EncodedStructuredLeaf:
    values = list(value)
    leaves: list[_InferredLeaf] = []
    nodes: list[_InferredNode] = []
    infer_structure(path, values, leaves, nodes)
    if nodes and _should_encode_recursive_structure(leaves):
        return _encode_recursive_structured_non_tensor_field(path, values, leaves, nodes)
    decision = _choose_leaf_codec(values)
    return _encode_structured_leaf(values, decision)


def _should_encode_recursive_structure(leaves: Sequence[_InferredLeaf]) -> bool:
    return any(_should_encode_recursive_leaf(leaf) for leaf in leaves)


def _should_encode_recursive_leaf(leaf: _InferredLeaf) -> bool:
    codec = leaf.decision.codec
    if codec in {"ragged_tensor", "bytes_ragged", "media_bytes", "media_list_ragged"}:
        return True
    if codec == "typed_ragged":
        return any(isinstance(value, np.ndarray) for value in _non_null(leaf.values))
    return False


def _recursive_leaf_decision(values: list[Any], decision: _CodecDecision) -> _CodecDecision:
    if decision.accepted:
        return decision
    if all(value is None or isinstance(value, _Missing) for value in values):
        return _CodecDecision(
            True,
            "json_ragged",
            "all rows are null or missing",
            "json",
        )
    return decision


def _encode_recursive_structured_non_tensor_field(
    path: str,
    values: list[Any],
    leaves: Sequence[_InferredLeaf],
    nodes: Sequence[_InferredNode],
) -> _EncodedStructuredLeaf:
    payload: dict[str, Any] = {}
    node_specs: list[dict[str, Any]] = []
    for node_id, node in enumerate(nodes):
        spec: dict[str, Any] = {
            "id": node_id,
            "path": node.path,
            "node_type": node.node_type,
            "children": list(node.children),
        }
        missing_payload_name = f"node.{node_id}.missing"
        payload[missing_payload_name] = np.asarray(
            [_lookup_structured_path(value, path, node.path) is MISSING for value in values],
            dtype=np.bool_,
        )
        spec["missing_payload"] = missing_payload_name
        if node.row_mask is not None:
            payload_name = f"node.{node_id}.row_mask"
            payload[payload_name] = np.asarray(node.row_mask, dtype=np.bool_)
            spec["row_mask_payload"] = payload_name
        if node.lengths is not None:
            payload_name = f"node.{node_id}.lengths"
            payload[payload_name] = np.asarray(node.lengths, dtype=np.int64)
            spec["lengths_payload"] = payload_name
        node_specs.append(spec)

    leaf_specs: list[dict[str, Any]] = []
    for leaf_id, leaf in enumerate(leaves):
        missing = np.asarray(
            [isinstance(value, _Missing) for value in leaf.values], dtype=np.bool_
        )
        codec_values = [None if is_missing else value for is_missing, value in zip(missing, leaf.values)]
        decision = _recursive_leaf_decision(leaf.values, leaf.decision)
        encoded = _encode_structured_leaf(codec_values, decision)
        leaf_payload_members: dict[str, str] = {}
        for payload_name, payload_value in encoded.payload.items():
            recursive_payload_name = f"leaf.{leaf_id}.{payload_name}"
            payload[recursive_payload_name] = payload_value
            leaf_payload_members[payload_name] = recursive_payload_name
        missing_payload_name = f"leaf.{leaf_id}.missing"
        payload[missing_payload_name] = missing
        leaf_payload_members["missing"] = missing_payload_name
        leaf_specs.append(
            {
                "id": leaf_id,
                "path": leaf.path,
                "codec": encoded.codec,
                "rows": encoded.rows,
                "metadata": encoded.metadata,
                "payload_members": leaf_payload_members,
            }
        )

    return _EncodedStructuredLeaf(
        codec="structured_recursive",
        rows=len(values),
        payload=payload,
        metadata={
            "schema_source": "inferred_from_runtime_values",
            "structure_version": 1,
            "root_path": path,
            "nodes": node_specs,
            "leaves": leaf_specs,
        },
    )


def _is_recursive_encoded_non_tensor(encoded: Mapping[str, Any]) -> bool:
    return encoded.get("codec") == "structured_recursive"


def _structured_path_tokens(path: str) -> list[Any]:
    tokens: list[Any] = []
    index = 0
    current = []
    while index < len(path):
        char = path[index]
        if char == "\\":
            index += 1
            if index < len(path):
                current.append(path[index])
        elif char == ".":
            tokens.append("".join(current))
            current = []
        elif char == "[":
            if current:
                tokens.append("".join(current))
                current = []
            end = path.index("]", index)
            tokens.append(int(path[index + 1 : end]))
            index = end
        else:
            current.append(char)
        index += 1
    if current:
        tokens.append("".join(current))
    return tokens


def _lookup_structured_path(value: Any, root_path: str, path: str) -> Any:
    root_tokens = _structured_path_tokens(root_path)
    path_tokens = _structured_path_tokens(path)
    current = value
    for token in path_tokens[len(root_tokens) :]:
        if isinstance(token, str):
            if current is None:
                return None
            if isinstance(current, _Missing) or not isinstance(current, dict):
                return MISSING
            current = current.get(token, MISSING)
            continue
        if current is None:
            return None
        if isinstance(current, _Missing) or not isinstance(current, (list, tuple)):
            return MISSING
        current = current[token] if token < len(current) else MISSING
    return current


def _encode_structured_leaf(
    values: list[Any], decision: _CodecDecision
) -> _EncodedStructuredLeaf:
    if not decision.accepted:
        raise ValueError(f"unsupported structured non-tensor field: {decision.reason}")
    codec = decision.codec
    if codec == "ragged_tensor":
        payload, metadata = _encode_ragged_tensor_values(values)
    elif codec == "typed_ragged":
        payload, metadata = _encode_typed_ragged_values(values)
    elif codec == "media_list_ragged":
        payload, metadata = _encode_media_list_values(values)
    elif codec == "ndarray":
        payload, metadata = _encode_numeric_scalar_values(values, decision)
    elif codec == "bytes_ragged":
        payload, metadata = _encode_bytes_like_values(values)
    elif codec == "media_bytes":
        payload, metadata = _encode_bytes_like_values(values)
    elif codec == "utf8_ragged":
        payload, metadata = _encode_bytes_like_values(
            [None if value is None else value.encode("utf-8") for value in values]
        )
    elif codec == "msgpack_ragged":
        payload, metadata = _encode_bytes_like_values(
            [
                None
                if value is None
                else _msgpack.packb(value, use_bin_type=True, strict_types=True)
                for value in values
            ]
        )
    elif codec == "json_ragged":
        payload, metadata = _encode_bytes_like_values(
            [
                None
                if value is None
                else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
                for value in values
            ]
        )
    else:
        raise ValueError(f"unsupported structured non-tensor codec: {codec}")
    metadata.update(
        {
            "schema_source": "inferred_from_runtime_values"
            if decision.accepted
            else "fallback",
            "decision_reason": decision.reason,
            "normalized_type": decision.normalized_type,
            **decision.metadata,
        }
    )
    return _EncodedStructuredLeaf(
        codec=codec, rows=len(values), payload=payload, metadata=metadata
    )



def _decode_structured_non_tensor_encoded(
    encoded: Mapping[str, Any],
    payload: dict[str, Any],
    rows: int,
    metadata: Optional[Mapping[str, Any]] = None,
) -> list[Any]:
    if _is_recursive_encoded_non_tensor(encoded):
        return _decode_structured_recursive_field(encoded, payload, rows)
    return _decode_structured_leaf(encoded["codec"], payload, rows, metadata)


def _copy_recursive_metadata_for_leaf_updates(
    metadata: Mapping[str, Any]
) -> dict[str, Any]:
    copied = dict(metadata)
    copied["leaves"] = [dict(leaf) for leaf in metadata.get("leaves", [])]
    return copied


def _decode_structured_recursive_field(
    encoded: Mapping[str, Any], payload: dict[str, Any], rows: int
) -> list[Any]:
    metadata = encoded.get("metadata") or {}
    leaf_values: dict[str, list[Any]] = {}
    for leaf in metadata.get("leaves", []):
        leaf_payload_members = leaf["payload_members"]
        leaf_payload = {
            name: payload[global_name]
            for name, global_name in leaf_payload_members.items()
            if name != "missing"
        }
        values = _decode_structured_leaf(
            leaf["codec"], leaf_payload, rows, leaf.get("metadata")
        )
        missing = payload[leaf_payload_members["missing"]]
        leaf_values[leaf["path"]] = [
            MISSING if bool(missing[row]) else values[row] for row in range(rows)
        ]
    return _reconstruct_structured_rows(metadata, payload, leaf_values, rows)


def _child_path(node: Mapping[str, Any], child: Any) -> str:
    if node["node_type"] == "dict":
        return f"{node['path']}.{_escape_key(str(child))}"
    return f"{node['path']}[{int(child)}]"


def _path_depth(path: str) -> int:
    return path.count(".") + path.count("[")


def _reconstruct_structured_rows(
    metadata: Mapping[str, Any],
    payload: Mapping[str, Any],
    leaf_values: Mapping[str, list[Any]],
    rows: int,
) -> list[Any]:
    values_by_path: dict[str, list[Any]] = dict(leaf_values)
    nodes = sorted(
        metadata.get("nodes", []), key=lambda node: _path_depth(node["path"]), reverse=True
    )
    for node in nodes:
        missing_payload = node.get("missing_payload")
        missing = payload[missing_payload] if missing_payload is not None else None
        row_mask_payload = node.get("row_mask_payload")
        row_mask = payload[row_mask_payload] if row_mask_payload is not None else None
        lengths_payload = node.get("lengths_payload")
        lengths = payload[lengths_payload] if lengths_payload is not None else None
        node_values = []
        for row in range(rows):
            if missing is not None and bool(missing[row]):
                node_values.append(MISSING)
                continue
            if row_mask is not None and not bool(row_mask[row]):
                node_values.append(None)
                continue
            if node["node_type"] == "dict":
                item = {}
                for child in node["children"]:
                    child_values = values_by_path[_child_path(node, child)]
                    child_value = child_values[row]
                    if not isinstance(child_value, _Missing):
                        item[child] = child_value
                node_values.append(item)
                continue
            length = int(lengths[row]) if lengths is not None else len(node["children"])
            item = [None] * length
            for child in node["children"]:
                index = int(child)
                if index >= length:
                    continue
                child_values = values_by_path[_child_path(node, child)]
                child_value = child_values[row]
                if not isinstance(child_value, _Missing):
                    item[index] = child_value
            node_values.append(item)
        values_by_path[node["path"]] = node_values
    return values_by_path[metadata["root_path"]]


def _decode_structured_leaf(
    codec: str,
    payload: dict[str, Any],
    rows: int,
    metadata: Optional[Mapping[str, Any]] = None,
) -> list[Any]:
    if codec == "ragged_tensor":
        return _decode_ragged_tensor_values(payload, rows)
    if codec == "typed_ragged":
        return _decode_typed_ragged_values(payload, rows)
    if codec == "media_list_ragged":
        return _decode_media_list_values(payload, rows, metadata)
    if codec == "ndarray":
        return _decode_numeric_scalar_values(payload)
    if codec == "media_bytes":
        return _decode_bytes_like_values(payload, rows, metadata)
    if codec == "bytes_ragged":
        return _decode_bytes_like_values(payload, rows)
    if codec == "utf8_ragged":
        return [
            None if value is None else value.decode("utf-8")
            for value in _decode_bytes_like_values(payload, rows)
        ]
    if codec == "msgpack_ragged":
        return [
            None if value is None else _msgpack.unpackb(value, raw=False)
            for value in _decode_bytes_like_values(payload, rows)
        ]
    if codec == "json_ragged":
        return [
            None if value is None else json.loads(value.decode("utf-8"))
            for value in _decode_bytes_like_values(payload, rows)
        ]
    raise ValueError(f"unknown structured non-tensor codec: {codec}")


def _encode_ragged_tensor_values(
    values: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if _torch is None:
        raise RuntimeError("torch is required to encode ragged tensor fields")
    tensors: list[Any] = []
    dtype = None
    max_ndim = 0
    for value in values:
        if value is None:
            tensors.append(None)
            continue
        tensor = value.detach()
        if tensor.device.type != "cpu" or not tensor.is_contiguous():
            tensor = tensor.cpu().contiguous()
        dtype = tensor.dtype if dtype is None else dtype
        if tensor.dtype != dtype:
            raise ValueError(f"mixed tensor dtype: {dtype} vs {tensor.dtype}")
        max_ndim = max(max_ndim, tensor.dim())
        tensors.append(tensor)
    offsets = _torch.zeros(len(tensors) + 1, dtype=_torch.int64)
    ndims = _torch.zeros(len(tensors), dtype=_torch.int16)
    shapes = _torch.zeros((len(tensors), max(max_ndim, 1)), dtype=_torch.int64)
    nulls = np.asarray([tensor is None for tensor in tensors], dtype=np.bool_)
    flat_parts = []
    offset = 0
    for row, tensor in enumerate(tensors):
        if tensor is None:
            offsets[row + 1] = offset
            continue
        flat = tensor.reshape(-1)
        flat_parts.append(flat)
        offset += flat.numel()
        offsets[row + 1] = offset
        ndims[row] = tensor.dim()
        if tensor.dim() > 0:
            shapes[row, : tensor.dim()] = _torch.tensor(
                list(tensor.shape), dtype=_torch.int64
            )
    data_dtype = dtype or _torch.float32
    data = (
        _torch.cat(flat_parts) if flat_parts else _torch.empty((0,), dtype=data_dtype)
    )
    payload = {
        "data": data.numpy(),
        "offsets": offsets.numpy(),
        "shapes": shapes.numpy(),
        "ndims": ndims.numpy(),
        "nulls": nulls,
    }
    return payload, {
        "dtype": str(data_dtype),
        "max_ndim": int(max_ndim),
        "shape_policy": "ragged",
    }


def _decode_ragged_tensor_values(payload: dict[str, Any], rows: int) -> list[Any]:
    if _torch is None:
        raise RuntimeError("torch is required to decode ragged tensor fields")
    data = _torch.from_numpy(payload["data"])
    offsets = payload["offsets"]
    shapes = payload["shapes"]
    ndims = payload["ndims"]
    nulls = payload["nulls"]
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        begin = int(offsets[row])
        end = int(offsets[row + 1])
        ndim = int(ndims[row])
        shape = tuple(int(v) for v in shapes[row, :ndim].tolist())
        values.append(data[begin:end].reshape(shape))
    return values


def _encode_typed_ragged_values(
    values: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_arrays = [np.asarray(value) for value in values if value is not None]
    dtype = np.result_type(*source_arrays) if source_arrays else np.dtype(np.int64)
    arrays = [
        np.asarray([], dtype=dtype)
        if value is None
        else np.ascontiguousarray(np.asarray(value, dtype=dtype))
        for value in values
    ]
    max_ndim = max((array.ndim for array in arrays), default=0)
    offsets = np.zeros(len(arrays) + 1, dtype=np.int64)
    ndims = np.zeros(len(arrays), dtype=np.int16)
    shapes = np.zeros((len(arrays), max(max_ndim, 1)), dtype=np.int64)
    nulls = np.asarray([value is None for value in values], dtype=np.bool_)
    flat_arrays = []
    offset = 0
    for row, array in enumerate(arrays):
        flat = array.reshape(-1)
        flat_arrays.append(flat)
        offset += flat.size
        offsets[row + 1] = offset
        ndims[row] = array.ndim
        if array.ndim > 0:
            shapes[row, : array.ndim] = array.shape
    data = np.concatenate(flat_arrays) if flat_arrays else np.asarray([], dtype=dtype)
    return (
        {
            "data": data,
            "offsets": offsets,
            "shapes": shapes,
            "ndims": ndims,
            "nulls": nulls,
        },
        {"dtype": str(data.dtype), "max_ndim": int(max_ndim), "shape_policy": "ragged"},
    )


def _decode_typed_ragged_values(payload: dict[str, Any], rows: int) -> list[Any]:
    data = payload["data"]
    offsets = payload["offsets"]
    shapes = payload["shapes"]
    ndims = payload["ndims"]
    nulls = payload["nulls"]
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        begin = int(offsets[row])
        end = int(offsets[row + 1])
        ndim = int(ndims[row])
        shape = tuple(int(v) for v in shapes[row, :ndim].tolist())
        values.append(data[begin:end].reshape(shape).tolist())
    return values


def _encode_numeric_scalar_values(
    values: list[Any], decision: _CodecDecision
) -> tuple[dict[str, Any], dict[str, Any]]:
    dtype = np.dtype(decision.metadata["dtype"])
    nulls = np.asarray([value is None for value in values], dtype=np.bool_)
    fill_value = False if dtype == np.dtype(np.bool_) else 0
    data = np.asarray(
        [fill_value if value is None else value for value in values], dtype=dtype
    )
    return {"data": data, "nulls": nulls}, {
        "dtype": str(data.dtype),
        "shape": list(data.shape),
    }


def _decode_numeric_scalar_values(payload: dict[str, Any]) -> list[Any]:
    data = payload["data"]
    nulls = payload["nulls"]
    if not bool(nulls.any()):
        return data.tolist()
    values = data.astype(object)
    values[nulls] = None
    return values.tolist()


def _value_to_media_bytes(value: Any) -> tuple[bytes, str | None, dict[str, Any]]:
    if value is None:
        return b"", None, {"kind": "null"}
    if _is_bytes_like(value):
        return bytes(value), None, {"kind": "bytes"}
    if _is_pil_image(value):
        image = (
            value.convert(value.mode) if getattr(value, "readonly", False) else value
        )
        return (
            image.tobytes(),
            "image/raw",
            {
                "kind": "pil_raw",
                "mode": image.mode,
                "size": list(image.size),
                "format": getattr(value, "format", None),
            },
        )
    return bytes(value), None, {"kind": "bytes"}


def _encode_bytes_like_values(
    values: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    offsets = [0]
    parts = []
    media_types = []
    encodings = []
    for value in values:
        data, media_type, encoding = _value_to_media_bytes(value)
        parts.append(data)
        media_types.append(media_type)
        encodings.append(encoding)
        offsets.append(offsets[-1] + len(data))
    payload_bytes = b"".join(parts)
    metadata: dict[str, Any] = {}
    if any(encoding.get("kind") == "pil_raw" for encoding in encodings):
        metadata["media_encodings"] = encodings
    non_null_media_types = sorted(
        {media_type for media_type in media_types if media_type}
    )
    if non_null_media_types:
        metadata["media_types"] = non_null_media_types
        metadata["encode_source"] = "raw_pixels_from_object"
    return (
        {
            "data": payload_bytes,
            "offsets": np.asarray(offsets, dtype=np.int64),
            "nulls": np.asarray([value is None for value in values], dtype=np.bool_),
        },
        metadata,
    )


def _decode_media_bytes(data: Any, encoding: Optional[Mapping[str, Any]]) -> Any:
    if not encoding or encoding.get("kind") != "pil_raw":
        return bytes(data)
    try:
        from PIL import Image
    except ImportError:
        return bytes(data)
    image = Image.frombuffer(
        encoding["mode"], tuple(encoding["size"]), data, "raw", encoding["mode"], 0, 1
    )
    image.format = encoding.get("format")
    return image


def _decode_bytes_like_values(
    payload: dict[str, Any], rows: int, metadata: Optional[Mapping[str, Any]] = None
) -> list[Any]:
    data = payload["data"]
    offsets = payload["offsets"]
    nulls = payload["nulls"]
    encodings = (metadata or {}).get("media_encodings", [])
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        item = memoryview(data)[int(offsets[row]) : int(offsets[row + 1])]
        encoding = encodings[row] if row < len(encodings) else None
        values.append(_decode_media_bytes(item, encoding))
    return values


def _encode_media_list_values(
    values: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    row_offsets = [0]
    byte_offsets = [0]
    parts = []
    media_types = []
    encodings = []
    for value in values:
        items = [] if value is None else list(value)
        for item in items:
            data, media_type, encoding = _value_to_media_bytes(item)
            parts.append(data)
            media_types.append(media_type)
            encodings.append(encoding)
            byte_offsets.append(byte_offsets[-1] + len(data))
        row_offsets.append(row_offsets[-1] + len(items))
    metadata: dict[str, Any] = {
        "encode_source": "raw_pixels_from_object",
        "media_encodings": encodings,
    }
    non_null_media_types = sorted(
        {media_type for media_type in media_types if media_type}
    )
    if non_null_media_types:
        metadata["media_types"] = non_null_media_types
    return (
        {
            "data": b"".join(parts),
            "row_offsets": np.asarray(row_offsets, dtype=np.int64),
            "byte_offsets": np.asarray(byte_offsets, dtype=np.int64),
            "nulls": np.asarray([value is None for value in values], dtype=np.bool_),
        },
        metadata,
    )


def _decode_media_list_values(
    payload: dict[str, Any], rows: int, metadata: Optional[Mapping[str, Any]] = None
) -> list[Any]:
    data = payload["data"]
    row_offsets = payload["row_offsets"]
    byte_offsets = payload["byte_offsets"]
    nulls = payload["nulls"]
    encodings = (metadata or {}).get("media_encodings", [])
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        items = []
        for item_index in range(int(row_offsets[row]), int(row_offsets[row + 1])):
            item = memoryview(data)[
                int(byte_offsets[item_index]) : int(byte_offsets[item_index + 1])
            ]
            encoding = encodings[item_index] if item_index < len(encodings) else None
            items.append(_decode_media_bytes(item, encoding))
        values.append(items)
    return values


class _StructuredObjectLayer:
    def __init__(self, bundle_store: "_BundleManifestStore") -> None:
        self._bundle_store = bundle_store

    def put_structured_object(
        self,
        payload: StructuredObjectPayload,
        partition: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
        max_inflight_put: Optional[int],
        pre_registered_buffers: Optional[Mapping[str, bool]],
    ) -> RemoteBundleRef:
        metadata, buffers = _encode_structured_fields(payload.metadata, payload.buffers)
        transfer_policy = self._bundle_store._policy(
            policy, max_inflight_put=max_inflight_put
        )
        if transfer_policy.copy_mode != "copy":
            _validate_pre_registered_structured_buffers(
                payload.buffers, buffers, pre_registered_buffers
            )
        return self._bundle_store.put_bundle(
            meta=_encode_structured_metadata(metadata),
            buffers=buffers,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=transfer_policy,
            max_inflight_put=None,
            pre_registered_buffers=pre_registered_buffers,
        )

    def read_spec(
        self, ref: RemoteBundleRef | Mapping[str, Any]
    ) -> StructuredObjectReadSpec:
        return StructuredObjectReadSpec(ref=ref)

    def materialize(self, spec: StructuredObjectReadSpec) -> StructuredObjectResult:
        return self.materialize_into(spec, destinations=None)

    def materialize_into(
        self,
        spec: StructuredObjectReadSpec,
        destinations: Optional[Mapping[str, Any]],
    ) -> StructuredObjectResult:
        metadata, buffers, selected = self._resolve_structured_read(spec)
        field_specs = _structured_field_specs(metadata)
        destination_map = destinations or {}
        member_slices = dict(spec.member_slices)
        objects = {
            name: self._read_structured_member(
                name=name,
                payload_spec=buffers[name],
                field_spec=field_specs.get(name, {"encoding": "bytes"}),
                member_slice=member_slices.get(name),
                destination=destination_map.get(name),
            )
            for name in selected
        }
        return StructuredObjectResult(metadata=dict(metadata), objects=objects)

    def _resolve_structured_read(
        self,
        spec: StructuredObjectReadSpec,
    ) -> tuple[dict[str, Any], Mapping[str, Any], list[str]]:
        if spec.member_names is not None and not spec.member_names:
            raise ValueError("structured read spec selected no members")
        manifest = self._bundle_store.resolve_manifest(spec.ref)
        metadata = _decode_structured_metadata(
            self._bundle_store.read_payload(manifest["meta"])
        )
        buffers = manifest["buffers"]
        selected = (
            list(buffers) if spec.member_names is None else list(spec.member_names)
        )
        missing = [name for name in selected if name not in buffers]
        if missing:
            raise KeyError(f"unknown bundle buffers: {missing}")
        return metadata, buffers, selected

    def _read_structured_member(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        member_slice: StructuredMemberSlice | None,
        destination: Any,
    ) -> Any:
        encoding = field_spec.get("encoding", "bytes")
        if encoding == "bytes":
            return self._read_bytes_member(
                name, payload_spec, member_slice, destination
            )
        if encoding == "torch_tensor":
            return self._read_torch_tensor_member(
                name, payload_spec, field_spec, member_slice, destination
            )
        if encoding != "ndarray":
            raise ValueError(f"unsupported structured field encoding: {encoding}")
        return self._read_ndarray_member(
            name, payload_spec, field_spec, member_slice, destination
        )

    def _read_torch_tensor_member(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        member_slice: StructuredMemberSlice | None,
        destination: Any,
    ) -> Any:
        if member_slice is not None:
            return self._read_sliced_torch_tensor_member(
                name, payload_spec, field_spec, member_slice, destination
            )
        if destination is not None:
            if not isinstance(destination, (_TensorObjectBufferPayload, _RawDestinationBuffer)):
                raise ValueError(
                    f"structured tensor member {name} only supports tensor_object_buffer or raw_destination destinations"
                )
            materialized = self._bundle_store.read_tensor_payload_into(
                payload_spec, destination
            )
            return destination if materialized is None else materialized
        if payload_spec.get("kind") == "tensor":
            return self._bundle_store.read_tensor_payload(payload_spec)
        if payload_spec.get("format") == "torch_save":
            return _deserialize_torch_save_payload(
                self._bundle_store.read_payload(payload_spec)
            )
        return _deserialize_tensor_payload(
            self._bundle_store.read_payload(payload_spec)
        )

    def _read_sliced_torch_tensor_member(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        member_slice: StructuredMemberSlice,
        destination: Any,
    ) -> Any:
        metadata_bytes = int(payload_spec.get("metadata_bytes", -1))
        shape = field_spec.get("shape")
        element_size = int(field_spec.get("element_size", 0))
        if metadata_bytes < 0 or not isinstance(shape, list) or element_size <= 0:
            raise ValueError(
                f"structured tensor field {name} is missing slice metadata"
            )
        if member_slice.axis != 0:
            raise ValueError("structured tensor slicing currently supports axis=0 only")
        if not shape:
            raise ValueError(
                "structured tensor slicing requires at least one dimension"
            )
        start, end, step = _normalized_member_slice(member_slice, int(shape[0]))
        if step != 1:
            raise ValueError("structured tensor slicing currently requires step=1")
        row_width = element_size * int(np.prod(shape[1:], dtype=np.int64))
        data_offset = metadata_bytes + start * row_width
        data_length = (end - start) * row_width
        metadata = self._bundle_store.read_payload_range(
            payload_spec, 0, metadata_bytes
        )
        sliced_metadata = _slice_tensor_metadata(
            metadata, (end - start, *shape[1:]), data_length
        )
        if destination is not None:
            if not isinstance(destination, (_TensorObjectBufferPayload, _RawDestinationBuffer)):
                raise ValueError(
                    f"structured tensor member {name} only supports tensor_object_buffer or raw_destination destinations"
                )
            expected_bytes = metadata_bytes + data_length
            if destination.size < expected_bytes:
                raise ValueError(
                    f"tensor destination has {destination.size} bytes, expected at least {expected_bytes}"
                )
            ctypes.memmove(destination.ptr, sliced_metadata, metadata_bytes)
            if isinstance(destination, _RawDestinationBuffer) and destination.pre_registered:
                self._bundle_store.read_payload_range_into_raw_destination(
                    payload_spec,
                    destination.ptr,
                    metadata_bytes,
                    data_offset,
                    data_length,
                )
            else:
                data = self._bundle_store.read_payload_range(
                    payload_spec, data_offset, data_length
                )
                ctypes.memmove(destination.ptr + metadata_bytes, data, data_length)
            _ = destination.owner
            return destination
        data = self._bundle_store.read_payload_range(
            payload_spec, data_offset, data_length
        )
        return _deserialize_tensor_payload(sliced_metadata + data)

    def _read_bytes_member(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        member_slice: StructuredMemberSlice | None,
        destination: Any,
    ) -> bytes:
        if member_slice is not None:
            raise ValueError(f"structured bytes member {name} does not support slicing")
        if destination is not None:
            raise ValueError(
                f"structured bytes member {name} does not support materialize_into"
            )
        return self._bundle_store.read_payload(payload_spec)

    def _read_ndarray_member(
        self,
        name: str,
        payload_spec: Mapping[str, Any],
        field_spec: Mapping[str, Any],
        member_slice: StructuredMemberSlice | None,
        destination: Any,
    ) -> np.ndarray:
        dtype = field_spec.get("dtype")
        shape = field_spec.get("shape")
        if not isinstance(dtype, str) or not isinstance(shape, list):
            raise ValueError(
                f"structured ndarray field {name} is missing dtype or shape"
            )
        read_plan = _resolve_ndarray_read_plan(
            tuple(int(dim) for dim in shape), np.dtype(dtype), member_slice
        )
        target = _resolve_ndarray_destination(
            name, destination, read_plan.dtype, read_plan.output_shape
        )
        destination_view = target.view(np.uint8).reshape(-1)
        if read_plan.byte_length == 0:
            return target
        if read_plan.step == 1:
            self._bundle_store.read_payload_range_into_destination(
                payload_spec,
                destination_view,
                read_plan.byte_offset,
            )
            return target
        temp_shape = (read_plan.cover_row_count, *read_plan.full_shape[1:])
        temp = np.empty(temp_shape, dtype=read_plan.dtype)
        temp_view = temp.view(np.uint8).reshape(-1)
        self._bundle_store.read_payload_range_into_destination(
            payload_spec, temp_view, read_plan.byte_offset
        )
        target[...] = temp[:: read_plan.step]
        return target


class _BundleManifestStore:
    def __init__(
        self,
        store: BundleStore,
        transport: "_MooncakePayloadTransport",
        key_prefix: str,
        default_chunk_bytes: int,
    ) -> None:
        self._store = store
        self._transport = transport
        self._key_prefix = key_prefix
        self._default_chunk_bytes = default_chunk_bytes

    def put_bundle(
        self,
        meta: bytes | bytearray | memoryview,
        buffers: Mapping[str, Any],
        partition: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
        max_inflight_put: Optional[int],
        pre_registered_buffers: Optional[Mapping[str, bool]] = None,
    ) -> RemoteBundleRef:
        _validate_key_segment(partition, "partition")
        meta_view = _bytes_view(meta, "meta")
        target_chunk_bytes = _validate_chunk_bytes(
            self._default_chunk_bytes if chunk_bytes is None else chunk_bytes
        )
        transfer_policy = self._policy(policy, max_inflight_put=max_inflight_put)
        object_id = f"{partition}/{uuid.uuid4().hex}"
        base_key = f"{self._key_prefix}/{object_id}"
        manifest_key = f"{base_key}/manifest"
        written_keys: list[str] = []
        buffer_specs: dict[str, Any] = {}
        pre_registered_map = dict(pre_registered_buffers or {})
        try:
            meta_spec, meta_keys = self._put_payload(
                f"{base_key}/meta",
                meta_view,
                target_chunk_bytes,
                _copy_transfer_policy(transfer_policy),
                pre_registered=False,
            )
            written_keys.extend(meta_keys)
            for name, value in buffers.items():
                _validate_key_segment(name, "buffer name")
                payload_key = f"{base_key}/buffer/{name}"
                if isinstance(value, (_TensorPayload, _TensorObjectBufferPayload)):
                    payload_spec, payload_keys = self._put_tensor_payload(
                        payload_key,
                        value,
                        transfer_policy,
                    )
                else:
                    payload_spec, payload_keys = self._put_payload(
                        payload_key,
                        _bytes_view(value, name),
                        target_chunk_bytes,
                        transfer_policy,
                        pre_registered=bool(pre_registered_map.get(name, False)),
                    )
                buffer_specs[name] = payload_spec
                written_keys.extend(payload_keys)
            manifest = {
                "version": 1,
                "layout": "bundle",
                "object_id": object_id,
                "meta": meta_spec,
                "buffers": buffer_specs,
            }
            manifest_blob = _encode_manifest(manifest)
            _check_status(
                self._store.put(manifest_key, manifest_blob), "put", manifest_key
            )
            written_keys.append(manifest_key)
        except Exception:
            _cleanup_keys(self._store, written_keys, strict=False)
            raise
        return RemoteBundleRef(manifest_key=manifest_key, manifest=manifest)

    def put_bundle_manifest(
        self,
        meta: bytes | bytearray | memoryview,
        buffers: Mapping[str, Any],
        *,
        partition: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
        cleanup_keys: Optional[Sequence[str]] = None,
    ) -> RemoteBundleRef:
        _validate_key_segment(partition, "partition")
        meta_view = _bytes_view(meta, "meta")
        target_chunk_bytes = _validate_chunk_bytes(
            self._default_chunk_bytes if chunk_bytes is None else chunk_bytes
        )
        transfer_policy = self._policy(policy)
        object_id = f"{partition}/{uuid.uuid4().hex}"
        base_key = f"{self._key_prefix}/{object_id}"
        manifest_key = f"{base_key}/manifest"
        written_keys: list[str] = []
        try:
            meta_spec, meta_keys = self._put_payload(
                f"{base_key}/meta",
                meta_view,
                target_chunk_bytes,
                _copy_transfer_policy(transfer_policy),
                pre_registered=False,
            )
            written_keys.extend(meta_keys)
            manifest = {
                "version": 1,
                "layout": "bundle",
                "object_id": object_id,
                "meta": meta_spec,
                "buffers": dict(buffers),
                "buffer_object_ids": sorted(
                    {
                        str(payload_spec["key"])
                        .removeprefix(f"{self._key_prefix}/")
                        .split("/buffer/", 1)[0]
                        for payload_spec in buffers.values()
                    }
                ),
            }
            if cleanup_keys:
                manifest["cleanup_keys"] = list(dict.fromkeys(cleanup_keys))
            self._validate_manifest(manifest)
            manifest_blob = _encode_manifest(manifest)
            _check_status(
                self._store.put(manifest_key, manifest_blob), "put", manifest_key
            )
            written_keys.append(manifest_key)
        except Exception:
            _cleanup_keys(self._store, written_keys, strict=False)
            raise
        return RemoteBundleRef(manifest_key=manifest_key, manifest=manifest)

    def remove_bundle(self, ref: RemoteBundleRef | Mapping[str, Any]) -> None:
        manifest = self.resolve_manifest(ref)
        keys = self._payload_keys(manifest)
        keys.extend(manifest.get("cleanup_keys", []))
        keys.append(self._manifest_key(ref, manifest))
        _cleanup_keys(self._store, keys, strict=True)

    def manifest_key(self, ref: RemoteBundleRef | Mapping[str, Any]) -> str:
        return self._manifest_key(ref, self.resolve_manifest(ref))

    def payload_keys(self, payload_spec: Mapping[str, Any]) -> list[str]:
        return [chunk["key"] for chunk in payload_spec["chunks"]]

    def remove_keys(self, keys: Sequence[str], *, strict: bool) -> None:
        _cleanup_keys(self._store, keys, strict=strict)

    def resolve_manifest(
        self, ref: RemoteBundleRef | Mapping[str, Any]
    ) -> dict[str, Any]:
        if isinstance(ref, RemoteBundleRef):
            manifest = ref.manifest
            if not manifest:
                manifest = _decode_manifest(self._store.get(ref.manifest_key))
        else:
            manifest = ref.get("manifest")
            if manifest is None:
                manifest_key = ref.get("manifest_key")
                if not isinstance(manifest_key, str):
                    raise ValueError("bundle ref must include manifest_key")
                manifest = _decode_manifest(self._store.get(manifest_key))
        self._validate_manifest(manifest)
        return manifest

    def read_payload(self, payload_spec: Mapping[str, Any]) -> bytes:
        return self._transport.read_payload(payload_spec)

    def read_payload_range(
        self, payload_spec: Mapping[str, Any], byte_offset: int, byte_length: int
    ) -> bytes:
        return self._transport.read_payload_range(
            payload_spec, byte_offset, byte_length
        )

    def read_tensor_payload(self, payload_spec: Mapping[str, Any]) -> Any:
        return self._transport.read_tensor_payload(payload_spec)

    def read_tensor_payload_into(
        self, payload_spec: Mapping[str, Any], destination: _TensorObjectBufferPayload
    ) -> Any:
        return self._transport.read_tensor_payload_into(payload_spec, destination)

    def read_payload_range_into_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination: np.ndarray,
        byte_offset: int,
        destination_pre_registered: bool = False,
    ) -> None:
        self._transport.read_payload_range_into_destination(
            payload_spec,
            destination,
            byte_offset,
            destination_pre_registered=destination_pre_registered,
        )

    def read_payload_range_into_raw_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination_ptr: int,
        destination_offset: int,
        byte_offset: int,
        byte_length: int,
    ) -> None:
        if not self._transport.read_payload_range_into_raw_destination(
            payload_spec, destination_ptr, destination_offset, byte_offset, byte_length
        ):
            data = self.read_payload_range(payload_spec, byte_offset, byte_length)
            ctypes.memmove(destination_ptr + destination_offset, data, byte_length)

    def read_payload_ranges_into_array(
        self,
        payload_spec: Mapping[str, Any],
        destination: np.ndarray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        if not ranges:
            return
        self._transport.read_payload_ranges_into_array(payload_spec, destination, ranges)

    def read_payload_ranges_into_bytearray(
        self,
        payload_spec: Mapping[str, Any],
        destination: bytearray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        if not ranges:
            return
        self._transport.read_payload_ranges_into_bytearray(
            payload_spec, destination, ranges
        )

    def read_payload_ranges_into_raw_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination_ptr: int,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        if not ranges:
            return
        if self._transport.read_payload_ranges_into_raw_destination(
            payload_spec, destination_ptr, ranges
        ):
            return
        for byte_offset, destination_offset, byte_length in ranges:
            data = self.read_payload_range(payload_spec, byte_offset, byte_length)
            ctypes.memmove(destination_ptr + destination_offset, data, byte_length)

    def _put_tensor_payload(
        self,
        key: str,
        value: _TensorPayload | _TensorObjectBufferPayload,
        transfer_policy: BundleTransferPolicy,
    ) -> tuple[dict[str, Any], list[str]]:
        if isinstance(value, _TensorObjectBufferPayload):
            total_bytes = self._transport.put_tensor_object_buffer(key, value)
            return {
                "key": key,
                "bytes": total_bytes,
                "chunks": [{"key": key, "bytes": total_bytes}],
            }, [key]
        if transfer_policy.copy_mode != "copy":
            if transfer_policy.copy_mode == "zero_copy":
                raise ValueError(
                    "zero-copy structured tensor fields require a BufferPool tensor-object buffer"
                )
            tensor_spec = self._transport.put_tensor_payload_direct(key, value)
            if tensor_spec is not None:
                return tensor_spec, [key]
            try:
                total_bytes = self._transport.put_tensor_payload_from_pool(key, value)
                return {
                    "key": key,
                    "bytes": total_bytes,
                    "chunks": [{"key": key, "bytes": total_bytes}],
                }, [key]
            except RuntimeError:
                pass
        if not _has_tensor_codec_helpers() or value.tensor.dim() == 0:
            payload = _torch_save_payload_bytes(value.tensor)
            payload_spec, payload_keys = self._put_payload(
                key,
                memoryview(payload),
                len(payload) or 1,
                transfer_policy,
                pre_registered=False,
            )
            payload_spec["format"] = "torch_save"
            return payload_spec, payload_keys
        payload, metadata_bytes = _tensor_payload_bytes(value)
        payload_spec, payload_keys = self._put_payload(
            key,
            memoryview(payload),
            len(payload) or 1,
            transfer_policy,
            pre_registered=False,
        )
        payload_spec["metadata_bytes"] = metadata_bytes
        return payload_spec, payload_keys

    def _put_payload(
        self,
        key: str,
        value: memoryview,
        chunk_bytes: int,
        transfer_policy: BundleTransferPolicy,
        pre_registered: bool,
    ) -> tuple[dict[str, Any], list[str]]:
        if len(value) == 0:
            return {"key": key, "bytes": 0, "chunks": []}, []
        chunks = _split_view(value, chunk_bytes)
        chunk_keys = [
            key if len(chunks) == 1 else f"{key}/chunk/{index}"
            for index in range(len(chunks))
        ]
        written_keys = self._transport.put_payload_chunks(
            chunk_keys,
            chunks,
            transfer_policy,
            pre_registered=pre_registered,
        )
        payload_spec = {
            "key": key,
            "bytes": len(value),
            "chunks": [
                {"key": chunk_key, "bytes": len(chunk)}
                for chunk_key, chunk in zip(chunk_keys, chunks)
            ],
        }
        return payload_spec, written_keys

    def _policy(
        self,
        policy: Optional[BundleTransferPolicy],
        max_inflight_put: Optional[int] = None,
    ) -> BundleTransferPolicy:
        result = policy or BundleTransferPolicy()
        if max_inflight_put is not None:
            result = BundleTransferPolicy(
                max_inflight_put=max_inflight_put,
                put_mode=result.put_mode,
                copy_mode=result.copy_mode,
            )
        if result.max_inflight_put < 1:
            raise ValueError("max_inflight_put must be positive")
        if result.put_mode not in {"auto", "batch", "parallel"}:
            raise ValueError(f"unsupported put_mode: {result.put_mode}")
        if result.copy_mode not in {"auto", "zero_copy", "copy"}:
            raise ValueError(f"unsupported copy_mode: {result.copy_mode}")
        return result

    def _validate_manifest(self, manifest: Mapping[str, Any]) -> None:
        if manifest.get("version") != 1 or manifest.get("layout") != "bundle":
            raise ValueError("invalid bundle manifest")
        object_id = manifest.get("object_id")
        if not isinstance(object_id, str):
            raise ValueError("bundle manifest object_id must be a string")
        base_key = f"{self._key_prefix}/{object_id}"
        buffer_object_ids = manifest.get("buffer_object_ids", [object_id])
        if not isinstance(buffer_object_ids, list) or not all(
            isinstance(item, str) for item in buffer_object_ids
        ):
            raise ValueError("bundle manifest buffer_object_ids must be a list of strings")
        allowed_buffer_base_keys = [
            f"{self._key_prefix}/{buffer_object_id}"
            for buffer_object_id in buffer_object_ids
        ]
        self._validate_payload_spec(manifest.get("meta"), base_keys=[base_key])
        cleanup_keys = manifest.get("cleanup_keys", [])
        if not isinstance(cleanup_keys, list) or not all(
            self._is_allowed_cleanup_key(item, allowed_buffer_base_keys)
            for item in cleanup_keys
        ):
            raise ValueError("bundle manifest cleanup_keys are outside the bundle namespace")
        buffers = manifest.get("buffers")
        if not isinstance(buffers, dict):
            raise ValueError("bundle manifest buffers must be a dict")
        for name, payload_spec in buffers.items():
            _validate_key_segment(name, "buffer name")
            self._validate_payload_spec(
                payload_spec, base_keys=allowed_buffer_base_keys
            )

    def _is_allowed_cleanup_key(self, key: Any, base_keys: Sequence[str]) -> bool:
        if not isinstance(key, str):
            return False
        return any(
            key == f"{base_key}/manifest" or key.startswith(f"{base_key}/")
            for base_key in base_keys
        )

    def _validate_payload_spec(
        self, payload_spec: Any, base_keys: Sequence[str] | None = None
    ) -> None:
        if not isinstance(payload_spec, dict):
            raise ValueError("bundle payload spec must be a dict")
        payload_key = payload_spec.get("key")
        if not isinstance(payload_key, str) or (
            base_keys is not None
            and not any(payload_key.startswith(f"{base_key}/") for base_key in base_keys)
        ):
            raise ValueError("bundle payload key is outside the bundle namespace")
        expected_bytes = int(payload_spec.get("bytes", -1))
        if expected_bytes < 0:
            raise ValueError("bundle payload bytes must be non-negative")
        chunks = payload_spec.get("chunks")
        if not isinstance(chunks, list) or (expected_bytes and not chunks):
            raise ValueError("bundle payload chunks are invalid")
        seen_keys = set()
        total_bytes = 0
        for chunk in chunks:
            if not isinstance(chunk, dict):
                raise ValueError("bundle chunk must be a dict")
            key = chunk.get("key")
            chunk_bytes = int(chunk.get("bytes", -1))
            if not isinstance(key, str) or (
                key != payload_key and not key.startswith(f"{payload_key}/")
            ):
                raise ValueError("bundle chunk key is outside the payload namespace")
            if key in seen_keys:
                raise ValueError("bundle chunk keys must be unique")
            if chunk_bytes < 0:
                raise ValueError("bundle chunk bytes must be non-negative")
            seen_keys.add(key)
            total_bytes += chunk_bytes
        if total_bytes != expected_bytes:
            raise ValueError(
                f"bundle payload chunks total {total_bytes} bytes, expected {expected_bytes}"
            )

    def _manifest_key(
        self, ref: RemoteBundleRef | Mapping[str, Any], manifest: Mapping[str, Any]
    ) -> str:
        expected = f"{self._key_prefix}/{manifest['object_id']}/manifest"
        manifest_key = (
            ref.manifest_key
            if isinstance(ref, RemoteBundleRef)
            else ref.get("manifest_key")
        )
        if manifest_key is None:
            return expected
        if manifest_key != expected:
            raise ValueError("bundle manifest_key does not match manifest object_id")
        return manifest_key

    def _payload_keys(self, manifest: Mapping[str, Any]) -> list[str]:
        return [
            chunk["key"]
            for payload_spec in [manifest["meta"], *manifest["buffers"].values()]
            for chunk in payload_spec["chunks"]
        ]


class _MooncakePayloadTransport:
    """Move payload bytes through Mooncake, preferring fast-path APIs and falling back to generic store calls."""

    def __init__(self, store: BundleStore, buffer_pool: Any = None) -> None:
        self._store = store
        self._buffer_pool = buffer_pool
        self._batch_put_from = getattr(store, "batch_put_from", None)
        self._put_tensor_from = getattr(store, "put_tensor_from", None)
        self._batch_get_into = getattr(store, "batch_get_into", None)
        self._get_into = getattr(store, "get_into", None)
        self._get_into_ranges = getattr(store, "get_into_ranges", None)
        self._register_buffer = getattr(store, "register_buffer", None)
        self._unregister_buffer = getattr(store, "unregister_buffer", None)

    def put_payload_chunks(
        self,
        chunk_keys: Sequence[str],
        chunks: Sequence[memoryview],
        transfer_policy: BundleTransferPolicy,
        pre_registered: bool,
    ) -> list[str]:
        if transfer_policy.copy_mode == "copy":
            return self._put_chunks_direct(chunk_keys, chunks)
        if not self._has_batch_put_support():
            if transfer_policy.copy_mode == "zero_copy":
                raise RuntimeError(
                    "zero-copy put requested but batch_put_from is unavailable"
                )
            return self._put_chunks_direct(chunk_keys, chunks)
        put_mode = self._resolve_put_mode(chunks, transfer_policy)
        if put_mode == "batch":
            self.batch_put_chunks_from(
                chunk_keys, chunks, pre_registered=pre_registered
            )
            return list(chunk_keys)
        return self._put_chunks_parallel(
            list(chunk_keys),
            list(chunks),
            transfer_policy.max_inflight_put,
            pre_registered=pre_registered,
        )

    def read_payload(self, payload_spec: Mapping[str, Any]) -> bytes:
        expected_bytes = int(payload_spec["bytes"])
        if expected_bytes == 0:
            return b""
        data = bytearray(expected_bytes)
        self.read_payload_into(payload_spec, data)
        return bytes(data)

    def read_tensor_payload(self, payload_spec: Mapping[str, Any]) -> Any:
        get_tensor = getattr(self._store, "get_tensor", None)
        key = payload_spec["key"]
        if not callable(get_tensor):
            raise RuntimeError("structured tensor payload does not support get_tensor")
        return get_tensor(key)

    def read_payload_into(
        self, payload_spec: Mapping[str, Any], destination: bytearray | np.ndarray
    ) -> None:
        chunks = payload_spec["chunks"]
        offsets = _chunk_offsets(chunks)
        if self._read_chunks_with_batch_get_into(chunks, offsets, destination):
            return
        for offset, chunk in zip(offsets, chunks):
            self._read_chunk_with_get(chunk, destination, offset)

    def read_tensor_payload_into(
        self,
        payload_spec: Mapping[str, Any],
        destination: _TensorObjectBufferPayload,
    ) -> Any:
        if payload_spec.get("kind") == "tensor":
            get_tensor_into = getattr(self._store, "get_tensor_into", None)
            if not callable(get_tensor_into):
                raise RuntimeError(
                    "structured tensor payload does not support get_tensor_into"
                )
            result = get_tensor_into(
                payload_spec["key"], destination.ptr, destination.size
            )
            _ = destination.owner
            return result
        expected_bytes = int(payload_spec["bytes"])
        if destination.size < expected_bytes:
            raise ValueError(
                f"tensor destination has {destination.size} bytes, expected at least {expected_bytes}"
            )
        chunks = payload_spec["chunks"]
        if not self._read_payload_range_into_raw_destination(
            chunks, destination.ptr, 0, expected_bytes, allow_get_into=False
        ):
            offset = 0
            for chunk in chunks:
                data = self._store.get(chunk["key"])
                size = int(chunk["bytes"])
                if len(data) != size:
                    raise RuntimeError(
                        f"get failed for {chunk['key']}: expected {size} bytes, got {len(data)}"
                    )
                ctypes.memmove(destination.ptr + offset, data, size)
                offset += size
        _ = destination.owner
        return None

    def read_payload_range(
        self,
        payload_spec: Mapping[str, Any],
        byte_offset: int,
        byte_length: int,
    ) -> bytes:
        if byte_length == 0:
            return b""
        data = bytearray(byte_length)
        if not self._read_payload_range_into_registered_destination(
            payload_spec["chunks"], data, byte_offset, byte_length, False
        ):
            self._copy_payload_range_into_bytearray(
                payload_spec["chunks"], data, byte_offset, byte_length
            )
        return bytes(data)

    def read_payload_range_into_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination: np.ndarray,
        byte_offset: int,
        destination_pre_registered: bool,
    ) -> None:
        chunks = payload_spec["chunks"]
        byte_length = _buffer_nbytes(destination)
        if not self._read_payload_range_into_registered_destination(
            chunks,
            destination,
            byte_offset,
            byte_length,
            destination_pre_registered=destination_pre_registered,
        ):
            self._copy_payload_range_into_destination(
                chunks, destination, byte_offset, byte_length
            )

    def read_payload_range_into_raw_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination_ptr: int,
        destination_offset: int,
        byte_offset: int,
        byte_length: int,
    ) -> bool:
        if byte_length == 0:
            return True
        return self._read_payload_range_into_raw_destination(
            payload_spec["chunks"],
            destination_ptr,
            byte_offset,
            byte_length,
            allow_get_into=False,
            destination_offset=destination_offset,
        )

    def read_payload_ranges_into_array(
        self,
        payload_spec: Mapping[str, Any],
        destination: np.ndarray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        if self._read_payload_ranges_into_registered_destination(
            payload_spec["chunks"], destination, ranges
        ):
            return
        self._copy_payload_ranges_into_destination(
            payload_spec["chunks"], destination.view(np.uint8).reshape(-1), ranges
        )

    def read_payload_ranges_into_bytearray(
        self,
        payload_spec: Mapping[str, Any],
        destination: bytearray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        if self._read_payload_ranges_into_registered_destination(
            payload_spec["chunks"], destination, ranges
        ):
            return
        self._copy_payload_ranges_into_destination(
            payload_spec["chunks"], destination, ranges
        )

    def read_payload_ranges_into_raw_destination(
        self,
        payload_spec: Mapping[str, Any],
        destination_ptr: int,
        ranges: Sequence[tuple[int, int, int]],
    ) -> bool:
        return self._read_payload_ranges_into_raw_destination(
            payload_spec["chunks"], destination_ptr, ranges
        )

    def put_tensor_object_buffer(
        self,
        key: str,
        value: _TensorObjectBufferPayload,
    ) -> int:
        put_tensor_from = self._put_tensor_from
        if not callable(put_tensor_from):
            raise RuntimeError("put_tensor_from is unavailable")
        _check_status(
            put_tensor_from(key, value.ptr, value.size), "put_tensor_from", key
        )
        _ = value.owner
        return value.size

    def put_tensor_payload_direct(
        self,
        key: str,
        value: _TensorPayload,
    ) -> dict[str, Any] | None:
        put_tensor = getattr(self._store, "put_tensor", None)
        if not callable(put_tensor):
            return None
        tensor = value.tensor
        nbytes = int(getattr(tensor, "nbytes", 0))
        metadata_bytes = _tensor_metadata_size()
        total_bytes = metadata_bytes + nbytes
        _check_status(put_tensor(key, tensor), "put_tensor", key)
        return {
            "key": key,
            "kind": "tensor",
            "bytes": total_bytes,
            "dtype": str(getattr(tensor, "dtype", "")),
            "shape": list(getattr(tensor, "shape", ())),
            "chunks": [{"key": key, "bytes": total_bytes}],
            "metadata_bytes": metadata_bytes,
        }

    def put_tensor_payload_from_pool(
        self,
        key: str,
        value: _TensorPayload,
    ) -> int:
        if self._buffer_pool is None:
            raise RuntimeError("structured tensor zero-copy requires a BufferPool")
        put_tensor_from = self._put_tensor_from
        if not callable(put_tensor_from):
            raise RuntimeError("put_tensor_from is unavailable")
        metadata, data_ptr, tensor_nbytes, owner = _tensor_payload_parts(value)
        total_bytes = len(metadata) + tensor_nbytes
        lease = self._buffer_pool.acquire(total_bytes)
        view = None
        try:
            view = lease.buffer
            view[: len(metadata)] = metadata
            if tensor_nbytes:
                ctypes.memmove(lease.ptr + len(metadata), data_ptr, tensor_nbytes)
            view.release()
            view = None
            _check_status(
                put_tensor_from(key, lease.ptr, total_bytes), "put_tensor_from", key
            )
            _ = owner
            return total_bytes
        finally:
            if view is not None:
                view.release()
            lease.release()

    def batch_put_chunks_from(
        self,
        chunk_keys: Sequence[str],
        chunks: Sequence[memoryview],
        pre_registered: bool,
    ) -> None:
        batch_put_from = self._batch_put_from
        if not callable(batch_put_from):
            raise RuntimeError("batch_put_from is unavailable")
        if not chunk_keys:
            return
        prepared_chunks = [_prepare_chunk_source_buffer(chunk) for chunk in chunks]
        buffer_ptrs = [ptr for _owner, ptr, _size in prepared_chunks]
        sizes = [size for _owner, _ptr, size in prepared_chunks]
        registered_ptrs = self._register_buffers(
            buffer_ptrs, sizes, pre_registered, "bundle source payload"
        )
        try:
            results = batch_put_from(list(chunk_keys), buffer_ptrs, sizes)
            if len(results) != len(chunk_keys):
                raise RuntimeError(
                    f"batch_put_from returned {len(results)} results for {len(chunk_keys)} chunks"
                )
            for chunk_key, status in zip(chunk_keys, results):
                _check_status(status, "batch_put_from", chunk_key)
        except Exception:
            _cleanup_keys(self._store, chunk_keys, strict=False)
            raise
        finally:
            self._unregister_buffers(registered_ptrs, "bundle source payload")

    def _put_chunks_direct(
        self,
        chunk_keys: Sequence[str],
        chunks: Sequence[memoryview],
    ) -> list[str]:
        written_keys: list[str] = []
        try:
            for chunk_key, chunk in zip(chunk_keys, chunks):
                _check_status(self._store.put(chunk_key, chunk), "put", chunk_key)
                written_keys.append(chunk_key)
        except Exception:
            _cleanup_keys(self._store, written_keys, strict=False)
            raise
        return list(chunk_keys)

    def _put_chunks_parallel(
        self,
        chunk_keys: list[str],
        chunks: list[memoryview],
        max_inflight_put: int,
        pre_registered: bool,
    ) -> list[str]:
        groups = self._group_chunk_ranges(chunk_keys, chunks, max_inflight_put)
        futures: list[Future[None]] = []
        try:
            with ThreadPoolExecutor(
                max_workers=min(max_inflight_put, len(groups))
            ) as executor:
                futures = [
                    executor.submit(
                        self.batch_put_chunks_from,
                        group_keys,
                        group_chunks,
                        pre_registered,
                    )
                    for group_keys, group_chunks in groups
                ]
                for future in as_completed(futures):
                    future.result()
        except Exception:
            for future in futures:
                future.cancel()
            for future in futures:
                if future.done() and not future.cancelled():
                    try:
                        future.result()
                    except Exception:
                        pass
            _cleanup_keys(self._store, chunk_keys, strict=False)
            raise
        return chunk_keys

    def _group_chunk_ranges(
        self,
        chunk_keys: Sequence[str],
        chunks: Sequence[memoryview],
        max_inflight_put: int,
    ) -> list[tuple[list[str], list[memoryview]]]:
        if not chunk_keys:
            return []
        group_count = max(1, min(max_inflight_put, len(chunks)))
        group_size = (len(chunks) + group_count - 1) // group_count
        return [
            (
                list(chunk_keys[start : start + group_size]),
                list(chunks[start : start + group_size]),
            )
            for start in range(0, len(chunks), group_size)
        ]

    def _read_chunks_with_batch_get_into(
        self,
        chunks: Sequence[Mapping[str, Any]],
        offsets: Sequence[int],
        destination: bytearray | np.ndarray,
    ) -> bool:
        batch_get_into = self._batch_get_into
        if not callable(batch_get_into) or not self._has_buffer_registration_support():
            return False
        with self._registered_buffer(destination, "bundle payload") as base_ptr:
            keys = [chunk["key"] for chunk in chunks]
            ptrs = [base_ptr + offset for offset in offsets]
            sizes = [int(chunk["bytes"]) for chunk in chunks]
            read_sizes = batch_get_into(keys, ptrs, sizes)
            if len(read_sizes) != len(keys):
                raise RuntimeError(
                    f"batch_get_into returned {len(read_sizes)} results for {len(keys)} chunks"
                )
            for key, expected_size, actual_size in zip(keys, sizes, read_sizes):
                if actual_size != expected_size:
                    raise RuntimeError(
                        f"batch_get_into failed for {key}: expected {expected_size}, got {actual_size}"
                    )
        return True

    def _read_chunk_with_get(
        self,
        chunk: Mapping[str, Any],
        destination: bytearray | np.ndarray,
        offset: int,
    ) -> None:
        chunk_bytes = int(chunk["bytes"])
        if chunk_bytes == 0:
            return
        data = self._store.get(chunk["key"])
        if len(data) != chunk_bytes:
            raise RuntimeError(
                f"get failed for {chunk['key']}: expected {chunk_bytes} bytes, got {len(data)}"
            )
        destination[offset : offset + chunk_bytes] = data

    def _read_payload_range_into_registered_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: np.ndarray,
        byte_offset: int,
        byte_length: int,
        destination_pre_registered: bool,
    ) -> bool:
        if not self._has_buffer_registration_support():
            return False
        with self._registered_buffer(
            destination,
            "structured ndarray payload",
            pre_registered=destination_pre_registered,
        ) as base_ptr:
            return self._read_payload_range_into_raw_destination(
                chunks, base_ptr, byte_offset, byte_length
            )

    def _read_payload_ranges_into_registered_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: bytearray | np.ndarray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> bool:
        if not self._has_buffer_registration_support():
            return False
        with self._registered_buffer(destination, "structured ranged payload") as base_ptr:
            return self._read_payload_ranges_into_raw_destination(
                chunks, base_ptr, ranges
            )

    def _read_payload_ranges_into_raw_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        base_ptr: int,
        ranges: Sequence[tuple[int, int, int]],
    ) -> bool:
        get_into_ranges = self._get_into_ranges
        if not callable(get_into_ranges):
            return False
        fragments = []
        for source_offset, destination_offset, byte_length in ranges:
            fragments.extend(
                _payload_range_fragments(chunks, source_offset, byte_length, destination_offset)
            )
        if not fragments:
            return True
        keys = [
            key
            for key, _chunk_size, _destination_offset, _source_offset, _size in fragments
        ]
        dst_offsets = [
            [destination_offset]
            for _key, _chunk_size, destination_offset, _source_offset, _size in fragments
        ]
        src_offsets = [
            [source_offset]
            for _key, _chunk_size, _destination_offset, source_offset, _size in fragments
        ]
        sizes = [
            [size]
            for _key, _chunk_size, _destination_offset, _source_offset, size in fragments
        ]
        results = get_into_ranges(
            [base_ptr], [keys], [dst_offsets], [src_offsets], [sizes]
        )
        if len(results) != 1 or len(results[0]) != len(keys):
            raise RuntimeError(
                f"get_into_ranges returned invalid ranged result shape for {len(keys)} chunks"
            )
        for key, expected_sizes, actual_sizes in zip(keys, sizes, results[0]):
            if len(actual_sizes) != len(expected_sizes):
                raise RuntimeError(
                    f"get_into_ranges returned invalid ranged fragment count for {key}"
                )
            for expected_size, actual_size in zip(expected_sizes, actual_sizes):
                if actual_size != expected_size:
                    raise RuntimeError(
                        f"get_into_ranges failed for {key}: expected {expected_size}, got {actual_size}"
                    )
        return True

    def _read_payload_range_into_raw_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        base_ptr: int,
        byte_offset: int,
        byte_length: int,
        allow_get_into: bool = True,
        destination_offset: int = 0,
    ) -> bool:
        get_into = self._get_into
        if (
            allow_get_into
            and len(chunks) == 1
            and byte_offset == 0
            and callable(get_into)
        ):
            expected_size = int(chunks[0]["bytes"])
            if expected_size == byte_length:
                read_size = get_into(
                    chunks[0]["key"], base_ptr + destination_offset, expected_size
                )
                if read_size != expected_size:
                    raise RuntimeError(
                        f"get_into failed for {chunks[0]['key']}: expected {expected_size}, got {read_size}"
                    )
                return True
        get_into_ranges = self._get_into_ranges
        if not callable(get_into_ranges):
            return False
        fragments = _payload_range_fragments(
            chunks, byte_offset, byte_length, destination_offset
        )
        if not fragments:
            return True
        keys = [
            key
            for key, _chunk_size, _destination_offset, _source_offset, _size in fragments
        ]
        dst_offsets = [
            [fragment_destination_offset]
            for _key, _chunk_size, fragment_destination_offset, _source_offset, _size in fragments
        ]
        src_offsets = [
            [source_offset]
            for _key, _chunk_size, _destination_offset, source_offset, _size in fragments
        ]
        sizes = [
            [size]
            for _key, _chunk_size, _destination_offset, _source_offset, size in fragments
        ]
        results = get_into_ranges(
            [base_ptr], [keys], [dst_offsets], [src_offsets], [sizes]
        )
        if len(results) != 1 or len(results[0]) != len(keys):
            raise RuntimeError(
                f"get_into_ranges returned invalid ranged result shape for {len(keys)} chunks"
            )
        for key, expected_sizes, actual_sizes in zip(keys, sizes, results[0]):
            if len(actual_sizes) != len(expected_sizes):
                raise RuntimeError(
                    f"get_into_ranges returned invalid ranged fragment count for {key}"
                )
            for expected_size, actual_size in zip(expected_sizes, actual_sizes):
                if actual_size != expected_size:
                    raise RuntimeError(
                        f"get_into_ranges failed for {key}: expected {expected_size}, got {actual_size}"
                    )
        return True

    def _copy_payload_range_into_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: np.ndarray,
        byte_offset: int,
        byte_length: int,
    ) -> None:
        data = bytearray(byte_length)
        self._copy_payload_range_into_bytearray(chunks, data, byte_offset, byte_length)
        destination[:byte_length] = np.frombuffer(data, dtype=np.uint8)

    def _copy_payload_ranges_into_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: bytearray | np.ndarray,
        ranges: Sequence[tuple[int, int, int]],
    ) -> None:
        for source_offset, destination_offset, byte_length in ranges:
            for (
                key,
                chunk_size,
                fragment_destination_offset,
                fragment_source_offset,
                size,
            ) in _payload_range_fragments(
                chunks, source_offset, byte_length, destination_offset
            ):
                data = self._store.get(key)
                if len(data) != chunk_size:
                    raise RuntimeError(
                        f"get failed for {key}: expected {chunk_size} bytes, got {len(data)}"
                    )
                destination[
                    fragment_destination_offset : fragment_destination_offset + size
                ] = data[fragment_source_offset : fragment_source_offset + size]

    def _copy_payload_range_into_bytearray(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: bytearray,
        byte_offset: int,
        byte_length: int,
    ) -> None:
        for (
            key,
            chunk_size,
            destination_offset,
            source_offset,
            size,
        ) in _payload_range_fragments(chunks, byte_offset, byte_length):
            data = self._store.get(key)
            if len(data) != chunk_size:
                raise RuntimeError(
                    f"get failed for {key}: expected {chunk_size} bytes, got {len(data)}"
                )
            destination[destination_offset : destination_offset + size] = data[
                source_offset : source_offset + size
            ]

    def _register_buffers(
        self,
        buffer_ptrs: Sequence[int],
        sizes: Sequence[int],
        pre_registered: bool,
        label: str,
    ) -> list[int]:
        register_buffer = self._register_buffer
        unregister_buffer = self._unregister_buffer
        if pre_registered:
            return []
        if not (callable(register_buffer) and callable(unregister_buffer)):
            raise RuntimeError(f"register_buffer APIs are unavailable for {label}")
        registered_ptrs: list[int] = []
        try:
            for ptr, size in zip(buffer_ptrs, sizes):
                if size == 0:
                    continue
                register_status = register_buffer(ptr, size)
                if register_status == 0:
                    registered_ptrs.append(ptr)
                    continue
                if not _is_duplicate_buffer_registration(register_status):
                    _check_status(register_status, "register_buffer", label)
        except Exception:
            self._unregister_buffers(registered_ptrs, label)
            raise
        return registered_ptrs

    def _unregister_buffers(self, registered_ptrs: Sequence[int], label: str) -> None:
        unregister_buffer = self._unregister_buffer
        if not callable(unregister_buffer):
            raise RuntimeError(f"unregister_buffer APIs are unavailable for {label}")
        for ptr in reversed(registered_ptrs):
            _check_status(unregister_buffer(ptr), "unregister_buffer", label)

    @contextmanager
    def _registered_buffer(
        self,
        destination: bytearray | np.ndarray,
        label: str,
        pre_registered: bool = False,
    ) -> Iterator[int]:
        base_ptr = _buffer_ptr(destination)
        if pre_registered:
            yield base_ptr
            return
        registered_ptrs = self._register_buffers(
            [base_ptr], [_buffer_nbytes(destination)], False, label
        )
        if not registered_ptrs:
            yield base_ptr
            return
        succeeded = False
        try:
            yield base_ptr
            succeeded = True
        finally:
            try:
                self._unregister_buffers(registered_ptrs, label)
            except Exception:
                if succeeded:
                    raise

    def _resolve_put_mode(
        self,
        chunks: Sequence[memoryview],
        transfer_policy: BundleTransferPolicy,
    ) -> Literal["batch", "parallel"]:
        if transfer_policy.put_mode == "parallel":
            return "parallel"
        if transfer_policy.put_mode == "batch":
            return "batch"
        if transfer_policy.max_inflight_put <= 1:
            return "batch"
        if len(chunks) < AUTO_PARALLEL_MIN_CHUNKS:
            return "batch"
        if sum(len(chunk) for chunk in chunks) < AUTO_PARALLEL_MIN_BYTES:
            return "batch"
        if min(transfer_policy.max_inflight_put, len(chunks)) < 2:
            return "batch"
        return "parallel"

    def _has_batch_put_support(self) -> bool:
        return (
            callable(self._batch_put_from) and self._has_buffer_registration_support()
        )

    def _has_buffer_registration_support(self) -> bool:
        return callable(self._register_buffer) and callable(self._unregister_buffer)


def _resolve_ndarray_destination(
    name: str,
    destination: Any,
    dtype: np.dtype[Any],
    shape: tuple[int, ...],
) -> np.ndarray:
    if destination is None:
        return np.empty(shape, dtype=dtype)
    if isinstance(destination, _RawDestinationBuffer):
        nbytes = int(np.prod(shape, dtype=np.int64)) * dtype.itemsize
        if destination.size < nbytes:
            raise ValueError(
                f"raw destination has {destination.size} bytes, expected at least {nbytes}"
            )
        _ = destination.owner
        return np.ctypeslib.as_array(
            (ctypes.c_uint8 * nbytes).from_address(destination.ptr)
        ).view(dtype).reshape(shape)
    if not isinstance(destination, np.ndarray):
        raise TypeError(
            f"structured ndarray field {name} destination must be a numpy.ndarray or raw_destination"
        )
    if destination.dtype != dtype:
        raise ValueError(
            f"structured ndarray field {name} destination dtype mismatch: expected {dtype.str}, got {destination.dtype.str}"
        )
    if tuple(destination.shape) != shape:
        raise ValueError(
            f"structured ndarray field {name} destination shape mismatch: expected {shape}, got {tuple(destination.shape)}"
        )
    if not destination.flags["C_CONTIGUOUS"]:
        raise ValueError(
            f"structured ndarray field {name} destination must be C-contiguous"
        )
    if not destination.flags["WRITEABLE"]:
        raise ValueError(
            f"structured ndarray field {name} destination must be writeable"
        )
    return destination


def _resolve_ndarray_read_plan(
    shape: tuple[int, ...],
    dtype: np.dtype[Any],
    member_slice: StructuredMemberSlice | None,
) -> _NdarrayReadPlan:
    if member_slice is None:
        return _NdarrayReadPlan(
            dtype=dtype,
            full_shape=shape,
            output_shape=shape,
            byte_offset=0,
            byte_length=int(np.prod(shape, dtype=np.int64)) * dtype.itemsize
            if shape
            else dtype.itemsize,
            step=1,
            cover_row_count=shape[0] if shape else 1,
        )
    if member_slice.axis != 0:
        raise ValueError("structured ndarray slicing currently supports axis=0 only")
    if not shape:
        raise ValueError("structured ndarray slicing requires at least one dimension")
    total_rows = shape[0]
    start, end, step = _normalized_member_slice(member_slice, total_rows)
    row_width = dtype.itemsize * int(np.prod(shape[1:], dtype=np.int64))
    if start >= end:
        return _NdarrayReadPlan(
            dtype=dtype,
            full_shape=shape,
            output_shape=(0, *shape[1:]),
            byte_offset=start * row_width,
            byte_length=0,
            step=step,
            cover_row_count=0,
        )
    selected_count = 1 + (end - 1 - start) // step
    cover_start = start
    cover_end = start + (selected_count - 1) * step + 1
    return _NdarrayReadPlan(
        dtype=dtype,
        full_shape=shape,
        output_shape=(selected_count, *shape[1:]),
        byte_offset=cover_start * row_width,
        byte_length=(cover_end - cover_start) * row_width,
        step=step,
        cover_row_count=cover_end - cover_start,
    )


def _normalized_member_slice(
    member_slice: StructuredMemberSlice, total_rows: int
) -> tuple[int, int, int]:
    if member_slice.step <= 0:
        raise ValueError("structured ndarray slicing step must be positive")
    return slice(member_slice.start, member_slice.end, member_slice.step).indices(
        total_rows
    )


def _bytes_view(value: Any, name: str) -> memoryview:
    try:
        view = memoryview(value)
    except TypeError as error:
        raise TypeError(
            f"{name} must be bytes-like, got {type(value).__name__}"
        ) from error
    if not view.contiguous:
        view = memoryview(bytes(view))
    return view.cast("B")


def _prepare_chunk_source_buffer(chunk: memoryview) -> tuple[Any, int, int]:
    if len(chunk) == 0:
        copied = ctypes.create_string_buffer(0)
        return copied, ctypes.addressof(copied), 0
    if chunk.c_contiguous and not chunk.readonly:
        return chunk, ctypes.addressof(ctypes.c_char.from_buffer(chunk)), len(chunk)
    copied = ctypes.create_string_buffer(bytes(chunk))
    return copied, ctypes.addressof(copied), len(chunk)


def _buffer_ptr(value: bytearray | np.ndarray) -> int:
    return ctypes.addressof(ctypes.c_char.from_buffer(value))


def _buffer_nbytes(value: bytearray | np.ndarray) -> int:
    if isinstance(value, np.ndarray):
        return int(value.nbytes)
    return len(value)


def _split_view(view: memoryview, chunk_bytes: int) -> list[memoryview]:
    if len(view) == 0:
        return [view]
    return [
        view[start : start + chunk_bytes] for start in range(0, len(view), chunk_bytes)
    ]


def _chunk_offsets(chunks: Sequence[Mapping[str, Any]]) -> list[int]:
    offsets = [0]
    for chunk in chunks[:-1]:
        offsets.append(offsets[-1] + int(chunk["bytes"]))
    return offsets


def _payload_range_fragments(
    chunks: Sequence[Mapping[str, Any]],
    byte_offset: int,
    byte_length: int,
    destination_offset: int = 0,
) -> list[tuple[str, int, int, int, int]]:
    fragments: list[tuple[str, int, int, int, int]] = []
    read_end = byte_offset + byte_length
    chunk_offset = 0
    for chunk in chunks:
        chunk_size = int(chunk["bytes"])
        chunk_end = chunk_offset + chunk_size
        overlap_start = max(byte_offset, chunk_offset)
        overlap_end = min(read_end, chunk_end)
        if overlap_start < overlap_end:
            fragments.append(
                (
                    chunk["key"],
                    chunk_size,
                    destination_offset + overlap_start - byte_offset,
                    overlap_start - chunk_offset,
                    overlap_end - overlap_start,
                )
            )
        chunk_offset = chunk_end
    return fragments


def _validate_chunk_bytes(chunk_bytes: int) -> int:
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    return chunk_bytes


def _normalize_key_prefix(key_prefix: str) -> str:
    prefix = key_prefix.strip("/")
    if not prefix or any(ord(char) < 32 for char in prefix):
        raise ValueError("key_prefix must be a non-empty key prefix")
    return prefix


def _validate_key_segment(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or "/" in value
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(f"invalid bundle {name}: {value!r}")


def _encode_manifest(manifest: Mapping[str, Any]) -> bytes:
    return _encode_json_dict(manifest, "bundle manifest")


def _decode_manifest(payload: bytes) -> dict[str, Any]:
    return _decode_json_dict(payload, "bundle manifest")


def _normalize_structured_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        raise TypeError("structured metadata must be a mapping")
    normalized = dict(metadata)
    if STRUCTURED_FIELD_SPECS_KEY in normalized:
        raise ValueError(
            f"structured metadata key {STRUCTURED_FIELD_SPECS_KEY!r} is reserved"
        )
    if not isinstance(normalized.get("layout", "structured"), str):
        raise ValueError("structured metadata layout must be a string")
    normalized.setdefault("layout", "structured")
    return normalized


def _encode_structured_fields(
    metadata: Mapping[str, Any],
    fields: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_metadata = _normalize_structured_metadata(metadata)
    field_specs: dict[str, Any] = {}
    encoded_fields: dict[str, Any] = {}
    for name, value in fields.items():
        _validate_key_segment(name, "buffer name")
        spec, encoded_value = _encode_structured_field(value)
        field_specs[name] = spec
        encoded_fields[name] = encoded_value
    if field_specs:
        normalized_metadata[STRUCTURED_FIELD_SPECS_KEY] = field_specs
    return normalized_metadata, encoded_fields


def tensor_object_buffer(
    ptr: int, size: int, owner: Any = None, batch_size: int | None = None
) -> _TensorObjectBufferPayload:
    return _TensorObjectBufferPayload(
        ptr=int(ptr), size=int(size), owner=owner, batch_size=batch_size
    )


def raw_destination(
    ptr: int, size: int, owner: Any = None, *, pre_registered: bool = False
) -> _RawDestinationBuffer:
    return _RawDestinationBuffer(
        ptr=int(ptr), size=int(size), owner=owner, pre_registered=pre_registered
    )


def _encode_structured_field(value: Any) -> tuple[dict[str, Any], Any]:
    if isinstance(value, _TensorObjectBufferPayload):
        return {"encoding": "torch_tensor"}, value
    if _torch is not None and isinstance(value, _torch.Tensor):
        return _encode_torch_tensor_field(value)
    if isinstance(value, np.ndarray):
        array = np.ascontiguousarray(value)
        return {
            "encoding": "ndarray",
            "dtype": array.dtype.str,
            "shape": list(array.shape),
        }, array.view(np.uint8).reshape(-1)
    return {"encoding": "bytes"}, value


def _encode_torch_tensor_field(value: Any) -> tuple[dict[str, Any], Any]:
    return {
        "encoding": "torch_tensor",
        "dtype": str(value.dtype),
        "shape": list(value.shape),
        "element_size": int(value.element_size()),
    }, _TensorPayload(tensor=value)


def _tensor_payload_parts(value: _TensorPayload) -> tuple[bytes, int, int, Any]:
    metadata, data_ptr, tensor_nbytes, owner = _tensor_codec_helper(
        "_serialize_tensor"
    )(value.tensor)
    return bytes(metadata), int(data_ptr), int(tensor_nbytes), owner


def _has_tensor_codec_helpers() -> bool:
    return (
        _mooncake_store is not None
        and callable(getattr(_mooncake_store, "_serialize_tensor", None))
        and callable(getattr(_mooncake_store, "_deserialize_tensor", None))
    )


def _tensor_metadata_size() -> int:
    helper = (
        None
        if _mooncake_store is None
        else getattr(_mooncake_store, "_tensor_metadata_size", None)
    )
    if callable(helper):
        return int(helper())
    return 0


def _torch_save_payload_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    _torch.save(value, buffer)
    return buffer.getvalue()


def _deserialize_torch_save_payload(payload: bytes) -> Any:
    return _torch.load(io.BytesIO(payload), weights_only=True)


def _slice_tensor_metadata(
    metadata: bytes, shape: Sequence[int], data_bytes: int
) -> bytes:
    patched = bytearray(metadata)
    ctypes.c_uint64.from_buffer(patched, 32).value = int(data_bytes)
    global_shape_offset = 40
    local_shape_offset = global_shape_offset + 8 * 8
    for index, dim in enumerate(shape):
        ctypes.c_int64.from_buffer(
            patched, global_shape_offset + index * 8
        ).value = int(dim)
        ctypes.c_int64.from_buffer(patched, local_shape_offset + index * 8).value = int(
            dim
        )
    return bytes(patched)


def _tensor_payload_bytes(value: _TensorPayload) -> tuple[bytes, int]:
    metadata, data_ptr, tensor_nbytes, _owner = _tensor_payload_parts(value)
    if tensor_nbytes == 0:
        return metadata, len(metadata)
    return metadata + ctypes.string_at(data_ptr, tensor_nbytes), len(metadata)


def _deserialize_tensor_payload(payload: bytes) -> Any:
    return _tensor_codec_helper("_deserialize_tensor")(payload)


def _tensor_codec_helper(name: str) -> Any:
    helper = None if _mooncake_store is None else getattr(_mooncake_store, name, None)
    if not callable(helper):
        raise RuntimeError(
            "mooncake.store tensor serialization helpers are required for structured tensor fields"
        )
    return helper


def _validate_pre_registered_structured_buffers(
    original_buffers: Mapping[str, Any],
    encoded_buffers: Mapping[str, Any],
    pre_registered_buffers: Optional[Mapping[str, bool]],
) -> None:
    if not pre_registered_buffers:
        return
    for name, pre_registered in pre_registered_buffers.items():
        if not pre_registered:
            continue
        if name not in original_buffers or name not in encoded_buffers:
            raise ValueError(f"unknown pre-registered structured buffer: {name}")
        if not _is_same_writable_buffer(original_buffers[name], encoded_buffers[name]):
            raise ValueError(
                f"pre-registered structured buffer {name} must be the same writable contiguous buffer used for transfer"
            )


def _is_same_writable_buffer(original: Any, encoded: Any) -> bool:
    try:
        original_view = memoryview(original)
        encoded_view = memoryview(encoded)
    except TypeError:
        return False
    if not original_view.c_contiguous or not encoded_view.c_contiguous:
        return False
    if original_view.readonly or encoded_view.readonly:
        return False
    if original_view.nbytes != encoded_view.nbytes:
        return False
    if original_view.nbytes == 0:
        return True
    try:
        original_bytes = original_view.cast("B")
        encoded_bytes = encoded_view.cast("B")
        original_ptr = ctypes.addressof(ctypes.c_char.from_buffer(original_bytes))
        encoded_ptr = ctypes.addressof(ctypes.c_char.from_buffer(encoded_bytes))
    except (BufferError, TypeError, ValueError):
        return False
    return original_ptr == encoded_ptr


def _structured_field_specs(metadata: Mapping[str, Any]) -> dict[str, Any]:
    field_specs = metadata.get(STRUCTURED_FIELD_SPECS_KEY, {})
    if not isinstance(field_specs, dict):
        raise ValueError("structured field specs must be a dict")
    return field_specs


def _merge_structured_stage_metadata(
    old_metadata: Mapping[str, Any], new_metadata: Mapping[str, Any]
) -> dict[str, Any]:
    old_dataproto = old_metadata.get("dataproto")
    new_dataproto = new_metadata.get("dataproto")
    if old_dataproto != new_dataproto:
        raise ValueError("DataProto stage metadata mismatch during manifest merge")
    merged = dict(old_metadata)
    old_specs = dict(_structured_field_specs(old_metadata))
    new_specs = dict(_structured_field_specs(new_metadata))
    collisions = sorted(set(old_specs) & set(new_specs))
    if collisions:
        raise ValueError(f"structured members already exist: {collisions}")
    old_specs.update(new_specs)
    if old_specs:
        merged[STRUCTURED_FIELD_SPECS_KEY] = old_specs
    return merged


def _encode_structured_metadata(metadata: Mapping[str, Any]) -> bytes:
    return _encode_json_dict(metadata, "structured metadata")


def _decode_structured_metadata(payload: bytes) -> dict[str, Any]:
    return _decode_json_dict(payload, "structured metadata")


def _encode_json_dict(value: Mapping[str, Any], label: str) -> bytes:
    try:
        return json.dumps(value, separators=(",", ":")).encode("utf-8")
    except TypeError as error:
        raise TypeError(f"{label} must be JSON-serializable") from error


def _decode_json_dict(payload: bytes, label: str) -> dict[str, Any]:
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must decode to a dict")
    return value


def _check_status(status: Any, operation: str, key: str) -> None:
    if status not in (None, 0):
        raise RuntimeError(f"{operation} failed for {key}: {status}")


def _is_duplicate_buffer_registration(status: Any) -> bool:
    # Mooncake returns -600 when a caller-owned buffer is already registered.
    return status == -600


def _cleanup_keys(store: BundleStore, keys: Sequence[str], strict: bool) -> None:
    pending_keys = list(dict.fromkeys(keys))
    batch_remove = getattr(store, "batch_remove", None)
    if callable(batch_remove) and pending_keys:
        try:
            try:
                results = batch_remove(pending_keys, True)
            except TypeError:
                results = batch_remove(pending_keys)
            if len(results) != len(pending_keys):
                raise RuntimeError(
                    f"batch_remove returned {len(results)} results for {len(pending_keys)} keys"
                )
            failed_results = [
                (key, status)
                for key, status in zip(pending_keys, results)
                if status not in (None, 0, MISSING_OBJECT_ERROR)
            ]
            if not failed_results:
                return
            pending_keys = [key for key, _status in failed_results]
        except Exception:
            if strict:
                raise

    retry_errors = []
    for key in pending_keys:
        try:
            status = store.remove(key, True)
        except KeyError:
            continue
        except Exception:
            if strict:
                raise
            continue
        if status not in (None, 0, MISSING_OBJECT_ERROR):
            retry_errors.append((key, status))
    if retry_errors and strict:
        raise RuntimeError(
            f"failed to remove {len(retry_errors)} Mooncake keys: {retry_errors[:3]}"
        )


# ---------------------------------------------------------------------------
# Codec inference and recursive structure expansion
# ---------------------------------------------------------------------------

_INFER_MAX_STRUCT_KEYS = 64
_INFER_MAX_LIST_LEN = 8
_INFER_MAX_SAMPLE_ROWS = 128
_INFER_MAX_JSON_BYTES = 1 << 20
_INFER_MAX_DEPTH = 32

try:
    import torch as _torch
except Exception:  # pragma: no cover
    _torch = None  # type: ignore[assignment]


@dataclass
class _CodecDecision:
    accepted: bool
    codec: str
    reason: str
    normalized_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _InferredLeaf:
    path: str
    values: list[Any]
    decision: _CodecDecision


@dataclass
class _EncodedStructuredLeaf:
    codec: str
    rows: int
    payload: dict[str, Any]
    metadata: dict[str, Any]


class _Missing:
    """Sentinel for dict keys absent from a row (distinct from value-is-None)."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<MISSING>"


MISSING = _Missing()


@dataclass
class _InferredNode:
    path: str
    node_type: str
    children: list[Any]
    lengths: list[int] | None = None
    row_mask: list[bool] | None = None


def _non_null(values: list[Any]) -> list[Any]:
    return [v for v in values if v is not None and not isinstance(v, _Missing)]


def _is_pil_image(value: Any) -> bool:
    module = getattr(value.__class__, "__module__", None) or ""
    return module.startswith("PIL.") and hasattr(value, "save")


def _is_bytes_like(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview))


def _is_media_list(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and all(
        _is_pil_image(item) or _is_bytes_like(item) for item in value
    )


def _can_media_list(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(
            False, "media_list_ragged", "all rows are null", "media list"
        )
    if not all(_is_media_list(v) for v in nn):
        return _CodecDecision(
            False, "media_list_ragged", "not all rows are media list", "media list"
        )
    if not any(len(v) > 0 for v in nn):
        return _CodecDecision(
            False, "media_list_ragged", "all media lists are empty", "media list"
        )
    return _CodecDecision(
        True, "media_list_ragged", "all non-null rows are media list", "media list"
    )


def _check_all(
    values: list[Any],
    predicate: Any,
    codec: str,
    normalized_type: str,
) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, codec, "all rows are null", normalized_type)
    if not all(predicate(v) for v in nn):
        return _CodecDecision(
            False, codec, f"not all rows are {normalized_type}", normalized_type
        )
    return _CodecDecision(
        True, codec, f"all non-null rows are {normalized_type}", normalized_type
    )


def _can_tensor(values: list[Any]) -> _CodecDecision:
    if _torch is None:
        return _CodecDecision(
            False, "ragged_tensor", "torch is not available", "torch.Tensor"
        )
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(
            False, "ragged_tensor", "all rows are null", "torch.Tensor"
        )
    if not all(isinstance(v, _torch.Tensor) for v in nn):
        return _CodecDecision(
            False, "ragged_tensor", "not all rows are Tensor", "torch.Tensor"
        )
    dtypes = sorted({str(v.dtype) for v in nn})
    if len(dtypes) != 1:
        return _CodecDecision(
            False, "ragged_tensor", f"mixed dtypes: {dtypes}", "torch.Tensor"
        )
    ndims = {v.ndim for v in nn}
    if len(ndims) != 1:
        return _CodecDecision(
            False, "ragged_tensor", f"mixed dimensions: {ndims}", "torch.Tensor"
        )
    return _CodecDecision(
        True,
        "ragged_tensor",
        "all non-null rows are Tensor",
        "torch.Tensor",
        {"dtype": dtypes[0]},
    )


def _can_numeric_sequence(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(
            False, "typed_ragged", "all rows are null", "numeric sequence"
        )
    dtypes = []
    for v in nn:
        if isinstance(v, np.ndarray):
            arr = v
        elif isinstance(v, (list, tuple)):
            try:
                arr = np.asarray(v)
            except (ValueError, TypeError):
                return _CodecDecision(
                    False,
                    "typed_ragged",
                    "row cannot be converted to ndarray",
                    "numeric sequence",
                )
        else:
            return _CodecDecision(
                False, "typed_ragged", "row is not array-like", "numeric sequence"
            )
        if arr.dtype == object or not np.issubdtype(arr.dtype, np.number):
            return _CodecDecision(
                False,
                "typed_ragged",
                f"non-numeric dtype: {arr.dtype}",
                "numeric sequence",
            )
        dtypes.append(arr.dtype)
    try:
        dtype = np.result_type(*dtypes)
    except (TypeError, ValueError, OverflowError):
        return _CodecDecision(
            False, "typed_ragged", "cannot determine common dtype", "numeric sequence"
        )
    return _CodecDecision(
        True,
        "typed_ragged",
        "all rows promote to common numeric dtype",
        "numeric sequence",
        {"dtype": str(dtype)},
    )


def _can_numeric_scalar(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "ndarray", "all rows are null", "numeric scalar")
    if not all(isinstance(v, (bool, int, float, np.number)) for v in nn):
        return _CodecDecision(
            False, "ndarray", "not all rows are numeric scalar", "numeric scalar"
        )
    try:
        dtype = np.result_type(*nn)
    except (TypeError, ValueError, OverflowError):
        return _CodecDecision(
            False, "ndarray", "cannot determine common dtype", "numeric scalar"
        )
    if not np.issubdtype(dtype, np.number) and not np.issubdtype(dtype, np.bool_):
        return _CodecDecision(
            False, "ndarray", f"non-numeric dtype: {dtype}", "numeric scalar"
        )
    return _CodecDecision(
        True,
        "ndarray",
        "all rows are numeric scalar",
        "numeric scalar",
        {"dtype": str(dtype)},
    )


def _can_msgpack(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "msgpack_ragged", "all rows are null", "msgpack")
    if not all(isinstance(v, (dict, list, tuple)) for v in nn):
        return _CodecDecision(
            False, "msgpack_ragged", "not all rows are structured objects", "msgpack"
        )
    sampled_bytes = 0
    for i, v in enumerate(nn):
        try:
            encoded = _msgpack.packb(v, use_bin_type=True, strict_types=True)
        except (TypeError, ValueError, OverflowError):
            return _CodecDecision(
                False, "msgpack_ragged", "serialization failed", "msgpack"
            )
        if i < _INFER_MAX_SAMPLE_ROWS:
            sampled_bytes += len(encoded)
    if sampled_bytes > _INFER_MAX_JSON_BYTES:
        return _CodecDecision(
            False, "msgpack_ragged", "sampled payload too large", "msgpack"
        )
    return _CodecDecision(
        True, "msgpack_ragged", "all rows pass msgpack serialization", "msgpack"
    )


def _can_json(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "json_ragged", "all rows are null", "json")
    sampled_bytes = 0
    for i, v in enumerate(nn):
        try:
            encoded = json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        except (TypeError, ValueError, OverflowError, RecursionError):
            return _CodecDecision(False, "json_ragged", "serialization failed", "json")
        if i < _INFER_MAX_SAMPLE_ROWS:
            sampled_bytes += len(encoded)
    if sampled_bytes > _INFER_MAX_JSON_BYTES:
        return _CodecDecision(False, "json_ragged", "sampled payload too large", "json")
    return _CodecDecision(
        True, "json_ragged", "all rows pass JSON serialization", "json"
    )


_CODEC_PREDICATES: tuple[Any, ...] = (
    _can_tensor,
    _can_media_list,
    _can_numeric_sequence,
    _can_numeric_scalar,
    lambda v: _check_all(v, _is_bytes_like, "bytes_ragged", "bytes-like"),
    lambda v: _check_all(v, _is_pil_image, "media_bytes", "media"),
    lambda v: _check_all(v, lambda x: isinstance(x, str), "utf8_ragged", "str"),
    _can_msgpack,
    _can_json,
)


def _choose_leaf_codec(values: list[Any]) -> _CodecDecision:
    for predicate in _CODEC_PREDICATES:
        decision = predicate(values)
        if decision.accepted:
            return decision
    return _CodecDecision(
        False, "pickle_ragged_fallback", "no optimized codec matched", "python object"
    )


def _try_expand_dict(values: list[Any]) -> list[str] | None:
    nn = _non_null(values)
    if not nn or not all(isinstance(v, dict) for v in nn):
        return None
    keys = {k for v in nn for k in v.keys()}
    if not all(isinstance(k, str) for k in keys) or len(keys) > _INFER_MAX_STRUCT_KEYS:
        return None
    return sorted(keys)


def _try_expand_list(values: list[Any]) -> tuple[int, list[int]] | None:
    nn = _non_null(values)
    if not nn or not all(isinstance(v, (list, tuple)) for v in nn):
        return None
    max_len = max(len(v) for v in nn)
    if max_len > _INFER_MAX_LIST_LEN:
        return None
    if not all(
        item is None or isinstance(item, (dict, list, tuple)) for v in nn for item in v
    ):
        return None
    lengths = [len(v) if isinstance(v, (list, tuple)) else 0 for v in values]
    return max_len, lengths


def _escape_key(key: str) -> str:
    return key.replace("\\", "\\\\").replace(".", "\\.").replace("[", "\\[")


def infer_structure(
    path: str,
    values: list[Any],
    leaves: list[_InferredLeaf],
    nodes: list[_InferredNode],
    *,
    _depth: int = 0,
) -> None:
    """Recursively expand *values* into leaves and interior nodes.

    *leaves* and *nodes* are output accumulators populated during recursion.
    Absent dict keys are represented as ``MISSING`` in child columns
    (distinct from ``None`` values).  Each interior node carries a
    ``row_mask`` that records which rows had a real parent container.
    """
    if _depth > _INFER_MAX_DEPTH:
        raise ValueError(
            f"infer_structure exceeded max depth {_INFER_MAX_DEPTH} at {path!r}"
        )
    dict_keys = _try_expand_dict(values)
    if dict_keys is not None:
        row_mask = [isinstance(v, dict) for v in values]
        nodes.append(_InferredNode(path, "dict", dict_keys, row_mask=row_mask))
        for key in dict_keys:
            child = [
                v.get(key, MISSING) if isinstance(v, dict) else None for v in values
            ]
            infer_structure(
                f"{path}.{_escape_key(key)}", child, leaves, nodes, _depth=_depth + 1
            )
        return
    list_result = _try_expand_list(values)
    if list_result is not None:
        max_len, lengths = list_result
        row_mask = [isinstance(v, (list, tuple)) for v in values]
        nodes.append(
            _InferredNode(
                path, "list", list(range(max_len)), lengths, row_mask=row_mask
            )
        )
        for index in range(max_len):
            child = [
                v[index]
                if isinstance(v, (list, tuple)) and index < len(v)
                else (MISSING if isinstance(v, (list, tuple)) else None)
                for v in values
            ]
            infer_structure(f"{path}[{index}]", child, leaves, nodes, _depth=_depth + 1)
        return
    leaves.append(_InferredLeaf(path, values, _choose_leaf_codec(values)))
