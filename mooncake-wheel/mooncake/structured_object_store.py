from __future__ import annotations

import ctypes
import io
import json
import uuid
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Optional, Protocol, Sequence

import numpy as np

_mooncake_store: Any | None = None
_mooncake_store_loaded = False

import msgpack as _msgpack

# -- C fast-path: concat_arrays_into(list[ndarray], dest_ptr) ----------------
try:
    from mooncake._fast_copy import concat_arrays_into as _concat_arrays_into
except ImportError:
    _concat_arrays_into = None

DEFAULT_BUNDLE_CHUNK_BYTES = 64 * 1024**2
AUTO_PARALLEL_MIN_BYTES = DEFAULT_BUNDLE_CHUNK_BYTES
AUTO_PARALLEL_MIN_CHUNKS = 8
MISSING_OBJECT_ERROR = (
    -704
)  # Mooncake remove returns -704 for an already-missing object.
STRUCTURED_FIELD_SPECS_KEY = "__mooncake_structured_fields__"
# Default dtype for ragged tensor fields when all values are None (no data to
# infer dtype from).  The actual bytes stored are empty, so the dtype only
# serves as a placeholder for metadata consistency between encoder and decoder.
_RAGGED_TENSOR_DEFAULT_DTYPE = "torch.float32"
_TYPED_RAGGED_DEFAULT_DTYPE = "float64"
# Fallback field spec when a member has no stored encoding metadata (e.g. older
# format or unknown field).  Treated as opaque bytes by the read path.
_BYTES_FIELD_SPEC: dict[str, str] = {"encoding": "bytes"}
_INFER_MAX_DEPTH = 32


def _get_mooncake_store() -> Any | None:
    global _mooncake_store, _mooncake_store_loaded
    if not _mooncake_store_loaded:
        try:
            import mooncake.store as store
        except Exception:  # pragma: no cover - depends on built extension
            store = None  # type: ignore[assignment]
        _mooncake_store = store
        _mooncake_store_loaded = True
    return _mooncake_store
_INFER_MAX_STRUCT_KEYS = 64
_INFER_MAX_LIST_LEN = 8


class BundleStore(Protocol):
    def put(self, key: str, value: Any, config: Any = None) -> int: ...

    def get(self, key: str) -> bytes: ...

    def remove(self, key: str, force: bool = False) -> int: ...


@dataclass(frozen=True)
class BundleTransferPolicy:
    """Controls generic bundle transfer parallelism."""

    max_inflight_put: int = 1
    put_mode: Literal["auto", "batch", "parallel"] = "auto"
    copy_mode: Literal["auto", "zero_copy", "copy"] = "auto"


@dataclass(frozen=True)
class FieldSchema:
    """Schema hint for structured DataProto fields.

    ``metadata["section"]`` may declare where a field belongs:
    ``"batch"``, ``"non_tensor_batch"``, or ``"meta_info"``.  For
    non_tensor_batch fields, ``codec`` selects the structured non-tensor encoder
    without running sample-based inference.

    Supported codec values:
      - "auto": use existing runtime codec inference for this non_tensor_batch field
      - "ragged_tensor_dict": list[dict[str, Tensor] | None]
      - "ragged_tensor": list[Tensor | None]
      - "typed_ragged": list[ndarray | list | None]
      - "utf8_ragged": list[str | None]
      - "msgpack_ragged": list[Any] (msgpack-serializable)
      - "json_ragged": list[Any] (json-serializable)
      - "bytes_ragged": list[bytes | None]
      - "ndarray": list[numeric scalar]
    """

    codec: str
    nullable: bool = True
    metadata: dict = field(default_factory=dict)


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
class _MultiBufferPayload:
    buffers: tuple[memoryview, ...]
    owners: tuple[Any, ...] = ()
    dtype: str | None = None
    shape: tuple[int, ...] | None = None

    @property
    def nbytes(self) -> int:
        return _buffer_group_nbytes(self.buffers)


@dataclass(frozen=True)
class _DirectCopyPayload:
    """Deferred-copy payload: holds list[ndarray] for direct C-level copy into pool.

    Unlike _MultiBufferPayload (which creates memoryviews at encode time),
    this defers ALL copying to the transport layer, where the C extension
    concat_arrays_into() copies arrays directly into RDMA-registered pool
    memory in a tight C loop (~30ns per array vs ~1us for Python memoryview).
    """
    arrays: list[np.ndarray]
    total_bytes: int
    dtype: str | None = None
    shape: tuple[int, ...] | None = None

    @property
    def nbytes(self) -> int:
        return self.total_bytes

    @staticmethod
    def from_flat_arrays(
        flat_arrays: list[np.ndarray], dtype: np.dtype, total_elems: int,
    ) -> "_DirectCopyPayload":
        return _DirectCopyPayload(
            arrays=flat_arrays,
            total_bytes=sum(a.nbytes for a in flat_arrays),
            dtype=np.dtype(dtype).str,
            shape=(total_elems,),
        )


def _validate_tensor_object_buffer_owner(value: _TensorObjectBufferPayload) -> None:
    owner = value.owner
    lease = getattr(owner, "lease", owner)
    lease_ptr = getattr(lease, "ptr", None)
    lease_size = getattr(lease, "size", None)
    if lease_ptr is None and lease_size is None:
        return
    if lease_ptr is None or lease_size is None:
        raise RuntimeError("tensor object buffer owner has incomplete buffer metadata")
    if int(lease_ptr) != value.ptr or int(lease_size) < value.size:
        raise ValueError("tensor object buffer must be backed by its owner lease")


@dataclass(frozen=True)
class _RawDestinationBuffer:
    ptr: int
    size: int
    owner: Any
    pre_registered: bool = False


class _PoolLeaseOwner:
    def __init__(self, lease: Any) -> None:
        self.lease = lease
        self.released = False

    def release(self) -> None:
        if not self.released:
            self.lease.release()
            self.released = True

    def __del__(self) -> None:
        self.release()


class _PoolBackedNdarray(np.ndarray):
    def __new__(
        cls, owner: _PoolLeaseOwner, dtype: np.dtype[Any], shape: tuple[int, ...]
    ) -> "_PoolBackedNdarray":
        nbytes = (
            int(np.prod(shape, dtype=np.int64)) * dtype.itemsize
            if shape
            else dtype.itemsize
        )
        array = (
            np.ctypeslib.as_array(
                (ctypes.c_uint8 * nbytes).from_address(owner.lease.ptr)
            )
            .view(dtype)
            .reshape(shape)
            .view(cls)
        )
        array._mooncake_pool_owner = owner
        return array

    @classmethod
    def _from_shared_pool(
        cls,
        owner: _PoolLeaseOwner,
        dtype: np.dtype[Any],
        shape: tuple[int, ...],
        offset: int,
    ) -> "_PoolBackedNdarray":
        """Create pool-backed ndarray at an offset within a shared lease."""
        nbytes = (
            int(np.prod(shape, dtype=np.int64)) * dtype.itemsize
            if shape
            else dtype.itemsize
        )
        array = (
            np.ctypeslib.as_array(
                (ctypes.c_uint8 * nbytes).from_address(owner.lease.ptr + offset)
            )
            .view(dtype)
            .reshape(shape)
            .view(cls)
        )
        array._mooncake_pool_owner = owner
        return array

    def __array_finalize__(self, obj: Any) -> None:
        # WARNING: slicing/viewing propagates the same _PoolLeaseOwner to the
        # derived array.  If the original array is GC'd first its __del__
        # releases the pool lease, leaving the slice pointing at freed memory.
        # Callers must ensure the original array outlives any derived views.
        if obj is not None:
            self._mooncake_pool_owner = getattr(obj, "_mooncake_pool_owner", None)


class _OwnerBackedList(list):
    def __init__(self, values: Sequence[Any], owner: Any) -> None:
        super().__init__(values)
        self._mooncake_pool_owner = owner


class _OwnerBackedObjectArray(np.ndarray):
    def __array_finalize__(self, obj: Any) -> None:
        if obj is not None:
            self._mooncake_pool_owner = getattr(obj, "_mooncake_pool_owner", None)

    def tolist(self) -> list[Any]:
        values = super().tolist()
        owner = getattr(self, "_mooncake_pool_owner", None)
        if owner is None:
            return values
        return _OwnerBackedList(values, owner)


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


export_ref = export_dataproto_ref
import_ref = import_dataproto_ref


def _resolve_ref(ref: DataProtoRefLike) -> MooncakeDataProtoRef:
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
        config: Any = None,
    ) -> RemoteBundleRef:
        """Store raw metadata bytes plus named buffers as a low-level bundle."""
        return self._bundle_store.put_bundle(
            meta=meta,
            buffers=buffers,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
            config=config,
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
        config: Any = None,
    ) -> RemoteBundleRef:
        """Store a structured object described by JSON metadata plus named members."""
        return self._structured_store.put_structured_object(
            payload=payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
            config=config,
        )

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

    def put(
        self,
        data: Any,
        *,
        type: Literal["dataproto", "dict"] = "dataproto",
        namespace: str = "default",
        partition: str = "default",
        stage: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        field_schemas: Optional[Mapping[str, FieldSchema]] = None,
        config: Any = None,
    ) -> MooncakeDataProtoRef:
        """Store a DataProto-like object or flat dict as a structured object."""
        if type == "dataproto":
            stage_data = data
        elif type == "dict":
            stage_data = _flat_dict_to_envelope_with_schema(data, field_schemas)
        else:
            raise ValueError(f"unsupported Mooncake payload type: {type!r}")
        return self._put_stage(
            None,
            stage_data,
            namespace=namespace,
            partition=partition,
            stage=stage,
            chunk_bytes=chunk_bytes,
            policy=policy,
            overwrite=False,
            field_schemas=field_schemas,
            config=config,
        )

    def get(
        self,
        ref: DataProtoRefLike,
        *,
        type: Literal["dataproto", "dict"] = "dataproto",
        fields: Optional[Sequence[str]] = None,
        batch_fields: Optional[Sequence[str]] = None,
        non_tensor_fields: Optional[Sequence[str]] = None,
        meta_info_keys: Optional[Sequence[str]] = None,
        data_cls: Optional[Any] = None,
        destinations: Optional[Mapping[str, Any]] = None,
        rows: slice | StructuredMemberSlice | Sequence[int] | None = None,
    ) -> Any:
        """Materialize a structured object as a DataProto-like object or flat dict."""
        if type not in {"dataproto", "dict"}:
            raise ValueError(f"unsupported Mooncake payload type: {type!r}")
        return self._materialize_ref(
            _resolve_ref(ref),
            fields=fields,
            batch_fields=batch_fields,
            non_tensor_fields=non_tensor_fields,
            meta_info_keys=meta_info_keys,
            data_cls=data_cls,
            destinations=destinations,
            rows=rows,
            flat_dict_output=(type == "dict"),
        )

    def put_dataproto(
        self,
        data: Any,
        *,
        namespace: str = "default",
        partition: str = "default",
        stage: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        field_schemas: Optional[dict[str, FieldSchema]] = None,
        config: Any = None,
    ) -> MooncakeDataProtoRef:
        """Store a DataProto-like object as a stage-level structured object."""
        return self.put(
            data,
            type="dataproto",
            namespace=namespace,
            partition=partition,
            stage=stage,
            chunk_bytes=chunk_bytes,
            policy=policy,
            field_schemas=field_schemas,
            config=config,
        )

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
        return self.get(
            ref,
            type="dataproto",
            fields=fields,
            batch_fields=batch_fields,
            non_tensor_fields=non_tensor_fields,
            meta_info_keys=meta_info_keys,
            data_cls=data_cls,
            destinations=destinations,
            rows=rows,
        )

    def _materialize_ref(
        self,
        ref: MooncakeDataProtoRef,
        *,
        fields: Optional[Sequence[str]] = None,
        batch_fields: Optional[Sequence[str]] = None,
        non_tensor_fields: Optional[Sequence[str]] = None,
        meta_info_keys: Optional[Sequence[str]] = None,
        data_cls: Optional[Any] = None,
        destinations: Optional[Mapping[str, Any]] = None,
        rows: slice | StructuredMemberSlice | Sequence[int] | None = None,
        flat_dict_output: bool = False,
    ) -> Any:
        """Core materialization logic shared by get_dataproto and get_dict."""
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
        if row_selection is None and encoded_requests:
            # Group encoded fields by stage for batched materialize
            by_stage_encoded: dict[str, list[tuple[str, Any]]] = {}
            for name, location in encoded_requests:
                by_stage_encoded.setdefault(location.stage, []).append(
                    (name, location)
                )
            for stage, entries in by_stage_encoded.items():
                stage_ref = ref.stage_refs[stage]
                all_members: list[str] = []
                for name, _loc in entries:
                    encoded = ref.encoded_non_tensor[name]
                    all_members.extend(encoded["payload_members"].values())
                # Single materialize for all sub-members in this stage
                result = self.materialize(
                    self.read_spec(stage_ref).select_members(all_members)
                )
                for name, _loc in entries:
                    encoded = ref.encoded_non_tensor[name]
                    payload = {
                        payload_name: result.objects[member]
                        for payload_name, member in encoded[
                            "payload_members"
                        ].items()
                    }
                    values = _decode_structured_non_tensor_encoded(
                        encoded, payload, ref.batch_size, encoded.get("metadata")
                    )
                    non_tensor_batch[name] = (
                        values
                        if flat_dict_output
                        else _object_array_from_decoded_values(values)
                    )
        else:
            for name, location in encoded_requests:
                stage_ref = ref.stage_refs[location.stage]
                encoded = ref.encoded_non_tensor[name]
                if row_indices is not None:
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
                non_tensor_batch[name] = (
                    values
                    if flat_dict_output
                    else _object_array_from_decoded_values(values)
                )
        meta_info = {k: ref.meta_info[k] for k in meta_info_keys if k in ref.meta_info} if meta_info_keys is not None else dict(ref.meta_info)
        if flat_dict_output:
            return _envelope_to_flat_dict(
                {
                    "batch": batch,
                    "non_tensor_batch": non_tensor_batch,
                    "meta_info": meta_info,
                }
            )
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
        field_spec = _structured_field_specs(metadata).get(member, _BYTES_FIELD_SPEC)
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
                ctypes.memmove(destination.ptr, (ctypes.c_char * nbytes).from_buffer(data), nbytes)
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

    @staticmethod
    def _scatter_gather_offsets(
        raw_offsets: np.ndarray,
    ) -> tuple[np.ndarray, list[int], np.ndarray]:
        """Compute scatter-gather offset arrays from paired (begin, end) offsets.

        ``raw_offsets`` must contain interleaved ``[begin0, end0, begin1, end1, ...]``
        values obtained by reading the stored offsets array at positions
        ``[row, row+1]`` for each selected row.

        Returns ``(begins, item_counts, gathered_offsets)`` where
        ``gathered_offsets`` is the cumulative-sum offset array for the
        gathered result.
        """
        begins = raw_offsets[0::2]
        ends = raw_offsets[1::2]
        item_counts = [int(end) - int(begin) for begin, end in zip(begins, ends)]
        gathered_offsets = np.empty(len(item_counts) + 1, dtype=raw_offsets.dtype)
        gathered_offsets[0] = 0
        for idx, count in enumerate(item_counts):
            gathered_offsets[idx + 1] = int(gathered_offsets[idx]) + count
        return begins, item_counts, gathered_offsets

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
                return data
            array = np.empty(total_bytes // dtype.itemsize, dtype=dtype)
            self._bundle_store.read_payload_ranges_into_array(
                payload_spec, array.view(np.uint8).reshape(-1), ranges
            )
            return array

        if encoded.get("codec") == "structured_recursive":
            metadata = dict(metadata)
            metadata["leaves"] = [dict(leaf) for leaf in metadata.get("leaves", [])]
            recursive_payload: dict[str, Any] = {}
            for node in metadata.get("nodes", []):
                for key in ("missing_payload", "row_mask_payload", "lengths_payload"):
                    payload_name = node.get(key)
                    if payload_name is not None:
                        recursive_payload[payload_name] = read_member_indices(payload_name, indices)
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

        if codec == "ragged_tensor_dict":
            keys = metadata.get("keys", [])
            result_payload: dict[str, Any] = {
                "null_mask": read_member_indices("null_mask", indices),
            }
            for key in keys:
                sub_offsets = read_member_indices(
                    f"{key}.offsets",
                    [index for row in indices for index in (row, row + 1)],
                )
                begins, item_counts, gathered_offsets = self._scatter_gather_offsets(sub_offsets)
                dtype = member_dtype(f"{key}.data")
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
                result_payload[f"{key}.data"] = read_data_ranges(
                    f"{key}.data", ranges, dtype
                )
                result_payload[f"{key}.offsets"] = gathered_offsets
                result_payload[f"{key}.shapes"] = read_member_indices(
                    f"{key}.shapes", indices
                )
                result_payload[f"{key}.ndims"] = read_member_indices(
                    f"{key}.ndims", indices
                )
                result_payload[f"{key}.nulls"] = read_member_indices(
                    f"{key}.nulls", indices
                )
            return result_payload, metadata

        if codec == "ndarray":
            return {
                "data": read_member_indices("data", indices),
                "nulls": read_member_indices("nulls", indices),
            }, metadata

        if codec in {"ragged_tensor", "typed_ragged"}:
            offsets = read_member_indices(
                "offsets", [index for row in indices for index in (row, row + 1)]
            )
            begins, item_counts, gathered_offsets = self._scatter_gather_offsets(offsets)
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
            if codec == "typed_ragged" and metadata.get("row_format") == "mixed":
                ndarray_rows = metadata.get("ndarray_rows") or []
                metadata["ndarray_rows"] = [ndarray_rows[index] for index in indices]
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
            begins, byte_counts, gathered_offsets = self._scatter_gather_offsets(offsets)
            ranges = []
            destination_offset = 0
            for begin, count in zip(begins, byte_counts):
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

        if encoded.get("codec") == "structured_recursive":
            metadata = dict(metadata)
            metadata["leaves"] = [dict(leaf) for leaf in metadata.get("leaves", [])]
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

        if codec == "ragged_tensor_dict":
            keys = metadata.get("keys", [])
            result_payload: dict[str, Any] = {
                "null_mask": read_member("null_mask", start, end),
            }
            for key in keys:
                sub_offsets = read_member(f"{key}.offsets", start, end + 1)
                base = int(sub_offsets[0])
                limit = int(sub_offsets[-1])
                sub_offsets = sub_offsets - base
                result_payload[f"{key}.data"] = read_member(f"{key}.data", base, limit)
                result_payload[f"{key}.offsets"] = sub_offsets
                result_payload[f"{key}.shapes"] = read_member(
                    f"{key}.shapes", start, end
                )
                result_payload[f"{key}.ndims"] = read_member(f"{key}.ndims", start, end)
                result_payload[f"{key}.nulls"] = read_member(f"{key}.nulls", start, end)
            return result_payload, metadata

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
            if codec == "typed_ragged" and metadata.get("row_format") == "mixed":
                metadata["ndarray_rows"] = (metadata.get("ndarray_rows") or [])[
                    start:end
                ]
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

    def _cleanup_ref(self, ref: DataProtoRefLike) -> None:
        """Remove all structured object stages referenced by a handle."""
        ref = _resolve_ref(ref)
        seen: set[str] = set()
        for stage_ref in ref.stage_refs.values():
            if stage_ref.manifest_key in seen:
                continue
            seen.add(stage_ref.manifest_key)
            self.remove_bundle(stage_ref)

    def cleanup_dataproto(self, ref: DataProtoRefLike) -> None:
        """Remove all structured object stages referenced by a DataProto handle."""
        self._cleanup_ref(ref)

    def put_dict(
        self,
        data: Mapping[str, Any],
        *,
        namespace: str = "default",
        partition: str = "default",
        stage: str = "default",
        chunk_bytes: Optional[int] = None,
        policy: Optional[BundleTransferPolicy] = None,
        field_schemas: Optional[Mapping[str, "FieldSchema"]] = None,
        config: Any = None,
    ) -> MooncakeDataProtoRef:
        """Store a flat dict as a structured object."""
        return self.put(
            data,
            type="dict",
            namespace=namespace,
            partition=partition,
            stage=stage,
            chunk_bytes=chunk_bytes,
            policy=policy,
            field_schemas=field_schemas,
            config=config,
        )

    def get_dict(self, ref: DataProtoRefLike) -> dict[str, Any]:
        """Materialize a flat dict stored by put_dict()."""
        return self.get(ref, type="dict")

    def remove_dict(self, ref: DataProtoRefLike) -> None:
        """Remove a flat dict stored by put_dict()."""
        self._cleanup_ref(ref)

    put_legacy_dict = put_dict
    get_legacy_dict = get_dict
    remove_legacy_dict = remove_dict

    @staticmethod
    def release_result(result: Any) -> None:
        """Release pool-backed buffers in a GET result.

        After get_dataproto / get_dict, ndarray payloads may be backed by
        the BufferPool.  Call this to release those leases deterministically
        instead of waiting for GC ``__del__``.

        Works for both flat dicts and nested envelope dicts
        (dataproto: {batch: {...}, non_tensor_batch: {...}, meta_info: {...}}).
        """
        released_owners: set[int] = set()
        visited_containers: set[int] = set()

        def release_owner(owner: Any) -> None:
            owner_id = id(owner)
            if owner_id in released_owners:
                return
            released_owners.add(owner_id)
            owner.release()

        def visit(value: Any) -> None:
            owner = getattr(value, "_mooncake_pool_owner", None)
            if owner is not None:
                release_owner(owner)
                return
            if isinstance(value, Mapping):
                container_id = id(value)
                if container_id in visited_containers:
                    return
                visited_containers.add(container_id)
                for item in value.values():
                    visit(item)
                return
            if isinstance(value, (list, tuple)):
                container_id = id(value)
                if container_id in visited_containers:
                    return
                visited_containers.add(container_id)
                for item in value:
                    visit(item)
                return
            if isinstance(value, np.ndarray) and value.dtype == object:
                container_id = id(value)
                if container_id in visited_containers:
                    return
                visited_containers.add(container_id)
                for item in value.flat:
                    visit(item)

        visit(result)

    def _append_dataproto_stage_manifest(
        self,
        old_stage_ref: RemoteBundleRef,
        payload: StructuredObjectPayload,
        *,
        partition: str,
        chunk_bytes: Optional[int],
        policy: Optional[BundleTransferPolicy],
        config: Any,
    ) -> RemoteBundleRef:
        new_stage_ref = self.put_structured_object(
            payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            config=config,
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
                config=config,
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

    def _put_stage(
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
        field_schemas: Optional[dict[str, FieldSchema]] = None,
        config: Any = None,
    ) -> MooncakeDataProtoRef:
        batch, non_tensor_batch, meta_info = _split_dataproto_like(data)
        _validate_dataproto_schema_sections(batch, non_tensor_batch, meta_info, field_schemas)
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
            schema = field_schemas.get(name) if field_schemas else None
            needs_non_tensor = (isinstance(value, np.ndarray) and value.dtype == object) or (
                schema is not None
                and not (isinstance(value, np.ndarray) and value.dtype != object and schema.codec in {"auto", "ndarray"})
            )
            if needs_non_tensor:
                if isinstance(value, np.ndarray):
                    encode_value = value
                elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                    encode_value = np.empty(len(value), dtype=object)
                    encode_value[:] = list(value)
                else:
                    raise TypeError(
                        f"non_tensor_batch field {name!r} with FieldSchema must be an ndarray or non-string sequence"
                    )
                path = f"non_tensor_batch.{name}"
                if schema is not None:
                    try:
                        encoded = _encode_with_schema(path, encode_value, schema)
                    except _ENCODING_FALLBACK_ERRORS:
                        if schema.codec == "auto":
                            raise
                        try:
                            encoded = _encode_with_fallback(path, encode_value)
                        except _ENCODING_FALLBACK_ERRORS as fallback_error:
                            raise ValueError(
                                f"unable to encode structured non-tensor field {path!r} with "
                                f"schema codec {schema.codec!r} or automatic fallback"
                            ) from fallback_error
                else:
                    encoded = _encode_with_fallback(path, encode_value)
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
                config=config,
            )
        else:
            stage_ref = self.put_structured_object(
                payload,
                partition=partition,
                chunk_bytes=chunk_bytes,
                policy=policy,
                config=config,
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


_DATAPROTO_SCHEMA_SECTIONS = frozenset({"batch", "non_tensor_batch", "meta_info"})


def _schema_section(name: str, schema: FieldSchema) -> str | None:
    section = schema.metadata.get("section")
    if section is None:
        return None
    if section not in _DATAPROTO_SCHEMA_SECTIONS:
        raise ValueError(
            f"FieldSchema for {name!r} metadata['section'] must be one of "
            f"{sorted(_DATAPROTO_SCHEMA_SECTIONS)}"
        )
    return section


def _schema_ndarray_dtype(schema: FieldSchema) -> np.dtype[Any] | None:
    dtype_name = schema.metadata.get("dtype")
    if dtype_name is None:
        return None
    try:
        dtype = np.dtype(dtype_name)
    except (TypeError, ValueError):
        return None
    if dtype.kind not in "biufc":
        return None
    return dtype


def _validate_dataproto_schema_sections(
    batch: Mapping[str, Any],
    non_tensor_batch: Mapping[str, Any],
    meta_info: Mapping[str, Any],
    field_schemas: Optional[Mapping[str, FieldSchema]],
) -> None:
    if not field_schemas:
        return
    for section, fields in (
        ("batch", batch),
        ("non_tensor_batch", non_tensor_batch),
        ("meta_info", meta_info),
    ):
        for name in fields:
            schema = field_schemas.get(name)
            if schema is None:
                continue
            declared = _schema_section(name, schema)
            if declared is None:
                continue
            if declared != section:
                raise ValueError(
                    f"FieldSchema for {name!r} declares section {declared!r}, "
                    f"but data contains it in {section!r}"
                )


def _flat_dict_to_envelope_with_schema(
    data: Mapping[str, Any], field_schemas: Mapping[str, FieldSchema] | None
) -> dict[str, Any]:
    if not field_schemas:
        field_schemas = {}
    schema_row_count = _flat_dict_schema_row_count(data, field_schemas)
    row_count = schema_row_count
    if row_count == 0:
        schema_meta_fields = {
            name
            for name, schema in field_schemas.items()
            if name in data and _schema_section(name, schema) == "meta_info"
        }
        row_count = _flat_dict_auto_row_count(data, exclude=schema_meta_fields)
    batch: dict[str, Any] = {}
    non_tensor_batch: dict[str, Any] = {}
    meta_info: dict[str, Any] = {}
    for key, value in data.items():
        schema = field_schemas.get(key)
        section = None if schema is None else _schema_section(key, schema)
        if section is None:
            if _is_row_aligned_dense_field(value, row_count):
                batch[key] = value
            elif schema_row_count == 0 and isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and len(value) == row_count:
                non_tensor_batch[key] = _coerce_flat_dict_non_tensor_field(
                    key, value, row_count, schema
                )
            else:
                meta_info[key] = value
            continue
        if section == "batch":
            batch[key] = value
        elif section == "non_tensor_batch":
            non_tensor_batch[key] = _coerce_flat_dict_non_tensor_field(
                key, value, row_count, schema
            )
        else:
            meta_info[key] = value
    return {
        "batch": batch,
        "non_tensor_batch": non_tensor_batch,
        "meta_info": meta_info,
    }


def _flat_dict_to_envelope(
    data: Mapping[str, Any],
    field_schemas: Optional[Mapping[str, FieldSchema]] = None,
) -> dict[str, Any]:
    return _flat_dict_to_envelope_with_schema(data, field_schemas)


def _flat_dict_schema_row_count(
    data: Mapping[str, Any], field_schemas: Mapping[str, FieldSchema]
) -> int:
    sizes = {
        _field_len(name, data[name])
        for name, schema in field_schemas.items()
        if name in data and _schema_section(name, schema) in {"batch", "non_tensor_batch"}
    }
    if not sizes:
        return 0
    if len(sizes) != 1:
        raise ValueError(f"flat dict fields have inconsistent batch sizes: {sorted(sizes)}")
    return sizes.pop()


def _field_len(name: str, value: Any) -> int:
    try:
        return len(value)
    except TypeError as error:
        raise ValueError(f"flat dict row-aligned field {name!r} must be sized") from error


def _coerce_flat_dict_non_tensor_field(
    name: str, value: Any, row_count: int, schema: Optional[FieldSchema] = None
) -> Any:
    if isinstance(value, np.ndarray):
        if len(value) != row_count:
            raise ValueError(
                f"flat dict non_tensor_batch field {name!r} has batch size {len(value)}, expected {row_count}"
            )
        if (schema is not None and schema.codec == "ndarray" and _schema_ndarray_dtype(schema) is not None
                and (value.dtype != object or all(item is not None for item in value))):
            return np.asarray(value, dtype=_schema_ndarray_dtype(schema))
        return value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) != row_count:
            raise ValueError(
                f"flat dict non_tensor_batch field {name!r} has batch size {len(value)}, expected {row_count}"
            )
        array = np.empty(row_count, dtype=object)
        array[:] = list(value)
        return array
    raise TypeError(
        f"flat dict non_tensor_batch field {name!r} must be an ndarray or non-string sequence"
    )




def _flat_dict_auto_row_count(
    data: Mapping[str, Any], exclude: set[str] | frozenset[str] = frozenset()
) -> int:
    sizes = {
        len(value)
        for key, value in data.items()
        if key not in exclude and (
            (_torch is not None and isinstance(value, _torch.Tensor) and value.ndim > 0)
            or (isinstance(value, np.ndarray) and value.ndim > 0)
            or (isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)))
        )
    }
    if not sizes:
        return 0
    if len(sizes) != 1:
        raise ValueError(
            f"flat dict fields have ambiguous batch sizes: {sorted(sizes)}; "
            "pass FieldSchema metadata['section'] for list-valued metadata fields"
        )
    return sizes.pop()


def _envelope_to_flat_dict(data: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(_mapping_to_dict(data.get("meta_info")))
    result.update(_mapping_to_dict(data.get("batch")))
    for key, value in _mapping_to_dict(data.get("non_tensor_batch")).items():
        result[key] = (
            value.tolist()
            if isinstance(value, np.ndarray) and value.dtype == object
            else value
        )
    return result


def _object_array_from_decoded_values(values: list[Any]) -> np.ndarray:
    array = np.empty(len(values), dtype=object)
    array[:] = values
    owner = getattr(values, "_mooncake_pool_owner", None) or next(
        (getattr(v, "_mooncake_pool_owner", None) for v in values if v is not None), None
    )
    if owner is None:
        return array
    result = array.view(_OwnerBackedObjectArray)
    result._mooncake_pool_owner = owner
    return result


def _is_row_aligned_dense_field(value: Any, row_count: int) -> bool:
    if _torch is not None and isinstance(value, _torch.Tensor):
        return value.shape[:1] == (row_count,)
    return (
        isinstance(value, np.ndarray)
        and value.dtype != object
        and value.shape[:1] == (row_count,)
    )


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
        envelope_keys = {"batch", "non_tensor_batch", "meta_info"}
        if (data and set(data).issubset(envelope_keys) and all(
            v is None or isinstance(v, Mapping) or callable(getattr(v, "items", None))
            for v in data.values()
        )):
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
        missing = [name for name in requested if name not in ref.field_index]
        if missing:
            raise KeyError(f"unknown DataProto fields: {missing}")
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
    missing = [name for name in [*batch_names, *non_tensor_names] if name not in ref.field_index]
    if missing:
        raise KeyError(f"unknown DataProto fields: {missing}")
    return batch_names, non_tensor_names


def _coerce_dataproto_row_selection(
    rows: slice | StructuredMemberSlice | Sequence[int] | None, total_rows: int
) -> _DataProtoRowSelection | None:
    if rows is None:
        return None
    if isinstance(rows, StructuredMemberSlice):
        if rows.axis != 0:
            raise ValueError("DataProto row slicing currently supports axis=0 only")
        _normalized_member_slice(rows, total_rows)
        s, e, st = _normalized_member_slice(rows, total_rows)
        return _DataProtoRowSelection(
            count=max(0, 1 + (e - 1 - s) // st) if s < e else 0, member_slice=rows
        )
    if isinstance(rows, slice):
        start, end, step = rows.indices(total_rows)
        if step <= 0:
            raise ValueError("DataProto row slicing step must be positive")
        member_slice = StructuredMemberSlice(axis=0, start=start, end=end, step=step)
        s, e, st = _normalized_member_slice(member_slice, total_rows)
        return _DataProtoRowSelection(
            count=max(0, 1 + (e - 1 - s) // st) if s < e else 0, member_slice=member_slice
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



# ---------------------------------------------------------------------------
# Schema-driven encoding dispatch
# ---------------------------------------------------------------------------


_ENCODING_FALLBACK_ERRORS = (TypeError, ValueError, AttributeError, OverflowError)


def _encode_with_schema(
    path: str, value: np.ndarray, schema: FieldSchema
) -> "_EncodedStructuredLeaf":
    """Encode a non_tensor_batch field using an explicit schema (no inference)."""
    values = list(value)
    codec = schema.codec
    if codec == "auto":
        return _encode_with_fallback(path, value)
    if codec == "ragged_tensor_dict":
        return _encode_ragged_tensor_dict_values(values, schema)
    if codec == "ragged_tensor":
        payload, metadata = _encode_ragged_tensor_values(values)
    elif codec == "typed_ragged":
        payload, metadata = _encode_typed_ragged_values(
            values, dtype_hint=_schema_ndarray_dtype(schema)
        )
    elif codec == "ndarray":
        dtype = _schema_ndarray_dtype(schema)
        if dtype is None:
            return _encode_with_fallback(path, value)
        decision = _CodecDecision(
            True, "ndarray", "schema", "numeric scalar", {"dtype": str(dtype)}
        )
        payload, metadata = _encode_numeric_scalar_values(values, decision)
    elif codec in ("bytes_ragged", "media_bytes"):
        payload, metadata = _encode_bytes_like_values(values)
    elif codec == "media_list_ragged":
        payload, metadata = _encode_media_list_values(values)
    elif codec == "utf8_ragged":
        payload, metadata = _encode_bytes_like_values(
            [None if v is None else v.encode("utf-8") for v in values]
        )
    elif codec == "msgpack_ragged":
        payload, metadata = _encode_msgpack_ragged_values(path, values)
    elif codec == "json_ragged":
        payload, metadata = _encode_bytes_like_values(
            [
                None
                if v is None
                else json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8"
                )
                for v in values
            ]
        )
    else:
        raise ValueError(f"unsupported schema codec: {codec!r}")
    return _EncodedStructuredLeaf(
        codec=codec, rows=len(values), payload=payload, metadata=metadata
    )


def _encode_recursive_if_structured(
    path: str, values: list[Any]
) -> "_EncodedStructuredLeaf | None":
    leaves: list[_InferredLeaf] = []
    nodes: list[_InferredNode] = []
    infer_structure(path, values, leaves, nodes)
    if not nodes or not any(
        (leaf.decision.codec == "typed_ragged" and leaf.decision.metadata.get("recursive_source") == "ndarray")
        or leaf.decision.codec in {"ragged_tensor", "bytes_ragged", "media_bytes", "media_list_ragged"}
        for leaf in leaves
    ):
        return None
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
        codec_values = [
            None if is_missing else value
            for is_missing, value in zip(missing, leaf.values)
        ]
        decision = leaf.decision
        if not decision.accepted and all(
            value is None or isinstance(value, _Missing) for value in leaf.values
        ):
            decision = _CodecDecision(True, "json_ragged", "all rows are null or missing", "json")
        encoded = _encode_with_schema(
            leaf.path,
            np.asarray(
                _normalize_values_for_fallback_codec(codec_values, decision.codec),
                dtype=object,
            ),
            FieldSchema(codec=decision.codec, metadata=decision.metadata),
        )
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


def _encode_with_fallback(path: str, value: np.ndarray) -> "_EncodedStructuredLeaf":
    """Encode using safe type-based fallbacks, with recursive expansion for structured rows."""
    values = list(value)
    errors: list[str] = []
    try:
        recursive = _encode_recursive_if_structured(path, values)
        if recursive is not None:
            return recursive
    except _ENCODING_FALLBACK_ERRORS as error:
        errors.append(f"structured_recursive: {error}")
    decision = _choose_leaf_codec(values)
    codecs = [decision.codec]
    for codec in ("msgpack_ragged", "json_ragged"):
        if codec not in codecs:
            codecs.append(codec)
    for codec in codecs:
        metadata = dict(decision.metadata or {}) if codec == decision.codec else {}
        codec_values = _normalize_values_for_fallback_codec(values, codec)
        codec_array = np.asarray(codec_values, dtype=object)
        try:
            return _encode_with_schema(path, codec_array, FieldSchema(codec=codec, metadata=metadata))
        except _ENCODING_FALLBACK_ERRORS as error:
            errors.append(f"{codec}: {error}")
    raise ValueError(
        f"unable to infer a safe codec for structured non-tensor field {path!r}; "
        f"tried {errors}"
    )


def _normalize_values_for_fallback_codec(values: list[Any], codec: str) -> list[Any]:
    if codec in {"msgpack_ragged", "json_ragged"}:
        return [_normalize_structured_scalar(value) for value in values]
    return values


def _normalize_structured_scalar(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        if value.dtype == object:
            # Object ndarrays do not have a stable contiguous typed representation.
            # For generic fallbacks, materialize their Python shape once so msgpack/json
            # can preserve the logical nested values instead of treating the object array
            # as a numeric ragged buffer.
            return _normalize_structured_scalar(value.tolist())
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Mapping):
        return {key: _normalize_structured_scalar(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_normalize_structured_scalar(item) for item in value]
    return value


def _decode_structured_non_tensor_encoded(
    encoded: Mapping[str, Any],
    payload: dict[str, Any],
    rows: int,
    metadata: Optional[Mapping[str, Any]] = None,
) -> list[Any]:
    codec = encoded.get("codec")
    if codec == "ragged_tensor_dict":
        return _decode_ragged_tensor_dict_values(
            payload, rows, metadata or encoded.get("metadata", {})
        )
    if encoded.get("codec") == "structured_recursive":
        if metadata is not None:
            encoded = {**encoded, "metadata": metadata}
        return _decode_structured_recursive_field(encoded, payload, rows)
    return _decode_structured_leaf(encoded["codec"], payload, rows, metadata)


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
        if leaf["codec"] == "typed_ragged":
            values = [
                value.tolist() if isinstance(value, np.ndarray) else value
                for value in values
            ]
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
        return _decode_ragged_tensor_values(payload, rows, metadata)
    if codec == "typed_ragged":
        return _decode_typed_ragged_values(payload, rows, metadata)
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
        return _decode_msgpack_ragged_values(payload, rows)
    if codec == "json_ragged":
        return [
            None if value is None else json.loads(value)
            for value in _decode_bytes_like_values(payload, rows)
        ]
    raise ValueError(f"unknown structured non-tensor codec: {codec}")


def _encode_ragged_tensor_values(
    values: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if _torch is None:
        raise RuntimeError("torch is required to encode ragged tensor fields")
    # Determine torch dtype and convert all values to numpy arrays.
    torch_dtype = None
    converted: list[Any] = []
    for value in values:
        if value is None:
            converted.append(None)
            continue
        if isinstance(value, np.ndarray):
            if value.dtype == object:
                raise ValueError("ragged tensor codec requires numeric ndarray values")
            if not value.flags.writeable:
                value = value.copy()
            tensor = _torch.as_tensor(value)
        else:
            tensor = value.detach()
        if tensor.device.type != "cpu" or not tensor.is_contiguous():
            tensor = tensor.cpu().contiguous()
        torch_dtype = tensor.dtype if torch_dtype is None else torch_dtype
        if tensor.dtype != torch_dtype:
            raise ValueError(f"mixed tensor dtype: {torch_dtype} vs {tensor.dtype}")
        converted.append(tensor.numpy())
    data_dtype = torch_dtype or _parse_torch_dtype(_RAGGED_TENSOR_DEFAULT_DTYPE)
    np_dtype = np.dtype(_torch_dtype_to_numpy(str(data_dtype)))
    payload, meta = _encode_typed_ragged_values(converted, np_dtype)
    meta["dtype"] = str(data_dtype)
    return payload, meta


def _decode_ragged_tensor_values(
    payload: dict[str, Any], rows: int, metadata: Optional[Mapping[str, Any]] = None
) -> list[Any]:
    if _torch is None:
        raise RuntimeError("torch is required to decode ragged tensor fields")
    # Metadata stores torch dtype strings (e.g. "torch.int64"); convert to numpy
    # dtype so _decode_typed_ragged_values can parse it.
    dtype_str = (metadata or {}).get("dtype", _RAGGED_TENSOR_DEFAULT_DTYPE)
    np_dtype = _torch_dtype_to_numpy(dtype_str)
    patched_meta = dict(metadata) if metadata else {}
    patched_meta["dtype"] = str(np_dtype)
    np_values = _decode_typed_ragged_values(payload, rows, patched_meta)
    torch_dtype = _parse_torch_dtype(dtype_str)
    result: list[Any] = []
    for v in np_values:
        if v is None:
            result.append(None)
        elif isinstance(v, np.ndarray):
            result.append(_torch.from_numpy(v).to(torch_dtype))
        else:
            result.append(_torch.as_tensor(v, dtype=torch_dtype))
    return result


# ---------------------------------------------------------------------------
# ragged_tensor_dict codec: list[dict[str, Tensor] | None]
# ---------------------------------------------------------------------------


def _encode_ragged_tensor_dict_values(
    values: list[Any],
    schema: FieldSchema,
) -> "_EncodedStructuredLeaf":
    """Encode list[dict[str, Tensor] | None] directly without inference.

    Each dict key's tensors are encoded as a separate ragged_tensor sub-payload,
    giving per-key zero-copy RDMA transfer and independent partial-read access.
    """
    if _torch is None:
        raise RuntimeError("torch is required to encode ragged_tensor_dict fields")
    rows = len(values)
    null_mask = np.asarray([v is None for v in values], dtype=np.bool_)
    non_null = [v for v in values if v is not None]

    if not non_null:
        return _EncodedStructuredLeaf(
            codec="ragged_tensor_dict",
            rows=rows,
            payload={"null_mask": null_mask},
            metadata={"keys": [], "key_codecs": {}},
        )

    keys = schema.metadata.get("keys")
    if not keys:
        all_keys: set[str] = set()
        for v in non_null:
            all_keys.update(v.keys())
        keys = sorted(all_keys)

    payload: dict[str, Any] = {"null_mask": null_mask}
    key_codecs: dict[str, Any] = {}
    for key in keys:
        key_values = [None if v is None else v.get(key) for v in values]
        sub_payload, sub_metadata = _encode_ragged_tensor_values(key_values)
        for payload_name, payload_value in sub_payload.items():
            payload[f"{key}.{payload_name}"] = payload_value
        key_codecs[key] = sub_metadata

    return _EncodedStructuredLeaf(
        codec="ragged_tensor_dict",
        rows=rows,
        payload=payload,
        metadata={"keys": keys, "key_codecs": key_codecs},
    )


def _decode_ragged_tensor_dict_values(
    payload: dict[str, Any],
    rows: int,
    metadata: Mapping[str, Any],
) -> list[Any]:
    """Decode ragged_tensor_dict payload back to list[dict[str, Tensor] | None]."""
    null_mask = payload["null_mask"]
    if isinstance(null_mask, (bytes, bytearray)):
        null_mask = np.frombuffer(null_mask, dtype=np.bool_)
    keys = metadata.get("keys", [])

    key_values: dict[str, list[Any]] = {}
    for key in keys:
        prefix = f"{key}."
        sub_payload = {
            name[len(prefix) :]: payload[name]
            for name in payload
            if name.startswith(prefix)
        }
        sub_metadata = metadata.get("key_codecs", {}).get(key)
        key_values[key] = _decode_ragged_tensor_values(sub_payload, rows, sub_metadata)

    result: list[Any] = []
    for row in range(rows):
        if bool(null_mask[row]):
            result.append(None)
        else:
            d: dict[str, Any] = {}
            for key in keys:
                val = key_values[key][row]
                if val is not None:
                    d[key] = val
            result.append(d if d else None)
    return result


def _encode_typed_ragged_values(
    values: list[Any], dtype_hint: np.dtype[Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    if dtype_hint is None:
        source_arrays = [np.asarray(value) for value in values if value is not None]
        dtype = np.result_type(*source_arrays) if source_arrays else np.dtype(_TYPED_RAGGED_DEFAULT_DTYPE)
    else:
        dtype = np.dtype(dtype_hint)
    if dtype.hasobject:
        raise ValueError("typed_ragged codec requires non-object dtype")

    ndarray_encoded = _encode_typed_ragged_ndarray_rows(values, dtype)
    if ndarray_encoded is not None:
        return ndarray_encoded

    arrays = [
        np.asarray([], dtype=dtype)
        if value is None
        else _as_contiguous_array_preserve_ndim(value, dtype)
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
    total_elems = int(offset)
    if flat_arrays:
        if _concat_arrays_into is not None:
            data = _DirectCopyPayload.from_flat_arrays(flat_arrays, dtype, total_elems)
        else:
            buffers = tuple(memoryview(flat.data).cast("B") for flat in flat_arrays)
            owners = tuple(flat_arrays)
            data = _MultiBufferPayload(
                buffers=buffers, owners=owners,
                dtype=np.dtype(dtype).str, shape=(total_elems,),
            )
    else:
        empty = np.empty(0, dtype=dtype)
        data = _MultiBufferPayload(
            buffers=(memoryview(empty.data).cast("B"),), owners=(empty,),
            dtype=np.dtype(dtype).str, shape=(0,),
        )
    ndarray_rows = [
        value is not None and isinstance(value, np.ndarray) for value in values
    ]
    if all(value is None or is_array for value, is_array in zip(values, ndarray_rows)):
        row_format = "ndarray"
    elif any(ndarray_rows):
        row_format = "mixed"
    else:
        row_format = "list"
    return (
        {
            "data": data,
            "offsets": offsets,
            "shapes": shapes,
            "ndims": ndims,
            "nulls": nulls,
        },
        {
            "dtype": str(dtype),
            "max_ndim": int(max_ndim),
            "shape_policy": "ragged",
            "row_format": row_format,
            "ndarray_rows": ndarray_rows if row_format == "mixed" else None,
        },
    )


def _encode_typed_ragged_regular_ndarray_rows(
    values: list[Any], dtype: np.dtype[Any]
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    row_count = len(values)
    if row_count < 2:
        return None
    first = values[0]
    if (
        not isinstance(first, np.ndarray)
        or first.dtype != dtype
        or not first.flags.c_contiguous
    ):
        return None
    shape = first.shape
    for value in values[1:]:
        if (
            not isinstance(value, np.ndarray)
            or value.dtype != dtype
            or not value.flags.c_contiguous
            or value.shape != shape
        ):
            return None

    row_elems = int(first.size)
    data = np.asarray(values, dtype=dtype).reshape(-1)
    offsets = np.arange(row_count + 1, dtype=np.int64) * row_elems
    nulls = np.zeros(row_count, dtype=np.bool_)
    max_ndim = int(first.ndim)
    if max_ndim == 0:
        shapes = np.zeros((row_count, 1), dtype=np.int64)
        ndims = np.zeros(row_count, dtype=np.int16)
    else:
        shapes = np.empty((row_count, max_ndim), dtype=np.int64)
        shapes[:] = shape
        ndims = np.full(row_count, max_ndim, dtype=np.int16)
    return (
        {
            "data": data,
            "offsets": offsets,
            "shapes": shapes,
            "ndims": ndims,
            "nulls": nulls,
        },
        {
            "dtype": str(dtype),
            "max_ndim": max_ndim,
            "shape_policy": "ragged",
            "row_format": "ndarray",
            "ndarray_rows": None,
            "physical_layout": "contiguous_flat",
        },
    )


def _encode_typed_ragged_ndarray_rows(
    values: list[Any], dtype: np.dtype[Any]
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    regular_encoded = _encode_typed_ragged_regular_ndarray_rows(values, dtype)
    if regular_encoded is not None:
        return regular_encoded

    row_count = len(values)
    if row_count == 0:
        return None

    nulls = np.empty(row_count, dtype=np.bool_)
    lengths = np.empty(row_count, dtype=np.int64)
    arrays: list[np.ndarray] = []
    tail_shape: tuple[int, ...] | None = None
    row_ndim: int | None = None
    tail_compatible = True
    saw_scalar = False
    saw_non_scalar = False

    for row, value in enumerate(values):
        if value is None:
            nulls[row] = True
            lengths[row] = 0
            continue
        if not isinstance(value, np.ndarray):
            return None

        array = _as_contiguous_array_preserve_ndim(value, dtype)
        arrays.append(array)
        nulls[row] = False
        lengths[row] = array.size
        if array.ndim == 0:
            saw_scalar = True
            if saw_non_scalar:
                tail_compatible = False
            continue

        saw_non_scalar = True
        if saw_scalar:
            tail_compatible = False
        current_tail = array.shape[1:]
        if tail_shape is None:
            tail_shape = current_tail
            row_ndim = array.ndim
        elif array.ndim != row_ndim or current_tail != tail_shape:
            tail_compatible = False

    if tail_compatible and tail_shape is not None and any(dim == 0 for dim in tail_shape):
        tail_compatible = False

    offsets = np.empty(row_count + 1, dtype=np.int64)
    offsets[0] = 0
    np.cumsum(lengths, out=offsets[1:])
    total_elems = int(offsets[-1])
    if len(arrays) == 1:
        flat = arrays[0].reshape(-1)
        data = _MultiBufferPayload(
            buffers=(memoryview(flat.data).cast("B"),),
            owners=(flat,),
            dtype=np.dtype(dtype).str,
            shape=(total_elems,),
        )
    elif total_elems:
        if _concat_arrays_into is not None:
            flat_arrays = [arr.reshape(-1) for arr in arrays]
            data = _DirectCopyPayload.from_flat_arrays(flat_arrays, dtype, total_elems)
        else:
            flat_arrays = [arr.reshape(-1) for arr in arrays]
            buffers = tuple(memoryview(flat.data).cast("B") for flat in flat_arrays)
            data = _MultiBufferPayload(
                buffers=buffers,
                owners=tuple(flat_arrays),
                dtype=np.dtype(dtype).str,
                shape=(total_elems,),
            )
    else:
        empty = np.empty(0, dtype=dtype)
        data = _MultiBufferPayload(
            buffers=(memoryview(empty.data).cast("B"),),
            owners=(empty,),
            dtype=np.dtype(dtype).str,
            shape=(0,),
        )

    if not arrays or saw_scalar and not saw_non_scalar:
        max_ndim = 0
    elif tail_compatible:
        max_ndim = int(row_ndim or 1)
    else:
        max_ndim = max(array.ndim for array in arrays)

    ndims = np.zeros(row_count, dtype=np.int16)
    if max_ndim == 0:
        shapes = np.zeros((row_count, 1), dtype=np.int64)
    elif tail_compatible:
        shapes = np.zeros((row_count, max_ndim), dtype=np.int64)
        tail = tail_shape or ()
        tail_elems = int(np.prod(tail, dtype=np.int64)) if tail else 1
        shapes[:, 0] = lengths // tail_elems
        if max_ndim > 1:
            shapes[:, 1:] = tail
        ndims[~nulls] = max_ndim
    else:
        shapes = np.zeros((row_count, max(max_ndim, 1)), dtype=np.int64)
        array_index = 0
        for row, is_null in enumerate(nulls):
            if bool(is_null):
                continue
            array = arrays[array_index]
            array_index += 1
            ndims[row] = array.ndim
            if array.ndim > 0:
                shapes[row, : array.ndim] = array.shape

    return (
        {
            "data": data,
            "offsets": offsets,
            "shapes": shapes,
            "ndims": ndims,
            "nulls": nulls,
        },
        {
            "dtype": str(dtype),
            "max_ndim": int(max_ndim),
            "shape_policy": "ragged",
            "row_format": "ndarray",
            "ndarray_rows": None,
            "physical_layout": "contiguous_flat",
        },
    )


def _as_contiguous_array_preserve_ndim(value: Any, dtype: np.dtype[Any]) -> np.ndarray:
    if isinstance(value, np.ndarray) and value.dtype == dtype and value.flags.c_contiguous:
        return value
    array = np.asarray(value, dtype=dtype)
    if array.flags.c_contiguous:
        return array
    return np.array(array, dtype=dtype, order="C", copy=True)


def _decode_typed_ragged_values(
    payload: dict[str, Any], rows: int, metadata: Optional[Mapping[str, Any]] = None
) -> list[Any]:
    raw = payload["data"]
    dtype_str = (metadata or {}).get("dtype", _TYPED_RAGGED_DEFAULT_DTYPE)
    np_dtype = np.dtype(dtype_str)
    # data may arrive as torch.Tensor, bytes, or numpy (pool-backed or typed)
    if _torch is not None and isinstance(raw, _torch.Tensor):
        data = raw.numpy()
    elif isinstance(raw, (bytes, bytearray, memoryview)):
        data = np.frombuffer(raw, dtype=np_dtype)
    elif isinstance(raw, np.ndarray):
        data = raw if raw.dtype == np_dtype else np.frombuffer(raw, dtype=np_dtype)
    else:
        data = np.asarray(raw, dtype=np_dtype)
    offsets = payload["offsets"]
    shapes = payload["shapes"]
    ndims = payload["ndims"]
    nulls = payload["nulls"]
    metadata = metadata or {}
    row_format = metadata.get("row_format")
    if (
        row_format == "ndarray"
        and metadata.get("physical_layout") == "contiguous_flat"
    ):
        fast_values = _decode_typed_ragged_ndarray_rows_fast(
            data, offsets, shapes, ndims, nulls, rows, metadata
        )
        if fast_values is not None:
            return fast_values

    ndarray_rows = metadata.get("ndarray_rows") or []
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        begin = int(offsets[row])
        end = int(offsets[row + 1])
        ndim = int(ndims[row])
        shape = tuple(int(v) for v in shapes[row, :ndim].tolist())
        array = data[begin:end].reshape(shape)
        if row_format == "ndarray" or (
            row_format == "mixed" and bool(ndarray_rows[row])
        ):
            values.append(array)
        else:
            values.append(array.tolist())
    return values


def _decode_typed_ragged_ndarray_rows_fast(
    data: np.ndarray,
    offsets: np.ndarray,
    shapes: np.ndarray,
    ndims: np.ndarray,
    nulls: np.ndarray,
    rows: int,
    metadata: Mapping[str, Any],
) -> list[Any] | None:
    max_ndim = int(metadata.get("max_ndim", 0))
    owner = getattr(data, "_mooncake_pool_owner", None)
    source = data.view(np.ndarray) if isinstance(data, np.ndarray) else data
    has_nulls = bool(nulls.any())

    def attach_owner(values: list[Any]) -> list[Any]:
        if owner is None:
            return values
        return _OwnerBackedList(values, owner)

    if max_ndim == 1:
        if not has_nulls:
            row_width = _regular_offsets_width(offsets, rows)
            if row_width is not None:
                return attach_owner(list(source.reshape((rows, row_width))))
            begins = offsets[:-1].tolist()
            ends = offsets[1:].tolist()
            return attach_owner(
                [source[b:e] for b, e in zip(begins, ends)]
            )
        values: list[Any] = []
        begins = offsets[:-1].tolist()
        ends = offsets[1:].tolist()
        for is_null, b, e in zip(nulls, begins, ends):
            if bool(is_null):
                values.append(None)
            else:
                values.append(source[b:e])
        return attach_owner(values)

    if max_ndim <= 1 or rows == 0:
        return None

    non_null_rows = np.flatnonzero(~nulls)
    if non_null_rows.size == 0:
        return [None] * rows
    first_row = int(non_null_rows[0])
    if int(ndims[first_row]) != max_ndim:
        return None
    tail = tuple(int(v) for v in shapes[first_row, 1:max_ndim])
    if any(dim == 0 for dim in tail):
        return None
    for row in non_null_rows:
        row_index = int(row)
        if int(ndims[row_index]) != max_ndim:
            return None
        if tuple(int(v) for v in shapes[row_index, 1:max_ndim]) != tail:
            return None

    tail_elems = int(np.prod(tail, dtype=np.int64)) if tail else 1
    if tail_elems <= 0:
        return None
    if int(offsets[-1]) % tail_elems != 0:
        return None
    flat_rows = source.reshape((-1, *tail))
    first_axis_offsets = offsets // tail_elems

    if not has_nulls:
        row_width = _regular_offsets_width(first_axis_offsets, rows)
        if row_width is not None:
            return attach_owner(list(flat_rows.reshape((rows, row_width, *tail))))
        fa_begins = first_axis_offsets[:-1].tolist()
        fa_ends = first_axis_offsets[1:].tolist()
        return attach_owner(
            [flat_rows[b:e] for b, e in zip(fa_begins, fa_ends)]
        )
    values = []
    fa_begins = first_axis_offsets[:-1].tolist()
    fa_ends = first_axis_offsets[1:].tolist()
    for is_null, b, e in zip(nulls, fa_begins, fa_ends):
        if bool(is_null):
            values.append(None)
        else:
            values.append(flat_rows[b:e])
    return attach_owner(values)


def _regular_offsets_width(offsets: np.ndarray, rows: int) -> int | None:
    if rows <= 0 or int(offsets[0]) != 0:
        return None
    total = int(offsets[-1])
    if total < 0 or total % rows != 0:
        return None
    row_width = total // rows
    if not bool(np.all(offsets[1:] - offsets[:-1] == row_width)):
        return None
    return row_width


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


def _value_to_media_bytes(
    value: Any,
) -> tuple[bytes | memoryview, str | None, dict[str, Any]]:
    if value is None:
        return b"", None, {"kind": "null"}
    if _is_bytes_like(value):
        return _bytes_view(value, "media value"), None, {"kind": "bytes"}
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


def _multi_buffer_bytes_payload(
    parts: Sequence[bytes | memoryview],
) -> _MultiBufferPayload:
    buffers = tuple(_bytes_view(part, "payload part") for part in parts if part)
    return _MultiBufferPayload(buffers, tuple(parts))


def _encode_msgpack_ragged_values(
    path: str, values: list[Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    row_count = len(values)
    offsets = np.empty(row_count + 1, dtype=np.int64)
    nulls = np.empty(row_count, dtype=np.bool_)
    offsets[0] = 0
    packer = _msgpack.Packer(use_bin_type=True, strict_types=True)
    buf = bytearray()
    try:
        for i, value in enumerate(values):
            if value is None:
                nulls[i] = True
                offsets[i + 1] = offsets[i]
            else:
                buf.extend(packer.pack(value))
                nulls[i] = False
                offsets[i + 1] = len(buf)
    except TypeError as exc:
        raise ValueError(
            f"unsupported structured non-tensor field {path}: "
            "msgpack codec cannot encode value"
        ) from exc
    return (
        {"data": buf, "offsets": offsets, "nulls": nulls},
        {},
    )


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
    payload_bytes = _multi_buffer_bytes_payload(parts)
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
    data_view = memoryview(data) if not isinstance(data, memoryview) else data
    values = []
    for row, is_null, begin, end in zip(range(rows), nulls, offsets[:-1], offsets[1:]):
        if bool(is_null):
            values.append(None)
            continue
        item = data_view[begin:end]
        encoding = encodings[row] if row < len(encodings) else None
        values.append(_decode_media_bytes(item, encoding))
    return values


def _decode_msgpack_ragged_values(payload: dict[str, Any], rows: int) -> list[Any]:
    data = payload["data"]
    nulls = payload["nulls"]
    has_nulls = bool(nulls.any()) if rows > 0 else False
    raw_data = bytes(data) if not isinstance(data, bytes) else data
    if not has_nulls:
        unpacker = _msgpack.Unpacker(raw=False)
        unpacker.feed(raw_data)
        return list(unpacker)
    # With nulls: stream decode non-null values, insert None at null positions
    unpacker = _msgpack.Unpacker(raw=False)
    unpacker.feed(raw_data)
    non_null_iter = iter(unpacker)
    values = []
    for is_null in nulls:
        if bool(is_null):
            values.append(None)
        else:
            values.append(next(non_null_iter))
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
            "data": _multi_buffer_bytes_payload(parts),
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
    data_view = memoryview(data) if not isinstance(data, memoryview) else data
    values = []
    for row in range(rows):
        if bool(nulls[row]):
            values.append(None)
            continue
        items = []
        for item_index in range(int(row_offsets[row]), int(row_offsets[row + 1])):
            item = data_view[
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
        config: Any = None,
    ) -> RemoteBundleRef:
        metadata, buffers = _encode_structured_fields(payload.metadata, payload.buffers)
        transfer_policy = self._bundle_store._policy(
            policy, max_inflight_put=max_inflight_put
        )
        return self._bundle_store.put_bundle(
            meta=_encode_structured_metadata(metadata),
            buffers=buffers,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=transfer_policy,
            max_inflight_put=None,
            config=config,
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

        # Identify members eligible for batched pool read
        batch_eligible: list[tuple[str, Any]] = []
        non_batch: list[str] = []
        for name in selected:
            fs = field_specs.get(name, _BYTES_FIELD_SPEC)
            encoding = fs.get("encoding", "bytes")
            nbytes = int(buffers[name].get("bytes", 0))
            ms = member_slices.get(name)
            if destination_map.get(name) is not None or nbytes == 0:
                non_batch.append(name)
                continue
            if encoding == "ndarray" and isinstance(fs.get("dtype"), str) and isinstance(fs.get("shape"), list):
                read_plan = _resolve_ndarray_read_plan(
                    tuple(int(d) for d in fs["shape"]),
                    np.dtype(fs["dtype"]),
                    ms,
                )
                if read_plan.byte_length > 0 and read_plan.step == 1:
                    batch_eligible.append((name, read_plan))
                    continue
            # bytes-encoded members use _read_bytes_member which returns
            # _PoolBackedNdarray (zero-copy from pool); batching would require
            # converting back to bytes via bytes(arr), adding a 24GB+ copy.
            non_batch.append(name)

        objects: dict[str, Any] = {}
        # Try pool-backed range read for every eligible ndarray member.  Even a
        # single full-member read uses this path so the full-read case shares the
        # same zero-copy/partial-read transport behavior.
        if batch_eligible:
            names_for_batch = [name for name, _ in batch_eligible]
            plans_for_batch = [plan for _, plan in batch_eligible]
            batched = self._batch_read_ndarray_members(
                names_for_batch, plans_for_batch, buffers
            )
            if batched is not None:
                objects.update(batched)
                batch_eligible = []

        # Remaining members: per-member read (original path)
        remaining = [name for name, _ in batch_eligible] + non_batch
        for name in remaining:
            objects[name] = self._read_structured_member(
                name=name,
                payload_spec=buffers[name],
                field_spec=field_specs.get(name, _BYTES_FIELD_SPEC),
                member_slice=member_slices.get(name),
                destination=destination_map.get(name),
            )
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
        metadata = self._bundle_store.read_payload_range(payload_spec, 0, metadata_bytes)
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
    ) -> bytes | np.ndarray:
        if member_slice is not None:
            raise ValueError(f"structured bytes member {name} does not support slicing")
        if destination is not None:
            raise ValueError(
                f"structured bytes member {name} does not support materialize_into"
            )
        # Try pool-backed read first: pool memory is pre-registered,
        # so RDMA reads go directly into pool memory without extra copies.
        expected_bytes = int(payload_spec.get("bytes", 0))
        if expected_bytes > 0:
            result = self._bundle_store.read_payload_range_into_pool_array(
                payload_spec, np.dtype(np.uint8), (expected_bytes,), 0
            )
            if result is not None:
                return result
        # Pool unavailable or size unknown — fall back to plain bytes.
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
        if destination is None and read_plan.byte_length > 0 and read_plan.step == 1:
            target = self._bundle_store.read_payload_range_into_pool_array(
                payload_spec,
                read_plan.dtype,
                read_plan.output_shape,
                read_plan.byte_offset,
            )
            if target is not None:
                return target
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

    def _batch_read_ndarray_members(
        self,
        names: list[str],
        read_plans: list[Any],
        buffers: Mapping[str, Any],
    ) -> dict[str, np.ndarray] | None:
        """Batched pool read: single pool.acquire + single get_into_ranges.
        Returns None to fall back to per-member reads.
        """
        transport = self._bundle_store._transport
        pool = transport._ensure_buffer_pool()
        get_into_ranges = transport._get_into_ranges
        if pool is None or not callable(get_into_ranges):
            return None

        # Compute layout: each member gets a contiguous region in the pool
        offsets = []
        total_bytes = 0
        for rp in read_plans:
            offsets.append(total_bytes)
            total_bytes += rp.byte_length
        if total_bytes == 0:
            return {n: np.empty(rp.output_shape, dtype=rp.dtype)
                    for n, rp in zip(names, read_plans)}

        try:
            lease = pool.acquire(total_bytes)
        except RuntimeError:
            return None

        owner = _PoolLeaseOwner(lease)
        try:
            # Collect all fragments for a single get_into_ranges call
            all_keys, all_dst, all_src, all_sizes = [], [], [], []
            for name, rp, off in zip(names, read_plans, offsets):
                for key, _, dest_off, src_off, size in _payload_range_fragments(
                    buffers[name]["chunks"], rp.byte_offset, rp.byte_length
                ):
                    all_keys.append(key)
                    all_dst.append([off + dest_off])
                    all_src.append([src_off])
                    all_sizes.append([size])

            if not all_keys:
                owner.release()
                return {n: np.empty(rp.output_shape, dtype=rp.dtype)
                        for n, rp in zip(names, read_plans)}

            results = get_into_ranges(
                [lease.ptr], [all_keys], [all_dst], [all_src], [all_sizes]
            )
            # Validate result shape
            if len(results) != 1 or len(results[0]) != len(all_keys):
                raise RuntimeError("batch get_into_ranges: unexpected result count")
            for exp, act in zip(all_sizes, results[0]):
                if exp != act:
                    raise RuntimeError("batch get_into_ranges: size mismatch")

            return {
                name: _PoolBackedNdarray._from_shared_pool(
                    owner, rp.dtype, rp.output_shape, off
                )
                for name, rp, off in zip(names, read_plans, offsets)
            }
        except Exception:
            owner.release()
            raise


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
        config: Any = None,
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
        deferred: list | None = None
        try:
            meta_spec, meta_keys = self._put_payload(
                f"{base_key}/meta",
                meta_view,
                target_chunk_bytes,
                _copy_transfer_policy(transfer_policy),
                _deferred=deferred,
                config=config,
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
                        config,
                    )
                elif isinstance(value, _DirectCopyPayload):
                    payload_spec, payload_keys = self._put_direct_copy_payload(
                        payload_key,
                        value,
                        target_chunk_bytes,
                        transfer_policy,
                        config,
                    )
                elif isinstance(value, _MultiBufferPayload):
                    payload_spec, payload_keys = self._put_multi_buffer_payload(
                        payload_key,
                        value,
                        target_chunk_bytes,
                        transfer_policy,
                        _deferred=deferred,
                        config=config,
                    )
                else:
                    payload_spec, payload_keys = self._put_payload(
                        payload_key,
                        _bytes_view(value, name),
                        target_chunk_bytes,
                        transfer_policy,
                        _deferred=deferred,
                        config=config,
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
                _put_with_optional_config(
                    self._store, manifest_key, manifest_blob, config
                ),
                "put",
                manifest_key,
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
        config: Any = None,
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
                config=config,
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
                _put_with_optional_config(
                    self._store, manifest_key, manifest_blob, config
                ),
                "put",
                manifest_key,
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

    def read_payload_into(
        self, payload_spec: Mapping[str, Any], destination: bytearray | np.ndarray
    ) -> None:
        return self._transport.read_payload_into(payload_spec, destination)

    def read_payload_range(
        self, payload_spec: Mapping[str, Any], byte_offset: int, byte_length: int
    ) -> bytes:
        return self._transport.read_payload_range(
            payload_spec, byte_offset, byte_length
        )

    def read_payload_range_into_pool_array(
        self,
        payload_spec: Mapping[str, Any],
        dtype: np.dtype[Any],
        shape: tuple[int, ...],
        byte_offset: int,
    ) -> np.ndarray | None:
        return self._transport.read_payload_range_into_pool_array(
            payload_spec, dtype, shape, byte_offset
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
        config: Any = None,
    ) -> tuple[dict[str, Any], list[str]]:
        if isinstance(value, _TensorObjectBufferPayload):
            total_bytes = self._transport.put_tensor_object_buffer(key, value, config)
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
            tensor_spec = self._transport.put_tensor_payload_direct(key, value, config)
            if tensor_spec is not None:
                return tensor_spec, [key]
            try:
                tensor_spec = self._transport.put_tensor_payload_from_pool(
                    key, value, config
                )
                return tensor_spec, [key]
            except RuntimeError:
                pass
        if not _has_tensor_codec_helpers() or value.tensor.dim() == 0:
            payload = _torch_save_payload_bytes(value.tensor)
            payload_spec, payload_keys = self._put_payload(
                key,
                memoryview(payload),
                len(payload) or 1,
                transfer_policy,
                config=config,
            )
            payload_spec["format"] = "torch_save"
            return payload_spec, payload_keys
        payload, metadata_bytes = _tensor_payload_bytes(value)
        payload_spec, payload_keys = self._put_payload(
            key,
            memoryview(payload),
            len(payload) or 1,
            transfer_policy,
            config=config,
        )
        payload_spec["metadata_bytes"] = metadata_bytes
        return payload_spec, payload_keys

    def _put_payload(
        self,
        key: str,
        value: memoryview,
        chunk_bytes: int,
        transfer_policy: BundleTransferPolicy,
        _deferred: list | None = None,
        config: Any = None,
    ) -> tuple[dict[str, Any], list[str]]:
        if len(value) == 0:
            return {"key": key, "bytes": 0, "chunks": []}, []
        chunks = _split_view(value, chunk_bytes)
        chunk_keys = [
            key if len(chunks) == 1 else f"{key}/chunk/{index}"
            for index in range(len(chunks))
        ]
        if _deferred is not None:
            for ck, chunk in zip(chunk_keys, chunks):
                _deferred.append((ck, [chunk]))
        else:
            self._transport.put_multi_buffer_payload_chunks(
                chunk_keys,
                [[c] for c in chunks],
                transfer_policy,
                config,
            )
        payload_spec = {
            "key": key,
            "bytes": len(value),
            "chunks": [
                {"key": chunk_key, "bytes": len(chunk)}
                for chunk_key, chunk in zip(chunk_keys, chunks)
            ],
        }
        return payload_spec, list(chunk_keys)

    def _put_multi_buffer_payload(
        self,
        key: str,
        value: _MultiBufferPayload,
        chunk_bytes: int,
        transfer_policy: BundleTransferPolicy,
        _deferred: list | None = None,
        config: Any = None,
    ) -> tuple[dict[str, Any], list[str]]:
        total_bytes = value.nbytes
        if total_bytes == 0:
            return {"key": key, "bytes": 0, "chunks": []}, []
        if len(value.buffers) == 1:
            return self._put_payload(
                key,
                value.buffers[0],
                chunk_bytes,
                transfer_policy,
                _deferred,
                config=config,
            )
        chunk_groups = _split_multi_buffer_payload(value.buffers, chunk_bytes)
        chunk_keys = [
            key if len(chunk_groups) == 1 else f"{key}/chunk/{index}"
            for index in range(len(chunk_groups))
        ]
        if _deferred is not None:
            for ck, group in zip(chunk_keys, chunk_groups):
                _deferred.append((ck, list(group)))
        else:
            self._transport.put_multi_buffer_payload_chunks(
                chunk_keys,
                chunk_groups,
                transfer_policy,
                config,
            )
        payload_spec = {
            "key": key,
            "bytes": total_bytes,
            "chunks": [
                {"key": chunk_key, "bytes": sum(len(part) for part in group)}
                for chunk_key, group in zip(chunk_keys, chunk_groups)
            ],
        }
        return payload_spec, list(chunk_keys)

    def _put_direct_copy_payload(
        self,
        key: str,
        value: _DirectCopyPayload,
        chunk_bytes: int,
        transfer_policy: BundleTransferPolicy,
        config: Any = None,
    ) -> tuple[dict[str, Any], list[str]]:
        total_bytes = value.total_bytes
        if total_bytes == 0:
            return {"key": key, "bytes": 0, "chunks": []}, []
        if len(value.arrays) == 1:
            buf = memoryview(value.arrays[0].data).cast("B")
            return self._put_payload(
                key, buf, chunk_bytes, transfer_policy,
                config=config,
            )
        arrays = value.arrays
        transport = self._transport
        pool = transport._ensure_buffer_pool()
        batch_put_from = transport._batch_put_from
        if pool is None or not callable(batch_put_from):
            buf = memoryview(np.concatenate(
                [a.ravel().view(np.uint8) for a in arrays]
            ).data).cast("B")
            return self._put_payload(
                key, buf, chunk_bytes, transfer_policy, config=config
            )
        # Group arrays into chunk-sized batches
        n = len(arrays)
        chunk_batches: list[tuple[int, int, int]] = []  # (start, count, bytes)
        batch_start = 0
        batch_bytes = 0
        fallback = False
        for i in range(n):
            ab = arrays[i].nbytes
            if ab > chunk_bytes:
                fallback = True
                break
            if batch_bytes + ab > chunk_bytes and batch_bytes > 0:
                chunk_batches.append((batch_start, i - batch_start, batch_bytes))
                batch_start = i
                batch_bytes = 0
            batch_bytes += ab
        if fallback:
            buf = memoryview(np.concatenate(
                [a.ravel().view(np.uint8) for a in arrays]
            ).data).cast("B")
            return self._put_payload(
                key, buf, chunk_bytes, transfer_policy, config=config
            )
        if batch_bytes > 0:
            chunk_batches.append((batch_start, n - batch_start, batch_bytes))
        num_chunks = len(chunk_batches)
        chunk_keys = [
            key if num_chunks == 1 else f"{key}/chunk/{idx}"
            for idx in range(num_chunks)
        ]
        def _put_chunk_batch(keys, batches, nthreads=1):
            for ck, (start, count, size) in zip(keys, batches):
                lease = pool.acquire(size)
                try:
                    _concat_arrays_into(arrays, lease.ptr, start, count, nthreads)
                    results = _batch_put_from_with_optional_config(
                        batch_put_from, [ck], [lease.ptr], [size], config
                    )
                    transport._check_batch_put_results(results, [ck], "batch_put_from")
                except Exception:
                    _cleanup_keys(self._store, [ck], strict=False)
                    raise
                finally:
                    lease.release()

        max_inflight = transfer_policy.max_inflight_put
        use_parallel = (
            transfer_policy.put_mode != "batch"
            and max_inflight > 1
            and num_chunks >= AUTO_PARALLEL_MIN_CHUNKS
            and total_bytes >= AUTO_PARALLEL_MIN_BYTES
        )
        if not use_parallel:
            _put_chunk_batch(chunk_keys, chunk_batches)
        else:
            group_count = max(1, min(max_inflight, num_chunks))
            group_size = (num_chunks + group_count - 1) // group_count
            groups = [
                (chunk_keys[s:s + group_size], chunk_batches[s:s + group_size])
                for s in range(0, num_chunks, group_size)
            ]
            futures: list = []
            try:
                with ThreadPoolExecutor(max_workers=len(groups)) as executor:
                    futures = [
                        executor.submit(_put_chunk_batch, gk, gb)
                        for gk, gb in groups
                    ]
                    for f in as_completed(futures):
                        f.result()
            except Exception:
                for f in futures:
                    f.cancel()
                _cleanup_keys(self._store, chunk_keys, strict=False)
                raise
        payload_spec = {
            "key": key,
            "bytes": total_bytes,
            "chunks": [
                {"key": ck, "bytes": cb[2]}
                for ck, cb in zip(chunk_keys, chunk_batches)
            ],
        }
        return payload_spec, list(chunk_keys)

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
        self._batch_put_from_multi_buffers = getattr(
            store, "batch_put_from_multi_buffers", None
        )
        self._batch_get_into = getattr(store, "batch_get_into", None)
        self._get_into = getattr(store, "get_into", None)
        self._get_into_ranges = getattr(store, "get_into_ranges", None)

    def _store_can_auto_create_buffer_pool(self) -> bool:
        get_capsule = getattr(self._store, "_get_pyclient_capsule", None)
        if not callable(get_capsule):
            return False
        try:
            return get_capsule() is not None
        except Exception:
            return False

    def _ensure_buffer_pool(self) -> Any:
        if self._buffer_pool is None:
            if not self._store_can_auto_create_buffer_pool():
                return None
            try:
                from mooncake.buffer_pool import BufferPool

                self._buffer_pool = BufferPool(self._store)
            except (ImportError, AttributeError, RuntimeError, TypeError):
                return None
        return self._buffer_pool

    def put_multi_buffer_payload_chunks(
        self,
        chunk_keys: Sequence[str],
        chunk_groups: Sequence[Sequence[memoryview]],
        transfer_policy: BundleTransferPolicy,
        config: Any = None,
    ) -> list[str]:
        def fallback_to_direct_put() -> list[str]:
            chunks = [memoryview(b"".join(group)) for group in chunk_groups]
            return self._put_chunks_direct(chunk_keys, chunks, config)

        if transfer_policy.copy_mode == "zero_copy":
            raise RuntimeError(
                "zero-copy put requires tensor-object buffers"
            )
        if transfer_policy.copy_mode == "copy" or self._ensure_buffer_pool() is None:
            return fallback_to_direct_put()
        if all(len(group) == 1 for group in chunk_groups):
            self.batch_put_buffer_groups_from(
                chunk_keys, [[group[0]] for group in chunk_groups], config
            )
            return list(chunk_keys)
        if not callable(self._batch_put_from):
            return fallback_to_direct_put()
        put_mode = self._resolve_buffer_group_put_mode(chunk_groups, transfer_policy)
        if put_mode == "batch":
            self.batch_put_buffer_groups_from(chunk_keys, chunk_groups, config)
            return list(chunk_keys)
        return self._put_buffer_groups_parallel(
            list(chunk_keys),
            [list(group) for group in chunk_groups],
            transfer_policy.max_inflight_put,
            config,
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
        if payload_spec.get("kind") == "tensor":
            if not callable(get_tensor):
                raise RuntimeError("structured tensor payload does not support get_tensor")
            return get_tensor(key)
        if callable(get_tensor):
            try:
                return get_tensor(key)
            except KeyError:
                pass
        return _deserialize_tensor_payload(self.read_payload(payload_spec))

    def read_payload_into(
        self, payload_spec: Mapping[str, Any], destination: bytearray | np.ndarray
    ) -> None:
        chunks = payload_spec["chunks"]
        offsets = _chunk_offsets(chunks)
        if self._read_chunks_with_pool_get_into(chunks, offsets, destination):
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
        if not self._read_with_pool_into_destination(
            payload_spec["chunks"], data,
            byte_offset=byte_offset, byte_length=byte_length,
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
        if destination_pre_registered:
            base_ptr = _buffer_ptr(destination)
            if self._read_payload_range_into_raw_destination(
                chunks, base_ptr, byte_offset, byte_length
            ):
                return
        if not self._read_with_pool_into_destination(
            chunks, destination,
            byte_offset=byte_offset, byte_length=byte_length,
        ):
            self._copy_payload_range_into_destination(
                chunks, destination, byte_offset, byte_length
            )

    def read_payload_range_into_pool_array(
        self,
        payload_spec: Mapping[str, Any],
        dtype: np.dtype[Any],
        shape: tuple[int, ...],
        byte_offset: int,
    ) -> np.ndarray | None:
        nbytes = (
            int(np.prod(shape, dtype=np.int64)) * dtype.itemsize
            if shape
            else dtype.itemsize
        )
        if nbytes == 0:
            return np.empty(shape, dtype=dtype)
        pool = self._ensure_buffer_pool()
        if pool is None:
            return None
        try:
            lease = pool.acquire(nbytes)
        except RuntimeError:
            return None
        owner = _PoolLeaseOwner(lease)
        try:
            if not self._read_payload_range_into_raw_destination(
                payload_spec["chunks"],
                lease.ptr,
                byte_offset,
                nbytes,
                allow_get_into=False,
            ):
                owner.release()
                return None
            return _PoolBackedNdarray(owner, dtype, shape)
        except Exception:
            owner.release()
            raise

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
        if self._read_with_pool_into_destination(
            payload_spec["chunks"], destination, ranges=ranges
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
        if self._read_with_pool_into_destination(
            payload_spec["chunks"], destination, ranges=ranges
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
        config: Any = None,
    ) -> int:
        _validate_tensor_object_buffer_owner(value)
        _check_status(
            _put_from_with_optional_config(
                self._store, key, value.ptr, value.size, config
            ),
            "put_from",
            key,
        )
        _ = value.owner
        return value.size

    def put_tensor_payload_direct(
        self,
        key: str,
        value: _TensorPayload,
        config: Any = None,
    ) -> dict[str, Any] | None:
        if config is not None:
            return None
        put_tensor = getattr(self._store, "put_tensor", None)
        if not callable(put_tensor):
            return None
        tensor = value.tensor
        nbytes = int(getattr(tensor, "nbytes", 0))
        metadata_bytes = _tensor_metadata_size()
        total_bytes = metadata_bytes + nbytes
        _check_status(
            put_tensor(key, tensor),
            "put_tensor",
            key,
        )
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
        config: Any = None,
    ) -> dict[str, Any]:
        pool = self._ensure_buffer_pool()
        if pool is None:
            raise RuntimeError("structured tensor staging requires a BufferPool")
        tensor = value.tensor
        metadata, data_ptr, tensor_nbytes, owner = _tensor_payload_parts(value)
        metadata_bytes = len(metadata)
        total_bytes = metadata_bytes + tensor_nbytes
        lease = pool.acquire(total_bytes)
        view = None
        try:
            view = lease.buffer
            view[: len(metadata)] = metadata
            if tensor_nbytes:
                ctypes.memmove(lease.ptr + len(metadata), data_ptr, tensor_nbytes)
            view.release()
            view = None
            _check_status(
                _put_from_with_optional_config(
                    self._store, key, lease.ptr, total_bytes, config
                ),
                "put_from",
                key,
            )
            _ = owner
            return {
                "key": key,
                "kind": "tensor",
                "bytes": total_bytes,
                "dtype": str(getattr(tensor, "dtype", "")),
                "shape": list(getattr(tensor, "shape", ())),
                "chunks": [{"key": key, "bytes": total_bytes}],
                "metadata_bytes": metadata_bytes,
            }
        finally:
            if view is not None:
                view.release()
            lease.release()

    def batch_put_buffer_groups_from(
        self,
        chunk_keys: Sequence[str],
        chunk_groups: Sequence[Sequence[memoryview]],
        config: Any = None,
    ) -> None:
        batch_put_from = self._batch_put_from
        if not callable(batch_put_from):
            raise RuntimeError("batch_put_from is unavailable")
        if not chunk_keys:
            return
        sizes = [_buffer_group_nbytes(group) for group in chunk_groups]
        pool = self._buffer_pool
        # Stream one chunk at a time: acquire → memcpy → put → release.
        # This keeps pool pressure minimal (one lease per call) and allows
        # RDMA transfers to pipeline across threads.
        for chunk_key, group, size in zip(chunk_keys, chunk_groups, sizes):
            lease = pool.acquire(size)
            try:
                _copy_memoryviews_to_lease(group, lease)
                results = _batch_put_from_with_optional_config(
                    batch_put_from, [chunk_key], [lease.ptr], [size], config
                )
                self._check_batch_put_results(results, [chunk_key], "batch_put_from")
            except Exception:
                _cleanup_keys(self._store, [chunk_key], strict=False)
                raise
            finally:
                lease.release()

    @staticmethod
    def _check_batch_put_results(
        results: Sequence[int], chunk_keys: Sequence[str], operation: str
    ) -> None:
        if len(results) != len(chunk_keys):
            raise RuntimeError(
                f"{operation} returned {len(results)} results for {len(chunk_keys)} chunks"
            )
        for chunk_key, status in zip(chunk_keys, results):
            _check_status(status, operation, chunk_key)

    def _put_chunks_direct(
        self,
        chunk_keys: Sequence[str],
        chunks: Sequence[memoryview],
        config: Any = None,
    ) -> list[str]:
        written_keys: list[str] = []
        try:
            for chunk_key, chunk in zip(chunk_keys, chunks):
                _check_status(
                    _put_with_optional_config(
                        self._store, chunk_key, chunk, config
                    ),
                    "put",
                    chunk_key,
                )
                written_keys.append(chunk_key)
        except Exception:
            _cleanup_keys(self._store, written_keys, strict=False)
            raise
        return list(chunk_keys)

    def _put_buffer_groups_parallel(
        self,
        chunk_keys: list[str],
        chunk_groups: list[Sequence[memoryview]],
        max_inflight_put: int,
        config: Any = None,
    ) -> list[str]:
        groups = self._group_buffer_group_ranges(
            chunk_keys, chunk_groups, max_inflight_put
        )
        futures: list[Future[None]] = []
        try:
            with ThreadPoolExecutor(
                max_workers=min(max_inflight_put, len(groups))
            ) as executor:
                futures = [
                    executor.submit(
                        self.batch_put_buffer_groups_from,
                        group_keys,
                        group_chunks,
                        config,
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

    def _group_buffer_group_ranges(
        self,
        chunk_keys: Sequence[str],
        chunk_groups: Sequence[Sequence[memoryview]],
        max_inflight_put: int,
    ) -> list[tuple[list[str], list[Sequence[memoryview]]]]:
        if not chunk_keys:
            return []
        group_count = max(1, min(max_inflight_put, len(chunk_groups)))
        group_size = (len(chunk_groups) + group_count - 1) // group_count
        return [
            (
                list(chunk_keys[start : start + group_size]),
                list(chunk_groups[start : start + group_size]),
            )
            for start in range(0, len(chunk_groups), group_size)
        ]

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
        if isinstance(destination, np.ndarray):
            destination[offset : offset + chunk_bytes] = np.frombuffer(data, dtype=np.uint8)
        else:
            destination[offset : offset + chunk_bytes] = data

    def _read_chunks_with_pool_get_into(
        self,
        chunks: Sequence[Mapping[str, Any]],
        offsets: Sequence[int],
        destination: bytearray | np.ndarray,
    ) -> bool:
        """Pool-based batch_get_into: acquire pool → RDMA read → copy to destination."""
        batch_get_into = self._batch_get_into
        if not callable(batch_get_into):
            return False
        pool = self._ensure_buffer_pool()
        if pool is None:
            return False
        total_bytes = sum(int(c["bytes"]) for c in chunks)
        if total_bytes == 0:
            return True
        try:
            lease = pool.acquire(total_bytes)
        except RuntimeError:
            return self._read_chunks_with_chunked_pool_get_into(
                chunks, offsets, destination
            )
        try:
            keys = [c["key"] for c in chunks]
            sizes = [int(c["bytes"]) for c in chunks]
            ptrs = [lease.ptr + off for off in offsets]
            read_sizes = batch_get_into(keys, ptrs, sizes)
            if len(read_sizes) != len(keys):
                raise RuntimeError(
                    f"batch_get_into returned {len(read_sizes)} results for {len(keys)} chunks"
                )
            for key, expected, actual in zip(keys, sizes, read_sizes):
                if actual != expected:
                    raise RuntimeError(
                        f"batch_get_into: {key} expected {expected}, got {actual}"
                    )
            ctypes.memmove(
                _buffer_ptr(destination), lease.ptr, total_bytes
            )
        finally:
            lease.release()
        return True

    def _read_chunks_with_chunked_pool_get_into(
        self,
        chunks: Sequence[Mapping[str, Any]],
        offsets: Sequence[int],
        destination: bytearray | np.ndarray,
    ) -> bool:
        """Per-chunk pool acquire + get_into fallback when full acquire fails."""
        batch_get_into = self._batch_get_into
        pool = self._buffer_pool
        if not callable(batch_get_into) or pool is None:
            return False
        for chunk, offset in zip(chunks, offsets):
            size = int(chunk["bytes"])
            if size == 0:
                continue
            try:
                lease = pool.acquire(size)
            except RuntimeError:
                return False
            try:
                read_sizes = batch_get_into([chunk["key"]], [lease.ptr], [size])
                if len(read_sizes) != 1 or read_sizes[0] != size:
                    raise RuntimeError(
                        f"batch_get_into: {chunk['key']} expected {size}, got {read_sizes}"
                    )
                ctypes.memmove(
                    _buffer_ptr(destination) + offset, lease.ptr, size
                )
            finally:
                lease.release()
        return True

    def _read_with_pool_into_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: bytearray | np.ndarray,
        *,
        byte_offset: int = 0,
        byte_length: int | None = None,
        ranges: Sequence[tuple[int, int, int]] | None = None,
    ) -> bool:
        """Pool-based read: acquire pool → RDMA read → copy to destination.

        Single-range mode (ranges is None): read byte_length bytes at byte_offset.
        Multi-range mode (ranges provided): read scattered ranges into destination.
        """
        pool = self._ensure_buffer_pool()
        if pool is None:
            return False
        if ranges is not None:
            if not callable(self._get_into_ranges):
                return False
            total_bytes = _buffer_nbytes(destination)
        else:
            if byte_length is None:
                return False
            total_bytes = byte_length
        try:
            lease = pool.acquire(total_bytes)
        except RuntimeError:
            # Full acquire failed; fall back to chunked pool read when
            # in single-range mode (no ranges) and reading from offset 0.
            if ranges is None and byte_offset == 0:
                offsets = _chunk_offsets(chunks)
                return self._read_chunks_with_chunked_pool_get_into(
                    chunks, offsets, destination
                )
            return False
        try:
            if ranges is not None:
                ok = self._read_payload_ranges_into_raw_destination(
                    chunks, lease.ptr, ranges
                )
            else:
                ok = self._read_payload_range_into_raw_destination(
                    chunks, lease.ptr, byte_offset, total_bytes
                )
            if not ok:
                return False
            ctypes.memmove(
                _buffer_ptr(destination), lease.ptr, total_bytes
            )
            return True
        except Exception:
            return False
        finally:
            lease.release()

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
                _payload_range_fragments(
                    chunks, source_offset, byte_length, destination_offset
                )
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
        return self._read_payload_ranges_into_raw_destination(
            chunks,
            base_ptr,
            [(byte_offset, destination_offset, byte_length)],
        )

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
                source_slice = data[fragment_source_offset : fragment_source_offset + size]
                if isinstance(destination, np.ndarray):
                    destination[
                        fragment_destination_offset : fragment_destination_offset + size
                    ] = np.frombuffer(source_slice, dtype=np.uint8)
                else:
                    destination[
                        fragment_destination_offset : fragment_destination_offset + size
                    ] = source_slice

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

    def _resolve_buffer_group_put_mode(
        self,
        chunk_groups: Sequence[Sequence[memoryview]],
        transfer_policy: BundleTransferPolicy,
    ) -> Literal["batch", "parallel"]:
        if transfer_policy.put_mode == "parallel":
            return "parallel"
        if transfer_policy.put_mode == "batch":
            return "batch"
        if transfer_policy.max_inflight_put <= 1:
            return "batch"
        if len(chunk_groups) < AUTO_PARALLEL_MIN_CHUNKS:
            return "batch"
        total_bytes = sum(_buffer_group_nbytes(group) for group in chunk_groups)
        if total_bytes < AUTO_PARALLEL_MIN_BYTES:
            return "batch"
        if min(transfer_policy.max_inflight_put, len(chunk_groups)) < 2:
            return "batch"
        return "parallel"

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


def _split_multi_buffer_payload(
    buffers: Sequence[memoryview], chunk_bytes: int
) -> list[list[memoryview]]:
    if len(buffers) == 1:
        return [[chunk] for chunk in _split_view(buffers[0], chunk_bytes)]
    groups: list[list[memoryview]] = []
    current: list[memoryview] = []
    current_bytes = 0
    for buffer in buffers:
        offset = 0
        while offset < len(buffer):
            remaining = chunk_bytes - current_bytes
            part = buffer[offset : offset + remaining]
            current.append(part)
            current_bytes += len(part)
            offset += len(part)
            if current_bytes == chunk_bytes:
                groups.append(current)
                current = []
                current_bytes = 0
    if current:
        groups.append(current)
    return groups


def _buffer_group_nbytes(buffers: Sequence[memoryview]) -> int:
    return sum(len(buffer) for buffer in buffers)


def _copy_memoryviews(buffers: Sequence[memoryview], destination: memoryview) -> None:
    dst_arr = np.frombuffer(destination, dtype=np.uint8)
    dst_ptr = dst_arr.ctypes.data
    offset = 0
    for buffer in buffers:
        n = len(buffer)
        if n == 0:
            continue
        src_arr = np.frombuffer(buffer, dtype=np.uint8)
        ctypes.memmove(dst_ptr + offset, src_arr.ctypes.data, n)
        offset += n


def _copy_memoryviews_to_lease(buffers: Sequence[memoryview], lease: Any) -> None:
    copy_from_buffers = getattr(lease, "copy_from_buffers", None)
    if callable(copy_from_buffers):
        copy_from_buffers(buffers)
        return
    view = lease.buffer
    try:
        _copy_memoryviews(buffers, view)
    finally:
        view.release()


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
    if isinstance(value, _TensorPayload):
        return _encode_torch_tensor_field(value.tensor)
    if isinstance(value, (_DirectCopyPayload, _MultiBufferPayload)):
        if value.dtype is not None and value.shape is not None:
            return {
                "encoding": "ndarray",
                "dtype": value.dtype,
                "shape": list(value.shape),
            }, value
        return _BYTES_FIELD_SPEC, value
    if _torch is not None and isinstance(value, _torch.Tensor):
        return _encode_torch_tensor_field(value)
    if isinstance(value, np.ndarray):
        array = np.ascontiguousarray(value)
        return {
            "encoding": "ndarray",
            "dtype": array.dtype.str,
            "shape": list(array.shape),
        }, array.view(np.uint8).reshape(-1)
    return _BYTES_FIELD_SPEC, value


def _encode_torch_tensor_field(value: Any) -> tuple[dict[str, Any], Any]:
    return {
        "encoding": "torch_tensor",
        "dtype": str(value.dtype),
        "shape": list(getattr(value, "shape", ())),
        "element_size": int(value.element_size()),
    }, _TensorPayload(tensor=value)


def _tensor_payload_parts(value: _TensorPayload) -> tuple[bytes, int, int, Any]:
    metadata, data_ptr, tensor_nbytes, owner = _tensor_codec_helper(
        "_serialize_tensor"
    )(value.tensor)
    return bytes(metadata), int(data_ptr), int(tensor_nbytes), owner


def _has_tensor_codec_helpers() -> bool:
    mooncake_store = _get_mooncake_store()
    return (
        mooncake_store is not None
        and callable(getattr(mooncake_store, "_serialize_tensor", None))
        and callable(getattr(mooncake_store, "_deserialize_tensor", None))
    )


def _tensor_metadata_size() -> int:
    mooncake_store = _get_mooncake_store()
    helper = (
        None
        if mooncake_store is None
        else getattr(mooncake_store, "_tensor_metadata_size", None)
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
    mooncake_store = _get_mooncake_store()
    helper = None if mooncake_store is None else getattr(mooncake_store, name, None)
    if not callable(helper):
        raise RuntimeError(
            "mooncake.store tensor serialization helpers are required for structured tensor fields"
        )
    return helper


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
        return json.dumps(value, default=_json_default, separators=(",", ":")).encode(
            "utf-8"
        )
    except TypeError as error:
        raise TypeError(f"{label} must be JSON-serializable") from error


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    if hasattr(value, "__int__"):
        return int(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _decode_json_dict(payload: bytes, label: str) -> dict[str, Any]:
    value = json.loads(payload.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{label} must decode to a dict")
    return value


def _call_write_with_optional_config(
    fn: Any, *args: Any, config: Any = None
) -> Any:
    if config is None:
        return fn(*args)
    return fn(*args, config=config)


def _put_with_optional_config(
    store: BundleStore, key: str, value: Any, config: Any = None
) -> int:
    return _call_write_with_optional_config(store.put, key, value, config=config)


def _put_from_with_optional_config(
    store: BundleStore, key: str, ptr: int, size: int, config: Any = None
) -> int:
    put_tensor_from = getattr(store, "put_tensor_from", None)
    if config is None and callable(put_tensor_from):
        return put_tensor_from(key, ptr, size)
    put_from = getattr(store, "put_from", None)
    if callable(put_from):
        return _call_write_with_optional_config(
            put_from, key, ptr, size, config=config
        )
    raise RuntimeError("put_from is unavailable")


def _batch_put_from_with_optional_config(
    batch_put_from: Any,
    keys: Sequence[str],
    ptrs: Sequence[int],
    sizes: Sequence[int],
    config: Any = None,
) -> Sequence[int]:
    return _call_write_with_optional_config(
        batch_put_from, keys, ptrs, sizes, config=config
    )


def _check_status(status: Any, operation: str, key: str) -> None:
    if status not in (None, 0):
        raise RuntimeError(f"{operation} failed for {key}: {status}")


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
# Internal helpers: torch, codec types, sentinel, decode utilities
# ---------------------------------------------------------------------------

try:
    import torch as _torch
except Exception:  # pragma: no cover
    _torch = None  # type: ignore[assignment]


def _parse_torch_dtype(dtype_str: str) -> Any:
    """Parse torch dtype string (e.g. 'torch.float32') to torch.dtype object."""
    name = dtype_str.removeprefix("torch.")
    dtype = getattr(_torch, name, None)
    if dtype is None or not isinstance(dtype, _torch.dtype):
        raise ValueError(f"unknown torch dtype: {dtype_str}")
    return dtype


def _torch_dtype_to_numpy(dtype_str: str) -> np.dtype:
    """Convert torch dtype string to numpy dtype for buffer interpretation."""
    torch_dtype = _parse_torch_dtype(dtype_str)
    # bfloat16 has no numpy equivalent; use float16 (same element size)
    if torch_dtype == _torch.bfloat16:
        return np.dtype(np.float16)
    return _torch.empty(0, dtype=torch_dtype).numpy().dtype


@dataclass
class _CodecDecision:
    accepted: bool
    codec: str
    reason: str
    normalized_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _EncodedStructuredLeaf:
    codec: str
    rows: int
    payload: dict[str, Any]
    metadata: dict[str, Any]


@dataclass
class _InferredLeaf:
    path: str
    values: list[Any]
    decision: _CodecDecision


@dataclass
class _InferredNode:
    path: str
    node_type: str
    children: list[Any]
    lengths: list[int] | None = None
    row_mask: list[bool] | None = None


class _Missing:
    """Sentinel for dict keys absent from a row (distinct from value-is-None).

    Kept for backward-compatible decoding of structured_recursive payloads.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<MISSING>"


MISSING = _Missing()


def _escape_key(key: str) -> str:
    return key.replace("\\", "\\\\").replace(".", "\\.").replace("[", "\\[")


def _is_pil_image(value: Any) -> bool:
    module = getattr(value.__class__, "__module__", None) or ""
    return module.startswith("PIL.") and hasattr(value, "save")


def _is_bytes_like(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview))


def _non_null(values: list[Any]) -> list[Any]:
    return [v for v in values if v is not None and not isinstance(v, _Missing)]


def _is_media_list(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and all(
        _is_pil_image(item) or _is_bytes_like(item) for item in value
    )


def _choose_leaf_codec(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "msgpack_ragged", "all rows are null", "msgpack")
    if _torch is not None and all(isinstance(value, _torch.Tensor) for value in nn):
        dtype = str(nn[0].dtype)
        return _CodecDecision(True, "ragged_tensor", "all non-null rows are Tensor", "torch.Tensor", {"dtype": dtype})
    if all(_is_media_list(value) for value in nn):
        return _CodecDecision(True, "media_list_ragged", "all rows are media lists", "media list")
    if all(isinstance(value, np.ndarray) for value in nn):
        if all(np.issubdtype(value.dtype, np.number) for value in nn):
            return _CodecDecision(
                True,
                "typed_ragged",
                "all rows are numeric ndarray",
                "numeric sequence",
                {"recursive_source": "ndarray"},
            )
        return _CodecDecision(False, "msgpack_ragged", "object ndarray rows", "python object")
    if all(isinstance(value, (list, tuple)) for value in nn):
        try:
            dtypes = [np.asarray(value).dtype for value in nn]
            dtype = np.result_type(*dtypes)
        except (TypeError, ValueError, OverflowError):
            dtype = None
        if dtype is not None and np.issubdtype(dtype, np.number):
            return _CodecDecision(True, "typed_ragged", "all rows are numeric sequences", "numeric sequence", {"dtype": str(dtype)})
    if all(isinstance(value, (bool, int, float, np.number)) for value in nn):
        dtype = np.result_type(*nn)
        return _CodecDecision(True, "ndarray", "all rows are numeric scalar", "numeric scalar", {"dtype": str(dtype)})
    if all(_is_bytes_like(value) for value in nn):
        return _CodecDecision(True, "bytes_ragged", "all rows are bytes-like", "bytes-like")
    if all(_is_pil_image(value) for value in nn):
        return _CodecDecision(True, "media_bytes", "all rows are media", "media")
    if all(isinstance(value, str) for value in nn):
        return _CodecDecision(True, "utf8_ragged", "all rows are str", "str")
    if all(isinstance(value, (dict, list, tuple)) for value in nn):
        return _CodecDecision(True, "msgpack_ragged", "all rows are msgpack-like", "msgpack")
    return _CodecDecision(False, "msgpack_ragged", "no codec matched", "python object")


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


def _structured_path_tokens(path: str) -> list[Any]:
    tokens: list[Any] = []
    index = 0
    current: list[str] = []
    while index < len(path):
        char = path[index]
        if char == "\\" and index + 1 < len(path):
            current.append(path[index + 1])
            index += 2
            continue
        if char == ".":
            if current:
                tokens.append("".join(current))
                current = []
            index += 1
            continue
        if char == "[":
            if current:
                tokens.append("".join(current))
                current = []
            end = path.index("]", index)
            tokens.append(int(path[index + 1 : end]))
            index = end + 1
            continue
        current.append(char)
        index += 1
    if current:
        tokens.append("".join(current))
    return tokens


def _lookup_structured_path(value: Any, root_path: str, target_path: str) -> Any:
    suffix = target_path[len(root_path) :]
    if suffix.startswith("."):
        suffix = suffix[1:]
    if not suffix:
        return value
    current = value
    for token in _structured_path_tokens(suffix):
        if isinstance(current, _Missing):
            return MISSING
        if isinstance(token, str):
            if not isinstance(current, dict) or token not in current:
                return MISSING
            current = current[token]
        else:
            if not isinstance(current, (list, tuple)) or token >= len(current):
                return MISSING
            current = current[token]
    return current
