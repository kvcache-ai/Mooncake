from __future__ import annotations

import ctypes
import json
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Literal, Mapping, Optional, Protocol, Sequence

import numpy as np

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


class MooncakeBundleTransfer:
    """Transfer structured objects through Mooncake, with a low-level bundle fallback."""

    def __init__(
        self,
        store: BundleStore,
        key_prefix: str = "bundle",
        default_chunk_bytes: int = DEFAULT_BUNDLE_CHUNK_BYTES,
    ) -> None:
        """Initialize a bundle transfer helper with a configurable default chunk size."""
        self.store = store
        self.key_prefix = _normalize_key_prefix(key_prefix)
        self.default_chunk_bytes = _validate_chunk_bytes(default_chunk_bytes)
        self._transport = _MooncakePayloadTransport(store)
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
    ) -> RemoteBundleRef:
        """Store a structured object described by JSON metadata plus named members."""
        return self._structured_store.put_structured_object(
            payload=payload,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
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
    ) -> RemoteBundleRef:
        metadata, buffers = _encode_structured_fields(payload.metadata, payload.buffers)
        return self._bundle_store.put_bundle(
            meta=_encode_structured_metadata(metadata),
            buffers=buffers,
            partition=partition,
            chunk_bytes=chunk_bytes,
            policy=policy,
            max_inflight_put=max_inflight_put,
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
        if encoding != "ndarray":
            raise ValueError(f"unsupported structured field encoding: {encoding}")
        return self._read_ndarray_member(
            name, payload_spec, field_spec, member_slice, destination
        )

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
                transfer_policy,
                pre_registered=False,
            )
            written_keys.extend(meta_keys)
            for name, value in buffers.items():
                _validate_key_segment(name, "buffer name")
                payload_view = _bytes_view(value, name)
                payload_spec, payload_keys = self._put_payload(
                    f"{base_key}/buffer/{name}",
                    payload_view,
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

    def remove_bundle(self, ref: RemoteBundleRef | Mapping[str, Any]) -> None:
        manifest = self.resolve_manifest(ref)
        keys = self._payload_keys(manifest)
        keys.append(self._manifest_key(ref, manifest))
        _cleanup_keys(self._store, keys, strict=True)

    def resolve_manifest(
        self, ref: RemoteBundleRef | Mapping[str, Any]
    ) -> dict[str, Any]:
        if isinstance(ref, RemoteBundleRef):
            manifest = ref.manifest
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

    def _put_payload(
        self,
        key: str,
        value: memoryview,
        chunk_bytes: int,
        transfer_policy: BundleTransferPolicy,
        pre_registered: bool,
    ) -> tuple[dict[str, Any], list[str]]:
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
                max_inflight_put=max_inflight_put, put_mode=result.put_mode
            )
        if result.max_inflight_put < 1:
            raise ValueError("max_inflight_put must be positive")
        if result.put_mode not in {"auto", "batch", "parallel"}:
            raise ValueError(f"unsupported put_mode: {result.put_mode}")
        return result

    def _validate_manifest(self, manifest: Mapping[str, Any]) -> None:
        if manifest.get("version") != 1 or manifest.get("layout") != "bundle":
            raise ValueError("invalid bundle manifest")
        object_id = manifest.get("object_id")
        if not isinstance(object_id, str):
            raise ValueError("bundle manifest object_id must be a string")
        base_key = f"{self._key_prefix}/{object_id}"
        self._validate_payload_spec(manifest.get("meta"), base_key)
        buffers = manifest.get("buffers")
        if not isinstance(buffers, dict):
            raise ValueError("bundle manifest buffers must be a dict")
        for name, payload_spec in buffers.items():
            _validate_key_segment(name, "buffer name")
            self._validate_payload_spec(payload_spec, base_key)

    def _validate_payload_spec(self, payload_spec: Any, base_key: str) -> None:
        if not isinstance(payload_spec, dict):
            raise ValueError("bundle payload spec must be a dict")
        payload_key = payload_spec.get("key")
        if not isinstance(payload_key, str) or not payload_key.startswith(
            f"{base_key}/"
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

    def __init__(self, store: BundleStore) -> None:
        self._store = store
        self._batch_put_from = getattr(store, "batch_put_from", None)
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
        if not self._has_batch_put_support():
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

    def read_payload_into(
        self, payload_spec: Mapping[str, Any], destination: bytearray | np.ndarray
    ) -> None:
        chunks = payload_spec["chunks"]
        offsets = _chunk_offsets(chunks)
        if self._read_chunks_with_batch_get_into(chunks, offsets, destination):
            return
        for offset, chunk in zip(offsets, chunks):
            self._read_chunk_with_get(chunk, destination, offset)

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
        get_into = self._get_into
        can_use_get_into = len(chunks) == 1 and byte_offset == 0 and callable(get_into)
        get_into_ranges = self._get_into_ranges
        can_use_get_into_ranges = callable(get_into_ranges)
        if not self._has_buffer_registration_support():
            return False
        if not can_use_get_into and not can_use_get_into_ranges:
            return False
        with self._registered_buffer(
            destination,
            "structured ndarray payload",
            pre_registered=destination_pre_registered,
        ) as base_ptr:
            if can_use_get_into and int(chunks[0]["bytes"]) == byte_length:
                expected_size = int(chunks[0]["bytes"])
                read_size = get_into(chunks[0]["key"], base_ptr, expected_size)
                if read_size != expected_size:
                    raise RuntimeError(
                        f"get_into failed for {chunks[0]['key']}: expected {expected_size}, got {read_size}"
                    )
                return True
            if not can_use_get_into_ranges:
                return False
            fragments = _payload_range_fragments(chunks, byte_offset, byte_length)
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

    def _copy_payload_range_into_destination(
        self,
        chunks: Sequence[Mapping[str, Any]],
        destination: np.ndarray,
        byte_offset: int,
        byte_length: int,
    ) -> None:
        for (
            key,
            chunk_size,
            destination_offset,
            source_offset,
            size,
        ) in _payload_range_fragments(
            chunks,
            byte_offset,
            byte_length,
        ):
            data = self._store.get(key)
            if len(data) != chunk_size:
                raise RuntimeError(
                    f"get failed for {key}: expected {chunk_size} bytes, got {len(data)}"
                )
            fragment = memoryview(data)[source_offset : source_offset + size]
            target_end = destination_offset + size
            destination[destination_offset:target_end] = np.frombuffer(
                fragment, dtype=np.uint8
            )

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
    if not isinstance(destination, np.ndarray):
        raise TypeError(
            f"structured ndarray field {name} destination must be a numpy.ndarray"
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
                    overlap_start - byte_offset,
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


def _encode_structured_field(value: Any) -> tuple[dict[str, Any], Any]:
    if isinstance(value, np.ndarray):
        array = np.ascontiguousarray(value)
        return {
            "encoding": "ndarray",
            "dtype": array.dtype.str,
            "shape": list(array.shape),
        }, array.view(np.uint8).reshape(-1)
    return {"encoding": "bytes"}, value


def _structured_field_specs(metadata: Mapping[str, Any]) -> dict[str, Any]:
    field_specs = metadata.get(STRUCTURED_FIELD_SPECS_KEY, {})
    if not isinstance(field_specs, dict):
        raise ValueError("structured field specs must be a dict")
    return field_specs


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
class _InferredNode:
    path: str
    node_type: str
    children: list[Any]
    lengths: list[int] | None = None


def _non_null(values: list[Any]) -> list[Any]:
    return [v for v in values if v is not None]


def _is_pil_image(value: Any) -> bool:
    module = getattr(value.__class__, "__module__", None) or ""
    return module.startswith("PIL.") and hasattr(value, "save")


def _is_bytes_like(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview))


def _is_media_list(value: Any) -> bool:
    return (
        isinstance(value, (list, tuple))
        and len(value) > 0
        and all(_is_pil_image(item) or _is_bytes_like(item) for item in value)
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
        return _CodecDecision(False, codec, f"not all rows are {normalized_type}", normalized_type)
    return _CodecDecision(True, codec, f"all non-null rows are {normalized_type}", normalized_type)


def _can_tensor(values: list[Any]) -> _CodecDecision:
    if _torch is None:
        return _CodecDecision(False, "ragged_tensor", "torch is not available", "torch.Tensor")
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "ragged_tensor", "all rows are null", "torch.Tensor")
    if not all(isinstance(v, _torch.Tensor) for v in nn):
        return _CodecDecision(False, "ragged_tensor", "not all rows are Tensor", "torch.Tensor")
    dtypes = sorted({str(v.dtype) for v in nn})
    if len(dtypes) != 1:
        return _CodecDecision(False, "ragged_tensor", f"mixed dtypes: {dtypes}", "torch.Tensor")
    ndims = {v.ndim for v in nn}
    if len(ndims) != 1:
        return _CodecDecision(False, "ragged_tensor", f"mixed dimensions: {ndims}", "torch.Tensor")
    return _CodecDecision(True, "ragged_tensor", "all non-null rows are Tensor", "torch.Tensor", {"dtype": dtypes[0]})


def _can_numeric_sequence(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "typed_ragged", "all rows are null", "numeric sequence")
    dtypes = []
    for v in nn:
        if isinstance(v, np.ndarray):
            arr = v
        elif isinstance(v, (list, tuple)):
            try:
                arr = np.asarray(v)
            except (ValueError, TypeError):
                return _CodecDecision(False, "typed_ragged", "row cannot be converted to ndarray", "numeric sequence")
        else:
            return _CodecDecision(False, "typed_ragged", "row is not array-like", "numeric sequence")
        if arr.dtype == object or not np.issubdtype(arr.dtype, np.number):
            return _CodecDecision(False, "typed_ragged", f"non-numeric dtype: {arr.dtype}", "numeric sequence")
        dtypes.append(arr.dtype)
    dtype = np.result_type(*dtypes)
    return _CodecDecision(True, "typed_ragged", "all rows promote to common numeric dtype", "numeric sequence", {"dtype": str(dtype)})


def _can_numeric_scalar(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "ndarray", "all rows are null", "numeric scalar")
    if not all(isinstance(v, (bool, int, float, np.number)) for v in nn):
        return _CodecDecision(False, "ndarray", "not all rows are numeric scalar", "numeric scalar")
    try:
        dtype = np.result_type(*nn)
    except (TypeError, ValueError, OverflowError):
        return _CodecDecision(False, "ndarray", "cannot determine common dtype", "numeric scalar")
    if not np.issubdtype(dtype, np.number) and not np.issubdtype(dtype, np.bool_):
        return _CodecDecision(False, "ndarray", f"non-numeric dtype: {dtype}", "numeric scalar")
    return _CodecDecision(True, "ndarray", "all rows are numeric scalar", "numeric scalar", {"dtype": str(dtype)})


def _can_json(values: list[Any]) -> _CodecDecision:
    nn = _non_null(values)
    if not nn:
        return _CodecDecision(False, "json_ragged", "all rows are null", "json")
    total_bytes = 0
    for v in nn[:_INFER_MAX_SAMPLE_ROWS]:
        try:
            total_bytes += len(json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        except (TypeError, ValueError):
            return _CodecDecision(False, "json_ragged", "serialization failed", "json")
    if total_bytes > _INFER_MAX_JSON_BYTES:
        return _CodecDecision(False, "json_ragged", "sampled payload too large", "json")
    return _CodecDecision(True, "json_ragged", "sampled rows pass JSON serialization", "json")


_CODEC_PREDICATES: tuple[Any, ...] = (
    _can_tensor,
    lambda v: _check_all(v, _is_media_list, "media_list_ragged", "media list"),
    _can_numeric_sequence,
    _can_numeric_scalar,
    lambda v: _check_all(v, _is_bytes_like, "bytes_ragged", "bytes-like"),
    lambda v: _check_all(v, _is_pil_image, "media_bytes", "media"),
    lambda v: _check_all(v, lambda x: isinstance(x, str), "utf8_ragged", "str"),
    _can_json,
)


def _choose_leaf_codec(values: list[Any]) -> _CodecDecision:
    for predicate in _CODEC_PREDICATES:
        decision = predicate(values)
        if decision.accepted:
            return decision
    return _CodecDecision(False, "pickle_ragged_fallback", "no optimized codec matched", "python object")


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
    if not all(item is None or isinstance(item, (dict, list, tuple)) for v in nn for item in v):
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
    """
    if _depth > _INFER_MAX_DEPTH:
        raise ValueError(f"infer_structure exceeded max depth {_INFER_MAX_DEPTH} at {path!r}")
    dict_keys = _try_expand_dict(values)
    if dict_keys is not None:
        nodes.append(_InferredNode(path, "dict", dict_keys))
        for key in dict_keys:
            child = [v.get(key) if isinstance(v, dict) else None for v in values]
            infer_structure(f"{path}.{key}", child, leaves, nodes, _depth=_depth + 1)
        return
    list_result = _try_expand_list(values)
    if list_result is not None:
        max_len, lengths = list_result
        nodes.append(_InferredNode(path, "list", list(range(max_len)), lengths))
        for index in range(max_len):
            child = [
                v[index] if isinstance(v, (list, tuple)) and index < len(v) else None
                for v in values
            ]
            infer_structure(f"{path}[{index}]", child, leaves, nodes, _depth=_depth + 1)
        return
    leaves.append(_InferredLeaf(path, values, _choose_leaf_codec(values)))
