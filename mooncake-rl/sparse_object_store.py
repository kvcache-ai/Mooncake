"""Mooncake Store facade for COO sparse structured objects."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
import threading
from typing import Any, Protocol

from mooncake.reshard.sparse.planner import (
    Box,
    Placement,
    SparseObjectIndex,
    SparseObjectPlan,
    SparseObjectRegion,
    SparseObjectStorePlanner,
    canonical_source_placement,
    is_canonical_source,
    normalize_placement,
    normalize_shape,
    object_ref_key,
)
from mooncake.reshard.weight import TensorDescriptor


@dataclass(frozen=True)
class StoredSparseObject:
    """Reference and compact metadata returned by ``put_sparse_object``."""

    ref: object
    metadata: Mapping[str, Any]
    index: SparseObjectIndex


@dataclass(frozen=True)
class StoredTargetSparseObject:
    """Reference and metadata for a target-local sparse object."""

    ref: object
    metadata: Mapping[str, Any]
    nnz: int


class SparseGenerationError(RuntimeError):
    """Base class for target-side sparse update fencing failures."""


class StaleSparseGenerationError(SparseGenerationError):
    """Raised when an update is older than the generation already applied."""


class SparseGenerationMismatchError(SparseGenerationError):
    """Raised when an incremental update does not extend the current base."""


class SparseGenerationNotInitializedError(SparseGenerationError):
    """Raised when a target has no controller-seeded current generation."""


class SparseGenerationInProgressError(SparseGenerationError):
    """Raised when the same target is already applying another update."""


@dataclass(frozen=True)
class _GenerationLease:
    target_key: str
    base_generation: int
    delta_generation: int
    update_key: str | None


class SparseGenerationFence:
    """Process-local generation fence for idempotent sparse target apply.

    The shared control plane must persist the same ``target_key`` and current
    generation for a multi-process deployment.  This helper deliberately keeps
    that policy small and deterministic: an update must start at the current
    generation, equal-generation retries are no-ops, and a newer update cannot
    overlap an in-flight update for the same target.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: dict[str, int] = {}
        self._applied_update_keys: dict[str, str | None] = {}
        self._inflight: set[str] = set()

    def current(self, target_key: str) -> int | None:
        with self._lock:
            return self._current.get(target_key)

    def set_current(self, target_key: str, generation: int) -> None:
        """Seed the generation of a full/dense target already in memory."""

        if not isinstance(target_key, str) or not target_key:
            raise ValueError("target_key must be a non-empty string")
        if type(generation) is not int or generation < 0:
            raise ValueError("generation must be a non-negative integer")
        with self._lock:
            if target_key in self._inflight:
                raise SparseGenerationInProgressError(
                    f"{target_key!r} already has an in-flight update"
                )
            current = self._current.get(target_key)
            if current is not None and generation < current:
                raise StaleSparseGenerationError(
                    f"{target_key!r} is at generation {current}, "
                    f"cannot reset it to {generation}"
                )
            self._current[target_key] = generation
            self._applied_update_keys.pop(target_key, None)

    def begin(
        self,
        target_key: str,
        base_generation: int,
        delta_generation: int,
        update_key: str | None = None,
    ) -> _GenerationLease | None:
        if not isinstance(target_key, str) or not target_key:
            raise ValueError("target_key must be a non-empty string")
        if (
            type(base_generation) is not int
            or type(delta_generation) is not int
            or base_generation < 0
            or delta_generation <= base_generation
        ):
            raise ValueError("invalid sparse update generation")
        if update_key is not None and (
            not isinstance(update_key, str) or not update_key
        ):
            raise ValueError("update_key must be a non-empty string")
        with self._lock:
            current = self._current.get(target_key)
            if current is None:
                raise SparseGenerationNotInitializedError(
                    f"{target_key!r} has no initialized generation"
                )
            if current is not None:
                if delta_generation < current:
                    raise StaleSparseGenerationError(
                        f"{target_key!r} is at generation {current}, "
                        f"update ends at {delta_generation}"
                    )
                if delta_generation == current and base_generation < current:
                    applied_key = self._applied_update_keys.get(target_key)
                    # An equal-generation retry is a no-op only when the
                    # committed update identity is reproduced exactly.
                    # Without an identity key we cannot distinguish a retry
                    # from a different payload at the same generation.
                    if (
                        applied_key is None
                        or update_key is None
                        or applied_key != update_key
                    ):
                        raise SparseGenerationMismatchError(
                            f"{target_key!r} already committed a different "
                            f"update at generation {current}"
                        )
                    return None
                if base_generation != current:
                    raise SparseGenerationMismatchError(
                        f"{target_key!r} is at generation {current}, "
                        f"update starts at {base_generation}"
                    )
            if target_key in self._inflight:
                raise SparseGenerationInProgressError(
                    f"{target_key!r} already has an in-flight update"
                )
            self._inflight.add(target_key)
            return _GenerationLease(
                target_key, base_generation, delta_generation, update_key
            )

    def commit(self, lease: _GenerationLease) -> None:
        with self._lock:
            if lease.target_key not in self._inflight:
                raise RuntimeError("sparse generation lease is not in flight")
            self._inflight.remove(lease.target_key)
            self._current[lease.target_key] = lease.delta_generation
            self._applied_update_keys[lease.target_key] = lease.update_key

    def abort(self, lease: _GenerationLease) -> None:
        with self._lock:
            self._inflight.discard(lease.target_key)


class StructuredObjectBackend(Protocol):
    """Subset of Mooncake structured-object APIs used by this facade."""

    def put_structured_object(self, payload: object, **kwargs: Any) -> object: ...

    def read_spec(self, ref: object) -> Any: ...

    def materialize(self, spec: Any) -> Any: ...


def _result_metadata_and_objects(
    result: Any,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Accept ``StructuredObjectResult`` and simple test doubles alike."""

    if hasattr(result, "metadata") and hasattr(result, "objects"):
        return result.metadata, result.objects
    if isinstance(result, Mapping):
        metadata = result.get("metadata", {})
        objects = result.get("objects", result)
        if isinstance(metadata, Mapping) and isinstance(objects, Mapping):
            return metadata, objects
    raise TypeError(
        "structured object materialize result must expose metadata and objects"
    )


class SparseObjectStore:
    """Sparse object-store facade built on Mooncake structured objects.

    The facade owns sparse-object encoding and compact index caching, while
    the backend owns transport, manifests, leases, and copy/zero-copy policy.  A backend
    can be a real ``MooncakeDistributedStore`` or a small test double.  The
    optional ``payload_type`` is useful for tests and avoids importing the
    Mooncake wheel until the first real PUT.
    """

    SCHEMA = "mooncake.sparse_object"
    VERSION = 1

    def __init__(
        self,
        backend: StructuredObjectBackend,
        *,
        payload_type: type | None = None,
        planner: SparseObjectStorePlanner | None = None,
    ) -> None:
        self.backend = backend
        self._payload_type = payload_type
        self.planner = planner or SparseObjectStorePlanner()
        self._index_cache: dict[str, SparseObjectIndex] = {}
        self._metadata_cache: dict[str, Mapping[str, Any]] = {}
        self._refs: dict[str, object] = {}
        self._range_cache: dict[
            tuple[str, int, int, str, tuple[int, int]],
            Future[tuple[Any, Any]],
        ] = {}
        self._range_cache_limit = 4096
        self._range_cache_lock = threading.Lock()

    @staticmethod
    def _payload_class(payload_type: type | None) -> type:
        if payload_type is not None:
            return payload_type
        try:
            from mooncake.structured_object_store import StructuredObjectPayload
        except Exception as error:  # pragma: no cover - depends on installation
            raise RuntimeError(
                "Mooncake StructuredObjectPayload is unavailable; pass payload_type "
                "when using a custom backend"
            ) from error
        return StructuredObjectPayload

    @staticmethod
    def _build_indexed_members(
        indices: Any,
        values: Any,
        tile_shape: Sequence[int],
        index_dtype: Any | None = None,
    ) -> tuple[Any, Any, Any, Any]:
        """Sort COO by tile and return aligned indices/values/index members."""

        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - Mooncake depends on NumPy
            raise RuntimeError("sparse object encoding requires NumPy") from error
        indices = np.asarray(indices)
        values = np.asarray(values)
        if indices.ndim != 2 or indices.shape[1] == 0:
            raise ValueError("indices must have shape [nnz, ndim]")
        if indices.dtype.kind not in "iu":
            raise ValueError("indices must use an integer dtype")
        if values.ndim != 1 or len(values) != len(indices):
            raise ValueError("values must have shape [nnz]")
        normalized_tile_shape = normalize_shape(tile_shape, "tile_shape", positive=True)
        ndim = indices.shape[1]
        if len(normalized_tile_shape) != ndim:
            raise ValueError("tile_shape rank differs from indices")
        if len(indices) and np.any(indices < 0):
            raise ValueError("indices must be non-negative global coordinates")
        max_index = int(indices.max()) if len(indices) else 0
        if index_dtype is None:
            output_dtype = np.uint32 if max_index < 2**32 else np.uint64
        else:
            output_dtype = np.dtype(index_dtype)
            if output_dtype.kind not in "iu":
                raise ValueError("index_dtype must be an integer dtype")
            if max_index >= 1 << (8 * output_dtype.itemsize):
                raise ValueError("index_dtype cannot represent COO coordinates")
        indices = np.ascontiguousarray(indices, dtype=output_dtype)
        values = np.ascontiguousarray(values)
        # Do tile arithmetic in a wide signed type.  The wire index may be
        # uint16 (the ROLL contract), but tile_shape itself can exceed 65535
        # and must not wrap before division.
        tiles = indices.astype(np.int64, copy=False) // np.asarray(
            normalized_tile_shape, dtype=np.int64
        )
        if len(indices):
            keys = [indices[:, d] for d in reversed(range(ndim))]
            keys.extend(tiles[:, d] for d in reversed(range(ndim)))
            order = np.lexsort(tuple(keys))
            indices = indices[order]
            values = values[order]
            tiles = tiles[order]
            changes = np.empty(len(tiles), dtype=bool)
            changes[0] = True
            changes[1:] = np.any(tiles[1:] != tiles[:-1], axis=1)
            first = np.flatnonzero(changes)
            # Tile coordinates are bounded by the corresponding element
            # coordinates, so the requested wire index dtype can represent
            # them as well.  Persist them in the same dtype as `indices`;
            # the division itself was intentionally performed in int64.
            tile_coords = tiles[first].astype(output_dtype, copy=False)
        else:
            tile_coords = np.empty((0, ndim), dtype=output_dtype)
            first = np.empty((0,), dtype=np.int64)
        tile_ptr = np.concatenate(
            (first, np.asarray([len(indices)], dtype=np.int64))
        ).astype(np.uint64, copy=False)
        return indices, values, tile_coords, tile_ptr

    @staticmethod
    def _member_spec(array: Any) -> dict[str, Any]:
        return {"dtype": str(array.dtype), "shape": list(array.shape)}

    @staticmethod
    def _normalize_dtype(dtype: Any, name: str) -> str:
        if isinstance(dtype, str):
            normalized = dtype.strip().lower()
            if normalized.startswith("torch."):
                normalized = normalized[6:]
            if normalized:
                try:
                    import numpy as np

                    return np.dtype(normalized).name
                except (TypeError, ValueError):
                    # TensorDescriptor also carries framework dtypes such as
                    # ``bfloat16`` and ``float8_e4m3fn`` that older NumPy
                    # versions cannot parse.  Preserve those canonical names
                    # for metadata-level equality checks.
                    return normalized
        try:
            import numpy as np

            return np.dtype(dtype).name
        except (TypeError, ValueError) as error:
            raise ValueError(f"{name} must be a supported dtype") from error

    @staticmethod
    def _normalize_generation(generation: Sequence[int]) -> tuple[int, int]:
        if (
            not isinstance(generation, Sequence)
            or isinstance(generation, (str, bytes, bytearray))
            or len(generation) != 2
            or type(generation[0]) is not int
            or type(generation[1]) is not int
            or generation[0] < 0
            or generation[1] <= generation[0]
        ):
            raise ValueError("generation must be (base, delta) with delta newer")
        return int(generation[0]), int(generation[1])

    @staticmethod
    def _normalize_geometry(
        global_shape: Sequence[int],
        global_offset: Sequence[int] | None,
        local_shape: Sequence[int] | None,
    ) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
        shape = normalize_shape(global_shape, "global_shape", positive=True)
        ndim = len(shape)
        offset = (
            normalize_shape(global_offset, "global_offset", positive=False)
            if global_offset is not None
            else (0,) * ndim
        )
        local = (
            normalize_shape(local_shape, "local_shape", positive=True)
            if local_shape is not None
            else shape
        )
        if len(offset) != ndim or len(local) != ndim:
            raise ValueError("local geometry rank differs from global_shape")
        if any(offset[d] + local[d] > shape[d] for d in range(ndim)):
            raise ValueError("local geometry exceeds global_shape")
        return shape, offset, local

    def _put_indexed_sparse_object(
        self,
        *,
        tensor_id: str,
        global_shape: Sequence[int],
        global_offset: Sequence[int] | None,
        local_shape: Sequence[int] | None,
        indices: Any,
        values: Any,
        generation: Sequence[int],
        coordinate_space: str,
        placement_field: str,
        placement: Placement,
        tile_shape: Sequence[int] | None,
        partition: str,
        index_dtype: Any | None = None,
        metadata_extra: Mapping[str, Any] | None = None,
        buffer_extra: Mapping[str, Any] | None = None,
        **put_kwargs: Any,
    ) -> tuple[object, Mapping[str, Any], SparseObjectIndex | None, int]:
        """Validate, index, encode and commit either source or target COO.

        Source and target sparse objects intentionally share one encoding path;
        only coordinate space and placement metadata differ.
        """

        if not isinstance(tensor_id, str) or not tensor_id:
            raise ValueError("tensor_id must be a non-empty string")
        shape, offset, local = self._normalize_geometry(
            global_shape, global_offset, local_shape
        )
        base_generation, delta_generation = self._normalize_generation(generation)
        normalized_placement = normalize_placement(tuple(placement))
        tile = (
            tuple(tile_shape)
            if tile_shape is not None
            else tuple(
                min(256, extent)
                for extent in (shape if coordinate_space == "global" else local)
            )
        )
        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - Mooncake depends on NumPy
            raise RuntimeError("sparse object encoding requires NumPy") from error
        raw_indices = np.asarray(indices)
        raw_values = np.asarray(values)
        ndim = len(shape)
        if raw_indices.ndim != 2 or raw_indices.shape[1] != ndim:
            raise ValueError("indices must have shape [nnz, len(global_shape)]")
        if raw_values.ndim != 1 or len(raw_values) != len(raw_indices):
            raise ValueError("values must have shape [nnz]")
        if len(raw_indices):
            if coordinate_space == "global":
                lower = np.asarray(offset)
                upper = lower + np.asarray(local)
            else:
                lower = np.zeros(ndim, dtype=raw_indices.dtype)
                upper = np.asarray(local)
            if np.any(raw_indices < lower) or np.any(raw_indices >= upper):
                raise ValueError("COO indices are outside the declared geometry")
        indexed_indices, indexed_values, tile_coords, tile_ptr = (
            self._build_indexed_members(
                raw_indices, raw_values, tile, index_dtype=index_dtype
            )
        )
        buffers: dict[str, Any] = {
            "indices": indexed_indices,
            "values": indexed_values,
            "tile_coords": tile_coords,
            "tile_ptr": tile_ptr,
        }
        if buffer_extra:
            for name, value in buffer_extra.items():
                if not isinstance(name, str) or not name or name in buffers:
                    raise ValueError(f"buffer_extra contains reserved name {name!r}")
                buffers[name] = value

        metadata: dict[str, Any] = {
            "schema": self.SCHEMA,
            "version": self.VERSION,
            "tensor_id": tensor_id,
            "global_shape": list(shape),
            "global_offset": list(offset),
            "local_shape": list(local),
            "coordinate_space": coordinate_space,
            "coordinate_rank": ndim,
            "nnz": len(indexed_indices),
            "index_dtype": str(indexed_indices.dtype),
            "value_dtype": str(indexed_values.dtype),
            "base_generation": base_generation,
            "delta_generation": delta_generation,
            "apply": "scatter_add",
            "duplicate_semantics": (
                "coalesced"
                if metadata_extra is not None and metadata_extra.get("coalesced")
                else "additive"
            ),
            placement_field: [list(item) for item in normalized_placement],
            "tile_shape": list(tile),
            "members": {
                name: self._member_spec(value) for name, value in buffers.items()
            },
        }
        if metadata_extra:
            for key, value in metadata_extra.items():
                if not isinstance(key, str) or key in metadata:
                    raise ValueError(f"metadata_extra contains reserved key {key!r}")
                metadata[key] = value
        ref = self.put_structured_object(
            metadata=metadata,
            buffers=buffers,
            partition=partition,
            **put_kwargs,
        )
        key = object_ref_key(ref)
        self._refs[key] = ref
        index = None
        if coordinate_space == "global":
            index = SparseObjectIndex(
                object_ref=key,
                tensor_id=tensor_id,
                global_shape=shape,
                global_offset=offset,
                local_shape=local,
                tile_shape=tile,
                tile_coords=tuple(
                    tuple(int(item) for item in row) for row in tile_coords.tolist()
                ),
                tile_ptr=tuple(int(item) for item in tile_ptr.tolist()),
                nnz=len(indexed_indices),
                base_generation=base_generation,
                delta_generation=delta_generation,
            )
            self._index_cache[key] = index
            self._metadata_cache[key] = metadata
        return ref, metadata, index, len(indexed_indices)

    def put_structured_object(
        self,
        *,
        metadata: Mapping[str, Any],
        buffers: Mapping[str, Any],
        partition: str = "default",
        **put_kwargs: Any,
    ) -> object:
        """Commit a generic structured object through the configured backend.

        Sparse source objects use :meth:`put_sparse_object` to add COO
        validation and tile indexing.  Target-local objects (or other
        structured payloads) can use this lower-level store entry point while
        still sharing Mooncake's manifest/copy/lease handling.
        """

        payload_class = self._payload_class(self._payload_type)
        payload = payload_class(metadata=metadata, buffers=buffers)
        return self.backend.put_structured_object(
            payload, partition=partition, **put_kwargs
        )

    def put_sparse_object(
        self,
        *,
        tensor_id: str,
        global_shape: Sequence[int],
        indices: Any,
        values: Any,
        generation: tuple[int, int],
        global_offset: Sequence[int] | None = None,
        local_shape: Sequence[int] | None = None,
        source_placement: Placement = (),
        tile_shape: Sequence[int] | None = None,
        index_dtype: Any | None = None,
        logical_update_key: str | None = None,
        placement_policy: str | None = None,
        partition: str = "default",
        **put_kwargs: Any,
    ) -> StoredSparseObject:
        """Encode and commit one global-coordinate sparse object."""
        ref, metadata, index, _nnz = self._put_indexed_sparse_object(
            tensor_id=tensor_id,
            global_shape=global_shape,
            global_offset=global_offset,
            local_shape=local_shape,
            indices=indices,
            values=values,
            generation=generation,
            coordinate_space="global",
            placement_field="source_placement",
            placement=source_placement,
            tile_shape=tile_shape,
            partition=partition,
            index_dtype=index_dtype,
            metadata_extra={
                key: value
                for key, value in (
                    ("logical_update_key", logical_update_key),
                    ("placement_policy", placement_policy),
                )
                if value is not None
            },
            **put_kwargs,
        )
        assert index is not None
        return StoredSparseObject(ref=ref, metadata=metadata, index=index)

    def put_sparse_object_if_owner(
        self,
        *,
        source_placement: Placement,
        source_placements: Iterable[Placement],
        **put_kwargs: Any,
    ) -> StoredSparseObject | None:
        """Publish a replicated source object from one deterministic owner.

        The admission decision is made next to object publication so all
        framework adapters share the same de-duplication rule.  Non-owners
        return ``None`` before COO encoding or transport allocation.
        """

        placements = tuple(source_placements)
        if not is_canonical_source(source_placement, placements):
            return None
        return self.put_sparse_object(source_placement=source_placement, **put_kwargs)

    @staticmethod
    def should_publish_source(
        source_placement: Placement, source_placements: Iterable[Placement]
    ) -> bool:
        """Return the deterministic admission decision for a source rank."""

        return is_canonical_source(source_placement, source_placements)

    def put_target_sparse_object(
        self,
        *,
        tensor_id: str,
        global_shape: Sequence[int],
        global_offset: Sequence[int],
        local_shape: Sequence[int],
        indices: Any,
        values: Any,
        generation: tuple[int, int],
        target_placement: Placement = (),
        tile_shape: Sequence[int] | None = None,
        logical_update_key: str | None = None,
        placement_policy: str | None = None,
        target_key: str | None = None,
        partition: str = "default",
        **put_kwargs: Any,
    ) -> StoredTargetSparseObject:
        """Commit a target-local COO object after sparse-aware materialization.

        ``indices`` are local to ``global_offset``.  Keeping this encoding
        operation in the Mooncake facade means a framework adapter does not
        need to duplicate target metadata, tile indexing, or structured-object
        publication logic.
        """
        ref, metadata, _index, nnz = self._put_indexed_sparse_object(
            tensor_id=tensor_id,
            global_shape=global_shape,
            global_offset=global_offset,
            local_shape=local_shape,
            indices=indices,
            values=values,
            generation=generation,
            coordinate_space="target_local",
            placement_field="target_placement",
            placement=target_placement,
            tile_shape=tile_shape,
            partition=partition,
            metadata_extra={
                key: value
                for key, value in (
                    ("logical_update_key", logical_update_key),
                    ("placement_policy", placement_policy),
                    ("target_key", target_key),
                )
                if value is not None
            },
            **put_kwargs,
        )
        return StoredTargetSparseObject(ref=ref, metadata=metadata, nnz=nnz)

    @staticmethod
    def _read_members(
        backend: StructuredObjectBackend, spec: Any
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        result = backend.materialize(spec)
        return _result_metadata_and_objects(result)

    def read_index(self, ref: object) -> SparseObjectIndex:
        """Read only ``tile_coords`` and ``tile_ptr`` from a source object."""

        index, _metadata = self.read_index_with_metadata(ref)
        return index

    def read_index_with_metadata(
        self, ref: object
    ) -> tuple[SparseObjectIndex, Mapping[str, Any]]:
        """Read the compact index and return its metadata without COO members.

        Structured-object metadata is part of the manifest and is returned by
        the same index-only materialization.  Keeping this method separate
        lets a store-side planner inspect dtype/tile configuration without
        issuing another request or materializing ``indices``/``values``.
        """

        key = object_ref_key(ref)
        self._refs.setdefault(key, ref)
        cached = self._index_cache.get(key)
        if cached is not None:
            return cached, self._metadata_cache[key]
        spec = self.backend.read_spec(ref).select_members(("tile_coords", "tile_ptr"))
        metadata, objects = self._read_members(self.backend, spec)
        index = SparseObjectIndex.from_metadata(
            object_ref=ref,
            metadata=metadata,
            tile_coords=objects["tile_coords"].tolist(),
            tile_ptr=objects["tile_ptr"].tolist(),
        )
        self._index_cache[key] = index
        self._metadata_cache[key] = metadata
        return index, metadata

    def read_coo_range(
        self, ref: object, entry_range: tuple[int, int]
    ) -> tuple[Any, Any]:
        """Read aligned ``indices``/``values`` axis-0 ranges only."""

        key = object_ref_key(ref)
        start, end = entry_range
        if type(start) is not int or type(end) is not int or start < 0 or end <= start:
            raise ValueError("COO range must be non-empty")
        # Explicit member selection is important: adding slices alone does
        # not stop a structured-object backend from materializing unrelated
        # tile-index members.
        backend_ref = self._refs.get(key, ref)
        spec = self.backend.read_spec(backend_ref).select_members(("indices", "values"))
        spec = spec.slice_member("indices", axis=0, start=start, end=end)
        spec = spec.slice_member("values", axis=0, start=start, end=end)
        _metadata, objects = self._read_members(self.backend, spec)
        return objects["indices"], objects["values"]

    def read_coo_range_cached(
        self,
        ref: object,
        entry_range: tuple[int, int],
        *,
        base_generation: int,
        delta_generation: int,
        physical_node: str | None,
    ) -> tuple[Any, Any]:
        """Read one COO range once per process-local physical-node namespace.

        Multiple TP/EP target workers can share a ``SparseObjectStore`` service
        in one process.  ``physical_node`` is a cache namespace, not a
        cross-process coordination mechanism.  The first consumer owns the
        Store/RDMA GET; later consumers receive the immutable range payload
        from this process-local cache and perform their own boundary
        filter/rebase.
        """

        if physical_node is None:
            return self.read_coo_range(ref, entry_range)
        if not isinstance(physical_node, str) or not physical_node:
            raise ValueError("physical_node must be a non-empty string")
        key = (
            object_ref_key(ref),
            base_generation,
            delta_generation,
            physical_node,
            entry_range,
        )
        with self._range_cache_lock:
            future = self._range_cache.get(key)
            owns_read = future is None
            if owns_read:
                future = Future()
                if len(self._range_cache) >= self._range_cache_limit:
                    completed_key = next(
                        (
                            cached_key
                            for cached_key, cached_future in self._range_cache.items()
                            if cached_future.done()
                        ),
                        None,
                    )
                    if completed_key is not None:
                        del self._range_cache[completed_key]
                self._range_cache[key] = future
        assert future is not None
        if not owns_read:
            return future.result()
        try:
            result = self.read_coo_range(ref, entry_range)
        except Exception as error:
            future.set_exception(error)
            with self._range_cache_lock:
                if self._range_cache.get(key) is future:
                    del self._range_cache[key]
            raise
        # Cache consumers share this payload.  Make NumPy results immutable so
        # one EP/TP worker cannot corrupt a later local consumer.
        try:
            result[0].setflags(write=False)
            result[1].setflags(write=False)
        except AttributeError:
            pass
        future.set_result(result)
        return result

    def clear_range_cache(
        self,
        *,
        object_ref: object | None = None,
        physical_node: str | None = None,
        before_delta_generation: int | None = None,
    ) -> None:
        """Release cached range payloads after target fan-out completes."""

        object_key = None if object_ref is None else object_ref_key(object_ref)
        if physical_node is not None and (
            not isinstance(physical_node, str) or not physical_node
        ):
            raise ValueError("physical_node must be a non-empty string")
        if before_delta_generation is not None and (
            type(before_delta_generation) is not int or before_delta_generation < 0
        ):
            raise ValueError("before_delta_generation must be non-negative")
        with self._range_cache_lock:
            remove = [
                key
                for key in self._range_cache
                if (object_key is None or key[0] == object_key)
                and (physical_node is None or key[3] == physical_node)
                and (
                    before_delta_generation is None or key[2] < before_delta_generation
                )
            ]
            for key in remove:
                del self._range_cache[key]

    def clear_cache(
        self,
        *,
        object_ref: object | None = None,
        physical_node: str | None = None,
        before_delta_generation: int | None = None,
    ) -> None:
        """Release metadata and range caches after an update fan-out."""

        key = None if object_ref is None else object_ref_key(object_ref)
        if key is None:
            self._index_cache.clear()
            self._metadata_cache.clear()
        else:
            self._index_cache.pop(key, None)
            self._metadata_cache.pop(key, None)
        self.planner.clear_index_cache(key)
        self.clear_range_cache(
            object_ref=key,
            physical_node=physical_node,
            before_delta_generation=before_delta_generation,
        )

    def _select_target_sources(
        self,
        *,
        tensor_id: str,
        tensor: TensorDescriptor,
        target: Any,
        source_fragments: Iterable[Any],
        base_generation: int,
        delta_generation: int,
    ) -> tuple[tuple[Any, ...], dict[str, SparseObjectIndex]]:
        """Read indexes for intersecting replicas, then select current sources."""

        source_fragments = tuple(source_fragments)
        possible_sources = self.planner.candidate_source_fragments(
            tensor_id=tensor_id,
            target=target,
            source_fragments=source_fragments,
            tensor=tensor,
        )
        source_indexes = {
            object_ref_key(source.object_ref): self.read_index(source.object_ref)
            for source in possible_sources
        }
        candidates = self.planner.select_source_fragments(
            tensor_id=tensor_id,
            target=target,
            source_fragments=source_fragments,
            tensor=tensor,
            source_indexes=source_indexes,
            base_generation=base_generation,
            delta_generation=delta_generation,
        )
        return candidates, source_indexes

    def plan_target(self, **kwargs: Any) -> SparseObjectPlan:
        """Filter candidates, then read only indexes needed by this target."""

        source_fragments = tuple(kwargs.pop("source_fragments"))
        source_indexes = kwargs.pop("source_indexes", None)
        if source_indexes is None:
            _candidates, source_indexes = self._select_target_sources(
                tensor_id=kwargs["tensor_id"],
                tensor=kwargs["tensor"],
                target=kwargs["target"],
                source_fragments=source_fragments,
                base_generation=kwargs["base_generation"],
                delta_generation=kwargs["delta_generation"],
            )
        return self.planner.plan_target(
            source_fragments=source_fragments,
            source_indexes=source_indexes,
            **kwargs,
        )

    def materialize_target(
        self,
        *,
        tensor_id: str,
        tensor: TensorDescriptor,
        target: Any,
        source_fragments: Iterable[Any],
        base_generation: int,
        delta_generation: int,
    ) -> tuple[Any, Any, tuple[int, ...] | None]:
        """Materialize one target payload after planner-side filtering.

        This is the Store-facing operation used by a framework adapter.  It
        keeps source candidate selection, compact-index reads, dtype/tile
        consistency checks, boundary filtering and coordinate rebasing inside
        Mooncake.
        """

        candidates, source_indexes = self._select_target_sources(
            tensor_id=tensor_id,
            tensor=tensor,
            target=target,
            source_fragments=tuple(source_fragments),
            base_generation=base_generation,
            delta_generation=delta_generation,
        )
        value_dtype: Any | None = None
        tile_shape: tuple[int, ...] | None = None
        for source in candidates:
            _index, metadata = self.read_index_with_metadata(source.object_ref)
            try:
                source_dtype = metadata["members"]["values"]["dtype"]
                source_tile_shape = tuple(int(item) for item in metadata["tile_shape"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("sparse source metadata is incomplete") from error
            source_dtype = self._normalize_dtype(source_dtype, "source value dtype")
            tensor_dtype = self._normalize_dtype(tensor.dtype, "tensor dtype")
            if source_dtype != tensor_dtype:
                raise ValueError(
                    f"source COO value dtype {source_dtype} does not match "
                    f"TensorDescriptor dtype {tensor_dtype}"
                )
            if value_dtype is not None and source_dtype != value_dtype:
                raise ValueError("source COO value dtypes differ")
            if tile_shape is not None and source_tile_shape != tile_shape:
                raise ValueError("source objects use inconsistent tile_shape")
            value_dtype = source_dtype
            tile_shape = source_tile_shape
        plan = self.planner.plan_target(
            tensor_id=tensor_id,
            tensor=tensor,
            target=target,
            source_fragments=candidates,
            source_indexes=source_indexes,
            base_generation=base_generation,
            delta_generation=delta_generation,
        )
        indices, values = self.materialize_plan(plan, value_dtype=value_dtype)
        return indices, values, tile_shape

    def read_plan(
        self, plan: SparseObjectPlan
    ) -> tuple[tuple[SparseObjectRegion, Any, Any], ...]:
        """Materialize only the COO ranges described by a store plan.

        This low-level method exposes the selected ranges for callers that
        need custom device handling.  Use :meth:`materialize_plan` when the
        store should perform boundary filtering and target-local rebasing.
        No unrelated structured members are materialized here.
        """

        return tuple(self._iter_plan(plan))

    def _iter_plan(
        self, plan: SparseObjectPlan
    ) -> Iterable[tuple[SparseObjectRegion, Any, Any]]:
        """Stream planned ranges for materialization without a read-all tuple."""

        for region in plan.regions:
            indices, values = self.read_coo_range_cached(
                region.source_object_ref,
                region.indices_range,
                base_generation=plan.base_generation,
                delta_generation=plan.delta_generation,
                physical_node=plan.physical_node,
            )
            yield region, indices, values

    def materialize_plan(
        self, plan: SparseObjectPlan, *, value_dtype: Any | None = None
    ) -> tuple[Any, Any]:
        """Filter and rebase a plan into target-local COO arrays.

        This is the store-side counterpart to ``read_plan``.  It keeps the
        framework adapter from reimplementing boundary filtering or coordinate
        rebasing; the adapter only applies the returned ``scatter_add``
        payload to its local weight.  Duplicate coordinates are preserved,
        matching COO additive semantics.
        """

        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover - Mooncake depends on NumPy
            raise RuntimeError(
                "sparse object materialization requires NumPy"
            ) from error

        selected_indices: list[Any] = []
        selected_values: list[Any] = []
        ndim = len(plan.target_local_shape)
        inferred_dtype = None
        for region, indices, values in self._iter_plan(plan):
            indices = np.asarray(indices)
            values = np.asarray(values)
            if indices.ndim != 2 or indices.shape[1] != ndim or values.ndim != 1:
                raise ValueError("COO range shape does not match sparse object plan")
            inferred_dtype = values.dtype
            begin, end = region.source_global_box
            if region.exact_coordinate_filter:
                mask = np.all(
                    (indices >= np.asarray(begin)) & (indices < np.asarray(end)),
                    axis=1,
                )
            else:
                mask = np.ones(len(indices), dtype=bool)
            if not np.any(mask):
                continue
            local = indices[mask].astype(np.int64, copy=True)
            local -= np.asarray(plan.target_global_offset, dtype=np.int64)
            if np.any(local < 0) or np.any(
                local >= np.asarray(plan.target_local_shape, dtype=np.int64)
            ):
                raise ValueError("COO range produced a coordinate outside target shape")
            selected_indices.append(local)
            selected_values.append(values[mask].copy())

        if not selected_indices:
            dtype = np.dtype(value_dtype or inferred_dtype or np.float32)
            return np.empty((0, ndim), dtype=np.uint32), np.empty((0,), dtype=dtype)
        indices_out = np.concatenate(selected_indices, axis=0)
        values_out = np.concatenate(selected_values, axis=0)
        order = np.lexsort(
            tuple(indices_out[:, dimension] for dimension in reversed(range(ndim)))
        )
        max_coordinate = int(indices_out.max()) if indices_out.size else 0
        index_dtype = np.uint32 if max_coordinate < 2**32 else np.uint64
        return indices_out[order].astype(index_dtype, copy=False), values_out[order]

    def apply_target_sparse_object(
        self,
        base: Any,
        ref: object,
        *,
        inplace: bool = True,
        generation_fence: SparseGenerationFence | None = None,
        target_key: str | None = None,
        expected_tensor_id: str | None = None,
        expected_target_placement: Placement | None = None,
        expected_generation: tuple[int, int] | None = None,
        expected_update_key: str | None = None,
        expected_target_key: str | None = None,
    ) -> Any:
        """Apply a target-local COO object in one additive update.

        ROLL supplies the target weight.  Mooncake owns the structured-object
        read and the additive COO operation.  The current facade materializes
        COO members through the backend and uses host staging before a Torch
        update; a registered destination fast path is a future optimization.
        """

        try:
            import numpy as np
        except ImportError as error:  # pragma: no cover
            raise RuntimeError("sparse object application requires NumPy") from error
        backend_ref = self._refs.get(object_ref_key(ref), ref)
        spec = self.backend.read_spec(backend_ref).select_members(("indices", "values"))
        metadata, objects = self._read_members(self.backend, spec)
        if metadata.get("schema") != self.SCHEMA:
            raise ValueError("unsupported sparse structured-object schema")
        if metadata.get("version") != self.VERSION:
            raise ValueError("unsupported sparse structured-object version")
        if metadata.get("coordinate_space") != "target_local":
            raise ValueError("target application requires target-local COO coordinates")
        if metadata.get("apply") != "scatter_add":
            raise ValueError("target sparse object does not use scatter_add semantics")
        if (
            expected_tensor_id is not None
            and metadata.get("tensor_id") != expected_tensor_id
        ):
            raise ValueError("target sparse object tensor_id does not match target")
        if expected_target_placement is not None:
            declared_placement = metadata.get("target_placement")
            expected_placement = [
                list(item)
                for item in normalize_placement(tuple(expected_target_placement))
            ]
            if declared_placement != expected_placement:
                raise ValueError("target sparse object placement does not match target")
        if expected_generation is not None:
            if tuple(expected_generation) != (
                metadata.get("base_generation"),
                metadata.get("delta_generation"),
            ):
                raise ValueError(
                    "target sparse object generation does not match target"
                )
        if (
            expected_update_key is not None
            and metadata.get("logical_update_key") != expected_update_key
        ):
            raise ValueError("target sparse object update key does not match target")
        if (
            expected_target_key is not None
            and metadata.get("target_key") != expected_target_key
        ):
            raise ValueError("target sparse object target key does not match target")
        expected_shape = tuple(int(item) for item in metadata.get("local_shape", ()))
        if tuple(getattr(base, "shape", ())) != expected_shape:
            raise ValueError(
                f"target base shape {tuple(getattr(base, 'shape', ()))!r} "
                f"does not match sparse object local_shape {expected_shape!r}"
            )
        indices = np.asarray(objects["indices"])
        values = np.asarray(objects["values"])
        if _is_torch_tensor(base):
            base_dtype = self._normalize_dtype(str(base.dtype), "base dtype")
            value_dtype = self._normalize_dtype(values.dtype, "target value dtype")
            if base_dtype != value_dtype:
                raise ValueError(
                    f"target value dtype does not match base dtype: "
                    f"{value_dtype} vs {base_dtype}"
                )
        elif np.asarray(base).dtype != values.dtype:
            raise ValueError(
                f"target value dtype does not match base dtype: {values.dtype} vs "
                f"{np.asarray(base).dtype}"
            )
        if indices.ndim != 2 or values.ndim != 1 or len(indices) != len(values):
            raise ValueError("target sparse object has misaligned COO members")
        if indices.shape[1] != len(expected_shape):
            raise ValueError("target sparse object coordinate rank differs from base")
        if indices.dtype.kind not in "iu":
            raise ValueError("target sparse object indices must be integral")
        if len(indices):
            upper = np.asarray(expected_shape, dtype=indices.dtype)
            if np.any(indices < 0) or np.any(indices >= upper):
                raise ValueError("target sparse object index exceeds local shape")
        members = metadata.get("members")
        if not isinstance(members, Mapping):
            raise ValueError("target sparse object metadata is missing members")
        declared_indices = members.get("indices")
        declared_values = members.get("values")
        if not isinstance(declared_indices, Mapping) or not isinstance(
            declared_values, Mapping
        ):
            raise ValueError("target sparse object metadata is missing COO members")
        if declared_indices.get("shape") != list(indices.shape) or declared_values.get(
            "shape"
        ) != list(values.shape):
            raise ValueError("target sparse object member shape does not match payload")
        if declared_indices.get("dtype") != str(indices.dtype) or declared_values.get(
            "dtype"
        ) != str(values.dtype):
            raise ValueError("target sparse object member dtype does not match payload")
        if (generation_fence is None) != (target_key is None):
            raise ValueError(
                "generation_fence and target_key must be provided together"
            )
        lease = None
        if generation_fence is not None:
            update_key = expected_update_key or metadata.get("logical_update_key")
            lease = generation_fence.begin(
                target_key,
                int(metadata.get("base_generation", -1)),
                int(metadata.get("delta_generation", -1)),
                update_key=update_key,
            )
        if not inplace:
            result = (
                base.clone() if _is_torch_tensor(base) else np.array(base, copy=True)
            )
        else:
            result = base
        if lease is None and generation_fence is not None:
            return result
        try:
            if len(indices):
                if _is_torch_tensor(result):
                    import torch

                    index_tensor = torch.as_tensor(
                        indices, dtype=torch.long, device=result.device
                    )
                    value_tensor = torch.as_tensor(values, device=result.device)
                    result.index_put_(
                        tuple(index_tensor.transpose(0, 1)),
                        value_tensor,
                        accumulate=True,
                    )
                else:
                    np.add.at(
                        result,
                        tuple(indices.astype(np.int64, copy=False).T),
                        values,
                    )
            if lease is not None:
                generation_fence.commit(lease)
            return result
        except Exception:
            if lease is not None:
                generation_fence.abort(lease)
            raise


def _is_torch_tensor(value: Any) -> bool:
    """Avoid importing torch on NumPy-only Mooncake installations."""

    try:
        import torch
    except ImportError:  # pragma: no cover - optional dependency
        return False
    return isinstance(value, torch.Tensor)


def plan_sparse_object_target(**kwargs: Any) -> SparseObjectPlan:
    """Functional facade for planning one target object."""

    return SparseObjectStorePlanner().plan_target(**kwargs)


__all__ = [
    "Box",
    "Placement",
    "canonical_source_placement",
    "is_canonical_source",
    "SparseObjectIndex",
    "SparseObjectStore",
    "SparseObjectStorePlanner",
    "SparseObjectPlan",
    "SparseObjectRegion",
    "SparseGenerationError",
    "SparseGenerationFence",
    "SparseGenerationInProgressError",
    "SparseGenerationMismatchError",
    "SparseGenerationNotInitializedError",
    "StaleSparseGenerationError",
    "StoredSparseObject",
    "StoredTargetSparseObject",
    "StructuredObjectBackend",
    "object_ref_key",
    "plan_sparse_object_target",
]
