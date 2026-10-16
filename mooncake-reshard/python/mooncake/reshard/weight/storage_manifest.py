"""Persisted model-weight fragments and manifest contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from math import prod
from collections.abc import Mapping, Sequence
from typing import Optional, Union, cast

from .._typing import TypeAlias

from .._compat import _strict_zip
from ..contracts import (
    ResourceId,
    ResourceKind,
    RevisionId,
    StoredFragmentSnapshotId,
    StoredResourceManifest,
    TensorId,
)
from .serde import _axis_from_wire, _axis_to_wire
from .types import (
    TensorDescriptor,
    ParallelAxis,
    _normalize_aliases,
    _require_integer,
    _require_integer_tuple,
    _require_manifest_items,
    _require_nonempty_string,
    _require_sequence,
    _require_u64,
    validate_fragment_geometry,
)


StoredGeometryKey: TypeAlias = tuple[TensorId, tuple[int, ...], tuple[int, ...]]
StoredAliasDescriptorKey: TypeAlias = tuple[
    tuple[int, ...],
    str,
    int,
    tuple[int, ...],
    tuple[ParallelAxis, ...],
    Optional[int],
    Optional[int],
    str,
]
StoredAliasGeometryKey: TypeAlias = tuple[
    tuple[TensorId, ...],
    tuple[int, ...],
    tuple[int, ...],
    int,
]


@dataclass(frozen=True)
class StoredManifestIdentity:
    """Stable identity for a committed Store manifest snapshot."""

    namespace: str
    resource_id: ResourceId
    revision: RevisionId
    weight_generation: int
    group_id: str
    manifest_key: str
    content_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "namespace",
            "resource_id",
            "revision",
            "group_id",
            "manifest_key",
            "content_sha256",
        ):
            _require_nonempty_string(getattr(self, name), name)
        _require_u64(self.weight_generation, "weight_generation")
        if len(self.content_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_sha256
        ):
            raise ValueError("content_sha256 must be a SHA-256 hex digest")


@dataclass(frozen=True)
class StoredFragmentSnapshot:
    fragment_id: StoredFragmentSnapshotId
    tensor_id: TensorId
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    object_key: str
    object_offset: int
    nbytes: int
    aliases: tuple[TensorId, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "global_offset",
            _require_integer_tuple(self.global_offset, "global_offset", minimum=0),
        )
        object.__setattr__(
            self,
            "local_shape",
            _require_integer_tuple(self.local_shape, "local_shape", minimum=1),
        )
        for name in ("fragment_id", "tensor_id", "object_key"):
            _require_nonempty_string(getattr(self, name), name)
        _require_integer(self.object_offset, "object_offset", minimum=0)
        _require_integer(self.nbytes, "nbytes", minimum=1)
        object.__setattr__(self, "aliases", _normalize_aliases(self.aliases))
        if self.aliases:
            if len(self.aliases) < 2:
                raise ValueError("alias group must contain at least two tensor IDs")
            if self.tensor_id not in self.aliases:
                raise ValueError("alias group must contain the fragment tensor_id")


@dataclass(frozen=True)
class StoredWeightManifest(StoredResourceManifest):
    revision: RevisionId
    weight_generation: int
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[StoredFragmentSnapshot, ...]
    manifest_digest: str = field(init=False)

    @property
    def resource_kind(self) -> ResourceKind:
        return ResourceKind.MODEL_WEIGHT

    def __post_init__(self) -> None:
        for name in (
            "namespace",
            "resource_id",
            "revision",
            "group_id",
            "manifest_key",
            "created_at",
        ):
            _require_nonempty_string(getattr(self, name), name)
        object.__setattr__(
            self,
            "tensors",
            _require_manifest_items(
                self.tensors,
                "StoredWeightManifest tensors",
                TensorDescriptor,
            ),
        )
        object.__setattr__(
            self,
            "fragments",
            _require_manifest_items(
                self.fragments,
                "StoredWeightManifest fragments",
                StoredFragmentSnapshot,
            ),
        )
        _require_u64(self.weight_generation, "weight_generation")
        if self.manifest_key != f"{self.group_id}/manifest":
            raise ValueError("manifest_key does not belong to manifest group")
        payload_prefix = f"{self.group_id}/payload/"
        if any(
            not fragment.object_key.startswith(payload_prefix)
            for fragment in self.fragments
        ):
            raise ValueError("payload object_key does not belong to manifest group")
        _validate_stored_fragments(self.tensors, self.fragments)
        _validate_stored_aliases(self.tensors, self.fragments)
        _validate_stored_coverage(self.tensors, self.fragments)
        _validate_stored_object_ranges(self.fragments)
        object.__setattr__(
            self,
            "manifest_digest",
            hashlib.sha256(self.to_json().encode("utf-8")).hexdigest(),
        )

    def __getstate__(self) -> tuple[object, ...]:
        """Serialize logical state and recompute the digest on restore."""

        return (
            self.namespace,
            self.resource_id,
            self.revision,
            self.weight_generation,
            self.group_id,
            self.manifest_key,
            self.tensors,
            self.fragments,
            self.created_at,
        )

    def __setstate__(self, state: tuple[object, ...]) -> None:
        """Restore only a fully validated persisted manifest state."""

        if not isinstance(state, tuple) or len(state) != 9:
            raise ValueError("StoredWeightManifest pickle state is invalid")
        for name, value in zip(
            (
                "namespace",
                "resource_id",
                "revision",
                "weight_generation",
                "group_id",
                "manifest_key",
                "tensors",
                "fragments",
                "created_at",
            ),
            state,
        ):
            object.__setattr__(self, name, value)
        self.__post_init__()

    @property
    def manifest_identity(self) -> StoredManifestIdentity:
        """Return the Store location plus a canonical content digest."""

        return StoredManifestIdentity(
            namespace=self.namespace,
            resource_id=self.resource_id,
            revision=self.revision,
            weight_generation=self.weight_generation,
            group_id=self.group_id,
            manifest_key=self.manifest_key,
            content_sha256=self.manifest_digest,
        )

    def to_json(self) -> str:
        tensors: list[dict[str, object]] = []
        for tensor in self.tensors:
            tensors.append(
                {
                    "tensor_id": tensor.tensor_id,
                    "global_shape": tensor.global_shape,
                    "dtype": tensor.dtype,
                    "itemsize": tensor.itemsize,
                    "layer_id": tensor.layer_id,
                    "expert_id": tensor.expert_id,
                    "layout_fingerprint": tensor.layout_fingerprint,
                    "shard_dims": tensor.shard_dims,
                    "parallel_axes": [
                        _axis_to_wire(axis) for axis in tensor.parallel_axes
                    ],
                }
            )
        raw: dict[str, object] = {
            "resource_kind": self.resource_kind.value,
            "namespace": self.namespace,
            "resource_id": self.resource_id,
            "revision": self.revision,
            "weight_generation": self.weight_generation,
            "group_id": self.group_id,
            "manifest_key": self.manifest_key,
            "tensors": tensors,
            "fragments": [asdict(fragment) for fragment in self.fragments],
            "created_at": self.created_at,
        }
        return json.dumps(
            raw,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @classmethod
    def from_json(
        cls,
        value: Union[str, bytes, bytearray],
    ) -> StoredWeightManifest:
        def reject_constant(constant: str) -> None:
            raise ValueError(f"non-finite JSON number is unsupported: {constant}")

        def reject_duplicate_fields(
            pairs: list[tuple[str, object]],
        ) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError(f"duplicate JSON field: {key}")
                result[key] = item
            return result

        try:
            raw = json.loads(
                value,
                parse_constant=reject_constant,
                object_pairs_hook=reject_duplicate_fields,
            )
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError("weight manifest is not valid JSON") from error
        return cls.from_dict(_require_mapping(cast(object, raw), "weight manifest"))

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> StoredWeightManifest:
        raw = _require_exact_fields(
            raw,
            frozenset(
                {
                    "resource_kind",
                    "namespace",
                    "resource_id",
                    "revision",
                    "weight_generation",
                    "group_id",
                    "manifest_key",
                    "tensors",
                    "fragments",
                    "created_at",
                }
            ),
            "weight manifest",
        )
        tensor_fields = frozenset(
            {
                "tensor_id",
                "global_shape",
                "dtype",
                "itemsize",
                "layer_id",
                "expert_id",
                "layout_fingerprint",
                "shard_dims",
                "parallel_axes",
            }
        )
        fragment_fields = frozenset(
            {
                "fragment_id",
                "tensor_id",
                "global_offset",
                "local_shape",
                "object_key",
                "object_offset",
                "nbytes",
                "aliases",
            }
        )
        tensors: list[TensorDescriptor] = []
        if raw["resource_kind"] != ResourceKind.MODEL_WEIGHT.value:
            raise ValueError("weight manifest resource_kind must be model_weight")
        for index, item in enumerate(
            _require_sequence(raw["tensors"], "StoredWeightManifest tensors")
        ):
            tensor_values: dict[str, object] = dict(
                _require_exact_fields(item, tensor_fields, "tensor descriptor")
            )
            parallel_axes = tuple(
                _axis_from_wire(axis, axis_index)
                for axis_index, axis in enumerate(
                    _require_sequence(
                        tensor_values["parallel_axes"],
                        f"stored tensor parallel_axes {index}",
                    )
                )
            )
            tensors.append(
                TensorDescriptor(
                    tensor_id=TensorId(
                        _require_nonempty_string(
                            tensor_values["tensor_id"],
                            "stored tensor tensor_id",
                        )
                    ),
                    global_shape=_require_integer_tuple(
                        tensor_values["global_shape"],
                        "stored tensor global_shape",
                        minimum=1,
                    ),
                    dtype=_require_nonempty_string(
                        tensor_values["dtype"], "stored tensor dtype"
                    ),
                    itemsize=_require_integer(
                        tensor_values["itemsize"], "stored tensor itemsize", minimum=1
                    ),
                    layer_id=_optional_integer(
                        tensor_values["layer_id"], "stored tensor layer_id", minimum=0
                    ),
                    expert_id=_optional_integer(
                        tensor_values["expert_id"], "stored tensor expert_id", minimum=0
                    ),
                    layout_fingerprint=_require_nonempty_string(
                        tensor_values["layout_fingerprint"],
                        "stored tensor layout_fingerprint",
                    ),
                    shard_dims=_require_integer_tuple(
                        tensor_values["shard_dims"],
                        "stored tensor shard_dims",
                        minimum=0,
                    ),
                    parallel_axes=parallel_axes,
                )
            )
        fragments: tuple[StoredFragmentSnapshot, ...] = tuple(
            _stored_fragment_from_wire(item, fragment_fields)
            for item in _require_sequence(
                raw["fragments"],
                "StoredWeightManifest fragments",
            )
        )
        return cls(
            namespace=_require_nonempty_string(raw["namespace"], "namespace"),
            resource_id=ResourceId(
                _require_nonempty_string(raw["resource_id"], "resource_id")
            ),
            revision=RevisionId(_require_nonempty_string(raw["revision"], "revision")),
            weight_generation=_require_u64(
                raw["weight_generation"], "weight_generation"
            ),
            group_id=_require_nonempty_string(raw["group_id"], "group_id"),
            manifest_key=_require_nonempty_string(raw["manifest_key"], "manifest_key"),
            tensors=tuple(tensors),
            fragments=fragments,
            created_at=_require_nonempty_string(raw["created_at"], "created_at"),
        )


def validate_weight_manifest_snapshot(
    manifest: StoredWeightManifest,
) -> StoredWeightManifest:
    """Rebuild a typed manifest before it is trusted at a plan boundary."""

    if not isinstance(manifest, StoredWeightManifest):
        raise ValueError("weight manifest snapshot is invalid")
    try:
        return StoredWeightManifest(
            namespace=manifest.namespace,
            resource_id=manifest.resource_id,
            revision=manifest.revision,
            weight_generation=manifest.weight_generation,
            group_id=manifest.group_id,
            manifest_key=manifest.manifest_key,
            tensors=manifest.tensors,
            fragments=manifest.fragments,
            created_at=manifest.created_at,
        )
    except (AttributeError, TypeError) as error:
        raise ValueError("weight manifest snapshot is invalid") from error


def _require_exact_fields(
    value: object,
    expected: frozenset[str],
    label: str,
) -> Mapping[str, object]:
    mapping = _require_mapping(value, label)
    if frozenset(mapping) != expected:
        raise ValueError(f"{label} schema fields do not match contract")
    return mapping


def _require_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    raw = cast(Mapping[object, object], value)
    if any(type(key) is not str for key in raw):
        raise ValueError(f"{label} must be a JSON object")
    return {cast(str, key): item for key, item in raw.items()}


def _optional_integer(
    value: object,
    label: str,
    *,
    minimum: int,
) -> Optional[int]:
    if value is None:
        return None
    return _require_integer(value, label, minimum=minimum)


def _stored_fragment_from_wire(
    value: object,
    fragment_fields: frozenset[str],
) -> StoredFragmentSnapshot:
    fragment = _require_exact_fields(value, fragment_fields, "stored fragment")
    return StoredFragmentSnapshot(
        fragment_id=StoredFragmentSnapshotId(
            _require_nonempty_string(
                fragment["fragment_id"], "stored fragment fragment_id"
            )
        ),
        tensor_id=TensorId(
            _require_nonempty_string(fragment["tensor_id"], "stored fragment tensor_id")
        ),
        global_offset=_require_integer_tuple(
            fragment["global_offset"], "stored fragment global_offset", minimum=0
        ),
        local_shape=_require_integer_tuple(
            fragment["local_shape"], "stored fragment local_shape", minimum=1
        ),
        object_key=_require_nonempty_string(
            fragment["object_key"], "stored fragment object_key"
        ),
        object_offset=_require_integer(
            fragment["object_offset"], "stored fragment object_offset", minimum=0
        ),
        nbytes=_require_integer(
            fragment["nbytes"], "stored fragment nbytes", minimum=1
        ),
        aliases=tuple(
            TensorId(_require_nonempty_string(alias, "stored fragment alias"))
            for alias in _require_sequence(
                fragment["aliases"], "stored fragment aliases"
            )
        ),
    )


def _validate_stored_fragments(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[StoredFragmentSnapshot],
) -> None:
    tensor_by_id: dict[TensorId, TensorDescriptor] = {}
    for tensor in tensors:
        if tensor.tensor_id in tensor_by_id:
            raise ValueError(f"duplicate tensor_id: {tensor.tensor_id}")
        tensor_by_id[tensor.tensor_id] = tensor

    fragment_ids: set[StoredFragmentSnapshotId] = set()
    for fragment in fragments:
        if fragment.fragment_id in fragment_ids:
            raise ValueError(f"duplicate fragment_id: {fragment.fragment_id}")
        fragment_ids.add(fragment.fragment_id)
        tensor = tensor_by_id.get(fragment.tensor_id)
        if tensor is None:
            raise ValueError(f"unknown tensor_id: {fragment.tensor_id}")
        validate_fragment_geometry(
            tensor,
            fragment_id=fragment.fragment_id,
            global_offset=fragment.global_offset,
            local_shape=fragment.local_shape,
            nbytes=fragment.nbytes,
        )


def _validate_stored_coverage(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[StoredFragmentSnapshot],
) -> None:
    by_tensor: dict[TensorId, list[StoredFragmentSnapshot]] = {}
    geometries: set[StoredGeometryKey] = set()
    for fragment in fragments:
        geometry = (
            fragment.tensor_id,
            fragment.global_offset,
            fragment.local_shape,
        )
        if geometry in geometries:
            raise ValueError(f"duplicate fragment geometry: {fragment.tensor_id}")
        geometries.add(geometry)
        by_tensor.setdefault(fragment.tensor_id, []).append(fragment)

    for tensor in tensors:
        tensor_fragments = by_tensor.get(tensor.tensor_id, [])
        covered_volume = sum(
            prod(fragment.local_shape) for fragment in tensor_fragments
        )
        if covered_volume != prod(tensor.global_shape) or _has_overlapping_boxes(
            tensor_fragments
        ):
            raise ValueError(f"tensor is not fully covered: {tensor.tensor_id}")


def _stored_alias_descriptor_key(
    tensor: TensorDescriptor,
) -> StoredAliasDescriptorKey:
    return (
        tensor.global_shape,
        tensor.dtype,
        tensor.itemsize,
        tensor.shard_dims,
        tensor.parallel_axes,
        tensor.layer_id,
        tensor.expert_id,
        tensor.layout_fingerprint,
    )


def _validate_stored_aliases(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[StoredFragmentSnapshot],
) -> None:
    tensor_by_id: dict[TensorId, TensorDescriptor] = {
        tensor.tensor_id: tensor for tensor in tensors
    }
    by_group_and_geometry: dict[
        StoredAliasGeometryKey, list[StoredFragmentSnapshot]
    ] = {}
    for fragment in fragments:
        if not fragment.aliases:
            continue
        unknown = sorted(set(fragment.aliases) - set(tensor_by_id))
        if unknown:
            raise ValueError(f"stored alias references unknown tensor: {unknown[0]}")
        key = (
            fragment.aliases,
            fragment.global_offset,
            fragment.local_shape,
            fragment.nbytes,
        )
        by_group_and_geometry.setdefault(key, []).append(fragment)

    for (aliases, *_), alias_fragments in by_group_and_geometry.items():
        tensor_ids = {fragment.tensor_id for fragment in alias_fragments}
        if tensor_ids != set(aliases):
            raise ValueError("stored alias group is incomplete for fragment geometry")
        descriptor_keys = {
            _stored_alias_descriptor_key(tensor_by_id[tensor_id])
            for tensor_id in tensor_ids
        }
        if len(descriptor_keys) != 1:
            raise ValueError("stored alias tensor descriptors differ")


def _validate_stored_object_ranges(
    fragments: Sequence[StoredFragmentSnapshot],
) -> None:
    by_object: dict[str, list[StoredFragmentSnapshot]] = {}
    for fragment in fragments:
        by_object.setdefault(fragment.object_key, []).append(fragment)

    for object_key, object_fragments in by_object.items():
        ordered = sorted(object_fragments, key=lambda item: item.object_offset)
        for previous, current in zip(ordered, ordered[1:]):
            if current.object_offset < previous.object_offset + previous.nbytes:
                raise ValueError(
                    "stored fragment object ranges overlap: "
                    f"{previous.fragment_id} and {current.fragment_id} "
                    f"in {object_key}"
                )


def _has_overlapping_boxes(fragments: Sequence[StoredFragmentSnapshot]) -> bool:
    if len(fragments) < 2:
        return False

    ndim = len(fragments[0].global_offset)
    sweep_dim = max(
        range(ndim),
        key=lambda dim: len(
            {
                (
                    fragment.global_offset[dim],
                    fragment.global_offset[dim] + fragment.local_shape[dim],
                )
                for fragment in fragments
            }
        ),
    )
    ordered = sorted(fragments, key=lambda item: item.global_offset[sweep_dim])
    active: list[StoredFragmentSnapshot] = []
    for fragment in ordered:
        begin = fragment.global_offset[sweep_dim]
        active = [
            candidate
            for candidate in active
            if candidate.global_offset[sweep_dim] + candidate.local_shape[sweep_dim]
            > begin
        ]
        for candidate in active:
            if all(
                left_offset < right_offset + right_extent
                and right_offset < left_offset + left_extent
                for left_offset, left_extent, right_offset, right_extent in _strict_zip(
                    candidate.global_offset,
                    candidate.local_shape,
                    fragment.global_offset,
                    fragment.local_shape,
                )
            ):
                return True
        active.append(fragment)
    return False
