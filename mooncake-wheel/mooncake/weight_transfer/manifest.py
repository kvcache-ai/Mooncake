"""Framework-neutral logical placement and ephemeral runtime binding contracts.

The logical half of the contract is serializable and contains no process-local
addresses. The runtime half carries addresses, endpoints, generations, leases,
and optional owner objects that keep framework allocations alive. Model
frameworks are responsible for supplying tensor semantics; this module does not
infer them from tensor names.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from math import prod
from typing import Any, Callable, Mapping, Optional, Sequence, Union


_MAX_U64 = (1 << 64) - 1


@dataclass(frozen=True)
class ParallelRank:
    """Framework-provided owner coordinates used only for routing.

    Logical sharding is defined by ``global_offset``, ``local_shape``, and
    ``shard_dims``. This coordinate is not a replacement for the topology and
    axis metadata used to synthesize a target placement.
    """

    dp: int = 0
    tp: int = 0
    pp: int = 0
    ep: int = 0

    def __post_init__(self) -> None:
        for name in ("dp", "tp", "pp", "ep"):
            _require_integer(getattr(self, name), f"parallel rank {name}", minimum=0)


@dataclass(frozen=True)
class TensorDescriptor:
    """Logical tensor identity, shape, dtype, and framework-supplied semantics."""

    tensor_id: str
    global_shape: tuple[int, ...]
    dtype: str
    itemsize: int
    partition_dim: Optional[int]
    layout_fingerprint: str
    layer_id: Optional[int] = None
    expert_id: Optional[int] = None
    shard_dims: Optional[tuple[int, ...]] = None

    def __post_init__(self) -> None:
        shape = _require_integer_tuple(self.global_shape, "global_shape", minimum=1)
        if not shape:
            raise ValueError("global_shape must not be empty")
        object.__setattr__(self, "global_shape", shape)
        _require_nonempty_string(self.tensor_id, "tensor_id")
        _require_nonempty_string(self.dtype, "dtype")
        _require_integer(self.itemsize, "itemsize", minimum=1)
        if self.partition_dim is not None:
            _require_integer(self.partition_dim, "partition_dim", minimum=0)
            if self.partition_dim >= len(shape):
                raise ValueError("partition_dim is out of range")
        if self.shard_dims is not None:
            shard_dims = _require_integer_tuple(
                self.shard_dims, "shard_dims", minimum=0
            )
            if len(shard_dims) != len(set(shard_dims)):
                raise ValueError("shard_dims must not contain duplicates")
            if tuple(sorted(shard_dims)) != shard_dims:
                raise ValueError("shard_dims must be sorted")
            if any(dim >= len(shape) for dim in shard_dims):
                raise ValueError("shard_dims contains an out-of-range dimension")
            if self.partition_dim is not None and shard_dims != (self.partition_dim,):
                raise ValueError("partition_dim conflicts with shard_dims")
            object.__setattr__(self, "shard_dims", shard_dims)
        for name in ("layer_id", "expert_id"):
            value = getattr(self, name)
            if value is not None:
                _require_integer(value, name, minimum=0)
        _require_nonempty_string(self.layout_fingerprint, "layout_fingerprint")

    @property
    def effective_shard_dims(self) -> tuple[int, ...]:
        """Return normalized shard dimensions for single-axis and N-D inputs."""

        if self.shard_dims is not None:
            return self.shard_dims
        if self.partition_dim is None:
            return ()
        return (self.partition_dim,)


@dataclass(frozen=True)
class RuntimeFragment:
    """One contiguous runtime view backing a logical tensor box.

    ``address`` always points at the first byte of this view, not at the base
    of the underlying framework storage.
    """

    fragment_id: str
    tensor_id: str
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    address: int
    nbytes: int
    worker_id: str
    endpoint: str
    device: str
    rank: ParallelRank
    lease_generation: int
    owner: Any = field(default=None, compare=False, repr=False)
    aliases: tuple[str, ...] = ()
    placement_fragment_id: Optional[str] = None

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
        for name in ("fragment_id", "tensor_id", "worker_id", "endpoint", "device"):
            _require_nonempty_string(getattr(self, name), name)
        _require_address_range(self.address, self.nbytes)
        _require_u64(self.lease_generation, "lease_generation")
        if not isinstance(self.rank, ParallelRank):
            raise ValueError("rank must be a ParallelRank")
        # Aliases are an opaque, framework-authoritative tied-storage group.
        # Shared addresses are still accepted only when geometry, dtype,
        # generation, layout, and the complete alias group also match.
        object.__setattr__(self, "aliases", _normalize_aliases(self.aliases))
        if self.placement_fragment_id is not None:
            _require_nonempty_string(
                self.placement_fragment_id, "placement_fragment_id"
            )


@dataclass(frozen=True)
class PlacementFragment:
    """An address-free logical tensor box assigned to one parallel rank."""

    placement_fragment_id: str
    tensor_id: str
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    nbytes: int
    rank: ParallelRank
    aliases: tuple[str, ...] = ()

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
        for name in ("placement_fragment_id", "tensor_id"):
            _require_nonempty_string(getattr(self, name), name)
        _require_u64(self.nbytes, "nbytes", minimum=1)
        if not isinstance(self.rank, ParallelRank):
            raise ValueError("rank must be a ParallelRank")
        object.__setattr__(self, "aliases", _normalize_aliases(self.aliases))

    @property
    def fragment_id(self) -> str:
        """Expose the common fragment identifier used by future planners."""

        return self.placement_fragment_id


@dataclass(frozen=True)
class RuntimeBindingFragment:
    """Contiguous physical runtime view for one placement fragment."""

    placement_fragment_id: str
    fragment_id: str
    address: int
    nbytes: int
    worker_id: str
    endpoint: str
    device: str
    owner: Any = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        for name in (
            "placement_fragment_id",
            "fragment_id",
            "worker_id",
            "endpoint",
            "device",
        ):
            _require_nonempty_string(getattr(self, name), name)
        _require_address_range(self.address, self.nbytes)


ManifestFragment = Union[RuntimeFragment, PlacementFragment]


def _require_nonempty_string(value: Any, name: str) -> None:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _require_integer(
    value: Any,
    name: str,
    *,
    minimum: Optional[int] = None,
) -> None:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


def _require_u64(value: Any, name: str, *, minimum: int = 0) -> None:
    _require_integer(value, name, minimum=minimum)
    if value > _MAX_U64:
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")


def _require_address_range(address: Any, nbytes: Any) -> None:
    _require_u64(address, "address", minimum=1)
    _require_u64(nbytes, "nbytes", minimum=1)
    if nbytes > _MAX_U64 - address:
        raise ValueError("address range must fit in an unsigned 64-bit integer")


def _require_sha256_digest(value: Any, name: str) -> None:
    _require_nonempty_string(value, name)
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_integer_tuple(
    value: Any,
    name: str,
    *,
    minimum: int,
) -> tuple[int, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{name} must contain integers")
    try:
        result = tuple(value)
    except TypeError as error:
        raise ValueError(f"{name} must contain integers") from error
    for item in result:
        _require_integer(item, name, minimum=minimum)
    return result


def _require_sequence(value: Any, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")
    return value


def _require_manifest_items(
    value: Any,
    name: str,
    item_type: type,
) -> tuple[Any, ...]:
    items = tuple(_require_sequence(value, name))
    if not all(isinstance(item, item_type) for item in items):
        raise ValueError(f"{name} must contain {item_type.__name__}")
    return items


def _read_field(value: Any, name: str) -> Any:
    try:
        if isinstance(value, Mapping):
            return value[name]
        return getattr(value, name)
    except (KeyError, AttributeError) as error:
        raise ValueError(f"missing required field: {name}") from error


def _read_optional_field(value: Any, name: str) -> Optional[Any]:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _read_aliases(value: Any) -> tuple[str, ...]:
    aliases = _read_optional_field(value, "aliases")
    if aliases is None:
        return ()
    if isinstance(aliases, (str, bytes, bytearray)) or not isinstance(
        aliases, Sequence
    ):
        raise ValueError("aliases must be a sequence of non-empty strings")
    return tuple(aliases)


def _normalize_aliases(value: Any) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError("aliases must contain non-empty strings")
    aliases = tuple(value)
    if any(type(alias) is not str or not alias for alias in aliases):
        raise ValueError("aliases must contain non-empty strings")
    if len(aliases) != len(set(aliases)):
        raise ValueError("aliases must not contain duplicates")
    return tuple(sorted(aliases))


def _canonical_stride(shape: tuple[int, ...]) -> tuple[int, ...]:
    stride = []
    running = 1
    for extent in reversed(shape):
        stride.append(running)
        running *= extent
    return tuple(reversed(stride))


def _validate_address_semantics(
    address_semantics: Optional[str],
    *,
    has_nonzero_offset: bool,
) -> None:
    if address_semantics is not None and address_semantics != "view":
        raise ValueError("address_semantics must be 'view'")
    if has_nonzero_offset and address_semantics != "view":
        raise ValueError("non-zero runtime offsets require address_semantics='view'")


def _validate_runtime_view(
    record: Any,
    descriptor: TensorDescriptor,
    address_semantics: Optional[str],
) -> None:
    is_contiguous = _read_field(record, "is_contiguous")
    if type(is_contiguous) is not bool or not is_contiguous:
        raise ValueError("runtime inventory tensor must be contiguous")

    local_shape = _require_integer_tuple(
        _read_field(record, "local_shape"), "local_shape", minimum=1
    )
    stride = _require_integer_tuple(_read_field(record, "stride"), "stride", minimum=0)
    if len(stride) != len(local_shape):
        raise ValueError("runtime inventory stride rank mismatch")
    canonical_stride = _canonical_stride(local_shape)
    if any(
        extent != 1 and actual != canonical
        for extent, actual, canonical in zip(local_shape, stride, canonical_stride)
    ):
        raise ValueError("runtime inventory tensor must use canonical stride")

    storage_offset = _read_field(record, "storage_offset")
    byte_offset = _read_field(record, "byte_offset")
    _require_integer(storage_offset, "storage_offset", minimum=0)
    _require_integer(byte_offset, "byte_offset", minimum=0)
    if byte_offset % descriptor.itemsize != 0:
        raise ValueError("runtime inventory byte_offset must be item-aligned")
    _validate_address_semantics(
        address_semantics,
        has_nonzero_offset=storage_offset != 0 or byte_offset != 0,
    )


def _runtime_binding_fragment_from_record(
    record: Any,
    owner_resolver: Optional[Callable[[Any], Any]],
    address_semantics: Optional[str],
    expected_generation: int,
) -> RuntimeBindingFragment:
    is_contiguous = _read_field(record, "is_contiguous")
    if type(is_contiguous) is not bool or not is_contiguous:
        raise ValueError("runtime binding allocation must be contiguous")
    fragment_generation = _read_optional_field(record, "lease_generation")
    if fragment_generation is not None:
        _require_u64(fragment_generation, "lease_generation")
        if fragment_generation != expected_generation:
            raise ValueError("runtime binding fragment lease generation mismatch")
    storage_offset = _read_optional_field(record, "storage_offset")
    byte_offset = _read_optional_field(record, "byte_offset")
    if storage_offset is not None:
        _require_integer(storage_offset, "storage_offset", minimum=0)
    if byte_offset is not None:
        _require_integer(byte_offset, "byte_offset", minimum=0)
    _validate_address_semantics(
        address_semantics,
        has_nonzero_offset=(
            storage_offset not in (None, 0) or byte_offset not in (None, 0)
        ),
    )
    return RuntimeBindingFragment(
        placement_fragment_id=_read_field(record, "placement_fragment_id"),
        fragment_id=_read_field(record, "fragment_id"),
        address=_read_field(record, "address"),
        nbytes=_read_field(record, "nbytes"),
        worker_id=_read_field(record, "worker_id"),
        endpoint=_read_field(record, "endpoint"),
        device=_read_field(record, "device"),
        owner=(owner_resolver(record) if owner_resolver is not None else None),
    )


def _require_exact_fields(
    value: Any, expected: frozenset[str], label: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f"{label} schema fields do not match contract")
    return value


def _load_json_object(value: str, label: str) -> Mapping[str, Any]:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    def reject_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
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
        raise ValueError(f"{label} is not valid JSON") from error
    if not isinstance(raw, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    return raw


def _validate_fragments(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[ManifestFragment],
) -> None:
    tensor_by_id: dict[str, TensorDescriptor] = {}
    for tensor in tensors:
        if tensor.tensor_id in tensor_by_id:
            raise ValueError(f"duplicate tensor_id: {tensor.tensor_id}")
        tensor_by_id[tensor.tensor_id] = tensor

    fragment_ids: set[str] = set()
    logical_fragments: set[tuple] = set()
    for fragment in fragments:
        if fragment.fragment_id in fragment_ids:
            raise ValueError(f"duplicate fragment_id: {fragment.fragment_id}")
        fragment_ids.add(fragment.fragment_id)
        logical_fragment = (
            fragment.tensor_id,
            fragment.rank,
            fragment.global_offset,
            fragment.local_shape,
        )
        if logical_fragment in logical_fragments:
            raise ValueError(
                "duplicate logical fragment for tensor and parallel rank: "
                f"{fragment.fragment_id}"
            )
        logical_fragments.add(logical_fragment)
        tensor = tensor_by_id.get(fragment.tensor_id)
        if tensor is None:
            raise ValueError(f"unknown tensor_id: {fragment.tensor_id}")
        _validate_fragment_geometry(tensor, fragment)


def _validate_fragment_geometry(
    tensor: TensorDescriptor,
    fragment: ManifestFragment,
) -> None:
    ndim = len(tensor.global_shape)
    if len(fragment.global_offset) != ndim or len(fragment.local_shape) != ndim:
        raise ValueError(f"fragment rank mismatch: {fragment.fragment_id}")
    for offset, extent, total in zip(
        fragment.global_offset,
        fragment.local_shape,
        tensor.global_shape,
    ):
        if offset + extent > total:
            raise ValueError(f"fragment is out of bounds: {fragment.fragment_id}")

    shard_dims = frozenset(tensor.effective_shard_dims)
    if not shard_dims:
        if fragment.global_offset != (0,) * ndim:
            raise ValueError(
                f"replicated fragment has an offset: {fragment.fragment_id}"
            )
        if fragment.local_shape != tensor.global_shape:
            raise ValueError(
                f"replicated fragment is incomplete: {fragment.fragment_id}"
            )
    else:
        for dim in range(ndim):
            if dim in shard_dims:
                continue
            if fragment.global_offset[dim] != 0:
                raise ValueError(f"fragment offset uses a non-shard axis: {dim}")
            if fragment.local_shape[dim] != tensor.global_shape[dim]:
                raise ValueError(f"fragment shape uses a non-shard axis: {dim}")

    expected_nbytes = prod(fragment.local_shape) * tensor.itemsize
    if fragment.nbytes != expected_nbytes:
        raise ValueError(
            f"fragment byte size mismatch: {fragment.fragment_id}: "
            f"expected {expected_nbytes}, got {fragment.nbytes}"
        )


def _runtime_alias_descriptor_key(tensor: TensorDescriptor) -> tuple:
    tensor = _canonical_tensor_descriptor(tensor)
    return (
        tensor.global_shape,
        tensor.dtype,
        tensor.itemsize,
        tensor.partition_dim,
        tensor.effective_shard_dims,
        tensor.layer_id,
        tensor.expert_id,
        tensor.layout_fingerprint,
    )


def _canonical_tensor_descriptor(tensor: TensorDescriptor) -> TensorDescriptor:
    shard_dims = tensor.effective_shard_dims
    partition_dim = shard_dims[0] if len(shard_dims) == 1 else None
    if tensor.shard_dims == shard_dims and tensor.partition_dim == partition_dim:
        return tensor
    return replace(
        tensor,
        partition_dim=partition_dim,
        shard_dims=shard_dims,
    )


def _is_exact_declared_runtime_alias(
    left: RuntimeFragment,
    right: RuntimeFragment,
    tensors: Mapping[str, TensorDescriptor],
) -> bool:
    return (
        left.address == right.address
        and left.nbytes == right.nbytes
        and left.lease_generation == right.lease_generation
        and len(left.aliases) >= 2
        and left.aliases == right.aliases
        and left.global_offset == right.global_offset
        and left.local_shape == right.local_shape
        and _runtime_alias_descriptor_key(tensors[left.tensor_id])
        == _runtime_alias_descriptor_key(tensors[right.tensor_id])
    )


def _validate_runtime_address_ranges(
    *,
    instance_id: str,
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[RuntimeFragment],
) -> None:
    tensor_by_id = {tensor.tensor_id: tensor for tensor in tensors}
    by_address_space: dict[tuple[str, str, str], list[RuntimeFragment]] = {}
    for fragment in fragments:
        address_space = (instance_id, fragment.worker_id, fragment.device)
        by_address_space.setdefault(address_space, []).append(fragment)

    for address_space, address_fragments in by_address_space.items():
        ordered = sorted(address_fragments, key=lambda item: item.address)
        active: list[RuntimeFragment] = []
        for current in ordered:
            active = [
                previous
                for previous in active
                if previous.address + previous.nbytes > current.address
            ]
            for previous in active:
                if _is_exact_declared_runtime_alias(
                    previous,
                    current,
                    tensor_by_id,
                ):
                    continue
                raise ValueError(
                    "runtime manifest address ranges overlap: "
                    f"{previous.fragment_id} and {current.fragment_id} "
                    f"in {address_space}"
                )
            active.append(current)


@dataclass(frozen=True)
class PlacementManifest:
    """Serializable address-free placement with a canonical content ID."""

    model_id: str
    revision: str
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[PlacementFragment, ...]
    placement_id: Optional[str] = None

    def __post_init__(self) -> None:
        tensors = _require_manifest_items(
            self.tensors, "PlacementManifest tensors", TensorDescriptor
        )
        tensors = tuple(_canonical_tensor_descriptor(tensor) for tensor in tensors)
        fragments = _require_manifest_items(
            self.fragments, "PlacementManifest fragments", PlacementFragment
        )
        object.__setattr__(
            self,
            "tensors",
            tuple(sorted(tensors, key=lambda item: item.tensor_id)),
        )
        object.__setattr__(
            self,
            "fragments",
            tuple(
                sorted(
                    fragments,
                    key=lambda item: item.placement_fragment_id,
                )
            ),
        )
        for name in ("model_id", "revision"):
            _require_nonempty_string(getattr(self, name), name)
        _validate_fragments(self.tensors, self.fragments)
        canonical_placement_id = _logical_placement_id(
            model_id=self.model_id,
            revision=self.revision,
            tensors=self.tensors,
            fragments=self.fragments,
        )
        if self.placement_id is None:
            object.__setattr__(self, "placement_id", canonical_placement_id)
        else:
            _require_nonempty_string(self.placement_id, "placement_id")
            if self.placement_id != canonical_placement_id:
                raise ValueError(
                    "placement_id does not match canonical logical content"
                )

    @property
    def digest(self) -> str:
        """Return the stable SHA-256 digest of the canonical JSON form."""

        return hashlib.sha256(self.to_json().encode()).hexdigest()

    def to_json(self) -> str:
        """Serialize logical placement without any runtime location."""

        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, value: str) -> PlacementManifest:
        """Parse a strict placement manifest."""

        manifest = _require_exact_fields(
            _load_json_object(value, "placement manifest"),
            frozenset(
                {
                    "model_id",
                    "revision",
                    "placement_id",
                    "tensors",
                    "fragments",
                }
            ),
            "placement manifest",
        )

        tensor_fields = frozenset(
            {
                "tensor_id",
                "global_shape",
                "dtype",
                "itemsize",
                "partition_dim",
                "layer_id",
                "expert_id",
                "layout_fingerprint",
                "shard_dims",
            }
        )
        tensors = []
        for index, item in enumerate(
            _require_sequence(manifest["tensors"], "placement tensors")
        ):
            tensor = _require_exact_fields(
                item, tensor_fields, f"placement tensor {index}"
            )
            tensors.append(
                TensorDescriptor(
                    tensor_id=tensor["tensor_id"],
                    global_shape=tensor["global_shape"],
                    dtype=tensor["dtype"],
                    itemsize=tensor["itemsize"],
                    partition_dim=tensor["partition_dim"],
                    layer_id=tensor["layer_id"],
                    expert_id=tensor["expert_id"],
                    layout_fingerprint=tensor["layout_fingerprint"],
                    shard_dims=(
                        tensor["shard_dims"]
                        if tensor["shard_dims"] is not None
                        else None
                    ),
                )
            )

        fragment_fields = frozenset(
            {
                "placement_fragment_id",
                "tensor_id",
                "global_offset",
                "local_shape",
                "nbytes",
                "rank",
                "aliases",
            }
        )
        rank_fields = frozenset({"dp", "tp", "pp", "ep"})
        fragments = []
        for index, item in enumerate(
            _require_sequence(manifest["fragments"], "placement fragments")
        ):
            fragment = _require_exact_fields(
                item, fragment_fields, f"placement fragment {index}"
            )
            rank = _require_exact_fields(
                fragment["rank"], rank_fields, f"placement rank {index}"
            )
            fragments.append(
                PlacementFragment(
                    placement_fragment_id=fragment["placement_fragment_id"],
                    tensor_id=fragment["tensor_id"],
                    global_offset=fragment["global_offset"],
                    local_shape=fragment["local_shape"],
                    nbytes=fragment["nbytes"],
                    rank=ParallelRank(**rank),
                    aliases=fragment["aliases"],
                )
            )
        return cls(
            model_id=manifest["model_id"],
            revision=manifest["revision"],
            placement_id=manifest["placement_id"],
            tensors=tuple(tensors),
            fragments=tuple(fragments),
        )

    @classmethod
    def from_runtime_inventory(cls, inventory: Any) -> PlacementManifest:
        """Import framework-provided logical placement records."""

        tensors: dict[str, TensorDescriptor] = {}
        fragments = []
        for record in _require_sequence(
            _read_field(inventory, "tensors"), "placement inventory tensors"
        ):
            shard_dims = _read_optional_field(record, "shard_dims")
            descriptor = TensorDescriptor(
                tensor_id=_read_field(record, "tensor_id"),
                global_shape=_read_field(record, "global_shape"),
                dtype=_read_field(record, "dtype"),
                itemsize=_read_field(record, "itemsize"),
                partition_dim=_read_field(record, "partition_dim"),
                layer_id=_read_optional_field(record, "layer_id"),
                expert_id=_read_optional_field(record, "expert_id"),
                layout_fingerprint=_read_field(record, "layout_fingerprint"),
                shard_dims=shard_dims,
            )
            descriptor = _canonical_tensor_descriptor(descriptor)
            previous = tensors.setdefault(descriptor.tensor_id, descriptor)
            if previous != descriptor:
                raise ValueError(
                    f"placement descriptor mismatch: {descriptor.tensor_id}"
                )
            rank = _read_field(record, "rank")
            fragments.append(
                PlacementFragment(
                    placement_fragment_id=_read_field(record, "placement_fragment_id"),
                    tensor_id=descriptor.tensor_id,
                    global_offset=_read_field(record, "global_offset"),
                    local_shape=_read_field(record, "local_shape"),
                    nbytes=_read_field(record, "nbytes"),
                    rank=ParallelRank(
                        dp=_read_field(rank, "dp"),
                        tp=_read_field(rank, "tp"),
                        pp=_read_field(rank, "pp"),
                        ep=_read_field(rank, "ep"),
                    ),
                    aliases=_read_aliases(record),
                )
            )
        return cls(
            model_id=_read_field(inventory, "model_id"),
            revision=_read_field(inventory, "revision"),
            tensors=tuple(sorted(tensors.values(), key=lambda item: item.tensor_id)),
            fragments=tuple(fragments),
            placement_id=_read_optional_field(inventory, "placement_id"),
        )


@dataclass(frozen=True)
class RuntimeBindingManifest:
    """Ephemeral physical locations and lifetime fence for one placement."""

    model_id: str
    revision: str
    placement_id: str
    placement_digest: str
    instance_id: str
    generation: int
    lease_id: str
    fragments: tuple[RuntimeBindingFragment, ...]

    def __post_init__(self) -> None:
        fragments = _require_manifest_items(
            self.fragments,
            "RuntimeBindingManifest fragments",
            RuntimeBindingFragment,
        )
        object.__setattr__(
            self,
            "fragments",
            tuple(
                sorted(
                    fragments,
                    key=lambda item: item.placement_fragment_id,
                )
            ),
        )
        for name in (
            "model_id",
            "revision",
            "placement_id",
            "instance_id",
            "lease_id",
        ):
            _require_nonempty_string(getattr(self, name), name)
        _require_sha256_digest(self.placement_digest, "placement_digest")
        _require_u64(self.generation, "generation")
        placement_ids = [item.placement_fragment_id for item in self.fragments]
        if len(placement_ids) != len(set(placement_ids)):
            raise ValueError("duplicate placement fragment in runtime binding")
        fragment_ids = [item.fragment_id for item in self.fragments]
        if len(fragment_ids) != len(set(fragment_ids)):
            raise ValueError("duplicate runtime fragment_id in runtime binding")

    @classmethod
    def from_runtime_inventory(
        cls,
        inventory: Any,
        *,
        owner_resolver: Optional[Callable[[Any], Any]] = None,
        address_semantics: Optional[str] = None,
    ) -> RuntimeBindingManifest:
        """Import framework runtime locations without importing the framework.

        A non-zero framework storage or byte offset requires
        ``address_semantics="view"`` to declare that each address is already
        normalized to the first byte of the transferable view.
        """

        _validate_address_semantics(
            address_semantics,
            has_nonzero_offset=False,
        )
        generation = _read_field(inventory, "generation")
        _require_u64(generation, "generation")
        return cls(
            model_id=_read_field(inventory, "model_id"),
            revision=_read_field(inventory, "revision"),
            placement_id=_read_field(inventory, "placement_id"),
            placement_digest=_read_field(inventory, "placement_digest"),
            instance_id=_read_field(inventory, "instance_id"),
            generation=generation,
            lease_id=_read_field(inventory, "lease_id"),
            fragments=tuple(
                _runtime_binding_fragment_from_record(
                    record,
                    owner_resolver,
                    address_semantics,
                    generation,
                )
                for record in _require_sequence(
                    _read_field(inventory, "fragments"),
                    "runtime binding fragments",
                )
            ),
        )


@dataclass(frozen=True)
class RuntimeManifest:
    """A validated runtime snapshot containing logical and physical facts."""

    model_id: str
    revision: str
    instance_id: str
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[RuntimeFragment, ...]
    lease_id: Optional[str] = None
    placement_id: Optional[str] = None
    generation: Optional[int] = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "tensors",
            _require_manifest_items(
                self.tensors,
                "RuntimeManifest tensors",
                TensorDescriptor,
            ),
        )
        object.__setattr__(
            self,
            "fragments",
            _require_manifest_items(
                self.fragments,
                "RuntimeManifest fragments",
                RuntimeFragment,
            ),
        )
        for name in ("model_id", "revision", "instance_id"):
            _require_nonempty_string(getattr(self, name), name)
        if self.lease_id is not None:
            _require_nonempty_string(self.lease_id, "lease_id")
        if self.placement_id is not None:
            _require_nonempty_string(self.placement_id, "placement_id")
        if self.generation is not None:
            _require_u64(self.generation, "generation")
        fragment_generations = {
            fragment.lease_generation for fragment in self.fragments
        }
        if len(fragment_generations) > 1:
            raise ValueError("runtime manifest has inconsistent lease generations")
        if fragment_generations:
            fragment_generation = next(iter(fragment_generations))
            if self.generation is None:
                object.__setattr__(self, "generation", fragment_generation)
            elif self.generation != fragment_generation:
                raise ValueError(
                    "runtime manifest generation does not match fragment generation"
                )

        _validate_fragments(self.tensors, self.fragments)
        _validate_runtime_address_ranges(
            instance_id=self.instance_id,
            tensors=self.tensors,
            fragments=self.fragments,
        )

    @classmethod
    def from_runtime_inventory(
        cls,
        inventory: Any,
        *,
        owner_resolver: Optional[Callable[[Any], Any]] = None,
        address_semantics: Optional[str] = None,
    ) -> RuntimeManifest:
        """Import and validate a framework runtime tensor inventory.

        A non-zero framework view offset requires
        ``address_semantics="view"`` to declare that ``address`` is already
        normalized to the first byte of that view.
        """

        _validate_address_semantics(
            address_semantics,
            has_nonzero_offset=False,
        )
        generation = _read_field(inventory, "generation")
        _require_integer(generation, "generation", minimum=0)
        tensors: dict[str, TensorDescriptor] = {}
        fragments = []
        for record in _require_sequence(
            _read_field(inventory, "tensors"), "runtime inventory tensors"
        ):
            shard_dims = _read_optional_field(record, "shard_dims")
            descriptor = TensorDescriptor(
                tensor_id=_read_field(record, "tensor_id"),
                global_shape=_read_field(record, "global_shape"),
                dtype=_read_field(record, "dtype"),
                itemsize=_read_field(record, "itemsize"),
                partition_dim=_read_field(record, "partition_dim"),
                layer_id=_read_optional_field(record, "layer_id"),
                expert_id=_read_optional_field(record, "expert_id"),
                layout_fingerprint=_read_field(record, "layout_fingerprint"),
                shard_dims=shard_dims,
            )
            descriptor = _canonical_tensor_descriptor(descriptor)
            previous = tensors.setdefault(descriptor.tensor_id, descriptor)
            if previous != descriptor:
                raise ValueError(
                    f"runtime inventory descriptor mismatch: {descriptor.tensor_id}"
                )
            _validate_runtime_view(record, descriptor, address_semantics)
            rank = _read_field(record, "rank")
            lease_generation = _read_field(record, "lease_generation")
            _require_integer(lease_generation, "lease_generation", minimum=0)
            if lease_generation != generation:
                raise ValueError(
                    "runtime inventory lease generation mismatch: "
                    f"{lease_generation} != {generation}"
                )
            fragments.append(
                RuntimeFragment(
                    fragment_id=_read_field(record, "fragment_id"),
                    tensor_id=descriptor.tensor_id,
                    global_offset=_read_field(record, "global_offset"),
                    local_shape=_read_field(record, "local_shape"),
                    address=_read_field(record, "address"),
                    nbytes=_read_field(record, "nbytes"),
                    worker_id=_read_field(record, "worker_id"),
                    endpoint=_read_field(record, "endpoint"),
                    device=_read_field(record, "device"),
                    rank=ParallelRank(
                        dp=_read_field(rank, "dp"),
                        tp=_read_field(rank, "tp"),
                        pp=_read_field(rank, "pp"),
                        ep=_read_field(rank, "ep"),
                    ),
                    lease_generation=lease_generation,
                    owner=(
                        owner_resolver(record) if owner_resolver is not None else None
                    ),
                    aliases=_read_aliases(record),
                    placement_fragment_id=_read_optional_field(
                        record, "placement_fragment_id"
                    ),
                )
            )
        return cls(
            model_id=_read_field(inventory, "model_id"),
            revision=_read_field(inventory, "revision"),
            instance_id=_read_field(inventory, "instance_id"),
            tensors=tuple(sorted(tensors.values(), key=lambda item: item.tensor_id)),
            fragments=tuple(fragments),
            lease_id=_read_optional_field(inventory, "lease_id"),
            placement_id=_read_optional_field(inventory, "placement_id"),
            generation=generation,
        )


def _canonical_json_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _logical_fragment_id(fragment: RuntimeFragment) -> str:
    if fragment.placement_fragment_id is not None:
        return fragment.placement_fragment_id
    content = {
        "tensor_id": fragment.tensor_id,
        "global_offset": fragment.global_offset,
        "local_shape": fragment.local_shape,
        "nbytes": fragment.nbytes,
        "rank": asdict(fragment.rank),
        "aliases": fragment.aliases,
    }
    return f"logical:{_canonical_json_digest(content)}"


def _logical_placement_id(
    *,
    model_id: str,
    revision: str,
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
) -> str:
    content = {
        "schema": "weight-placement",
        "model_id": model_id,
        "revision": revision,
        "tensors": [
            asdict(_canonical_tensor_descriptor(tensor))
            for tensor in sorted(tensors, key=lambda item: item.tensor_id)
        ],
        "fragments": [
            asdict(fragment)
            for fragment in sorted(
                fragments,
                key=lambda item: item.placement_fragment_id,
            )
        ],
    }
    return f"sha256:{_canonical_json_digest(content)}"


def placement_manifest_from_runtime_manifest(
    manifest: RuntimeManifest,
    *,
    placement_id: Optional[str] = None,
) -> PlacementManifest:
    """Project a runtime snapshot into address-free placement semantics."""

    fragments = tuple(
        PlacementFragment(
            placement_fragment_id=_logical_fragment_id(fragment),
            tensor_id=fragment.tensor_id,
            global_offset=fragment.global_offset,
            local_shape=fragment.local_shape,
            nbytes=fragment.nbytes,
            rank=fragment.rank,
            aliases=fragment.aliases,
        )
        for fragment in manifest.fragments
    )
    if placement_id is not None:
        _require_nonempty_string(placement_id, "placement_id")
    effective_placement_id = placement_id or manifest.placement_id
    if effective_placement_id is None:
        effective_placement_id = _logical_placement_id(
            model_id=manifest.model_id,
            revision=manifest.revision,
            tensors=manifest.tensors,
            fragments=fragments,
        )
    return PlacementManifest(
        model_id=manifest.model_id,
        revision=manifest.revision,
        placement_id=effective_placement_id,
        tensors=manifest.tensors,
        fragments=fragments,
    )


def runtime_binding_from_runtime_manifest(
    manifest: RuntimeManifest,
    *,
    placement_id: Optional[str] = None,
) -> RuntimeBindingManifest:
    """Project runtime locations into the ephemeral binding contract."""

    if manifest.lease_id is None:
        raise ValueError("runtime manifest has no lease_id for runtime binding")
    if manifest.generation is None:
        raise ValueError("runtime manifest has no generation for runtime binding")
    placement = placement_manifest_from_runtime_manifest(
        manifest,
        placement_id=placement_id,
    )
    return RuntimeBindingManifest(
        model_id=manifest.model_id,
        revision=manifest.revision,
        placement_id=placement.placement_id,
        placement_digest=placement.digest,
        instance_id=manifest.instance_id,
        generation=manifest.generation,
        lease_id=manifest.lease_id,
        fragments=tuple(
            RuntimeBindingFragment(
                placement_fragment_id=_logical_fragment_id(fragment),
                fragment_id=fragment.fragment_id,
                address=fragment.address,
                nbytes=fragment.nbytes,
                worker_id=fragment.worker_id,
                endpoint=fragment.endpoint,
                device=fragment.device,
                owner=fragment.owner,
            )
            for fragment in manifest.fragments
        ),
    )


def bind_runtime_manifest(
    placement: PlacementManifest,
    binding: RuntimeBindingManifest,
) -> RuntimeManifest:
    """Bind physical runtime locations to an exact logical placement."""

    if placement.model_id != binding.model_id:
        raise ValueError("placement and runtime binding model_id differ")
    if placement.revision != binding.revision:
        raise ValueError("placement and runtime binding revision differ")
    if placement.placement_id != binding.placement_id:
        raise ValueError("placement_id and runtime binding placement_id differ")
    if placement.digest != binding.placement_digest:
        raise ValueError("placement digest and runtime binding placement digest differ")

    placement_by_id = {
        fragment.placement_fragment_id: fragment for fragment in placement.fragments
    }
    binding_by_id = {
        fragment.placement_fragment_id: fragment for fragment in binding.fragments
    }
    unknown = sorted(binding_by_id.keys() - placement_by_id.keys())
    if unknown:
        raise ValueError(f"unknown placement fragment in runtime binding: {unknown[0]}")
    missing = sorted(placement_by_id.keys() - binding_by_id.keys())
    if missing:
        raise ValueError(f"missing placement fragment in runtime binding: {missing[0]}")

    fragments = []
    for placement_fragment in placement.fragments:
        runtime = binding_by_id[placement_fragment.placement_fragment_id]
        if runtime.nbytes != placement_fragment.nbytes:
            raise ValueError(
                "runtime binding byte size does not match placement: "
                f"{placement_fragment.placement_fragment_id}"
            )
        fragments.append(
            RuntimeFragment(
                fragment_id=runtime.fragment_id,
                tensor_id=placement_fragment.tensor_id,
                global_offset=placement_fragment.global_offset,
                local_shape=placement_fragment.local_shape,
                address=runtime.address,
                nbytes=runtime.nbytes,
                worker_id=runtime.worker_id,
                endpoint=runtime.endpoint,
                device=runtime.device,
                rank=placement_fragment.rank,
                lease_generation=binding.generation,
                owner=runtime.owner,
                aliases=placement_fragment.aliases,
                placement_fragment_id=placement_fragment.placement_fragment_id,
            )
        )
    return RuntimeManifest(
        model_id=placement.model_id,
        revision=placement.revision,
        instance_id=binding.instance_id,
        tensors=placement.tensors,
        fragments=tuple(fragments),
        lease_id=binding.lease_id,
        placement_id=placement.placement_id,
        generation=binding.generation,
    )
