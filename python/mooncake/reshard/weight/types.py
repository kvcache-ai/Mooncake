"""Public model-weight tensor and fragment contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from math import prod
from typing import Literal, Optional, TypeVar, Union, cast

from ..contracts import PlacementFragmentId, TensorId
from ..contracts import RuntimeBindingFragment as _RuntimeBindingFragment

# Preserve the existing weight.types import surface while keeping the canonical
# runtime fragment definition resource-neutral in mooncake.reshard.contracts.
RuntimeBindingFragment = _RuntimeBindingFragment

_MAX_U64 = (1 << 64) - 1
ParallelAxisKind = Literal["dp", "pp", "ep", "tp"]
SplitAxisKind = Literal["ep", "tp"]
_PARALLEL_AXIS_ORDER: dict[ParallelAxisKind, int] = {
    "dp": 0,
    "pp": 1,
    "ep": 2,
    "tp": 3,
}
_T = TypeVar("_T")


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
class SplitAxis:
    """An axis whose ranks collectively partition one logical tensor."""

    kind: SplitAxisKind
    dim: int

    def __post_init__(self) -> None:
        _validate_parallel_axis_kind(self.kind)
        if self.kind not in {"ep", "tp"}:
            raise ValueError(f"{self.kind} cannot use split semantics")
        _require_integer(self.dim, "split axis dim", minimum=0)
        if self.kind == "ep" and self.dim != 0:
            raise ValueError("EP must split the leading logical expert dimension")


@dataclass(frozen=True)
class ReplicatedAxis:
    """An axis whose ranks each contain an independent complete tensor."""

    kind: ParallelAxisKind

    def __post_init__(self) -> None:
        _validate_parallel_axis_kind(self.kind)


@dataclass(frozen=True)
class OwnershipAxis:
    """An axis where only explicitly declared owner ranks contain the tensor."""

    kind: ParallelAxisKind

    def __post_init__(self) -> None:
        _validate_parallel_axis_kind(self.kind)


ParallelAxis = Union[SplitAxis, ReplicatedAxis, OwnershipAxis]


@dataclass(frozen=True)
class TensorDescriptor:
    """Logical tensor identity, shape, dtype, and framework-supplied semantics."""

    tensor_id: TensorId
    global_shape: tuple[int, ...]
    dtype: str
    itemsize: int
    shard_dims: tuple[int, ...]
    layout_fingerprint: str
    parallel_axes: tuple[ParallelAxis, ...]
    layer_id: Optional[int] = None
    expert_id: Optional[int] = None

    def __post_init__(self) -> None:
        shape = _require_integer_tuple(self.global_shape, "global_shape", minimum=1)
        if not shape:
            raise ValueError("global_shape must not be empty")
        object.__setattr__(self, "global_shape", shape)
        _require_nonempty_string(self.tensor_id, "tensor_id")
        _require_nonempty_string(self.dtype, "dtype")
        _require_integer(self.itemsize, "itemsize", minimum=1)
        shard_dims = _require_integer_tuple(self.shard_dims, "shard_dims", minimum=0)
        if len(shard_dims) != len(set(shard_dims)):
            raise ValueError("shard_dims must not contain duplicates")
        if tuple(sorted(shard_dims)) != shard_dims:
            raise ValueError("shard_dims must be sorted")
        if any(dim >= len(shape) for dim in shard_dims):
            raise ValueError("shard_dims contains an out-of-range dimension")
        object.__setattr__(self, "shard_dims", shard_dims)
        raw_axes = _require_sequence(
            self.parallel_axes,
            "TensorDescriptor parallel_axes",
        )
        if not all(
            isinstance(axis, (SplitAxis, ReplicatedAxis, OwnershipAxis))
            for axis in raw_axes
        ):
            raise ValueError(
                "TensorDescriptor parallel_axes must contain explicit axis values"
            )
        parallel_axes: tuple[ParallelAxis, ...] = tuple(
            cast(ParallelAxis, axis) for axis in raw_axes
        )
        parallel_axes = tuple(
            sorted(parallel_axes, key=lambda axis: _PARALLEL_AXIS_ORDER[axis.kind])
        )
        axis_kinds = tuple(axis.kind for axis in parallel_axes)
        if len(axis_kinds) != len(set(axis_kinds)):
            raise ValueError("parallel_axes must not contain duplicate kinds")
        split_dims = tuple(
            axis.dim for axis in parallel_axes if isinstance(axis, SplitAxis)
        )
        if len(split_dims) != len(set(split_dims)):
            raise ValueError("split axes must not share a dimension")
        if any(dim >= len(shape) for dim in split_dims):
            raise ValueError("split axis dimension is out of range")
        if tuple(sorted(split_dims)) != shard_dims:
            raise ValueError("parallel axis split dimensions conflict with shard_dims")
        object.__setattr__(self, "parallel_axes", parallel_axes)
        for name in ("layer_id", "expert_id"):
            value = getattr(self, name)
            if value is not None:
                _require_integer(value, name, minimum=0)
        if self.expert_id is not None and any(
            axis.kind == "ep" and isinstance(axis, SplitAxis) for axis in parallel_axes
        ):
            raise ValueError(
                "an individually allocated expert requires explicit EP ownership"
            )
        _require_nonempty_string(self.layout_fingerprint, "layout_fingerprint")


def validate_fragment_geometry(
    tensor: TensorDescriptor,
    *,
    fragment_id: str,
    global_offset: tuple[int, ...],
    local_shape: tuple[int, ...],
    nbytes: int,
) -> None:
    """Validate shared local geometry without requiring complete tensor coverage."""

    ndim = len(tensor.global_shape)
    if len(global_offset) != ndim or len(local_shape) != ndim:
        raise ValueError(f"fragment rank mismatch: {fragment_id}")
    for offset, extent, total in zip(
        global_offset,
        local_shape,
        tensor.global_shape,
    ):
        if offset + extent > total:
            raise ValueError(f"fragment is out of bounds: {fragment_id}")

    expected_nbytes = prod(local_shape) * tensor.itemsize
    if nbytes != expected_nbytes:
        raise ValueError(
            f"fragment byte size mismatch: {fragment_id}: "
            f"expected {expected_nbytes}, got {nbytes}"
        )


def canonical_strides_bytes(
    shape: tuple[int, ...],
    itemsize: int,
) -> tuple[int, ...]:
    strides: list[int] = []
    running = itemsize
    for extent in reversed(shape):
        strides.append(running)
        running *= extent
    return tuple(reversed(strides))


@dataclass(frozen=True, init=False)
class PlacementFragment:
    """An address-free logical tensor box assigned to one parallel rank."""

    tensor_id: TensorId
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    nbytes: int
    rank: ParallelRank
    pipeline_stage_id: Optional[int]
    aliases: tuple[TensorId, ...]
    placement_fragment_id: PlacementFragmentId

    def __init__(
        self,
        tensor_id: TensorId,
        global_offset: tuple[int, ...],
        local_shape: tuple[int, ...],
        nbytes: int,
        rank: ParallelRank,
        pipeline_stage_id: Optional[int] = None,
        aliases: tuple[TensorId, ...] = (),
        placement_fragment_id: Optional[PlacementFragmentId] = None,
    ) -> None:
        normalized_offset = _require_integer_tuple(
            global_offset,
            "global_offset",
            minimum=0,
        )
        normalized_shape = _require_integer_tuple(
            local_shape,
            "local_shape",
            minimum=1,
        )
        _require_nonempty_string(tensor_id, "tensor_id")
        _require_u64(nbytes, "nbytes", minimum=1)
        if not isinstance(rank, ParallelRank):
            raise ValueError("rank must be a ParallelRank")  # noqa: TRY004
        if pipeline_stage_id is not None:
            _require_integer(pipeline_stage_id, "pipeline_stage_id", minimum=0)
        normalized_aliases = _normalize_aliases(aliases)
        if normalized_aliases:
            if len(normalized_aliases) < 2:
                raise ValueError("alias group must contain at least two tensor IDs")
            if tensor_id not in normalized_aliases:
                raise ValueError("alias group must contain the fragment tensor_id")
        if placement_fragment_id is not None:
            _require_nonempty_string(
                placement_fragment_id,
                "placement_fragment_id",
            )
            resolved_fragment_id = placement_fragment_id
        else:
            resolved_fragment_id = _canonical_placement_fragment_id(
                tensor_id=tensor_id,
                global_offset=normalized_offset,
                local_shape=normalized_shape,
                nbytes=nbytes,
                rank=rank,
                pipeline_stage_id=pipeline_stage_id,
                aliases=normalized_aliases,
            )
        object.__setattr__(self, "tensor_id", tensor_id)
        object.__setattr__(self, "global_offset", normalized_offset)
        object.__setattr__(self, "local_shape", normalized_shape)
        object.__setattr__(self, "nbytes", nbytes)
        object.__setattr__(self, "rank", rank)
        object.__setattr__(self, "pipeline_stage_id", pipeline_stage_id)
        object.__setattr__(self, "aliases", normalized_aliases)
        object.__setattr__(self, "placement_fragment_id", resolved_fragment_id)

    @property
    def fragment_id(self) -> PlacementFragmentId:
        """Expose the common fragment identifier used by future planners."""

        return self.placement_fragment_id


def _require_nonempty_string(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _require_integer(
    value: object,
    name: str,
    *,
    minimum: Optional[int] = None,
) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _require_u64(value: object, name: str, *, minimum: int = 0) -> int:
    integer = _require_integer(value, name, minimum=minimum)
    if integer > _MAX_U64:
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")
    return integer


def require_sha256_digest(value: object, name: str) -> None:
    digest = _require_nonempty_string(value, name)
    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_integer_tuple(
    value: object,
    name: str,
    *,
    minimum: int,
) -> tuple[int, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must contain integers")  # noqa: TRY004
    items = cast(Sequence[object], value)
    return tuple(_require_integer(item, name, minimum=minimum) for item in items)


def _require_sequence(value: object, name: str) -> Sequence[object]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")  # noqa: TRY004
    return cast(Sequence[object], value)


def require_manifest_items(
    value: object,
    name: str,
    item_type: type[_T],
) -> tuple[_T, ...]:
    items = tuple(_require_sequence(value, name))
    if not all(isinstance(item, item_type) for item in items):
        raise ValueError(f"{name} must contain {item_type.__name__}")
    return tuple(cast(_T, item) for item in items)


def _normalize_aliases(value: object) -> tuple[TensorId, ...]:
    aliases = tuple(_require_sequence(value, "aliases"))
    if any(type(alias) is not str or not alias for alias in aliases):
        raise ValueError("aliases must contain non-empty strings")
    normalized_aliases = tuple(cast(str, alias) for alias in aliases)
    if len(normalized_aliases) != len(set(normalized_aliases)):
        raise ValueError("aliases must not contain duplicates")
    return tuple(TensorId(alias) for alias in sorted(normalized_aliases))


def _canonical_placement_fragment_id(
    *,
    tensor_id: TensorId,
    global_offset: tuple[int, ...],
    local_shape: tuple[int, ...],
    nbytes: int,
    rank: ParallelRank,
    pipeline_stage_id: Optional[int],
    aliases: tuple[TensorId, ...],
) -> PlacementFragmentId:
    content = {
        "schema": "weight-placement-fragment",
        "tensor_id": tensor_id,
        "global_offset": global_offset,
        "local_shape": local_shape,
        "nbytes": nbytes,
        "rank": asdict(rank),
        "aliases": aliases,
    }
    if pipeline_stage_id is not None:
        content["pipeline_stage_id"] = pipeline_stage_id
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    return PlacementFragmentId(f"sha256:{hashlib.sha256(encoded).hexdigest()}")


def _validate_parallel_axis_kind(kind: ParallelAxisKind) -> None:
    _require_nonempty_string(kind, "parallel axis kind")
    if kind not in _PARALLEL_AXIS_ORDER:
        raise ValueError(f"unsupported parallel axis kind: {kind}")


# Retain internal imports used by this first-stage contract package. These
# aliases do not widen the public wire or runtime contract.
_canonical_strides_bytes = canonical_strides_bytes
_require_manifest_items = require_manifest_items
_require_sha256_digest = require_sha256_digest
