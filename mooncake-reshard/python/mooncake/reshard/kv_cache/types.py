"""Canonical KV-cache values shared by placement and runtime contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from enum import Enum
from typing import TypeVar, cast

from ..contracts import (
    ParticipantId,
    PlacementFragmentId,
    RuntimeBindingFragment,
)

_MAX_U64 = (1 << 64) - 1
_T = TypeVar("_T")


class KVCacheComponent(str, Enum):
    KEY = "key"
    VALUE = "value"


class KVCacheLayout(str, Enum):
    NHD = "nhd"


# Preserve the KV-cache public name while sharing the resource-neutral type.
KVCacheRuntimeBuffer = RuntimeBindingFragment


@dataclass(frozen=True)
class KVCacheRank:
    """Framework-provided participant coordinates."""

    dp: int = 0
    pp: int = 0
    tp: int = 0

    def __post_init__(self) -> None:
        for name in ("dp", "pp", "tp"):
            require_integer(getattr(self, name), f"parallel rank {name}")


@dataclass(frozen=True)
class KVCacheDescriptor:
    """Framework-neutral logical schema shared by one complete placement."""

    global_layer_ids: tuple[int, ...]
    dtype: str
    itemsize: int
    page_size: int
    total_kv_heads: int
    key_head_dim: int
    value_head_dim: int
    layout: KVCacheLayout = KVCacheLayout.NHD

    def __post_init__(self) -> None:
        layers = require_integer_tuple(self.global_layer_ids, "global_layer_ids")
        if not layers:
            raise ValueError("global_layer_ids must not be empty")
        if len(layers) != len(set(layers)):
            raise ValueError("global_layer_ids must be unique")
        object.__setattr__(self, "global_layer_ids", tuple(sorted(layers)))
        require_nonempty_string(self.dtype, "dtype")
        for name in (
            "itemsize",
            "page_size",
            "total_kv_heads",
            "key_head_dim",
            "value_head_dim",
        ):
            require_integer(getattr(self, name), name, minimum=1)
        if not isinstance(self.layout, KVCacheLayout):
            raise ValueError("layout must be a KVCacheLayout")  # noqa: TRY004
        if self.layout is not KVCacheLayout.NHD:
            raise ValueError("only NHD KV-cache layout is supported")


def placement_fragment_id(
    participant_id: ParticipantId,
    global_layer_id: int,
    component: KVCacheComponent,
    *,
    head_start: int,
    head_count: int,
) -> PlacementFragmentId:
    """Derive stable logical buffer identity from canonical placement facts."""

    require_nonempty_string(participant_id, "participant_id")
    require_integer(global_layer_id, "global_layer_id")
    require_integer(head_start, "head_start")
    require_integer(head_count, "head_count", minimum=1)
    if not isinstance(component, KVCacheComponent):
        raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004
    content = {
        "schema": "kv-cache-placement-fragment",
        "participant_id": participant_id,
        "global_layer_id": global_layer_id,
        "component": component.value,
        "head_start": head_start,
        "head_count": head_count,
    }
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    return PlacementFragmentId(f"sha256:{hashlib.sha256(encoded).hexdigest()}")


def canonical_strides_bytes(
    shape: tuple[int, ...], itemsize: int
) -> tuple[int, ...]:
    strides: list[int] = []
    running = itemsize
    for extent in reversed(shape):
        strides.append(running)
        running *= extent
    return tuple(reversed(strides))


def require_nonempty_string(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def require_integer(
    value: object, name: str, *, minimum: int = 0
) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if value > _MAX_U64:
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")
    return value


def require_integer_tuple(
    values: object,
    name: str,
    *,
    minimum: int = 0,
) -> tuple[int, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(
        values, Sequence
    ):
        raise ValueError(f"{name} must contain integers")  # noqa: TRY004
    items = cast(Sequence[object], values)
    return tuple(
        require_integer(value, f"{name}[{index}]", minimum=minimum)
        for index, value in enumerate(items)
    )


def require_manifest_items(
    value: object, name: str, item_type: type[_T]
) -> tuple[_T, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence")  # noqa: TRY004
    items = tuple(cast(Sequence[object], value))
    if not all(isinstance(item, item_type) for item in items):
        raise ValueError(f"{name} must contain {item_type.__name__}")
    return tuple(cast(_T, item) for item in items)


def require_sha256(value: object, name: str) -> str:
    digest = require_nonempty_string(value, name)
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return digest


def descriptor_identity(descriptor: KVCacheDescriptor) -> dict[str, object]:
    value = asdict(descriptor)
    value["layout"] = descriptor.layout.value
    return value


__all__ = [
    "KVCacheComponent",
    "KVCacheDescriptor",
    "KVCacheLayout",
    "KVCacheRank",
    "KVCacheRuntimeBuffer",
    "placement_fragment_id",
]
