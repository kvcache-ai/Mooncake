"""Resource-neutral physical runtime fragments."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Optional, cast

from .ids import PlacementFragmentId, RuntimeFragmentId


_MAX_U64 = (1 << 64) - 1


@dataclass(frozen=True)
class RuntimeBindingFragment:
    """Contiguous physical view bound to one logical placement fragment."""

    placement_fragment_id: PlacementFragmentId
    fragment_id: RuntimeFragmentId
    address: int
    nbytes: int
    worker_id: str
    endpoint: str
    device: str
    itemsize: int
    local_shape: tuple[int, ...]
    strides_bytes: tuple[int, ...]
    storage_address: int
    storage_nbytes: int
    storage_offset_bytes: int
    owner: Optional[object] = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        for name in (
            "placement_fragment_id",
            "fragment_id",
            "worker_id",
            "endpoint",
            "device",
        ):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"{name} must be a non-empty string")
        _require_address_range(self.address, self.nbytes)
        _require_integer(self.itemsize, "runtime itemsize", minimum=1)
        local_shape = _require_integer_tuple(
            self.local_shape,
            "runtime local_shape",
            minimum=1,
        )
        strides_bytes = _require_integer_tuple(
            self.strides_bytes,
            "runtime strides_bytes",
            minimum=0,
        )
        if len(strides_bytes) != len(local_shape):
            raise ValueError("runtime stride rank differs from local_shape")
        if any(
            extent > 1 and stride == 0
            for extent, stride in zip(local_shape, strides_bytes)
        ):
            raise ValueError(
                "runtime stride must be positive for non-singleton dimensions"
            )
        strides_bytes = _normalize_singleton_strides_bytes(
            local_shape,
            strides_bytes,
            self.itemsize,
        )
        object.__setattr__(self, "local_shape", local_shape)
        object.__setattr__(self, "strides_bytes", strides_bytes)
        _require_address_range(self.storage_address, self.storage_nbytes)
        _require_u64(self.storage_offset_bytes, "storage_offset_bytes")
        if self.storage_offset_bytes > _MAX_U64 - self.storage_address:
            raise ValueError("normalized runtime address must fit in 64 bits")
        if self.address != self.storage_address + self.storage_offset_bytes:
            raise ValueError(
                "runtime address must equal storage_address plus storage_offset_bytes"
            )
        if self.storage_offset_bytes > self.storage_nbytes - self.nbytes:
            raise ValueError("runtime view exceeds storage allocation bounds")

    def __reduce__(self):
        """Keep framework-owned allocation references out of process wire state.

        ``owner`` is intentionally process-local. A receiving runtime must obtain
        a fresh binding under its framework allocation guard before it can submit
        Store or TE I/O; serializing an arbitrary Python owner would weaken that
        boundary and is not reliable across processes.
        """

        return (
            type(self),
            (
                self.placement_fragment_id,
                self.fragment_id,
                self.address,
                self.nbytes,
                self.worker_id,
                self.endpoint,
                self.device,
                self.itemsize,
                self.local_shape,
                self.strides_bytes,
                self.storage_address,
                self.storage_nbytes,
                self.storage_offset_bytes,
            ),
        )


def _require_integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def _canonical_strides_bytes(
    shape: tuple[int, ...],
    itemsize: int,
) -> tuple[int, ...]:
    strides: list[int] = []
    running = itemsize
    for extent in reversed(shape):
        strides.append(running)
        running *= extent
    return tuple(reversed(strides))


def _normalize_singleton_strides_bytes(
    shape: tuple[int, ...],
    strides_bytes: tuple[int, ...],
    itemsize: int,
) -> tuple[int, ...]:
    canonical = _canonical_strides_bytes(shape, itemsize)
    return tuple(
        expected if extent == 1 else observed
        for extent, observed, expected in zip(shape, strides_bytes, canonical)
    )


def _require_integer_tuple(
    values: object,
    name: str,
    *,
    minimum: int,
) -> tuple[int, ...]:
    if type(values) not in (tuple, list):
        raise ValueError(f"{name} must be a tuple or list")
    items = cast(Sequence[object], values)
    return tuple(
        _require_integer(value, f"{name}[{index}]", minimum=minimum)
        for index, value in enumerate(items)
    )


def _require_u64(value: object, name: str, *, minimum: int = 0) -> int:
    integer = _require_integer(value, name, minimum=minimum)
    if integer > _MAX_U64:
        raise ValueError(f"{name} must fit in an unsigned 64-bit integer")
    return integer


def _require_address_range(address: object, nbytes: object) -> None:
    normalized_address = _require_u64(address, "address", minimum=1)
    normalized_nbytes = _require_u64(nbytes, "nbytes", minimum=1)
    if normalized_nbytes > _MAX_U64 - normalized_address:
        raise ValueError("address range must fit in an unsigned 64-bit integer")
