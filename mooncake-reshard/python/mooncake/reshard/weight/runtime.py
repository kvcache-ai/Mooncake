"""Runtime inventory adapters and ephemeral binding contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..contracts import (
    ResourceKind,
    RuntimeBindingManifest,
)
from .types import (
    RuntimeBindingFragment,
    _read_field,
    _read_optional_field,
    _read_weight_resource_id,
    _require_integer,
    _require_integer_tuple,
    _require_manifest_items,
    _require_nonempty_string,
    _require_sequence,
    _require_u64,
)


@dataclass(frozen=True)
class WeightRuntimeBindingManifest(RuntimeBindingManifest):
    """Ephemeral physical locations and lifetime fence for one placement."""

    revision: str
    participant_id: str
    fragments: tuple[RuntimeBindingFragment, ...]

    @property
    def resource_kind(self) -> ResourceKind:
        """Identify this binding as model weight data."""

        return ResourceKind.MODEL_WEIGHT

    @property
    def model_id(self) -> str:
        """Return the weight-specific name for the common resource ID."""

        return self.resource_id

    def __post_init__(self) -> None:
        super().__post_init__()
        fragments = _require_manifest_items(
            self.fragments,
            "WeightRuntimeBindingManifest fragments",
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
        _require_nonempty_string(self.revision, "revision")
        _require_nonempty_string(self.participant_id, "participant_id")
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
    ) -> WeightRuntimeBindingManifest:
        """Import framework runtime locations without importing the framework.

        Each record must carry shape and stride evidence. ``address`` is the
        normalized first byte of the view and is checked against the storage
        base plus the normalized byte offset.
        """

        generation = _read_field(inventory, "generation")
        _require_u64(generation, "generation")
        return cls(
            resource_id=_read_weight_resource_id(inventory),
            revision=_read_field(inventory, "revision"),
            placement_id=_read_field(inventory, "placement_id"),
            placement_digest=_read_field(inventory, "placement_digest"),
            instance_id=_read_field(inventory, "instance_id"),
            participant_id=_read_field(inventory, "participant_id"),
            generation=generation,
            lease_id=_read_field(inventory, "lease_id"),
            fragments=tuple(
                _runtime_binding_fragment_from_record(
                    record,
                    owner_resolver,
                    generation,
                )
                for record in _require_sequence(
                    _read_field(inventory, "fragments"),
                    "runtime binding fragments",
                )
            ),
        )


def _runtime_binding_fragment_from_record(
    record: Any,
    owner_resolver: Optional[Callable[[Any], Any]],
    expected_generation: int,
) -> RuntimeBindingFragment:
    is_contiguous = _read_optional_field(record, "is_contiguous")
    if is_contiguous is not None and (
        type(is_contiguous) is not bool or not is_contiguous
    ):
        raise ValueError("runtime binding allocation must be contiguous")
    fragment_generation = _read_optional_field(record, "lease_generation")
    if fragment_generation is not None:
        _require_u64(fragment_generation, "lease_generation")
        if fragment_generation != expected_generation:
            raise ValueError("runtime binding fragment lease generation mismatch")
    local_shape = _require_integer_tuple(
        _read_field(record, "local_shape"),
        "runtime local_shape",
        minimum=1,
    )
    itemsize = _read_field(record, "itemsize")
    _require_integer(itemsize, "runtime itemsize", minimum=1)
    strides_bytes = _runtime_strides_bytes(
        record,
        itemsize=itemsize,
        ndim=len(local_shape),
    )
    storage_offset_bytes = _runtime_storage_offset_bytes(
        record,
        itemsize=itemsize,
    )
    address = _read_field(record, "address")
    storage_address = _read_optional_field(record, "storage_address")
    if storage_address is None:
        if storage_offset_bytes != 0:
            raise ValueError(
                "non-zero runtime offset requires storage_address evidence"
            )
        storage_address = address
    storage_nbytes = _read_optional_field(record, "storage_nbytes")
    if storage_nbytes is None:
        if storage_offset_bytes != 0:
            raise ValueError("non-zero runtime offset requires storage_nbytes evidence")
        storage_nbytes = _read_field(record, "nbytes")
    return RuntimeBindingFragment(
        placement_fragment_id=_read_field(record, "placement_fragment_id"),
        fragment_id=_read_field(record, "fragment_id"),
        address=address,
        nbytes=_read_field(record, "nbytes"),
        worker_id=_read_field(record, "worker_id"),
        endpoint=_read_field(record, "endpoint"),
        device=_read_field(record, "device"),
        itemsize=itemsize,
        local_shape=local_shape,
        strides_bytes=strides_bytes,
        storage_address=storage_address,
        storage_nbytes=storage_nbytes,
        storage_offset_bytes=storage_offset_bytes,
        owner=(owner_resolver(record) if owner_resolver is not None else None),
    )


def _runtime_strides_bytes(
    record: Any,
    *,
    itemsize: int,
    ndim: int,
) -> tuple[int, ...]:
    stride = _read_optional_field(record, "stride")
    strides_bytes = _read_optional_field(record, "strides_bytes")
    if stride is None and strides_bytes is None:
        raise ValueError("runtime binding requires stride evidence")

    normalized_from_elements = None
    if stride is not None:
        element_strides = _require_integer_tuple(
            stride,
            "runtime stride",
            minimum=1,
        )
        normalized_from_elements = tuple(value * itemsize for value in element_strides)
    normalized_bytes = None
    if strides_bytes is not None:
        normalized_bytes = _require_integer_tuple(
            strides_bytes,
            "runtime strides_bytes",
            minimum=1,
        )
    if (
        normalized_from_elements is not None
        and normalized_bytes is not None
        and normalized_from_elements != normalized_bytes
    ):
        raise ValueError("runtime stride and strides_bytes disagree")
    result = (
        normalized_bytes if normalized_bytes is not None else normalized_from_elements
    )
    if result is None or len(result) != ndim:
        raise ValueError("runtime stride rank differs from local_shape")
    return result


def _runtime_storage_offset_bytes(record: Any, *, itemsize: int) -> int:
    storage_offset = _read_optional_field(record, "storage_offset")
    byte_offset = _read_optional_field(record, "byte_offset")
    normalized_from_elements = None
    if storage_offset is not None:
        _require_integer(storage_offset, "storage_offset", minimum=0)
        normalized_from_elements = storage_offset * itemsize
    if byte_offset is not None:
        _require_integer(byte_offset, "byte_offset", minimum=0)
    if (
        normalized_from_elements is not None
        and byte_offset is not None
        and normalized_from_elements != byte_offset
    ):
        raise ValueError("storage_offset and byte_offset disagree")
    result = byte_offset if byte_offset is not None else normalized_from_elements
    if result is None:
        return 0
    _require_u64(result, "storage_offset_bytes")
    return result
