from __future__ import annotations

from typing import Sequence, Union

from ..manifest import (
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    validate_runtime_bindings,
    validate_runtime_binding,
)
from ..planner import BoundWeightFragment, RuntimeFragmentSnapshot
from .errors import WeightStoreError


def same_runtime_snapshot(
    current: RuntimeBindingFragment,
    planned: Union[RuntimeBindingFragment, RuntimeFragmentSnapshot],
) -> bool:
    if isinstance(planned, RuntimeBindingFragment):
        return current == planned
    return (
        current.placement_fragment_id == planned.placement_fragment_id
        and current.fragment_id == planned.fragment_id
        and current.address == planned.address
        and current.nbytes == planned.nbytes
        and current.worker_id == planned.worker_id
        and current.endpoint == planned.endpoint
        and current.device == planned.device
        and current.itemsize == planned.itemsize
        and current.local_shape == planned.local_shape
        and current.strides_bytes == planned.strides_bytes
        and current.storage_address == planned.storage_address
        and current.storage_nbytes == planned.storage_nbytes
        and current.storage_offset_bytes == planned.storage_offset_bytes
    )


def validate_manifest_pair(
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    label: str,
) -> None:
    try:
        validate_runtime_binding(placement, binding)
    except ValueError as error:
        raise WeightStoreError(f"invalid {label} runtime binding: {error}") from error


def validate_manifest_set(
    placement: WeightPlacementManifest,
    bindings: Sequence[WeightRuntimeBindingManifest],
    label: str,
) -> None:
    try:
        validate_runtime_bindings(placement, bindings)
    except (TypeError, ValueError) as error:
        raise WeightStoreError(f"invalid {label} runtime bindings: {error}") from error


def pair_manifests(
    placement: WeightPlacementManifest,
    bindings: Sequence[WeightRuntimeBindingManifest],
    label: str,
) -> tuple[tuple[WeightPlacementManifest, WeightRuntimeBindingManifest], ...]:
    if not isinstance(placement, WeightPlacementManifest):
        raise ValueError(f"{label} placement must be a WeightPlacementManifest")
    items = tuple(bindings)
    if not items:
        raise WeightStoreError(f"{label} runtime bindings must not be empty")
    if not all(isinstance(item, WeightRuntimeBindingManifest) for item in items):
        raise WeightStoreError(
            f"{label} runtime bindings must contain WeightRuntimeBindingManifest"
        )
    participant_ids = [item.participant_id for item in items]
    if len(participant_ids) != len(set(participant_ids)):
        raise WeightStoreError(f"duplicate {label} runtime binding participant")
    for binding in items:
        validate_manifest_pair(placement, binding, label)
    return tuple((placement, binding) for binding in items)


def runtime_binding_fragment(
    fragment: Union[RuntimeBindingFragment, BoundWeightFragment],
) -> RuntimeBindingFragment:
    if isinstance(fragment, RuntimeBindingFragment):
        return fragment
    if isinstance(fragment, BoundWeightFragment):
        return fragment.binding
    raise WeightStoreError(
        "transfer plan physical fragment must be a RuntimeBindingFragment "
        "or expose one as .binding"
    )
