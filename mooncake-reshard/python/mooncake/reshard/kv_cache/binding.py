"""Validate live KV-cache buffers against a complete global placement."""

from __future__ import annotations

from collections.abc import Iterator
from heapq import merge
from itertools import pairwise

from ..contracts import RuntimeInstanceId
from .placement import KVCachePlacementManifest
from .runtime import KVCacheRuntimeBindingManifest
from .snapshot import KVCacheSnapshotDescriptor
from .types import (
    KVCacheLayout,
    KVCacheRuntimeBuffer,
    canonical_strides_bytes,
    placement_fragment_id,
    require_manifest_items,
)

# Charge candidate pairs and mixed-stride payload intervals before expansion.
_MAX_OVERLAP_VALIDATION_WORK = 1_000_000


def validate_runtime_binding(
    placement: KVCachePlacementManifest,
    binding: KVCacheRuntimeBindingManifest,
    *,
    snapshot: KVCacheSnapshotDescriptor | None = None,
) -> None:
    if not isinstance(placement, KVCachePlacementManifest):
        raise TypeError("placement must be a KVCachePlacementManifest")
    if not isinstance(binding, KVCacheRuntimeBindingManifest):
        raise TypeError("binding must be a KVCacheRuntimeBindingManifest")
    if placement.resource_kind != binding.resource_kind:
        raise ValueError("placement and runtime binding resource_kind differ")
    if placement.resource_id != binding.resource_id:
        raise ValueError("placement and runtime binding resource_id differ")
    if placement.placement_id != binding.placement_id:
        raise ValueError("placement_id and runtime binding placement_id differ")
    if placement.digest != binding.placement_digest:
        raise ValueError("placement digest and runtime binding placement digest differ")
    if snapshot is not None:
        if not isinstance(snapshot, KVCacheSnapshotDescriptor):
            raise TypeError("snapshot must be a KVCacheSnapshotDescriptor")
        if binding.snapshot_id != snapshot.snapshot_id:
            raise ValueError("runtime binding snapshot_id differs")
        if binding.snapshot_digest != snapshot.digest:
            raise ValueError("runtime binding snapshot digest differs")
    if placement.revision != binding.revision:
        raise ValueError("placement and runtime binding revision differ")
    part = placement.part(binding.participant_id)
    expected_keys = {
        (layer_id, component)
        for layer_id in part.layer_ids
        for component in placement.descriptor.components
    }
    actual_keys = {(item.global_layer_id, item.component) for item in binding.buffers}
    if expected_keys != actual_keys:
        missing = sorted(
            (layer, component.value) for layer, component in expected_keys - actual_keys
        )
        unknown = sorted(
            (layer, component.value) for layer, component in actual_keys - expected_keys
        )
        detail = missing[0] if missing else unknown[0]
        raise ValueError(f"runtime binding buffer membership differs: {detail}")

    descriptor = placement.descriptor
    for item in binding.buffers:
        fragment = item.fragment
        expected_fragment_id = placement_fragment_id(
            part.participant_id,
            item.global_layer_id,
            item.component,
            head_start=part.head_start,
            head_count=part.head_count,
        )
        if fragment.placement_fragment_id != expected_fragment_id:
            raise ValueError("runtime binding placement_fragment_id differs")
        if fragment.itemsize != descriptor.itemsize:
            raise ValueError("runtime binding itemsize differs from placement")
        if len(fragment.local_shape) != 3:
            raise ValueError("NHD runtime buffer must have rank three")
        expected_dim = descriptor.head_dim(item.component)
        if fragment.local_shape[1:] != (part.head_count, expected_dim):
            raise ValueError("runtime binding local_shape differs from placement")
        expected_strides = canonical_strides_bytes(
            fragment.local_shape, fragment.itemsize
        )
        if descriptor.layout is KVCacheLayout.NHD:
            if fragment.strides_bytes != expected_strides:
                raise ValueError("runtime binding is not a contiguous NHD row layout")
        elif (
            fragment.strides_bytes[1:] != expected_strides[1:]
            or fragment.strides_bytes[0] < expected_strides[0]
            or fragment.strides_bytes[0] % fragment.itemsize
        ):
            raise ValueError(
                "runtime binding must contain disjoint contiguous NHD rows"
            )
        expected_nbytes = (fragment.local_shape[0] - 1) * fragment.strides_bytes[
            0
        ] + expected_strides[0]
        if fragment.nbytes != expected_nbytes:
            raise ValueError("runtime binding nbytes differs from local_shape")
    _validate_address_ranges(binding)


def validate_runtime_bindings(
    placement: KVCachePlacementManifest,
    bindings: object,
    *,
    snapshot: KVCacheSnapshotDescriptor | None = None,
) -> None:
    items = require_manifest_items(
        bindings,
        "runtime bindings",
        KVCacheRuntimeBindingManifest,
    )
    participant_ids = [item.participant_id for item in items]
    if len(participant_ids) != len(set(participant_ids)):
        raise ValueError("duplicate runtime binding participant")
    for binding in items:
        validate_runtime_binding(placement, binding, snapshot=snapshot)
    expected = {part.participant_id for part in placement.parts if part.layer_ids}
    actual = set(participant_ids)
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"missing runtime binding participant: {missing[0]}")
    unknown = sorted(actual - expected)
    if unknown:
        raise ValueError(f"unknown runtime binding participant: {unknown[0]}")
    _validate_shared_instance_ranges(items)


def _validate_address_ranges(binding: KVCacheRuntimeBindingManifest) -> None:
    _validate_shared_instance_ranges((binding,))


def _payload_ranges(fragment: KVCacheRuntimeBuffer) -> Iterator[tuple[int, int]]:
    # Geometry was validated above: heads/dimensions form a contiguous row.
    row_bytes = fragment.local_shape[1] * fragment.local_shape[2] * fragment.itemsize
    row_stride = fragment.strides_bytes[0]
    if row_stride == row_bytes:
        yield fragment.address, fragment.address + fragment.nbytes
        return
    for row in range(fragment.local_shape[0]):
        start = fragment.address + row * row_stride
        yield start, start + row_bytes


def _view_payloads_overlap(
    left: KVCacheRuntimeBuffer, right: KVCacheRuntimeBuffer
) -> bool:
    # The caller orders views by address and checks their bounding spans first.
    if left.strides_bytes[0] == right.strides_bytes[0]:
        stride = left.strides_bytes[0]
        row, offset = divmod(right.address - left.address, stride)
        left_width = left.local_shape[1] * left.local_shape[2] * left.itemsize
        right_width = right.local_shape[1] * right.local_shape[2] * right.itemsize
        # Later right-hand rows have the same phase, moving only past the left view.
        return (row < left.local_shape[0] and offset < left_width) or (
            row + 1 < left.local_shape[0] and offset + right_width > stride
        )
    payloads = merge(_payload_ranges(left), _payload_ranges(right))
    return any(current[0] < previous[1] for previous, current in pairwise(payloads))


def _validate_shared_instance_ranges(
    bindings: tuple[KVCacheRuntimeBindingManifest, ...],
) -> None:
    # Virtual addresses are comparable only within the declared address space.
    by_space: dict[tuple[RuntimeInstanceId, str, str], list[KVCacheRuntimeBuffer]] = {}
    for binding in bindings:
        for item in binding.buffers:
            fragment = item.fragment
            key = (binding.instance_id, fragment.worker_id, fragment.device)
            by_space.setdefault(key, []).append(fragment)
    work = 0
    for fragments in by_space.values():
        fragments.sort(key=lambda f: f.address)
        for index, left in enumerate(fragments):
            for other in range(index + 1, len(fragments)):
                right = fragments[other]
                if right.address >= left.address + left.nbytes:
                    break
                work += 1
                if left.strides_bytes[0] != right.strides_bytes[0]:
                    work += sum(
                        1
                        if f.strides_bytes[0]
                        == f.local_shape[1] * f.local_shape[2] * f.itemsize
                        else f.local_shape[0]
                        for f in (left, right)
                    )
                if work > _MAX_OVERLAP_VALIDATION_WORK:
                    raise ValueError("runtime overlap validation work limit exceeded")
                if _view_payloads_overlap(left, right):
                    raise ValueError("runtime KV buffers overlap in one address space")
        # Several disjoint views may share the same complete allocation. Distinct
        # allocation claims may not overlap, including unequal nested claims.
        allocations = sorted(
            {
                (
                    fragment.storage_address,
                    fragment.storage_address + fragment.storage_nbytes,
                )
                for fragment in fragments
            }
        )
        for previous, current in pairwise(allocations):
            if current[0] < previous[1]:
                raise ValueError(
                    "runtime KV backing allocations overlap in one address space"
                )


__all__ = ["validate_runtime_binding", "validate_runtime_bindings"]
