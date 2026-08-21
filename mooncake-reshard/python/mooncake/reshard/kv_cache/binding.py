"""Validate live KV-cache buffers against a complete global placement."""

from __future__ import annotations

from itertools import pairwise

from ..contracts import RuntimeInstanceId, validate_resource_binding_identity
from .placement import KVCachePlacementManifest
from .runtime import KVCacheRuntimeBindingManifest
from .types import (
    KVCacheComponent,
    canonical_strides_bytes,
    placement_fragment_id,
    require_manifest_items,
)


def validate_runtime_binding(
    placement: KVCachePlacementManifest,
    binding: KVCacheRuntimeBindingManifest,
) -> None:
    if not isinstance(placement, KVCachePlacementManifest):
        raise TypeError("placement must be a KVCachePlacementManifest")
    if not isinstance(binding, KVCacheRuntimeBindingManifest):
        raise TypeError("binding must be a KVCacheRuntimeBindingManifest")
    validate_resource_binding_identity(placement, binding)
    if placement.revision != binding.revision:
        raise ValueError("placement and runtime binding revision differ")
    part = placement.part(binding.participant_id)
    expected_keys = {
        (layer_id, component)
        for layer_id in part.layer_ids
        for component in KVCacheComponent
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
        expected_dim = (
            descriptor.key_head_dim
            if item.component is KVCacheComponent.KEY
            else descriptor.value_head_dim
        )
        if fragment.local_shape[1:] != (part.head_count, expected_dim):
            raise ValueError("runtime binding local_shape differs from placement")
        expected_nbytes = (
            fragment.local_shape[0]
            * fragment.local_shape[1]
            * fragment.local_shape[2]
            * fragment.itemsize
        )
        if fragment.nbytes != expected_nbytes:
            raise ValueError("runtime binding nbytes differs from local_shape")
        expected_strides = canonical_strides_bytes(
            fragment.local_shape, fragment.itemsize
        )
        if fragment.strides_bytes != expected_strides:
            raise ValueError("runtime binding is not a contiguous NHD row layout")
    _validate_address_ranges(binding)


def validate_runtime_bindings(
    placement: KVCachePlacementManifest,
    bindings: object,
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
        validate_runtime_binding(placement, binding)
    expected = {part.participant_id for part in placement.parts if part.layer_ids}
    actual = set(participant_ids)
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"missing runtime binding participant: {missing[0]}")
    unknown = sorted(actual - expected)
    if unknown:
        raise ValueError(f"unknown runtime binding participant: {unknown[0]}")
    by_instance: dict[RuntimeInstanceId, list[KVCacheRuntimeBindingManifest]] = {}
    for binding in items:
        by_instance.setdefault(binding.instance_id, []).append(binding)
    for instance_bindings in by_instance.values():
        _validate_shared_instance_ranges(tuple(instance_bindings))


def _validate_address_ranges(binding: KVCacheRuntimeBindingManifest) -> None:
    ranges = sorted(
        (
            item.fragment.address,
            item.fragment.address + item.fragment.nbytes,
            item.fragment.fragment_id,
        )
        for item in binding.buffers
    )
    for previous, current in pairwise(ranges):
        if current[0] < previous[1]:
            raise ValueError(
                "runtime KV buffers overlap within one participant: "
                f"{previous[2]} and {current[2]}"
            )


def _validate_shared_instance_ranges(
    bindings: tuple[KVCacheRuntimeBindingManifest, ...],
) -> None:
    ranges = sorted(
        (
            item.fragment.address,
            item.fragment.address + item.fragment.nbytes,
            item.fragment.fragment_id,
        )
        for binding in bindings
        for item in binding.buffers
    )
    for previous, current in pairwise(ranges):
        if current[0] < previous[1]:
            raise ValueError(
                "runtime KV buffers overlap in one runtime instance: "
                f"{previous[2]} and {current[2]}"
            )


__all__ = ["validate_runtime_binding", "validate_runtime_bindings"]
