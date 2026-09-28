"""Validation between logical weight placement and runtime locations."""

from __future__ import annotations

from ..contracts import (
    RuntimeFragmentId,
    RuntimeInstanceId,
    validate_resource_binding_identity,
)
from .placement import WeightPlacementManifest
from .runtime import WeightRuntimeBindingManifest
from .types import canonical_strides_bytes, require_manifest_items
from .validation import _validate_runtime_binding_address_ranges


def validate_runtime_binding(
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
) -> None:
    """Validate an ephemeral binding against one exact logical placement."""

    if not isinstance(placement, WeightPlacementManifest):
        raise ValueError("placement must be a WeightPlacementManifest")  # noqa: TRY004
    if not isinstance(binding, WeightRuntimeBindingManifest):
        raise ValueError("binding must be a WeightRuntimeBindingManifest")  # noqa: TRY004
    validate_resource_binding_identity(placement, binding)
    if placement.revision != binding.revision:
        raise ValueError("placement and runtime binding revision differ")

    try:
        placement_part = next(
            part
            for part in placement.parts
            if part.participant_id == binding.participant_id
        )
    except StopIteration as error:
        raise ValueError(
            f"unknown runtime binding participant: {binding.participant_id}"
        ) from error

    placement_by_id = {
        fragment.placement_fragment_id: fragment
        for fragment in placement_part.fragments
    }
    binding_by_id = {
        fragment.placement_fragment_id: fragment for fragment in binding.fragments
    }
    tensor_by_id = {tensor.tensor_id: tensor for tensor in placement.tensors}
    unknown = sorted(binding_by_id.keys() - placement_by_id.keys())
    if unknown:
        raise ValueError(f"unknown placement fragment in runtime binding: {unknown[0]}")
    missing = sorted(placement_by_id.keys() - binding_by_id.keys())
    if missing:
        raise ValueError(f"missing placement fragment in runtime binding: {missing[0]}")

    for placement_fragment in placement_part.fragments:
        runtime_fragment = binding_by_id[placement_fragment.placement_fragment_id]
        if runtime_fragment.nbytes != placement_fragment.nbytes:
            raise ValueError(
                "runtime binding byte size does not match placement: "
                f"{placement_fragment.placement_fragment_id}"
            )
        if runtime_fragment.local_shape != placement_fragment.local_shape:
            raise ValueError(
                "runtime binding local_shape does not match placement: "
                f"{placement_fragment.placement_fragment_id}"
            )
        tensor = tensor_by_id[placement_fragment.tensor_id]
        if runtime_fragment.itemsize != tensor.itemsize:
            raise ValueError(
                "runtime binding itemsize does not match placement: "
                f"{placement_fragment.placement_fragment_id}"
            )
        expected_strides_bytes = canonical_strides_bytes(
            placement_fragment.local_shape,
            tensor.itemsize,
        )
        if runtime_fragment.strides_bytes != expected_strides_bytes:
            raise ValueError(
                "runtime binding stride does not describe a canonical "
                "contiguous view: "
                f"{placement_fragment.placement_fragment_id}"
            )

    _validate_runtime_binding_address_ranges(
        instance_id=binding.instance_id,
        tensors=placement.tensors,
        placements=placement_part.fragments,
        bindings=binding.fragments,
    )


def _validate_runtime_binding_subset(
    placement: WeightPlacementManifest,
    bindings: object,
) -> tuple[WeightRuntimeBindingManifest, ...]:
    """Validate one or more participants and their shared address spaces."""

    items = require_manifest_items(
        bindings,
        "runtime bindings",
        WeightRuntimeBindingManifest,
    )
    participant_ids = [item.participant_id for item in items]
    if len(participant_ids) != len(set(participant_ids)):
        raise ValueError("duplicate runtime binding participant")
    runtime_fragment_ids: set[RuntimeFragmentId] = set()
    for binding in items:
        validate_runtime_binding(placement, binding)
        for fragment in binding.fragments:
            if fragment.fragment_id in runtime_fragment_ids:
                raise ValueError(
                    f"duplicate runtime fragment_id across participants: "
                    f"{fragment.fragment_id}"
                )
            runtime_fragment_ids.add(fragment.fragment_id)

    part_by_participant = {part.participant_id: part for part in placement.parts}
    by_instance: dict[RuntimeInstanceId, list[WeightRuntimeBindingManifest]] = {}
    for binding in items:
        by_instance.setdefault(binding.instance_id, []).append(binding)
    for instance_id, instance_bindings in by_instance.items():
        _validate_runtime_binding_address_ranges(
            instance_id=instance_id,
            tensors=placement.tensors,
            placements=tuple(
                fragment
                for binding in instance_bindings
                for fragment in part_by_participant[binding.participant_id].fragments
            ),
            bindings=tuple(
                fragment
                for binding in instance_bindings
                for fragment in binding.fragments
            ),
        )
    return items


def validate_runtime_bindings(
    placement: WeightPlacementManifest,
    bindings: object,
) -> None:
    """Validate the exact runtime binding set for a complete placement."""

    items = _validate_runtime_binding_subset(placement, bindings)
    expected = {part.participant_id for part in placement.parts if part.fragments}
    actual = {item.participant_id for item in items}
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"missing runtime binding participant: {missing[0]}")
    unknown = sorted(actual - expected)
    if unknown:
        raise ValueError(f"unknown runtime binding participant: {unknown[0]}")
