"""Validation between logical weight placement and runtime locations."""

from __future__ import annotations

from ..contracts import validate_resource_binding_identity
from .placement import WeightPlacementManifest
from .runtime import WeightRuntimeBindingManifest
from .types import _canonical_stride, _require_sequence
from .validation import _validate_runtime_binding_address_ranges


def validate_runtime_binding(
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
) -> None:
    """Validate an ephemeral binding against one exact logical placement."""

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
        expected_strides_bytes = tuple(
            stride * tensor.itemsize
            for stride in _canonical_stride(placement_fragment.local_shape)
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
    bindings: tuple[WeightRuntimeBindingManifest, ...],
) -> tuple[WeightRuntimeBindingManifest, ...]:
    """Validate one or more participants and their shared address spaces."""

    items = tuple(_require_sequence(bindings, "runtime bindings"))
    if not all(isinstance(item, WeightRuntimeBindingManifest) for item in items):
        raise ValueError("runtime bindings must contain WeightRuntimeBindingManifest")
    participant_ids = [item.participant_id for item in items]
    if len(participant_ids) != len(set(participant_ids)):
        raise ValueError("duplicate runtime binding participant")
    for binding in items:
        validate_runtime_binding(placement, binding)

    part_by_participant = {part.participant_id: part for part in placement.parts}
    by_instance: dict[str, list[WeightRuntimeBindingManifest]] = {}
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
    bindings: tuple[WeightRuntimeBindingManifest, ...],
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
