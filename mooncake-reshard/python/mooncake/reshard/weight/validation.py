"""Cross-fragment logical and physical validation."""

from __future__ import annotations

from math import prod
from typing import Mapping, Sequence

from .types import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    _canonical_tensor_descriptor,
    _parallel_tensor_owner,
)


def _validate_fragments(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
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
    _validate_logical_fragment_overlaps(fragments)


def _validate_complete_weight_placement(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
    *,
    selected_dp_ranks: frozenset[int],
) -> None:
    """Require every selected DP replica to contain one complete logical model."""

    if not tensors:
        raise ValueError("global weight placement must contain tensors")
    if not fragments:
        raise ValueError("global weight placement must contain fragments")

    by_dp_and_tensor: dict[int, dict[str, list[PlacementFragment]]] = {}
    for fragment in fragments:
        by_dp_and_tensor.setdefault(fragment.rank.dp, {}).setdefault(
            fragment.tensor_id, []
        ).append(fragment)

    if not selected_dp_ranks:
        raise ValueError("global weight placement must select a DP replica")

    for dp_rank in sorted(selected_dp_ranks):
        by_tensor = by_dp_and_tensor.get(dp_rank, {})
        for tensor in tensors:
            tensor_fragments = by_tensor.get(tensor.tensor_id, [])
            fragments_by_owner: dict[
                tuple[int, int | None], list[PlacementFragment]
            ] = {}
            for fragment in tensor_fragments:
                owner = _parallel_tensor_owner(tensor, fragment.rank)
                fragments_by_owner.setdefault(owner, []).append(fragment)
            if not fragments_by_owner or any(
                not _fragments_exactly_cover_tensor(tensor, owner_fragments)
                for owner_fragments in fragments_by_owner.values()
            ):
                raise ValueError(
                    "global placement tensor is not fully covered: "
                    f"{tensor.tensor_id}: dp={dp_rank}"
                )


def _fragments_exactly_cover_tensor(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
) -> bool:
    boxes = tuple(
        dict.fromkeys(
            (fragment.global_offset, fragment.local_shape) for fragment in fragments
        )
    )
    if not boxes:
        return False
    if sum(prod(shape) for _, shape in boxes) != prod(tensor.global_shape):
        return False
    if any(
        any(
            offset < 0 or offset + extent > total
            for offset, extent, total in zip(
                box_offset,
                box_shape,
                tensor.global_shape,
            )
        )
        for box_offset, box_shape in boxes
    ):
        return False
    return not _boxes_overlap(boxes)


def _boxes_overlap(
    boxes: Sequence[tuple[tuple[int, ...], tuple[int, ...]]],
) -> bool:
    if len(boxes) < 2:
        return False
    ndim = len(boxes[0][0])
    sweep_dim = max(
        range(ndim),
        key=lambda dim: len(
            {(offset[dim], offset[dim] + shape[dim]) for offset, shape in boxes}
        ),
    )
    ordered = sorted(boxes, key=lambda item: item[0][sweep_dim])
    active: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
    for offset, shape in ordered:
        begin = offset[sweep_dim]
        active = [
            candidate
            for candidate in active
            if candidate[0][sweep_dim] + candidate[1][sweep_dim] > begin
        ]
        if any(
            all(
                left_begin < right_begin + right_extent
                and right_begin < left_begin + left_extent
                for left_begin, left_extent, right_begin, right_extent in zip(
                    candidate_offset,
                    candidate_shape,
                    offset,
                    shape,
                )
            )
            for candidate_offset, candidate_shape in active
        ):
            return True
        active.append((offset, shape))
    return False


def _validate_logical_fragment_overlaps(
    fragments: Sequence[PlacementFragment],
) -> None:
    by_tensor_and_rank: dict[tuple[str, ParallelRank], list[PlacementFragment]] = {}
    for fragment in fragments:
        by_tensor_and_rank.setdefault((fragment.tensor_id, fragment.rank), []).append(
            fragment
        )

    for owner_fragments in by_tensor_and_rank.values():
        if len(owner_fragments) < 2:
            continue
        ndim = len(owner_fragments[0].global_offset)
        sweep_dim = max(
            range(ndim),
            key=lambda dim: len(
                {
                    (
                        fragment.global_offset[dim],
                        fragment.global_offset[dim] + fragment.local_shape[dim],
                    )
                    for fragment in owner_fragments
                }
            ),
        )
        ordered = sorted(
            owner_fragments,
            key=lambda fragment: fragment.global_offset[sweep_dim],
        )
        active: list[PlacementFragment] = []
        for current in ordered:
            current_begin = current.global_offset[sweep_dim]
            active = [
                previous
                for previous in active
                if previous.global_offset[sweep_dim] + previous.local_shape[sweep_dim]
                > current_begin
            ]
            for previous in active:
                if all(
                    previous_offset < current_offset + current_extent
                    and current_offset < previous_offset + previous_extent
                    for (
                        previous_offset,
                        previous_extent,
                        current_offset,
                        current_extent,
                    ) in zip(
                        previous.global_offset,
                        previous.local_shape,
                        current.global_offset,
                        current.local_shape,
                    )
                ):
                    raise ValueError(
                        "logical fragment boxes overlap for tensor and "
                        "parallel rank: "
                        f"{previous.fragment_id} and {current.fragment_id}"
                    )
            active.append(current)


def _validate_fragment_geometry(
    tensor: TensorDescriptor,
    fragment: PlacementFragment,
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
        tensor.parallel_axes,
        tensor.layer_id,
        tensor.expert_id,
        tensor.layout_fingerprint,
    )


def _is_exact_declared_runtime_alias(
    left_placement: PlacementFragment,
    left_binding: RuntimeBindingFragment,
    right_placement: PlacementFragment,
    right_binding: RuntimeBindingFragment,
    tensors: Mapping[str, TensorDescriptor],
) -> bool:
    return (
        left_binding.address == right_binding.address
        and left_binding.nbytes == right_binding.nbytes
        and len(left_placement.aliases) >= 2
        and left_placement.aliases == right_placement.aliases
        and left_placement.tensor_id in left_placement.aliases
        and right_placement.tensor_id in left_placement.aliases
        and left_placement.global_offset == right_placement.global_offset
        and left_placement.local_shape == right_placement.local_shape
        and _runtime_alias_descriptor_key(tensors[left_placement.tensor_id])
        == _runtime_alias_descriptor_key(tensors[right_placement.tensor_id])
    )


def _validate_runtime_binding_address_ranges(
    *,
    instance_id: str,
    tensors: Sequence[TensorDescriptor],
    placements: Sequence[PlacementFragment],
    bindings: Sequence[RuntimeBindingFragment],
) -> None:
    tensor_by_id = {tensor.tensor_id: tensor for tensor in tensors}
    placement_by_id = {
        fragment.placement_fragment_id: fragment for fragment in placements
    }
    by_address_space: dict[
        tuple[str, str, str],
        list[tuple[PlacementFragment, RuntimeBindingFragment]],
    ] = {}
    for binding in bindings:
        placement = placement_by_id[binding.placement_fragment_id]
        address_space = (instance_id, binding.worker_id, binding.device)
        by_address_space.setdefault(address_space, []).append((placement, binding))

    for address_space, fragments in by_address_space.items():
        ordered = sorted(fragments, key=lambda item: item[1].address)
        active: list[tuple[PlacementFragment, RuntimeBindingFragment]] = []
        for current_placement, current_binding in ordered:
            active = [
                (previous_placement, previous_binding)
                for previous_placement, previous_binding in active
                if previous_binding.address + previous_binding.nbytes
                > current_binding.address
            ]
            for previous_placement, previous_binding in active:
                if _is_exact_declared_runtime_alias(
                    previous_placement,
                    previous_binding,
                    current_placement,
                    current_binding,
                    tensor_by_id,
                ):
                    continue
                raise ValueError(
                    "runtime binding address ranges overlap: "
                    f"{previous_binding.fragment_id} and "
                    f"{current_binding.fragment_id} "
                    f"in {address_space}"
                )
            active.append((current_placement, current_binding))
