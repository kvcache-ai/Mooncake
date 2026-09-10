"""Cross-fragment logical and physical validation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from itertools import product as cartesian_product

from ..contracts import RuntimeBindingFragment, RuntimeInstanceId, TensorId
from ..geometry import boxes_exactly_cover, boxes_overlap
from .topology import ParallelTopology
from .types import (
    OwnershipAxis,
    ParallelRank,
    PlacementFragment,
    ReplicatedAxis,
    SplitAxis,
    TensorDescriptor,
    validate_fragment_geometry,
)


def _validate_fragments(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
    *,
    require_complete_alias_groups: bool = False,
) -> None:
    tensor_by_id: dict[TensorId, TensorDescriptor] = {}
    for tensor in tensors:
        if tensor.tensor_id in tensor_by_id:
            raise ValueError(f"duplicate tensor_id: {tensor.tensor_id}")
        tensor_by_id[tensor.tensor_id] = tensor

    fragment_ids: set[str] = set()
    logical_fragments: set[
        tuple[TensorId, ParallelRank, tuple[int, ...], tuple[int, ...]]
    ] = set()
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
    if require_complete_alias_groups:
        _validate_complete_alias_groups(tensor_by_id, fragments)
    _validate_logical_fragment_overlaps(fragments)


def _validate_complete_alias_groups(
    tensors_by_id: Mapping[TensorId, TensorDescriptor],
    fragments: Sequence[PlacementFragment],
) -> None:
    """Validate alias authorization only after all placement parts are assembled.

    A placement part can legitimately name an alias member owned by another
    participant. The complete placement is the first layer with the full tensor
    set, so it alone can prove that every declared member exists and declares
    the same group.
    """

    fragments_by_tensor: dict[TensorId, list[PlacementFragment]] = {}
    alias_groups: set[tuple[TensorId, ...]] = set()
    for fragment in fragments:
        fragments_by_tensor.setdefault(fragment.tensor_id, []).append(fragment)
        if fragment.aliases:
            alias_groups.add(fragment.aliases)

    known_tensor_ids = set(tensors_by_id)
    for aliases in alias_groups:
        unknown_tensor_ids = sorted(set(aliases) - known_tensor_ids)
        if unknown_tensor_ids:
            raise ValueError(
                f"alias group references unknown tensor: {unknown_tensor_ids[0]}"
            )
        for tensor_id in aliases:
            member_groups = {
                fragment.aliases for fragment in fragments_by_tensor[tensor_id]
            }
            if member_groups != {aliases}:
                raise ValueError(
                    f"alias group is not declared consistently for tensor: {tensor_id}"
                )


def _validate_complete_weight_placement(
    tensors: Sequence[TensorDescriptor],
    fragments: Sequence[PlacementFragment],
    *,
    topology: ParallelTopology,
) -> None:
    """Validate explicit split, replication, and ownership semantics."""

    if not tensors:
        raise ValueError("global weight placement must contain tensors")
    if not fragments:
        raise ValueError("global weight placement must contain fragments")

    by_tensor: dict[TensorId, list[PlacementFragment]] = {}
    for fragment in fragments:
        by_tensor.setdefault(fragment.tensor_id, []).append(fragment)

    for tensor in tensors:
        tensor_fragments = by_tensor.get(tensor.tensor_id, [])
        if not tensor_fragments:
            raise ValueError(
                f"global placement tensor is not fully covered: {tensor.tensor_id}"
            )
        _validate_tensor_axis_covers(tensor, tensor_fragments, topology=topology)


def _split_axis_kinds(tensor: TensorDescriptor) -> tuple[str, ...]:
    return tuple(
        axis.kind for axis in tensor.parallel_axes if isinstance(axis, SplitAxis)
    )


def _replicated_axis_kinds(tensor: TensorDescriptor) -> tuple[str, ...]:
    return tuple(
        axis.kind for axis in tensor.parallel_axes if isinstance(axis, ReplicatedAxis)
    )


def _ownership_axis_kinds(tensor: TensorDescriptor) -> tuple[str, ...]:
    return tuple(
        axis.kind for axis in tensor.parallel_axes if isinstance(axis, OwnershipAxis)
    )


def _axis_rank(rank: ParallelRank, kinds: Sequence[str]) -> tuple[tuple[str, int], ...]:
    return tuple((kind, getattr(rank, kind)) for kind in kinds)


def _rank_matches(rank: ParallelRank, coordinates: tuple[tuple[str, int], ...]) -> bool:
    return all(getattr(rank, kind) == value for kind, value in coordinates)


def _validate_tensor_axis_covers(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
    *,
    topology: ParallelTopology,
) -> None:
    _validate_undeclared_axis_dependencies(tensor, fragments, topology=topology)

    ownership_kinds = _ownership_axis_kinds(tensor)
    replicated_kinds = _replicated_axis_kinds(tensor)
    declared_owners = {
        _axis_rank(fragment.rank, ownership_kinds) for fragment in fragments
    }

    for owner in sorted(declared_owners):
        owner_fragments = [
            fragment
            for fragment in fragments
            if _axis_rank(fragment.rank, ownership_kinds) == owner
        ]
        expected_replicas = {
            _axis_rank(participant.rank, replicated_kinds)
            for participant in topology.participants
            if _rank_matches(participant.rank, owner)
        }
        actual_replicas = {
            _axis_rank(fragment.rank, replicated_kinds) for fragment in owner_fragments
        }
        if actual_replicas != expected_replicas:
            raise ValueError(
                "global placement tensor is not fully covered: missing or "
                "unexpected replicated-axis participant for "
                f"{tensor.tensor_id}"
            )

        for replica in sorted(expected_replicas):
            cover = [
                fragment
                for fragment in owner_fragments
                if _axis_rank(fragment.rank, replicated_kinds) == replica
            ]
            if not _fragments_exactly_cover_tensor(tensor, cover):
                raise ValueError(
                    f"global placement tensor is not fully covered: {tensor.tensor_id}"
                )
            _validate_split_axis_participation(
                tensor,
                cover,
                topology=topology,
                fixed_coordinates=owner + replica,
            )
            _validate_split_axis_geometry(tensor, cover)


def _validate_undeclared_axis_dependencies(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
    *,
    topology: ParallelTopology,
) -> None:
    """Reject implicit ownership while allowing explicitly coupled TP/EP ranks."""

    declared_axis_kinds = tuple(axis.kind for axis in tensor.parallel_axes)
    varying_declared_axis_kinds = tuple(
        kind
        for kind in declared_axis_kinds
        if len({getattr(fragment.rank, kind) for fragment in fragments}) > 1
    )
    for kind in ("dp", "pp", "ep", "tp"):
        if kind in declared_axis_kinds or getattr(topology, f"{kind}_size") == 1:
            continue
        coordinates = {getattr(fragment.rank, kind) for fragment in fragments}
        if kind in {"dp", "pp"} or len(coordinates) <= 1:
            raise ValueError(
                "global placement tensor requires explicit parallel semantics "
                f"for active undeclared {kind} axis: {tensor.tensor_id}"
            )
        if not varying_declared_axis_kinds:
            raise ValueError(
                "global placement tensor varies across undeclared "
                f"{kind} axis: {tensor.tensor_id}"
            )

        coordinate_by_declared_rank: dict[tuple[tuple[str, int], ...], int] = {}
        for fragment in fragments:
            declared_rank = _axis_rank(fragment.rank, varying_declared_axis_kinds)
            coordinate = getattr(fragment.rank, kind)
            previous = coordinate_by_declared_rank.setdefault(
                declared_rank,
                coordinate,
            )
            if previous != coordinate:
                raise ValueError(
                    "global placement tensor varies independently across undeclared "
                    f"{kind} axis: {tensor.tensor_id}"
                )


def _validate_split_axis_geometry(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
) -> None:
    """Validate that every declared split rank owns its logical dimension."""

    split_axes = tuple(
        axis for axis in tensor.parallel_axes if isinstance(axis, SplitAxis)
    )
    if not split_axes:
        return
    coordinate_values = tuple(
        tuple(sorted({getattr(fragment.rank, axis.kind) for fragment in fragments}))
        for axis in split_axes
    )
    observed_coordinates = {
        tuple(getattr(fragment.rank, axis.kind) for axis in split_axes)
        for fragment in fragments
    }
    expected_coordinates = set(cartesian_product(*coordinate_values))
    if observed_coordinates != expected_coordinates:
        raise ValueError(
            "non-Cartesian split-axis participant mapping cannot prove "
            f"rank-to-dimension ownership: {tensor.tensor_id}"
        )

    for axis in split_axes:
        intervals_by_rank: dict[int, list[tuple[int, int]]] = {}
        for fragment in fragments:
            begin = fragment.global_offset[axis.dim]
            end = begin + fragment.local_shape[axis.dim]
            intervals_by_rank.setdefault(getattr(fragment.rank, axis.kind), []).append(
                (begin, end)
            )

        owned_intervals = sorted(
            (begin, end, rank)
            for rank, intervals in intervals_by_rank.items()
            for begin, end in _merge_intervals(intervals)
        )
        cursor = 0
        for begin, end, _ in owned_intervals:
            if begin != cursor:
                raise ValueError(
                    "split-axis rank geometry conflicts with declared dimension: "
                    f"{tensor.tensor_id}: {axis.kind} -> dim {axis.dim}"
                )
            cursor = end
        if cursor != tensor.global_shape[axis.dim]:
            raise ValueError(
                "split-axis rank geometry conflicts with declared dimension: "
                f"{tensor.tensor_id}: {axis.kind} -> dim {axis.dim}"
            )


def _merge_intervals(
    intervals: Sequence[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    merged: list[list[int]] = []
    for begin, end in sorted(set(intervals)):
        if not merged or begin > merged[-1][1]:
            merged.append([begin, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return tuple((begin, end) for begin, end in merged)


def _parallel_split_rank(
    tensor: TensorDescriptor,
    rank: ParallelRank,
) -> tuple[tuple[str, int], ...]:
    return _axis_rank(rank, _split_axis_kinds(tensor))


def _validate_split_axis_participation(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
    *,
    topology: ParallelTopology,
    fixed_coordinates: tuple[tuple[str, int], ...],
) -> None:
    split_axis_kinds = _split_axis_kinds(tensor)
    if not split_axis_kinds:
        return

    expected = {
        _parallel_split_rank(tensor, participant.rank)
        for participant in topology.participants
        if _rank_matches(participant.rank, fixed_coordinates)
    }
    actual = {_parallel_split_rank(tensor, fragment.rank) for fragment in fragments}
    complete_axis_ranges = all(
        {dict(split_rank)[kind] for split_rank in expected}
        == set(range(getattr(topology, f"{kind}_size")))
        for kind in split_axis_kinds
    )
    if not complete_axis_ranges or actual != expected:
        raise ValueError(
            "global placement tensor is not fully covered: missing or unexpected "
            f"split-axis participant for {tensor.tensor_id}"
        )


def _fragments_exactly_cover_tensor(
    tensor: TensorDescriptor,
    fragments: Sequence[PlacementFragment],
) -> bool:
    boxes = tuple(
        (fragment.global_offset, fragment.local_shape) for fragment in fragments
    )
    return boxes_exactly_cover(
        (0,) * len(tensor.global_shape), tensor.global_shape, boxes
    )


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
        boxes = tuple(
            (fragment.global_offset, fragment.local_shape)
            for fragment in owner_fragments
        )
        if boxes_overlap(boxes):
            raise ValueError(
                "logical fragment boxes overlap for tensor and parallel rank"
            )


def _validate_fragment_geometry(
    tensor: TensorDescriptor,
    fragment: PlacementFragment,
) -> None:
    validate_fragment_geometry(
        tensor,
        fragment_id=fragment.fragment_id,
        global_offset=fragment.global_offset,
        local_shape=fragment.local_shape,
        nbytes=fragment.nbytes,
    )


def _runtime_alias_descriptor_key(tensor: TensorDescriptor) -> tuple[object, ...]:
    return (
        tensor.global_shape,
        tensor.dtype,
        tensor.itemsize,
        tensor.shard_dims,
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
    tensors: Mapping[TensorId, TensorDescriptor],
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
    instance_id: RuntimeInstanceId,
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

        allocation_order = sorted(
            (binding for _, binding in fragments),
            key=lambda binding: binding.storage_address,
        )
        active_allocations: list[RuntimeBindingFragment] = []
        for current in allocation_order:
            active_allocations = [
                previous
                for previous in active_allocations
                if previous.storage_address + previous.storage_nbytes
                > current.storage_address
            ]
            for previous in active_allocations:
                if (
                    previous.storage_address == current.storage_address
                    and previous.storage_nbytes == current.storage_nbytes
                ):
                    continue
                raise ValueError(
                    "runtime binding storage allocation ranges overlap: "
                    f"{previous.fragment_id} and {current.fragment_id} "
                    f"in {address_space}"
                )
            active_allocations.append(current)
