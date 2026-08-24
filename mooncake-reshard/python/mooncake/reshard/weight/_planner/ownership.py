from __future__ import annotations

from typing import Sequence

from ...contracts import TensorId
from ...geometry import boxes_exactly_cover
from ..manifest import OwnershipAxis, PlacementFragment, TensorDescriptor
from .contracts import (
    RuntimeTensorOwner,
)
from .fragments import LogicalSourceFragment, LogicalTargetFragment


def _fragments_fully_cover_tensor(
    tensor: TensorDescriptor,
    fragments: Sequence[LogicalSourceFragment],
) -> bool:
    geometries = {
        (fragment.global_offset, fragment.local_shape): fragment
        for fragment in fragments
        if fragment.tensor_id == tensor.tensor_id
    }
    boxes = tuple(geometries)
    return boxes_exactly_cover(
        (0,) * len(tensor.global_shape), tensor.global_shape, boxes
    )


def parallel_tensor_owner(
    tensor: TensorDescriptor, fragment: PlacementFragment
) -> RuntimeTensorOwner:
    return tuple(
        (axis.kind, getattr(fragment.rank, axis.kind))
        for axis in tensor.parallel_axes
        if isinstance(axis, OwnershipAxis)
    )


def has_dp_ownership(tensor: TensorDescriptor) -> bool:
    """Return whether DP names tensor ownership instead of replica identity."""

    return any(
        isinstance(axis, OwnershipAxis) and axis.kind == "dp"
        for axis in tensor.parallel_axes
    )


def complete_dp_owned_source_owners(
    source_tensors: dict[TensorId, TensorDescriptor],
    source_fragments: Sequence[PlacementFragment],
) -> dict[TensorId, RuntimeTensorOwner]:
    """Return the complete declared source owner for each DP-owned tensor.

    Unlike a replicated DP tensor, an ``OwnershipAxis("dp")`` tensor is not
    expected to exist on every DP rank. Its placement fragments name the owner
    directly. A logical tensor may have exactly one complete owner; a partial
    or duplicate owner declaration is ambiguous and rejected before planning.
    """

    owned_tensors = tuple(
        tensor for tensor in source_tensors.values() if has_dp_ownership(tensor)
    )
    if not owned_tensors:
        return {}

    fragments_by_tensor: dict[TensorId, list[PlacementFragment]] = {}
    for fragment in source_fragments:
        fragments_by_tensor.setdefault(fragment.tensor_id, []).append(fragment)

    owners: dict[TensorId, RuntimeTensorOwner] = {}
    for tensor in owned_tensors:
        fragments_by_owner: dict[RuntimeTensorOwner, list[PlacementFragment]] = {}
        for fragment in fragments_by_tensor.get(tensor.tensor_id, ()):
            fragments_by_owner.setdefault(
                parallel_tensor_owner(tensor, fragment), []
            ).append(fragment)
        complete_owners = sorted(
            owner
            for owner, fragments in fragments_by_owner.items()
            if _fragments_fully_cover_tensor(tensor, fragments)
        )
        if len(complete_owners) != 1 or len(complete_owners) != len(
            fragments_by_owner
        ):
            if not complete_owners:
                raise ValueError(
                    "DP-owned source tensor has no complete declared owner: "
                    f"{tensor.tensor_id}"
                )
            raise ValueError(
                "DP-owned source tensor has ambiguous declared owners: "
                f"{tensor.tensor_id}"
            )
        owners[tensor.tensor_id] = complete_owners[0]
    return owners


def _validate_target_coverage(
    target_tensors: dict[TensorId, TensorDescriptor],
    target_fragments: Sequence[LogicalTargetFragment],
) -> None:
    if not target_fragments:
        raise ValueError("target manifests have no fragments")
    fragments_by_dp_and_tensor: dict[
        int, dict[TensorId, list[LogicalTargetFragment]]
    ] = {}
    for fragment in target_fragments:
        fragments_by_dp_and_tensor.setdefault(fragment.rank.dp, {}).setdefault(
            fragment.tensor_id, []
        ).append(fragment)
    for tensor in target_tensors.values():
        if has_dp_ownership(tensor):
            fragments_by_owner: dict[
                RuntimeTensorOwner, list[LogicalTargetFragment]
            ] = {}
            for by_tensor in fragments_by_dp_and_tensor.values():
                for fragment in by_tensor.get(tensor.tensor_id, ()):
                    fragments_by_owner.setdefault(
                        parallel_tensor_owner(tensor, fragment), []
                    ).append(fragment)
            if not fragments_by_owner or any(
                not _fragments_fully_cover_tensor(tensor, fragments)
                for fragments in fragments_by_owner.values()
            ):
                raise ValueError(
                    f"target tensor is not fully covered by its DP owner: "
                    f"{tensor.tensor_id}"
                )
            continue
        for dp_rank in sorted(fragments_by_dp_and_tensor):
            fragments_by_owner: dict[
                RuntimeTensorOwner, list[LogicalTargetFragment]
            ] = {}
            for fragment in fragments_by_dp_and_tensor[dp_rank].get(
                tensor.tensor_id, ()
            ):
                fragments_by_owner.setdefault(
                    parallel_tensor_owner(tensor, fragment), []
                ).append(fragment)
            if not fragments_by_owner or any(
                not _fragments_fully_cover_tensor(tensor, fragments)
                for fragments in fragments_by_owner.values()
            ):
                raise ValueError(
                    f"target tensor is not fully covered: {tensor.tensor_id}: "
                    f"dp={dp_rank}"
                )


def complete_parallel_source_replicas(
    source_tensors: dict[TensorId, TensorDescriptor],
    source_fragments: Sequence[PlacementFragment],
) -> dict[int, dict[TensorId, RuntimeTensorOwner]]:
    replicas: dict[int, dict[TensorId, RuntimeTensorOwner]] = {}
    fragments_by_dp_and_tensor: dict[int, dict[TensorId, list[PlacementFragment]]] = {}
    for fragment in source_fragments:
        dp_rank = fragment.rank.dp
        fragments_by_dp_and_tensor.setdefault(dp_rank, {}).setdefault(
            fragment.tensor_id, []
        ).append(fragment)
    replica_tensors = tuple(
        tensor for tensor in source_tensors.values() if not has_dp_ownership(tensor)
    )
    if not replica_tensors:
        return {}
    for dp_rank in sorted(fragments_by_dp_and_tensor):
        owner_by_tensor: dict[TensorId, RuntimeTensorOwner] = {}
        complete = True
        for tensor in replica_tensors:
            fragments_by_owner: dict[RuntimeTensorOwner, list[PlacementFragment]] = {}
            for fragment in fragments_by_dp_and_tensor[dp_rank].get(
                tensor.tensor_id, ()
            ):
                fragments_by_owner.setdefault(
                    parallel_tensor_owner(tensor, fragment), []
                ).append(fragment)
            complete_owners = [
                owner
                for owner, fragments in fragments_by_owner.items()
                if _fragments_fully_cover_tensor(tensor, fragments)
            ]
            if not fragments_by_owner or len(complete_owners) != len(
                fragments_by_owner
            ):
                complete = False
                break
            owner_by_tensor[tensor.tensor_id] = min(complete_owners)
        if complete:
            replicas[dp_rank] = owner_by_tensor
    if not replicas:
        raise ValueError(
            "source manifests have no complete DP replica; tensors are not fully covered"
        )
    return replicas


def _validate_local_target_inventory(
    target_tensors: dict[TensorId, TensorDescriptor],
    target_fragments: Sequence[LogicalTargetFragment],
) -> None:
    if not target_fragments:
        raise ValueError("local target manifest has no fragments")
    ranks = {fragment.rank for fragment in target_fragments}
    if len(ranks) != 1:
        raise ValueError("local target manifest must describe exactly one executor")
    missing = sorted(
        set(target_tensors) - {item.tensor_id for item in target_fragments}
    )
    if missing:
        raise ValueError(
            f"local target manifest is missing fragments: {', '.join(missing)}"
        )


__all__ = [
    "complete_dp_owned_source_owners",
    "complete_parallel_source_replicas",
    "has_dp_ownership",
    "parallel_tensor_owner",
]
