from __future__ import annotations

from typing import Optional

from ...contracts import ParticipantId
from ..manifest import WeightPlacementManifest
from ..storage_manifest import StoredWeightManifest
from .contracts import LogicalTransferPlan, PlanningLimits
from .core import (
    _collect_placements,
    _logical_transfer_plan,
    _plan_transfer,
)


def plan_placement_transfer(
    source_placement: WeightPlacementManifest,
    target_placement: WeightPlacementManifest,
    *,
    planning_limits: Optional[PlanningLimits] = None,
) -> LogicalTransferPlan:
    """Plan a reshard between two complete address-free placements."""

    source_tensors, source_fragments = _collect_placements(
        (source_placement,),
        "source",
    )
    target_tensors, target_fragments = _collect_placements(
        (target_placement,),
        "target",
    )
    source = source_placement
    target = target_placement
    if source.resource_id != target.resource_id:
        raise ValueError("source and target resource_id differ")
    if source.revision != target.revision:
        raise ValueError("source and target revision differ")
    if source.weight_generation != target.weight_generation:
        raise ValueError("source and target weight_generation differ")
    transfer = _plan_transfer(
        source.resource_id,
        source.revision,
        source_tensors,
        source_fragments,
        target_tensors,
        target_fragments,
        planning_limits=planning_limits,
    )
    return _logical_transfer_plan(
        transfer,
        source_tensors=source_tensors,
        target_tensors=target_tensors,
        source_placement=source_placement,
        source_manifest=None,
        target_placement=target_placement,
    )


def plan_placement_transfer_to_local_target(
    source_placement: WeightPlacementManifest,
    target_placement: WeightPlacementManifest,
    target_participant_id: Optional[ParticipantId] = None,
    *,
    planning_limits: Optional[PlanningLimits] = None,
) -> LogicalTransferPlan:
    """Plan one target executor using address-free source and target layouts."""

    source_tensors, source_fragments = _collect_placements(
        (source_placement,),
        "source",
    )
    if target_participant_id is None:
        if target_placement.topology.world_size != 1:
            raise ValueError(
                "target_participant_id is required for a multi-participant target"
            )
        target_participant_id = target_placement.parts[0].participant_id
    try:
        target_part = next(
            part
            for part in target_placement.parts
            if part.participant_id == target_participant_id
        )
    except StopIteration as error:
        raise ValueError(
            f"unknown target participant: {target_participant_id}"
        ) from error
    target_tensors = {tensor.tensor_id: tensor for tensor in target_part.tensors}
    target_fragments = list(target_part.fragments)
    source = source_placement
    if source.resource_id != target_placement.resource_id:
        raise ValueError("source and target resource_id differ")
    if source.revision != target_placement.revision:
        raise ValueError("source and target revision differ")
    if source.weight_generation != target_placement.weight_generation:
        raise ValueError("source and target weight_generation differ")
    transfer = _plan_transfer(
        source.resource_id,
        source.revision,
        source_tensors,
        source_fragments,
        target_tensors,
        target_fragments,
        local_target=True,
        planning_limits=planning_limits,
    )
    result = _logical_transfer_plan(
        transfer,
        source_tensors=source_tensors,
        target_tensors=target_tensors,
        source_placement=source_placement,
        source_manifest=None,
        target_placement=target_placement,
        target_participant_ids=frozenset({target_participant_id}),
    )
    if len(result.target_executors) != 1:
        raise ValueError("local target placement must describe exactly one executor")
    return result


def plan_stored_transfer_to_target_placement(
    source_manifest: StoredWeightManifest,
    target_placement: WeightPlacementManifest,
    *,
    planning_limits: Optional[PlanningLimits] = None,
) -> LogicalTransferPlan:
    """Plan a Store-backed transfer into address-free target placements."""

    target_tensors, target_fragments = _collect_placements(
        (target_placement,),
        "target",
    )
    target = target_placement
    if source_manifest.resource_id != target.resource_id:
        raise ValueError("source and target resource_id differ")
    if source_manifest.revision != target.revision:
        raise ValueError("source and target revision differ")
    if source_manifest.weight_generation != target.weight_generation:
        raise ValueError("source and target weight_generation differ")
    source_tensors = {tensor.tensor_id: tensor for tensor in source_manifest.tensors}
    transfer = _plan_transfer(
        source_manifest.resource_id,
        source_manifest.revision,
        source_tensors,
        source_manifest.fragments,
        target_tensors,
        target_fragments,
        planning_limits=planning_limits,
    )
    return _logical_transfer_plan(
        transfer,
        source_tensors=source_tensors,
        target_tensors=target_tensors,
        source_placement=None,
        source_manifest=source_manifest,
        target_placement=target_placement,
    )


__all__ = [
    "plan_placement_transfer",
    "plan_placement_transfer_to_local_target",
    "plan_stored_transfer_to_target_placement",
]
