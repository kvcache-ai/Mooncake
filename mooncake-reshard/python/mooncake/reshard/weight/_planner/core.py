from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from ...contracts import (
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    ResourceId,
    RevisionId,
    TensorId,
)
from ...geometry import boxes_exactly_cover
from ..placement import WeightPlacementManifest
from ..storage_manifest import StoredFragment, WeightManifest
from ..types import ParallelRank, PlacementFragment, TensorDescriptor
from . import geometry
from .contracts import (
    LogicalTransferPlan,
    LogicalTransferOperation,
    PlacementExecutorPlan,
    PlanningLimits,
    TransferRegion,
)
from .fragments import LogicalSourceFragment, LogicalTargetFragment
from .ownership import (
    _validate_local_target_inventory,
    _validate_target_coverage,
    complete_dp_owned_source_owners,
    complete_parallel_source_replicas,
    has_dp_ownership,
    parallel_tensor_owner,
)
from .validation import (
    _validate_tensor_compatibility,
    _validate_tensor_sets,
    _validate_tensor_subset,
)


@dataclass(frozen=True)
class _PlannedTransfer:
    resource_id: ResourceId
    revision: RevisionId
    operations: tuple[LogicalTransferOperation, ...]
    planning_limits: PlanningLimits


def _collect_placements(
    manifests: Sequence[WeightPlacementManifest],
    label: str,
) -> tuple[dict[TensorId, TensorDescriptor], list[PlacementFragment]]:
    if not manifests:
        raise ValueError(f"{label} placement manifests must not be empty")
    if len(manifests) != 1:
        raise ValueError(f"{label} requires one complete WeightPlacementManifest")
    if any(not isinstance(manifest, WeightPlacementManifest) for manifest in manifests):
        raise ValueError(f"{label} placement manifests must be WeightPlacementManifest")
    resource_id = manifests[0].resource_id
    revision = manifests[0].revision
    placement_ids: set[PlacementId] = set()
    fragment_ids: set[PlacementFragmentId] = set()
    tensors: dict[TensorId, TensorDescriptor] = {}
    fragments: list[PlacementFragment] = []
    for manifest in manifests:
        if manifest.resource_id != resource_id or manifest.revision != revision:
            raise ValueError(
                f"{label} placement manifests describe different revisions"
            )
        if manifest.placement_id in placement_ids:
            raise ValueError(f"duplicate {label} placement_id: {manifest.placement_id}")
        placement_ids.add(manifest.placement_id)
        for tensor in manifest.tensors:
            previous = tensors.setdefault(tensor.tensor_id, tensor)
            if previous != tensor:
                raise ValueError(
                    f"{label} placement tensor descriptor mismatch: {tensor.tensor_id}"
                )
        for fragment in manifest.fragments:
            if fragment.placement_fragment_id in fragment_ids:
                raise ValueError(
                    f"duplicate {label} placement fragment: "
                    f"{fragment.placement_fragment_id}"
                )
            fragment_ids.add(fragment.placement_fragment_id)
            fragments.append(fragment)
    return tensors, fragments


def _build_placement_executor_plans(
    manifests: Sequence[WeightPlacementManifest],
    operations: Sequence[LogicalTransferOperation],
    side: str,
    participant_ids: Optional[frozenset[ParticipantId]] = None,
) -> tuple[PlacementExecutorPlan, ...]:
    if side not in ("source", "target"):
        raise ValueError(f"invalid placement executor side: {side}")
    operation_indices_by_fragment: dict[PlacementFragmentId, list[int]] = {}
    for index, operation in enumerate(operations):
        fragment = operation.source if side == "source" else operation.target
        if not isinstance(fragment, PlacementFragment):
            raise ValueError(f"placement executor operation {side} is not a placement")
        operation_indices_by_fragment.setdefault(
            fragment.placement_fragment_id, []
        ).append(index)

    result: list[PlacementExecutorPlan] = []
    ranks: set[ParallelRank] = set()
    for manifest in manifests:
        for part in manifest.parts:
            if (
                participant_ids is not None
                and part.participant_id not in participant_ids
            ):
                continue
            fragments = part.fragments
            if not fragments:
                continue
            rank = part.rank
            if rank in ranks:
                raise ValueError(f"duplicate {side} placement executor rank: {rank}")
            ranks.add(rank)
            fragment_ids = tuple(
                sorted(fragment.placement_fragment_id for fragment in fragments)
            )
            if not any(
                fragment_id in operation_indices_by_fragment
                for fragment_id in fragment_ids
            ):
                continue
            result.append(
                PlacementExecutorPlan(
                    placement_id=manifest.placement_id,
                    participant_id=part.participant_id,
                    rank=rank,
                    placement_fragment_ids=fragment_ids,
                )
            )
    result.sort(
        key=lambda item: (item.rank.dp, item.rank.pp, item.rank.ep, item.rank.tp)
    )
    return tuple(result)


def _plan_transfer(
    resource_id: ResourceId,
    revision: RevisionId,
    source_tensors: dict[TensorId, TensorDescriptor],
    source_fragments: Sequence[LogicalSourceFragment],
    target_tensors: dict[TensorId, TensorDescriptor],
    target_fragments: Sequence[LogicalTargetFragment],
    *,
    local_target: bool = False,
    planning_limits: Optional[PlanningLimits] = None,
) -> _PlannedTransfer:
    planning_limits = planning_limits or PlanningLimits()
    if local_target:
        _validate_tensor_subset(source_tensors, target_tensors)
        _validate_local_target_inventory(target_tensors, target_fragments)
    else:
        _validate_tensor_sets(source_tensors, target_tensors)
        _validate_target_coverage(target_tensors, target_fragments)
    placement_source_fragments = tuple(
        fragment
        for fragment in source_fragments
        if isinstance(fragment, PlacementFragment)
    )
    stored_source_fragments = tuple(
        fragment
        for fragment in source_fragments
        if isinstance(fragment, StoredFragment)
    )
    if (
        len(placement_source_fragments) + len(stored_source_fragments)
        != len(source_fragments)
        or not source_fragments
    ):
        raise ValueError("source fragments mix placement and stored locations")
    if placement_source_fragments and stored_source_fragments:
        raise ValueError("source fragments mix placement and stored locations")
    parallel_sources = bool(placement_source_fragments)
    source_replicas = (
        complete_parallel_source_replicas(source_tensors, placement_source_fragments)
        if parallel_sources
        else {}
    )
    source_dp_owners = (
        complete_dp_owned_source_owners(
            source_tensors, placement_source_fragments
        )
        if parallel_sources
        else {}
    )
    source_dp_ranks = sorted(source_replicas)
    source_dp_by_target_dp = (
        {
            target_dp: source_dp_ranks[target_dp % len(source_dp_ranks)]
            for target_dp in {fragment.rank.dp for fragment in target_fragments}
        }
        if parallel_sources and source_dp_ranks
        else {}
    )
    candidates: dict[
        TensorId,
        dict[geometry.GeometryKey, list[LogicalSourceFragment]],
    ] = {}
    for fragment in source_fragments:
        candidates.setdefault(fragment.tensor_id, {}).setdefault(
            geometry._geometry_key(fragment), []
        ).append(fragment)
    for tensor_candidates in candidates.values():
        for group in tensor_candidates.values():
            group.sort(key=geometry._source_sort_key)
    candidate_indexes = {
        tensor_id: geometry._CandidateBoxIndex.build(
            tuple(tuple(group) for group in tensor_candidates.values())
        )
        for tensor_id, tensor_candidates in candidates.items()
    }

    operations: list[TransferRegion[LogicalSourceFragment, LogicalTargetFragment]] = []
    total_lowered_segments = 0
    for target in sorted(target_fragments, key=lambda item: item.fragment_id):
        target_tensor = target_tensors[target.tensor_id]
        source_tensor = source_tensors.get(target.tensor_id)
        if source_tensor is None:
            raise ValueError(f"missing source tensor: {target.tensor_id}")
        _validate_tensor_compatibility(source_tensor, target_tensor)

        overlaps: list[
            tuple[tuple[int, ...], tuple[int, ...], LogicalSourceFragment]
        ] = []
        candidate_index = candidate_indexes.get(target.tensor_id)
        candidate_groups = (
            candidate_index.query(target) if candidate_index is not None else ()
        )
        for group in candidate_groups:
            if parallel_sources:
                if has_dp_ownership(source_tensor):
                    source_owner = source_dp_owners[target.tensor_id]
                    eligible = [
                        fragment
                        for fragment in group
                        if isinstance(fragment, PlacementFragment)
                        and parallel_tensor_owner(source_tensor, fragment)
                        == source_owner
                    ]
                else:
                    source_dp = source_dp_by_target_dp[target.rank.dp]
                    source_owner = source_replicas[source_dp][target.tensor_id]
                    eligible = [
                        fragment
                        for fragment in group
                        if isinstance(fragment, PlacementFragment)
                        and fragment.rank.dp == source_dp
                        and parallel_tensor_owner(source_tensor, fragment)
                        == source_owner
                    ]
                if not eligible:
                    continue
                representative = eligible[0]
                selected = representative
            else:
                representative = group[0]
                selected = representative
            overlap = geometry._overlap_box(representative, target)
            if overlap is None:
                continue
            if (
                len(operations) + len(overlaps) + 1
                > planning_limits.max_transfer_regions
            ):
                raise ValueError(
                    "logical transfer plan exceeds max_transfer_regions: "
                    f"{planning_limits.max_transfer_regions}"
                )
            overlaps.append((*overlap, selected))
        overlaps.sort(key=lambda item: (item[0], item[1], item[2].fragment_id))

        if not boxes_exactly_cover(
            target.global_offset,
            target.local_shape,
            tuple((offset, shape) for offset, shape, _ in overlaps),
        ):
            raise ValueError(
                f"target fragment is not fully covered: {target.fragment_id}"
            )
        for overlap_offset, overlap_shape, source in overlaps:
            region = geometry._transfer_region(
                target_tensor,
                source,
                target,
                overlap_offset,
                overlap_shape,
            )
            if region.segment_count > planning_limits.max_segments_per_region:
                raise ValueError(
                    "logical transfer region exceeds max_segments_per_region: "
                    f"{region.segment_count} > "
                    f"{planning_limits.max_segments_per_region}"
                )
            next_total_lowered_segments = total_lowered_segments + region.segment_count
            if next_total_lowered_segments > planning_limits.max_total_lowered_segments:
                raise ValueError(
                    "logical transfer plan exceeds max_total_lowered_segments: "
                    f"{next_total_lowered_segments} > "
                    f"{planning_limits.max_total_lowered_segments}"
                )
            operations.append(region)
            total_lowered_segments = next_total_lowered_segments

    operations.sort(
        key=lambda item: (
            item.target.fragment_id,
            item.target_base_offset,
            item.source.fragment_id,
            item.source_base_offset,
        )
    )
    return _PlannedTransfer(
        resource_id=resource_id,
        revision=revision,
        operations=tuple(operations),
        planning_limits=planning_limits,
    )


def _logical_transfer_plan(
    transfer: _PlannedTransfer,
    *,
    source_tensors: dict[TensorId, TensorDescriptor],
    target_tensors: dict[TensorId, TensorDescriptor],
    source_placement: Optional[WeightPlacementManifest],
    source_manifest: Optional[WeightManifest],
    target_placement: WeightPlacementManifest,
    source_participant_ids: Optional[frozenset[ParticipantId]] = None,
    target_participant_ids: Optional[frozenset[ParticipantId]] = None,
) -> LogicalTransferPlan:
    operations = transfer.operations
    return LogicalTransferPlan(
        resource_id=transfer.resource_id,
        revision=transfer.revision,
        source_placement=source_placement,
        target_placement=target_placement,
        source_tensors=tuple(
            sorted(source_tensors.values(), key=lambda item: item.tensor_id)
        ),
        target_tensors=tuple(
            sorted(target_tensors.values(), key=lambda item: item.tensor_id)
        ),
        operations=operations,
        source_manifest=source_manifest,
        planning_limits=transfer.planning_limits,
        source_executors=(
            _build_placement_executor_plans(
                (source_placement,),
                operations,
                "source",
                source_participant_ids,
            )
            if source_placement is not None
            else ()
        ),
        target_executors=_build_placement_executor_plans(
            (target_placement,),
            operations,
            "target",
            target_participant_ids,
        ),
    )
