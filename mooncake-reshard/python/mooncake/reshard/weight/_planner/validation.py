from __future__ import annotations

from ...contracts import ParticipantId, PlacementFragmentId, TensorId
from ...geometry import regions_exactly_cover
from ..manifest import (
    PlacementFragment,
    TensorDescriptor,
)
from .contracts import (
    LogicalTransferOperation,
    LogicalTransferPlan,
)


def _validate_logical_target_coverage(logical_plan: LogicalTransferPlan) -> None:
    """Reject plans that would leave a selected target fragment unwritten."""

    target_parts = {
        part.participant_id: part for part in logical_plan.target_placement.parts
    }
    participant_by_fragment = {
        fragment.placement_fragment_id: part.participant_id
        for part in logical_plan.target_placement.parts
        for fragment in part.fragments
    }
    operation_participants: set[ParticipantId] = set()
    operations_by_fragment: dict[
        PlacementFragmentId, list[LogicalTransferOperation]
    ] = {}

    for operation in logical_plan.operations:
        target = operation.target
        if not isinstance(target, PlacementFragment):
            raise ValueError("logical transfer operation target must be a placement")
        target_participant_id = participant_by_fragment.get(
            target.placement_fragment_id
        )
        if target_participant_id is None or not any(
            fragment == target
            for fragment in target_parts[target_participant_id].fragments
        ):
            raise ValueError(
                "logical plan and target placement fragment snapshots differ"
            )
        operations_by_fragment.setdefault(target.placement_fragment_id, []).append(
            operation
        )
        operation_participants.add(target_participant_id)

    if not logical_plan.target_executors:
        raise ValueError("logical plan has no target executor metadata")

    seen_participants: set[ParticipantId] = set()
    selected_fragments: dict[PlacementFragmentId, PlacementFragment] = {}
    selected_tensors: dict[TensorId, TensorDescriptor] = {}
    for executor in logical_plan.target_executors:
        if executor.placement_id != logical_plan.target_placement.placement_id:
            raise ValueError("logical target executor placement differs from target")
        if executor.participant_id in seen_participants:
            raise ValueError("logical plan has duplicate target executor participant")
        seen_participants.add(executor.participant_id)
        part = target_parts.get(executor.participant_id)
        if part is None or executor.rank != part.rank:
            raise ValueError("logical target executor differs from target placement")
        expected_fragment_ids = tuple(
            sorted(fragment.placement_fragment_id for fragment in part.fragments)
        )
        if executor.placement_fragment_ids != expected_fragment_ids:
            raise ValueError(
                "logical target executor fragments differ from target placement"
            )
        if not logical_plan.operation_indices_for_executor(executor, "target"):
            raise ValueError(
                "logical target executor operations differ from target placement"
            )
        selected_fragments.update(
            {fragment.placement_fragment_id: fragment for fragment in part.fragments}
        )
        for tensor in part.tensors:
            previous = selected_tensors.setdefault(tensor.tensor_id, tensor)
            if previous != tensor:
                raise ValueError("logical target tensor catalog differs from placement")

    if seen_participants != operation_participants:
        raise ValueError("logical target executors differ from planned operations")
    if logical_plan.target_tensors != tuple(
        sorted(selected_tensors.values(), key=lambda item: item.tensor_id)
    ):
        raise ValueError("logical target tensor catalog differs from placement")

    for fragment_id, target in selected_fragments.items():
        operations = operations_by_fragment.get(fragment_id, ())
        if not regions_exactly_cover(target, operations):
            raise ValueError(
                f"logical target fragment is not fully covered: {fragment_id}"
            )


def _validate_logical_source_placement(logical_plan: LogicalTransferPlan) -> None:
    """Reject placement-source plans whose public projections contradict the source."""

    source_placement = logical_plan.source_placement
    if source_placement is None:
        return
    if logical_plan.source_tensors != source_placement.tensors:
        raise ValueError("logical plan source tensor catalog differs from placement")
    if not logical_plan.source_executors:
        raise ValueError("logical plan has no source executor metadata")

    source_parts = {part.participant_id: part for part in source_placement.parts}
    participant_by_fragment = {
        fragment.placement_fragment_id: part.participant_id
        for part in source_placement.parts
        for fragment in part.fragments
    }
    operation_participants: set[ParticipantId] = set()
    for operation in logical_plan.operations:
        source = operation.source
        if not isinstance(source, PlacementFragment):
            raise ValueError("logical transfer operation source must be a placement")
        source_participant_id = participant_by_fragment.get(
            source.placement_fragment_id
        )
        if source_participant_id is None or not any(
            fragment == source
            for fragment in source_parts[source_participant_id].fragments
        ):
            raise ValueError(
                "logical plan and source placement fragment snapshots differ"
            )
        operation_participants.add(source_participant_id)

    seen_participants: set[ParticipantId] = set()
    for executor in logical_plan.source_executors:
        if executor.placement_id != source_placement.placement_id:
            raise ValueError("logical source executor placement differs from source")
        if executor.participant_id in seen_participants:
            raise ValueError("logical plan has duplicate source executor participant")
        seen_participants.add(executor.participant_id)
        part = source_parts.get(executor.participant_id)
        if part is None or executor.rank != part.rank:
            raise ValueError("logical source executor differs from source placement")
        expected_fragment_ids = tuple(
            sorted(fragment.placement_fragment_id for fragment in part.fragments)
        )
        if executor.placement_fragment_ids != expected_fragment_ids:
            raise ValueError(
                "logical source executor fragments differ from source placement"
            )
        if not logical_plan.operation_indices_for_executor(executor, "source"):
            raise ValueError(
                "logical source executor operations differ from source placement"
            )

    if seen_participants != operation_participants:
        raise ValueError("logical source executors differ from planned operations")


def _validate_tensor_compatibility(
    source: TensorDescriptor, target: TensorDescriptor
) -> None:
    if source.layout_fingerprint != target.layout_fingerprint:
        raise ValueError(f"layout mismatch for tensor {source.tensor_id}")
    if (
        source.global_shape != target.global_shape
        or source.dtype != target.dtype
        or source.itemsize != target.itemsize
        or source.layer_id != target.layer_id
        or source.expert_id != target.expert_id
    ):
        raise ValueError(f"tensor descriptor mismatch: {source.tensor_id}")


def _validate_tensor_sets(
    source_tensors: dict[TensorId, TensorDescriptor],
    target_tensors: dict[TensorId, TensorDescriptor],
) -> None:
    source_ids = set(source_tensors)
    target_ids = set(target_tensors)
    missing = sorted(source_ids - target_ids)
    if missing:
        raise ValueError(f"target manifests are missing tensors: {', '.join(missing)}")
    unexpected = sorted(target_ids - source_ids)
    if unexpected:
        raise ValueError(
            f"target manifests contain unknown tensors: {', '.join(unexpected)}"
        )


def _validate_tensor_subset(
    source_tensors: dict[TensorId, TensorDescriptor],
    target_tensors: dict[TensorId, TensorDescriptor],
) -> None:
    unexpected = sorted(set(target_tensors) - set(source_tensors))
    if unexpected:
        raise ValueError(
            f"target manifests contain unknown tensors: {', '.join(unexpected)}"
        )
