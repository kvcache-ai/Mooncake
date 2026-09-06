from __future__ import annotations

from typing import Sequence, Union, cast

from ...contracts import ParticipantId, RuntimeFragmentId, RuntimeInstanceId
from ..manifest import (
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    validate_runtime_binding,
)
from ..planner import (
    BoundWeightFragment,
    ExecutableTransferOperation,
    ExecutorTransferPlan,
    LiveTransferOperation,
    RuntimeFragmentSnapshot,
    TransferPlan,
    resolve_executor_plans,
)
from .completion import TransferEngineError


def validate_lowering_limits(
    *,
    max_batch_operations: int,
    max_region_segments: int,
    max_total_lowered_segments: int,
    max_completion_drain_attempts: int,
    completion_drain_timeout_ms: int,
) -> None:
    for name, value in (
        ("max_batch_operations", max_batch_operations),
        ("max_region_segments", max_region_segments),
        ("max_total_lowered_segments", max_total_lowered_segments),
    ):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    for name, value in (
        ("max_completion_drain_attempts", max_completion_drain_attempts),
        ("completion_drain_timeout_ms", completion_drain_timeout_ms),
    ):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")


def validate_lowering_budget(
    operations: Sequence[object],
    *,
    max_region_segments: int,
    max_total_lowered_segments: int,
) -> None:
    """Reject an executable lowering before any range expansion starts."""

    total_segments = 0
    for operation in operations:
        segment_count = getattr(operation, "segment_count", None)
        if type(segment_count) is not int or segment_count <= 0:
            raise TransferEngineError("transfer operation has invalid segment_count")
        if segment_count > max_region_segments:
            raise TransferEngineError(
                "transfer region exceeds max_region_segments: "
                f"{segment_count} > {max_region_segments}"
            )
        total_segments += segment_count
        if total_segments > max_total_lowered_segments:
            raise TransferEngineError(
                "transfer plan exceeds max_total_lowered_segments: "
                f"{total_segments} > {max_total_lowered_segments}"
            )


def validate_plan_identity(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    label: str,
) -> None:
    if not isinstance(plan, TransferPlan):
        raise TransferEngineError("plan must be a TransferPlan")
    if not isinstance(placement, WeightPlacementManifest):
        raise TransferEngineError(
            f"{label} placement must be a WeightPlacementManifest"
        )
    if placement.resource_id != plan.resource_id:
        raise TransferEngineError(f"{label} resource_id mismatch")
    if placement.revision != plan.revision:
        raise TransferEngineError(f"{label} revision mismatch")
    if placement.weight_generation != plan.weight_generation:
        raise TransferEngineError(f"{label} weight_generation mismatch")


def validate_manifest_pair(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    label: str,
) -> None:
    try:
        validate_runtime_binding(placement, binding)
    except ValueError as error:
        raise TransferEngineError(
            f"invalid {label} runtime binding: {error}"
        ) from error
    validate_plan_identity(plan, placement, label)


def runtime_binding_fragment(
    fragment: Union[RuntimeBindingFragment, BoundWeightFragment],
) -> RuntimeBindingFragment:
    if isinstance(fragment, RuntimeBindingFragment):
        return fragment
    if isinstance(fragment, BoundWeightFragment):
        return fragment.binding
    raise TransferEngineError(
        "transfer plan physical fragment must be a RuntimeBindingFragment "
        "or expose one as .binding"
    )


def require_live_transfer_operation(
    operation: ExecutableTransferOperation,
) -> LiveTransferOperation:
    """Reject Store-backed or non-canonical operations before TE submission."""

    if not isinstance(operation.source, BoundWeightFragment) or not isinstance(
        operation.target, BoundWeightFragment
    ):
        raise TransferEngineError(
            "live TE execution requires runtime-bound source and target fragments"
        )
    return cast(LiveTransferOperation, operation)


def pair_manifests(
    placement: WeightPlacementManifest,
    bindings: Sequence[WeightRuntimeBindingManifest],
    label: str,
) -> tuple[tuple[WeightPlacementManifest, WeightRuntimeBindingManifest], ...]:
    if not isinstance(placement, WeightPlacementManifest):
        raise TransferEngineError(
            f"{label} placement must be a WeightPlacementManifest"
        )
    binding_items = tuple(bindings)
    if not all(
        isinstance(binding, WeightRuntimeBindingManifest) for binding in binding_items
    ):
        raise TransferEngineError(
            f"{label} binding must be a WeightRuntimeBindingManifest"
        )
    participant_ids = [binding.participant_id for binding in binding_items]
    if len(participant_ids) != len(set(participant_ids)):
        raise TransferEngineError(f"duplicate {label} runtime binding participant")
    if any(binding.placement_id != placement.placement_id for binding in binding_items):
        raise TransferEngineError(f"{label} placement and binding IDs differ")
    return tuple((placement, binding) for binding in binding_items)


def validate_execution_input_types(
    plan: TransferPlan,
    source_placement: WeightPlacementManifest,
    source_bindings: Sequence[WeightRuntimeBindingManifest],
    target_placement: WeightPlacementManifest,
    target_bindings: Sequence[WeightRuntimeBindingManifest],
) -> None:
    if not isinstance(plan, TransferPlan):
        raise TransferEngineError("plan must be a TransferPlan")
    for label, placement in (
        ("source", source_placement),
        ("target", target_placement),
    ):
        if not isinstance(placement, WeightPlacementManifest):
            raise TransferEngineError(
                f"{label} placement must be a WeightPlacementManifest"
            )
    for label, bindings in (
        ("source", source_bindings),
        ("target", target_bindings),
    ):
        if not all(
            isinstance(binding, WeightRuntimeBindingManifest) for binding in bindings
        ):
            raise TransferEngineError(
                f"{label} binding must be a WeightRuntimeBindingManifest"
            )


def resolve_runtime_executors(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    label: str,
) -> tuple[ExecutorTransferPlan, ...]:
    return resolve_executor_plans(plan, placement, binding, label)


def validate_selected_executor_snapshot(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    label: str,
) -> tuple[ExecutorTransferPlan, ...]:
    """Validate a planned participant before its framework guard is acquired.

    The guard is the allocation-lifetime authority, but it must never be asked
    to pin an arbitrary participant or fragment set. This check uses only the
    manifest snapshot and plan identity; the executor repeats the same check
    against the fresh binding returned under the framework pin.
    """

    expected_participants = {
        executor.participant_id
        for executor in (
            plan.source_executors if label == "source" else plan.target_executors
        )
    }
    if binding.participant_id not in expected_participants:
        return ()
    try:
        return resolve_runtime_executors(plan, placement, binding, label)
    except (KeyError, ValueError) as error:
        raise TransferEngineError(str(error)) from error


def select_worker_executors(
    executors: Sequence[ExecutorTransferPlan],
    worker_id: object,
    side: str,
) -> tuple[ExecutorTransferPlan, ...]:
    executor_items = tuple(executors)
    if worker_id is None:
        return executor_items
    if type(worker_id) is not str or not worker_id:
        raise TransferEngineError(f"{side}_worker_id must be a non-empty string")
    selected = tuple(
        executor for executor in executor_items if executor.worker_id == worker_id
    )
    if not selected:
        raise TransferEngineError(f"unknown {side} worker: {worker_id}")
    return selected


def validate_scoped_executor_snapshot(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    label: str,
    required_fragment_ids: Sequence[RuntimeFragmentId],
) -> tuple[ExecutorTransferPlan, ...]:
    validate_manifest_pair(plan, placement, binding, label)
    required = frozenset(required_fragment_ids)
    if not required:
        raise TransferEngineError(f"{label} executor fragment scope is empty")
    executors = plan.source_executors if label == "source" else plan.target_executors
    matching = tuple(
        executor
        for executor in executors
        if executor.instance_id == binding.instance_id
        and executor.participant_id == binding.participant_id
        and required.intersection(executor.fragment_ids)
    )
    covered = frozenset(
        fragment_id
        for executor in matching
        for fragment_id in executor.fragment_ids
        if fragment_id in required
    )
    if covered != required:
        raise TransferEngineError(f"{label} executor snapshot mismatch")

    try:
        placement_part = next(
            part
            for part in placement.parts
            if part.participant_id == binding.participant_id
        )
    except StopIteration as error:
        raise TransferEngineError(f"{label} executor snapshot mismatch") from error
    placement_by_id = {
        fragment.placement_fragment_id: fragment
        for fragment in placement_part.fragments
    }
    runtime_by_id = {fragment.fragment_id: fragment for fragment in binding.fragments}
    planned_snapshots = {
        snapshot.fragment_id: snapshot
        for executor in matching
        for snapshot in executor.fragment_snapshots
        if snapshot.fragment_id in required
    }
    current_snapshots: dict[RuntimeFragmentId, RuntimeFragmentSnapshot] = {}
    for fragment_id in required:
        runtime_fragment = runtime_by_id.get(fragment_id)
        if runtime_fragment is None:
            raise TransferEngineError(f"{label} executor snapshot mismatch")
        placement_fragment = placement_by_id.get(runtime_fragment.placement_fragment_id)
        if placement_fragment is None:
            raise TransferEngineError(f"{label} executor snapshot mismatch")
        current_snapshots[fragment_id] = RuntimeFragmentSnapshot.from_attested_pair(
            placement_fragment,
            runtime_fragment,
            lease_generation=binding.generation,
        )
    if current_snapshots != planned_snapshots:
        raise TransferEngineError(f"{label} executor snapshot mismatch")
    return matching


def operation_indices_for_executors(
    plan: TransferPlan,
    executors: Sequence[ExecutorTransferPlan],
    side: str,
) -> tuple[int, ...]:
    return tuple(
        sorted(
            {
                index
                for executor in executors
                for index in plan.operation_indices_for_executor(executor, side)
            }
        )
    )


def executor_requirements_for_operation_indices(
    plan: TransferPlan,
    operation_indices: Sequence[int],
    side: str,
) -> dict[tuple[RuntimeInstanceId, ParticipantId], tuple[RuntimeFragmentId, ...]]:
    selected_indices = set(operation_indices)
    executors = plan.source_executors if side == "source" else plan.target_executors
    requirements: dict[
        tuple[RuntimeInstanceId, ParticipantId], set[RuntimeFragmentId]
    ] = {}
    covered_indices: set[int] = set()
    for executor in executors:
        executor_indices = selected_indices.intersection(
            plan.operation_indices_for_executor(executor, side)
        )
        if not executor_indices:
            continue
        key = (executor.instance_id, executor.participant_id)
        fragment_ids = requirements.setdefault(key, set())
        for index in executor_indices:
            operation = require_live_transfer_operation(plan.operations[index])
            fragment = operation.source if side == "source" else operation.target
            if fragment.fragment_id not in executor.fragment_ids:
                raise TransferEngineError(
                    f"{side} executor does not own planned fragment: "
                    f"{fragment.fragment_id}"
                )
            fragment_ids.add(fragment.fragment_id)
        covered_indices.update(executor_indices)
    if covered_indices != selected_indices:
        raise TransferEngineError(f"{side} executor scope is incomplete")
    return {
        key: tuple(sorted(fragment_ids)) for key, fragment_ids in requirements.items()
    }


def executors_for_operation_indices(
    plan: TransferPlan,
    operation_indices: Sequence[int],
    side: str,
) -> tuple[ExecutorTransferPlan, ...]:
    selected_indices = set(operation_indices)
    executors = plan.source_executors if side == "source" else plan.target_executors
    return tuple(
        executor
        for executor in executors
        if selected_indices.intersection(
            plan.operation_indices_for_executor(executor, side)
        )
    )
