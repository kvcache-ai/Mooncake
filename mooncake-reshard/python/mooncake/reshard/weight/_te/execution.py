from __future__ import annotations

from typing import Sequence, Union, cast

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
    if (
        max_batch_operations <= 0
        or max_region_segments <= 0
        or max_total_lowered_segments <= 0
        or max_completion_drain_attempts <= 0
        or completion_drain_timeout_ms < 0
    ):
        raise ValueError("transfer lowering limits must be positive")


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
