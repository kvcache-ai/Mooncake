from __future__ import annotations

from typing import Sequence, Union

from ...contracts import (
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    RuntimeFragmentId,
    TensorId,
)
from ..binding import _validate_runtime_binding_subset, validate_runtime_binding
from ..manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)
from ..storage_manifest import (
    StoredFragmentSnapshot,
    StoredWeightManifest,
    validate_weight_manifest_snapshot,
)
from .bound_contracts import ExecutorTransferPlan, RuntimeFragmentSnapshot, TransferPlan
from .bound_validation import _validate_target_physical_ranges
from .contracts import (
    BoundWeightFragment,
    ExecutableTransferOperation,
    LogicalTransferOperation,
    LogicalTransferPlan,
    PlacementExecutorPlan,
    TransferRegion,
)
from .core import _collect_placements
from .attestation import RuntimeBindingAttestation
from .validation import _validate_logical_target_coverage


def _validated_bindings_by_placement_id(
    placements: Sequence[WeightPlacementManifest],
    bindings: Sequence[WeightRuntimeBindingManifest],
    label: str,
    required_participants: dict[PlacementId, frozenset[ParticipantId]],
    *,
    retain_unselected: bool = False,
) -> dict[tuple[PlacementId, ParticipantId], WeightRuntimeBindingManifest]:
    if not placements:
        if bindings:
            raise ValueError(f"logical plan has no {label} placements")
        return {}
    if not bindings:
        raise ValueError(f"logical plan requires {label} runtime bindings")

    placement_by_id = {placement.placement_id: placement for placement in placements}
    binding_by_id: dict[
        tuple[PlacementId, ParticipantId], WeightRuntimeBindingManifest
    ] = {}
    runtime_fragment_ids: set[RuntimeFragmentId] = set()
    for binding in bindings:
        if not isinstance(binding, WeightRuntimeBindingManifest):
            raise ValueError(f"invalid {label} runtime binding input")
        key = (binding.placement_id, binding.participant_id)
        if key in binding_by_id:
            raise ValueError(
                f"duplicate {label} runtime binding participant: "
                f"{binding.participant_id}"
            )
        binding_by_id[key] = binding
        for fragment in binding.fragments:
            if fragment.fragment_id in runtime_fragment_ids:
                raise ValueError(
                    f"duplicate {label} runtime fragment_id: {fragment.fragment_id}"
                )
            runtime_fragment_ids.add(fragment.fragment_id)

    unknown_placements = sorted(
        {placement_id for placement_id, _ in binding_by_id} - set(placement_by_id)
    )
    if unknown_placements:
        raise ValueError(f"logical plan and {label} placement IDs differ")
    for placement_id, placement in placement_by_id.items():
        placement_bindings = tuple(
            binding
            for (binding_placement_id, _), binding in binding_by_id.items()
            if binding_placement_id == placement_id
        )
        required = required_participants.get(placement_id, frozenset())
        actual = {binding.participant_id for binding in placement_bindings}
        all_participants = {
            part.participant_id for part in placement.parts if part.fragments
        }
        if not required.issubset(actual) or not actual.issubset(all_participants):
            raise ValueError(
                f"logical plan and {label} runtime binding participants differ"
            )
        _validate_runtime_binding_subset(placement, placement_bindings)
    if retain_unselected:
        return binding_by_id
    return {
        key: binding
        for key, binding in binding_by_id.items()
        if key[1] in required_participants.get(key[0], frozenset())
    }


def _bound_fragment(
    placement: PlacementFragment,
    binding: RuntimeBindingFragment,
    attestation: RuntimeBindingAttestation,
) -> BoundWeightFragment:
    evidence = attestation.binding_fragment(placement.placement_fragment_id)
    return BoundWeightFragment(
        placement=placement,
        binding=evidence,
        instance_id=attestation.evidence.instance_id,
        runtime_lease_id=attestation.evidence.lease_id,
        lease_generation=attestation.evidence.generation,
        owner=None,
        attestation=attestation,
    )


def _bound_fragments_by_placement_fragment_id(
    placements: Sequence[WeightPlacementManifest],
    binding_by_placement_id: dict[
        tuple[PlacementId, ParticipantId], WeightRuntimeBindingManifest
    ],
    label: str,
) -> dict[PlacementFragmentId, BoundWeightFragment]:
    result: dict[PlacementFragmentId, BoundWeightFragment] = {}
    for placement in placements:
        for part in placement.parts:
            binding = binding_by_placement_id.get(
                (placement.placement_id, part.participant_id)
            )
            if binding is None:
                continue
            attestation = RuntimeBindingAttestation(placement, binding)
            runtime_by_id = {
                fragment.placement_fragment_id: fragment
                for fragment in binding.fragments
            }
            for placement_fragment in part.fragments:
                placement_fragment_id = placement_fragment.placement_fragment_id
                if placement_fragment_id in result:
                    raise ValueError(
                        f"duplicate bound {label} placement fragment: "
                        f"{placement_fragment_id}"
                    )
                result[placement_fragment_id] = _bound_fragment(
                    placement_fragment,
                    runtime_by_id[placement_fragment_id],
                    attestation,
                )
    return result


def _tensor_map(
    tensors: Sequence[TensorDescriptor],
) -> dict[TensorId, TensorDescriptor]:
    result = {tensor.tensor_id: tensor for tensor in tensors}
    if len(result) != len(tensors):
        raise ValueError("logical transfer plan has duplicate tensor descriptors")
    return result


def _validated_store_source_manifest(
    logical_plan: LogicalTransferPlan,
    source_manifest: StoredWeightManifest | None,
) -> StoredWeightManifest | None:
    if logical_plan.source_manifest is None:
        if source_manifest is not None:
            raise ValueError("runtime source plan must not receive a source manifest")
        return None
    if source_manifest is None:
        raise ValueError("stored source plan requires a source manifest")
    authoritative_manifest = validate_weight_manifest_snapshot(source_manifest)
    if (
        logical_plan.source_manifest_identity
        != authoritative_manifest.manifest_identity
    ):
        raise ValueError("logical plan source manifest identity differs")
    return authoritative_manifest


def _validate_logical_tensor_snapshots(
    logical_plan: LogicalTransferPlan,
    source_manifest: StoredWeightManifest | None,
) -> None:
    source_tensors = _tensor_map(logical_plan.source_tensors)
    target_tensors = _tensor_map(logical_plan.target_tensors)
    if logical_plan.source_placement is not None:
        current_source_tensors, _ = _collect_placements(
            (logical_plan.source_placement,),
            "source",
        )
        if current_source_tensors != source_tensors:
            raise ValueError(
                "logical plan and source placement tensor descriptors differ"
            )
    elif source_manifest is not None:
        current_source_tensors = {
            tensor.tensor_id: tensor for tensor in source_manifest.tensors
        }
        if current_source_tensors != source_tensors:
            raise ValueError(
                "logical plan and source manifest tensor descriptors differ"
            )
    current_target_tensors, _ = _collect_placements(
        (logical_plan.target_placement,),
        "target",
    )
    target_participant_ids = {
        executor.participant_id for executor in logical_plan.target_executors
    }
    if target_participant_ids:
        current_target_tensors = {
            tensor.tensor_id: tensor
            for part in logical_plan.target_placement.parts
            if part.participant_id in target_participant_ids
            for tensor in part.tensors
        }
    if current_target_tensors != target_tensors:
        raise ValueError("logical plan and target placement tensor descriptors differ")


def _validate_logical_fragment_snapshots(
    logical_plan: LogicalTransferPlan,
    source_manifest: StoredWeightManifest | None,
) -> None:
    logical_plan.validate_source_manifest_snapshot()
    target_by_id = {
        fragment.placement_fragment_id: fragment
        for fragment in logical_plan.target_placement.fragments
    }
    source_by_id = (
        {
            fragment.placement_fragment_id: fragment
            for fragment in logical_plan.source_placement.fragments
        }
        if logical_plan.source_placement is not None
        else {}
    )
    stored_source_by_id = (
        {fragment.fragment_id: fragment for fragment in source_manifest.fragments}
        if source_manifest is not None
        else {}
    )
    for operation in logical_plan.operations:
        target = operation.target
        if (
            not isinstance(target, PlacementFragment)
            or target_by_id.get(target.placement_fragment_id) != target
        ):
            raise ValueError(
                "logical plan and target placement fragment snapshots differ"
            )
        source = operation.source
        if logical_plan.source_placement is not None and (
            not isinstance(source, PlacementFragment)
            or source_by_id.get(source.placement_fragment_id) != source
        ):
            raise ValueError(
                "logical plan and source placement fragment snapshots differ"
            )
        if logical_plan.source_manifest is not None and (
            not isinstance(source, StoredFragmentSnapshot)
            or stored_source_by_id.get(source.fragment_id) != source
        ):
            raise ValueError(
                "logical plan and source manifest fragment snapshots differ"
            )


def _required_participants(
    executors: Sequence[PlacementExecutorPlan],
) -> dict[PlacementId, frozenset[ParticipantId]]:
    participants: dict[PlacementId, set[ParticipantId]] = {}
    for executor in executors:
        participants.setdefault(executor.placement_id, set()).add(
            executor.participant_id
        )
    return {
        placement_id: frozenset(values) for placement_id, values in participants.items()
    }


def _bind_operation(
    operation: LogicalTransferOperation,
    source: Union[BoundWeightFragment, StoredFragmentSnapshot],
    target: BoundWeightFragment,
) -> ExecutableTransferOperation:
    """Rebuild a logical value object with the bound execution fragments."""

    if isinstance(operation, TransferRegion):
        return TransferRegion(
            tensor_id=operation.tensor_id,
            source=source,
            target=target,
            overlap_offset=operation.overlap_offset,
            overlap_shape=operation.overlap_shape,
            source_base_offset=operation.source_base_offset,
            target_base_offset=operation.target_base_offset,
            inner_bytes=operation.inner_bytes,
            outer_loop_counts=operation.outer_loop_counts,
            source_strides=operation.source_strides,
            target_strides=operation.target_strides,
        )
    raise ValueError("logical transfer operation is not canonical")


def _build_executor_plans(
    placements: Sequence[WeightPlacementManifest],
    bindings: Sequence[WeightRuntimeBindingManifest],
    side: str,
    *,
    selected_fragment_ids: frozenset[RuntimeFragmentId] | None = None,
) -> tuple[ExecutorTransferPlan, ...]:
    if side not in ("source", "target"):
        raise ValueError(f"invalid executor side: {side}")
    binding_by_participant = {
        (binding.placement_id, binding.participant_id): binding for binding in bindings
    }
    if len(binding_by_participant) != len(bindings):
        raise ValueError(f"duplicate {side} runtime binding participant")

    result: list[ExecutorTransferPlan] = []
    executor_keys: set[tuple[ParallelRank, str]] = set()
    observed_selected_fragment_ids: set[RuntimeFragmentId] = set()
    for placement in placements:
        for part in placement.parts:
            binding = binding_by_participant.get(
                (placement.placement_id, part.participant_id)
            )
            if binding is None:
                continue
            validate_runtime_binding(placement, binding)
            attestation = RuntimeBindingAttestation(placement, binding)
            if not part.fragments:
                continue
            runtime_by_placement_fragment_id = {
                fragment.placement_fragment_id: fragment
                for fragment in binding.fragments
            }
            fragments = [
                BoundWeightFragment(
                    placement=placement_fragment,
                    binding=runtime_by_placement_fragment_id[
                        placement_fragment.placement_fragment_id
                    ],
                    instance_id=binding.instance_id,
                    runtime_lease_id=binding.lease_id,
                    lease_generation=binding.generation,
                    owner=runtime_by_placement_fragment_id[
                        placement_fragment.placement_fragment_id
                    ].owner,
                    attestation=attestation,
                )
                for placement_fragment in part.fragments
                if selected_fragment_ids is None
                or runtime_by_placement_fragment_id[
                    placement_fragment.placement_fragment_id
                ].fragment_id
                in selected_fragment_ids
            ]
            fragments_by_worker: dict[str, list[BoundWeightFragment]] = {}
            for fragment in fragments:
                fragments_by_worker.setdefault(fragment.worker_id, []).append(fragment)
            for worker_id, worker_fragments in sorted(fragments_by_worker.items()):
                ordered_fragments = sorted(
                    worker_fragments,
                    key=lambda fragment: fragment.fragment_id,
                )
                executor_key = (part.rank, worker_id)
                if executor_key in executor_keys:
                    raise ValueError(
                        f"duplicate {side} executor rank and worker: {executor_key}"
                    )
                executor_keys.add(executor_key)
                fragment_ids = tuple(
                    fragment.fragment_id for fragment in ordered_fragments
                )
                observed_selected_fragment_ids.update(fragment_ids)
                result.append(
                    ExecutorTransferPlan(
                        instance_id=binding.instance_id,
                        placement_id=placement.placement_id,
                        participant_id=part.participant_id,
                        placement_digest=placement.digest,
                        runtime_lease_id=binding.lease_id,
                        worker_id=worker_id,
                        rank=part.rank,
                        fragment_ids=fragment_ids,
                        fragment_snapshots=tuple(
                            RuntimeFragmentSnapshot.from_fragment(fragment)
                            for fragment in ordered_fragments
                        ),
                        attestation=attestation,
                    )
                )
    result.sort(
        key=lambda item: (item.rank.dp, item.rank.pp, item.rank.ep, item.rank.tp)
    )
    if selected_fragment_ids is not None and observed_selected_fragment_ids != set(
        selected_fragment_ids
    ):
        raise ValueError(f"missing selected {side} runtime fragment")
    return tuple(result)


def _selected_runtime_fragment_ids(
    operations: Sequence[ExecutableTransferOperation],
    side: str,
) -> frozenset[RuntimeFragmentId]:
    if side not in ("source", "target"):
        raise ValueError(f"invalid executor side: {side}")
    selected: set[RuntimeFragmentId] = set()
    for operation in operations:
        fragment = operation.source if side == "source" else operation.target
        if isinstance(fragment, BoundWeightFragment):
            selected.add(fragment.fragment_id)
    return frozenset(selected)


def bind_logical_transfer_plan(
    logical_plan: LogicalTransferPlan,
    target_bindings: Sequence[WeightRuntimeBindingManifest],
    *,
    source_bindings: Sequence[WeightRuntimeBindingManifest] = (),
    source_manifest: StoredWeightManifest | None = None,
) -> TransferPlan:
    """Bind an address-free logical plan to validated runtime locations."""

    if not isinstance(logical_plan, LogicalTransferPlan):
        raise ValueError("logical_plan must be a LogicalTransferPlan")
    # Re-check the immutable plan contract at the public binding boundary. This
    # protects deserialized or otherwise reconstructed plan values as well.
    _validate_logical_target_coverage(logical_plan)
    authoritative_source_manifest = _validated_store_source_manifest(
        logical_plan,
        source_manifest,
    )
    _validate_logical_tensor_snapshots(logical_plan, authoritative_source_manifest)
    _validate_logical_fragment_snapshots(logical_plan, authoritative_source_manifest)
    target_required = _required_participants(logical_plan.target_executors)
    source_required = _required_participants(logical_plan.source_executors)
    target_binding_by_id = _validated_bindings_by_placement_id(
        (logical_plan.target_placement,),
        target_bindings,
        "target",
        target_required,
    )
    source_binding_by_id = _validated_bindings_by_placement_id(
        (
            (logical_plan.source_placement,)
            if logical_plan.source_placement is not None
            else ()
        ),
        source_bindings,
        "source",
        source_required,
    )
    runtime_targets = _bound_fragments_by_placement_fragment_id(
        (logical_plan.target_placement,),
        target_binding_by_id,
        "target",
    )
    runtime_sources = (
        _bound_fragments_by_placement_fragment_id(
            (
                (logical_plan.source_placement,)
                if logical_plan.source_placement is not None
                else ()
            ),
            source_binding_by_id,
            "source",
        )
        if source_binding_by_id
        else {}
    )

    operations: list[ExecutableTransferOperation] = []
    for operation in logical_plan.operations:
        placement_source = operation.source
        placement_target = operation.target
        if not isinstance(placement_target, PlacementFragment):
            raise ValueError("logical transfer operation target is runtime-bound")
        runtime_target = runtime_targets.get(placement_target.placement_fragment_id)
        if runtime_target is None:
            raise ValueError(
                "missing runtime binding for placement fragment: "
                f"{placement_target.placement_fragment_id}"
            )
        if isinstance(placement_source, PlacementFragment):
            runtime_source = runtime_sources.get(placement_source.placement_fragment_id)
            if runtime_source is None:
                raise ValueError(
                    "missing source runtime binding for placement fragment: "
                    f"{placement_source.placement_fragment_id}"
                )
        elif isinstance(placement_source, StoredFragmentSnapshot):
            runtime_source = placement_source
        else:
            raise ValueError("logical transfer operation source is runtime-bound")
        operations.append(_bind_operation(operation, runtime_source, runtime_target))

    # Each logical tensor remains an explicit operation. A physical alias may
    # share an allocation only after the target-range validator proves the same
    # complete alias group and attested lease scope.
    bound_operations = tuple(operations)
    _validate_target_physical_ranges(
        bound_operations,
        max_segment_checks=logical_plan.planning_limits.max_total_lowered_segments,
    )
    target_binding_values = tuple(target_binding_by_id.values())
    source_binding_values = tuple(source_binding_by_id.values())
    source_placements: tuple[WeightPlacementManifest, ...] = ()
    if source_binding_values:
        source_placement = logical_plan.source_placement
        if source_placement is None:
            raise ValueError("source runtime bindings require a source placement")
        source_placements = (source_placement,)

    return TransferPlan(
        resource_id=logical_plan.resource_id,
        revision=logical_plan.revision,
        weight_generation=logical_plan.target_placement.weight_generation,
        target_placement=logical_plan.target_placement,
        operations=bound_operations,
        planning_limits=logical_plan.planning_limits,
        source_manifest=authoritative_source_manifest,
        source_manifest_identity=logical_plan.source_manifest_identity,
        source_executors=(
            _build_executor_plans(
                source_placements,
                source_binding_values,
                "source",
                selected_fragment_ids=_selected_runtime_fragment_ids(
                    bound_operations,
                    "source",
                ),
            )
            if source_binding_values
            else ()
        ),
        target_executors=_build_executor_plans(
            (logical_plan.target_placement,),
            target_binding_values,
            "target",
            selected_fragment_ids=_selected_runtime_fragment_ids(
                bound_operations,
                "target",
            ),
        ),
    )


def resolve_executor_plans(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    side: str,
) -> tuple[ExecutorTransferPlan, ...]:
    if side == "source":
        executors = plan.source_executors
    elif side == "target":
        executors = plan.target_executors
    else:
        raise ValueError(f"invalid executor side: {side}")
    if not executors:
        raise ValueError(f"transfer plan has no {side} executor metadata")
    if (
        plan.resource_id != placement.resource_id
        or plan.revision != placement.revision
        or plan.weight_generation != placement.weight_generation
    ):
        raise ValueError(f"transfer plan identity differs from {side} placement")
    validate_runtime_binding(placement, binding)
    expected_executors = tuple(
        executor
        for executor in executors
        if executor.instance_id == binding.instance_id
        and executor.participant_id == binding.participant_id
    )
    if not expected_executors:
        raise ValueError(f"{side} executor snapshot mismatch: unknown instance")
    expected_fragment_ids = frozenset(
        fragment_id
        for executor in expected_executors
        for fragment_id in executor.fragment_ids
    )
    try:
        current_executors = _build_executor_plans(
            (placement,),
            (binding,),
            side,
            selected_fragment_ids=expected_fragment_ids,
        )
    except ValueError as error:
        if str(error) == f"missing selected {side} runtime fragment":
            raise ValueError(f"{side} executor snapshot mismatch") from error
        raise
    executor_keys = [
        (executor.rank, executor.worker_id) for executor in expected_executors
    ]
    if len(executor_keys) != len(set(executor_keys)):
        raise ValueError(f"{side} executor snapshot has duplicate rank and worker")
    if current_executors != expected_executors:
        raise ValueError(f"{side} executor snapshot mismatch")
    return expected_executors


def resolve_executor_plan(
    plan: TransferPlan,
    placement: WeightPlacementManifest,
    binding: WeightRuntimeBindingManifest,
    side: str,
) -> ExecutorTransferPlan:
    executors = resolve_executor_plans(plan, placement, binding, side)
    if len(executors) != 1:
        raise ValueError(f"{side} executor snapshot contains multiple ranks")
    return executors[0]


__all__ = [
    "bind_logical_transfer_plan",
    "resolve_executor_plan",
    "resolve_executor_plans",
]
