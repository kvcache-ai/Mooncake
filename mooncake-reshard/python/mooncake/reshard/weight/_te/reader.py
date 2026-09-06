from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence
from uuid import uuid4

from ...contracts import LeaseId, RuntimeFragmentId
from ...transfer_engine import (
    MooncakeTransferEngineExecutor,
    TransferCompletionFailedError,
    TransferBatch,
    TransferDirection,
    TransferEngineError,
)
from ..manifest import (
    ParallelRank,
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)
from ..planner import BoundWeightFragment, LiveTransferOperation, TransferPlan
from .batching import iter_transfer_batches
from .execution import (
    pair_manifests,
    resolve_runtime_executors,
    validate_selected_executor_snapshot,
    require_live_transfer_operation,
    runtime_binding_fragment,
    validate_execution_input_types,
    validate_lowering_budget,
    validate_lowering_limits,
    validate_manifest_pair,
)
from .registration import (
    MemoryRegistrationLease,
    registered_targets,
    registration_map,
    same_runtime_snapshot,
    validate_registration,
)
from .lifetime import (
    WeightAllocationGuardProviders,
    acquire_weight_lifetime_tokens,
)
from ...transfer_engine.lifetime import AllocationTokenSet, TerminalTransferState


@dataclass(frozen=True)
class DirectReadReceipt:
    source_endpoint: str
    target_worker_id: str
    operation_count: int
    nbytes: int


class MooncakeTransferEngineReader:
    """Execute a local target plan with target-initiated zero-copy reads."""

    def __init__(
        self,
        engine: Any,
        *,
        max_batch_operations: int = 1024,
        max_region_segments: int = 1_000_000,
        max_total_lowered_segments: int = 10_000_000,
        max_completion_drain_attempts: int = 3,
        completion_drain_timeout_ms: int = 1000,
    ) -> None:
        validate_lowering_limits(
            max_batch_operations=max_batch_operations,
            max_region_segments=max_region_segments,
            max_total_lowered_segments=max_total_lowered_segments,
            max_completion_drain_attempts=max_completion_drain_attempts,
            completion_drain_timeout_ms=completion_drain_timeout_ms,
        )
        self.engine = engine
        self.max_batch_operations = max_batch_operations
        self.max_region_segments = max_region_segments
        self.max_total_lowered_segments = max_total_lowered_segments
        self.max_completion_drain_attempts = max_completion_drain_attempts
        self.completion_drain_timeout_ms = completion_drain_timeout_ms
        self.transfer_executor = MooncakeTransferEngineExecutor(
            engine,
            max_completion_drain_attempts=max_completion_drain_attempts,
            completion_drain_timeout_ms=completion_drain_timeout_ms,
        )
        self._pending = self.transfer_executor.pending_manager

    def execute(
        self,
        plan: TransferPlan,
        source_placement: WeightPlacementManifest,
        source_bindings: Sequence[WeightRuntimeBindingManifest],
        target_placement: WeightPlacementManifest,
        target_binding: WeightRuntimeBindingManifest,
        *,
        target_worker_id: Optional[str] = None,
        source_pre_registered: bool = True,
        source_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        target_pre_registered: bool = False,
        target_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        source_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        target_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        transfer_id: Optional[str] = None,
    ) -> tuple[DirectReadReceipt, ...]:
        validate_execution_input_types(
            plan,
            source_placement,
            source_bindings,
            target_placement,
            (target_binding,),
        )
        # This preflight is diagnostic only. The executor repeats validation
        # after framework guards return a freshly pinned binding snapshot.
        validate_manifest_pair(plan, target_placement, target_binding, "target")
        validate_selected_executor_snapshot(
            plan,
            target_placement,
            target_binding,
            "target",
        )
        for binding in source_bindings:
            validate_manifest_pair(plan, source_placement, binding, "source")
            validate_selected_executor_snapshot(
                plan,
                source_placement,
                binding,
                "source",
            )
        with self.transfer_executor.submission():
            transfer_id = transfer_id or uuid4().hex
            source_keys = {
                (executor.instance_id, executor.participant_id)
                for executor in plan.source_executors
            }
            target_keys = {
                (executor.instance_id, executor.participant_id)
                for executor in plan.target_executors
            }
            source_tokens: Optional[AllocationTokenSet] = None
            target_tokens: Optional[AllocationTokenSet] = None
            lifetime_tokens: Optional[AllocationTokenSet] = None
            terminal_state = TerminalTransferState.ABORTED
            try:
                acquired_sources, source_tokens = acquire_weight_lifetime_tokens(
                    transfer_id=transfer_id,
                    plan=plan,
                    bindings=tuple(
                        binding
                        for binding in source_bindings
                        if (binding.instance_id, binding.participant_id) in source_keys
                    ),
                    side="source",
                    providers=source_allocation_guards,
                )
                acquired_targets, target_tokens = acquire_weight_lifetime_tokens(
                    transfer_id=transfer_id,
                    plan=plan,
                    bindings=(target_binding,)
                    if (target_binding.instance_id, target_binding.participant_id)
                    in target_keys
                    else (),
                    side="target",
                    providers=target_allocation_guards,
                )
                lifetime_tokens = AllocationTokenSet(
                    (*source_tokens.tokens, *target_tokens.tokens)
                )
                if len(acquired_targets) != 1:
                    raise TransferEngineError(
                        "target allocation guards did not acquire one local binding"
                    )
                result = self._execute_reserved(
                    plan,
                    source_placement,
                    acquired_sources,
                    target_placement,
                    acquired_targets[0],
                    target_worker_id=target_worker_id,
                    source_pre_registered=source_pre_registered,
                    source_registrations=source_registrations,
                    target_pre_registered=target_pre_registered,
                    target_registrations=target_registrations,
                    lifetime_tokens=lifetime_tokens,
                )
                terminal_state = TerminalTransferState.COMPLETED
                return result
            except TransferCompletionFailedError:
                terminal_state = TerminalTransferState.FAILED_DRAINED
                raise
            finally:
                if lifetime_tokens is not None:
                    lifetime_tokens.release_after_terminal(terminal_state)
                else:
                    if target_tokens is not None:
                        target_tokens.release_after_terminal(terminal_state)
                    if source_tokens is not None:
                        source_tokens.release_after_terminal(terminal_state)

    def _execute_reserved(
        self,
        plan: TransferPlan,
        source_placement: WeightPlacementManifest,
        source_bindings: Sequence[WeightRuntimeBindingManifest],
        target_placement: WeightPlacementManifest,
        target_binding: WeightRuntimeBindingManifest,
        *,
        target_worker_id: Optional[str] = None,
        source_pre_registered: bool = True,
        source_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        target_pre_registered: bool = False,
        target_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        lifetime_tokens: Optional[AllocationTokenSet] = None,
    ) -> tuple[DirectReadReceipt, ...]:
        if not source_pre_registered:
            raise TransferEngineError("remote source memory must be pre-registered")
        source_registration_by_id = registration_map(source_registrations, "source")
        if target_registrations is not None and not target_pre_registered:
            raise TransferEngineError(
                "target_registrations require target_pre_registered=True"
            )
        validate_manifest_pair(plan, target_placement, target_binding, "target")
        try:
            target_executors = resolve_runtime_executors(
                plan, target_placement, target_binding, "target"
            )
        except ValueError as error:
            raise TransferEngineError(str(error)) from error
        if target_worker_id is not None:
            target_executors = tuple(
                executor
                for executor in target_executors
                if executor.worker_id == target_worker_id
            )
        if len(target_executors) != 1:
            raise TransferEngineError(
                "target binding requires exactly one local worker executor"
            )
        target_executor = target_executors[0]

        sources: dict[RuntimeFragmentId, RuntimeBindingFragment] = {}
        source_runtime_lease_ids: dict[RuntimeFragmentId, LeaseId] = {}
        source_generations: dict[RuntimeFragmentId, int] = {}
        source_executor_keys: set[tuple[ParallelRank, str]] = set()
        expected_source_participants = {
            executor.participant_id for executor in plan.source_executors
        }
        source_pairs = pair_manifests(source_placement, source_bindings, "source")
        for placement, binding in source_pairs:
            validate_manifest_pair(plan, placement, binding, "source")
            if binding.participant_id not in expected_source_participants:
                continue
            try:
                executors = resolve_runtime_executors(
                    plan, placement, binding, "source"
                )
            except ValueError as error:
                raise TransferEngineError(str(error)) from error
            for executor in executors:
                executor_key = (executor.rank, executor.worker_id)
                if executor_key in source_executor_keys:
                    raise TransferEngineError(
                        f"duplicate source executor rank and worker: {executor_key}"
                    )
                source_executor_keys.add(executor_key)
            for fragment in binding.fragments:
                if fragment.fragment_id in sources:
                    raise TransferEngineError(
                        f"duplicate source fragment: {fragment.fragment_id}"
                    )
                sources[fragment.fragment_id] = fragment
                source_runtime_lease_ids[fragment.fragment_id] = binding.lease_id
                source_generations[fragment.fragment_id] = binding.generation
        expected_source_executor_keys = {
            (executor.rank, executor.worker_id) for executor in plan.source_executors
        }
        if source_executor_keys != expected_source_executor_keys:
            raise TransferEngineError("source executor set is incomplete")

        targets = {
            fragment.fragment_id: fragment for fragment in target_binding.fragments
        }
        registration_by_id = (
            registration_map(target_registrations, "target")
            if target_pre_registered
            else {}
        )
        operations_by_endpoint: dict[
            str,
            list[
                tuple[
                    LiveTransferOperation,
                    BoundWeightFragment,
                    BoundWeightFragment,
                ]
            ],
        ] = {}
        used_targets: dict[RuntimeFragmentId, RuntimeBindingFragment] = {}
        for index in plan.operation_indices_for_executor(target_executor, "target"):
            operation = require_live_transfer_operation(plan.operations[index])
            planned_source = runtime_binding_fragment(operation.source)
            planned_target = runtime_binding_fragment(operation.target)
            source = sources.get(planned_source.fragment_id)
            target = targets.get(planned_target.fragment_id)
            if source is None or not same_runtime_snapshot(source, planned_source):
                raise TransferEngineError(
                    f"stale source fragment: {planned_source.fragment_id}"
                )
            runtime_lease_id = source_runtime_lease_ids[source.fragment_id]
            validate_registration(
                source,
                source_registration_by_id,
                "source",
                lease_generation=source_generations[source.fragment_id],
                runtime_lease_id=runtime_lease_id,
            )
            if target is None or not same_runtime_snapshot(target, planned_target):
                raise TransferEngineError(
                    f"stale target fragment: {planned_target.fragment_id}"
                )
            if target_pre_registered:
                validate_registration(
                    target,
                    registration_by_id,
                    "target",
                    lease_generation=target_binding.generation,
                    runtime_lease_id=target_binding.lease_id,
                )
            operation.validate_bounds()
            if operation.repeat > self.max_region_segments:
                raise TransferEngineError(
                    f"transfer region exceeds max_region_segments: "
                    f"{operation.tensor_id}: {operation.repeat} > "
                    f"{self.max_region_segments}"
                )
            used_targets[target.fragment_id] = target
            operations_by_endpoint.setdefault(source.endpoint, []).append(
                (operation, operation.source, operation.target)
            )

        validate_lowering_budget(
            tuple(
                operation
                for operations in operations_by_endpoint.values()
                for operation, _, _ in operations
            ),
            max_region_segments=self.max_region_segments,
            max_total_lowered_segments=self.max_total_lowered_segments,
        )

        with registered_targets(
            self.engine,
            self._pending,
            tuple(used_targets.values()),
            pre_registered=target_pre_registered,
            resources=(
                source_placement,
                tuple(source_bindings),
                target_placement,
                target_binding,
                source_registrations,
                target_registrations,
            ),
            lifetime_tokens=lifetime_tokens,
        ):
            receipts: list[DirectReadReceipt] = []
            for endpoint in sorted(operations_by_endpoint):
                operations = sorted(
                    operations_by_endpoint[endpoint],
                    key=lambda item: (
                        item[2].address + item[0].target_offset,
                        item[1].address + item[0].source_offset,
                    ),
                )
                operation_count = 0
                total_bytes = 0
                for batch in iter_transfer_batches(
                    endpoint,
                    operations,
                    max_batch_operations=self.max_batch_operations,
                    max_region_segments=self.max_region_segments,
                ):
                    self._transfer_batch(batch)
                    operation_count += batch.operation_count
                    total_bytes += batch.nbytes
                receipts.append(
                    DirectReadReceipt(
                        source_endpoint=endpoint,
                        target_worker_id=target_executor.worker_id,
                        operation_count=operation_count,
                        nbytes=total_bytes,
                    )
                )
        return tuple(receipts)

    def _transfer_batch(
        self,
        batch: TransferBatch,
    ) -> None:
        self.transfer_executor._execute_reserved_batch(
            batch,
            TransferDirection.READ,
        )

    def pending_transfer_ids(self) -> tuple[str, ...]:
        return self._pending.pending_transfer_ids()

    def pending_transfer_status(self, pending_transfer_id: str) -> str:
        return self._pending.pending_transfer_status(pending_transfer_id)

    def drain_pending_transfer(
        self,
        pending_transfer_id: str,
        *,
        timeout_ms: int = 1000,
    ) -> str:
        return self._pending.drain_pending_transfer(
            pending_transfer_id,
            timeout_ms=timeout_ms,
        )
