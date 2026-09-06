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
    registered_sources,
    registration_map,
    same_runtime_snapshot,
    validate_registration,
)
from .lifetime import (
    WeightAllocationGuardProviders,
    acquire_weight_lifetime_tokens,
)
from ...transfer_engine.lifetime import AllocationTokenSet, TerminalTransferState
from ...transfer_engine.lifetime import AllocationLifetimeToken


@dataclass(frozen=True)
class DirectTransferReceipt:
    source_worker_id: str
    target_endpoint: str
    operation_count: int
    nbytes: int


class MooncakeTransferEngineSink:
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
        self.transfer_executor = MooncakeTransferEngineExecutor(
            engine,
            max_completion_drain_attempts=max_completion_drain_attempts,
            completion_drain_timeout_ms=completion_drain_timeout_ms,
        )
        self._pending = self.transfer_executor.pending_manager
        self._engine_identity = self._pending.engine_identity
        self.max_batch_operations = max_batch_operations
        self.max_region_segments = max_region_segments
        self.max_total_lowered_segments = max_total_lowered_segments
        self.max_completion_drain_attempts = max_completion_drain_attempts
        self.completion_drain_timeout_ms = completion_drain_timeout_ms

    def execute(
        self,
        plan: TransferPlan,
        source_placement: WeightPlacementManifest,
        source_binding: WeightRuntimeBindingManifest,
        target_placement: WeightPlacementManifest,
        target_bindings: Sequence[WeightRuntimeBindingManifest],
        *,
        source_worker_id: Optional[str] = None,
        target_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        source_pre_registered: bool = False,
        source_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        source_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        target_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        transfer_id: Optional[str] = None,
    ) -> tuple[DirectTransferReceipt, ...]:
        validate_execution_input_types(
            plan,
            source_placement,
            (source_binding,),
            target_placement,
            target_bindings,
        )
        # This preflight is diagnostic only. The executor repeats validation
        # after framework guards return a freshly pinned binding snapshot.
        validate_manifest_pair(plan, source_placement, source_binding, "source")
        planned_source_participant_ids = {
            executor.participant_id for executor in plan.source_executors
        }
        if source_binding.participant_id not in planned_source_participant_ids:
            return ()
        planned_source_keys = {
            (executor.instance_id, executor.participant_id)
            for executor in plan.source_executors
        }
        validate_selected_executor_snapshot(
            plan,
            source_placement,
            source_binding,
            "source",
        )
        for binding in target_bindings:
            validate_manifest_pair(plan, target_placement, binding, "target")
            validate_selected_executor_snapshot(
                plan,
                target_placement,
                binding,
                "target",
            )
        with self.transfer_executor.submission():
            transfer_id = transfer_id or uuid4().hex
            source_keys = planned_source_keys
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
                    bindings=(source_binding,)
                    if (source_binding.instance_id, source_binding.participant_id)
                    in source_keys
                    else (),
                    side="source",
                    providers=source_allocation_guards,
                )
                acquired_targets, target_tokens = acquire_weight_lifetime_tokens(
                    transfer_id=transfer_id,
                    plan=plan,
                    bindings=tuple(
                        binding
                        for binding in target_bindings
                        if (binding.instance_id, binding.participant_id) in target_keys
                    ),
                    side="target",
                    providers=target_allocation_guards,
                )
                lifetime_tokens = AllocationTokenSet(
                    (*source_tokens.tokens, *target_tokens.tokens)
                )
                if len(acquired_sources) != 1:
                    raise TransferEngineError(
                        "source allocation guards did not acquire one local binding"
                    )
                result = self._execute_reserved(
                    plan,
                    source_placement,
                    acquired_sources[0],
                    target_placement,
                    acquired_targets,
                    source_worker_id=source_worker_id,
                    target_registrations=target_registrations,
                    source_pre_registered=source_pre_registered,
                    source_registrations=source_registrations,
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
        source_binding: WeightRuntimeBindingManifest,
        target_placement: WeightPlacementManifest,
        target_bindings: Sequence[WeightRuntimeBindingManifest],
        *,
        source_worker_id: Optional[str] = None,
        target_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        source_pre_registered: bool = False,
        source_registrations: Optional[Sequence[MemoryRegistrationLease]] = None,
        lifetime_tokens: Optional[AllocationTokenSet] = None,
    ) -> tuple[DirectTransferReceipt, ...]:
        validate_manifest_pair(plan, source_placement, source_binding, "source")
        try:
            source_executors = resolve_runtime_executors(
                plan, source_placement, source_binding, "source"
            )
        except ValueError as error:
            raise TransferEngineError(str(error)) from error
        if source_worker_id is not None:
            source_executors = tuple(
                executor
                for executor in source_executors
                if executor.worker_id == source_worker_id
            )
        source_workers = {executor.worker_id for executor in source_executors}
        if len(source_workers) != 1:
            raise TransferEngineError(
                "source binding requires exactly one local worker executor"
            )
        local_source_worker_id = next(iter(source_workers))

        targets: dict[RuntimeFragmentId, RuntimeBindingFragment] = {}
        target_runtime_lease_ids: dict[RuntimeFragmentId, LeaseId] = {}
        target_generations: dict[RuntimeFragmentId, int] = {}
        target_executor_keys: set[tuple[ParallelRank, str]] = set()
        expected_target_participants = {
            executor.participant_id for executor in plan.target_executors
        }
        target_pairs = pair_manifests(target_placement, target_bindings, "target")
        for placement, binding in target_pairs:
            validate_manifest_pair(plan, placement, binding, "target")
            if binding.participant_id not in expected_target_participants:
                continue
            try:
                executors = resolve_runtime_executors(
                    plan, placement, binding, "target"
                )
            except ValueError as error:
                raise TransferEngineError(str(error)) from error
            for executor in executors:
                executor_key = (executor.rank, executor.worker_id)
                if executor_key in target_executor_keys:
                    raise TransferEngineError(
                        f"duplicate target executor rank and worker: {executor_key}"
                    )
                target_executor_keys.add(executor_key)
            for fragment in binding.fragments:
                if fragment.fragment_id in targets:
                    raise TransferEngineError(
                        f"duplicate target fragment: {fragment.fragment_id}"
                    )
                targets[fragment.fragment_id] = fragment
                target_runtime_lease_ids[fragment.fragment_id] = binding.lease_id
                target_generations[fragment.fragment_id] = binding.generation
        expected_target_executor_keys = {
            (executor.rank, executor.worker_id) for executor in plan.target_executors
        }
        if target_executor_keys != expected_target_executor_keys:
            raise TransferEngineError("target executor set is incomplete")

        local_operations = [
            require_live_transfer_operation(plan.operations[index])
            for executor in source_executors
            for index in plan.operation_indices_for_executor(executor, "source")
        ]
        if not local_operations:
            return ()

        local = {
            fragment.fragment_id: fragment for fragment in source_binding.fragments
        }
        target_registration_by_id = registration_map(
            target_registrations,
            "target",
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
        used_sources: dict[RuntimeFragmentId, RuntimeBindingFragment] = {}
        for operation in local_operations:
            planned_source = runtime_binding_fragment(operation.source)
            planned_target = runtime_binding_fragment(operation.target)
            current = local.get(planned_source.fragment_id)
            if current is None or not same_runtime_snapshot(current, planned_source):
                raise TransferEngineError(
                    f"stale source fragment: {planned_source.fragment_id}"
                )
            target = targets.get(planned_target.fragment_id)
            if target is None:
                raise TransferEngineError(
                    f"missing planned target fragment: {planned_target.fragment_id}"
                )
            if not same_runtime_snapshot(target, planned_target):
                raise TransferEngineError(
                    f"stale target fragment: {planned_target.fragment_id}"
                )
            if not target.endpoint:
                raise TransferEngineError(
                    f"target endpoint is empty: {operation.target.fragment_id}"
                )
            try:
                operation.validate_bounds()
            except ValueError as error:
                raise TransferEngineError(
                    f"invalid copy range for {operation.tensor_id}: {error}"
                ) from error
            if operation.repeat > self.max_region_segments:
                raise TransferEngineError(
                    f"transfer region exceeds max_region_segments: "
                    f"{operation.tensor_id}: {operation.repeat} > "
                    f"{self.max_region_segments}"
                )
            validate_registration(
                target,
                target_registration_by_id,
                "target",
                lease_generation=target_generations[target.fragment_id],
                runtime_lease_id=target_runtime_lease_ids[target.fragment_id],
            )
            used_sources[current.fragment_id] = current
            operations_by_endpoint.setdefault(target.endpoint, []).append(
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

        with registered_sources(
            self.engine,
            self,
            tuple(used_sources.values()),
            pre_registered=source_pre_registered,
            registrations=source_registrations,
            lease_generation=source_binding.generation,
            runtime_lease_id=source_binding.lease_id,
            resources=(
                source_placement,
                source_binding,
                target_placement,
                tuple(target_bindings),
                source_registrations,
                target_registrations,
            ),
            lifetime_tokens=lifetime_tokens,
        ):
            receipts: list[DirectTransferReceipt] = []
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
                    DirectTransferReceipt(
                        source_worker_id=local_source_worker_id,
                        target_endpoint=endpoint,
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
            TransferDirection.WRITE,
        )

    def _retain_pending_ticket(self, ticket: Any) -> str:
        return self._pending._retain_pending_ticket(ticket)

    def _retain_pending_resources(
        self,
        pending_transfer_id: str,
        *,
        registrations: Sequence[int],
        resources: Sequence[Any],
        allocation_tokens: Sequence[AllocationLifetimeToken] = (),
    ) -> None:
        self._pending._retain_pending_resources(
            pending_transfer_id,
            registrations=registrations,
            resources=resources,
            allocation_tokens=allocation_tokens,
        )

    def _reserve_submission(self) -> None:
        self._pending._reserve_submission()

    def _release_submission(self) -> None:
        self._pending._release_submission()

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
