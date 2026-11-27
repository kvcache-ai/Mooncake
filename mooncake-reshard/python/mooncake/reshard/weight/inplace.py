"""Explicit same-participant live weight reshard execution."""

from __future__ import annotations

from collections.abc import Sequence

from .manifest import WeightPlacementManifest, WeightRuntimeBindingManifest
from .plan_cache import WeightTransferPlanCache
from .planner import PlanningLimits, bind_logical_transfer_plan
from .te import (
    DirectReadReceipt,
    MemoryRegistrationLease,
    MooncakeTransferEngineReader,
    WeightAllocationGuardProviders,
)


class InplaceWeightResharder:
    """Plan and freshly bind reads within one existing participant fleet.

    The framework must stop source writes, clear KV state, and provision target
    allocations without overwriting source data still needed by any reader.
    Participant identity alone does not establish physical GPU identity or a
    safe overlapping-memory schedule. The reader retains its lifetime and
    completion-unknown quarantine contract; failures never trigger fallback.
    """

    def __init__(
        self,
        reader: MooncakeTransferEngineReader,
        *,
        plan_cache: WeightTransferPlanCache | None = None,
    ) -> None:
        if not isinstance(reader, MooncakeTransferEngineReader):
            raise ValueError("reader must be a MooncakeTransferEngineReader")
        if plan_cache is not None and not isinstance(
            plan_cache, WeightTransferPlanCache
        ):
            raise ValueError("plan_cache must be a WeightTransferPlanCache")
        self.reader = reader
        self.plan_cache = (
            WeightTransferPlanCache.from_environment()
            if plan_cache is None
            else plan_cache
        )

    def execute(
        self,
        source_placement: WeightPlacementManifest,
        source_bindings: Sequence[WeightRuntimeBindingManifest],
        target_placement: WeightPlacementManifest,
        target_binding: WeightRuntimeBindingManifest,
        *,
        planning_limits: PlanningLimits | None = None,
        target_worker_id: str | None = None,
        source_pre_registered: bool = True,
        source_registrations: Sequence[MemoryRegistrationLease] | None = None,
        target_pre_registered: bool = False,
        target_registrations: Sequence[MemoryRegistrationLease] | None = None,
        source_allocation_guards: WeightAllocationGuardProviders | None = None,
        target_allocation_guards: WeightAllocationGuardProviders | None = None,
        transfer_id: str | None = None,
    ) -> tuple[DirectReadReceipt, ...]:
        """Execute one local target using current bindings and framework guards."""
        if not isinstance(source_placement, WeightPlacementManifest) or not isinstance(
            target_placement, WeightPlacementManifest
        ):
            raise ValueError("inplace reshard requires typed complete placements")
        if not isinstance(target_binding, WeightRuntimeBindingManifest):
            raise ValueError("target_binding must be a WeightRuntimeBindingManifest")
        source_participants = {
            participant.participant_id
            for participant in source_placement.topology.participants
        }
        target_participants = {
            participant.participant_id
            for participant in target_placement.topology.participants
        }
        if source_participants != target_participants:
            raise ValueError("inplace reshard requires the same participant set")
        logical_plan = self.plan_cache.plan(
            source_placement,
            target_placement,
            target_binding.participant_id,
            planning_limits=planning_limits,
        )
        plan = bind_logical_transfer_plan(
            logical_plan,
            (target_binding,),
            source_bindings=source_bindings,
        )
        return self.reader.execute(
            plan,
            source_placement,
            source_bindings,
            target_placement,
            target_binding,
            target_worker_id=target_worker_id,
            source_pre_registered=source_pre_registered,
            source_registrations=source_registrations,
            target_pre_registered=target_pre_registered,
            target_registrations=target_registrations,
            source_allocation_guards=source_allocation_guards,
            target_allocation_guards=target_allocation_guards,
            transfer_id=transfer_id,
        )
