"""Optional process-local cache of address-free local-target weight plans."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import NamedTuple, Optional

from ..contracts import ParticipantId
from .placement import WeightPlacementManifest
from .planner import (
    LogicalTransferPlan,
    PlanningLimits,
    plan_placement_transfer_to_local_target,
)
from .serde import weight_placement_from_json, weight_placement_to_json


class PlanCacheInfo(NamedTuple):
    hits: int
    misses: int
    maxsize: int
    currsize: int


class WeightTransferPlanCache:
    """Bound retained logical plans by entry count, with caching off by default."""

    def __init__(self, enabled: bool = False, max_entries: int = 2) -> None:
        if type(enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        if type(max_entries) is not int or max_entries <= 0:
            raise ValueError("max_entries must be a positive integer")
        self.enabled = enabled
        self.max_entries = max_entries

        @lru_cache(maxsize=max_entries)
        def cached_plan(
            source_json: str,
            target_json: str,
            participant_id: Optional[ParticipantId],
            limits: PlanningLimits,
        ) -> LogicalTransferPlan:
            return plan_placement_transfer_to_local_target(
                weight_placement_from_json(source_json),
                weight_placement_from_json(target_json),
                participant_id,
                planning_limits=limits,
            )

        self._cached_plan = cached_plan

    @classmethod
    def from_environment(cls) -> WeightTransferPlanCache:
        """Read cache configuration explicitly, without import-time side effects."""
        enabled = os.environ.get("MC_RESHARD_PLAN_CACHE_ENABLED", "0")
        if enabled not in ("0", "1"):
            raise ValueError("MC_RESHARD_PLAN_CACHE_ENABLED must be 0 or 1")
        raw_capacity = os.environ.get("MC_RESHARD_PLAN_CACHE_MAX_ENTRIES", "2")
        if not raw_capacity.isascii() or not raw_capacity.isdecimal():
            raise ValueError(
                "MC_RESHARD_PLAN_CACHE_MAX_ENTRIES must be a positive integer"
            )
        return cls(enabled=enabled == "1", max_entries=int(raw_capacity))

    def plan(
        self,
        source_placement: WeightPlacementManifest,
        target_placement: WeightPlacementManifest,
        target_participant_id: Optional[ParticipantId] = None,
        *,
        planning_limits: Optional[PlanningLimits] = None,
    ) -> LogicalTransferPlan:
        if not self.enabled:
            return plan_placement_transfer_to_local_target(
                source_placement,
                target_placement,
                target_participant_id,
                planning_limits=planning_limits,
            )
        if planning_limits is not None and not isinstance(
            planning_limits, PlanningLimits
        ):
            raise ValueError("planning_limits must be PlanningLimits")
        limits = (
            PlanningLimits(
                max_transfer_regions=planning_limits.max_transfer_regions,
                max_segments_per_region=planning_limits.max_segments_per_region,
                max_total_lowered_segments=planning_limits.max_total_lowered_segments,
            )
            if planning_limits is not None
            else PlanningLimits()
        )
        return self._cached_plan(
            weight_placement_to_json(source_placement),
            weight_placement_to_json(target_placement),
            target_participant_id,
            limits,
        )

    def cache_info(self) -> PlanCacheInfo:
        """Return standard LRU hit, miss, capacity and retained-entry counts."""
        info = self._cached_plan.cache_info()
        return PlanCacheInfo(info.hits, info.misses, self.max_entries, info.currsize)

    def clear(self) -> None:
        """Drop retained plans and reset LRU statistics."""
        self._cached_plan.cache_clear()
