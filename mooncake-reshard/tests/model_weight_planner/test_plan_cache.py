from __future__ import annotations

from dataclasses import fields
from concurrent.futures import ThreadPoolExecutor
import gc
import weakref

import pytest

from mooncake.reshard.weight import plan_cache
from mooncake.reshard.weight.plan_cache import WeightTransferPlanCache
from mooncake.reshard.weight.planner import PlanningLimits
from mooncake.reshard.weight.serde import (
    weight_placement_from_json,
    weight_placement_to_json,
)

from .helpers import global_placement_from_fragments, tp_manifests


def placements(tp=2, address_base=0x10000):
    return tuple(
        tp_manifests(
            tp=tp,
            pp_rank=0,
            ep_rank=0,
            address_base=address_base,
            worker_prefix=prefix,
        ).placement
        for prefix in ("source", "target")
    )


def test_disabled_has_no_serialization_or_retention(monkeypatch):
    source, target = placements()
    participant = target.parts[0].participant_id
    expected = plan_cache.plan_placement_transfer_to_local_target(
        source, target, participant
    )

    def unexpected(*args):
        pytest.fail("disabled cache serialized a placement")

    monkeypatch.setattr(plan_cache, "weight_placement_to_json", unexpected)
    cache = WeightTransferPlanCache()
    assert cache.plan(source, target, participant) == expected
    assert cache.cache_info() == (0, 0, 2, 0)


def test_enabled_equivalent_and_canonical_roundtrip_hits():
    source, target = placements()
    participant = target.parts[0].participant_id
    cache = WeightTransferPlanCache(enabled=True)
    first = cache.plan(source, target, participant)
    assert first == plan_cache.plan_placement_transfer_to_local_target(
        source, target, participant
    )
    copied = weight_placement_from_json(weight_placement_to_json(source))
    assert cache.plan(copied, target, participant) is first
    assert cache.cache_info() == (1, 1, 2, 1)


def test_lru_eviction_and_clear():
    source, target = placements(tp=4)
    cache = WeightTransferPlanCache(enabled=True, max_entries=2)
    participants = [part.participant_id for part in target.parts]
    first = cache.plan(source, target, participants[0])
    cache.plan(source, target, participants[1])
    assert cache.plan(source, target, participants[0]) is first
    cache.plan(source, target, participants[2])
    assert cache.cache_info() == (1, 3, 2, 2)
    cache.plan(source, target, participants[1])
    assert cache.cache_info() == (1, 4, 2, 2)
    cache.clear()
    assert cache.cache_info() == (0, 0, 2, 0)


@pytest.mark.parametrize("field", [field.name for field in fields(PlanningLimits)])
def test_every_planning_limit_is_in_key(field):
    source, target = placements(tp=1)
    cache = WeightTransferPlanCache(enabled=True)
    first = cache.plan(source, target)
    assert cache.plan(source, target, planning_limits=PlanningLimits()) is first
    defaults = PlanningLimits()
    limits = PlanningLimits(**{field: getattr(defaults, field) + 1})
    assert cache.plan(source, target, planning_limits=limits) is not first
    assert cache.cache_info() == (1, 2, 2, 2)


def test_limits_are_canonical_and_do_not_retain_caller_objects():
    class CallerLimits(PlanningLimits):
        def __hash__(self):
            pytest.fail("caller limits hash must not be used")

    class RuntimeObject:
        pass

    source, target = placements(tp=1)
    limits = CallerLimits()
    runtime = RuntimeObject()
    reference = weakref.ref(runtime)
    limits_reference = weakref.ref(limits)
    object.__setattr__(limits, "runtime_tensor", runtime)
    cache = WeightTransferPlanCache(enabled=True)
    first = cache.plan(source, target, planning_limits=limits)
    del runtime, limits
    gc.collect()
    assert reference() is None
    assert limits_reference() is None
    assert cache.plan(source, target, planning_limits=PlanningLimits()) is first


@pytest.mark.parametrize("invalid", [True, 0, -1, "1", None])
def test_enabled_cache_revalidates_limits_values(invalid):
    source, target = placements(tp=1)
    limits = PlanningLimits()
    object.__setattr__(limits, "max_transfer_regions", invalid)
    cache = WeightTransferPlanCache(enabled=True)
    with pytest.raises(ValueError, match="positive integer"):
        cache.plan(source, target, planning_limits=limits)
    assert cache.cache_info().currsize == 0


def test_enabled_cache_rejects_non_limits():
    source, target = placements(tp=1)
    with pytest.raises(ValueError, match="planning_limits"):
        WeightTransferPlanCache(enabled=True).plan(source, target, planning_limits=True)


@pytest.mark.parametrize(
    "field", ["resource_id", "revision", "weight_generation", "placement_set_id"]
)
def test_identity_change_misses(field):
    source, target = placements(tp=1)
    cache = WeightTransferPlanCache(enabled=True)
    cache.plan(source, target)
    changed = []
    for placement in (source, target):
        values = dict(
            resource_id=placement.resource_id,
            revision=placement.revision,
            weight_generation=placement.weight_generation,
            placement_set_id=placement.placement_set_id,
            tensors=placement.tensors,
            fragments=placement.fragments,
            participant_ids={
                p.rank: p.participant_id for p in placement.topology.participants
            },
        )
        values[field] = 2 if field == "weight_generation" else "new-identity"
        changed.append(global_placement_from_fragments(**values))
    cache.plan(*changed)
    assert cache.cache_info().misses == 2


def test_stale_digest_does_not_hide_mutation_and_extra_objects_are_not_retained():
    source, target = placements(tp=1)
    cache = WeightTransferPlanCache(enabled=True)
    original_digest = source.digest
    extra = object()
    object.__setattr__(source, "extra_runtime_tensor", extra)
    first = cache.plan(source, target)
    changed_source, changed_target = placements(tp=2)
    object.__setattr__(changed_source, "_digest_cache", original_digest)
    assert changed_source.digest == original_digest
    second = cache.plan(
        changed_source, changed_target, changed_target.parts[0].participant_id
    )
    assert second is not first
    assert cache.cache_info().misses == 2
    # The serde snapshot must not keep caller-owned objects attached to placement.
    assert first.source_placement is not source
    assert not hasattr(first.source_placement, "extra_runtime_tensor")


def test_runtime_address_changes_do_not_change_logical_key():
    source, target = placements(tp=1)
    rebound_source, rebound_target = placements(tp=1, address_base=0x90000)
    cache = WeightTransferPlanCache(enabled=True)
    first = cache.plan(source, target)
    assert cache.plan(rebound_source, rebound_target) is first


def test_invalid_input_is_not_retained():
    source, target = placements(tp=1)
    cache = WeightTransferPlanCache(enabled=True)
    cache.plan(source, target)
    object.__setattr__(source, "weight_generation", 2)
    with pytest.raises(ValueError):
        cache.plan(source, target)
    assert cache.cache_info().currsize == 1


def test_concurrent_requests_leave_bounded_reusable_entries():
    source, target = placements(tp=2)
    cache = WeightTransferPlanCache(enabled=True, max_entries=1)
    participant = target.parts[0].participant_id
    with ThreadPoolExecutor(max_workers=4) as executor:
        plans = list(
            executor.map(lambda _: cache.plan(source, target, participant), range(12))
        )
    assert all(plan == plans[0] for plan in plans)
    assert cache.cache_info().currsize == 1
    before = cache.cache_info().hits
    cache.plan(source, target, participant)
    assert cache.cache_info().hits == before + 1


def test_environment_defaults_and_explicit_configuration(monkeypatch):
    monkeypatch.delenv("MC_RESHARD_PLAN_CACHE_ENABLED", raising=False)
    monkeypatch.delenv("MC_RESHARD_PLAN_CACHE_MAX_ENTRIES", raising=False)
    default = WeightTransferPlanCache.from_environment()
    assert not default.enabled
    assert default.max_entries == 2
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", "1")
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_MAX_ENTRIES", "2")
    configured = WeightTransferPlanCache.from_environment()
    assert configured.enabled
    assert configured.max_entries == 2


@pytest.mark.parametrize("value", ["", "true", "2", " 1"])
def test_invalid_environment_switch_only_affects_explicit_factory(monkeypatch, value):
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", value)
    source, target = placements(tp=1)
    WeightTransferPlanCache().plan(source, target)
    with pytest.raises(ValueError, match="ENABLED"):
        WeightTransferPlanCache.from_environment()


@pytest.mark.parametrize("value", ["", "0", "-1", "1.5", " 2", "true", "２"])
def test_invalid_environment_capacity(monkeypatch, value):
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_ENABLED", "0")
    monkeypatch.setenv("MC_RESHARD_PLAN_CACHE_MAX_ENTRIES", value)
    with pytest.raises(ValueError, match="positive integer"):
        WeightTransferPlanCache.from_environment()


@pytest.mark.parametrize(
    "kwargs", [{"enabled": 1}, {"max_entries": True}, {"max_entries": 0}]
)
def test_invalid_constructor_configuration(kwargs):
    with pytest.raises(ValueError):
        WeightTransferPlanCache(**kwargs)
