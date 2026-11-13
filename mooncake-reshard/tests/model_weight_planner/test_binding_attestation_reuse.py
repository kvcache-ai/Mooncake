from __future__ import annotations

from copy import copy
import importlib
import pickle

import pytest

from mooncake.reshard.weight import (
    bind_logical_transfer_plan,
    plan_placement_transfer,
)
from mooncake.reshard.weight._planner import attestation, binding

from .helpers import tp_manifests


def _inputs():
    return tuple(
        tp_manifests(
            tp=1,
            pp_rank=rank,
            ep_rank=0,
            address_base=base,
            worker_prefix=side,
        )
        for rank, base, side in ((0, 0x10000, "source"), (1, 0x40000, "target"))
    )


def _count_validation(monkeypatch):
    counts = {"validation": 0, "evidence": 0}
    runtime_binding = importlib.import_module("mooncake.reshard.weight.binding")
    original_validate = runtime_binding.validate_runtime_binding
    original_evidence = attestation.RuntimeBindingEvidence.from_binding.__func__

    def validate(*args, **kwargs):
        counts["validation"] += 1
        return original_validate(*args, **kwargs)

    def evidence(cls, *args, **kwargs):
        counts["evidence"] += 1
        return original_evidence(cls, *args, **kwargs)

    for module in (runtime_binding, binding, attestation):
        monkeypatch.setattr(module, "validate_runtime_binding", validate)
    monkeypatch.setattr(
        attestation.RuntimeBindingEvidence, "from_binding", classmethod(evidence)
    )
    return counts


def test_public_binding_reuses_only_current_call_attestations(monkeypatch) -> None:
    source, target = _inputs()
    logical = plan_placement_transfer(source.placement, target.placement)
    counts = _count_validation(monkeypatch)
    plans = []
    for _ in range(2):
        counts.update(validation=0, evidence=0)
        plan = bind_logical_transfer_plan(
            logical, target.bindings, source_bindings=source.bindings
        )
        assert counts == {"validation": 4, "evidence": 2}
        for side in ("source", "target"):
            executor = getattr(plan, f"{side}_executors")[0]
            fragment = getattr(plan.operations[0], side)
            assert executor.attestation is fragment.attestation
            assert all(
                item.owner is None for item in executor.attestation.evidence.fragments
            )
        plans.append(plan)
    assert (
        plans[0].target_executors[0].attestation
        is not plans[1].target_executors[0].attestation
    )


def test_standalone_executor_builder_keeps_full_validation(monkeypatch) -> None:
    source, _ = _inputs()
    counts = _count_validation(monkeypatch)
    executors = binding._build_executor_plans(
        (source.placement,), source.bindings, "source"
    )
    assert len(executors) == 1
    assert counts == {"validation": 2, "evidence": 1}
    forged = copy(source.binding)
    object.__setattr__(forged, "revision", "forged-revision")
    with pytest.raises(ValueError, match="revision"):
        binding._build_executor_plans((source.placement,), (forged,), "source")


def test_reused_attestation_still_revalidates_after_pickle(monkeypatch) -> None:
    source, target = _inputs()
    plan = bind_logical_transfer_plan(
        plan_placement_transfer(source.placement, target.placement),
        target.bindings,
        source_bindings=source.bindings,
    )
    counts = _count_validation(monkeypatch)
    restored = pickle.loads(pickle.dumps(plan))
    assert counts["validation"] > 0
    assert restored.total_bytes == plan.total_bytes
    assert (
        restored.target_executors[0].attestation
        is restored.operations[0].target.attestation
    )


@pytest.mark.parametrize("changed", ["placement", "binding"])
def test_executor_builder_rejects_attestation_from_other_input(changed: str) -> None:
    source, _ = _inputs()
    attestations = {}
    binding._bound_fragments_by_placement_fragment_id(
        (source.placement,),
        {
            (
                source.placement.placement_id,
                source.binding.participant_id,
            ): source.binding
        },
        "source",
        attestations=attestations,
    )
    placement = copy(source.placement) if changed == "placement" else source.placement
    runtime_binding = copy(source.binding) if changed == "binding" else source.binding
    with pytest.raises(ValueError, match="runtime attestation inputs differ"):
        binding._build_executor_plans(
            (placement,), (runtime_binding,), "source", attestations=attestations
        )
