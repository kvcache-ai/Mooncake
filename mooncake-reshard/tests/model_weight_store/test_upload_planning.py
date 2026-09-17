from __future__ import annotations

from dataclasses import replace
import gc
import weakref

import pytest

from mooncake.reshard.weight import (
    OwnershipAxis,
    WeightPlacementManifest,
    WeightUploadPlan,
    plan_weight_upload,
)

from model_weight_planner.helpers import RuntimeInputs, descriptor, tp_manifests


def _rebuild_inputs(
    inputs: RuntimeInputs,
    *,
    tensors=None,
    weight_generation: int | None = None,
) -> RuntimeInputs:
    placement = WeightPlacementManifest.from_fragments(
        resource_id=inputs.placement.resource_id,
        revision=inputs.placement.revision,
        weight_generation=(
            inputs.placement.weight_generation
            if weight_generation is None
            else weight_generation
        ),
        placement_set_id=inputs.placement.placement_set_id,
        topology=inputs.placement.topology,
        tensors=tuple(tensors if tensors is not None else inputs.placement.tensors),
        fragments=inputs.placement.fragments,
    )
    return RuntimeInputs(
        placement,
        tuple(
            replace(
                binding,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
            )
            for binding in inputs.bindings
        ),
    )


def test_upload_plan_selects_one_complete_generation_consistent_dp_replica() -> None:
    sources = tp_manifests(
        tp=2,
        dp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    rank_by_participant = {
        part.participant_id: part.rank for part in sources.placement.parts
    }
    sources = RuntimeInputs(
        sources.placement,
        tuple(
            replace(
                binding,
                generation=(
                    3
                    if rank_by_participant[binding.participant_id].dp == 1
                    and rank_by_participant[binding.participant_id].tp == 1
                    else 2
                ),
            )
            for binding in sources.bindings
        ),
    )

    plan = plan_weight_upload(sources.placement, sources.bindings)

    assert {operation.source_placement.rank.dp for operation in plan.operations} == {0}
    assert {operation.source_generation for operation in plan.operations} == {2}
    assert {operation.target for operation in plan.operations} == set(
        plan.manifest.fragments
    )
    assert plan.manifest.group_id.endswith("/1")
    assert plan.control_key == f"{plan.transaction_group_id}/decision"


def test_upload_plan_rejects_complete_dp_replicas_at_different_generations() -> None:
    sources = tp_manifests(
        tp=2,
        dp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    rank_by_participant = {
        part.participant_id: part.rank for part in sources.placement.parts
    }
    bindings = tuple(
        replace(
            binding,
            generation=rank_by_participant[binding.participant_id].dp + 1,
        )
        for binding in sources.bindings
    )

    with pytest.raises(ValueError, match="inconsistent lease generations"):
        plan_weight_upload(sources.placement, bindings)


def test_upload_plan_rejects_incomplete_dp_replica() -> None:
    sources = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )

    with pytest.raises(ValueError, match="generation-consistent DP replica"):
        plan_weight_upload(sources.placement, sources.bindings[:1])


def test_upload_plan_rejects_dp_owned_source_tensor() -> None:
    sources = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    tensor = replace(
        descriptor(),
        shard_dims=(),
        parallel_axes=(
            OwnershipAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            OwnershipAxis(kind="ep"),
        ),
    )
    sources = _rebuild_inputs(sources, tensors=(tensor,))

    with pytest.raises(ValueError, match="requires replicated DP source tensors"):
        plan_weight_upload(sources.placement, sources.bindings)


def test_upload_plan_uses_generation_scoped_store_identity() -> None:
    generation_17 = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source-17",
    )
    generation_18 = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x20000,
        worker_prefix="source-18",
    )
    generation_17 = _rebuild_inputs(generation_17, weight_generation=17)
    generation_18 = _rebuild_inputs(generation_18, weight_generation=18)

    plan_17 = plan_weight_upload(generation_17.placement, generation_17.bindings)
    plan_18 = plan_weight_upload(generation_18.placement, generation_18.bindings)

    assert plan_17.manifest.group_id.endswith("/17")
    assert plan_18.manifest.group_id.endswith("/18")
    assert plan_17.manifest.group_id != plan_18.manifest.group_id
    assert {operation.target.object_key for operation in plan_17.operations}.isdisjoint(
        operation.target.object_key for operation in plan_18.operations
    )


def test_upload_plan_preserves_tp_pp_and_ep_fragment_ownership() -> None:
    sources = tp_manifests(
        tp=2,
        pp_rank=3,
        ep_rank=2,
        address_base=0x10000,
        worker_prefix="source",
    )

    plan = plan_weight_upload(sources.placement, sources.bindings)

    assert len(plan.operations) == 2
    assert {operation.source_placement.rank.tp for operation in plan.operations} == {
        0,
        1,
    }
    assert {operation.source_placement.rank.pp for operation in plan.operations} == {3}
    assert {operation.source_placement.rank.ep for operation in plan.operations} == {2}


def test_upload_plan_does_not_retain_framework_allocation_owners() -> None:
    class AllocationOwner:
        pass

    sources = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    owner = AllocationOwner()
    owner_ref = weakref.ref(owner)
    sources = RuntimeInputs(
        sources.placement,
        (
            replace(
                sources.bindings[0],
                fragments=(replace(sources.bindings[0].fragments[0], owner=owner),),
            ),
        ),
    )

    plan = plan_weight_upload(sources.placement, sources.bindings)

    assert not hasattr(plan.operations[0].source_snapshot, "owner")
    del owner
    del sources
    gc.collect()
    assert owner_ref() is None


def test_upload_plan_rejects_payload_outside_its_upload_transaction() -> None:
    sources = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    plan = plan_weight_upload(sources.placement, sources.bindings)
    target = replace(
        plan.operations[0].target,
        object_key=f"{plan.manifest.group_id}/payload/other-transaction/fragment",
    )
    manifest = replace(plan.manifest, fragments=(target,))
    operation = replace(plan.operations[0], target=target)

    with pytest.raises(ValueError, match="does not own payload members"):
        WeightUploadPlan(
            manifest=manifest,
            source_placement_id=plan.source_placement_id,
            source_placement_digest=plan.source_placement_digest,
            transaction_group_id=plan.transaction_group_id,
            control_key=plan.control_key,
            operations=(operation,),
        )
