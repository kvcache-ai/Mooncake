from __future__ import annotations

import json
import pickle
from types import SimpleNamespace
import weakref
from dataclasses import replace

import pytest

from mooncake.reshard.contracts import StoredResourceManifest
from mooncake.reshard.weight import WeightUploadPlan, plan_weight_upload
from mooncake.reshard.weight._store.backend import default_config_factory
from mooncake.reshard.weight.manifest import OwnershipAxis, ParallelRank
from mooncake.reshard.weight.storage_manifest import StoredWeightManifest
from mooncake.reshard.weight.store import WeightStore, WeightStoreError

from model_weight_planner.helpers import tp_manifests

from .helpers import (
    RuntimeInputs,
    make_weight_store,
    plan_transfer,
    rebuild_runtime_inputs,
    source_manifests,
    target_manifests,
    tensor_descriptor,
    upload_all,
    with_empty_participant,
)


def test_upload_deduplicates_dp_and_commits_manifest_last() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()

    plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    receipts = upload_all(weight_store, plan, sources)

    assert len(plan.operations) == 2
    assert len(receipts) == 2
    assert plan.manifest.manifest_key not in store.objects
    assert len(store.objects) == 2

    manifest = weight_store.commit_upload(plan, receipts)

    assert (
        StoredWeightManifest.from_json(store.objects[manifest.manifest_key]) == manifest
    )
    assert len(store.objects) == 4
    revision_keys = {
        manifest.manifest_key,
        *(operation.target.object_key for operation in plan.operations),
    }
    assert {store.group_ids[key] for key in revision_keys} == {manifest.group_id}
    assert store.group_ids[plan.control_key] == plan.transaction_group_id
    assert plan.transaction_group_id != manifest.group_id
    assert store.register_calls == 2
    assert store.unregister_calls == 2
    assert store.registered == set()


def test_store_manifest_json_round_trip_persists_weight_generation() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2, weight_generation=17)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    persisted = weight_store.commit_upload(
        plan,
        upload_all(weight_store, plan, sources),
    )
    raw = store.objects[persisted.manifest_key]
    decoded = StoredWeightManifest.from_json(raw)

    assert json.loads(raw)["weight_generation"] == 17
    assert decoded.weight_generation == 17
    assert decoded == persisted == plan.manifest


def test_stored_weight_manifest_implements_stored_resource_contract() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    stored: StoredResourceManifest = weight_store.commit_upload(
        plan,
        upload_all(weight_store, plan, sources),
    )

    assert isinstance(stored, StoredResourceManifest)
    assert stored.resource_id == sources.placement.resource_id
    assert stored.resource_kind.value == "model_weight"
    assert stored.manifest_key == plan.manifest.manifest_key


def test_stored_resource_manifest_requires_a_concrete_resource_kind() -> None:
    with pytest.raises(TypeError, match="abstract"):
        StoredResourceManifest(
            namespace="test",
            resource_id="model",
            group_id="weights/test/model/revision",
            manifest_key="weights/test/model/revision/manifest",
            created_at="2026-08-29T00:00:00Z",
        )


def test_stored_weight_manifest_pickle_restores_validated_snapshot() -> None:
    _store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(source.placement, source.bindings)

    restored = pickle.loads(pickle.dumps(plan.manifest))

    assert restored == plan.manifest
    assert restored.manifest_digest == plan.manifest.manifest_digest


def test_plan_upload_does_not_require_binding_for_empty_participant() -> None:
    _store, weight_store = make_weight_store()
    sources = with_empty_participant(
        source_manifests(dp=1, tp=1),
        participant_id="inactive-pp1",
        rank=ParallelRank(pp=1),
    )

    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    assert len(plan.operations) == 1


def test_plan_upload_keeps_owner_free_runtime_evidence() -> None:
    class AllocationOwner:
        pass

    sources = source_manifests(dp=1, tp=1)
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
    _store, weight_store = make_weight_store()

    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    assert not hasattr(plan.operations[0].source_snapshot, "owner")
    del owner
    del sources
    assert owner_ref() is None


def test_plan_upload_explicitly_rejects_dp_owned_source_tensors() -> None:
    """Store snapshots still require one complete replicated DP source."""

    sources = source_manifests(dp=1, tp=1)
    tensor = replace(
        tensor_descriptor(),
        shard_dims=(),
        parallel_axes=(
            OwnershipAxis(kind="dp"),
            OwnershipAxis(kind="pp"),
            OwnershipAxis(kind="ep"),
        ),
    )
    sources = rebuild_runtime_inputs(sources, tensors=(tensor,))
    _store, weight_store = make_weight_store()

    with pytest.raises(ValueError, match="requires replicated DP source tensors"):
        weight_store.plan_upload(sources.placement, sources.bindings)


def test_plan_upload_rejects_missing_nonempty_participant_binding() -> None:
    sources = source_manifests(dp=1, tp=2)
    tensors = (
        replace(
            tensor_descriptor(),
            tensor_id="layers.0.mlp.weight",
            global_shape=(4,),
            expert_id=None,
        ),
        replace(
            tensor_descriptor(),
            tensor_id="layers.1.mlp.weight",
            global_shape=(4,),
            expert_id=None,
        ),
    )
    fragments = {}
    for index, part in enumerate(sources.placement.parts):
        fragments[part.participant_id] = (
            replace(
                part.fragments[0],
                tensor_id=tensors[index].tensor_id,
                global_offset=(0,),
                local_shape=(4,),
                rank=ParallelRank(pp=index),
            ),
        )
    sources = rebuild_runtime_inputs(
        sources,
        tensors=tensors,
        placement_fragments=fragments,
    )
    _store, weight_store = make_weight_store()

    with pytest.raises(
        ValueError, match="no complete generation-consistent DP replica"
    ):
        weight_store.plan_upload(
            sources.placement,
            (sources.bindings[0],),
        )


def test_same_revision_different_generations_have_disjoint_store_identity() -> None:
    store, weight_store = make_weight_store()
    generation_17 = source_manifests(dp=1, tp=2, weight_generation=17)
    generation_18 = source_manifests(dp=1, tp=2, weight_generation=18)

    plan_17 = weight_store.plan_upload(
        generation_17.placement,
        generation_17.bindings,
    )
    plan_18 = weight_store.plan_upload(
        generation_18.placement,
        generation_18.bindings,
    )

    assert generation_17.placement.revision == generation_18.placement.revision
    assert generation_17.placement.placement_id != generation_18.placement.placement_id
    assert generation_17.placement.digest != generation_18.placement.digest
    assert plan_17.manifest.group_id.endswith("/17")
    assert plan_18.manifest.group_id.endswith("/18")
    assert plan_17.manifest.group_id != plan_18.manifest.group_id
    assert plan_17.manifest.manifest_key != plan_18.manifest.manifest_key
    assert {operation.target.object_key for operation in plan_17.operations}.isdisjoint(
        operation.target.object_key for operation in plan_18.operations
    )
    assert all(
        operation.target.object_key.startswith(f"{plan_17.manifest.group_id}/")
        for operation in plan_17.operations
    )
    assert all(
        operation.target.object_key.startswith(f"{plan_18.manifest.group_id}/")
        for operation in plan_18.operations
    )

    manifest_17 = weight_store.commit_upload(
        plan_17,
        upload_all(weight_store, plan_17, generation_17),
    )
    manifest_18 = weight_store.commit_upload(
        plan_18,
        upload_all(weight_store, plan_18, generation_18),
    )

    assert weight_store.load_manifest(manifest_17.manifest_key) == manifest_17
    assert weight_store.load_manifest(manifest_18.manifest_key) == manifest_18
    assert manifest_17 != manifest_18


def test_plan_upload_selects_one_complete_generation_consistent_dp_replica() -> None:
    sources = source_manifests(dp=2, tp=2)
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

    _, weight_store = make_weight_store()
    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    assert {operation.source_placement.rank.dp for operation in plan.operations} == {0}
    assert {operation.source_generation for operation in plan.operations} == {2}


def test_plan_upload_rejects_complete_dp_replicas_at_different_generations() -> None:
    sources = source_manifests(dp=2, tp=2)
    rank_by_participant = {
        part.participant_id: part.rank for part in sources.placement.parts
    }
    sources = RuntimeInputs(
        sources.placement,
        tuple(
            replace(
                binding,
                generation=rank_by_participant[binding.participant_id].dp + 1,
            )
            for binding in sources.bindings
        ),
    )
    _, weight_store = make_weight_store()

    with pytest.raises(ValueError, match="inconsistent lease generations"):
        weight_store.plan_upload(sources.placement, sources.bindings)


def test_plan_upload_rejects_mixed_generations_within_one_dp_replica() -> None:
    sources = source_manifests(dp=1, tp=2)
    sources = RuntimeInputs(
        sources.placement,
        (sources.bindings[0], replace(sources.bindings[1], generation=2)),
    )
    _, weight_store = make_weight_store()

    with pytest.raises(ValueError, match="generation-consistent DP replica"):
        weight_store.plan_upload(sources.placement, sources.bindings)


def test_plan_upload_matches_te_planner_dp_replica_selection() -> None:
    sources = source_manifests(dp=2, tp=2)
    targets = target_manifests(dp=1, tp=1)
    _, weight_store = make_weight_store()

    te_plan = plan_transfer(sources, targets)
    store_plan = weight_store.plan_upload(sources.placement, sources.bindings)

    assert {operation.source.rank.dp for operation in te_plan.operations} == {0}
    assert {
        operation.source_placement.rank.dp for operation in store_plan.operations
    } == {0}


def test_global_placement_rejects_cross_owner_coverage_before_store() -> None:
    sources = source_manifests(dp=1, tp=2)
    fragments = {}
    for part in sources.placement.parts:
        fragments[part.participant_id] = tuple(
            replace(
                fragment,
                rank=replace(fragment.rank, pp=fragment.rank.tp, ep=fragment.rank.tp),
            )
            for fragment in part.fragments
        )

    with pytest.raises(ValueError, match="not fully covered"):
        rebuild_runtime_inputs(sources, placement_fragments=fragments)


def test_plan_upload_rejects_dense_tp_shards_across_ep_replicas() -> None:
    tensor = replace(
        tensor_descriptor(),
        tensor_id="layers.2.self_attn.q_proj.weight",
        expert_id=None,
    )
    sources = source_manifests(dp=1, tp=2)
    fragments = {
        part.participant_id: tuple(
            replace(
                fragment,
                tensor_id=tensor.tensor_id,
                rank=replace(fragment.rank, ep=fragment.rank.tp),
            )
            for fragment in part.fragments
        )
        for part in sources.placement.parts
    }
    with pytest.raises(ValueError, match="not fully covered"):
        rebuild_runtime_inputs(
            sources,
            tensors=(tensor,),
            placement_fragments=fragments,
        )


def test_store_rejects_binding_for_different_placement_digest() -> None:
    sources = source_manifests(dp=1, tp=1)

    with pytest.raises(WeightStoreError, match="placement digest"):
        WeightStore(object()).plan_upload(
            sources.placement,
            (replace(sources.bindings[0], placement_digest="0" * 64),),
        )


def test_native_store_requires_group_semantics_binding(monkeypatch) -> None:
    class ReplicateConfigWithoutGroups:
        __slots__ = ("with_hard_pin", "data_type")

        def __init__(self) -> None:
            self.with_hard_pin = False
            self.data_type = None

    class ObjectDataType:
        WEIGHT = "weight"
        METADATA = "metadata"

    native_store_module = SimpleNamespace(
        ReplicateConfig=ReplicateConfigWithoutGroups,
        ObjectDataType=ObjectDataType,
    )

    monkeypatch.setattr(
        "mooncake.reshard.weight._store.backend.import_module",
        lambda _: native_store_module,
    )

    with pytest.raises(WeightStoreError, match="ReplicateConfig.group_ids"):
        default_config_factory(("weight-group",), "payload")


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
