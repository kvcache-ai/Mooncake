from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.manifest import WeightPlacementManifest
from mooncake.reshard.weight.store import WeightStoreError

from .helpers import (
    RuntimeInputs,
    coalesce_runtime_inputs,
    make_weight_store,
    source_manifests,
    upload_all,
)


def test_weight_group_objects_are_hard_pinned_and_typed() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)

    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    weight_store.commit_upload(plan, receipts)

    payload_keys = {operation.target.object_key for operation in plan.operations}
    assert {store.configs[key] for key in payload_keys} == {("WEIGHT", True)}
    assert store.configs[plan.control_key] == ("METADATA", True)
    assert store.configs[plan.manifest.manifest_key] == ("METADATA", True)


def test_upload_waits_for_complete_payload_before_returning_receipt() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    key = plan.operations[0].target.object_key
    store.processing_keys.add(key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.upload(plan, sources[0].placement, sources[0].binding)

    assert key in store.processing_keys
    assert store.removed_keys == []


def test_payload_completion_queries_are_bounded() -> None:
    store, weight_store = make_weight_store(max_ranges_per_request=1)
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.exist_batch_sizes.clear()

    weight_store.commit_upload(plan, receipts)

    assert store.exist_batch_sizes == [1, 1, 1, 1]


def test_upload_batches_payload_puts_by_range_limit() -> None:
    store, weight_store = make_weight_store(max_ranges_per_request=2)
    sources = source_manifests(dp=1, tp=4)
    combined = coalesce_runtime_inputs(
        sources,
        instance_id="combined-source",
    )
    plan = weight_store.plan_upload(combined.placement, combined.bindings)

    receipts = weight_store.upload(plan, combined.placement, combined.binding)

    expected_keys = [operation.target.object_key for operation in plan.operations]
    assert max(map(len, store.put_batches)) <= 2
    assert [key for batch in store.put_batches for key in batch] == expected_keys
    assert [receipt.object_key for receipt in receipts] == expected_keys


def test_upload_routes_shared_worker_operations_by_participant() -> None:
    _store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    shared = RuntimeInputs(
        sources.placement,
        tuple(
            replace(
                binding,
                instance_id="shared-source-instance",
                fragments=tuple(
                    replace(
                        fragment,
                        worker_id="shared-source-worker",
                        endpoint="shared-source-endpoint",
                    )
                    for fragment in binding.fragments
                ),
            )
            for binding in sources.bindings
        ),
    )
    plan = weight_store.plan_upload(shared.placement, shared.bindings)

    receipts = upload_all(weight_store, plan, shared)

    assert len(receipts) == len(plan.operations) == 2
    assert {receipt.fragment_id for receipt in receipts} == {
        operation.target.fragment_id for operation in plan.operations
    }


def test_payload_failure_does_not_publish_manifest() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    store.fail_key = plan.operations[1].target.object_key

    with pytest.raises(WeightStoreError, match="batch_put_from"):
        upload_all(weight_store, plan, sources)

    assert plan.manifest.manifest_key not in store.objects
    assert not any("/payload/" in key for key in store.objects)
    assert set(store.removed_keys) == {
        operation.target.object_key for operation in plan.operations
    }
    assert all(store.remove_forces)


def test_upload_surfaces_scalar_batch_put_from_error() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    store.batch_put_from = lambda *args, **kwargs: -17

    with pytest.raises(WeightStoreError, match="batch_put_from failed: -17"):
        upload_all(weight_store, plan, sources)


def test_upload_surfaces_scalar_batch_is_exist_error() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    store.batch_is_exist = lambda *args, **kwargs: -18

    with pytest.raises(WeightStoreError, match="existence check failed: -18"):
        upload_all(weight_store, plan, sources)


def test_upload_rejects_stale_runtime_binding_fragment() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    stale_binding = replace(
        sources.bindings[0],
        fragments=(
            replace(
                sources.bindings[0].fragments[0],
                address=sources.bindings[0].fragments[0].address + 1,
                storage_address=(sources.bindings[0].fragments[0].storage_address + 1),
            ),
        ),
    )

    with pytest.raises(WeightStoreError, match="stale source fragment"):
        weight_store.upload(plan, sources.placement, stale_binding)


def test_upload_rejects_weight_generation_rollover_before_store_io() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=1, weight_generation=7)[0]
    plan = weight_store.plan_upload(source.placement, source.bindings)
    current_placement = WeightPlacementManifest(
        resource_id=source.placement.resource_id,
        revision=source.placement.revision,
        weight_generation=8,
        placement_set_id=source.placement.placement_set_id,
        topology=source.placement.topology,
        parts=tuple(
            replace(part, weight_generation=8) for part in source.placement.parts
        ),
    )
    current_binding = replace(
        source.binding,
        placement_id=current_placement.placement_id,
        placement_digest=current_placement.digest,
    )
    store.calls.clear()

    with pytest.raises(WeightStoreError, match="weight generation mismatch"):
        weight_store.upload(plan, current_placement, current_binding)

    assert store.calls == []


def test_upload_rejects_manifest_lease_rollover_before_store_io() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=1)[0]
    source = RuntimeInputs(
        source.placement,
        (replace(source.binding, lease_id="source-lease-1"),),
    )
    plan = weight_store.plan_upload(source.placement, source.bindings)
    current = replace(source.binding, lease_id="source-lease-2")
    store.calls.clear()

    with pytest.raises(WeightStoreError, match="stale source lease"):
        weight_store.upload(plan, source.placement, current)

    assert store.calls == []


def test_upload_rejects_generation_scoped_fragment_id_rollover() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    replacement = replace(
        sources.bindings[0].fragments[0],
        fragment_id="replacement-fragment",
    )
    current = replace(
        sources.bindings[0],
        generation=2,
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="missing planned source fragment"):
        weight_store.upload(plan, sources.placement, current)
