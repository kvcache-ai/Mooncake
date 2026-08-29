from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight.store import UploadReceipt, WeightStoreError

from .helpers import make_weight_store, source_manifests, upload_all


def test_finalize_upload_transaction_keeps_committed_revision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit_upload(plan, receipts)

    weight_store.finalize_upload_transaction(plan)
    weight_store.finalize_upload_transaction(plan)

    assert plan.control_key in store.objects
    assert manifest.manifest_key in store.objects
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )


def test_commit_rejects_incomplete_receipts() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    receipts = upload_all(weight_store, plan, sources)

    with pytest.raises(WeightStoreError, match="missing upload receipts"):
        weight_store.commit_upload(plan, receipts[:-1])

    assert plan.manifest.manifest_key not in store.objects


def test_commit_rechecks_every_payload_after_receipts_are_issued() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.objects.pop(receipts[0].object_key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.commit_upload(plan, receipts)

    assert plan.manifest.manifest_key not in store.objects


def test_incomplete_payload_does_not_lock_upload_into_commit_decision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.objects.pop(receipts[0].object_key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.commit_upload(plan, receipts)

    weight_store.abort_upload(plan, receipts)
    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )


def test_abort_cleans_the_whole_plan_when_a_receipt_was_lost() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)

    weight_store.abort_upload(plan, receipts[:-1])

    assert not any(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert set(store.removed_keys) == {
        operation.target.object_key for operation in plan.operations
    }


def test_abort_does_not_delete_payload_while_manifest_commit_is_processing() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.processing_keys.add(plan.control_key)

    with pytest.raises(WeightStoreError, match="not complete"):
        weight_store.abort_upload(plan, receipts)

    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert store.removed_keys == []


def test_abort_loses_after_commit_claims_plan_while_manifest_is_processing() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.processing_keys.add(plan.manifest.manifest_key)

    with pytest.raises(WeightStoreError, match="manifest put failed"):
        weight_store.commit_upload(plan, receipts)
    with pytest.raises(WeightStoreError, match="already chose commit"):
        weight_store.abort_upload(plan, receipts)

    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert store.removed_keys == []


def test_upload_fails_and_cleans_up_when_abort_wins_after_complete_check() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=1)[0]
    plan = weight_store.plan_upload(source.placement, source.bindings)
    store.after_batch_is_exist = lambda: weight_store.abort_upload(plan, ())

    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.upload(plan, source.placement, source.binding)

    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )


def test_commit_is_idempotent_for_the_same_upload_plan() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    receipts = upload_all(weight_store, plan, sources)
    first = weight_store.commit_upload(plan, receipts)

    assert weight_store.commit_upload(plan, receipts) == first
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )


def test_commit_conflict_keeps_winner_and_force_cleans_loser_payloads() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    winner = weight_store.plan_upload(sources.placement, sources.bindings)
    winner_receipts = upload_all(weight_store, winner, sources)
    weight_store.commit_upload(winner, winner_receipts)
    loser = weight_store.plan_upload(sources.placement, sources.bindings)
    loser_receipts = upload_all(weight_store, loser, sources)

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit_upload(loser, loser_receipts)

    assert weight_store.load_manifest(winner.manifest.manifest_key) == winner.manifest
    assert all(
        operation.target.object_key not in store.objects
        for operation in loser.operations
    )
    assert store.remove_forces[-len(loser.operations) :] == [True] * len(
        loser.operations
    )


def test_finalize_conflicting_commit_keeps_terminal_decision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    winner = weight_store.plan_upload(sources.placement, sources.bindings)
    winner_receipts = upload_all(weight_store, winner, sources)
    winner_manifest = weight_store.commit_upload(winner, winner_receipts)
    loser = weight_store.plan_upload(sources.placement, sources.bindings)
    loser_receipts = upload_all(weight_store, loser, sources)

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit_upload(loser, loser_receipts)
    assert loser.control_key in store.objects

    weight_store.finalize_upload_transaction(loser)

    assert loser.control_key in store.objects
    assert weight_store.load_manifest(winner_manifest.manifest_key) == winner_manifest
    assert all(
        fragment.object_key in store.objects for fragment in winner_manifest.fragments
    )


def test_conflict_cleanup_preserves_payloads_referenced_by_winner() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    winner = replace(plan.manifest, created_at="2026-07-19T00:00:00Z")
    store.objects[plan.manifest.manifest_key] = winner.to_json().encode()

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit_upload(plan, receipts)
    weight_store.finalize_upload_transaction(plan)

    assert all(fragment.object_key in store.objects for fragment in winner.fragments)
    assert plan.control_key in store.objects


def test_commit_finalize_rejects_late_abort_and_preserves_ready_revision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit_upload(plan, receipts)
    weight_store.finalize_upload_transaction(plan)

    with pytest.raises(WeightStoreError, match="published weight revision"):
        weight_store.abort_upload(plan, receipts)

    assert weight_store.load_manifest(manifest.manifest_key) == manifest
    assert all(fragment.object_key in store.objects for fragment in manifest.fragments)
    assert plan.control_key in store.objects


def test_abort_finalize_rejects_late_upload_and_commit() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    weight_store.abort_upload(plan, receipts)
    weight_store.finalize_upload_transaction(plan)

    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.upload(plan, sources[0].placement, sources[0].binding)
    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.commit_upload(plan, receipts)

    assert plan.manifest.manifest_key not in store.objects
    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )
    assert plan.control_key in store.objects


def test_abort_checks_ready_manifest_when_commit_tombstone_was_lost() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit_upload(plan, receipts)
    store.objects.pop(plan.control_key)

    with pytest.raises(WeightStoreError, match="published weight revision"):
        weight_store.abort_upload(plan, receipts)

    assert weight_store.load_manifest(manifest.manifest_key) == manifest
    assert all(fragment.object_key in store.objects for fragment in manifest.fragments)


def test_commit_detects_a_concurrent_winner_after_manifest_preflight() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    winner = weight_store.plan_upload(sources.placement, sources.bindings)
    loser = weight_store.plan_upload(sources.placement, sources.bindings)
    winner_receipts = upload_all(weight_store, winner, sources)
    loser_receipts = upload_all(weight_store, loser, sources)
    assert len(winner_receipts) == len(loser_receipts)
    store.manifest_race_key = loser.manifest.manifest_key
    store.manifest_race_value = winner.manifest.to_json().encode()

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit_upload(loser, loser_receipts)

    assert weight_store.load_manifest(winner.manifest.manifest_key) == winner.manifest
    assert all(
        operation.target.object_key in store.objects for operation in winner.operations
    )
    assert all(
        operation.target.object_key not in store.objects
        for operation in loser.operations
    )


def test_commit_rejects_duplicate_or_forged_receipts() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)

    with pytest.raises(WeightStoreError, match="duplicate upload receipt"):
        weight_store.commit_upload(plan, [*receipts, receipts[0]])

    forged = UploadReceipt(
        fragment_id=receipts[0].fragment_id,
        object_key="forged",
        worker_id=receipts[0].worker_id,
    )
    with pytest.raises(WeightStoreError, match="invalid upload receipt"):
        weight_store.commit_upload(plan, [forged, receipts[1]])
