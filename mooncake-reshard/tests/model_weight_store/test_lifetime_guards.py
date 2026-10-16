from __future__ import annotations

import pytest

from mooncake.reshard.lifetime import TerminalTransferState
from mooncake.reshard.weight.store import WeightStore, WeightStoreError

from .helpers import (
    FakeReplicateConfig,
    InMemoryStore,
    allocation_guards_for_bindings,
    make_weight_store,
    source_manifests,
    target_manifests,
    upload_all,
)


def _raw_store(store: InMemoryStore) -> WeightStore:
    return WeightStore(
        store,
        config_factory=lambda group_ids, record_type: FakeReplicateConfig(
            list(group_ids),
            data_type=("WEIGHT" if record_type == "payload" else "METADATA"),
            with_hard_pin=True,
        ),
    )


def test_store_rejects_raw_gpu_addresses_without_allocation_guards() -> None:
    store = InMemoryStore()
    weight_store = _raw_store(store)
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)

    with pytest.raises(WeightStoreError, match="source allocation guard providers"):
        weight_store.upload(plan, sources.placement, sources.binding)

    assert store.register_calls == 0
    assert store.put_batches == []


def test_store_upload_releases_pin_after_sync_completion() -> None:
    store = InMemoryStore()
    weight_store = _raw_store(store)
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    guards = allocation_guards_for_bindings((sources.binding,))

    weight_store.upload(
        plan,
        sources.placement,
        sources.binding,
        source_allocation_guards=guards,
    )

    token = next(iter(guards.values())).tokens[0]
    assert token.released_states == [TerminalTransferState.COMPLETED]
    assert store.registered == set()


def test_store_quarantines_pin_until_unregister_cleanup_succeeds() -> None:
    store = InMemoryStore()
    weight_store = _raw_store(store)
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    guards = allocation_guards_for_bindings((sources.binding,))
    address = sources.binding.fragments[0].address
    store.unregister_results[address] = -9

    with pytest.raises(WeightStoreError, match="allocation lifetime is quarantined"):
        weight_store.upload(
            plan,
            sources.placement,
            sources.binding,
            source_allocation_guards=guards,
        )

    token = next(iter(guards.values())).tokens[0]
    assert token.released_states == []
    (pending_id,) = weight_store.pending_registration_ids()
    with pytest.raises(WeightStoreError, match="blocked by pending registration"):
        weight_store.upload(
            plan,
            sources.placement,
            sources.binding,
            source_allocation_guards=allocation_guards_for_bindings((sources.binding,)),
        )

    store.unregister_results.pop(address)
    weight_store.drain_pending_registration(pending_id)

    assert token.released_states == [TerminalTransferState.COMPLETED]
    assert weight_store.pending_registration_ids() == ()
    assert store.registered == set()


def test_store_load_holds_target_pin_through_sync_ranged_read() -> None:
    store, staging_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    upload_plan = staging_store.plan_upload(sources.placement, sources.bindings)
    manifest = staging_store.commit_upload(
        upload_plan,
        upload_all(staging_store, upload_plan, sources),
    )
    target = target_manifests(dp=1, tp=1)[0]
    raw_store = _raw_store(store)
    load_plan = raw_store.plan_load(manifest, target.placement, target.bindings)
    guards = allocation_guards_for_bindings((target.binding,))

    raw_store.load(
        load_plan,
        target.placement,
        target.binding,
        target_allocation_guards=guards,
    )

    token = next(iter(guards.values())).tokens[0]
    assert token.released_states == [TerminalTransferState.COMPLETED]
    assert store.registered == set()


def test_typed_pre_registered_lease_retains_pin_until_explicit_close() -> None:
    store = InMemoryStore()
    weight_store = _raw_store(store)
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    guards = allocation_guards_for_bindings((sources.binding,))
    lease = weight_store.register_weight_buffers(
        sources.binding,
        fragment_ids=tuple(
            operation.source_snapshot.fragment_id for operation in plan.operations
        ),
        allocation_guards=guards,
        side="source",
    )

    weight_store.upload(
        plan,
        sources.placement,
        sources.binding,
        registration_lease=lease,
    )

    token = next(iter(guards.values())).tokens[0]
    assert token.released_states == []
    assert store.registered == {sources.binding.fragments[0].address}

    lease.close()

    assert token.released_states == [TerminalTransferState.COMPLETED]
    assert store.registered == set()


def test_typed_lease_cleanup_failure_quarantines_and_closes_lease() -> None:
    store = InMemoryStore()
    weight_store = _raw_store(store)
    sources = source_manifests(dp=1, tp=1)
    guards = allocation_guards_for_bindings((sources.binding,))
    address = sources.binding.fragments[0].address
    lease = weight_store.register_weight_buffers(
        sources.binding,
        fragment_ids=(sources.binding.fragments[0].fragment_id,),
        allocation_guards=guards,
        side="source",
    )
    token = next(iter(guards.values())).tokens[0]
    store.unregister_results[address] = -9

    with pytest.raises(WeightStoreError, match="allocation lifetime is quarantined"):
        lease.close()

    assert token.released_states == []
    (pending_id,) = weight_store.pending_registration_ids()
    assert lease.closed is True

    store.unregister_results.pop(address)
    weight_store.drain_pending_registration(pending_id)

    assert token.released_states == [TerminalTransferState.COMPLETED]
    lease.close()
    assert store.registered == set()
