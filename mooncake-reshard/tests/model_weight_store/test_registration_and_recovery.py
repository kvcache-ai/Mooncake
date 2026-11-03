from __future__ import annotations

import ctypes

import pytest

from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
)
from mooncake.reshard.weight.store import WeightStoreError

from .helpers import (
    RuntimeParticipant,
    coalesce_runtime_inputs,
    make_runtime_inputs,
    make_weight_store,
    source_manifests,
    upload_all,
)


def test_register_invalid_params_is_not_treated_as_already_registered() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    store.register_result = -600

    with pytest.raises(WeightStoreError, match="register_buffer failed"):
        weight_store.upload(plan, sources[0].placement, sources[0].binding)


def test_registration_deduplicates_exact_aliases_with_same_address() -> None:
    aliases = ("a.weight", "b.weight")
    tensors = tuple(
        TensorDescriptor(
            tensor_id=tensor_id,
            global_shape=(8,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(),
            layout_fingerprint="test:contiguous:v1",
            parallel_axes=(),
        )
        for tensor_id in aliases
    )
    owner = (ctypes.c_ubyte * 8)(*range(8))
    address = ctypes.addressof(owner)
    placement_fragments = tuple(
        PlacementFragment(
            placement_fragment_id=f"placement-{tensor.tensor_id}",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=tensor.global_shape,
            nbytes=8,
            rank=ParallelRank(),
            aliases=aliases,
        )
        for tensor in tensors
    )
    source = make_runtime_inputs(
        resource_id="qwen",
        revision="rev",
        weight_generation=1,
        placement_set_id="alias-source",
        tensors=tensors,
        participants=(
            RuntimeParticipant(
                participant_id="source",
                rank=ParallelRank(),
                placement_fragments=placement_fragments,
                binding_fragments=tuple(
                    RuntimeBindingFragment(
                        placement_fragment_id=fragment.placement_fragment_id,
                        fragment_id=f"fragment-{fragment.tensor_id}",
                        address=address,
                        nbytes=8,
                        worker_id="source",
                        endpoint="source:12345",
                        device="cuda:0",
                        itemsize=1,
                        local_shape=(8,),
                        strides_bytes=(1,),
                        storage_address=address,
                        storage_nbytes=8,
                        storage_offset_bytes=0,
                        owner=owner,
                    )
                    for fragment in placement_fragments
                ),
                instance_id="source",
            ),
        ),
    )
    store, weight_store = make_weight_store()
    plan = weight_store.plan_upload(source.placement, source.bindings)

    weight_store.upload(plan, source.placement, source.binding)

    assert store.register_args == [(address, 8)]


def test_manifest_put_failure_keeps_payloads_for_an_idempotent_retry() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.fail_key = plan.manifest.manifest_key

    with pytest.raises(WeightStoreError, match="manifest put failed"):
        weight_store.commit_upload(plan, receipts)

    assert all(receipt.object_key in store.objects for receipt in receipts)
    store.fail_key = None
    assert weight_store.commit_upload(plan, receipts) == plan.manifest


def test_commit_recovers_when_manifest_response_is_lost_after_write() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.plan_upload(sources.placement, sources.bindings)
    receipts = upload_all(weight_store, plan, sources)
    store.fail_after_write_key = plan.manifest.manifest_key

    assert weight_store.commit_upload(plan, receipts) == plan.manifest
    assert weight_store.load_manifest(plan.manifest.manifest_key) == plan.manifest
    assert all(receipt.object_key in store.objects for receipt in receipts)


def test_upload_unregisters_every_buffer_without_deleting_unowned_payloads() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = coalesce_runtime_inputs(
        sources,
        instance_id="combined-source",
    )
    plan = weight_store.plan_upload(combined.placement, combined.bindings)
    first_address = combined.binding.fragments[0].address
    store.unregister_results[first_address] = -9

    with pytest.raises(WeightStoreError, match="unregister_buffer failed"):
        weight_store.upload(plan, combined.placement, combined.binding)

    assert store.unregister_calls == 2
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert store.remove_forces == []


def test_unregister_failure_does_not_mask_payload_transfer_failure() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = coalesce_runtime_inputs(
        sources,
        instance_id="combined-source",
    )
    plan = weight_store.plan_upload(combined.placement, combined.bindings)
    store.fail_key = plan.operations[1].target.object_key
    store.unregister_results[combined.binding.fragments[0].address] = -9

    with pytest.raises(WeightStoreError) as error:
        weight_store.upload(plan, combined.placement, combined.binding)

    assert "batch_put_from failed" in str(error.value)
    assert "unregister_buffer failed" in str(error.value)
    assert store.unregister_calls == 2
    assert plan.operations[0].target.object_key in store.objects
    assert store.remove_forces == []


def test_unregister_attempts_every_buffer_when_cleanup_raises() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = coalesce_runtime_inputs(
        sources,
        instance_id="combined-source",
    )
    plan = weight_store.plan_upload(combined.placement, combined.bindings)
    first, second = combined.binding.fragments
    store.fail_key = plan.operations[1].target.object_key
    store.unregister_exceptions[first.address] = RuntimeError("unregister exploded")
    store.unregister_results[second.address] = -9

    with pytest.raises(WeightStoreError) as error:
        weight_store.upload(plan, combined.placement, combined.binding)

    assert "batch_put_from failed" in str(error.value)
    assert "unregister exploded" in str(error.value)
    assert "-9" in str(error.value)
    assert store.unregister_addresses == [second.address, first.address]
