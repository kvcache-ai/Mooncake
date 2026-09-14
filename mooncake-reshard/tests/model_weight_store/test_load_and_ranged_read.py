from __future__ import annotations

import ctypes
import copy
from dataclasses import replace
from itertools import product
from math import prod

import pytest

from mooncake.reshard.weight.manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    OwnershipAxis,
    ReplicatedAxis,
    SplitAxis,
)
from mooncake.reshard.weight.store import WeightStoreError
from mooncake.reshard.weight.store import WeightStore
from mooncake.reshard.weight._store.contracts import WeightLoadPlan
from mooncake.reshard.weight._store.validation import same_runtime_snapshot

from .helpers import (
    RuntimeInputs,
    RuntimeParticipant,
    allocation_guards_for_bindings,
    bound_fragments,
    coalesce_runtime_inputs,
    expected_multi_dim_fragment,
    load_all,
    make_runtime_inputs,
    make_weight_store,
    multi_dim_store_manifests,
    rebuild_runtime_inputs,
    source_manifests,
    target_manifests,
    upload_all,
    with_empty_participant,
)


def runtime_fragment_for_snapshot(**overrides) -> RuntimeBindingFragment:
    values = {
        "placement_fragment_id": "placement-0",
        "fragment_id": "runtime-0",
        "address": 0x1000,
        "nbytes": 8,
        "worker_id": "worker-0",
        "endpoint": "worker-0:12345",
        "device": "cuda:0",
        "itemsize": 2,
        "local_shape": (2, 2),
        "strides_bytes": (4, 2),
        "storage_address": 0x0FF0,
        "storage_nbytes": 32,
        "storage_offset_bytes": 16,
    }
    values.update(overrides)
    return RuntimeBindingFragment(**values)


@pytest.mark.parametrize(
    ("field", "different_value"),
    [
        ("placement_fragment_id", "placement-1"),
        ("fragment_id", "runtime-1"),
        ("address", 0x1001),
        ("nbytes", 4),
        ("worker_id", "worker-1"),
        ("endpoint", "worker-1:12345"),
        ("device", "cuda:1"),
        ("itemsize", 1),
        ("local_shape", (1, 4)),
        ("strides_bytes", (8, 2)),
        ("storage_address", 0x0FE0),
        ("storage_nbytes", 64),
        ("storage_offset_bytes", 8),
    ],
)
def test_same_runtime_snapshot_compares_all_physical_evidence(
    field: str,
    different_value: object,
) -> None:
    current = runtime_fragment_for_snapshot()
    planned = copy.copy(current)
    object.__setattr__(planned, field, different_value)

    assert not same_runtime_snapshot(current, planned)


def test_same_runtime_snapshot_ignores_owner() -> None:
    current = runtime_fragment_for_snapshot(owner=object())
    planned = replace(current, owner=object())

    assert same_runtime_snapshot(current, planned)


@pytest.mark.parametrize("target_dim", [1, 2])
def test_store_preserves_expert_boxes_and_loads_cross_dim(
    target_dim: int,
) -> None:
    store, weight_store = make_weight_store(max_ranges_per_request=5)
    sources = multi_dim_store_manifests("source", source=True)
    targets = multi_dim_store_manifests("target", source=False, target_dim=target_dim)

    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)

    assert upload_plan.manifest.tensors[0].shard_dims == (0,)
    assert len(upload_plan.operations) == 4
    assert len({item.target.object_key for item in upload_plan.operations}) == 4
    assert {item.target.global_offset for item in upload_plan.operations} == {
        (rank, 0, 0) for rank in range(4)
    }

    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    loaded = weight_store.load_manifest(manifest.manifest_key)
    load_plan = weight_store.plan_load(loaded, targets.placement, targets.bindings)
    load_all(weight_store, load_plan, targets)

    assert loaded == manifest
    assert all(route.source_pp is None for route in load_plan.transfer.pipeline_routes)
    assert max(store.range_batch_sizes) <= 5
    for target in bound_fragments(targets):
        assert bytes(target.owner) == expected_multi_dim_fragment(target.placement)


class _ManifestOnlyReader:
    def __init__(self, store) -> None:
        self._store = store

    def get(self, key: str) -> bytes:
        return self._store.get(key)

    def is_exist(self, key: str) -> int:
        return self._store.is_exist(key)

    def register_buffer(self, address: int, nbytes: int) -> int:
        return self._store.register_buffer(address, nbytes)

    def unregister_buffer(self, address: int) -> int:
        return self._store.unregister_buffer(address)

    def get_into_ranges(self, *args):
        return self._store.get_into_ranges(*args)

    def __getattr__(self, name: str):
        raise AssertionError(f"manifest restore accessed legacy Store API: {name}")


def test_restore_reconstructs_from_stored_manifest_without_legacy_tensor_metadata() -> (
    None
):
    store, writer = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    targets = target_manifests(dp=1, tp=4)
    upload_plan = writer.plan_upload(sources.placement, sources.bindings)
    manifest = writer.commit_upload(
        upload_plan, upload_all(writer, upload_plan, sources)
    )
    reader = WeightStore(_ManifestOnlyReader(store))

    loaded = reader.load_manifest(manifest.manifest_key)
    load_plan = reader.plan_load(loaded, targets.placement, targets.bindings)
    for binding in targets.bindings:
        reader.load(
            load_plan,
            targets.placement,
            binding,
            target_allocation_guards=allocation_guards_for_bindings((binding,)),
        )

    assert loaded == manifest
    for fragment in bound_fragments(targets):
        start = fragment.global_offset[0]
        assert bytes(fragment.owner) == bytes(range(start, start + fragment.nbytes))


def test_store_commit_preserves_mixed_single_axis_descriptor() -> None:
    single_axis = TensorDescriptor(
        tensor_id="layers.0.attn.qkv",
        global_shape=(4,),
        dtype="uint8",
        itemsize=1,
        shard_dims=(0,),
        layer_id=0,
        expert_id=None,
        layout_fingerprint="framework:single-axis-contiguous",
        parallel_axes=(SplitAxis(kind="ep", dim=0),),
    )
    sources = multi_dim_store_manifests("source", source=True)
    placement_updates = {}
    binding_updates = {}
    for rank, binding in enumerate(sources.bindings):
        part = next(
            item
            for item in sources.placement.parts
            if item.participant_id == binding.participant_id
        )
        owner = (ctypes.c_ubyte * 1)(rank)
        placement_fragment_id = f"source-{rank}-single-axis-placement"
        single_axis_placement = PlacementFragment(
            placement_fragment_id=placement_fragment_id,
            tensor_id=single_axis.tensor_id,
            global_offset=(rank,),
            local_shape=(1,),
            nbytes=1,
            rank=part.rank,
        )
        single_axis_binding = RuntimeBindingFragment(
            placement_fragment_id=placement_fragment_id,
            fragment_id=f"source-{rank}-single-axis",
            address=ctypes.addressof(owner),
            nbytes=1,
            worker_id=binding.fragments[0].worker_id,
            endpoint=binding.fragments[0].endpoint,
            device="cuda:0",
            itemsize=1,
            local_shape=(1,),
            strides_bytes=(1,),
            storage_address=ctypes.addressof(owner),
            storage_nbytes=1,
            storage_offset_bytes=0,
            owner=owner,
        )
        placement_updates[part.participant_id] = (
            *part.fragments,
            single_axis_placement,
        )
        binding_updates[binding.participant_id] = (
            *binding.fragments,
            single_axis_binding,
        )
    sources = rebuild_runtime_inputs(
        sources,
        tensors=(*sources.placement.tensors, single_axis),
        placement_fragments=placement_updates,
        binding_fragments=binding_updates,
    )

    _store, weight_store = make_weight_store()
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    persisted = weight_store.commit_upload(
        upload_plan,
        upload_all(weight_store, upload_plan, sources),
    )
    loaded = weight_store.load_manifest(persisted.manifest_key)

    assert loaded == persisted == upload_plan.manifest
    loaded_single_axis = next(
        tensor for tensor in loaded.tensors if tensor.tensor_id == single_axis.tensor_id
    )
    assert loaded_single_axis.shard_dims == (0,)


def test_load_rejects_payloads_without_committed_manifest() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    targets = target_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    upload_all(weight_store, upload_plan, sources)
    load_plan = weight_store.plan_load(
        upload_plan.manifest,
        targets.placement,
        targets.bindings,
    )
    register_calls = store.register_calls
    range_get_calls = store.range_get_calls

    with pytest.raises(WeightStoreError, match="manifest is not committed"):
        weight_store.load(load_plan, targets[0].placement, targets[0].binding)

    assert store.register_calls == register_calls
    assert store.range_get_calls == range_get_calls


def test_store_multi_dim_lowering_limit_fails_before_registration_or_read() -> None:
    store, weight_store = make_weight_store(max_region_segments=5)
    sources = multi_dim_store_manifests("source", source=True)
    targets = multi_dim_store_manifests("target", source=False, target_dim=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    register_calls = store.register_calls
    range_get_calls = store.range_get_calls

    with pytest.raises(WeightStoreError, match="max_region_segments"):
        weight_store.load(load_plan, targets[0].placement, targets[0].binding)

    assert store.register_calls == register_calls
    assert store.range_get_calls == range_get_calls


def test_load_reshards_tp_and_fans_out_dp_across_target_participants() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=2, tp=2)
    upload_plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=3, tp=4)

    loaded_manifest = weight_store.load_manifest(manifest.manifest_key)
    load_plan = weight_store.plan_load(
        loaded_manifest, targets.placement, targets.bindings
    )
    load_all(weight_store, load_plan, targets)

    assert load_plan.transfer.total_bytes == 3 * 8
    assert store.range_get_calls == len(targets)
    for fragment in bound_fragments(targets):
        start = fragment.global_offset[0]
        assert bytes(fragment.owner) == bytes(range(start, start + fragment.nbytes))
    assert store.register_calls == 2 + len(targets)
    assert store.unregister_calls == 2 + len(targets)
    assert store.registered == set()


def test_store_round_trip_moves_layers_and_experts_across_all_parallel_axes() -> None:
    def make_manifests(*, dp: int, tp: int, source: bool):
        tensors = {}
        participants = []
        for layer_id, expert_id, dp_rank, tp_rank in product(
            range(2), range(2), range(dp), range(tp)
        ):
            tensor_index = layer_id * 2 + expert_id
            tensor = TensorDescriptor(
                tensor_id=f"layers.{layer_id}.experts.{expert_id}.w1",
                global_shape=(8,),
                dtype="uint8",
                itemsize=1,
                shard_dims=(0,),
                layer_id=layer_id,
                expert_id=expert_id,
                layout_fingerprint="sglang:qwen3.5:uint8:test",
                parallel_axes=(
                    ReplicatedAxis(kind="dp"),
                    OwnershipAxis(kind="pp"),
                    OwnershipAxis(kind="ep"),
                    SplitAxis(kind="tp", dim=0),
                ),
            )
            tensors[tensor.tensor_id] = tensor
            extent = 8 // tp
            offset = tp_rank * extent
            values = (
                range(tensor_index * 16 + offset, tensor_index * 16 + offset + extent)
                if source
                else [255] * extent
            )
            owner = (ctypes.c_ubyte * extent)(*values)
            prefix = "source" if source else "target"
            worker_id = f"{prefix}-l{layer_id}-e{expert_id}-d{dp_rank}-t{tp_rank}"
            placement_fragment_id = f"{worker_id}-placement"
            rank = ParallelRank(
                dp=dp_rank,
                tp=tp_rank,
                pp=layer_id if source else 1 - layer_id,
                ep=expert_id if source else 1 - expert_id,
            )
            participants.append(
                RuntimeParticipant(
                    participant_id=worker_id,
                    rank=rank,
                    instance_id=worker_id,
                    placement_fragments=(
                        PlacementFragment(
                            placement_fragment_id=placement_fragment_id,
                            tensor_id=tensor.tensor_id,
                            global_offset=(offset,),
                            local_shape=(extent,),
                            nbytes=extent,
                            rank=rank,
                        ),
                    ),
                    binding_fragments=(
                        RuntimeBindingFragment(
                            placement_fragment_id=placement_fragment_id,
                            fragment_id=f"{worker_id}-fragment",
                            address=ctypes.addressof(owner),
                            nbytes=extent,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=1,
                            local_shape=(extent,),
                            strides_bytes=(1,),
                            storage_address=ctypes.addressof(owner),
                            storage_nbytes=extent,
                            storage_offset_bytes=0,
                            owner=owner,
                        ),
                    ),
                )
            )
        return make_runtime_inputs(
            resource_id="qwen3.5-moe",
            revision="step-42",
            placement_set_id=f"{prefix}-all-axes-dp{dp}-tp{tp}",
            tensors=tuple(tensors.values()),
            participants=tuple(participants),
        )

    sources = make_manifests(dp=2, tp=2, source=True)
    targets = make_manifests(dp=3, tp=4, source=False)
    store, weight_store = make_weight_store()
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)

    load_all(weight_store, load_plan, targets)

    assert len(upload_plan.operations) == 4 * 2
    assert load_plan.transfer.total_bytes == 4 * 3 * 8
    for fragment in bound_fragments(targets):
        tensor = next(
            item
            for item in targets.placement.tensors
            if item.tensor_id == fragment.tensor_id
        )
        tensor_index = tensor.layer_id * 2 + tensor.expert_id
        begin = tensor_index * 16 + fragment.global_offset[0]
        assert bytes(fragment.owner) == bytes(range(begin, begin + fragment.nbytes))


def test_load_merges_store_fragments_for_larger_target_shards() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=4)
    upload_plan = weight_store.plan_upload(
        sources.placement, sources.bindings, namespace="default"
    )
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=2)

    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    load_all(weight_store, load_plan, targets)

    assert len(load_plan.transfer.operations) == 4
    target_fragments = bound_fragments(targets)
    assert bytes(target_fragments[0].owner) == bytes(range(4))
    assert bytes(target_fragments[1].owner) == bytes(range(4, 8))


def test_load_rejects_partial_range_result() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    original = store.get_into_ranges

    def partial(*args, **kwargs):
        results = original(*args, **kwargs)
        results[0][0][0] -= 1
        return results

    store.get_into_ranges = partial
    with pytest.raises(WeightStoreError, match="get_into_ranges failed"):
        weight_store.load(load_plan, targets[0].placement, targets[0].binding)


def test_load_surfaces_scalar_get_into_ranges_error() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    store.get_into_ranges = lambda *args, **kwargs: -19

    with pytest.raises(WeightStoreError, match="get_into_ranges failed: -19"):
        weight_store.load(load_plan, targets[0].placement, targets[0].binding)


def test_plan_load_rejects_different_target_weight_generation() -> None:
    _store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2, weight_generation=7)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan,
        upload_all(weight_store, upload_plan, sources),
    )
    targets = target_manifests(dp=1, tp=4, weight_generation=8)

    with pytest.raises(WeightStoreError, match="target placement revision mismatch"):
        weight_store.plan_load(manifest, targets.placement, targets.bindings)


def test_load_plan_rejects_transfer_weight_generation_mismatch() -> None:
    _store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2, weight_generation=7)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan,
        upload_all(weight_store, upload_plan, sources),
    )
    targets = target_manifests(dp=1, tp=4, weight_generation=7)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)

    # WeightLoadPlan remains fail-closed if a decoded/foreign plan has bypassed
    # TransferPlan construction. A normal replacement fails even earlier at the
    # live target attestation boundary.
    object.__setattr__(
        load_plan.transfer,
        "weight_generation",
        manifest.weight_generation + 1,
    )

    with pytest.raises(ValueError, match="load plan transfer and manifest identity"):
        WeightLoadPlan(
            manifest=manifest,
            transfer=load_plan.transfer,
        )


def test_plan_load_does_not_require_binding_for_empty_participant() -> None:
    _store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan,
        upload_all(weight_store, upload_plan, sources),
    )
    targets = with_empty_participant(
        target_manifests(dp=1, tp=1),
        participant_id="inactive-pp1",
        rank=ParallelRank(pp=1),
    )

    load_plan = weight_store.plan_load(
        manifest,
        targets.placement,
        targets.bindings,
    )

    assert len(load_plan.transfer.operations) == 1


def test_load_rejects_stale_target_generation() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    stale_binding = replace(targets.bindings[0], generation=2)

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, targets.placement, stale_binding)


def test_load_rejects_generation_scoped_target_id_rollover() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    replacement = replace(
        targets.bindings[0].fragments[0],
        fragment_id="replacement-target-fragment",
    )
    current = replace(
        targets.bindings[0],
        generation=2,
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, targets.placement, current)


def test_load_rejects_worker_and_generation_rollover_instead_of_succeeding_noop() -> (
    None
):
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    replacement = replace(
        targets.bindings[0].fragments[0],
        fragment_id="replacement-target-fragment",
        worker_id="replacement-target-worker",
    )
    current = replace(
        targets.bindings[0],
        instance_id="replacement-target-instance",
        generation=2,
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, targets.placement, current)


def test_load_chunks_large_ranges_to_bound_host_staging() -> None:
    """GPU range GET uses a host temporary per range, so each range is capped."""
    store, weight_store = make_weight_store(max_range_bytes=2)
    sources = source_manifests(dp=1, tp=1)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=1)

    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)
    weight_store.load(load_plan, targets[0].placement, targets[0].binding)

    assert store.range_sizes == [2, 2, 2, 2]
    assert bytes(bound_fragments(targets)[0].owner) == bytes(range(8))


def test_load_expands_strided_ranges_in_bounded_requests() -> None:
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(5, 8),
        dtype="uint8",
        itemsize=1,
        shard_dims=(1,),
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
        parallel_axes=(SplitAxis(kind="tp", dim=1),),
    )

    def make_manifests(tp: int, prefix: str, *, source: bool):
        participants = []
        extent = tensor.global_shape[1] // tp
        for tp_rank in range(tp):
            values = []
            for row in range(tensor.global_shape[0]):
                begin = row * tensor.global_shape[1] + tp_rank * extent
                values.extend(range(begin, begin + extent))
            if not source:
                values = [255] * len(values)
            owner = (ctypes.c_ubyte * len(values))(*values)
            worker_id = f"{prefix}-t{tp_rank}"
            placement_fragment_id = f"{worker_id}-placement"
            nbytes = prod((tensor.global_shape[0], extent))
            rank = ParallelRank(tp=tp_rank)
            participants.append(
                RuntimeParticipant(
                    participant_id=worker_id,
                    rank=rank,
                    instance_id=worker_id,
                    placement_fragments=(
                        PlacementFragment(
                            placement_fragment_id=placement_fragment_id,
                            tensor_id=tensor.tensor_id,
                            global_offset=(0, tp_rank * extent),
                            local_shape=(tensor.global_shape[0], extent),
                            nbytes=nbytes,
                            rank=rank,
                        ),
                    ),
                    binding_fragments=(
                        RuntimeBindingFragment(
                            placement_fragment_id=placement_fragment_id,
                            fragment_id=f"{worker_id}-fragment",
                            address=ctypes.addressof(owner),
                            nbytes=nbytes,
                            worker_id=worker_id,
                            endpoint=f"{worker_id}:12345",
                            device="cuda:0",
                            itemsize=1,
                            local_shape=(tensor.global_shape[0], extent),
                            strides_bytes=(extent, 1),
                            storage_address=ctypes.addressof(owner),
                            storage_nbytes=nbytes,
                            storage_offset_bytes=0,
                            owner=owner,
                        ),
                    ),
                )
            )
        return make_runtime_inputs(
            resource_id="qwen3.5-0.8b",
            revision="step-42",
            placement_set_id=f"{prefix}-strided-tp{tp}",
            tensors=(tensor,),
            participants=tuple(participants),
        )

    sources = make_manifests(2, "source", source=True)
    targets = make_manifests(4, "target", source=False)
    store, weight_store = make_weight_store(max_ranges_per_request=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    load_plan = weight_store.plan_load(manifest, targets.placement, targets.bindings)

    load_all(weight_store, load_plan, targets)

    for tp_rank, target in enumerate(bound_fragments(targets)):
        expected = []
        for row in range(tensor.global_shape[0]):
            begin = row * tensor.global_shape[1] + tp_rank * 2
            expected.extend(range(begin, begin + 2))
        assert bytes(target.owner) == bytes(expected)
    assert max(store.range_batch_sizes) <= 2
    assert store.range_get_calls == 12


def test_load_batches_multiple_local_target_buffers_in_one_request() -> None:
    tensors = tuple(
        TensorDescriptor(
            tensor_id=f"layers.{index}.norm.weight",
            global_shape=(4,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(),
            layer_id=index,
            layout_fingerprint="sglang:qwen3.5:uint8:test",
            parallel_axes=(),
        )
        for index in range(4)
    )

    def make_manifest(prefix: str, *, source: bool) -> RuntimeInputs:
        placement_fragments = []
        binding_fragments = []
        for index, tensor in enumerate(tensors):
            values = range(index * 4, index * 4 + 4) if source else [255] * 4
            owner = (ctypes.c_ubyte * 4)(*values)
            placement_fragment_id = f"{prefix}-{index}-placement"
            placement_fragments.append(
                PlacementFragment(
                    placement_fragment_id=placement_fragment_id,
                    tensor_id=tensor.tensor_id,
                    global_offset=(0,),
                    local_shape=(4,),
                    nbytes=4,
                    rank=ParallelRank(),
                )
            )
            binding_fragments.append(
                RuntimeBindingFragment(
                    placement_fragment_id=placement_fragment_id,
                    fragment_id=f"{prefix}-{index}",
                    address=ctypes.addressof(owner),
                    nbytes=4,
                    worker_id=prefix,
                    endpoint=f"{prefix}:12345",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=ctypes.addressof(owner),
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                    owner=owner,
                )
            )
        return make_runtime_inputs(
            resource_id="qwen3.5-0.8b",
            revision="step-42",
            placement_set_id=f"{prefix}-multi-buffer",
            tensors=tensors,
            participants=(
                RuntimeParticipant(
                    participant_id=prefix,
                    rank=ParallelRank(),
                    placement_fragments=tuple(placement_fragments),
                    binding_fragments=tuple(binding_fragments),
                    instance_id=prefix,
                ),
            ),
        )

    source = make_manifest("source", source=True)
    target = make_manifest("target", source=False)
    store, weight_store = make_weight_store()
    upload_plan = weight_store.plan_upload(source.placement, source.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, source)
    )
    load_plan = weight_store.plan_load(manifest, target.placement, target.bindings)

    weight_store.load(load_plan, target.placement, target.binding)

    assert store.range_get_calls == 1
    assert store.range_batch_sizes == [4]
    for index, fragment in enumerate(bound_fragments(target)):
        assert bytes(fragment.owner) == bytes(range(index * 4, index * 4 + 4))


def test_one_participant_binding_can_execute_multiple_fragments() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.plan_upload(sources.placement, sources.bindings)
    manifest = weight_store.commit_upload(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    target_ranks = target_manifests(dp=1, tp=2)
    target = coalesce_runtime_inputs(
        target_ranks,
        instance_id="combined-target",
        worker_id="combined-target",
    )

    load_plan = weight_store.plan_load(manifest, target.placement, target.bindings)
    weight_store.load(load_plan, target.placement, target.binding)

    assert store.range_get_calls == 1
    assert [bytes(fragment.owner) for fragment in bound_fragments(target)] == [
        bytes(range(4)),
        bytes(range(4, 8)),
    ]
