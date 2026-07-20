from __future__ import annotations

import ctypes
from dataclasses import dataclass, replace
from itertools import product
from math import prod

import pytest

from mooncake.weight_transfer.manifest import (
    ParallelRank,
    RuntimeFragment,
    RuntimeManifest,
    TensorDescriptor,
    WeightManifest,
)
from mooncake.weight_transfer.store import (
    UploadReceipt,
    WeightStore,
    WeightStoreError,
)


@dataclass
class FakeReplicateConfig:
    group_ids: list[str]
    data_type: str
    with_hard_pin: bool


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.group_ids: dict[str, str] = {}
        self.configs: dict[str, tuple[str, bool]] = {}
        self.registered: set[int] = set()
        self.register_calls = 0
        self.register_args: list[tuple[int, int]] = []
        self.unregister_calls = 0
        self.unregister_addresses: list[int] = []
        self.fail_key: str | None = None
        self.processing_keys: set[str] = set()
        self.range_get_calls = 0
        self.range_sizes: list[int] = []
        self.range_batch_sizes: list[int] = []
        self.exist_batch_sizes: list[int] = []
        self.register_result = 0
        self.removed_keys: list[str] = []
        self.remove_forces: list[bool] = []
        self.unregister_results: dict[int, int] = {}
        self.unregister_exceptions: dict[int, Exception] = {}
        self.fail_after_write_key: str | None = None
        self.manifest_race_value: bytes | None = None
        self.manifest_race_key: str | None = None
        self.after_batch_is_exist = None

    def register_buffer(self, address: int, nbytes: int) -> int:
        self.register_calls += 1
        self.register_args.append((address, nbytes))
        if self.register_result != 0:
            return self.register_result
        self.registered.add(address)
        return 0

    def unregister_buffer(self, address: int) -> int:
        self.unregister_calls += 1
        self.unregister_addresses.append(address)
        if address in self.unregister_exceptions:
            raise self.unregister_exceptions[address]
        result = self.unregister_results.get(address, 0)
        if result == 0:
            self.registered.remove(address)
        return result

    def batch_put_from(
        self,
        keys: list[str],
        addresses: list[int],
        sizes: list[int],
        config: FakeReplicateConfig,
    ) -> list[int]:
        results = []
        for key, address, size, group_id in zip(
            keys, addresses, sizes, config.group_ids
        ):
            if key == self.fail_key:
                results.append(-1)
                continue
            if key in self.processing_keys:
                results.append(0)
                continue
            self.objects[key] = ctypes.string_at(address, size)
            self.group_ids[key] = group_id
            self.configs[key] = (config.data_type, config.with_hard_pin)
            results.append(0)
        return results

    def put(self, key: str, value, config: FakeReplicateConfig) -> int:
        if self.manifest_race_value is not None and key == self.manifest_race_key:
            self.objects[key] = self.manifest_race_value
            self.group_ids[key] = config.group_ids[0]
            self.configs[key] = (config.data_type, config.with_hard_pin)
            self.manifest_race_value = None
            return 0
        if key == self.fail_key:
            return -1
        if key in self.processing_keys:
            return 0
        if key in self.objects:
            return 0
        self.objects[key] = bytes(value)
        self.group_ids[key] = config.group_ids[0]
        self.configs[key] = (config.data_type, config.with_hard_pin)
        if key == self.fail_after_write_key:
            return -1
        return 0

    def get(self, key: str) -> bytes:
        return self.objects[key]

    def is_exist(self, key: str) -> int:
        return int(key in self.objects)

    def batch_is_exist(self, keys: list[str]) -> list[int]:
        self.exist_batch_sizes.append(len(keys))
        results = [self.is_exist(key) for key in keys]
        callback = self.after_batch_is_exist
        self.after_batch_is_exist = None
        if callback is not None:
            callback()
        return results

    def remove(self, key: str, force: bool = False) -> int:
        self.removed_keys.append(key)
        self.remove_forces.append(force)
        self.objects.pop(key, None)
        self.processing_keys.discard(key)
        self.group_ids.pop(key, None)
        return 0

    def get_into_ranges(
        self,
        addresses: list[int],
        all_keys: list[list[str]],
        all_dst_offsets: list[list[list[int]]],
        all_src_offsets: list[list[list[int]]],
        all_sizes: list[list[list[int]]],
    ) -> list[list[list[int]]]:
        self.range_get_calls += 1
        self.range_batch_sizes.append(
            sum(len(sizes) for buffer in all_sizes for sizes in buffer)
        )
        results = []
        for address, keys, dst_offsets, src_offsets, sizes in zip(
            addresses,
            all_keys,
            all_dst_offsets,
            all_src_offsets,
            all_sizes,
        ):
            buffer_results = []
            for key, dst_group, src_group, size_group in zip(
                keys, dst_offsets, src_offsets, sizes
            ):
                object_data = self.objects[key]
                range_results = []
                for dst, src, size in zip(dst_group, src_group, size_group):
                    self.range_sizes.append(size)
                    ctypes.memmove(address + dst, object_data[src : src + size], size)
                    range_results.append(size)
                buffer_results.append(range_results)
            results.append(buffer_results)
        return results


def tensor_descriptor() -> TensorDescriptor:
    return TensorDescriptor(
        tensor_id="layers.2.experts.3.w1",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        partition_dim=0,
        layer_id=2,
        expert_id=3,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
    )


def source_manifests(dp: int = 2, tp: int = 2) -> tuple[RuntimeManifest, ...]:
    tensor = tensor_descriptor()
    extent = tensor.global_shape[0] // tp
    manifests = []
    for dp_rank in range(dp):
        for tp_rank in range(tp):
            start = tp_rank * extent
            owner = (ctypes.c_ubyte * extent)(*range(start, start + extent))
            worker_id = f"source-d{dp_rank}-t{tp_rank}"
            fragment = RuntimeFragment(
                fragment_id=f"{worker_id}-fragment",
                tensor_id=tensor.tensor_id,
                global_offset=(start,),
                local_shape=(extent,),
                address=ctypes.addressof(owner),
                nbytes=extent,
                worker_id=worker_id,
                endpoint=f"{worker_id}:12345",
                rank=ParallelRank(dp=dp_rank, tp=tp_rank, pp=1, ep=1),
                lease_generation=1,
                owner=owner,
            )
            manifests.append(
                RuntimeManifest(
                    model_id="qwen3.5-0.8b",
                    revision="step-42",
                    instance_id=worker_id,
                    tensors=(tensor,),
                    fragments=(fragment,),
                )
            )
    return tuple(manifests)


def target_manifests(
    dp: int = 3, tp: int = 4, pp_rank: int = 3, ep_rank: int = 7
) -> tuple[RuntimeManifest, ...]:
    tensor = tensor_descriptor()
    extent = tensor.global_shape[0] // tp
    manifests = []
    for dp_rank in range(dp):
        for tp_rank in range(tp):
            owner = (ctypes.c_ubyte * extent)(*[255] * extent)
            worker_id = f"target-d{dp_rank}-t{tp_rank}"
            fragment = RuntimeFragment(
                fragment_id=f"{worker_id}-fragment",
                tensor_id=tensor.tensor_id,
                global_offset=(tp_rank * extent,),
                local_shape=(extent,),
                address=ctypes.addressof(owner),
                nbytes=extent,
                worker_id=worker_id,
                endpoint=f"{worker_id}:12345",
                rank=ParallelRank(
                    dp=dp_rank,
                    tp=tp_rank,
                    pp=pp_rank,
                    ep=ep_rank,
                ),
                lease_generation=1,
                owner=owner,
            )
            manifests.append(
                RuntimeManifest(
                    model_id="qwen3.5-0.8b",
                    revision="step-42",
                    instance_id=worker_id,
                    tensors=(tensor,),
                    fragments=(fragment,),
                )
            )
    return tuple(manifests)


def make_weight_store(
    store: InMemoryStore | None = None,
    *,
    max_range_bytes: int = 64 * 1024 * 1024,
    max_ranges_per_request: int = 1024,
):
    current = store or InMemoryStore()
    return current, WeightStore(
        current,
        config_factory=lambda group_ids, record_type: FakeReplicateConfig(
            list(group_ids),
            data_type=("WEIGHT" if record_type == "payload" else "METADATA"),
            with_hard_pin=True,
        ),
        max_range_bytes=max_range_bytes,
        max_ranges_per_request=max_ranges_per_request,
    )


def upload_all(weight_store: WeightStore, plan, manifests):
    receipts = []
    try:
        for manifest in manifests:
            receipts.extend(weight_store.upload(plan, manifest))
    except Exception:
        weight_store.abort_upload(plan, receipts)
        raise
    return receipts


def test_upload_deduplicates_dp_and_commits_manifest_last() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()

    plan = weight_store.prepare_upload(sources, namespace="default")
    receipts = upload_all(weight_store, plan, sources)

    assert len(plan.operations) == 2
    assert len(receipts) == 2
    assert plan.manifest.manifest_key not in store.objects
    assert len(store.objects) == 2

    manifest = weight_store.commit(plan, receipts)

    assert WeightManifest.from_json(store.objects[manifest.manifest_key]) == manifest
    assert len(store.objects) == 4
    revision_keys = {
        manifest.manifest_key,
        *(operation.target.object_key for operation in plan.operations),
    }
    assert {store.group_ids[key] for key in revision_keys} == {manifest.group_id}
    assert store.group_ids[plan.control_key] == plan.session_group_id
    assert plan.session_group_id != manifest.group_id
    assert store.register_calls == 2
    assert store.unregister_calls == 2
    assert store.registered == set()


def test_weight_group_objects_are_hard_pinned_and_typed() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)

    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    weight_store.commit(plan, receipts)

    payload_keys = {operation.target.object_key for operation in plan.operations}
    assert {store.configs[key] for key in payload_keys} == {("WEIGHT", True)}
    assert store.configs[plan.control_key] == ("METADATA", True)
    assert store.configs[plan.manifest.manifest_key] == ("METADATA", True)


def test_finalize_upload_session_keeps_committed_revision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit(plan, receipts)

    weight_store.finalize_upload_session(plan)
    weight_store.finalize_upload_session(plan)

    assert plan.control_key in store.objects
    assert manifest.manifest_key in store.objects
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )


def test_commit_rejects_incomplete_receipts() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.prepare_upload(sources, namespace="default")
    receipts = upload_all(weight_store, plan, sources)

    with pytest.raises(WeightStoreError, match="missing upload receipts"):
        weight_store.commit(plan, receipts[:-1])

    assert plan.manifest.manifest_key not in store.objects


def test_upload_waits_for_complete_payload_before_returning_receipt() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.prepare_upload(sources)
    key = plan.operations[0].target.object_key
    store.processing_keys.add(key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.upload(plan, sources[0])

    assert key in store.processing_keys
    assert store.removed_keys == []


def test_commit_rechecks_every_payload_after_receipts_are_issued() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.objects.pop(receipts[0].object_key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.commit(plan, receipts)

    assert plan.manifest.manifest_key not in store.objects


def test_incomplete_payload_does_not_lock_upload_into_commit_decision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.objects.pop(receipts[0].object_key)

    with pytest.raises(WeightStoreError, match="payload is not complete"):
        weight_store.commit(plan, receipts)

    weight_store.abort_upload(plan, receipts)
    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )


def test_payload_completion_queries_are_bounded() -> None:
    store, weight_store = make_weight_store(max_ranges_per_request=1)
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.exist_batch_sizes.clear()

    weight_store.commit(plan, receipts)

    assert store.exist_batch_sizes == [1, 1, 1, 1]


def test_abort_cleans_the_whole_plan_when_a_receipt_was_lost() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
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
    plan = weight_store.prepare_upload(sources)
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
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.processing_keys.add(plan.manifest.manifest_key)

    with pytest.raises(WeightStoreError, match="manifest put failed"):
        weight_store.commit(plan, receipts)
    with pytest.raises(WeightStoreError, match="already chose commit"):
        weight_store.abort_upload(plan, receipts)

    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert store.removed_keys == []


def test_upload_fails_and_cleans_up_when_abort_wins_after_complete_check() -> None:
    store, weight_store = make_weight_store()
    source = source_manifests(dp=1, tp=1)[0]
    plan = weight_store.prepare_upload((source,))
    store.after_batch_is_exist = lambda: weight_store.abort_upload(plan, ())

    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.upload(plan, source)

    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )


def test_payload_failure_does_not_publish_manifest() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.prepare_upload(sources, namespace="default")
    store.fail_key = plan.operations[1].target.object_key

    with pytest.raises(WeightStoreError, match="batch_put_from"):
        upload_all(weight_store, plan, sources)

    assert plan.manifest.manifest_key not in store.objects
    assert not any("/payload/" in key for key in store.objects)
    assert set(store.removed_keys) == {
        operation.target.object_key for operation in plan.operations
    }
    assert all(store.remove_forces)


def test_commit_is_idempotent_for_the_same_upload_plan() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests()
    plan = weight_store.prepare_upload(sources, namespace="default")
    receipts = upload_all(weight_store, plan, sources)
    first = weight_store.commit(plan, receipts)

    assert weight_store.commit(plan, receipts) == first
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )


def test_commit_conflict_keeps_winner_and_force_cleans_loser_payloads() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    winner = weight_store.prepare_upload(sources)
    winner_receipts = upload_all(weight_store, winner, sources)
    weight_store.commit(winner, winner_receipts)
    loser = weight_store.prepare_upload(sources)
    loser_receipts = upload_all(weight_store, loser, sources)

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit(loser, loser_receipts)

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
    winner = weight_store.prepare_upload(sources)
    winner_receipts = upload_all(weight_store, winner, sources)
    winner_manifest = weight_store.commit(winner, winner_receipts)
    loser = weight_store.prepare_upload(sources)
    loser_receipts = upload_all(weight_store, loser, sources)

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit(loser, loser_receipts)
    assert loser.control_key in store.objects

    weight_store.finalize_upload_session(loser)

    assert loser.control_key in store.objects
    assert weight_store.load_manifest(winner_manifest.manifest_key) == winner_manifest
    assert all(
        fragment.object_key in store.objects for fragment in winner_manifest.fragments
    )


def test_conflict_cleanup_preserves_payloads_referenced_by_winner() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    winner = replace(plan.manifest, created_at="2026-07-19T00:00:00Z")
    store.objects[plan.manifest.manifest_key] = winner.to_json().encode()

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit(plan, receipts)
    weight_store.finalize_upload_session(plan)

    assert all(fragment.object_key in store.objects for fragment in winner.fragments)
    assert plan.control_key in store.objects


def test_commit_finalize_rejects_late_abort_and_preserves_ready_revision() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit(plan, receipts)
    weight_store.finalize_upload_session(plan)

    with pytest.raises(WeightStoreError, match="published weight revision"):
        weight_store.abort_upload(plan, receipts)

    assert weight_store.load_manifest(manifest.manifest_key) == manifest
    assert all(fragment.object_key in store.objects for fragment in manifest.fragments)
    assert plan.control_key in store.objects


def test_abort_finalize_rejects_late_upload_and_commit() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    weight_store.abort_upload(plan, receipts)
    weight_store.finalize_upload_session(plan)

    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.upload(plan, sources[0])
    with pytest.raises(WeightStoreError, match="already chose abort"):
        weight_store.commit(plan, receipts)

    assert plan.manifest.manifest_key not in store.objects
    assert all(
        operation.target.object_key not in store.objects
        for operation in plan.operations
    )
    assert plan.control_key in store.objects


def test_abort_checks_ready_manifest_when_commit_tombstone_was_lost() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    manifest = weight_store.commit(plan, receipts)
    store.objects.pop(plan.control_key)

    with pytest.raises(WeightStoreError, match="published weight revision"):
        weight_store.abort_upload(plan, receipts)

    assert weight_store.load_manifest(manifest.manifest_key) == manifest
    assert all(fragment.object_key in store.objects for fragment in manifest.fragments)


def test_commit_detects_a_concurrent_winner_after_manifest_preflight() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    winner = weight_store.prepare_upload(sources)
    loser = weight_store.prepare_upload(sources)
    winner_receipts = upload_all(weight_store, winner, sources)
    loser_receipts = upload_all(weight_store, loser, sources)
    assert len(winner_receipts) == len(loser_receipts)
    store.manifest_race_key = loser.manifest.manifest_key
    store.manifest_race_value = winner.manifest.to_json().encode()

    with pytest.raises(WeightStoreError, match="conflicting weight revision"):
        weight_store.commit(loser, loser_receipts)

    assert weight_store.load_manifest(winner.manifest.manifest_key) == winner.manifest
    assert all(
        operation.target.object_key in store.objects for operation in winner.operations
    )
    assert all(
        operation.target.object_key not in store.objects
        for operation in loser.operations
    )


def test_upload_rejects_stale_runtime_fragment() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    stale_fragment = RuntimeFragment(
        **{
            **sources[0].fragments[0].__dict__,
            "lease_generation": 2,
        }
    )
    stale_manifest = RuntimeManifest(
        model_id=sources[0].model_id,
        revision=sources[0].revision,
        instance_id=sources[0].instance_id,
        tensors=sources[0].tensors,
        fragments=(stale_fragment,),
    )

    with pytest.raises(WeightStoreError, match="stale source fragment"):
        weight_store.upload(plan, stale_manifest)


def test_upload_rejects_generation_scoped_fragment_id_rollover() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    replacement = RuntimeFragment(
        **{
            **sources[0].fragments[0].__dict__,
            "fragment_id": "replacement-fragment",
            "lease_generation": 2,
        }
    )
    current = RuntimeManifest(
        model_id=sources[0].model_id,
        revision=sources[0].revision,
        instance_id=sources[0].instance_id,
        tensors=sources[0].tensors,
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="missing planned source fragment"):
        weight_store.upload(plan, current)


def test_register_invalid_params_is_not_treated_as_already_registered() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=1)
    plan = weight_store.prepare_upload(sources)
    store.register_result = -600

    with pytest.raises(WeightStoreError, match="register_buffer failed"):
        weight_store.upload(plan, sources[0])


def test_registration_uses_largest_extent_for_fragments_with_same_address() -> None:
    small = TensorDescriptor(
        tensor_id="a.small",
        global_shape=(4,),
        dtype="uint8",
        itemsize=1,
        partition_dim=None,
    )
    large = TensorDescriptor(
        tensor_id="b.large",
        global_shape=(8,),
        dtype="uint8",
        itemsize=1,
        partition_dim=None,
    )
    owner = (ctypes.c_ubyte * 8)(*range(8))
    address = ctypes.addressof(owner)
    fragments = tuple(
        RuntimeFragment(
            fragment_id=f"fragment-{tensor.tensor_id}",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=tensor.global_shape,
            address=address,
            nbytes=tensor.global_shape[0],
            worker_id="source",
            endpoint="source:12345",
            rank=ParallelRank(),
            lease_generation=1,
            owner=owner,
        )
        for tensor in (small, large)
    )
    source = RuntimeManifest(
        model_id="qwen",
        revision="rev",
        instance_id="source",
        tensors=(small, large),
        fragments=fragments,
    )
    store, weight_store = make_weight_store()
    plan = weight_store.prepare_upload((source,))

    weight_store.upload(plan, source)

    assert store.register_args == [(address, 8)]


def test_commit_rejects_duplicate_or_forged_receipts() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)

    with pytest.raises(WeightStoreError, match="duplicate upload receipt"):
        weight_store.commit(plan, [*receipts, receipts[0]])

    forged = UploadReceipt(
        fragment_id=receipts[0].fragment_id,
        object_key="forged",
        worker_id=receipts[0].worker_id,
    )
    with pytest.raises(WeightStoreError, match="invalid upload receipt"):
        weight_store.commit(plan, [forged, receipts[1]])


def test_manifest_put_failure_keeps_payloads_for_an_idempotent_retry() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.fail_key = plan.manifest.manifest_key

    with pytest.raises(WeightStoreError, match="manifest put failed"):
        weight_store.commit(plan, receipts)

    assert all(receipt.object_key in store.objects for receipt in receipts)
    store.fail_key = None
    assert weight_store.commit(plan, receipts) == plan.manifest


def test_commit_recovers_when_manifest_response_is_lost_after_write() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    plan = weight_store.prepare_upload(sources)
    receipts = upload_all(weight_store, plan, sources)
    store.fail_after_write_key = plan.manifest.manifest_key

    assert weight_store.commit(plan, receipts) == plan.manifest
    assert weight_store.load_manifest(plan.manifest.manifest_key) == plan.manifest
    assert all(receipt.object_key in store.objects for receipt in receipts)


def test_load_reshards_tp_and_fans_out_dp_with_new_pp_ep_owners() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=2, tp=2)
    upload_plan = weight_store.prepare_upload(sources, namespace="default")
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=3, tp=4, pp_rank=3, ep_rank=7)

    loaded_manifest = weight_store.load_manifest(manifest.manifest_key)
    load_plan = weight_store.plan_load(loaded_manifest, targets)
    for target in targets:
        weight_store.load(load_plan, target)

    assert load_plan.transfer.total_bytes == 3 * 8
    assert store.range_get_calls == len(targets)
    for target in targets:
        fragment = target.fragments[0]
        start = fragment.global_offset[0]
        assert bytes(fragment.owner) == bytes(range(start, start + fragment.nbytes))
    assert store.register_calls == 2 + len(targets)
    assert store.unregister_calls == 2 + len(targets)
    assert store.registered == set()


def test_store_round_trip_moves_layers_and_experts_across_all_parallel_axes() -> None:
    def make_manifests(*, dp: int, tp: int, source: bool):
        manifests = []
        for layer_id, expert_id, dp_rank, tp_rank in product(
            range(2), range(2), range(dp), range(tp)
        ):
            tensor_index = layer_id * 2 + expert_id
            tensor = TensorDescriptor(
                tensor_id=f"layers.{layer_id}.experts.{expert_id}.w1",
                global_shape=(8,),
                dtype="uint8",
                itemsize=1,
                partition_dim=0,
                layer_id=layer_id,
                expert_id=expert_id,
                layout_fingerprint="sglang:qwen3.5:uint8:test",
            )
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
            fragment = RuntimeFragment(
                fragment_id=f"{worker_id}-fragment",
                tensor_id=tensor.tensor_id,
                global_offset=(offset,),
                local_shape=(extent,),
                address=ctypes.addressof(owner),
                nbytes=extent,
                worker_id=worker_id,
                endpoint=f"{worker_id}:12345",
                rank=ParallelRank(
                    dp=dp_rank,
                    tp=tp_rank,
                    pp=layer_id if source else 1 - layer_id,
                    ep=expert_id if source else 1 - expert_id,
                ),
                lease_generation=1,
                owner=owner,
            )
            manifests.append(
                RuntimeManifest(
                    model_id="qwen3.5-moe",
                    revision="step-42",
                    instance_id=worker_id,
                    tensors=(tensor,),
                    fragments=(fragment,),
                )
            )
        return tuple(manifests)

    sources = make_manifests(dp=2, tp=2, source=True)
    targets = make_manifests(dp=3, tp=4, source=False)
    store, weight_store = make_weight_store()
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    load_plan = weight_store.plan_load(manifest, targets)

    for target in targets:
        weight_store.load(load_plan, target)

    assert len(upload_plan.operations) == 4 * 2
    assert load_plan.transfer.total_bytes == 4 * 3 * 8
    for target in targets:
        fragment = target.fragments[0]
        tensor = next(
            item for item in target.tensors if item.tensor_id == fragment.tensor_id
        )
        tensor_index = tensor.layer_id * 2 + tensor.expert_id
        begin = tensor_index * 16 + fragment.global_offset[0]
        assert bytes(fragment.owner) == bytes(range(begin, begin + fragment.nbytes))


def test_load_merges_store_fragments_for_larger_target_shards() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=4)
    upload_plan = weight_store.prepare_upload(sources, namespace="default")
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=2, pp_rank=0, ep_rank=0)

    load_plan = weight_store.plan_load(manifest, targets)
    for target in targets:
        weight_store.load(load_plan, target)

    assert len(load_plan.transfer.operations) == 4
    assert bytes(targets[0].fragments[0].owner) == bytes(range(4))
    assert bytes(targets[1].fragments[0].owner) == bytes(range(4, 8))


def test_load_rejects_partial_range_result() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets)
    original = store.get_into_ranges

    def partial(*args, **kwargs):
        results = original(*args, **kwargs)
        results[0][0][0] -= 1
        return results

    store.get_into_ranges = partial
    with pytest.raises(WeightStoreError, match="get_into_ranges failed"):
        weight_store.load(load_plan, targets[0])


def test_load_rejects_stale_target_generation() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets)
    stale_fragment = RuntimeFragment(
        **{
            **targets[0].fragments[0].__dict__,
            "lease_generation": 2,
        }
    )
    stale_manifest = RuntimeManifest(
        model_id=targets[0].model_id,
        revision=targets[0].revision,
        instance_id=targets[0].instance_id,
        tensors=targets[0].tensors,
        fragments=(stale_fragment,),
    )

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, stale_manifest)


def test_load_rejects_generation_scoped_target_id_rollover() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets)
    replacement = RuntimeFragment(
        **{
            **targets[0].fragments[0].__dict__,
            "fragment_id": "replacement-target-fragment",
            "lease_generation": 2,
        }
    )
    current = RuntimeManifest(
        model_id=targets[0].model_id,
        revision=targets[0].revision,
        instance_id=targets[0].instance_id,
        tensors=targets[0].tensors,
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, current)


def test_load_rejects_worker_and_generation_rollover_instead_of_succeeding_noop() -> (
    None
):
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=4)
    load_plan = weight_store.plan_load(manifest, targets)
    replacement = replace(
        targets[0].fragments[0],
        fragment_id="replacement-target-fragment",
        worker_id="replacement-target-worker",
        lease_generation=2,
    )
    current = replace(
        targets[0],
        instance_id="replacement-target-instance",
        fragments=(replacement,),
    )

    with pytest.raises(WeightStoreError, match="target executor snapshot mismatch"):
        weight_store.load(load_plan, current)


def test_upload_unregisters_every_buffer_without_deleting_unowned_payloads() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = RuntimeManifest(
        model_id=sources[0].model_id,
        revision=sources[0].revision,
        instance_id="combined-source",
        tensors=sources[0].tensors,
        fragments=tuple(
            fragment for manifest in sources for fragment in manifest.fragments
        ),
    )
    plan = weight_store.prepare_upload((combined,))
    first_address = combined.fragments[0].address
    store.unregister_results[first_address] = -9

    with pytest.raises(WeightStoreError, match="unregister_buffer failed"):
        weight_store.upload(plan, combined)

    assert store.unregister_calls == 2
    assert all(
        operation.target.object_key in store.objects for operation in plan.operations
    )
    assert store.remove_forces == []


def test_unregister_failure_does_not_mask_payload_transfer_failure() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = RuntimeManifest(
        model_id=sources[0].model_id,
        revision=sources[0].revision,
        instance_id="combined-source",
        tensors=sources[0].tensors,
        fragments=tuple(
            fragment for manifest in sources for fragment in manifest.fragments
        ),
    )
    plan = weight_store.prepare_upload((combined,))
    store.fail_key = plan.operations[1].target.object_key
    store.unregister_results[combined.fragments[0].address] = -9

    with pytest.raises(WeightStoreError) as error:
        weight_store.upload(plan, combined)

    assert "batch_put_from failed" in str(error.value)
    assert "unregister_buffer failed" in str(error.value)
    assert store.unregister_calls == 2
    assert plan.operations[0].target.object_key in store.objects
    assert store.remove_forces == []


def test_unregister_attempts_every_buffer_when_cleanup_raises() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    combined = RuntimeManifest(
        model_id=sources[0].model_id,
        revision=sources[0].revision,
        instance_id="combined-source",
        tensors=sources[0].tensors,
        fragments=tuple(
            fragment for manifest in sources for fragment in manifest.fragments
        ),
    )
    plan = weight_store.prepare_upload((combined,))
    first, second = combined.fragments
    store.fail_key = plan.operations[1].target.object_key
    store.unregister_exceptions[first.address] = RuntimeError("unregister exploded")
    store.unregister_results[second.address] = -9

    with pytest.raises(WeightStoreError) as error:
        weight_store.upload(plan, combined)

    assert "batch_put_from failed" in str(error.value)
    assert "unregister exploded" in str(error.value)
    assert "-9" in str(error.value)
    assert store.unregister_addresses == [second.address, first.address]


def test_load_chunks_large_ranges_to_bound_host_staging() -> None:
    """GPU range GET uses a host temporary per range, so each range is capped."""
    store, weight_store = make_weight_store(max_range_bytes=2)
    sources = source_manifests(dp=1, tp=1)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    targets = target_manifests(dp=1, tp=1)

    load_plan = weight_store.plan_load(manifest, targets)
    weight_store.load(load_plan, targets[0])

    assert store.range_sizes == [2, 2, 2, 2]
    assert bytes(targets[0].fragments[0].owner) == bytes(range(8))


def test_load_expands_strided_ranges_in_bounded_requests() -> None:
    tensor = TensorDescriptor(
        tensor_id="layers.0.mlp.down_proj.weight",
        global_shape=(5, 8),
        dtype="uint8",
        itemsize=1,
        partition_dim=1,
        layer_id=0,
        layout_fingerprint="sglang:qwen3.5:uint8:test",
    )

    def make_manifests(tp: int, prefix: str, *, source: bool):
        manifests = []
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
            fragment = RuntimeFragment(
                fragment_id=f"{worker_id}-fragment",
                tensor_id=tensor.tensor_id,
                global_offset=(0, tp_rank * extent),
                local_shape=(tensor.global_shape[0], extent),
                address=ctypes.addressof(owner),
                nbytes=prod((tensor.global_shape[0], extent)),
                worker_id=worker_id,
                endpoint=f"{worker_id}:12345",
                rank=ParallelRank(tp=tp_rank),
                lease_generation=1,
                owner=owner,
            )
            manifests.append(
                RuntimeManifest(
                    model_id="qwen3.5-0.8b",
                    revision="step-42",
                    instance_id=worker_id,
                    tensors=(tensor,),
                    fragments=(fragment,),
                )
            )
        return tuple(manifests)

    sources = make_manifests(2, "source", source=True)
    targets = make_manifests(4, "target", source=False)
    store, weight_store = make_weight_store(max_ranges_per_request=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    load_plan = weight_store.plan_load(manifest, targets)

    for target in targets:
        weight_store.load(load_plan, target)

    for tp_rank, target in enumerate(targets):
        expected = []
        for row in range(tensor.global_shape[0]):
            begin = row * tensor.global_shape[1] + tp_rank * 2
            expected.extend(range(begin, begin + 2))
        assert bytes(target.fragments[0].owner) == bytes(expected)
    assert max(store.range_batch_sizes) <= 2
    assert store.range_get_calls == 12


def test_load_batches_multiple_local_target_buffers_in_one_request() -> None:
    tensors = tuple(
        TensorDescriptor(
            tensor_id=f"layers.{index}.norm.weight",
            global_shape=(4,),
            dtype="uint8",
            itemsize=1,
            partition_dim=None,
            layer_id=index,
            layout_fingerprint="sglang:qwen3.5:uint8:test",
        )
        for index in range(4)
    )

    def make_manifest(prefix: str, *, source: bool) -> RuntimeManifest:
        fragments = []
        for index, tensor in enumerate(tensors):
            values = range(index * 4, index * 4 + 4) if source else [255] * 4
            owner = (ctypes.c_ubyte * 4)(*values)
            fragments.append(
                RuntimeFragment(
                    fragment_id=f"{prefix}-{index}",
                    tensor_id=tensor.tensor_id,
                    global_offset=(0,),
                    local_shape=(4,),
                    address=ctypes.addressof(owner),
                    nbytes=4,
                    worker_id=prefix,
                    endpoint=f"{prefix}:12345",
                    rank=ParallelRank(),
                    lease_generation=1,
                    owner=owner,
                )
            )
        return RuntimeManifest(
            model_id="qwen3.5-0.8b",
            revision="step-42",
            instance_id=prefix,
            tensors=tensors,
            fragments=tuple(fragments),
        )

    source = make_manifest("source", source=True)
    target = make_manifest("target", source=False)
    store, weight_store = make_weight_store()
    upload_plan = weight_store.prepare_upload((source,))
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, (source,))
    )
    load_plan = weight_store.plan_load(manifest, (target,))

    weight_store.load(load_plan, target)

    assert store.range_get_calls == 1
    assert store.range_batch_sizes == [4]
    for index, fragment in enumerate(target.fragments):
        assert bytes(fragment.owner) == bytes(range(index * 4, index * 4 + 4))


def test_one_runtime_manifest_can_execute_multiple_rank_subplans() -> None:
    store, weight_store = make_weight_store()
    sources = source_manifests(dp=1, tp=2)
    upload_plan = weight_store.prepare_upload(sources)
    manifest = weight_store.commit(
        upload_plan, upload_all(weight_store, upload_plan, sources)
    )
    target_ranks = target_manifests(dp=1, tp=2)
    target = RuntimeManifest(
        model_id=target_ranks[0].model_id,
        revision=target_ranks[0].revision,
        instance_id="combined-target",
        tensors=target_ranks[0].tensors,
        fragments=tuple(
            replace(fragment, worker_id="combined-target")
            for rank_manifest in target_ranks
            for fragment in rank_manifest.fragments
        ),
    )

    load_plan = weight_store.plan_load(manifest, (target,))
    weight_store.load(load_plan, target)

    assert store.range_get_calls == 1
    assert [bytes(fragment.owner) for fragment in target.fragments] == [
        bytes(range(4)),
        bytes(range(4, 8)),
    ]
