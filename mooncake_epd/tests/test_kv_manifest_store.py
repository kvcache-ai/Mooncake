from __future__ import annotations

import ctypes
import threading
from dataclasses import asdict

import pytest
import torch

from mooncake_epd.core.state import (
    BlockRef,
    MooncakeKVPageStore,
    MooncakeKVStateStore,
    PagedKVManager,
)


class _RegisteredBufferStore:
    """Pointer-based Store fake; rejects the accidental bytes/get path."""

    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}
        self.registered: set[int] = set()

    def register_buffer(self, pointer: int, _size: int) -> int:
        self.registered.add(int(pointer))
        return 0

    def unregister_buffer(self, pointer: int) -> int:
        self.registered.discard(int(pointer))
        return 0

    def batch_put_from(self, keys, pointers, sizes):
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            self.payloads[str(key)] = ctypes.string_at(int(pointer), int(size))
        return [0] * len(keys)

    def batch_get_into(self, keys, pointers, sizes):
        output = []
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            payload = self.payloads[str(key)]
            if len(payload) != int(size):
                output.append(-1)
                continue
            ctypes.memmove(int(pointer), payload, int(size))
            output.append(int(size))
        return output

    def remove(self, key, _force=False):
        self.payloads.pop(str(key), None)
        return 0


class _FailingSecondPutStore(_RegisteredBufferStore):
    def batch_put_from(self, keys, pointers, sizes):
        statuses = super().batch_put_from(keys, pointers, sizes)
        statuses[-1] = -1
        return statuses

    def batch_remove(self, keys, _force=False):
        # Simulate a partial batch-delete failure. Per-key remove must retry
        # only the failed key rather than silently leaving it behind.
        statuses = []
        for index, key in enumerate(keys):
            if index == 0:
                statuses.append(-1)
            else:
                self.payloads.pop(str(key), None)
                statuses.append(0)
        return statuses


class _UnremovableStore(_FailingSecondPutStore):
    def remove(self, key, _force=False):
        return -1


class _BlockingPutStore(_RegisteredBufferStore):
    def __init__(self) -> None:
        super().__init__()
        self.put_entered = threading.Event()
        self.allow_put = threading.Event()

    def batch_put_from(self, keys, pointers, sizes):
        self.put_entered.set()
        assert self.allow_put.wait(timeout=5.0)
        return super().batch_put_from(keys, pointers, sizes)


def _manager(node_id: str) -> PagedKVManager:
    return PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id=node_id,
    )


def test_kv_manifest_canonical_payload_matches_dataclass_fields_without_timestamp():
    manager = _manager("prefill-a")
    ref = manager.allocate_page(filled=4)
    manifest = MooncakeKVStateStore(manager, node_id="prefill-a").register_state(
        [ref], workflow_id="wf", state_id="state"
    ).manifests[0]

    expected = asdict(manifest)
    expected.pop("created_at")
    assert manifest.canonical_payload() == expected


def test_store_manifest_clone_is_payload_free_and_materializes_verified_pages():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    value = key + 100
    source.write_page_slots(ref, key, value)
    state = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        model_id="qwen-vl",
        model_revision="revision-a",
        source_generation="prefill-g1",
        target_generation="decode-g2",
    ).register_state([ref], workflow_id="wf", state_id="parent", version_id="v1")
    page_store = MooncakeKVPageStore(_RegisteredBufferStore())

    exported = page_store.export_pages(source, [ref], state.manifests)
    assert exported[0].has_store_payload
    assert page_store.stats()["export_bytes"] == exported[0].key_nbytes + exported[0].value_nbytes

    child = page_store.clone_manifests(
        exported,
        state_id="branch",
        version_id="branch-v1",
        snapshot_epoch=1,
        owner_node_id="prefill-a",
        target_node_id="decode-b",
        lease_deadline=exported[0].lease_deadline,
    )
    assert child[0].key_store_key == exported[0].key_store_key
    assert child[0].value_store_key == exported[0].value_store_key
    assert page_store.stats()["export_bytes"] > 0

    target = _manager("decode-b")
    imported = page_store.materialize_pages(target, child)
    moved_key, moved_value = target.get_page(imported[0])
    assert torch.equal(moved_key, key)
    assert torch.equal(moved_value, value)
    assert page_store.stats()["import_bytes"] == page_store.stats()["export_bytes"]


def test_store_manifest_export_and_import_preserve_bfloat16_kv_bytes():
    source = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.bfloat16,
        device=torch.device("cpu"),
        node_id="prefill-bf16",
    )
    target = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.bfloat16,
        device=torch.device("cpu"),
        node_id="decode-bf16",
    )
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2).to(torch.bfloat16)
    source.write_page_slots(ref, key, key + 9)
    state = MooncakeKVStateStore(source, node_id="prefill-bf16").register_state(
        [ref], workflow_id="wf", state_id="parent"
    )
    page_store = MooncakeKVPageStore(_RegisteredBufferStore())

    exported = page_store.export_pages(source, [ref], state.manifests)
    imported = page_store.materialize_pages(target, exported)
    moved_key, moved_value = target.get_page(imported[0])

    assert torch.equal(moved_key, key)
    assert torch.equal(moved_value, key + 9)


def test_store_manifest_rejects_checksum_corruption_without_leaking_target_page():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    key = torch.ones((2, 1, 4, 2), dtype=torch.float32)
    source.write_page_slots(ref, key, key)
    state = MooncakeKVStateStore(source, node_id="prefill-a").register_state(
        [ref], workflow_id="wf", state_id="parent"
    )
    backing = _RegisteredBufferStore()
    page_store = MooncakeKVPageStore(backing)
    exported = page_store.export_pages(source, [ref], state.manifests)
    backing.payloads[str(exported[0].key_store_key)] = b"x" * int(exported[0].key_nbytes)
    target = _manager("decode-b")

    with pytest.raises(Exception, match="checksum"):
        page_store.materialize_pages(target, exported)
    assert target.refcount("decode-b-block-0") == 0


def test_kv_state_store_can_materialize_descriptor_clone_from_store_pages():
    source = _manager("prefill-a")
    target = _manager("decode-b")
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(ref, key, key + 9)
    page_store = MooncakeKVPageStore(_RegisteredBufferStore())
    states = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        page_managers_by_node={"decode-b": target},
        allow_remote_descriptor_sharing=True,
        kv_page_store=page_store,
    )
    states.register_state([ref], workflow_id="wf", state_id="parent")
    shared = states.clone_state(
        "parent",
        child_state_id="branch",
        target_node_id="decode-b",
        share_remote_descriptor=True,
    )
    assert shared.manifests[0].has_store_payload

    materialized = states.materialize_for_write("branch", target_node_id="decode-b")
    assert materialized.owner_node_id == "decode-b"
    moved_key, moved_value = target.get_page(states.get_refs("branch")[0])
    assert torch.equal(moved_key, key)
    assert torch.equal(moved_value, key + 9)
    assert page_store.stats()["import_pages"] == 1


def test_kv_state_store_can_restore_catalogued_store_state_after_restart():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(ref, key, key + 7)
    backing = _RegisteredBufferStore()
    page_store = MooncakeKVPageStore(backing)
    original = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        kv_page_store=page_store,
    ).register_state([ref], workflow_id="wf", state_id="recoverable")

    recovered_manager = _manager("decode-b")
    recovered_store = MooncakeKVStateStore(
        recovered_manager,
        node_id="decode-b",
        kv_page_store=page_store,
    )
    restored = recovered_store.restore_state_from_store(
        "recoverable",
        original.manifests,
    )
    restored_ref = recovered_store.get_refs("recoverable")[0]
    moved_key, moved_value = recovered_manager.get_page(restored_ref)
    assert restored.owner_node_id == "decode-b"
    assert restored.metadata["restored_from_store"] is True
    assert torch.equal(moved_key, key)
    assert torch.equal(moved_value, key + 7)


def test_page_fork_rolls_back_earlier_refcount_when_later_ref_is_invalid():
    manager = _manager("node-a")
    valid = manager.allocate_page(filled=4)
    missing = BlockRef(physical_id=999, filled=4, global_block_id="missing", physical_node_id="node-a")

    with pytest.raises(KeyError):
        manager.fork_refs([valid, missing])
    assert manager.refcount(valid.global_block_id) == 1


def test_kv_state_store_ttl_sweep_releases_leased_pages():
    manager = _manager("node-a")
    ref = manager.allocate_page(filled=4)
    states = MooncakeKVStateStore(manager, node_id="node-a")
    states.register_state(
        [ref],
        workflow_id="wf",
        state_id="expires",
        ttl_deadline=0.0,
    )
    assert manager.kv_directory.get_record(ref.global_block_id).lease_count == 1
    assert states.sweep_expired_states(now=1.0) == 1
    assert manager.kv_directory.get_record(ref.global_block_id) is None


def test_duplicate_state_id_is_rejected_before_store_export():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    source.write_page_slots(
        ref,
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
    )
    backing = _RegisteredBufferStore()
    states = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        kv_page_store=MooncakeKVPageStore(backing),
    )
    states.register_state([ref], workflow_id="wf", state_id="duplicate")
    payloads_before = dict(backing.payloads)

    with pytest.raises(ValueError, match="state already registered"):
        states.register_state([ref], workflow_id="wf", state_id="duplicate")

    assert backing.payloads == payloads_before


def test_pending_state_id_is_rejected_while_first_store_export_is_in_progress():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    source.write_page_slots(
        ref,
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
    )
    backing = _BlockingPutStore()
    states = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        kv_page_store=MooncakeKVPageStore(backing),
    )
    result: dict[str, object] = {}

    def register_first() -> None:
        try:
            result["descriptor"] = states.register_state(
                [ref], workflow_id="wf", state_id="concurrent"
            )
        except Exception as exc:  # pragma: no cover - surfaced below
            result["error"] = exc

    first = threading.Thread(target=register_first)
    first.start()
    assert backing.put_entered.wait(timeout=5.0)

    with pytest.raises(ValueError, match="state already registered"):
        states.register_state([ref], workflow_id="wf", state_id="concurrent")

    backing.allow_put.set()
    first.join(timeout=5.0)
    assert not first.is_alive()
    assert "error" not in result
    assert states.get_state("concurrent") is result["descriptor"]
    assert len(backing.payloads) == 2


def test_failed_store_export_releases_state_id_reservation_for_retry():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    source.write_page_slots(
        ref,
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
    )
    states = MooncakeKVStateStore(
        source,
        node_id="prefill-a",
        kv_page_store=MooncakeKVPageStore(_FailingSecondPutStore()),
    )

    with pytest.raises(Exception, match="batch_put_from failed"):
        states.register_state([ref], workflow_id="wf", state_id="retry")

    states.kv_page_store = MooncakeKVPageStore(_RegisteredBufferStore())
    descriptor = states.register_state([ref], workflow_id="wf", state_id="retry")
    assert descriptor.state_id == "retry"


def test_store_export_rollback_retries_failed_batch_remove_keys():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    source.write_page_slots(
        ref,
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
    )
    states = MooncakeKVStateStore(source, node_id="prefill-a")
    descriptor = states.register_state([ref], workflow_id="wf", state_id="rollback")
    backing = _FailingSecondPutStore()
    page_store = MooncakeKVPageStore(backing)

    with pytest.raises(Exception, match="batch_put_from failed"):
        page_store.export_pages(source, [ref], descriptor.manifests)

    assert backing.payloads == {}


def test_store_export_rollback_raises_when_page_objects_cannot_be_removed():
    source = _manager("prefill-a")
    ref = source.allocate_page(filled=4)
    source.write_page_slots(
        ref,
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
        torch.ones((2, 1, 4, 2), dtype=torch.float32),
    )
    states = MooncakeKVStateStore(source, node_id="prefill-a")
    descriptor = states.register_state([ref], workflow_id="wf", state_id="rollback-error")
    page_store = MooncakeKVPageStore(_UnremovableStore())

    with pytest.raises(Exception, match="failed to clean up"):
        page_store.export_pages(source, [ref], descriptor.manifests)
