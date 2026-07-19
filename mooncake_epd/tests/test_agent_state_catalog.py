from __future__ import annotations

import ctypes
import time

import pytest
import torch

from mooncake_epd.core.state import (
    AgentStateCatalog,
    AgentStateCatalogConflict,
    AgentStateCatalogError,
    MooncakeKVPageStore,
    MooncakeKVStateStore,
    PagedKVManager,
)


class _RegisteredBufferStore:
    """Minimal pointer-only Store fake used by the page-store adapter."""

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
        result = []
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            payload = self.payloads[str(key)]
            ctypes.memmove(int(pointer), payload, int(size))
            result.append(int(size))
        return result

    def remove(self, key, _force=False):
        self.payloads.pop(str(key), None)
        return 0


def _exported_parent():
    manager = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="prefill-0",
    )
    ref = manager.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    manager.write_page_slots(ref, key, key + 100)
    backing = _RegisteredBufferStore()
    page_store = MooncakeKVPageStore(backing)
    states = MooncakeKVStateStore(
        manager,
        node_id="prefill-0",
        model_id="qwen-vl",
        model_revision="revision-a",
        kv_page_store=page_store,
    )
    parent = states.register_state(
        [ref],
        workflow_id="wf-catalog",
        state_id="parent",
        version_id="parent-v1",
    )
    assert parent.manifests[0].has_store_payload
    return backing, page_store, parent


def _reclaimer(page_store: MooncakeKVPageStore):
    def reclaim(refs):
        page_store.remove_page_objects(
            [
                key
                for ref in refs
                for key in (ref.key_store_key, ref.value_store_key)
            ]
        )

    return reclaim


def test_catalog_fork_is_payload_free_and_reclaims_only_after_last_release():
    backing, page_store, parent = _exported_parent()
    catalog = AgentStateCatalog(page_reclaimer=_reclaimer(page_store))
    parent_record = catalog.register_state(
        state_id="parent",
        workflow_id="wf-catalog",
        state_store_id="parent",
        manifests=parent.manifests,
        owner_node_id="prefill-0",
        target_node_id="prefill-0",
        lease_deadline_unix=time.time() + 30.0,
    )
    child_manifests = page_store.clone_manifests(
        parent.manifests,
        state_id="child",
        version_id="child-v1",
        snapshot_epoch=1,
        owner_node_id="prefill-0",
        target_node_id="decode-0",
        lease_deadline=parent.manifests[0].lease_deadline,
    )
    child = catalog.fork_state(
        parent_state_id="parent",
        child_state_id="child",
        child_state_store_id="child",
        manifests=child_manifests,
        owner_node_id="prefill-0",
        target_node_id="decode-0",
        expected_parent_version=parent_record.catalog_version,
    )
    assert child.clone_payload_bytes == 0
    assert catalog.stats()["active_store_pages"] == 1
    assert catalog.stats()["active_store_page_references"] == 2

    catalog.release_state("parent")
    assert backing.payloads
    assert catalog.stats()["active_store_page_references"] == 1

    catalog.release_state("child", expected_version=child.catalog_version)
    assert backing.payloads == {}
    assert catalog.stats()["active_states"] == 0
    assert page_store.stats()["gc_objects"] == 2


def test_catalog_rejects_stale_cas_and_tampered_manifest():
    _backing, _page_store, parent = _exported_parent()
    catalog = AgentStateCatalog()
    record = catalog.register_state(
        state_id="parent",
        workflow_id="wf-catalog",
        state_store_id="parent",
        manifests=parent.manifests,
        owner_node_id="prefill-0",
        target_node_id="prefill-0",
        lease_deadline_unix=time.time() + 30.0,
    )
    with pytest.raises(AgentStateCatalogConflict, match="catalog version conflict"):
        catalog.release_state("parent", expected_version=record.catalog_version + 1)

    tampered = parent.manifests[0].as_control_payload()
    tampered["checksum"] = "not-a-valid-checksum"
    with pytest.raises(AgentStateCatalogError, match="checksum mismatch"):
        catalog.register_state(
            state_id="tampered",
            workflow_id="wf-catalog",
            state_store_id="parent",
            manifests=[tampered],
            owner_node_id="prefill-0",
            target_node_id="prefill-0",
            lease_deadline_unix=time.time() + 30.0,
        )


def test_catalog_wal_recovery_preserves_page_refs_and_sweeps_expired(tmp_path):
    _backing, page_store, parent = _exported_parent()
    wal = tmp_path / "agent-catalog.jsonl"
    deadline = time.time() + 1.0
    first = AgentStateCatalog(str(wal))
    first.register_state(
        state_id="parent",
        workflow_id="wf-catalog",
        state_store_id="parent",
        manifests=parent.manifests,
        owner_node_id="prefill-0",
        target_node_id="prefill-0",
        lease_deadline_unix=deadline,
    )
    child_manifests = page_store.clone_manifests(
        parent.manifests,
        state_id="child",
        version_id="child-v1",
        snapshot_epoch=1,
        owner_node_id="prefill-0",
        target_node_id="decode-0",
        lease_deadline=parent.manifests[0].lease_deadline,
    )
    first.fork_state(
        parent_state_id="parent",
        child_state_id="child",
        child_state_store_id="child",
        manifests=child_manifests,
        owner_node_id="prefill-0",
        target_node_id="decode-0",
    )

    recovered = AgentStateCatalog(str(wal), page_reclaimer=_reclaimer(page_store))
    assert recovered.stats()["active_store_page_references"] == 2
    assert recovered.sweep_expired(now=deadline + 10.0) == 2
    assert recovered.get_record("parent").status == "RELEASED"
    assert recovered.get_record("child").status == "RELEASED"
