from __future__ import annotations

import ctypes

import torch

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.state import MooncakeKVPageStore, MooncakeKVStateStore, PagedKVManager


class _RegisteredBufferStore:
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
        results = []
        for key, pointer, size in zip(keys, pointers, sizes):
            assert int(pointer) in self.registered
            ctypes.memmove(int(pointer), self.payloads[str(key)], int(size))
            results.append(int(size))
        return results

    def remove(self, key, _force=False):
        self.payloads.pop(str(key), None)
        return 0


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


def test_catalog_recovery_rematerializes_store_pages_without_old_physical_ids(tmp_path):
    backing = _RegisteredBufferStore()
    page_store = MooncakeKVPageStore(backing)
    source = _manager("prefill-0")
    source_ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(source_ref, key, key + 33)
    wal = str(tmp_path / "agent-lifecycle.jsonl")

    first_store = MooncakeKVStateStore(
        source,
        node_id="prefill-0",
        kv_page_store=page_store,
    )
    first = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-1",
            enable_agent_state_clone=True,
            require_real_agent_state_store=True,
            workflow_registry_wal_path=wal,
        ),
        kv_directory=source.kv_directory,
        kv_state_store=first_store,
    )
    first.register_agent_state(
        workflow_id="wf-recover",
        state_id="parent",
        kv_block_ids=[],
        kv_refs=[source_ref],
        target_node_id="prefill-0",
    )
    assert backing.payloads

    # Simulate a new proxy/worker process: only the durable WAL and Store page
    # objects survive.  The old PagedKVManager is intentionally not passed to
    # the replacement control plane.
    target = _manager("decode-0")
    second_store = MooncakeKVStateStore(
        target,
        node_id="decode-0",
        kv_page_store=page_store,
    )
    second = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-2",
            enable_agent_state_clone=True,
            require_real_agent_state_store=True,
            workflow_registry_wal_path=wal,
        ),
        kv_directory=target.kv_directory,
        kv_state_store=second_store,
    )
    materialized = second.materialize_agent_state(
        state_id="parent",
        target_node_id="decode-0",
    )
    assert materialized["materialized"] is True
    assert materialized["kv_block_ids"][0] != source_ref.global_block_id
    restored_ref = second_store.get_refs("parent")[0]
    restored_key, restored_value = target.get_page(restored_ref)
    assert torch.equal(restored_key, key)
    assert torch.equal(restored_value, key + 33)

    released = second.release_agent_state(state_id="parent")
    assert released["catalog_gc_objects"] == 2
    assert backing.payloads == {}
