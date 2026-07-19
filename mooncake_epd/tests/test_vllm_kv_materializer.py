from __future__ import annotations

import ctypes

import pytest
import torch

from mooncake_epd.core.state import (
    MooncakeKVPageStore,
    MooncakeKVStateStore,
    PagedKVManager,
    VLLMKVMaterializationError,
    VLLMKVMaterializer,
)


class _RegisteredBufferStore:
    """Pointer-only Store fake so the adapter exercises real page movement."""

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
            if len(payload) != int(size):
                result.append(-1)
                continue
            ctypes.memmove(int(pointer), payload, int(size))
            result.append(int(size))
        return result

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


def _shared_store_state() -> tuple[MooncakeKVStateStore, PagedKVManager]:
    source = _manager("prefill-0")
    target = _manager("decode-1")
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(ref, key, key + 11)
    store = MooncakeKVPageStore(_RegisteredBufferStore())
    states = MooncakeKVStateStore(
        source,
        node_id="prefill-0",
        page_managers_by_node={"decode-1": target},
        allow_remote_descriptor_sharing=True,
        kv_page_store=store,
    )
    states.register_state([ref], workflow_id="wf", state_id="parent", version_id="v1")
    states.clone_state(
        "parent",
        child_state_id="branch",
        target_node_id="decode-1",
        share_remote_descriptor=True,
    )
    return states, target


def test_materializer_imports_store_pages_into_fresh_target_blocks_and_commits_connector():
    states, target = _shared_store_state()
    commits: list[dict] = []

    def commit(envelope):
        commits.append(dict(envelope))
        return {"accepted": True, "request_id": "decode-request-1"}

    result = VLLMKVMaterializer(states, commit_to_connector=commit).materialize_write(
        "branch",
        target_node_id="decode-1",
        require_connector_commit=True,
    )

    assert result.committed is True
    assert result.backend == "mooncake_store_pages"
    assert result.materialized_bytes > 0
    assert result.owner_node_id == "decode-1"
    assert result.block_ids != ("prefill-0-block-0",)
    assert all(block_id.startswith("decode-1-block-") for block_id in result.block_ids)
    assert commits[0]["vllm_kv_block_ids"] == list(result.block_ids)
    assert commits[0]["materialized_bytes"] == result.materialized_bytes
    assert result.connector_metadata["connector_commit"] == {
        "accepted": True,
        "request_id": "decode-request-1",
    }
    moved_key, moved_value = target.get_page(states.get_refs("branch")[0])
    assert torch.equal(moved_key, torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2))
    assert torch.equal(moved_value, moved_key + 11)


def test_materializer_requires_explicit_connector_commit_when_requested():
    states, _target = _shared_store_state()
    materializer = VLLMKVMaterializer(states)

    with pytest.raises(VLLMKVMaterializationError, match="connector commit callback"):
        materializer.materialize_read(
            "parent",
            target_node_id="prefill-0",
            require_connector_commit=True,
        )
