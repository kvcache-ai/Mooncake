from __future__ import annotations

import pytest
import torch

from mooncake_epd.agent import AgentStateCloner
from mooncake_epd.core.state import MooncakeKVStateStore, MooncakeRemoteKVMaterializer, PagedKVManager
from mooncake_epd.core.transfer import TransferEngine


def _pm(node_id: str) -> PagedKVManager:
    return PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id=node_id,
    )


def test_agent_state_cloner_uses_kv_state_store_for_zero_copy_forks():
    pm = _pm("prefill-a")
    refs = pm.allocate_pages(2, filled=4)
    store = MooncakeKVStateStore(pm, node_id="prefill-a")
    cloner = AgentStateCloner(kv_state_store=store)

    parent = cloner.register_kv_refs("state-parent", refs, workflow_id="wf-agent")
    assert parent.owner_node_id == "prefill-a"
    first = cloner.clone_kv_same_node_zero_copy("state-parent", "branch-explicit")
    branches = [first] + cloner.fork_kv_branches("state-parent", 2)

    assert first.clone_semantics == "same_node_block_ref_zero_copy"
    assert [b.kv_state_id for b in branches] == [b.branch_id for b in branches]
    for ref in refs:
        assert pm.refcount(ref.global_block_id) == 4  # parent + 3 branches
    assert cloner.get_stats()["kv_state_store_enabled"] is True
    assert cloner.get_stats()["kv_state_branches"] == 4

    for branch in branches:
        cloner.release_branch(branch.branch_id)
    for ref in refs:
        assert pm.refcount(ref.global_block_id) == 1
    cloner.release_branch("state-parent")
    assert store.stats() == {"states": 0, "blocks": 0, "tokens": 0}


def test_agent_state_cloner_materializes_descriptor_shared_branch_for_write():
    src = _pm("prefill-a")
    dst = _pm("decode-b")
    refs = src.allocate_pages(1, filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    val = key + 100
    src.write_page_slots(refs[0], key, val)
    materializer = MooncakeRemoteKVMaterializer(
        src,
        dst,
        transfer_engine=TransferEngine(protocol="local"),
    )
    store = MooncakeKVStateStore(
        src,
        node_id="prefill-a",
        page_managers_by_node={"decode-b": dst},
        remote_materializer=materializer,
        allow_remote_descriptor_sharing=True,
    )
    cloner = AgentStateCloner(kv_state_store=store)
    cloner.register_kv_refs("parent", refs, workflow_id="wf-agent")
    branch = cloner.share_kv_state_descriptor_cross_node(
        "parent",
        "branch-decode",
        target_node_id="decode-b",
    )
    assert branch.clone_semantics == "cross_node_descriptor_share"

    with pytest.raises(RuntimeError, match="materialized before write"):
        cloner.get_branch_kv_refs(branch.branch_id, target_node_id="decode-b", for_write=True)

    materialized = cloner.materialize_cross_node_descriptor_for_write(
        branch.branch_id,
        target_node_id="decode-b",
    )
    assert materialized.owner_node_id == "decode-b"
    assert cloner._branches[branch.branch_id].clone_semantics == "cross_node_materialized_for_write"
    refs_on_decode = cloner.get_branch_kv_refs(branch.branch_id, target_node_id="decode-b", for_write=True)
    moved_key, moved_val = dst.get_page_slice(refs_on_decode[0])
    assert torch.equal(moved_key, key)
    assert torch.equal(moved_val, val)


def test_agent_state_cloner_same_node_tensor_api_is_explicit_zero_copy():
    cloner = AgentStateCloner()
    key = torch.zeros(1, 1, 1, 1, 1)
    value = torch.ones(1, 1, 1, 1, 1)
    cloner.register_kv_cache("tensor-parent", (key, value))

    branch = cloner.clone_same_node_zero_copy("tensor-parent", "tensor-branch")

    assert branch.clone_semantics == "same_node_tensor_zero_copy"
    cached_key, cached_value = cloner.get_branch_kv_cache("tensor-branch")
    assert cached_key is key
    assert cached_value is value
    assert cloner.get_stats()["total_ref_counts"] == 2


def test_agent_state_cloner_rejects_same_node_descriptor_share_api():
    pm = _pm("prefill-a")
    refs = pm.allocate_pages(1, filled=4)
    store = MooncakeKVStateStore(
        pm,
        node_id="prefill-a",
        allow_remote_descriptor_sharing=True,
    )
    cloner = AgentStateCloner(kv_state_store=store)
    cloner.register_kv_refs("parent", refs, workflow_id="wf-agent")

    with pytest.raises(ValueError, match="must differ"):
        cloner.share_kv_state_descriptor_cross_node(
            "parent",
            "bad-same-node-share",
            target_node_id="prefill-a",
        )
