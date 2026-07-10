from __future__ import annotations

import torch

from mooncake_epd.core.control import LocalKVDirectory
from mooncake_epd.core.state import FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta


def _make_state_layer():
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-a",
    )
    tree = RadixTree(pm, max_entries=64)
    fs = FeatureStore(max_bytes=8 * 1024 * 1024)
    return StateLayer(pm, tree, fs, default_ttl_seconds=30.0)


def test_local_kv_directory_lifecycle_and_promote():
    directory = LocalKVDirectory(node_id="owner-0")
    gid = directory.register_block(local_pid=7, workflow_id="wf-1", node_id="owner-0")

    assert directory.refcount(gid) == 1
    assert directory.resolve_physical_id(gid) == 7
    assert directory.gid_for_physical_id(7) == gid

    assert directory.incref(gid) == 2
    assert directory.decref(gid) == 1

    promote = directory.promote_block(gid, node_id="owner-1")
    assert promote.old_owner == "owner-0"
    assert promote.new_owner == "owner-1"
    assert directory.get_record(gid).physical_node_id == "owner-1"

    assert directory.lease(gid) == 1
    assert directory.release_lease(gid) == 0


def test_paged_kv_manager_uses_directory_as_refcount_authority():
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-a",
    )

    ref = pm.allocate_page(filled=4)
    key = torch.randn(2, 2, 4, 8, dtype=torch.float32)
    value = torch.randn_like(key)
    pm.write_page_slots(ref, key, value, offset=0)

    child = pm.fork_refs([ref])[0]
    assert child.global_block_id == ref.global_block_id
    assert pm.refcount(ref.global_block_id) == 2

    child_private = pm.cow_page(child)
    assert child_private.global_block_id != ref.global_block_id
    assert pm.refcount(ref.global_block_id) == 1
    assert pm.refcount(child_private.global_block_id) == 1

    pm.release_refs([child_private])
    pm.release_refs([ref])
    stats = pm.stats()
    assert stats["total_pages"] == 0
    assert stats["directory_blocks"] == 0


def test_state_layer_commits_workflow_epochs_into_directory():
    sl = _make_state_layer()
    base_tokens = [10, 11, 12, 13]
    refs = sl.pm.allocate_pages(1, filled=4)
    k = torch.randn(2, 2, 4, 8, dtype=torch.float32)
    v = torch.randn_like(k)
    sl.pm.write_page_slots(refs[0], k, v, offset=0)

    state0 = sl.register(
        kv_refs=refs,
        feature_hash=None,
        meta=StateMeta(
            token_ids=base_tokens,
            workflow_id="wf-epoch",
            agent_id="agent-a",
            step=0,
        ),
    )
    assert sl.pm.kv_directory.workflow_epoch("wf-epoch") == 0

    matched, delta, _ = sl.advance_step_prepare(state0, [20, 21, 22, 23], [])
    assert sum(ref.filled for ref in matched) == 4
    assert delta == [20, 21, 22, 23]

    new_refs = list(matched) + sl.pm.allocate_pages(1, filled=4)
    k2 = torch.randn(2, 2, 4, 8, dtype=torch.float32)
    v2 = torch.randn_like(k2)
    sl.pm.write_page_slots(new_refs[-1], k2, v2, offset=0)
    state1 = sl.advance_step_commit(
        state0,
        [20, 21, 22, 23],
        [],
        new_kv_refs=new_refs,
        matched_pages=matched,
    )

    versions = sl.pm.kv_directory.workflow_versions("wf-epoch")
    assert state1.snapshot_epoch == 1
    assert versions[0] == (0, state0.version_id)
    assert versions[-1] == (1, state1.version_id)

    sl.release(state1)
    sl.release(state0)
    sl.radix.clear()
