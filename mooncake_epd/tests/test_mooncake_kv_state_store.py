from __future__ import annotations

import pytest
import torch

from mooncake_epd.core.state import FeatureBundle, FeatureStore, MooncakeKVStateStore, PagedKVManager


def _make_pm() -> PagedKVManager:
    return PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="node-a",
    )


def test_mooncake_kv_state_store_clone_is_zero_copy_and_refcounted():
    pm = _make_pm()
    refs = pm.allocate_pages(2, filled=4)
    for ref in refs:
        assert pm.refcount(ref.global_block_id) == 1

    store = MooncakeKVStateStore(pm, node_id="node-a")
    parent = store.register_state(
        refs,
        workflow_id="wf-agent",
        version_id="v-parent",
        snapshot_epoch=7,
        state_id="state-parent",
    )
    child = store.clone_state("state-parent", child_state_id="state-child")

    assert child.parent_version_id == parent.version_id
    assert child.snapshot_epoch == parent.snapshot_epoch
    assert child.block_ids == parent.block_ids
    child_refs = store.get_refs("state-child")
    assert [ref.physical_id for ref in child_refs] == [ref.physical_id for ref in refs]
    for ref in refs:
        assert pm.refcount(ref.global_block_id) == 2

    assert store.release_state("state-parent") == 0
    for ref in child_refs:
        assert pm.refcount(ref.global_block_id) == 1
    assert store.release_state("state-child") == 2
    assert store.stats() == {"states": 0, "blocks": 0, "tokens": 0}


def test_mooncake_kv_state_store_feature_refcounts_follow_clones():
    pm = _make_pm()
    fs = FeatureStore(max_bytes=1024 * 1024)
    bundle = FeatureBundle("img-a", torch.randn(2, 4))
    fs.put("img-a", bundle)
    refs = pm.allocate_pages(1, filled=3)
    store = MooncakeKVStateStore(pm, feature_store=fs, node_id="node-a")

    store.register_state(refs, workflow_id="wf", state_id="parent", feature_hashes=["img-a"])
    assert fs.refcount("img-a") == 1
    store.clone_state("parent", child_state_id="child")
    assert fs.refcount("img-a") == 2
    store.release_state("parent")
    assert fs.refcount("img-a") == 1
    store.release_state("child")
    assert fs.refcount("img-a") == 0


def test_mooncake_kv_state_store_cow_after_clone_copies_only_mutated_page():
    pm = _make_pm()
    refs = pm.allocate_pages(2, filled=4)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")
    store.clone_state("parent", child_state_id="child")
    child_refs = store.get_refs("child")

    cow_ref = pm.cow_page(child_refs[0])
    assert cow_ref.physical_id != child_refs[0].physical_id
    assert pm.refcount(refs[0].global_block_id) == 1
    assert pm.refcount(cow_ref.global_block_id) == 1
    assert pm.refcount(refs[1].global_block_id) == 2

    # Commit an append-only child version.  cow_page() already consumed the old
    # child reference for refs[0], so the store must not release it again.
    updated_child_refs = [cow_ref, child_refs[1]]
    updated = store.commit_state_version(
        "child",
        updated_child_refs,
        state_id="child-v2",
        consumed_old_block_ids=[child_refs[0].global_block_id],
    )
    assert updated.block_descriptors[0].global_block_id == cow_ref.global_block_id

    store.release_state("parent")
    store.release_state("child-v2")


def test_mooncake_kv_state_store_cross_node_clone_requires_real_materializer():
    pm = _make_pm()
    refs = pm.allocate_pages(1, filled=2)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")

    with pytest.raises(NotImplementedError, match="remote_materializer"):
        store.clone_state("parent", target_node_id="node-b")
