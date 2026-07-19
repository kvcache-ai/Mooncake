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


def test_mooncake_kv_state_store_same_node_clone_does_not_rehash_kv_pages(monkeypatch):
    """Forking already-captured immutable pages is descriptor-only on CUDA too."""

    pm = _make_pm()
    refs = pm.allocate_pages(2, filled=4)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")

    def fail_if_rehashed(*_args, **_kwargs):
        raise AssertionError("same-node clone must reuse captured page checksums")

    monkeypatch.setattr(pm, "page_checksum", fail_if_rehashed)
    child = store.clone_state("parent", child_state_id="child")

    assert child.block_ids == store.get_state("parent").block_ids
    store.release_state("parent")
    store.release_state("child")


def test_mooncake_kv_state_store_supports_bfloat16_page_checksums():
    """Real vLLM KV pages are commonly bfloat16, not NumPy-native dtype."""

    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.bfloat16,
        device=torch.device("cpu"),
        node_id="node-bf16",
    )
    ref = pm.allocate_page(filled=4)
    key = torch.ones((2, 1, 4, 2), dtype=torch.bfloat16)
    pm.write_page_slots(ref, key, key + 1)

    digest_before = pm.page_checksum(ref)
    assert len(digest_before) == 64
    pm.write_page_slots(ref, key + 2, key + 3)
    assert pm.page_checksum(ref) != digest_before


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


def test_mooncake_kv_state_store_can_replace_logical_state_id_on_cow_commit():
    """A branch handle remains stable while its immutable version advances."""

    pm = _make_pm()
    refs = pm.allocate_pages(2, filled=4)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")
    child = store.clone_state("parent", child_state_id="branch")
    child_refs = store.get_refs("branch")

    cow_ref = pm.cow_page(child_refs[0])
    updated = store.commit_state_version(
        "branch",
        [cow_ref, child_refs[1]],
        # ``state_id`` is the stable serving branch handle; ``version_id`` is
        # the append-only identity and must advance without changing clients'
        # branch references.
        state_id="branch",
        version_id="branch-v2",
        consumed_old_block_ids=[child_refs[0].global_block_id],
    )

    assert updated.state_id == "branch"
    assert updated.version_id == "branch-v2"
    assert updated.parent_version_id == child.version_id
    assert store.get_state("branch") is updated
    assert store.get_refs("branch")[0].global_block_id == cow_ref.global_block_id

    store.release_state("parent")
    store.release_state("branch")
    assert store.stats() == {"states": 0, "blocks": 0, "tokens": 0}


def test_cow_commit_reuses_explicitly_immutable_page_checksums(monkeypatch):
    pm = _make_pm()
    refs = pm.allocate_pages(2, filled=4)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")
    store.clone_state("parent", child_state_id="branch")
    child_refs = store.get_refs("branch")
    cow_ref = pm.cow_page(child_refs[0])
    original_checksum = pm.page_checksum
    checksum_calls = []

    def record_checksum(ref, **kwargs):
        checksum_calls.append((ref.global_block_id, kwargs.get("filled_only", True)))
        return original_checksum(ref, **kwargs)

    monkeypatch.setattr(pm, "page_checksum", record_checksum)
    updated = store.commit_state_version(
        "branch",
        [cow_ref, child_refs[1]],
        state_id="branch",
        consumed_old_block_ids=[child_refs[0].global_block_id],
        immutable_reused_block_ids=[child_refs[1].global_block_id],
    )

    assert updated.manifests[1].data_checksum == store.get_state("parent").manifests[1].data_checksum
    assert checksum_calls == [(cow_ref.global_block_id, True)]
    store.release_state("parent")
    store.release_state("branch")


def test_mooncake_kv_state_store_cross_node_clone_requires_real_materializer():
    pm = _make_pm()
    refs = pm.allocate_pages(1, filled=2)
    store = MooncakeKVStateStore(pm, node_id="node-a")
    store.register_state(refs, workflow_id="wf", state_id="parent")

    with pytest.raises(NotImplementedError, match="remote_materializer"):
        store.clone_state("parent", target_node_id="node-b")
