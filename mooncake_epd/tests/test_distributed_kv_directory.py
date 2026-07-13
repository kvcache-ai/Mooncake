from __future__ import annotations

from mooncake_epd.core.control import DistributedKVDirectory, LocalKVDirectory


def _find_cross_shard_target(directory: DistributedKVDirectory, source_shard: str) -> str:
    for idx in range(128):
        node_id = f"target-node-{idx}"
        if directory._route_target_node(node_id) != source_shard:  # noqa: SLF001
            return node_id
    raise AssertionError("failed to find a cross-shard target")


def test_distributed_kv_directory_routes_blocks_and_commits_epochs():
    directory = DistributedKVDirectory(shards=3)

    gid_a = directory.register_block(local_pid=11, workflow_id="wf-a")
    gid_b = directory.register_block(local_pid=12, workflow_id="wf-b")

    assert directory.get_record(gid_a) is not None
    assert directory.get_record(gid_b) is not None
    assert directory.refcount(gid_a) == 1
    directory.incref(gid_a)
    assert directory.refcount(gid_a) == 2
    assert directory.decref(gid_a) == 1

    epoch0 = directory.commit_epoch("wf-a", "v1")
    epoch1 = directory.commit_epoch("wf-a", "v2")
    assert epoch0 == 0
    assert epoch1 == 1
    assert directory.workflow_epoch("wf-a") == 1
    assert len(directory.workflow_versions("wf-a")) == 2


def test_distributed_kv_directory_cross_shard_handoff_moves_metadata():
    directory = DistributedKVDirectory(shards=2)
    workflow_id = "wf-cross-shard"
    gid = directory.register_block(local_pid=21, workflow_id=workflow_id)
    source_shard = directory._route_workflow(workflow_id)  # noqa: SLF001
    target_node_id = _find_cross_shard_target(directory, source_shard)
    target_shard = directory._route_target_node(target_node_id)  # noqa: SLF001

    handoff = directory.begin_handoff(
        handoff_id="handoff-1",
        state_id="state-1",
        workflow_id=workflow_id,
        source_agent_id="agent-a",
        target_agent_id="agent-b",
        source_node_id="node-a",
        target_node_id=target_node_id,
        block_ids=[gid],
        feature_hashes=["img-0"],
    )
    assert handoff.status == "PREPARED"
    prepared = directory.get_handoff("handoff-1")
    assert prepared is not None
    assert prepared.target_node_id == target_node_id

    committed = directory.commit_handoff("handoff-1")
    assert committed is not None
    record = directory.get_record(gid)
    assert record is not None
    assert record.owner_shard == target_shard
    assert record.physical_node_id == target_node_id
    assert record.external_placeholder is False
    assert record.local_physical_id == 21

    cleared = directory.clear_handoff("handoff-1")
    assert cleared is not None


def test_distributed_kv_directory_rollback_cleans_target_placeholders():
    directory = DistributedKVDirectory(shards=2)
    workflow_id = "wf-cross-shard-rollback"
    gid = directory.register_block(local_pid=31, workflow_id=workflow_id)
    source_shard = directory._route_workflow(workflow_id)  # noqa: SLF001
    target_node_id = _find_cross_shard_target(directory, source_shard)

    directory.begin_handoff(
        handoff_id="handoff-rollback",
        state_id="state-rollback",
        workflow_id=workflow_id,
        source_agent_id="agent-a",
        target_agent_id="agent-b",
        source_node_id="node-a",
        target_node_id=target_node_id,
        block_ids=[gid],
    )
    rolled = directory.rollback_handoff("handoff-rollback")
    assert rolled is not None
    record = directory.get_record(gid)
    assert record is not None
    assert record.workflow_id == workflow_id


def test_distributed_kv_directory_clear_prepared_handoff_rolls_back_and_cleans_placeholder():
    directory = DistributedKVDirectory(shards=2)
    workflow_id = "wf-clear-prepared"
    gid = directory.register_block(local_pid=41, workflow_id=workflow_id)
    source_shard = directory._route_workflow(workflow_id)  # noqa: SLF001
    target_node_id = _find_cross_shard_target(directory, source_shard)
    target_shard = directory._route_target_node(target_node_id)  # noqa: SLF001

    directory.begin_handoff(
        handoff_id="handoff-clear-prepared",
        state_id="state-clear-prepared",
        workflow_id=workflow_id,
        source_agent_id="agent-a",
        target_agent_id="agent-b",
        source_node_id="node-a",
        target_node_id=target_node_id,
        block_ids=[gid],
    )

    placeholder = directory._shards[target_shard].get_record(gid)  # noqa: SLF001
    assert placeholder is not None
    assert placeholder.external_placeholder is True

    cleared = directory.clear_handoff("handoff-clear-prepared")

    assert cleared is not None
    assert cleared.status == "ROLLED_BACK"
    assert directory._shards[target_shard].get_record(gid) is None  # noqa: SLF001
    assert directory.get_record(gid) is not None


def test_distributed_kv_directory_rollback_after_commit_is_idempotent_noop():
    directory = DistributedKVDirectory(shards=2)
    workflow_id = "wf-rollback-after-commit"
    gid = directory.register_block(local_pid=51, workflow_id=workflow_id)
    source_shard = directory._route_workflow(workflow_id)  # noqa: SLF001
    target_node_id = _find_cross_shard_target(directory, source_shard)
    target_shard = directory._route_target_node(target_node_id)  # noqa: SLF001

    directory.begin_handoff(
        handoff_id="handoff-commit-noop",
        state_id="state-commit-noop",
        workflow_id=workflow_id,
        source_agent_id="agent-a",
        target_agent_id="agent-b",
        source_node_id="node-a",
        target_node_id=target_node_id,
        block_ids=[gid],
    )
    committed = directory.commit_handoff("handoff-commit-noop")
    rolled = directory.rollback_handoff("handoff-commit-noop")

    assert committed is not None
    assert rolled is not None
    assert rolled.status == "COMMITTED"
    record = directory.get_record(gid)
    assert record is not None
    assert record.owner_shard == target_shard
    assert record.physical_node_id == target_node_id


def test_distributed_kv_directory_failed_commit_restores_source_and_placeholders(monkeypatch):
    source = LocalKVDirectory(node_id="shard-a")
    target = LocalKVDirectory(node_id="shard-b")
    directory = DistributedKVDirectory(shards={"shard-a": source, "shard-b": target})
    target_node_id = _find_cross_shard_target(directory, "shard-a")
    gid_a = directory.ensure_block_record(
        "gid-a",
        workflow_id="wf-failed-commit",
        owner_shard="shard-a",
        physical_node_id="node-a",
        local_pid=101,
        refcount=1,
    ).global_block_id
    gid_b = directory.ensure_block_record(
        "gid-b",
        workflow_id="wf-failed-commit",
        owner_shard="shard-a",
        physical_node_id="node-a",
        local_pid=102,
        refcount=1,
    ).global_block_id

    directory.begin_handoff(
        handoff_id="handoff-failed-commit",
        state_id="state-failed-commit",
        workflow_id="wf-failed-commit",
        source_agent_id="agent-a",
        target_agent_id="agent-b",
        source_node_id="node-a",
        target_node_id=target_node_id,
        block_ids=[gid_a, gid_b],
    )

    original_ensure = target.ensure_block_record
    calls = {"count": 0}

    def flaky_ensure(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("inject target commit failure")
        return original_ensure(*args, **kwargs)

    monkeypatch.setattr(target, "ensure_block_record", flaky_ensure)

    try:
        directory.commit_handoff("handoff-failed-commit")
    except RuntimeError as exc:
        assert "inject target commit failure" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected injected commit failure")

    source_record_a = source.get_record(gid_a)
    source_record_b = source.get_record(gid_b)
    target_record_a = target.get_record(gid_a)
    target_record_b = target.get_record(gid_b)

    assert source_record_a is not None
    assert source_record_b is not None
    assert source_record_a.owner_shard == "shard-a"
    assert source_record_b.owner_shard == "shard-a"
    assert target_record_a is not None
    assert target_record_b is not None
    assert target_record_a.external_placeholder is True
    assert target_record_b.external_placeholder is True
    handoff = directory.get_handoff("handoff-failed-commit")
    assert handoff is not None
    assert handoff.status == "PREPARED"
