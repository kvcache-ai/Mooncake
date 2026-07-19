from __future__ import annotations

from fastapi.testclient import TestClient
import torch
import pytest

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.state import MooncakeKVStateStore, PagedKVManager, WorkflowStateRegistry
from mooncake_epd.scripts.vllm_disagg_proxy import ProxyConfig, create_app


def _control_plane(tmp_path):
    return ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-agent-api",
            enable_agent_state_clone=True,
            workflow_registry_wal_path=str(tmp_path / "agent-registry.jsonl"),
        ),
        workflow_registry=WorkflowStateRegistry(str(tmp_path / "agent-registry.jsonl")),
    )


def test_agent_state_register_release_stats_are_idempotent(tmp_path):
    cp = _control_plane(tmp_path)
    cp.kv_directory.ensure_block_record(
        "engine:1",
        workflow_id="wf-agent",
        owner_shard="prefill-0",
        physical_node_id="prefill-0",
        external_placeholder=True,
    )
    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)

    with TestClient(app) as client:
        reg = client.post(
            "/mooncake_epd/agent_state/register",
            json={
                "workflow_id": "wf-agent",
                "state_id": "state-parent",
                "kv_block_ids": ["engine:1"],
                "target_node_id": "prefill-0",
            },
        )
        assert reg.status_code == 200
        assert reg.json()["registered"] is True
        assert reg.json()["refcounts"] == {"engine:1": 2}

        reg2 = client.post(
            "/mooncake_epd/agent_state/register",
            json={
                "workflow_id": "wf-agent",
                "state_id": "state-parent",
                "kv_block_ids": ["engine:1"],
            },
        )
        assert reg2.status_code == 200
        assert reg2.json()["idempotent"] is True
        assert cp.kv_directory.refcount("engine:1") == 2

        stats = client.get("/mooncake_epd/agent_state/stats").json()
        assert stats["active_states"] == 1
        assert stats["refcounts"] == {"engine:1": 2}

        rel = client.post(
            "/mooncake_epd/agent_state/release",
            json={"state_id": "state-parent", "release_physical": False},
        )
        assert rel.status_code == 200
        assert rel.json()["released"] is True
        assert rel.json()["refcounts"] == {"engine:1": 1}

        rel2 = client.post("/mooncake_epd/agent_state/release", json={"state_id": "state-parent"})
        assert rel2.status_code == 200
        assert rel2.json()["idempotent"] is True
        assert cp.kv_directory.refcount("engine:1") == 1


def test_agent_state_capture_refuses_client_block_ids_without_adapter_owned_refs(tmp_path):
    cp = _control_plane(tmp_path)
    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)

    with TestClient(app) as client:
        response = client.post(
            "/mooncake_epd/agent_state/capture",
            json={"request_id": "prefill-request-1", "kv_block_ids": ["untrusted:7"]},
        )

    assert response.status_code == 501
    assert "server-owned allocator KV refs" in response.text


def test_agent_state_get_exposes_catalog_and_lifecycle_metadata_not_page_payloads(tmp_path):
    cp = _control_plane(tmp_path)
    cp.kv_directory.ensure_block_record(
        "engine:status",
        workflow_id="wf-status",
        owner_shard="prefill-0",
        physical_node_id="prefill-0",
        external_placeholder=True,
    )
    cp.register_agent_state(
        workflow_id="wf-status",
        state_id="state-status",
        kv_block_ids=["engine:status"],
        target_node_id="prefill-0",
    )
    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)

    with TestClient(app) as client:
        response = client.get("/mooncake_epd/agent_state/state-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["state_id"] == "state-status"
    assert payload["kv"]["block_ids"] == ["engine:status"]
    assert payload["physical_state"]["store_backed"] is False
    assert "payload" not in payload["kv"]


def test_agent_state_fork_materialize_and_release_orphans(tmp_path):
    cp = _control_plane(tmp_path)
    cp.register_stage_workers("prefill", ["prefill-0"])
    cp.register_stage_workers("decode", ["decode-0"])
    ctx = cp.start_request({"messages": [{"role": "user", "content": "hello"}], "metadata": {"workflow_id": "wf-fork"}}, "req-parent")
    prefill = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, prefill, decode_worker_id="decode-0")
    cp.note_prefill_response(
        ctx,
        {
            "transfer_id": "xfer",
            "remote_engine_id": "engine",
            "remote_bootstrap_addr": "http://engine",
            "remote_block_ids": [7],
        },
        decode_worker_id="decode-0",
    )
    cp.commit_handoff(ctx)
    cp.finish_request("req-parent")

    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)
    with TestClient(app) as client:
        fork = client.post(
            "/mooncake_epd/agent_state/fork",
            json={"workflow_id": "wf-fork", "parent_request_id": "req-parent", "branch_count": 2},
        )
        assert fork.status_code == 200
        payload = fork.json()
        assert payload["clone_semantics"] == "same_node_block_ref_zero_copy"
        assert cp.kv_directory.refcount("engine:7") == 3

        branch_id = payload["branches"][0]["branch_id"]
        mat = client.post(
            "/mooncake_epd/agent_state/materialize",
            json={"state_id": branch_id, "target_node_id": "decode-0", "for_write": False},
        )
        assert mat.status_code == 200
        assert mat.json()["materialized"] is True

        mat_write = client.post(
            "/mooncake_epd/agent_state/materialize",
            json={"state_id": branch_id, "target_node_id": "decode-0", "for_write": True},
        )
        assert mat_write.status_code == 409
        assert "real KV materializer" in mat_write.text

        for item in payload["branches"]:
            assert client.post("/mooncake_epd/agent_state/release", json={"state_id": item["branch_id"]}).status_code == 200
        assert client.post("/mooncake_epd/agent_state/release", json={"state_id": "req-parent"}).status_code == 200
        assert cp.kv_directory.get_record("engine:7") is None
        stats = client.get("/mooncake_epd/agent_state/stats").json()
        assert stats["active_states"] == 0


def test_agent_state_remote_read_materialize_returns_consume_kv_params(tmp_path):
    cp = _control_plane(tmp_path)
    cp.kv_directory.ensure_block_record(
        "prefill-engine:42",
        workflow_id="wf-remote-read",
        owner_shard="prefill-0",
        physical_node_id="prefill-0",
        external_placeholder=True,
    )
    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)
    kv = {
        "remote_engine_id": "prefill-engine",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "remote_block_ids": [[42]],
        "a2a_source_node": "prefill-0",
        "mooncake_protocol": "tcp",
    }
    with TestClient(app) as client:
        reg = client.post(
            "/mooncake_epd/agent_state/register",
            json={
                "workflow_id": "wf-remote-read",
                "state_id": "state-remote-read",
                "kv_block_ids": ["prefill-engine:42"],
                "target_node_id": "decode-0",
                "kv_transfer_params": kv,
            },
        )
        assert reg.status_code == 200
        mat = client.post(
            "/mooncake_epd/agent_state/materialize",
            json={"state_id": "state-remote-read", "target_node_id": "decode-1"},
        )
        assert mat.status_code == 200
        payload = mat.json()
        assert payload["clone_semantics"] == "remote_read_via_mooncake_connector"
        consume = payload["consume_kv_transfer_params"]
        assert consume["do_remote_prefill"] is True
        assert consume["remote_engine_id"] == "prefill-engine"
        assert consume["remote_block_ids"] == [[42]]
        assert consume["a2a_target_node"] == "decode-1"


def test_agent_state_ttl_sweep_releases_expired_branch(tmp_path):
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-ttl",
            enable_agent_state_clone=True,
            workflow_registry_wal_path=str(tmp_path / "ttl-registry.jsonl"),
            request_state_ttl_seconds=1.0,
        ),
        workflow_registry=WorkflowStateRegistry(str(tmp_path / "ttl-registry.jsonl")),
    )
    cp.kv_directory.ensure_block_record(
        "engine:ttl",
        workflow_id="wf-ttl",
        owner_shard="decode-0",
        physical_node_id="decode-0",
        external_placeholder=True,
    )
    cp.register_agent_state(
        workflow_id="wf-ttl",
        state_id="state-ttl",
        kv_block_ids=["engine:ttl"],
        target_node_id="decode-0",
    )
    assert cp.kv_directory.refcount("engine:ttl") == 2
    released = cp.sweep_expired_agent_states(now=__import__("time").monotonic() + 10.0)
    assert released == 1
    assert cp.workflow_registry.get_record("state-ttl").status == "RELEASED"
    assert cp.kv_directory.refcount("engine:ttl") == 1


def test_server_owned_store_state_fork_and_materialize(tmp_path):
    source = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="prefill-0",
    )
    target = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="decode-0",
    )
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(ref, key, key + 10)
    state_store = MooncakeKVStateStore(
        source,
        node_id="prefill-0",
        page_managers_by_node={"decode-0": target},
        allow_remote_descriptor_sharing=True,
        remote_materializer=lambda descriptor, node: [
            target.allocate_page(filled=source_ref.filled)
            for source_ref in descriptor.to_refs()
        ],
    )
    connector_commits = []

    def _commit_to_connector(envelope):
        connector_commits.append(dict(envelope))
        return {"accepted": True, "target": envelope["target_node_id"]}

    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy",
            enable_agent_state_clone=True,
            require_real_agent_state_store=True,
            # Strict Agent continuation must not report Store materialization
            # as a vLLM-consumable state until a version-locked connector has
            # acknowledged the target block metadata.
            strict_no_fallback=True,
        ),
        kv_directory=source.kv_directory,
        kv_state_store=state_store,
        agent_state_connector_committer=_commit_to_connector,
    )
    registered = cp.register_agent_state(
        workflow_id="wf-real",
        state_id="parent",
        capture_request_id="prefill-capture-request",
        kv_block_ids=[],
        kv_refs=[ref],
        target_node_id="prefill-0",
    )
    assert registered["store_backed"] is True
    app = create_app(ProxyConfig(enable_mm_prefetch=False), control_plane=cp)
    with TestClient(app) as client:
        captured = client.post(
            "/mooncake_epd/agent_state/capture",
            json={"request_id": "prefill-capture-request"},
        )
    assert captured.status_code == 200
    assert captured.json()["captured"] is True
    assert captured.json()["physical_state"]["allocator_state_live"] is True
    materialized_parent = cp.materialize_agent_state(
        state_id="parent",
        target_node_id="prefill-0",
    )
    assert materialized_parent["materialized"] is True
    assert materialized_parent["backend"] == "same_node_block_ref"
    assert materialized_parent["connector_committed"] is True
    assert materialized_parent["connector_commit"] == {
        "accepted": True,
        "target": "prefill-0",
    }
    assert connector_commits == [
        {
            "epd_agent_state_id": materialized_parent["state_id"],
            "workflow_id": "wf-real",
            "vllm_kv_block_ids": materialized_parent["kv_block_ids"],
            "kv_state_manifests": materialized_parent["kv_manifests"],
            "target_node_id": "prefill-0",
            "materialized_bytes": 0,
            "backend": "same_node_block_ref",
        }
    ]
    result = cp.fork_workflow_state(
        workflow_id="wf-real",
        parent_request_id="parent",
        branch_count=2,
        target_node_id="prefill-0",
    )
    assert result["clone_semantics"] == "same_node_physical_zero_copy"
    assert all(item["agent_state_store_state_id"] for item in result["branches"])
    for branch in result["branches"]:
        assert cp.release_agent_state(state_id=branch["branch_id"])["released"] is True
    assert cp.release_agent_state(state_id="parent")["released"] is True


def test_strict_store_materialization_rejects_missing_vllm_connector_commit():
    source = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id="prefill-0",
    )
    ref = source.allocate_page(filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    source.write_page_slots(ref, key, key + 10)
    state_store = MooncakeKVStateStore(source, node_id="prefill-0")
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy",
            enable_agent_state_clone=True,
            strict_no_fallback=True,
        ),
        kv_directory=source.kv_directory,
        kv_state_store=state_store,
    )
    cp.register_agent_state(
        workflow_id="wf-strict-store",
        state_id="state-strict-store",
        kv_block_ids=[],
        kv_refs=[ref],
        target_node_id="prefill-0",
    )

    with pytest.raises(RuntimeError, match="connector commit callback"):
        cp.materialize_agent_state(
            state_id="state-strict-store",
            target_node_id="prefill-0",
        )


def test_strict_agent_state_registration_rejects_external_block_id_placeholder():
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            enable_agent_state_clone=True,
            strict_no_fallback=True,
        )
    )
    with pytest.raises(RuntimeError, match="server-owned KV refs"):
        cp.register_agent_state(
            workflow_id="wf-strict",
            state_id="external",
            kv_block_ids=["untrusted:block"],
        )
