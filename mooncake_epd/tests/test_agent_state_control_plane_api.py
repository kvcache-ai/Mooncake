from __future__ import annotations

from fastapi.testclient import TestClient

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.state import WorkflowStateRegistry
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
