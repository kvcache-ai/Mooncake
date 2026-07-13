from __future__ import annotations

import torch

from mooncake_epd.agent.coordination.scheduler import AgentType
from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.omni_pipeline import OmniPipeline, OmniPipelineRuntime
from mooncake_epd.core.state import FeatureBundle, MooncakeKVStateStore, PagedKVManager
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy
from mooncake_epd.scripts.vllm_disagg_proxy import _merge_control_headers


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


def test_cross_node_descriptor_share_increments_owner_refs_without_copy():
    pm = _pm("prefill-a")
    refs = pm.allocate_pages(1, filled=4)
    key = torch.ones((2, 1, 4, 2), dtype=torch.float32)
    val = torch.full((2, 1, 4, 2), 2.0, dtype=torch.float32)
    pm.write_page_slots(refs[0], key, val)
    before = pm.refcount(refs[0].global_block_id)

    store = MooncakeKVStateStore(
        pm,
        node_id="prefill-a",
        allow_remote_descriptor_sharing=True,
    )
    parent = store.register_state(refs, workflow_id="wf", state_id="parent")
    child = store.clone_state(
        parent.state_id,
        child_state_id="child",
        target_node_id="decode-b",
        share_remote_descriptor=True,
    )

    assert child.owner_node_id == "prefill-a"
    assert child.metadata["remote_descriptor_shared"] is True
    assert child.metadata["target_node_id"] == "decode-b"
    assert child.block_descriptors[0].physical_id == refs[0].physical_id
    assert pm.refcount(refs[0].global_block_id) == before + 1

    # Releasing the child decrements the owner page; releasing parent frees it.
    assert store.release_state("child") == 0
    assert pm.refcount(refs[0].global_block_id) == before
    assert store.release_state("parent") == 1


def test_serving_control_plane_accepts_agent_type_priority_metadata():
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy"))
    cp.register_stage_workers("prefill", ["prefill-a", "prefill-b"])
    cp.update_worker_load("prefill", "prefill-a", current_load=5, max_capacity=8, queue_size=3)
    cp.update_worker_load("prefill", "prefill-b", current_load=0, max_capacity=8, queue_size=0)
    req = {
        "metadata": {
            "agent_type": "THINKING",
            "priority": 9,
            "workflow_id": "wf-agent",
        },
        "messages": [{"role": "user", "content": "think deeply"}],
    }
    ctx = cp.start_request(req, "req-agent")
    decision = cp.admit_stage("prefill", ctx)
    kv = cp.build_prefill_kv_params(ctx, decision, decode_worker_id="decode-a")

    assert ctx.agent_type_hint is AgentType.THINKING
    assert ctx.priority == 9
    assert kv["agent_type"] == "THINKING"
    assert kv["priority"] == 9
    assert decision.worker_id == "prefill-b"


class _Headers:
    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _Request:
    def __init__(self, headers):
        self.headers = _Headers(headers)


def test_proxy_control_headers_merge_without_overwriting_body_metadata():
    body = {"metadata": {"agent_type": "HYBRID", "workflow_id": "body-wf"}}
    merged = _merge_control_headers(
        body,
        _Request(
            {
                "X-Agent-Type": "THINKING",
                "X-Agent-Priority": "7",
                "X-Workflow-Id": "header-wf",
            }
        ),
    )
    assert merged["metadata"]["agent_type"] == "HYBRID"
    assert merged["metadata"]["workflow_id"] == "body-wf"
    assert merged["metadata"]["priority"] == "7"
    assert body["metadata"].get("priority") is None


def test_omni_pipeline_runtime_moves_tensors_and_feature_bundle():
    class AR:
        name = "ar"

        def run(self, inputs):
            return [
                torch.tensor([1.0, 2.0]),
                FeatureBundle(
                    image_hash="img",
                    last_hidden=torch.tensor([[3.0, 4.0]]),
                    intermediates=[],
                    grid_thw=torch.tensor([[1, 1, 1]]),
                ),
            ]

    class Gen:
        name = "generation"

        def run(self, inputs):
            tensor, bundle = inputs
            return [tensor + bundle.last_hidden.flatten()]

    pipe = OmniPipeline(
        [AR(), Gen()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu"],
        policy_per_edge=[
            TransferPolicy(
                Mode.SHM,
                channel=Channel.AGENT_TO_AGENT,
                extra={"force_copy": True},
            )
        ],
    )
    runtime = OmniPipelineRuntime(pipe, queue_size=2)
    try:
        out = runtime.run([], timeout=5.0)
    finally:
        runtime.stop()
    assert torch.equal(out[0], torch.tensor([4.0, 6.0]))
    stats = runtime.stats()
    assert stats["edges"]["ar->generation"]["transfers"] == 1
    assert stats["runtime"]["jobs"] == 0
