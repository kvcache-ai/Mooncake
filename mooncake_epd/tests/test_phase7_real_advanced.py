"""Phase 7: advanced coordination features validated with real Qwen3-VL tensors.

This module intentionally avoids mock tensors. It exercises advanced
capabilities on top of actual multimodal feature bundles and KV pages:

- pull-mode feature/page transfer
- offload / restore lifecycle
- anchor-based A2A handoff
- scheduler preemption coupled with real-state offload
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.agent.coordination import (  # noqa: E402
    AgentRequest,
    AgentScheduler,
    AgentType,
    OffloadManager,
    WorkerLoad,
)
from mooncake_epd.core.epd_workers import EncoderWorker, PrefillWorker  # noqa: E402
from mooncake_epd.core.state import (  # noqa: E402
    AnchorPool,
    FeatureStore,
    PagedKVManager,
    RadixTree,
    StateLayer,
    StateMeta,
)
from mooncake_epd.core.transfer import (  # noqa: E402
    Channel,
    Mode,
    TransferEngine,
    TransferPolicy,
)
from mooncake_epd.tests.conftest import (  # noqa: E402
    MODEL_PATH,
    cleanup_torch,
    get_test_gpu_id,
    get_test_gpu_device,
    require_real_model_and_gpus,
)
from mooncake_epd.tests.dataset import build_dataset, make_image  # noqa: E402


pytestmark = [pytest.mark.real_model, pytest.mark.gpu, pytest.mark.slow]

GPU_ENC_ID = get_test_gpu_id("ENC", 3)
GPU_PRE_ID = get_test_gpu_id("PRE", 4)
GPU_DEC_ID = get_test_gpu_id("DEC", 5)
GPU_ENC = get_test_gpu_device("ENC", 3)
GPU_PRE = get_test_gpu_device("PRE", 4)
GPU_DEC = get_test_gpu_device("DEC", 5)


def load_model(device: torch.device):
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration

    proc = AutoProcessor.from_pretrained(MODEL_PATH)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        MODEL_PATH,
        dtype=torch.bfloat16,
        device_map={"": device},
        low_cpu_mem_usage=True,
    )
    model.eval()
    return model, proc


def build_inputs(proc, *, device: torch.device):
    ex = build_dataset()[0]
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": ex.system_prompt}],
        },
        {
            "role": "user",
            "content": [
                {"type": "image", "image": make_image(ex.image_name)},
                {"type": "text", "text": ex.steps[0]},
            ],
        },
    ]
    inputs = proc.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=False,
        return_dict=True,
        return_tensors="pt",
    )
    return ex, {k: v.to(device) for k, v in inputs.items()}


@pytest.fixture(scope="module")
def model_proc_enc():
    require_real_model_and_gpus(GPU_ENC_ID, GPU_PRE_ID, GPU_DEC_ID)
    model, proc = load_model(GPU_ENC)
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[GPU_ENC_ID])


@pytest.fixture(scope="module")
def model_proc_pre():
    model, proc = load_model(GPU_PRE)
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[GPU_PRE_ID])


@pytest.fixture(scope="module")
def transfer_engine():
    return TransferEngine(protocol="local")


@pytest.fixture()
def real_payload(model_proc_enc, model_proc_pre, transfer_engine):
    model_enc, proc_enc = model_proc_enc
    model_pre, proc_pre = model_proc_pre
    ex, inputs_enc = build_inputs(proc_enc, device=GPU_ENC)
    encoder = EncoderWorker(model_enc, proc_enc, GPU_ENC)
    enc_out = encoder.encode(inputs_enc["pixel_values"], inputs_enc["image_grid_thw"])
    pm_pre = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=GPU_PRE,
    )
    prefill = PrefillWorker(model_pre, proc_pre, GPU_PRE, pm_pre)
    bundle_pre = transfer_engine.transfer_feature_bundle(
        enc_out.bundle,
        GPU_PRE,
        TransferPolicy(mode=Mode.PULL, channel=Channel.ENCODER_TO_PREFILL),
    )
    pout = prefill.prefill(
        input_ids=inputs_enc["input_ids"].to(GPU_PRE),
        bundle=bundle_pre,
        attention_mask=inputs_enc.get("attention_mask").to(GPU_PRE),
        mm_token_type_ids=(inputs_enc.get("mm_token_type_ids").to(GPU_PRE) if inputs_enc.get("mm_token_type_ids") is not None else None),
    )
    try:
        yield {
            "example": ex,
            "inputs_enc": inputs_enc,
            "enc_out": enc_out,
            "bundle_pre": bundle_pre,
            "pm_pre": pm_pre,
            "pout": pout,
        }
    finally:
        try:
            pm_pre.release_refs(pout.kv_refs)
        except Exception:
            pass


def test_real_pull_transfer_pages_and_features(real_payload, transfer_engine):
    enc_out = real_payload["enc_out"]
    bundle_pre = real_payload["bundle_pre"]
    pm_pre = real_payload["pm_pre"]
    pout = real_payload["pout"]

    bundle_pull = transfer_engine.transfer_feature_bundle(
        enc_out.bundle,
        GPU_PRE,
        TransferPolicy(mode=Mode.PULL, channel=Channel.ENCODER_TO_PREFILL),
    )
    assert bundle_pull.image_hash == enc_out.bundle.image_hash
    assert bundle_pull.last_hidden.device == GPU_PRE
    assert bundle_pull.last_hidden.shape == bundle_pre.last_hidden.shape

    pm_dec, refs_dec = transfer_engine.transfer_pages(
        pm_pre,
        pout.kv_refs,
        GPU_DEC,
        TransferPolicy(mode=Mode.PULL, channel=Channel.PREFILL_TO_DECODE),
    )
    try:
        assert pm_dec.device == GPU_DEC
        assert len(refs_dec) == len(pout.kv_refs)
        for src_ref, dst_ref in zip(pout.kv_refs, refs_dec):
            src_k, src_v = pm_pre.get_page(src_ref)
            dst_k, dst_v = pm_dec.get_page(dst_ref)
            assert src_k.shape == dst_k.shape
            assert src_v.shape == dst_v.shape
            assert (src_k.to(GPU_DEC) - dst_k).abs().max().item() == 0.0
            assert (src_v.to(GPU_DEC) - dst_v).abs().max().item() == 0.0
    finally:
        pm_dec.release_refs(refs_dec)


def test_real_offload_restore_qwen_state(real_payload):
    pm_pre = real_payload["pm_pre"]
    pout = real_payload["pout"]
    bundle_pre = real_payload["bundle_pre"]
    tokens = real_payload["inputs_enc"]["input_ids"][0].tolist()

    sl = StateLayer(
        pm_pre,
        RadixTree(pm_pre, max_entries=128),
        FeatureStore(),
        default_ttl_seconds=120.0,
    )
    sl.features.put(bundle_pre.image_hash, bundle_pre)
    state = sl.register(
        kv_refs=pout.kv_refs,
        feature_hash=bundle_pre.image_hash,
        meta=StateMeta(
            token_ids=tokens,
            image_ids=[bundle_pre.image_hash],
            workflow_id="wf-offload-real",
            agent_id="agent-offload",
        ),
    )
    original_pages = [
        tuple(t.clone() for t in pm_pre.get_page(ref))
        for ref in state.kv_refs
    ]

    om = OffloadManager(sl, GPU_PRE)
    handle = om.offload(state, expected_return_seconds=10.0)
    assert handle == state.state_id
    assert state.status == "OFFLOADED"
    assert state.kv_refs == []

    restored = om.restore(handle)
    assert restored is state
    assert restored.status == "ACTIVE"
    assert len(restored.kv_refs) == len(original_pages)
    for (orig_k, orig_v), new_ref in zip(original_pages, restored.kv_refs):
        new_k, new_v = pm_pre.get_page(new_ref)
        assert (orig_k - new_k).abs().max().item() == 0.0
        assert (orig_v - new_v).abs().max().item() == 0.0

    sl.release(restored)
    sl.radix.clear()
    sl.features.clear()


def test_real_anchor_handoff_and_scheduler_preemption(real_payload):
    pm_pre = real_payload["pm_pre"]
    pout = real_payload["pout"]
    bundle_pre = real_payload["bundle_pre"]
    tokens = real_payload["inputs_enc"]["input_ids"][0].tolist()

    sl = StateLayer(
        pm_pre,
        RadixTree(pm_pre, max_entries=128),
        FeatureStore(),
        default_ttl_seconds=120.0,
    )
    sl.features.put(bundle_pre.image_hash, bundle_pre)
    state = sl.register(
        kv_refs=pout.kv_refs,
        feature_hash=bundle_pre.image_hash,
        meta=StateMeta(
            token_ids=tokens,
            image_ids=[bundle_pre.image_hash],
            workflow_id="wf-a2a-real",
            agent_id="agent-A",
        ),
    )

    pool = AnchorPool(pm_pre)
    pool.register_anchor(
        agent_id="agent-A",
        token_ids=state.meta.token_ids,
        refs=state.kv_refs,
        feature_hashes=state.feature_hashes,
    )
    hit_refs, unmatched = pool.lookup(state.meta.token_ids + [11, 12, 13], exclude_agent="agent-B")
    assert sum(ref.filled for ref in hit_refs) > 0
    assert len(unmatched) < len(state.meta.token_ids) + 3
    for ref in hit_refs:
        pm_pre.decref(ref.physical_id)

    sl.handoff(state, to_agent_id="agent-B")
    assert state.meta.agent_id == "agent-B"
    assert state.status == "ACTIVE"

    om = OffloadManager(sl, GPU_PRE)
    sched = AgentScheduler()
    sched.add_worker(WorkerLoad("decode-1", "decode", current_load=79, max_capacity=80, avg_latency_ms=40.0))
    low = AgentRequest("low-real", AgentType.INTERACTIVE, input_tokens=len(tokens), priority=1)
    low.state = state
    sched.register_active(low)
    high = AgentRequest("high-real", AgentType.THINKING, input_tokens=1024, priority=10)
    victims = sched.preempt_for(high, offload_manager=om)

    assert len(victims) == 1
    assert victims[0].request_id == "low-real"
    assert state.status == "OFFLOADED"
    assert state.kv_refs == []
    assert om.stats()["offloaded"] == 1

    restored = om.restore(state.state_id)
    assert restored is state
    assert restored.status == "ACTIVE"
    assert len(restored.kv_refs) > 0

    pool.release_all()
    sl.release(restored)
    sl.radix.clear()
    sl.features.clear()
