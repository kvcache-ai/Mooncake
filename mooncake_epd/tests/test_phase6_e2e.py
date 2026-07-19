"""Phase 6: real-model end-to-end metrics on the agent dataset.

Uses true Qwen3-VL tensors and four-way GPU separation:

- cuda:3 -> Encoder
- cuda:4 -> Prefill / state / reuse accounting
- cuda:5 -> Decode
- cuda:6 -> single-node baseline

No mock path is used in this module.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List

import pytest
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import DecodeWorker, EncoderWorker, PrefillWorker  # noqa: E402
from mooncake_epd.core.state import (  # noqa: E402
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
from mooncake_epd.tests.dataset import (  # noqa: E402
    WorkflowExample,
    build_dataset,
    make_image,
    summarize,
)


pytestmark = [
    pytest.mark.real_model,
    pytest.mark.gpu,
    pytest.mark.gpu_single_node,
    pytest.mark.slow,
]

GPU_ENC_ID = get_test_gpu_id("ENC", 3)
GPU_PRE_ID = get_test_gpu_id("PRE", 4)
GPU_DEC_ID = get_test_gpu_id("DEC", 5)
GPU_BASE_ID = get_test_gpu_id("BASE", 6)
GPU_ENC = get_test_gpu_device("ENC", 3)
GPU_PRE = get_test_gpu_device("PRE", 4)
GPU_DEC = get_test_gpu_device("DEC", 5)
GPU_BASE = get_test_gpu_device("BASE", 6)
ARTIFACTS_DIR = REPO_ROOT / "artifacts"
ARTIFACT_PATH = ARTIFACTS_DIR / "phase6_metrics.json"


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


def build_messages(
    ex: WorkflowExample,
    up_to_step: int,
    *,
    assistant_reply: str = "",
) -> List[dict]:
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": ex.system_prompt}],
        }
    ]
    messages.append(
        {
            "role": "user",
            "content": [
                {"type": "image", "image": make_image(ex.image_name)},
                {"type": "text", "text": ex.steps[0]},
            ],
        }
    )
    if assistant_reply:
        messages.append(
            {
                "role": "assistant",
                "content": [{"type": "text", "text": assistant_reply}],
            }
        )
    for i in range(1, up_to_step + 1):
        messages.append(
            {
                "role": "user",
                "content": [{"type": "text", "text": ex.steps[i]}],
            }
        )
    return messages


def prepare_inputs(proc, messages: List[dict], device: torch.device, *, add_generation_prompt: bool):
    inputs = proc.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=add_generation_prompt,
        return_dict=True,
        return_tensors="pt",
    )
    return {k: v.to(device) for k, v in inputs.items()}


def choose_examples() -> List[WorkflowExample]:
    ds = build_dataset()
    chosen: List[WorkflowExample] = []
    for scenario in ("tool_use_vqa", "multi_turn", "tree_of_thought", "a2a_handoff"):
        for ex in ds:
            if ex.scenario == scenario:
                chosen.append(ex)
                break
    return chosen


@pytest.fixture(scope="module")
def phase6_report():
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    report: Dict[str, object] = {
        "dataset": summarize(build_dataset()),
        "selected_examples": [],
        "baseline": [],
        "epd_step0": [],
        "cross_step_reuse": [],
        "summary": {},
    }
    try:
        yield report
    finally:
        with ARTIFACT_PATH.open("w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)


@pytest.fixture(scope="module")
def examples(phase6_report):
    require_real_model_and_gpus(GPU_ENC_ID, GPU_PRE_ID, GPU_DEC_ID, GPU_BASE_ID)
    chosen = choose_examples()
    phase6_report["selected_examples"] = [
        {"scenario": ex.scenario, "image": ex.image_name, "steps": len(ex.steps)}
        for ex in chosen
    ]
    return chosen


@pytest.fixture(scope="module")
def model_proc_enc():
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
def model_proc_dec():
    model, proc = load_model(GPU_DEC)
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[GPU_DEC_ID])


@pytest.fixture(scope="module")
def model_proc_base():
    model, proc = load_model(GPU_BASE)
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[GPU_BASE_ID])


@pytest.fixture(scope="module")
def transfer_engine():
    return TransferEngine(protocol="local")


@pytest.fixture(scope="module")
def epd_stack(model_proc_enc, model_proc_pre, model_proc_dec):
    model_enc, proc_enc = model_proc_enc
    model_pre, proc_pre = model_proc_pre
    model_dec, proc_dec = model_proc_dec
    pm_pre = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=GPU_PRE,
    )
    return {
        "encoder": EncoderWorker(model_enc, proc_enc, GPU_ENC),
        "prefill": PrefillWorker(model_pre, proc_pre, GPU_PRE, pm_pre),
        "decode": DecodeWorker(model_dec, proc_dec, GPU_DEC, pm_pre),
        "pm_pre": pm_pre,
        "proc_enc": proc_enc,
        "proc_pre": proc_pre,
        "proc_dec": proc_dec,
    }


def test_phase6_baseline_dataset(examples, model_proc_base, phase6_report):
    model_base, proc_base = model_proc_base
    results = []
    for ex in examples:
        messages = build_messages(ex, up_to_step=0)
        inputs = prepare_inputs(proc_base, messages, GPU_BASE, add_generation_prompt=True)
        torch.cuda.synchronize(GPU_BASE)
        t0 = time.perf_counter()
        out = model_base.generate(**inputs, max_new_tokens=12, do_sample=False)
        torch.cuda.synchronize(GPU_BASE)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        prompt_len = int(inputs["input_ids"].shape[-1])
        gen_len = int(out.shape[-1] - prompt_len)
        text = proc_base.decode(out[0, prompt_len:], skip_special_tokens=True)
        result = {
            "scenario": ex.scenario,
            "image": ex.image_name,
            "prompt_tokens": prompt_len,
            "generated_tokens": gen_len,
            "total_ms": float(elapsed_ms),
            "tokens_per_sec": float(gen_len / max(elapsed_ms / 1000, 1e-6)),
            "text": text[:160],
        }
        print(
            f"[baseline] {ex.scenario}/{ex.image_name}: "
            f"prompt={prompt_len} gen={gen_len} total={elapsed_ms:.1f}ms "
            f"tps={result['tokens_per_sec']:.2f}"
        )
        assert gen_len > 0
        results.append(result)

    phase6_report["baseline"] = results
    phase6_report["summary"]["baseline_avg_ms"] = statistics.mean(r["total_ms"] for r in results)
    phase6_report["summary"]["baseline_avg_tps"] = statistics.mean(r["tokens_per_sec"] for r in results)


def test_phase6_epd_step0_dataset(examples, epd_stack, transfer_engine, phase6_report):
    encoder = epd_stack["encoder"]
    prefill = epd_stack["prefill"]
    decode = epd_stack["decode"]
    proc_enc = epd_stack["proc_enc"]
    pm_pre = epd_stack["pm_pre"]

    results = []
    for ex in examples:
        messages = build_messages(ex, up_to_step=0)
        inputs_enc = prepare_inputs(proc_enc, messages, GPU_ENC, add_generation_prompt=True)
        torch.cuda.synchronize(GPU_ENC)
        torch.cuda.synchronize(GPU_PRE)
        torch.cuda.synchronize(GPU_DEC)
        t0 = time.perf_counter()
        enc_out = encoder.encode(inputs_enc["pixel_values"], inputs_enc["image_grid_thw"])
        bundle_pre = transfer_engine.transfer_feature_bundle(
            enc_out.bundle,
            GPU_PRE,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
        )
        inputs_pre = {
            "input_ids": inputs_enc["input_ids"].to(GPU_PRE),
            "attention_mask": inputs_enc.get("attention_mask").to(GPU_PRE),
        }
        pout = prefill.prefill(
            input_ids=inputs_pre["input_ids"],
            bundle=bundle_pre,
            attention_mask=inputs_pre["attention_mask"],
            mm_token_type_ids=(inputs_enc.get("mm_token_type_ids").to(GPU_PRE) if inputs_enc.get("mm_token_type_ids") is not None else None),
        )
        torch.cuda.synchronize(GPU_PRE)
        ttft_ms = (time.perf_counter() - t0) * 1000
        pm_dec, refs_dec = transfer_engine.transfer_pages(
            pm_pre,
            pout.kv_refs,
            GPU_DEC,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
        )
        first_logits_dec = transfer_engine.transfer_tensor(
            pout.first_logits,
            GPU_DEC,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
        )
        decode.pm = pm_dec
        dout = decode.decode(
            first_logits=first_logits_dec,
            kv_refs=refs_dec,
            max_new_tokens=12,
            rope_deltas=pout.rope_deltas,
        )
        torch.cuda.synchronize(GPU_DEC)
        total_ms = (time.perf_counter() - t0) * 1000
        result = {
            "scenario": ex.scenario,
            "image": ex.image_name,
            "prompt_tokens": int(inputs_enc["input_ids"].shape[-1]),
            "encode_ms": float(enc_out.encode_time_ms),
            "prefill_ms": float(pout.prefill_time_ms),
            "ttft_ms": float(ttft_ms),
            "decode_ms": float(dout.decode_time_ms),
            "generated_tokens": len(dout.generated_ids),
            "decode_tok_per_sec": float(dout.tokens_per_second),
            "total_ms": float(total_ms),
            "text": dout.generated_text[:160],
        }
        print(
            f"[epd] {ex.scenario}/{ex.image_name}: "
            f"enc={enc_out.encode_time_ms:.1f}ms prefill={pout.prefill_time_ms:.1f}ms "
            f"ttft={ttft_ms:.1f}ms decode={dout.tokens_per_second:.2f} tok/s"
        )
        assert len(pout.kv_refs) > 0
        assert len(dout.generated_ids) > 0
        results.append(result)

        pm_dec.release_refs(refs_dec)
        pm_pre.release_refs(pout.kv_refs)

    phase6_report["epd_step0"] = results
    phase6_report["summary"]["epd_avg_ttft_ms"] = statistics.mean(r["ttft_ms"] for r in results)
    phase6_report["summary"]["epd_avg_decode_tps"] = statistics.mean(r["decode_tok_per_sec"] for r in results)


def test_phase6_cross_step_reuse_dataset(examples, epd_stack, transfer_engine, phase6_report):
    encoder = epd_stack["encoder"]
    prefill = epd_stack["prefill"]
    proc_enc = epd_stack["proc_enc"]
    pm = epd_stack["pm_pre"]
    sl = StateLayer(
        pm,
        RadixTree(pm, max_entries=256),
        FeatureStore(),
        default_ttl_seconds=300.0,
    )

    results = []
    for ex in examples:
        if len(ex.steps) < 2:
            continue

        messages0 = build_messages(ex, up_to_step=0)
        inputs0 = prepare_inputs(proc_enc, messages0, GPU_ENC, add_generation_prompt=False)
        enc_out = encoder.encode(inputs0["pixel_values"], inputs0["image_grid_thw"])
        bundle_pre = transfer_engine.transfer_feature_bundle(
            enc_out.bundle,
            GPU_PRE,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
        )
        sl.features.put(bundle_pre.image_hash, bundle_pre)
        pout0 = prefill.prefill(
            input_ids=inputs0["input_ids"].to(GPU_PRE),
            bundle=bundle_pre,
            attention_mask=inputs0.get("attention_mask").to(GPU_PRE),
            mm_token_type_ids=(inputs0.get("mm_token_type_ids").to(GPU_PRE) if inputs0.get("mm_token_type_ids") is not None else None),
        )
        workflow_id = f"wf-{ex.scenario}-{ex.image_name}"
        state0 = sl.register(
            kv_refs=pout0.kv_refs,
            feature_hash=bundle_pre.image_hash,
            meta=StateMeta(
                token_ids=inputs0["input_ids"][0].tolist(),
                image_ids=[bundle_pre.image_hash],
                workflow_id=workflow_id,
                step=0,
                agent_id="phase6-agent",
            ),
        )

        messages1 = build_messages(
            ex,
            up_to_step=1,
            assistant_reply="I have inspected the image and extracted the visible details.",
        )
        inputs1 = prepare_inputs(proc_enc, messages1, GPU_ENC, add_generation_prompt=False)
        tokens1 = inputs1["input_ids"][0].tolist()
        torch.cuda.synchronize(GPU_PRE)
        t0 = time.perf_counter()
        matched, delta = sl.borrow_prefix_refs(tokens1, workflow_id=workflow_id)
        torch.cuda.synchronize(GPU_PRE)
        match_ms = (time.perf_counter() - t0) * 1000
        matched_tokens = sum(r.filled for r in matched)
        total_tokens = len(tokens1)
        reuse_ratio = matched_tokens / total_tokens if total_tokens else 0.0
        result = {
            "scenario": ex.scenario,
            "image": ex.image_name,
            "step0_tokens": len(state0.meta.token_ids),
            "step1_tokens": total_tokens,
            "matched_tokens": matched_tokens,
            "delta_tokens": len(delta),
            "reuse_ratio": float(reuse_ratio),
            "match_time_ms": float(match_ms),
            "feature_refcount": sl.features.refcount(bundle_pre.image_hash),
        }
        print(
            f"[reuse] {ex.scenario}/{ex.image_name}: "
            f"matched={matched_tokens}/{total_tokens} reuse={reuse_ratio:.2f} "
            f"match={match_ms:.3f}ms"
        )
        assert matched_tokens > 0
        assert reuse_ratio > 0.0
        results.append(result)

        sl.pm.release_refs(matched)
        sl.release(state0)
        sl.radix.clear()
        sl.features.clear()

    phase6_report["cross_step_reuse"] = results
    phase6_report["summary"]["cross_step_avg_reuse_ratio"] = statistics.mean(
        r["reuse_ratio"] for r in results
    )
    assert ARTIFACT_PATH.parent.exists()
