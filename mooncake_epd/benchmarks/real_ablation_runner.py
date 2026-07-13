"""Real-model RFC §8.5 ablation runners for B1/B2/B3/B5/B6/B7/B8.

Industrial constraints:
- no mock paths;
- real Qwen3-VL model + real GPUs;
- reusable artifact schema for RFC matrix integration.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import torch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mooncake_epd.agent.coordination.reuse_pipeline import ReusePipeline
from mooncake_epd.core.epd_workers import DecodeWorker, EncoderWorker, PrefillWorker
from mooncake_epd.core.state import FeatureBundle, FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy
from mooncake_epd.tests.dataset import WorkflowExample, build_dataset, make_image, summarize


MODEL_PATH = os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
DEFAULT_GPU_ENC = 3
DEFAULT_GPU_PRE = 4
DEFAULT_GPU_DEC = 5
DEFAULT_GPU_BASE = 6


def _cuda_device(index: int) -> torch.device:
    return torch.device(f"cuda:{int(index)}")


@dataclass
class RuntimeStack:
    gpu_enc: torch.device
    gpu_pre: torch.device
    gpu_dec: torch.device
    gpu_base: torch.device
    proc_enc: Any
    proc_pre: Any
    proc_dec: Any
    proc_base: Any
    model_enc: Any
    model_pre: Any
    model_dec: Any
    model_base: Any
    encoder: EncoderWorker
    prefill: PrefillWorker
    decode: DecodeWorker
    pm_pre: PagedKVManager
    transfer_engine: TransferEngine

    def close(self) -> None:
        for dev in (self.gpu_enc, self.gpu_pre, self.gpu_dec, self.gpu_base):
            idx = int(dev.index)
            try:
                with torch.cuda.device(idx):
                    torch.cuda.empty_cache()
            except Exception:
                pass


def require_real_env() -> None:
    if not Path(MODEL_PATH).exists():
        raise RuntimeError(f"real model not found: {MODEL_PATH}")
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA not available")
    if torch.cuda.device_count() < 4:
        raise RuntimeError(f"need at least 4 CUDA devices, found count={torch.cuda.device_count()}")


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


def build_runtime_stack(
    *,
    gpu_enc: int = DEFAULT_GPU_ENC,
    gpu_pre: int = DEFAULT_GPU_PRE,
    gpu_dec: int = DEFAULT_GPU_DEC,
    gpu_base: int = DEFAULT_GPU_BASE,
) -> RuntimeStack:
    require_real_env()
    devices = {
        "enc": _cuda_device(gpu_enc),
        "pre": _cuda_device(gpu_pre),
        "dec": _cuda_device(gpu_dec),
        "base": _cuda_device(gpu_base),
    }
    for name, dev in devices.items():
        idx = int(dev.index)
        if idx < 0 or idx >= torch.cuda.device_count():
            raise RuntimeError(f"requested {name} gpu index {idx} out of range; cuda count={torch.cuda.device_count()}")
    model_enc, proc_enc = load_model(devices["enc"])
    model_pre, proc_pre = load_model(devices["pre"])
    model_dec, proc_dec = load_model(devices["dec"])
    model_base, proc_base = load_model(devices["base"])
    pm_pre = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=devices["pre"],
    )
    transfer_engine = TransferEngine(protocol="local")
    return RuntimeStack(
        gpu_enc=devices["enc"],
        gpu_pre=devices["pre"],
        gpu_dec=devices["dec"],
        gpu_base=devices["base"],
        proc_enc=proc_enc,
        proc_pre=proc_pre,
        proc_dec=proc_dec,
        proc_base=proc_base,
        model_enc=model_enc,
        model_pre=model_pre,
        model_dec=model_dec,
        model_base=model_base,
        encoder=EncoderWorker(model_enc, proc_enc, devices["enc"]),
        prefill=PrefillWorker(model_pre, proc_pre, devices["pre"], pm_pre),
        decode=DecodeWorker(model_dec, proc_dec, devices["dec"], pm_pre),
        pm_pre=pm_pre,
        transfer_engine=transfer_engine,
    )


def build_messages(example: WorkflowExample, up_to_step: int, *, assistant_reply: str = "") -> List[dict]:
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": example.system_prompt}],
        },
        {
            "role": "user",
            "content": [
                {"type": "image", "image": make_image(example.image_name)},
                {"type": "text", "text": example.steps[0]},
            ],
        },
    ]
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
                "content": [{"type": "text", "text": example.steps[i]}],
            }
        )
    return messages


def prepare_inputs(proc, messages: List[dict], device: torch.device, *, add_generation_prompt: bool = False):
    inputs = proc.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=add_generation_prompt,
        return_dict=True,
        return_tensors="pt",
    )
    return {k: v.to(device) for k, v in inputs.items()}


def choose_examples() -> List[WorkflowExample]:
    chosen: List[WorkflowExample] = []
    for scenario in ("tool_use_vqa", "multi_turn", "tree_of_thought", "a2a_handoff"):
        for ex in build_dataset():
            if ex.scenario == scenario:
                chosen.append(ex)
                break
    return chosen


def _decode_from_prefill(
    stack: RuntimeStack,
    kv_refs: List,
    first_logits: torch.Tensor,
    rope_deltas: Optional[torch.Tensor],
    *,
    max_new_tokens: int,
) -> Dict[str, float]:
    pm_dec, refs_dec = stack.transfer_engine.transfer_pages(
        stack.pm_pre,
        kv_refs,
        stack.gpu_dec,
        TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
    )
    try:
        first_logits_dec = stack.transfer_engine.transfer_tensor(
            first_logits,
            stack.gpu_dec,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
        )
        stack.decode.pm = pm_dec
        out = stack.decode.decode(
            first_logits=first_logits_dec,
            kv_refs=refs_dec,
            max_new_tokens=max_new_tokens,
            rope_deltas=rope_deltas,
        )
        return {
            "decode_ms": float(out.decode_time_ms),
            "decode_tps": float(out.tokens_per_second),
            "generated_tokens": int(len(out.generated_ids)),
        }
    finally:
        pm_dec.release_refs(refs_dec)


def run_b1_native_pd(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for ex in examples:
        inputs = prepare_inputs(stack.proc_base, build_messages(ex, 0), stack.gpu_base, add_generation_prompt=True)
        mm_token_type_ids = inputs.get("mm_token_type_ids")
        torch.cuda.synchronize(stack.gpu_base)
        t0 = time.perf_counter()
        out = stack.model_base(
            input_ids=inputs["input_ids"],
            attention_mask=inputs.get("attention_mask"),
            pixel_values=inputs.get("pixel_values"),
            image_grid_thw=inputs.get("image_grid_thw"),
            mm_token_type_ids=mm_token_type_ids,
            use_cache=True,
            return_dict=True,
        )
        torch.cuda.synchronize(stack.gpu_base)
        ttft_ms = (time.perf_counter() - t0) * 1000.0
        prompt_len = int(inputs["input_ids"].shape[-1])
        pkv = getattr(out, "past_key_values")
        pm_pd = PagedKVManager(
            page_size=16,
            num_layers=36,
            num_kv_heads=8,
            head_dim=128,
            dtype=torch.bfloat16,
            device=stack.gpu_base,
        )
        refs = pm_pd.ingest_kv_cache(pkv)
        try:
            raw_logits = out.logits[:, -1:, :]
            decode_worker = DecodeWorker(stack.model_base, stack.proc_base, stack.gpu_base, pm_pd)
            dout = decode_worker.decode(raw_logits, refs, max_new_tokens=max_new_tokens, rope_deltas=getattr(out, "rope_deltas", None))
            total_ms = ttft_ms + dout.decode_time_ms
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "ttft_ms": float(ttft_ms),
                    "total_ms": float(total_ms),
                    "decode_tps": float(dout.tokens_per_second),
                    "generated_tokens": int(len(dout.generated_ids)),
                }
            )
        finally:
            pm_pd.release_refs(refs)
    return summarize_ablation("B1", rows)


def _encode_prefill_step0(stack: RuntimeStack, example: WorkflowExample):
    inputs_enc = prepare_inputs(stack.proc_enc, build_messages(example, 0), stack.gpu_enc, add_generation_prompt=False)
    enc_out = stack.encoder.encode(inputs_enc["pixel_values"], inputs_enc["image_grid_thw"])
    bundle_pre = stack.transfer_engine.transfer_feature_bundle(
        enc_out.bundle,
        stack.gpu_pre,
        TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
    )
    pout = stack.prefill.prefill(
        input_ids=inputs_enc["input_ids"].to(stack.gpu_pre),
        bundle=bundle_pre,
        attention_mask=inputs_enc.get("attention_mask").to(stack.gpu_pre),
        mm_token_type_ids=inputs_enc.get("mm_token_type_ids").to(stack.gpu_pre)
        if inputs_enc.get("mm_token_type_ids") is not None
        else None,
    )
    return inputs_enc, enc_out, bundle_pre, pout


def run_b3_feature_cache(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    feature_store = FeatureStore(max_bytes=2 * 1024 * 1024 * 1024)
    try:
        for ex in examples:
            inputs_enc = prepare_inputs(stack.proc_enc, build_messages(ex, 0), stack.gpu_enc, add_generation_prompt=False)
            image_hash = f"feature-cache:{ex.image_name}"
            cached_bundle = feature_store.get(image_hash)
            encode_ms = 0.0
            encoder_skipped = cached_bundle is not None
            if cached_bundle is None:
                enc_out = stack.encoder.encode(inputs_enc["pixel_values"], inputs_enc["image_grid_thw"], image_id=image_hash)
                cached_bundle = enc_out.bundle
                encode_ms = enc_out.encode_time_ms
                feature_store.put(image_hash, cached_bundle)
            bundle_pre = stack.transfer_engine.transfer_feature_bundle(
                cached_bundle,
                stack.gpu_pre,
                TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
            )
            t0 = time.perf_counter()
            pout = stack.prefill.prefill(
                input_ids=inputs_enc["input_ids"].to(stack.gpu_pre),
                bundle=bundle_pre,
                attention_mask=inputs_enc.get("attention_mask").to(stack.gpu_pre),
                mm_token_type_ids=inputs_enc.get("mm_token_type_ids").to(stack.gpu_pre)
                if inputs_enc.get("mm_token_type_ids") is not None
                else None,
            )
            torch.cuda.synchronize(stack.gpu_pre)
            prefill_ms = (time.perf_counter() - t0) * 1000.0
            decode_stats = _decode_from_prefill(stack, pout.kv_refs, pout.first_logits, pout.rope_deltas, max_new_tokens=max_new_tokens)
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "ttft_ms": float(encode_ms + prefill_ms),
                    "total_ms": float(encode_ms + prefill_ms + decode_stats["decode_ms"]),
                    "decode_tps": decode_stats["decode_tps"],
                    "generated_tokens": decode_stats["generated_tokens"],
                    "encoder_skipped": encoder_skipped,
                }
            )
            stack.pm_pre.release_refs(pout.kv_refs)
    finally:
        feature_store.clear()
    report = summarize_ablation("B3", rows)
    report["encoder_skip_rate"] = (
        sum(1 for row in rows if row.get("encoder_skipped")) / len(rows) if rows else 0.0
    )
    return report


def _prepare_reuse_state(
    stack: RuntimeStack,
    example: WorkflowExample,
    workflow_id: str,
) -> tuple[StateLayer, Dict[str, Any], Any, FeatureBundle, Any]:
    sl = StateLayer(
        stack.pm_pre,
        RadixTree(stack.pm_pre, max_entries=512),
        FeatureStore(max_bytes=2 * 1024 * 1024 * 1024),
        default_ttl_seconds=300.0,
    )
    inputs0, enc_out, bundle_pre, pout0 = _encode_prefill_step0(stack, example)
    sl.features.put(bundle_pre.image_hash, bundle_pre)
    state0 = sl.register(
        kv_refs=pout0.kv_refs,
        feature_hash=bundle_pre.image_hash,
        meta=StateMeta(
            token_ids=inputs0["input_ids"][0].tolist(),
            image_ids=[bundle_pre.image_hash],
            workflow_id=workflow_id,
            step=0,
            agent_id="ablation-agent",
        ),
    )
    return sl, inputs0, state0, bundle_pre, pout0


def run_b2_prefix_cache(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for ex in examples:
        if len(ex.steps) < 2:
            continue
        workflow_id = f"b2:{ex.scenario}:{ex.image_name}"
        sl, _, state0, _, pout0 = _prepare_reuse_state(stack, ex, workflow_id)
        try:
            messages1 = build_messages(ex, 1, assistant_reply="I have inspected the image and extracted the visible details.")
            inputs1 = prepare_inputs(stack.proc_pre, messages1, stack.gpu_pre, add_generation_prompt=False)
            t0 = time.perf_counter()
            matched, delta = sl.borrow_prefix_refs(inputs1["input_ids"][0].tolist(), workflow_id=workflow_id)
            delta_ids = torch.tensor([delta], dtype=torch.long, device=stack.gpu_pre)
            delta_mm = inputs1.get("mm_token_type_ids")
            if delta_mm is not None:
                delta_mm = delta_mm[:, -len(delta):].to(stack.gpu_pre)
            pout1 = stack.prefill.prefill(
                input_ids=delta_ids,
                bundle=None,
                attention_mask=torch.ones_like(delta_ids),
                prefix_kv_refs=matched,
                prefix_rope_deltas=pout0.rope_deltas,
                mm_token_type_ids=delta_mm,
            )
            torch.cuda.synchronize(stack.gpu_pre)
            ttft_ms = (time.perf_counter() - t0) * 1000.0
            combined = sl._normalize_committed_refs(matched_pages=matched, committed_refs=pout1.kv_refs)
            decode_stats = _decode_from_prefill(stack, combined, pout1.first_logits, pout1.rope_deltas or pout0.rope_deltas, max_new_tokens=max_new_tokens)
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "ttft_ms": float(ttft_ms),
                    "total_ms": float(ttft_ms + decode_stats["decode_ms"]),
                    "decode_tps": decode_stats["decode_tps"],
                    "generated_tokens": decode_stats["generated_tokens"],
                    "matched_tokens": int(sum(ref.filled for ref in matched)),
                    "delta_tokens": int(len(delta)),
                    "prefix_hit": bool(matched),
                }
            )
            stack.pm_pre.release_refs(combined)
        finally:
            sl.release(state0)
            sl.radix.clear()
            sl.features.clear()
    report = summarize_ablation("B2", rows)
    report["prefix_hit_rate"] = sum(1 for row in rows if row.get("prefix_hit")) / len(rows) if rows else 0.0
    return report


def run_b6_cow_no_cross_step(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    branch_examples = [ex for ex in build_dataset() if ex.scenario == "tree_of_thought"]
    for ex in branch_examples:
        workflow_id = f"b6:{ex.scenario}:{ex.image_name}"
        sl, _, state0, _, pout0 = _prepare_reuse_state(stack, ex, workflow_id)
        try:
            t0 = time.perf_counter()
            branch = sl.fork(state0, child_agent_id="branch-agent")
            fork_ms = (time.perf_counter() - t0) * 1000.0
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "latency_ms": float(fork_ms),
                    "ttft_ms": float(pout0.prefill_time_ms),
                    "total_ms": float(pout0.prefill_time_ms),
                    "decode_tps": 0.0,
                    "shared_pages_before_cow": int(sum(1 for ref in branch.kv_refs if stack.pm_pre.refcount(ref.physical_id) > 1)),
                }
            )
            sl.release(branch)
        finally:
            sl.release(state0)
            sl.radix.clear()
            sl.features.clear()
    report = summarize_ablation("B6", rows, latency_key="latency_ms")
    report["shared_pages_avg"] = statistics.mean(row["shared_pages_before_cow"] for row in rows) if rows else 0.0
    return report


def run_b5_deepcopy_fork(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    branch_examples = [ex for ex in build_dataset() if ex.scenario == "tree_of_thought"]
    for ex in branch_examples:
        workflow_id = f"b5:{ex.scenario}:{ex.image_name}"
        sl, _, state0, _, _ = _prepare_reuse_state(stack, ex, workflow_id)
        try:
            t0 = time.perf_counter()
            copied_refs = []
            for ref in state0.kv_refs:
                key, value = stack.pm_pre.get_page(ref)
                new_ref = stack.pm_pre.allocate_page(filled=ref.filled)
                stack.pm_pre.write_page_slots(
                    new_ref,
                    key[:, :, : ref.filled, :].clone(),
                    value[:, :, : ref.filled, :].clone(),
                    offset=0,
                )
                copied_refs.append(new_ref)
            fork_ms = (time.perf_counter() - t0) * 1000.0
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "latency_ms": float(fork_ms),
                    "ttft_ms": 0.0,
                    "total_ms": float(fork_ms),
                    "decode_tps": 0.0,
                    "copied_pages": len(copied_refs),
                }
            )
            stack.pm_pre.release_refs(copied_refs)
        finally:
            sl.release(state0)
            sl.radix.clear()
            sl.features.clear()
    report = summarize_ablation("B5", rows, latency_key="latency_ms")
    report["copied_pages_avg"] = statistics.mean(row["copied_pages"] for row in rows) if rows else 0.0
    return report


def run_b7_exact_reuse(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for ex in examples:
        if len(ex.steps) < 2:
            continue
        workflow_id = f"b7:{ex.scenario}:{ex.image_name}"
        sl, _, state0, _, pout0 = _prepare_reuse_state(stack, ex, workflow_id)
        try:
            messages1 = build_messages(ex, 1, assistant_reply="I have inspected the image and extracted the visible details.")
            inputs1 = prepare_inputs(stack.proc_pre, messages1, stack.gpu_pre, add_generation_prompt=False)
            matched, delta = sl.borrow_prefix_refs(inputs1["input_ids"][0].tolist(), workflow_id=workflow_id)
            delta_ids = torch.tensor([delta], dtype=torch.long, device=stack.gpu_pre)
            delta_mm = inputs1.get("mm_token_type_ids")
            if delta_mm is not None:
                delta_mm = delta_mm[:, -len(delta):].to(stack.gpu_pre)
            t0 = time.perf_counter()
            pout1 = stack.prefill.prefill(
                input_ids=delta_ids,
                bundle=None,
                attention_mask=torch.ones_like(delta_ids),
                prefix_kv_refs=matched,
                prefix_rope_deltas=pout0.rope_deltas,
                mm_token_type_ids=delta_mm,
            )
            torch.cuda.synchronize(stack.gpu_pre)
            ttft_ms = (time.perf_counter() - t0) * 1000.0
            combined = sl._normalize_committed_refs(matched_pages=matched, committed_refs=pout1.kv_refs)
            decode_stats = _decode_from_prefill(stack, combined, pout1.first_logits, pout1.rope_deltas or pout0.rope_deltas, max_new_tokens=max_new_tokens)
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "ttft_ms": float(ttft_ms),
                    "total_ms": float(ttft_ms + decode_stats["decode_ms"]),
                    "decode_tps": decode_stats["decode_tps"],
                    "generated_tokens": decode_stats["generated_tokens"],
                    "reuse_ratio": float(sum(ref.filled for ref in matched) / max(1, len(inputs1["input_ids"][0]))),
                }
            )
            stack.pm_pre.release_refs(combined)
        finally:
            sl.release(state0)
            sl.radix.clear()
            sl.features.clear()
    report = summarize_ablation("B7", rows)
    report["exact_reuse_ratio"] = statistics.mean(row["reuse_ratio"] for row in rows) if rows else 0.0
    return report


def run_b8_approx_reuse(stack: RuntimeStack, examples: List[WorkflowExample], *, max_new_tokens: int) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for ex in examples:
        if len(ex.steps) < 2:
            continue
        workflow_id = f"b8:{ex.scenario}:{ex.image_name}"
        sl, _, state0, _, pout0 = _prepare_reuse_state(stack, ex, workflow_id)
        try:
            messages1 = build_messages(ex, 1, assistant_reply="I have inspected the image and extracted the visible details.")
            inputs1 = prepare_inputs(stack.proc_pre, messages1, stack.gpu_pre, add_generation_prompt=False)

            def delta_prefill(delta_tokens, prefix_kv_refs=None, **kwargs):
                prefix_kv_refs = list(prefix_kv_refs or [])
                delta_ids = torch.tensor([list(delta_tokens)], dtype=torch.long, device=stack.gpu_pre)
                pout = stack.prefill.prefill(
                    input_ids=delta_ids,
                    bundle=None,
                    attention_mask=torch.ones_like(delta_ids),
                    prefix_kv_refs=prefix_kv_refs if prefix_kv_refs else None,
                    prefix_rope_deltas=pout0.rope_deltas,
                    mm_token_type_ids=torch.zeros_like(delta_ids),
                )
                return pout.kv_refs, pout.first_logits

            reuse = ReusePipeline(sl, delta_prefill, relay_threshold=0.95, attention_threshold=0.90, enable_tier2=True, enable_tier3=True)
            prev_tokens = state0.meta.token_ids
            new_suffix = inputs1["input_ids"][0].tolist()[len(prev_tokens):]
            t0 = time.perf_counter()
            refs, logits, stats = reuse.run(
                new_tokens=new_suffix,
                prev_tokens=prev_tokens,
                prev_refs=state0.kv_refs,
                workflow_id=workflow_id,
            )
            torch.cuda.synchronize(stack.gpu_pre)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            # ``ReusePipeline.run`` already returns refs covering the full new
            # sequence (tier-1 matches + tier-2/3 fills). Re-normalizing against
            # ``state0.kv_refs`` can accidentally reintroduce the parent's original
            # refs instead of the borrowed refs, which breaks refcount ownership on
            # release. Decode directly from the pipeline-owned full ref set.
            combined = list(refs)
            decode_stats = _decode_from_prefill(
                stack,
                combined,
                logits if logits is not None else pout0.first_logits,
                pout0.rope_deltas,
                max_new_tokens=max_new_tokens,
            )
            rows.append(
                {
                    "scenario": ex.scenario,
                    "image": ex.image_name,
                    "ttft_ms": float(elapsed_ms),
                    "total_ms": float(elapsed_ms + decode_stats["decode_ms"]),
                    "decode_tps": decode_stats["decode_tps"],
                    "generated_tokens": decode_stats["generated_tokens"],
                    "reuse_ratio": float(stats.reuse_ratio),
                    "delta_reuse_ratio": float(stats.delta_reuse_ratio),
                    "full_prompt_tokens": int(stats.total_tokens),
                    "delta_tokens": int(stats.delta_tokens),
                    "tier1_matched_tokens": int(stats.tier1_matched_tokens),
                    "tier2_reused_tokens": int(stats.tier2_reused_tokens),
                    "tier2_recomputed_tokens": int(stats.tier2_recomputed_tokens),
                    "tier3_accepted_pages": int(stats.tier3_accepted_pages),
                    "tier3_candidate_pages": int(stats.tier3_candidate_pages),
                    "tier3_reused_tokens": int(stats.tier3_reused_tokens),
                    "tier3_mean_similarity": float(stats.tier3_mean_similarity),
                    "reuse_pipeline_ms": float(stats.elapsed_ms),
                    "tier1_ms": float(stats.tier1_ms),
                    "tier2_ms": float(stats.tier2_ms),
                    "tier3_ms": float(stats.tier3_ms),
                    "delta_prefill_calls": int(stats.delta_prefill_calls),
                    "delta_prefill_tokens": int(stats.delta_prefill_tokens),
                    "delta_prefill_ms": float(stats.delta_prefill_ms),
                    "pipeline_overhead_ms": float(max(0.0, stats.elapsed_ms - stats.delta_prefill_ms)),
                    "relay_segments": int(stats.relay_segments),
                    "relay_reusable_segments": int(stats.relay_reusable_segments),
                    "relay_recompute_segments": int(stats.relay_recompute_segments),
                    "relay_substring_hits": int(stats.relay_substring_hits),
                    "relay_substring_misses": int(stats.relay_substring_misses),
                }
            )
            stack.pm_pre.release_refs(combined)
        finally:
            sl.release(state0)
            sl.radix.clear()
            sl.features.clear()
    report = summarize_ablation("B8", rows)
    report["approx_reuse_ratio"] = statistics.mean(row["reuse_ratio"] for row in rows) if rows else 0.0
    return report


def summarize_ablation(
    baseline_id: str,
    rows: List[Dict[str, Any]],
    *,
    latency_key: str = "ttft_ms",
) -> Dict[str, Any]:
    latency_values = [float(row.get(latency_key, 0.0) or 0.0) for row in rows if row.get(latency_key) is not None]
    total_values = [float(row.get("total_ms", 0.0) or 0.0) for row in rows if row.get("total_ms") is not None]
    decode_values = [float(row.get("decode_tps", 0.0) or 0.0) for row in rows if row.get("decode_tps") is not None]
    return {
        "id": baseline_id,
        "available": bool(rows),
        "samples": len(rows),
        "latency_ms": statistics.mean(latency_values) if latency_values else None,
        "ttft_ms": statistics.mean([float(row.get("ttft_ms", 0.0) or 0.0) for row in rows]) if rows else None,
        "total_ms": statistics.mean(total_values) if total_values else None,
        "decode_tps": statistics.mean(decode_values) if decode_values else None,
        "rows": rows,
    }


def run_selected_ablations(
    baseline_ids: List[str],
    *,
    max_new_tokens: int,
    gpu_enc: int = DEFAULT_GPU_ENC,
    gpu_pre: int = DEFAULT_GPU_PRE,
    gpu_dec: int = DEFAULT_GPU_DEC,
    gpu_base: int = DEFAULT_GPU_BASE,
) -> Dict[str, Any]:
    examples = choose_examples()
    stack = build_runtime_stack(
        gpu_enc=gpu_enc,
        gpu_pre=gpu_pre,
        gpu_dec=gpu_dec,
        gpu_base=gpu_base,
    )
    try:
        runners = {
            "B1": run_b1_native_pd,
            "B2": run_b2_prefix_cache,
            "B3": run_b3_feature_cache,
            "B5": run_b5_deepcopy_fork,
            "B6": run_b6_cow_no_cross_step,
            "B7": run_b7_exact_reuse,
            "B8": run_b8_approx_reuse,
        }
        out: Dict[str, Any] = {}
        for baseline_id in baseline_ids:
            out[baseline_id] = runners[baseline_id](stack, examples, max_new_tokens=max_new_tokens)
        return {
            "model": MODEL_PATH,
            "runtime_devices": {
                "enc": gpu_enc,
                "pre": gpu_pre,
                "dec": gpu_dec,
                "base": gpu_base,
            },
            "dataset_summary": summarize(build_dataset()),
            "baselines": out,
        }
    finally:
        stack.close()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run real-model RFC ablation baselines.")
    ap.add_argument(
        "--baselines",
        nargs="+",
        default=["B1", "B2", "B3", "B5", "B6", "B7", "B8"],
    )
    ap.add_argument("--max-new-tokens", type=int, default=12)
    ap.add_argument("--gpu-enc", type=int, default=int(os.environ.get("MOONCAKE_EPD_GPU_ENC", DEFAULT_GPU_ENC)))
    ap.add_argument("--gpu-pre", type=int, default=int(os.environ.get("MOONCAKE_EPD_GPU_PRE", DEFAULT_GPU_PRE)))
    ap.add_argument("--gpu-dec", type=int, default=int(os.environ.get("MOONCAKE_EPD_GPU_DEC", DEFAULT_GPU_DEC)))
    ap.add_argument("--gpu-base", type=int, default=int(os.environ.get("MOONCAKE_EPD_GPU_BASE", DEFAULT_GPU_BASE)))
    ap.add_argument(
        "--output",
        default=str(REPO_ROOT / "mooncake_epd" / "artifacts" / "real_ablation_report.json"),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    report = run_selected_ablations(
        args.baselines,
        max_new_tokens=args.max_new_tokens,
        gpu_enc=args.gpu_enc,
        gpu_pre=args.gpu_pre,
        gpu_dec=args.gpu_dec,
        gpu_base=args.gpu_base,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {
        baseline_id: {
            "latency_ms": payload.get("latency_ms"),
            "ttft_ms": payload.get("ttft_ms"),
            "total_ms": payload.get("total_ms"),
            "decode_tps": payload.get("decode_tps"),
            "samples": payload.get("samples"),
        }
        for baseline_id, payload in report["baselines"].items()
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
