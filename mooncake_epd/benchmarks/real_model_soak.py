"""Real-model soak benchmark for EPD disaggregation on GPUs 3-6."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import List

import torch

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mooncake_epd.core.epd_workers import DecodeWorker, EncoderWorker, PrefillWorker
from mooncake_epd.core.state import PagedKVManager
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy
from mooncake_epd.benchmarks.workflow_trace import build_workflow_traces
from mooncake_epd.tests.dataset import WorkflowExample, build_dataset, make_image, summarize


MODEL_PATH = os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
GPU_ENC = torch.device("cuda:3")
GPU_PRE = torch.device("cuda:4")
GPU_DEC = torch.device("cuda:5")
GPU_BASE = torch.device("cuda:6")


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


def build_messages(ex: WorkflowExample):
    return [
        {"role": "system", "content": [{"type": "text", "text": ex.system_prompt}]},
        {
            "role": "user",
            "content": [
                {"type": "image", "image": make_image(ex.image_name)},
                {"type": "text", "text": ex.steps[0]},
            ],
        },
    ]


def prepare_inputs(proc, ex: WorkflowExample, device: torch.device):
    inputs = proc.apply_chat_template(
        build_messages(ex),
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )
    return {k: v.to(device) for k, v in inputs.items()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--max-new-tokens", type=int, default=12)
    ap.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent.parent / "artifacts" / "real_soak_report.json"),
    )
    args = ap.parse_args()

    base_dataset: List[WorkflowExample] = build_dataset()
    base_traces = build_workflow_traces(base_dataset)
    dataset: List[WorkflowExample] = base_dataset * args.rounds
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    model_enc, proc_enc = load_model(GPU_ENC)
    model_pre, proc_pre = load_model(GPU_PRE)
    model_dec, proc_dec = load_model(GPU_DEC)
    model_base, proc_base = load_model(GPU_BASE)

    engine = TransferEngine(protocol="local")
    pm_pre = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=GPU_PRE,
    )
    encoder = EncoderWorker(model_enc, proc_enc, GPU_ENC)
    prefill = PrefillWorker(model_pre, proc_pre, GPU_PRE, pm_pre)
    decode = DecodeWorker(model_dec, proc_dec, GPU_DEC, pm_pre)

    results = []
    baseline_ms = []
    epd_ttft = []
    epd_total = []
    epd_decode_tps = []
    failures = []

    for idx, ex in enumerate(dataset):
        trace = base_traces[idx % len(base_traces)]
        round_idx = idx // len(base_traces)
        item = {
            "index": idx,
            "round": round_idx,
            "workflow_id": f"{trace.workflow_id}:round{round_idx}",
            "source_dataset": trace.source_dataset,
            "task_type": trace.task_type,
            "priority_class": trace.priority_class,
            "scenario": ex.scenario,
            "image": ex.image_name,
        }
        try:
            inputs_base = prepare_inputs(proc_base, ex, GPU_BASE)
            torch.cuda.synchronize(GPU_BASE)
            t0 = time.perf_counter()
            out = model_base.generate(**inputs_base, max_new_tokens=args.max_new_tokens, do_sample=False)
            torch.cuda.synchronize(GPU_BASE)
            base_ms = (time.perf_counter() - t0) * 1000
            baseline_ms.append(base_ms)
            item["baseline_ms"] = base_ms
            item["baseline_tokens"] = int(out.shape[-1] - inputs_base["input_ids"].shape[-1])

            inputs_enc = prepare_inputs(proc_enc, ex, GPU_ENC)
            torch.cuda.synchronize(GPU_ENC)
            torch.cuda.synchronize(GPU_PRE)
            torch.cuda.synchronize(GPU_DEC)
            t0 = time.perf_counter()
            enc_out = encoder.encode(inputs_enc["pixel_values"], inputs_enc["image_grid_thw"])
            bundle_pre = engine.transfer_feature_bundle(
                enc_out.bundle,
                GPU_PRE,
                TransferPolicy(mode=Mode.STREAM, channel=Channel.ENCODER_TO_PREFILL),
            )
            pout = prefill.prefill(
                input_ids=inputs_enc["input_ids"].to(GPU_PRE),
                bundle=bundle_pre,
                attention_mask=inputs_enc.get("attention_mask").to(GPU_PRE),
                mm_token_type_ids=inputs_enc.get("mm_token_type_ids").to(GPU_PRE),
            )
            torch.cuda.synchronize(GPU_PRE)
            ttft_ms = (time.perf_counter() - t0) * 1000

            pm_dec, refs_dec = engine.transfer_pages(
                pm_pre,
                pout.kv_refs,
                GPU_DEC,
                TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
            )
            decode.pm = pm_dec
            dout = decode.decode(
                first_logits=engine.transfer_tensor(
                    pout.first_logits,
                    GPU_DEC,
                    TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
                ),
                kv_refs=refs_dec,
                max_new_tokens=args.max_new_tokens,
                rope_deltas=pout.rope_deltas,
            )
            torch.cuda.synchronize(GPU_DEC)
            total_ms = (time.perf_counter() - t0) * 1000

            epd_ttft.append(ttft_ms)
            epd_total.append(total_ms)
            epd_decode_tps.append(dout.tokens_per_second)
            item["epd_ttft_ms"] = ttft_ms
            item["epd_total_ms"] = total_ms
            item["epd_decode_tps"] = dout.tokens_per_second

            pm_dec.release_refs(refs_dec)
            pm_pre.release_refs(pout.kv_refs)
        except Exception as e:
            item["error"] = f"{type(e).__name__}: {e}"
            failures.append(item)
        results.append(item)

    report = {
        "model": MODEL_PATH,
        "dataset_summary": summarize(base_dataset),
        "rounds": args.rounds,
        "items": len(dataset),
        "results": results,
        "summary": {
            "baseline_avg_ms": statistics.mean(baseline_ms) if baseline_ms else None,
            "epd_ttft_avg_ms": statistics.mean(epd_ttft) if epd_ttft else None,
            "epd_total_avg_ms": statistics.mean(epd_total) if epd_total else None,
            "epd_decode_tps_avg": statistics.mean(epd_decode_tps) if epd_decode_tps else None,
            "failures": len(failures),
            "gpu_peak_gb": {
                "enc": torch.cuda.max_memory_allocated(GPU_ENC) / (1024 ** 3),
                "pre": torch.cuda.max_memory_allocated(GPU_PRE) / (1024 ** 3),
                "dec": torch.cuda.max_memory_allocated(GPU_DEC) / (1024 ** 3),
                "base": torch.cuda.max_memory_allocated(GPU_BASE) / (1024 ** 3),
            },
        },
    }
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))
    print(f"wrote {output_path}")


if __name__ == "__main__":
    main()
