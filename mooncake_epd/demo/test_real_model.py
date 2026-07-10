"""
Qwen3-VL-8B 真实模型 EPD 分离测试 (v2 - 修正版)

在 GPU 6 和 GPU 7 上进行：
1. Vision Encoder 单独测试
2. EPD 三阶段分离测试 (E→P→D)
3. Agent State Cloning 测试（真实 KV Cache）
4. Hidden State Prefix Caching 测试
5. 跨 GPU 传输带宽测试
6. 单机 vs EPD 分离对比
"""

import os
import sys
import time
import json
import gc
import logging
from typing import Dict, Any
from dataclasses import dataclass, field

import pytest
import torch
import numpy as np
from PIL import Image

pytestmark = pytest.mark.skipif(
    os.environ.get("MOONCAKE_RUN_DEMO_REAL_MODEL_TESTS") != "1",
    reason="demo benchmark script; enable with MOONCAKE_RUN_DEMO_REAL_MODEL_TESTS=1",
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

MODEL_PATH = os.getenv("MOONCAKE_EPD_MODEL", "models/Qwen3-VL-8B-Instruct")
# With CUDA_VISIBLE_DEVICES=6,7: cuda:0=GPU6, cuda:1=GPU7
ENCODER_GPU = 0
PREFILL_GPU = 0
DECODE_GPU = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class TestMetrics:
    __test__ = False
    test_name: str
    metrics: Dict[str, Any] = field(default_factory=dict)


def load_model():
    from transformers import Qwen3VLForConditionalGeneration, AutoProcessor
    logger.info(f"Loading Qwen3-VL-8B from {MODEL_PATH}...")
    t0 = time.perf_counter()
    processor = AutoProcessor.from_pretrained(MODEL_PATH, trust_remote_code=True)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        MODEL_PATH, dtype=torch.bfloat16,
        device_map=f"cuda:{ENCODER_GPU}", trust_remote_code=True,
    )
    model.eval()
    logger.info(f"Loaded in {time.perf_counter()-t0:.1f}s")
    return model, processor


def make_inputs(model, processor, image, text, device):
    msgs = [{"role": "user", "content": [{"type": "image", "image": image}, {"type": "text", "text": text}]}]
    prompt = processor.apply_chat_template(msgs, add_generation_prompt=True)
    inputs = processor(text=[prompt], images=[image], padding=True, return_tensors="pt")
    return {k: v.to(device) for k, v in inputs.items()}


def make_test_image(w=448, h=448):
    img = Image.new("RGB", (w, h))
    px = img.load()
    for x in range(w):
        for y in range(h):
            px[x, y] = (int(255*x/w), int(255*y/h), int(255*(x+y)/(w+h)))
    return img


# ============================================================
# Test 1: Vision Encoder
# ============================================================
def test_vision_encoder(model, processor):
    logger.info("=" * 60)
    logger.info("Test 1: Vision Encoder")
    logger.info("=" * 60)
    device = torch.device(f"cuda:{ENCODER_GPU}")
    m = TestMetrics("vision_encoder")

    for res in [224, 336, 448]:
        img = make_test_image(res, res)
        inp = make_inputs(model, processor, img, "Describe.", device)
        pv = inp["pixel_values"]
        gt = inp["image_grid_thw"]

        with torch.no_grad():
            _ = model.model.visual(pv, grid_thw=gt)
        torch.cuda.synchronize()

        times = []
        for _ in range(5):
            torch.cuda.synchronize()
            t0 = time.perf_counter()
            with torch.no_grad():
                vis_out = model.model.visual(pv, grid_thw=gt)
            torch.cuda.synchronize()
            times.append((time.perf_counter() - t0) * 1000)

        avg = np.mean(times)
        hs = vis_out.last_hidden_state
        ds = vis_out.deepstack_features if hasattr(vis_out, "deepstack_features") else None
        logger.info(f"  {res}x{res}: {avg:.2f}ms, hidden={list(hs.shape)}"
                     + (f", deepstack={[list(d.shape) for d in ds]}" if ds else ""))
        m.metrics[f"encode_{res}x{res}_ms"] = round(avg, 2)
        m.metrics[f"hidden_shape_{res}"] = str(list(hs.shape))

    return m


# ============================================================
# Test 2: EPD 三阶段分离
# ============================================================
def test_epd_disaggregation(model, processor):
    logger.info("=" * 60)
    logger.info("Test 2: EPD 三阶段分离")
    logger.info("=" * 60)
    device_p = torch.device(f"cuda:{PREFILL_GPU}")
    device_d = torch.device(f"cuda:{DECODE_GPU}")
    m = TestMetrics("epd_disaggregation")

    img = make_test_image()
    inp = make_inputs(model, processor, img, "Describe this image in detail.", device_p)

    # === E: Vision Encoder ===
    pv, gt = inp["pixel_values"], inp["image_grid_thw"]
    torch.cuda.synchronize()
    t0 = time.perf_counter()
    with torch.no_grad():
        vis_out = model.model.visual(pv, grid_thw=gt)
    torch.cuda.synchronize()
    enc_ms = (time.perf_counter() - t0) * 1000
    m.metrics["encode_ms"] = round(enc_ms, 2)
    logger.info(f"  [E] encode={enc_ms:.2f}ms, features={list(vis_out.last_hidden_state.shape)}")

    # === P: Prefill ===
    torch.cuda.synchronize()
    t0 = time.perf_counter()
    with torch.no_grad():
        prefill_out = model(**inp, use_cache=True)
    torch.cuda.synchronize()
    prefill_ms = (time.perf_counter() - t0) * 1000
    m.metrics["prefill_ms"] = round(prefill_ms, 2)
    m.metrics["input_tokens"] = inp["input_ids"].shape[1]
    logger.info(f"  [P] prefill={prefill_ms:.2f}ms, tokens={inp['input_ids'].shape[1]}")

    # KV Cache info
    pkv = prefill_out.past_key_values
    n_layers = len(pkv)
    kv_shape = list(pkv.layers[0].keys.shape)
    kv_bytes = sum(l.keys.nelement() * l.keys.element_size() + l.values.nelement() * l.values.element_size() for l in pkv.layers)
    m.metrics["kv_layers"] = n_layers
    m.metrics["kv_shape_per_layer"] = str(kv_shape)
    m.metrics["kv_size_mb"] = round(kv_bytes / 1024 / 1024, 2)
    logger.info(f"  [KV] layers={n_layers}, shape={kv_shape}, size={kv_bytes/1024/1024:.2f}MB")

    # === Transfer P→D (GPU0→GPU1) - measure time, then transfer back for decode ===
    torch.cuda.synchronize()
    t0 = time.perf_counter()
    for layer in pkv.layers:
        layer.keys = layer.keys.to(device_d)
        layer.values = layer.values.to(device_d)
    torch.cuda.synchronize()
    xfer_ms = (time.perf_counter() - t0) * 1000
    m.metrics["transfer_p2d_ms"] = round(xfer_ms, 2)
    bandwidth = (kv_bytes * 8) / (xfer_ms / 1000) / 1e9
    m.metrics["transfer_bandwidth_gbps"] = round(bandwidth, 2)
    logger.info(f"  [P→D GPU{PREFILL_GPU}→GPU{DECODE_GPU}] transfer={xfer_ms:.2f}ms, bw={bandwidth:.2f}Gbps")

    # Transfer KV back to prefill GPU for decode (model is only on GPU 0)
    # In real EPD, decode node would have its own model instance
    for layer in pkv.layers:
        layer.keys = layer.keys.to(device_p)
        layer.values = layer.values.to(device_p)
    torch.cuda.synchronize()

    # === D: Decode ===
    logits = prefill_out.logits[:, -1, :]
    next_tok = torch.argmax(logits, dim=-1, keepdim=True)

    max_new = 16
    gen_toks = []
    dec_times = []

    # For decode, need model on decode GPU or use .generate with past_key_values
    # Since model is on GPU 0, we transfer KV back for decode steps
    # In real EPD, a separate model instance would run on decode GPU
    torch.cuda.synchronize()
    t_dec = time.perf_counter()
    cur_tok = next_tok
    cur_pkv = prefill_out.past_key_values

    for step in range(max_new):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        with torch.no_grad():
            step_out = model(input_ids=cur_tok, past_key_values=cur_pkv, use_cache=True)
        torch.cuda.synchronize()
        dec_times.append((time.perf_counter() - t0) * 1000)
        cur_tok = torch.argmax(step_out.logits[:, -1, :], dim=-1, keepdim=True)
        gen_toks.append(cur_tok.item())
        cur_pkv = step_out.past_key_values
        if cur_tok.item() == 151645:
            break

    torch.cuda.synchronize()
    dec_total = (time.perf_counter() - t_dec) * 1000

    n_gen = len(gen_toks)
    avg_step = np.mean(dec_times)
    tps = n_gen / (dec_total / 1000) if dec_total > 0 else 0
    m.metrics["decode_total_ms"] = round(dec_total, 2)
    m.metrics["decode_steps"] = n_gen
    m.metrics["avg_decode_step_ms"] = round(avg_step, 2)
    m.metrics["tokens_per_second"] = round(tps, 2)
    m.metrics["ttft_ms"] = round(enc_ms + prefill_ms + xfer_ms + (dec_times[0] if dec_times else 0), 2)
    logger.info(f"  [D] tokens={n_gen}, total={dec_total:.2f}ms, step={avg_step:.2f}ms, tps={tps:.1f}")
    logger.info(f"  [TTFT] {m.metrics['ttft_ms']:.2f}ms")

    try:
        txt = processor.tokenizer.decode(gen_toks, skip_special_tokens=True)
        logger.info(f"  [Text] {txt[:200]}")
        m.metrics["generated_text"] = txt[:200]
    except Exception:
        pass

    return m


# ============================================================
# Test 3: 单机 generate 对比
# ============================================================
def test_single_node(model, processor):
    logger.info("=" * 60)
    logger.info("Test 3: 单机 generate 对比")
    logger.info("=" * 60)
    device = torch.device(f"cuda:{ENCODER_GPU}")
    m = TestMetrics("single_node")
    img = make_test_image()
    inp = make_inputs(model, processor, img, "What is shown in this image?", device)

    # warmup
    with torch.no_grad():
        model.generate(**inp, max_new_tokens=4, do_sample=False)
    torch.cuda.synchronize()

    # benchmark
    times = []
    for i in range(3):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        with torch.no_grad():
            out = model.generate(**inp, max_new_tokens=32, do_sample=False)
        torch.cuda.synchronize()
        times.append((time.perf_counter() - t0) * 1000)

    avg = np.mean(times)
    n_tok = out.shape[1] - inp["input_ids"].shape[1]
    tps = n_tok / (avg / 1000)
    m.metrics["avg_total_ms"] = round(avg, 2)
    m.metrics["tokens_generated"] = n_tok
    m.metrics["tokens_per_second"] = round(tps, 2)
    m.metrics["ttft_approx_ms"] = round(avg / max(n_tok, 1), 2)
    logger.info(f"  time={avg:.2f}ms, tokens={n_tok}, tps={tps:.1f}")

    try:
        txt = processor.tokenizer.decode(out[0], skip_special_tokens=True)
        logger.info(f"  [Text] {txt[:300]}")
    except Exception:
        pass

    return m


# ============================================================
# Test 4: Agent State Cloning
# ============================================================
def test_agent_cloning(model, processor):
    logger.info("=" * 60)
    logger.info("Test 4: Agent State Cloning")
    logger.info("=" * 60)
    device = torch.device(f"cuda:{ENCODER_GPU}")
    m = TestMetrics("agent_cloning")

    img = make_test_image()
    inp = make_inputs(model, processor, img, "Analyze this image step by step.", device)
    with torch.no_grad():
        out = model(**inp, use_cache=True)
    pkv = out.past_key_values

    kv_bytes = sum(l.keys.nelement() * l.keys.element_size() + l.values.nelement() * l.values.element_size() for l in pkv.layers)
    m.metrics["kv_cache_size_mb"] = round(kv_bytes / 1024 / 1024, 2)
    logger.info(f"  KV Cache: {kv_bytes/1024/1024:.2f}MB, {len(pkv)} layers")

    # Build stacked tensors for cloning
    keys = torch.stack([l.keys for l in pkv.layers])
    values = torch.stack([l.values for l in pkv.layers])
    kv_tuple = (keys, values)

    from mooncake_epd.agent.state_clone import AgentStateCloner
    cloner = AgentStateCloner()
    cloner.register_kv_cache("real_base", kv_tuple)

    for nb in [2, 4, 8]:
        clone_times = []
        for _ in range(5):
            t0 = time.perf_counter()
            branches = cloner.fork_branches("real_base", nb)
            clone_times.append((time.perf_counter() - t0) * 1000)
            for b in branches:
                cloner.release_branch(b.branch_id)
        avg = np.mean(clone_times)
        m.metrics[f"clone_{nb}_total_ms"] = round(avg, 4)
        m.metrics[f"clone_{nb}_per_branch_ms"] = round(avg / nb, 4)
        logger.info(f"  {nb} branches: {avg:.4f}ms total, {avg/nb:.4f}ms/branch")

    return m


# ============================================================
# Test 5: Prefix Caching
# ============================================================
def test_prefix_caching(model, processor):
    logger.info("=" * 60)
    logger.info("Test 5: Hidden State Prefix Caching")
    logger.info("=" * 60)
    device = torch.device(f"cuda:{ENCODER_GPU}")
    m = TestMetrics("prefix_caching")

    from mooncake_epd.agent.prefix_cache import HiddenStatePrefixCache
    cache = HiddenStatePrefixCache(max_cache_size_bytes=8 * 1024**3)

    images = [make_test_image(448, 448) for _ in range(5)]
    enc_times = []
    put_times = []

    for i, img in enumerate(images):
        inp = make_inputs(model, processor, img, "Test", device)
        pv, gt = inp["pixel_values"], inp["image_grid_thw"]

        torch.cuda.synchronize()
        t0 = time.perf_counter()
        with torch.no_grad():
            vis = model.model.visual(pv, grid_thw=gt)
        torch.cuda.synchronize()
        enc_times.append((time.perf_counter() - t0) * 1000)

        hidden = vis.last_hidden_state
        t0 = time.perf_counter()
        cache.put(pv.cpu(), hidden.cpu(), {"idx": i})
        put_times.append((time.perf_counter() - t0) * 1000)

    avg_enc = np.mean(enc_times)
    m.metrics["avg_encode_ms"] = round(avg_enc, 2)
    m.metrics["avg_cache_put_ms"] = round(np.mean(put_times), 2)

    # Lookups
    lookup_times = []
    hits = 0
    for img in images:
        inp = make_inputs(model, processor, img, "Test", device)
        pv = inp["pixel_values"]
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        res = cache.get(pv.cpu())
        torch.cuda.synchronize()
        lookup_times.append((time.perf_counter() - t0) * 1000)
        if res is not None:
            hits += 1

    avg_lk = np.mean(lookup_times)
    m.metrics["hit_rate"] = hits / len(images)
    m.metrics["avg_lookup_ms"] = round(avg_lk, 4)
    speedup = avg_enc / max(avg_lk, 0.001)
    m.metrics["speedup_vs_encode"] = round(speedup, 1)
    m.metrics["cache_size_mb"] = round(cache.get_stats()["cache_size_mb"], 2)
    logger.info(f"  encode={avg_enc:.2f}ms, lookup={avg_lk:.4f}ms, hit={hits}/{len(images)}, speedup={speedup:.1f}x")

    return m


# ============================================================
# Test 6: 跨 GPU 传输带宽
# ============================================================
def test_transfer_bandwidth(model, processor):
    logger.info("=" * 60)
    logger.info("Test 6: 跨 GPU 传输带宽")
    logger.info("=" * 60)
    device_d = torch.device(f"cuda:{DECODE_GPU}")
    m = TestMetrics("transfer_bandwidth")

    img = make_test_image()
    inp = make_inputs(model, processor, img, "Test.", torch.device(f"cuda:{ENCODER_GPU}"))
    with torch.no_grad():
        out = model(**inp, use_cache=True)
    pkv = out.past_key_values

    kv_bytes = sum(l.keys.nelement() * l.keys.element_size() + l.values.nelement() * l.values.element_size() for l in pkv.layers)
    logger.info(f"  KV size: {kv_bytes/1024/1024:.2f}MB")

    # warmup
    for _ in range(3):
        for layer in pkv.layers:
            _ = layer.keys.to(device_d)
    torch.cuda.synchronize()

    times = []
    for _ in range(10):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        for layer in pkv.layers:
            _ = layer.keys.to(device_d)
            _ = layer.values.to(device_d)
        torch.cuda.synchronize()
        times.append((time.perf_counter() - t0) * 1000)

    avg = np.mean(times)
    bw = (kv_bytes * 8) / (avg / 1000) / 1e9
    m.metrics["kv_size_mb"] = round(kv_bytes / 1024 / 1024, 2)
    m.metrics["avg_transfer_ms"] = round(avg, 2)
    m.metrics["p50_ms"] = round(np.percentile(times, 50), 2)
    m.metrics["bandwidth_gbps"] = round(bw, 2)
    logger.info(f"  transfer={avg:.2f}ms, bandwidth={bw:.2f}Gbps")

    # Also test hidden state transfer
    with torch.no_grad():
        vis = model.model.visual(inp["pixel_values"], grid_thw=inp["image_grid_thw"])
    hs = vis.last_hidden_state
    hs_bytes = hs.nelement() * hs.element_size()

    for _ in range(3):
        _ = hs.to(device_d)
    torch.cuda.synchronize()

    hs_times = []
    for _ in range(10):
        torch.cuda.synchronize()
        t0 = time.perf_counter()
        _ = hs.to(device_d)
        torch.cuda.synchronize()
        hs_times.append((time.perf_counter() - t0) * 1000)

    hs_avg = np.mean(hs_times)
    hs_bw = (hs_bytes * 8) / (hs_avg / 1000) / 1e9
    m.metrics["hidden_state_size_mb"] = round(hs_bytes / 1024 / 1024, 2)
    m.metrics["hidden_state_transfer_ms"] = round(hs_avg, 2)
    m.metrics["hidden_state_bandwidth_gbps"] = round(hs_bw, 2)
    logger.info(f"  hidden_state: {hs_avg:.2f}ms, {hs_bytes/1024/1024:.2f}MB, {hs_bw:.2f}Gbps")

    return m


def main():
    logger.info("#" * 60)
    logger.info("# Qwen3-VL-8B Real Model EPD Test")
    logger.info("#" * 60)

    model, processor = load_model()
    all_results = {}

    tests = [
        ("vision_encoder", test_vision_encoder),
        ("transfer_bandwidth", test_transfer_bandwidth),
        ("epd_disaggregation", test_epd_disaggregation),
        ("single_node", test_single_node),
        ("agent_cloning", test_agent_cloning),
        ("prefix_caching", test_prefix_caching),
    ]

    for name, fn in tests:
        try:
            m = fn(model, processor)
            all_results[m.test_name] = m.metrics
        except Exception as e:
            logger.error(f"{name} FAILED: {e}")
            import traceback; traceback.print_exc()
            all_results[name] = {"error": str(e)}
        gc.collect()
        torch.cuda.empty_cache()

    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(json.dumps(all_results, indent=2, ensure_ascii=False, default=str))

    out_path = os.path.join(os.path.dirname(__file__), "..", "benchmarks", "qwen3vl_real_results.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({
            "model": "Qwen3-VL-8B-Instruct",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "gpu_encoder": f"GPU6 (cuda:{ENCODER_GPU})",
            "gpu_decode": f"GPU7 (cuda:{DECODE_GPU})",
            "results": all_results,
        }, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
