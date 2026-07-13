"""Phase 3: EPD workers with real Qwen3-VL-8B + GPU 3-6 separation."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
import torch
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import DecodeWorker, EncoderWorker, PrefillWorker  # noqa: E402
from mooncake_epd.core.state import FeatureHandleRegistry, MMStore  # noqa: E402
from mooncake_epd.core.state import PagedKVManager  # noqa: E402
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


pytestmark = [pytest.mark.real_model, pytest.mark.gpu, pytest.mark.slow]

GPU_ENC_ID = get_test_gpu_id("ENC", 3)
GPU_PRE_ID = get_test_gpu_id("PRE", 4)
GPU_DEC_ID = get_test_gpu_id("DEC", 5)
GPU_SINGLE_ID = get_test_gpu_id("BASE", 6)
GPU_ENC = get_test_gpu_device("ENC", 3)
GPU_PRE = get_test_gpu_device("PRE", 4)
GPU_DEC = get_test_gpu_device("DEC", 5)
GPU_SINGLE = get_test_gpu_device("BASE", 6)


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


def _test_image(w: int = 224, h: int = 224) -> Image.Image:
    arr = torch.linspace(0, 1, w * h * 3).reshape(h, w, 3)
    arr = (arr * 255).to(torch.uint8).numpy()
    return Image.fromarray(arr)


def prepare_inputs(proc, text: str, image: Image.Image, device: torch.device):
    messages = [{
        "role": "user",
        "content": [
            {"type": "image", "image": image},
            {"type": "text", "text": text},
        ],
    }]
    inputs = proc.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )
    return {k: v.to(device) for k, v in inputs.items()}


@pytest.fixture(scope="module")
def model_proc_enc():
    require_real_model_and_gpus(GPU_ENC_ID, GPU_PRE_ID, GPU_DEC_ID, GPU_SINGLE_ID)
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
def model_proc_single():
    model, proc = load_model(GPU_SINGLE)
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[GPU_SINGLE_ID])


@pytest.fixture(scope="module")
def enc_out_and_inputs(model_proc_enc):
    model, proc = model_proc_enc
    enc = EncoderWorker(model, proc, GPU_ENC)
    inputs = prepare_inputs(proc, "Describe this image.", _test_image(), GPU_ENC)
    out = enc.encode(inputs["pixel_values"], inputs["image_grid_thw"])
    return out, inputs


@pytest.fixture(scope="module")
def prefill_out_and_pm(enc_out_and_inputs, model_proc_pre):
    enc_out, inputs = enc_out_and_inputs
    model_pre, proc_pre = model_proc_pre
    engine = TransferEngine(protocol="local")
    mm_store = MMStore(transfer_engine=engine)
    feature_registry = FeatureHandleRegistry(mm_store, store_id="phase3-mm-store")
    feature_handle = feature_registry.publish_bundle(enc_out.bundle, checksum=False)
    inputs_pre = {k: v.to(GPU_PRE) for k, v in inputs.items() if k in {"input_ids", "attention_mask"}}
    pm = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=GPU_PRE,
    )
    pre = PrefillWorker(model_pre, proc_pre, GPU_PRE, pm)
    mm_token_type_ids = inputs.get("mm_token_type_ids")
    try:
        pout = pre.prefill_from_feature_handle(
            input_ids=inputs_pre["input_ids"],
            feature_handle=feature_handle,
            feature_registry=feature_registry,
            target_worker_id="prefill-gpu4",
            attention_mask=inputs_pre.get("attention_mask"),
            mm_token_type_ids=(mm_token_type_ids.to(GPU_PRE) if mm_token_type_ids is not None else None),
            timeout=30.0,
            policy=TransferPolicy(
                mode=Mode.STREAM,
                channel=Channel.ENCODER_TO_PREFILL,
            ),
        )
        bundle_pre = mm_store.lookup("prefill-gpu4", feature_handle.feature_id)
        assert bundle_pre is not None
    finally:
        mm_store.stop()
    return enc_out, bundle_pre, inputs_pre, pout, pm


def test_encoder_worker(enc_out_and_inputs):
    out, _ = enc_out_and_inputs
    print(
        f"encoder: {out.encode_time_ms:.1f}ms pooled={tuple(out.bundle.last_hidden.shape)} "
        f"deepstack={len(out.bundle.intermediates)}"
    )
    assert out.bundle.last_hidden.shape[0] > 0
    assert out.bundle.metadata["num_images"] >= 1
    descriptor = out.bundle.descriptor(checksum=False)
    descriptor.validate_bundle(out.bundle)
    assert descriptor.feature_id == out.bundle.image_hash
    assert descriptor.model_fingerprint
    for idx, feat in out.bundle.intermediates:
        assert isinstance(idx, int)
        assert feat.shape[0] > 0


def test_prefill_worker(prefill_out_and_pm, model_proc_pre):
    _, _, _, pout, _ = prefill_out_and_pm
    model_pre, _ = model_proc_pre
    print(
        f"prefill: {pout.prefill_time_ms:.1f}ms pages={len(pout.kv_refs)} "
        f"seq={sum(r.filled for r in pout.kv_refs)} logits={tuple(pout.first_logits.shape)}"
    )
    assert len(pout.kv_refs) > 0
    assert pout.first_logits.shape[-1] == model_pre.config.text_config.vocab_size


def test_decode_worker(prefill_out_and_pm, model_proc_dec):
    _, _, _, pout, pm = prefill_out_and_pm
    model_dec, proc_dec = model_proc_dec
    engine = TransferEngine(protocol="local")
    target_pm, target_refs = engine.transfer_pages(
        pm,
        pout.kv_refs,
        GPU_DEC,
        TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
    )
    dec = DecodeWorker(model_dec, proc_dec, GPU_DEC, target_pm)
    dout = dec.decode(
        first_logits=pout.first_logits.to(GPU_DEC),
        kv_refs=target_refs,
        max_new_tokens=24,
        rope_deltas=pout.rope_deltas,
    )
    print(
        f"decode: {dout.decode_time_ms:.1f}ms generated={len(dout.generated_ids)} "
        f"tps={dout.tokens_per_second:.1f}"
    )
    assert len(dout.generated_ids) > 1
    assert dout.tokens_per_second > 0


def test_text_only_prefill(model_proc_pre):
    model_pre, proc_pre = model_proc_pre
    pm = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=GPU_PRE,
    )
    pre = PrefillWorker(model_pre, proc_pre, GPU_PRE, pm)
    messages = [{"role": "user", "content": [{"type": "text", "text": "Hello"}]}]
    inputs = proc_pre.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )
    inputs = {k: v.to(GPU_PRE) for k, v in inputs.items()}
    pout = pre.prefill(
        input_ids=inputs["input_ids"],
        bundle=None,
        attention_mask=inputs.get("attention_mask"),
        mm_token_type_ids=inputs.get("mm_token_type_ids"),
    )
    assert sum(r.filled for r in pout.kv_refs) > 0


def test_single_node_baseline(model_proc_single):
    model_single, proc_single = model_proc_single
    inputs = prepare_inputs(proc_single, "Describe this image briefly.", _test_image(), GPU_SINGLE)
    torch.cuda.synchronize(GPU_SINGLE)
    t0 = time.perf_counter()
    out = model_single.generate(**inputs, max_new_tokens=24, do_sample=False)
    torch.cuda.synchronize(GPU_SINGLE)
    ms = (time.perf_counter() - t0) * 1000
    prompt_len = inputs["input_ids"].shape[-1]
    gen_len = out.shape[-1] - prompt_len
    print(f"single-node: total={ms:.1f}ms gen={gen_len} tok")
    assert gen_len > 0
