"""Phase 1: state layer exercised with real Qwen3-VL-8B KV tensors."""

from __future__ import annotations

import gc
import sys
import time
from pathlib import Path

import pytest
import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from mooncake_epd.core.state import (  # noqa: E402
    FeatureStore,
    PagedKVManager,
    RadixTree,
    StateLayer,
    StateMeta,
)
from mooncake_epd.tests.conftest import (  # noqa: E402
    MODEL_PATH,
    cleanup_torch,
    require_real_model_and_gpus,
)


pytestmark = [pytest.mark.real_model, pytest.mark.gpu, pytest.mark.gpu_single_node]

DEVICE = torch.device("cuda:3")


def load_model_and_processor():
    from transformers import AutoProcessor, Qwen3VLForConditionalGeneration

    proc = AutoProcessor.from_pretrained(MODEL_PATH)
    model = Qwen3VLForConditionalGeneration.from_pretrained(
        MODEL_PATH,
        dtype=torch.bfloat16,
        device_map={"": DEVICE},
        low_cpu_mem_usage=True,
    )
    model.eval()
    return model, proc


def run_prefill(model, proc, text: str, image=None):
    messages = [{"role": "user", "content": []}]
    if image is not None:
        messages[0]["content"].append({"type": "image", "image": image})
    messages[0]["content"].append({"type": "text", "text": text})
    inputs = proc.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )
    inputs = {k: v.to(DEVICE) for k, v in inputs.items()}
    with torch.no_grad():
        out = model(**inputs, use_cache=True)
    from mooncake_epd.core.state.page_manager import _normalize_kv_cache

    pkv = tuple(_normalize_kv_cache(out.past_key_values))
    return inputs["input_ids"], pkv


def _test_image(w: int, h: int):
    from PIL import Image

    arr = torch.linspace(0, 1, w * h * 3).reshape(h, w, 3)
    arr = (arr * 255).to(torch.uint8).numpy()
    return Image.fromarray(arr)


@pytest.fixture(scope="module")
def model_and_proc():
    require_real_model_and_gpus(3)
    model, proc = load_model_and_processor()
    try:
        yield model, proc
    finally:
        cleanup_torch(model, proc, devices=[3])


@pytest.fixture(scope="module")
def model(model_and_proc):
    return model_and_proc[0]


@pytest.fixture(scope="module")
def proc(model_and_proc):
    return model_and_proc[1]


@pytest.fixture(scope="module")
def past_key_values(model, proc):
    _, pkv = run_prefill(
        model,
        proc,
        "Write a detailed essay about the history of artificial intelligence, "
        "covering its origins in the 1950s, the AI winters, expert systems, "
        "the rise of deep learning, and modern large language models.",
    )
    return pkv


def test_page_manager_roundtrip(past_key_values):
    pm = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=DEVICE,
    )
    t0 = time.perf_counter()
    refs = pm.ingest_kv_cache(past_key_values)
    ingest_ms = (time.perf_counter() - t0) * 1000
    seq_len = sum(r.filled for r in refs)

    t0 = time.perf_counter()
    pkv = pm.materialize_kv_cache(refs)
    mat_ms = (time.perf_counter() - t0) * 1000
    orig_seq = past_key_values[0][0].shape[-2]
    rebuilt_seq = pkv[0][0].shape[-2]
    max_diff = 0.0
    for (ok, ov), (nk, nv) in zip(past_key_values, pkv):
        max_diff = max(
            max_diff,
            (ok.squeeze(0)[:, :rebuilt_seq] - nk[:, :rebuilt_seq]).abs().max().item(),
            (ov.squeeze(0)[:, :rebuilt_seq] - nv[:, :rebuilt_seq]).abs().max().item(),
        )

    print(
        f"roundtrip: pages={len(refs)} seq={seq_len} ingest={ingest_ms:.1f}ms "
        f"materialize={mat_ms:.1f}ms max_diff={max_diff:.3e}"
    )
    assert rebuilt_seq == orig_seq
    assert max_diff == 0.0


def test_page_manager_fork_cow(past_key_values):
    pm = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=DEVICE,
    )
    refs = pm.ingest_kv_cache(past_key_values)
    child_refs = pm.fork_refs(refs)
    for r in refs:
        assert pm.refcount(r.physical_id) == 2

    last = child_refs[-1]
    new_ref = pm.cow_page(last)
    assert pm.refcount(last.physical_id) == 1
    assert pm.refcount(new_ref.physical_id) == 1

    k, v = pm.get_page(new_ref)
    pm.write_page_slots(new_ref, k[:, :, :1, :].clone(), v[:, :, :1, :].clone(), offset=0)

    child_refs[-1] = new_ref
    pm.release_refs(child_refs)
    for r in refs:
        assert pm.refcount(r.physical_id) == 1
    pm.release_refs(refs)
    assert pm.stats()["total_pages"] == 0


def test_radix_tree():
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=8,
        dtype=torch.bfloat16,
        device=DEVICE,
    )
    tree = RadixTree(pm, max_entries=100)
    seq_a = [1, 2, 3, 4, 5, 6, 7, 8]
    refs_a = pm.allocate_pages(2, filled=4)
    tree.insert(seq_a[:4], [refs_a[0]])
    tree.insert(seq_a[:8], [refs_a[1]])

    matched, unmatched = tree.match_longest_prefix([1, 2, 3, 4, 5, 6, 9, 10])
    assert sum(r.filled for r in matched) == 4
    assert unmatched == [5, 6, 9, 10]

    matched, unmatched = tree.match_longest_prefix([1, 2, 3, 4, 5, 6, 7, 8])
    assert sum(r.filled for r in matched) == 8
    assert unmatched == []

    matched, unmatched = tree.match_longest_prefix([99, 100])
    assert not matched and unmatched == [99, 100]


def test_feature_store():
    from mooncake_epd.core.state.feature_store import FeatureBundle

    fs = FeatureStore(max_bytes=1024 * 1024, max_entries=2)
    b1 = FeatureBundle("h1", torch.randn(10, 128))
    b2 = FeatureBundle("h2", torch.randn(10, 128))
    b3 = FeatureBundle("h3", torch.randn(10, 128))
    fs.put("h1", b1)
    fs.put("h2", b2)
    assert fs.get("h1") is not None
    fs.put("h3", b3)
    assert fs.get("h2") is None
    assert fs.get("h1") is not None
    assert fs.get("h3") is not None


def test_state_layer_with_real_model(model, proc):
    pm = PagedKVManager(
        page_size=16,
        num_layers=36,
        num_kv_heads=8,
        head_dim=128,
        dtype=torch.bfloat16,
        device=DEVICE,
    )
    tree = RadixTree(pm, max_entries=1024)
    fs = FeatureStore()
    sl = StateLayer(pm, tree, fs, default_ttl_seconds=60.0)

    ids_a, pkv_a = run_prefill(
        model,
        proc,
        "Describe this image in detail.",
        image=_test_image(224, 224),
    )
    refs_a = pm.ingest_kv_cache(pkv_a)
    del pkv_a
    gc.collect()
    torch.cuda.empty_cache()

    meta_a = StateMeta(
        token_ids=ids_a[0].tolist(),
        agent_id="agent-A",
        workflow_id="wf1",
        step=1,
    )
    state_a = sl.register(refs_a, feature_hash=None, meta=meta_a)
    state_b = sl.fork(state_a, child_agent_id="agent-B")
    assert state_b.num_kv_tokens == state_a.num_kv_tokens

    new_tokens = [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007]
    matched, delta, _ = sl.advance_step_prepare(state_a, new_tokens, new_image_hashes=[])
    n_new_pages = (len(delta) + pm.page_size - 1) // pm.page_size
    new_refs = matched + pm.allocate_pages(n_new_pages, filled=pm.page_size)

    import dataclasses

    last_fill = len(delta) % pm.page_size or pm.page_size
    new_refs[-1] = dataclasses.replace(new_refs[-1], filled=last_fill)
    new_state = sl.advance_step_commit(
        state_a,
        new_tokens,
        new_image_hashes=[],
        new_kv_refs=new_refs,
        matched_pages=matched,
    )

    sl.release(state_a)
    assert state_b.num_kv_tokens > 0
    assert new_state.num_kv_tokens > 0
    sl.release(state_b)
    sl.release(new_state)
