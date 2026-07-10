"""Phase 2 tests: transfer engine + policy.

Runs on GPUs 3-6. Exercises:

- tensor transfer between GPU pairs with various TransferPolicy settings
- per-level compression of DeepStack feature bundles (error + speedup)
- page transfer with page manager re-materialization
- bandwidth measurement between GPU pairs (real CUDA memcpy)
- async transfer handles

Run: ``python -m mooncake_epd.tests.test_phase2_transfer``
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import (  # noqa: E402
    FeatureBundle,
    PagedKVManager,
)
from mooncake_epd.core.transfer import (  # noqa: E402
    Channel,
    CompressMode,
    HwCaps,
    Mode,
    Precision,
    TransferEngine,
    TransferPolicy,
    default_policy_for,
)


GPUS = [torch.device(f"cuda:{i}") for i in (3, 4, 5, 6)]


def _make_feature_bundle(device: torch.device, n_layers: int = 4) -> FeatureBundle:
    # Simulate DeepStack: 4 intermediate features at LLM layers [8, 16, 24, 35]
    layer_idxs = [8, 16, 24, 35]
    intermediates = [
        (idx, torch.randn(1, 196, 1152, device=device, dtype=torch.bfloat16))
        for idx in layer_idxs
    ]
    last_hidden = torch.randn(1, 196, 4096, device=device, dtype=torch.bfloat16)
    return FeatureBundle(
        image_hash="test_hash_abc",
        last_hidden=last_hidden,
        intermediates=intermediates,
        grid_thw=torch.tensor([[1, 14, 14]], device=device),
    )


def test_tensor_transfer():
    print("\n[1] Tensor transfer across GPUs with various policies")
    engine = TransferEngine(protocol="local", hw_caps=HwCaps.detect())
    t = torch.randn(4, 4096, device=GPUS[0], dtype=torch.bfloat16)

    # Warmup: first CUDA cross-device copy compiles kernels; exclude from timing.
    _ = engine.transfer_tensor(
        t, GPUS[1], TransferPolicy(Mode.STREAM, precision=Precision.BF16),
    )
    torch.cuda.synchronize(GPUS[1])

    cases = [
        ("stream/bf16", TransferPolicy(Mode.STREAM, precision=Precision.BF16)),
        ("stream/fp8",  TransferPolicy(Mode.STREAM, precision=Precision.FP8)),
        ("shm/bf16",    TransferPolicy(Mode.SHM,    precision=Precision.BF16)),
    ]
    for name, policy in cases:
        torch.cuda.synchronize(GPUS[0])
        torch.cuda.synchronize(GPUS[1])
        t0 = time.perf_counter()
        out = engine.transfer_tensor(t, GPUS[1], policy)
        torch.cuda.synchronize(GPUS[1])
        ms = (time.perf_counter() - t0) * 1000
        max_diff = (out.to(GPUS[0]) - t).abs().max().item()
        print(f"   {name:14s}: {ms:.3f} ms, max_diff={max_diff:.3e}, "
              f"dtype={out.dtype}, device={out.device}")
        assert out.device == GPUS[1]
        assert out.shape == t.shape
    print("   OK")


def test_per_level_compression():
    print("\n[2] Per-level compression on FeatureBundle")
    engine = TransferEngine(protocol="local")
    bundle = _make_feature_bundle(GPUS[0])
    orig_bytes = bundle.nbytes()

    policy_plain = TransferPolicy(Mode.SHM, CompressMode.NONE, Precision.BF16,
                                  channel=Channel.ENCODER_TO_PREFILL)
    policy_per = TransferPolicy(Mode.SHM, CompressMode.PER_LEVEL, Precision.BF16,
                                channel=Channel.ENCODER_TO_PREFILL)

    t0 = time.perf_counter()
    out_plain = engine.transfer_feature_bundle(bundle, GPUS[1], policy_plain)
    t_plain = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    out_per = engine.transfer_feature_bundle(bundle, GPUS[1], policy_per)
    t_per = (time.perf_counter() - t0) * 1000

    print(f"   orig bytes : {orig_bytes}")
    print(f"   plain time : {t_plain:.3f} ms")
    print(f"   per_level  : {t_per:.3f} ms")
    # Inspect intermediate dtypes. Layers 8 and 16 (rel < 0.6) should be
    # compressed to fp8 (or fp16 fallback); layers 24 and 35 keep bf16.
    fp8_dtype = getattr(torch, "float8_e4m3fn", None)
    fp8_ok = {torch.float16}
    if fp8_dtype is not None:
        fp8_ok.add(fp8_dtype)
    expected = {8: fp8_ok, 16: fp8_ok, 24: {torch.bfloat16}, 35: {torch.bfloat16}}
    for (i1, t1), (i2, t2) in zip(bundle.intermediates, out_per.intermediates):
        print(f"   layer {i1}: src dtype={t1.dtype}, dst dtype={t2.dtype}")
        assert t2.dtype in expected[i1], (
            f"layer {i1}: expected dtype in {expected[i1]}, got {t2.dtype}"
        )
    # last_hidden must be preserved (bf16 round-trip is exact)
    max_diff = (out_plain.last_hidden.to(GPUS[0]) - bundle.last_hidden).abs().max().item()
    print(f"   last_hidden max_diff (plain): {max_diff:.3e}")
    assert max_diff == 0.0
    print("   OK")


def test_page_transfer():
    print("\n[3] Page transfer GPU3 -> GPU5 with page manager round-trip")
    src_pm = PagedKVManager(
        page_size=16, num_layers=36, num_kv_heads=8, head_dim=128,
        dtype=torch.bfloat16, device=GPUS[0],
    )
    refs = src_pm.allocate_pages(3, filled=16)
    # Fill with random data
    for ref in refs:
        k, v = src_pm.get_page(ref)
        k.normal_(); v.normal_()
        src_pm.write_page_slots(ref, k, v, offset=0)
    orig_k = [src_pm.get_page(r)[0].clone() for r in refs]

    engine = TransferEngine(protocol="local")
    policy = TransferPolicy(Mode.STREAM, channel=Channel.PREFILL_TO_DECODE)
    t0 = time.perf_counter()
    target_pm, new_refs = engine.transfer_pages(src_pm, refs, GPUS[2], policy)
    ms = (time.perf_counter() - t0) * 1000
    total_bytes = sum(r.filled * 2 * 36 * 8 * 128 * 2 for r in refs)
    print(f"   {len(refs)} pages ({total_bytes/1e6:.1f} MB) in {ms:.2f} ms "
          f"({(total_bytes*8)/(ms/1000)/1e9:.2f} Gbps)")
    print(f"   src_pm.device={src_pm.device} -> target_pm.device={target_pm.device}")
    print(f"   src physical_ids={[r.physical_id for r in refs]}, "
          f"new physical_ids={[r.physical_id for r in new_refs]}")
    assert target_pm is not src_pm, "target should be a fresh manager"
    assert target_pm.device == GPUS[2]
    # Verify content matches by reading from target_mgr
    for (orig, new_ref) in zip(orig_k, new_refs):
        nk, _ = target_pm.get_page(new_ref)
        diff = (nk.to(GPUS[0]) - orig).abs().max().item()
        assert diff == 0.0, f"page content mismatch: {diff}"
    print("   OK")


def test_bandwidth():
    print("\n[4] Bandwidth between GPU pairs (real CUDA memcpy)")
    engine = TransferEngine(protocol="local")
    sizes_mb = [1, 4, 16]
    policy = TransferPolicy(Mode.STREAM, precision=Precision.BF16)
    for src_gpu, dst_gpu in [(GPUS[0], GPUS[1]), (GPUS[0], GPUS[2]), (GPUS[0], GPUS[3])]:
        print(f"   {src_gpu} -> {dst_gpu}:")
        for mb in sizes_mb:
            n = mb * 1024 * 1024 // 2  # bf16 = 2 bytes
            t = torch.randn(n, device=src_gpu, dtype=torch.bfloat16)
            torch.cuda.synchronize(src_gpu)
            torch.cuda.synchronize(dst_gpu)
            t0 = time.perf_counter()
            out = engine.transfer_tensor(t, dst_gpu, policy)
            torch.cuda.synchronize(dst_gpu)
            ms = (time.perf_counter() - t0) * 1000
            bw_gbps = (t.nelement() * t.element_size() * 8) / (ms / 1000) / 1e9
            print(f"      {mb:>3} MB: {ms:.2f} ms, {bw_gbps:.1f} Gbps")
            assert out.device == dst_gpu
    print("   OK")


def test_default_policy_selection():
    print("\n[5] Default policy selection")
    hw = HwCaps(has_rdma=False, has_cuda_ipc=True)
    for ch in list(Channel):
        p = default_policy_for(ch, hw, same_host=True)
        print(f"   {ch.value:10s} -> mode={p.mode.value:11s} "
              f"compress={p.compress.value:10s} precision={p.precision.value}")
    print("   OK")


def test_async_handle():
    print("\n[6] Async transfer handles")
    engine = TransferEngine(protocol="local")
    tensors = [torch.randn(1024, 1024, device=GPUS[0], dtype=torch.bfloat16) for _ in range(4)]
    handles = [
        engine.transfer_async([t], TransferPolicy(Mode.STREAM), GPUS[1])
        for t in tensors
    ]
    results = [h.result(timeout=5.0) for h in handles]
    print(f"   submitted {len(handles)} async transfers, all completed")
    assert len(results) == 4
    print("   OK")


def main():
    print("=" * 60)
    print("Phase 2: transfer layer tests")
    print("=" * 60)
    print(f"Detected HW: {HwCaps.detect()}")
    test_default_policy_selection()
    test_tensor_transfer()
    test_per_level_compression()
    test_page_transfer()
    test_bandwidth()
    test_async_handle()
    print("\nAll Phase 2 tests passed.")


if __name__ == "__main__":
    main()
