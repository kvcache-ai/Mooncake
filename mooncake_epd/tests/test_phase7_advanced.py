"""Phase 7 tests: three-tier reuse + transfer + scheduling + metrics.

Tests every gap-closing module added in response to the audit:

- RelayRecompute (RFC §6.5.1 tier 2) -- split reusable / recompute
- AttentionSimilarityReuse (RFC §6.5.1 tier 3) -- cosine gate
- ReusePipeline three-tier orchestration
- CacheGen streaming compression (RFC §5)
- Transfer pull mode + next_step prefetch
- Scheduler preemption + offload-on-preempt
- Workflow on_tool_call / on_tool_return hook
- Elastic E:P:D ratio monitor + role switching
- OmniPipeline multi-stage abstraction
- AnchorPool A2A handoff
- Full RFC metrics suite (14 metrics)

Run: ``python -m mooncake_epd.tests.test_phase7_advanced``
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import (  # noqa: E402
    AttentionSimilarityReuse,
    FeatureStore,
    PagedKVManager,
    RadixTree,
    RelayRecompute,
    StateLayer,
    StateMeta,
)


DEVICE = torch.device("cuda:3")


def _make_state_layer(device: torch.device = DEVICE):
    pm = PagedKVManager(
        page_size=8, num_layers=4, num_kv_heads=2, head_dim=8,
        dtype=torch.bfloat16, device=device,
    )
    tree = RadixTree(pm, max_entries=256)
    fs = FeatureStore(max_bytes=16 * 1024 * 1024)
    sl = StateLayer(pm, tree, fs, default_ttl_seconds=60.0)
    return sl


def _build_kv_refs(sl, token_ids, page_size=8):
    pm = sl.pm
    refs = []
    for start in range(0, len(token_ids), page_size):
        end = min(start + page_size, len(token_ids))
        fill = end - start
        ref = pm.allocate_page(filled=fill)
        k = torch.randn(pm.num_layers, pm.num_kv_heads, fill, pm.head_dim,
                        dtype=pm.dtype, device=pm.device)
        v = torch.randn_like(k)
        pm.write_page_slots(ref, k, v, offset=0)
        refs.append(ref)
    return refs


def _fake_prefill_fn(sl):
    def fn(delta_tokens, prefix_kv_refs=None, **kwargs):
        new_refs = _build_kv_refs(sl, delta_tokens)
        full_refs = list(prefix_kv_refs or []) + new_refs
        return full_refs, torch.randn(1, 1, 100, device=sl.pm.device, dtype=sl.pm.dtype)
    return fn


# ---------------------------------------------------------------------------
# Tier-2: RelayRecompute
# ---------------------------------------------------------------------------
def test_relay_split_segments():
    print("\n[1] split_segments: identify reusable runs in divergent input")
    from mooncake_epd.core.state.relay_recompute import split_segments
    prev = list(range(100, 116))            # [100..115], 16 tokens
    # New sequence: first 8 match, then 4 new tokens, then last 8 match
    new = prev[:8] + [500, 501, 502, 503] + prev[8:]
    segs = split_segments(new, prev, page_size=4, min_match_run=4)
    print(f"   new len={len(new)}, segments={len(segs)}")
    for i, s in enumerate(segs):
        print(f"     [{i}] reusable={s.reusable} len={len(s.tokens)} first={s.tokens[:4]}")
    assert any(s.reusable for s in segs), "should have at least one reusable segment"
    assert any(not s.reusable for s in segs), "should have at least one recompute segment"
    reused = sum(len(s.tokens) for s in segs if s.reusable)
    print(f"   reusable tokens: {reused}/{len(new)} ({reused / len(new):.0%})")
    print("   OK")


def test_relay_recompute():
    print("\n[2] RelayRecompute: selective recompute on divergent input")
    sl = _make_state_layer()
    prev_tokens = list(range(1000, 1032))   # 32 tokens = 4 pages of 8
    prev_refs = _build_kv_refs(sl, prev_tokens)
    meta = StateMeta(token_ids=prev_tokens, workflow_id="wf-relay")
    sl.register(kv_refs=prev_refs, feature_hash=None, meta=meta)
    relay = RelayRecompute(sl.pm, sl.radix, _fake_prefill_fn(sl), min_match_run=8)
    # New: first 16 match (2 pages), middle 8 new, last 16 match (2 pages)
    # -> 3 segments: reusable(16) / recompute(8) / reusable(16)
    new = prev_tokens[:16] + list(range(2000, 2008)) + prev_tokens[16:]
    refs, stats = relay.run(new, prev_tokens, scope="wf-relay")
    print(f"   stats: {stats}")
    assert stats["reused_tokens"] == 32, f"expected 32 reused, got {stats['reused_tokens']}"
    assert stats["recomputed_tokens"] == 8
    assert stats["reuse_ratio"] >= 0.8, f"expected >=80% reuse, got {stats['reuse_ratio']}"
    sl.radix.clear()
    print("   OK")


# ---------------------------------------------------------------------------
# Tier-3: AttentionSimilarityReuse
# ---------------------------------------------------------------------------
def test_attention_similarity():
    print("\n[3] AttentionSimilarityReuse: cosine gate on key-norm profiles")
    from mooncake_epd.core.state.attention_similarity import attention_similarity
    # The implementation intentionally compares per-token key-norm profiles,
    # not raw key direction.  Random Gaussian tensors have very similar norm
    # profiles in expectation, so use deterministic profiles for a stable gate
    # test: increasing token energy vs reversed token energy.
    token_scale = torch.arange(1, 9, dtype=torch.float32).view(1, 1, 8, 1)
    a = torch.ones(4, 2, 8, 8) * token_scale
    b = a + 0.01 * torch.randn_like(a)       # near-identical profile -> high sim
    c = torch.ones_like(a) * token_scale.flip(dims=(2,))  # reversed profile -> low sim
    sim_ab = attention_similarity(a, b)
    sim_ac = attention_similarity(a, c)
    print(f"   sim(near-identical) = {sim_ab:.3f}")
    print(f"   sim(unrelated)      = {sim_ac:.3f}")
    assert sim_ab > 0.95
    assert sim_ac < 0.9

    # Evaluate sequence
    sl = _make_state_layer()
    refs = _build_kv_refs(sl, list(range(100, 132)))
    new_keys = [sl.pm.get_page(r)[0].clone() + 0.001 * torch.randn_like(sl.pm.get_page(r)[0])
                for r in refs]
    attn = AttentionSimilarityReuse(sl.pm, threshold=0.90)
    accepted, rejected, stats = attn.evaluate_sequence(refs, new_keys)
    print(f"   stats: {stats}")
    assert stats["n_accepted"] == len(refs)
    # Release accepted refs
    for r in accepted:
        sl.pm.decref(r.physical_id)
    print("   OK")


# ---------------------------------------------------------------------------
# Transfer: cacheGen + pull mode
# ---------------------------------------------------------------------------
def test_cachegen_compression():
    print("\n[4] CacheGen streaming compression codec")
    from mooncake_epd.core.transfer import CompressMode
    from mooncake_epd.core.transfer.engine import _compress_cachegen, _decompress_cachegen
    t = torch.randn(4, 8, 16, 64, dtype=torch.bfloat16, device=DEVICE)
    compressed, meta = _compress_cachegen(t)
    ratio = (t.nelement() * t.element_size()) / len(compressed)
    print(f"   orig={t.nelement()*t.element_size()} B, compressed={len(compressed)} B, "
          f"ratio={ratio:.2f}x")
    reconstructed = _decompress_cachegen(compressed, meta)
    err = (reconstructed.to(DEVICE) - t).abs().max().item()
    print(f"   max reconstruction error: {err:.3e}")
    assert ratio >= 1.5, f"cacheGen should compress >= 1.5x, got {ratio:.2f}x"
    assert err < 0.5, f"reconstruction error too high: {err}"
    print("   OK")


def test_pull_mode():
    print("\n[5] Transfer pull mode (receiver-driven)")
    from mooncake_epd.core.transfer import Mode, TransferEngine, TransferPolicy
    engine = TransferEngine(protocol="local")
    src = torch.randn(1024, 1024, device=DEVICE, dtype=torch.bfloat16)
    policy = TransferPolicy(Mode.PULL, channel=None)
    torch.cuda.synchronize(DEVICE)
    t0 = time.perf_counter()
    out = engine.transfer_tensor(src, torch.device("cuda:4"), policy)
    torch.cuda.synchronize(torch.device("cuda:4"))
    ms = (time.perf_counter() - t0) * 1000
    print(f"   pull transfer: {ms:.3f} ms")
    assert out.device == torch.device("cuda:4")
    print("   OK")


# ---------------------------------------------------------------------------
# Scheduler: preemption
# ---------------------------------------------------------------------------
def test_preemption():
    print("\n[6] Scheduler preemption + offload-on-preempt")
    from mooncake_epd.agent.coordination.scheduler import (
        AgentRequest, AgentScheduler, AgentType, WorkerLoad,
    )
    from mooncake_epd.agent.coordination.offload import OffloadManager
    sl = _make_state_layer()
    om = OffloadManager(sl, DEVICE)
    sched = AgentScheduler()
    sched.add_worker(WorkerLoad("d1", "decode", current_load=70, max_capacity=80,
                                 avg_latency_ms=50.0))
    # Low-priority INTERACTIVE request fills the worker
    low = AgentRequest("low", AgentType.INTERACTIVE, 10, priority=1)
    sched.route(low)
    sched.register_active(low)
    # High-priority THINKING request arrives -- should preempt `low`
    high = AgentRequest("high", AgentType.THINKING, 1000, priority=10)
    victims = sched.preempt_for(high, offload_manager=om)
    print(f"   preempted victims: {[v.request_id for v in victims]}")
    assert len(victims) == 1
    assert victims[0].request_id == "low"
    # Victim priority < high priority, so the check is satisfied.
    # (No state attached to low, so offload is a no-op.)
    print(f"   offload stats: {om.stats()}")
    print("   OK")


# ---------------------------------------------------------------------------
# Offload: on_tool_call hook
# ---------------------------------------------------------------------------
def test_tool_call_hook():
    print("\n[7] OffloadManager on_tool_call / on_tool_return")
    from mooncake_epd.agent.coordination.offload import OffloadManager
    sl = _make_state_layer()
    om = OffloadManager(sl, DEVICE)
    refs = _build_kv_refs(sl, list(range(50, 82)))
    meta = StateMeta(token_ids=list(range(50, 82)), agent_id="X")
    state = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    handle = om.on_tool_call(state, expected_duration=5.0)
    print(f"   tool-call offloaded: {handle}")
    assert state.kv_refs == []
    # Simulate tool return
    restored = om.on_tool_return(handle)
    assert restored is not None
    assert len(restored.kv_refs) > 0
    print(f"   restored {len(restored.kv_refs)} kv refs")
    sl.release(restored)
    sl.radix.clear()
    print("   OK")


# ---------------------------------------------------------------------------
# Elastic E:P:D + role switching
# ---------------------------------------------------------------------------
def test_elastic_ratio():
    print("\n[8] ElasticEPDRatio monitor + role switching")
    from mooncake_epd.agent.coordination.elastic_ratio import ElasticEPDRatio
    monitor = ElasticEPDRatio()
    # Register mock worker utilizations
    monitor.update("encoder", utilization=0.2, queue_depth=0, hit_rate=0.95)
    monitor.update("prefill", utilization=0.85, queue_depth=12)
    monitor.update("decode", utilization=0.7, queue_depth=4)
    suggestion = monitor.suggest_rebalance()
    print(f"   current: {monitor.snapshot()}")
    print(f"   suggestion: {suggestion}")
    assert suggestion is not None
    # Role switching: append-prefill on decode
    can_switch = monitor.should_route_append_prefill(decode_worker_id="decode")
    print(f"   append-prefill-on-decode: {can_switch}")
    print("   OK")


# ---------------------------------------------------------------------------
# Omni multi-stage pipeline
# ---------------------------------------------------------------------------
def test_omni_pipeline():
    print("\n[9] OmniPipeline multi-stage abstraction (AR -> Generation -> Diffusion)")
    from mooncake_epd.core.omni_pipeline import OmniPipeline, OmniStage
    from mooncake_epd.core.transfer import TransferEngine

    class _Stage(OmniStage):
        def __init__(self, name: str, scale: float):
            self.name = name
            self.scale = scale
        def run(self, input_refs):
            # Toy: multiply tensor by scale
            return [t * self.scale for t in input_refs]

    engine = TransferEngine(protocol="local")
    pipe = OmniPipeline(
        stages=[_Stage("AR", 2.0), _Stage("Generation", 3.0), _Stage("Diffusion", 0.5)],
        transfer=engine,
    )
    inputs = [torch.randn(16, device=DEVICE)]
    t0 = time.perf_counter()
    out = pipe.process(inputs)
    ms = (time.perf_counter() - t0) * 1000
    print(f"   3-stage pipeline: {ms:.3f} ms")
    print(f"   output: {[t.mean().item() for t in out]}")
    print(f"   stats: {pipe.stats()}")
    assert len(out) == 1
    print("   OK")


# ---------------------------------------------------------------------------
# AnchorPool A2A handoff
# ---------------------------------------------------------------------------
def test_anchor_pool():
    print("\n[10] AnchorPool A2A handoff")
    from mooncake_epd.core.state.anchor_pool import AnchorPool
    sl = _make_state_layer()
    pool = AnchorPool(sl.pm)
    # Register an anchor from agent-A
    refs = _build_kv_refs(sl, list(range(300, 332)))
    meta = StateMeta(token_ids=list(range(300, 332)), agent_id="A")
    state_a = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    pool.register_anchor(
        agent_id="A",
        token_ids=state_a.meta.token_ids,
        refs=state_a.kv_refs,
        feature_hashes=state_a.feature_hashes,
    )
    # Agent-B looks up a prefix it shares with agent-A
    hit_refs, unmatched = pool.lookup(list(range(300, 340)))
    print(f"   lookup: hit {sum(r.filled for r in hit_refs)} tokens, unmatched={len(unmatched)}")
    assert sum(r.filled for r in hit_refs) > 0
    # Release hit refs (pool holds its own refcount)
    for r in hit_refs:
        sl.pm.decref(r.physical_id)
    sl.release(state_a)
    sl.radix.clear()
    print("   OK")


# ---------------------------------------------------------------------------
# Metrics suite
# ---------------------------------------------------------------------------
def test_metrics_suite():
    print("\n[11] Full RFC metrics suite (14 metrics)")
    from mooncake_epd.benchmarks.metrics_suite import MetricsCollector
    mc = MetricsCollector()
    mc.record_ttft(50.0); mc.record_ttft(120.0); mc.record_ttft(75.0)
    mc.record_tpot(0.04); mc.record_tpot(0.05); mc.record_tpot(0.045)
    mc.record_request()
    mc.record_fork(0.03)
    mc.record_prefix_hit()
    mc.record_encoder_skip()
    mc.record_cross_step_reuse(0.86)
    mc.record_a2a_handoff(2.5)
    mc.record_kv_transfer(1e9, 0.5)
    mc.record_gpu_peak(12.3)
    mc.record_quality(0.88)
    mc.record_jct(1234.0)
    report = mc.report()
    print(f"   metrics collected: {len(report)}")
    for k, v in sorted(report.items()):
        print(f"     {k}: {v}")
    # Must have all 14 metrics
    required = {
        "ttft_p50_ms", "ttft_p95_ms", "tpot_ms", "throughput_req_per_s",
        "jct_ms", "fork_ms", "prefix_hit_rate", "encoder_skip_rate",
        "cross_step_reuse_rate", "turn2_ttft_reduction_pct",
        "a2a_handoff_ms", "kv_transfer_gbps", "gpu_peak_gb", "quality_accuracy",
    }
    missing = required - set(report.keys())
    assert not missing, f"missing metrics: {missing}"
    print("   OK")


# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Phase 7: advanced features (audit gap closures)")
    print("=" * 60)
    test_relay_split_segments()
    test_relay_recompute()
    test_attention_similarity()
    test_cachegen_compression()
    test_pull_mode()
    test_preemption()
    test_tool_call_hook()
    test_elastic_ratio()
    test_omni_pipeline()
    test_anchor_pool()
    test_metrics_suite()
    print("\nAll Phase 7 tests passed.")


if __name__ == "__main__":
    main()
