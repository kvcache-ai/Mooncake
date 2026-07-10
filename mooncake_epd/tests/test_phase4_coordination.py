"""Phase 4 tests: Agent coordination (fork, workflow, scheduler, offload).

Uses synthetic KV tensors (no model load) for fast iteration. Tests:

- Workflow: multi-step chain with prefix reuse
- AgentScheduler: type-aware routing
- OffloadManager: offload to CPU, restore to GPU
- Cross-step reuse rate (the RFC's core innovation metric)

Run: ``python -m mooncake_epd.tests.test_phase4_coordination``
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import (  # noqa: E402
    FeatureStore,
    PagedKVManager,
    RadixTree,
    StateLayer,
    StateMeta,
)
from mooncake_epd.agent.coordination import (  # noqa: E402
    AgentRequest,
    AgentScheduler,
    AgentType,
    OffloadManager,
    WorkerLoad,
    Workflow,
)


DEVICE = torch.device("cuda:3")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_state_layer(device: torch.device = DEVICE):
    pm = PagedKVManager(
        page_size=8, num_layers=4, num_kv_heads=2, head_dim=8,
        dtype=torch.bfloat16, device=device,
    )
    tree = RadixTree(pm, max_entries=256)
    fs = FeatureStore(max_bytes=16 * 1024 * 1024)
    sl = StateLayer(pm, tree, fs, default_ttl_seconds=60.0)
    return sl


def _build_kv_refs(sl: StateLayer, token_ids, page_size=8):
    """Create a KV cache that matches ``token_ids`` in length.

    Allocates pages of ``page_size`` tokens and writes random data into
    them. The page manager's num_layers/num_kv_heads/head_dim come from
    ``sl.pm``.
    """
    pm = sl.pm
    n = len(token_ids)
    refs = []
    for start in range(0, n, page_size):
        end = min(start + page_size, n)
        fill = end - start
        ref = pm.allocate_page(filled=fill)
        k = torch.randn(
            pm.num_layers, pm.num_kv_heads, fill, pm.head_dim,
            dtype=pm.dtype, device=pm.device,
        )
        v = torch.randn_like(k)
        pm.write_page_slots(ref, k, v, offset=0)
        refs.append(ref)
    return refs


def _fake_prefill_fn_factory(sl: StateLayer):
    """Return a prefill_fn that allocates fresh KV pages for the delta and
    concatenates them with the prefix refs."""
    def prefill_fn(
        delta_tokens, prefix_kv_refs=None,
        pixel_values=None, image_grid_thw=None,
    ):
        new_refs = _build_kv_refs(sl, delta_tokens)
        full_refs = list(prefix_kv_refs or []) + new_refs
        # Fake first logits
        first_logits = torch.randn(1, 1, 100, device=sl.pm.device, dtype=sl.pm.dtype)
        return full_refs, first_logits
    return prefill_fn


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_workflow_cross_step_reuse():
    print("\n[1] Workflow: multi-step chain with prefix reuse")
    sl = _make_state_layer()
    wf = Workflow(
        agent_id="agent-A",
        state_layer=sl,
        prefill_fn=_fake_prefill_fn_factory(sl),
    )

    # Step 0: a long context (system prompt + history + shared image)
    shared_prefix = list(range(1000, 1032))   # 32 tokens, 4 pages of 8
    refs = _build_kv_refs(sl, shared_prefix)
    meta = StateMeta(
        token_ids=shared_prefix, agent_id="agent-A",
        workflow_id=wf.workflow_id, step=0,
    )
    state0 = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    from mooncake_epd.agent.coordination.workflow import WorkflowStep
    wf.steps.append(WorkflowStep(
        step_index=0, state=state0, added_tokens=shared_prefix,
    ))
    print(f"   step 0: {len(shared_prefix)} tokens, {len(refs)} pages")

    # Step 1: append 8 new tokens. Full prefix matches -> reuse_ratio ~ 0.8
    new_tokens_1 = list(range(2000, 2008))
    step1 = wf.advance(new_tokens_1)
    print(f"   step 1: +{len(new_tokens_1)} tokens, reuse_ratio={step1.reuse_ratio:.2f}, "
          f"prefill {step1.prefill_time_ms:.3f} ms")
    assert step1.reuse_ratio >= 0.75, f"expected high reuse, got {step1.reuse_ratio}"

    # Step 2: append 8 more tokens. Full chain prefix still matches.
    new_tokens_2 = list(range(3000, 3008))
    step2 = wf.advance(new_tokens_2)
    print(f"   step 2: +{len(new_tokens_2)} tokens, reuse_ratio={step2.reuse_ratio:.2f}")
    assert step2.reuse_ratio >= 0.80

    stats = wf.reuse_stats()
    print(f"   workflow stats: {stats}")
    assert stats["avg_reuse_ratio"] > 0.7

    wf.release()
    # The RadixTree intentionally retains page references for cross-workflow
    # prefix reuse (RFC §6.5.2). Clearing it at workflow end frees the pages.
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0, "no leaked pages"
    print("   OK")


def test_workflow_divergence():
    print("\n[2] Workflow: divergent step (tool return) still runs but lower reuse")
    sl = _make_state_layer()
    wf = Workflow(agent_id="agent-B", state_layer=sl,
                  prefill_fn=_fake_prefill_fn_factory(sl))
    base_tokens = list(range(100, 116))   # 2 pages
    refs = _build_kv_refs(sl, base_tokens)
    meta = StateMeta(token_ids=base_tokens, workflow_id=wf.workflow_id)
    state0 = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    from mooncake_epd.agent.coordination.workflow import WorkflowStep
    wf.steps.append(WorkflowStep(0, state0, added_tokens=base_tokens))

    # Truly divergent step: first 8 tokens match the base, but tokens 8..15
    # differ, so the prefix match stops after the first page.
    divergent_tokens = base_tokens[:8] + list(range(500, 508))
    step1 = wf.advance(divergent_tokens)
    print(f"   divergent step: +{len(divergent_tokens)} tokens, "
          f"reuse_ratio={step1.reuse_ratio:.2f}")
    # The new tokens diverge from the base at position 8. The prefix match
    # will reuse the 16 base tokens (they're still in the tree under a
    # different path? No -- the state has 16 tokens; full = 16+16=32 but
    # divergent tokens replace positions 8-15 of the base. The RadixTree
    # only stores the base's 16-token path, so matching full[0:32] against
    # base[0:16] will match all 16 base tokens, then fail at divergent[0:16].
    # reuse_ratio = 16/32 = 0.5. We just verify it's less than the
    # same-prefix case (>= 0.75) and that the advance didn't crash.
    assert step1.reuse_ratio < 0.8
    assert step1.reuse_ratio >= 0.0
    wf.release()
    sl.radix.clear()
    print("   OK")


def test_scheduler_routing():
    print("\n[3] AgentScheduler: type-aware routing")
    sched = AgentScheduler()
    sched.add_worker(WorkerLoad("p1", "prefill", current_load=10, max_capacity=50,
                                 avg_latency_ms=200.0))
    sched.add_worker(WorkerLoad("p2", "prefill", current_load=40, max_capacity=50,
                                 avg_latency_ms=400.0))
    sched.add_worker(WorkerLoad("d1", "decode", current_load=5, max_capacity=80,
                                 avg_latency_ms=50.0))
    sched.add_worker(WorkerLoad("d2", "decode", current_load=60, max_capacity=80,
                                 avg_latency_ms=200.0))

    # THINKING -> prefill worker with more remaining capacity
    r_think = AgentRequest("r1", AgentType.THINKING, input_tokens=1000)
    w = sched.route(r_think)
    print(f"   THINKING -> {w.worker_id}")
    assert w.worker_id == "p1"

    # INTERACTIVE -> decode worker with lower latency
    r_inter = AgentRequest("r2", AgentType.INTERACTIVE, input_tokens=50)
    w = sched.route(r_inter)
    print(f"   INTERACTIVE -> {w.worker_id}")
    assert w.worker_id == "d1"

    # HYBRID -> any worker, balanced scoring
    r_hyb = AgentRequest("r3", AgentType.HYBRID, input_tokens=200)
    w = sched.route(r_hyb)
    print(f"   HYBRID -> {w.worker_id}")

    # Batch routing: priority-sorted but order-preserving
    reqs = [
        AgentRequest("a", AgentType.INTERACTIVE, 50, priority=1),
        AgentRequest("b", AgentType.THINKING, 1000, priority=10),
        AgentRequest("c", AgentType.INTERACTIVE, 30, priority=5),
    ]
    out = sched.batch_route(reqs)
    print(f"   batch assignments: {[w.worker_id if w else None for w in out]}")
    print(f"   scheduler stats: {sched.stats()}")
    print("   OK")


def test_offload_restore():
    print("\n[4] OffloadManager: offload blocked state, then restore")
    sl = _make_state_layer()
    om = OffloadManager(sl, DEVICE)

    # Create a state with some KV
    tokens = list(range(42, 58))   # 2 pages
    refs = _build_kv_refs(sl, tokens)
    # Snapshot original content
    orig_k = [sl.pm.get_page(r)[0].clone() for r in refs]
    meta = StateMeta(token_ids=tokens, agent_id="agent-X", workflow_id="wf-X")
    state = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    assert sl.pm.stats()["total_pages"] == 2

    # Offload: KV moves to CPU, GPU pages freed
    handle = om.offload(state, expected_return_seconds=5.0)
    assert state.kv_refs == [], "GPU refs should be cleared"
    print(f"   offloaded, freed: {om.stats()['bytes_freed']} bytes")

    # Restore
    restored = om.restore(handle)
    assert restored is not None
    assert len(restored.kv_refs) == 2
    # Content must match original
    for orig, new_ref in zip(orig_k, restored.kv_refs):
        nk, _ = sl.pm.get_page(new_ref)
        diff = (nk - orig).abs().max().item()
        assert diff == 0.0, f"content mismatch after restore: {diff}"

    sl.release(restored)
    # RadixTree retains refs for future reuse; clear to reclaim pages.
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0
    print(f"   offload stats: {om.stats()}")
    print("   OK")


def test_handoff():
    print("\n[5] Handoff: state reference migration between agents")
    sl = _make_state_layer()
    tokens = list(range(70, 86))
    refs = _build_kv_refs(sl, tokens)
    meta = StateMeta(token_ids=tokens, agent_id="agent-A")
    state = sl.register(kv_refs=refs, feature_hash=None, meta=meta)
    assert state.meta.agent_id == "agent-A"
    sl.handoff(state, to_agent_id="agent-B")
    assert state.meta.agent_id == "agent-B"
    sl.release(state)
    print("   OK")


def main():
    print("=" * 60)
    print("Phase 4: agent coordination")
    print("=" * 60)
    test_workflow_cross_step_reuse()
    test_workflow_divergence()
    test_scheduler_routing()
    test_offload_restore()
    test_handoff()
    print("\nAll Phase 4 tests passed.")


if __name__ == "__main__":
    main()
