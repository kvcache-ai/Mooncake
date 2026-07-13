from __future__ import annotations

import pytest
import torch

from mooncake_epd.core.control.serving_controller import ServingControlPlane
from mooncake_epd.core.state import FeatureBundle, FeatureStore, PagedKVManager, RadixTree, StateLayer, StateMeta
from mooncake_epd.core.state.feature_store import hash_pixel_values
from mooncake_epd.core.transfer import CompressMode, Mode, Precision, TransferEngine, TransferPolicy


def _state_layer() -> StateLayer:
    pm = PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=2,
        head_dim=4,
        dtype=torch.float32,
        device=torch.device("cpu"),
    )
    return StateLayer(pm, RadixTree(pm, max_entries=64), FeatureStore(max_bytes=8 * 1024 * 1024))


def _refs(sl: StateLayer, token_count: int):
    refs = []
    for _ in range((token_count + sl.pm.page_size - 1) // sl.pm.page_size):
        filled = min(sl.pm.page_size, token_count - len(refs) * sl.pm.page_size)
        ref = sl.pm.allocate_page(filled=filled)
        k = torch.randn(sl.pm.num_layers, sl.pm.num_kv_heads, filled, sl.pm.head_dim)
        v = torch.randn_like(k)
        sl.pm.write_page_slots(ref, k, v, offset=0)
        refs.append(ref)
    return refs


def test_hash_pixel_values_covers_unsampled_bytes():
    a = torch.zeros(8192, dtype=torch.float32)
    b = a.clone()
    b[123] = 1.0
    assert hash_pixel_values(a, sample_budget=16) != hash_pixel_values(b, sample_budget=16)


def test_advance_step_passes_cached_new_image_feature_and_blocks_kv_reuse():
    sl = _state_layer()
    base = sl.register(
        _refs(sl, 4),
        feature_hash="img-a",
        meta=StateMeta(token_ids=[1, 2, 3, 4], image_ids=["img-a"], workflow_id="wf-mm", agent_id="a"),
    )
    bundle = FeatureBundle("img-b", torch.ones(1, 2))
    sl.features.put("img-b", bundle)

    observed = {}

    def prefill(delta_tokens, prefix_kv_refs=None, delta_features=None):
        observed["delta_tokens"] = list(delta_tokens)
        observed["prefix_len"] = len(prefix_kv_refs or [])
        observed["features"] = list(delta_features or [])
        return _refs(sl, len(delta_tokens)), None

    next_state = sl.advance_step(base, [], ["img-b"], prefill_fn=prefill)
    assert observed["delta_tokens"] == [1, 2, 3, 4]
    assert observed["prefix_len"] == 0
    assert observed["features"] and observed["features"][0].image_hash == "img-b"
    assert "img-b" in next_state.feature_hashes


def test_advance_step_fails_fast_for_missing_new_image_feature():
    sl = _state_layer()
    base = sl.register(
        _refs(sl, 4),
        feature_hash="img-a",
        meta=StateMeta(token_ids=[1, 2, 3, 4], image_ids=["img-a"], workflow_id="wf-mm-miss", agent_id="a"),
    )
    with pytest.raises(KeyError, match="missing FeatureBundle"):
        sl.advance_step(base, [], ["img-missing"], prefill_fn=lambda *a, **k: ([], None))


def test_q4_transfer_is_not_silently_shape_corrupting():
    engine = TransferEngine(protocol="local")
    with pytest.raises(NotImplementedError):
        engine.transfer_tensor(torch.randn(2, 3), torch.device("cpu"), TransferPolicy(Mode.STREAM, precision=Precision.Q4))


def test_per_level_feature_intermediates_use_transfer_tensor(monkeypatch):
    engine = TransferEngine(protocol="local")
    calls = {"count": 0}
    original = engine.transfer_tensor

    def wrapped(tensor, target_device, policy=None):
        calls["count"] += 1
        return original(tensor, target_device, policy)

    monkeypatch.setattr(engine, "transfer_tensor", wrapped)
    bundle = FeatureBundle(
        "img",
        last_hidden=torch.randn(1, 4),
        intermediates=[(1, torch.randn(1, 2)), (2, torch.randn(1, 2))],
        grid_thw=torch.tensor([[1, 1, 2]]),
    )
    out = engine.transfer_feature_bundle(
        bundle,
        torch.device("cpu"),
        TransferPolicy(Mode.SHM, compress=CompressMode.PER_LEVEL),
    )
    assert calls["count"] == 4  # last_hidden + grid_thw + 2 intermediates
    assert len(out.intermediates) == 2


def test_serving_control_plane_rejects_empty_remote_block_ids():
    cp = ServingControlPlane()
    cp.register_stage_workers("prefill", ["pre-a"])
    cp.register_stage_workers("decode", ["dec-a"])
    ctx = cp.start_request({"messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]}, "req-empty-blocks")
    decision = cp.admit_stage("prefill", ctx)
    cp.build_prefill_kv_params(ctx, decision, decode_worker_id="dec-a")
    with pytest.raises(RuntimeError, match="remote_block_ids"):
        cp.note_prefill_response(
            ctx,
            {"remote_engine_id": "engine-a", "remote_bootstrap_addr": "addr", "remote_block_ids": []},
            decode_worker_id="dec-a",
        )


def test_radix_split_within_page_returns_only_matching_token_span():
    pm = PagedKVManager(
        page_size=4,
        num_layers=1,
        num_kv_heads=1,
        head_dim=1,
        dtype=torch.float32,
        device=torch.device("cpu"),
    )
    tree = RadixTree(pm, max_entries=16)

    ref_a = pm.allocate_page(filled=2)
    key_a = torch.tensor([[[[10.0], [20.0]]]])
    val_a = key_a.clone()
    pm.write_page_slots(ref_a, key_a, val_a, offset=0)

    ref_b = pm.allocate_page(filled=2)
    key_b = torch.tensor([[[[10.0], [30.0]]]])
    val_b = key_b.clone()
    pm.write_page_slots(ref_b, key_b, val_b, offset=0)

    tree.insert([1, 2], [ref_a])
    tree.insert([1, 3], [ref_b])

    matched_a, delta_a = tree.match_longest_prefix([1, 2])
    assert delta_a == []
    rebuilt_a = pm.materialize_kv_cache(matched_a)
    assert rebuilt_a[0][0].reshape(-1).tolist() == [10.0, 20.0]

    matched_b, delta_b = tree.match_longest_prefix([1, 3])
    assert delta_b == []
    rebuilt_b = pm.materialize_kv_cache(matched_b)
    assert rebuilt_b[0][0].reshape(-1).tolist() == [10.0, 30.0]

    tree.clear()
    pm.release_refs([ref_a, ref_b])
    assert pm.stats()["total_pages"] == 0


def test_state_mutations_blocked_during_offload_status():
    sl = _state_layer()
    state = sl.register(
        _refs(sl, 4),
        feature_hash=None,
        meta=StateMeta(token_ids=[1, 2, 3, 4], workflow_id="wf-lock", agent_id="a"),
    )
    state.status = "OFFLOADING"
    with pytest.raises(RuntimeError, match="offloading"):
        sl.fork(state)
    with pytest.raises(RuntimeError, match="offloading"):
        sl.advance_step_prepare(state, [5], [])


def test_ttl_reaper_releases_expired_state_with_kv_refs():
    sl = _state_layer()
    state = sl.register(
        _refs(sl, 4),
        feature_hash=None,
        meta=StateMeta(token_ids=[1, 2, 3, 4], workflow_id="wf-ttl", agent_id="a"),
    )
    state.ttl_deadline = 0.0
    state.meta.ttl_deadline = 0.0
    assert sl.pm.stats()["total_pages"] > 0
    sl._reap_once()
    assert sl.get_state(state.state_id) is None
    # Radix prefix cache still owns a cache ref; clearing it must release the
    # final owner and prove the reaper did not leave an orphan state ref.
    sl.radix.clear()
    assert sl.pm.stats()["total_pages"] == 0


def test_same_device_page_transfer_uses_refcounted_zero_copy():
    sl = _state_layer()
    refs = _refs(sl, 4)
    engine = TransferEngine(protocol="local")
    before_pages = sl.pm.stats()["total_pages"]
    target_pm, new_refs = engine.transfer_pages(sl.pm, refs, torch.device("cpu"), TransferPolicy(Mode.STREAM))
    assert target_pm is sl.pm
    assert sl.pm.stats()["total_pages"] == before_pages
    assert [r.physical_id for r in new_refs] == [r.physical_id for r in refs]
    for ref in refs:
        assert sl.pm.refcount(ref.physical_id) == 2
    sl.pm.release_refs(new_refs)
    sl.pm.release_refs(refs)
    assert sl.pm.stats()["total_pages"] == 0


def test_preempt_does_not_remove_victim_when_offload_fails():
    from mooncake_epd.agent.coordination import AgentRequest, AgentScheduler, AgentType

    sched = AgentScheduler([])
    low = AgentRequest("low", AgentType.THINKING, input_tokens=1, priority=1)
    high = AgentRequest("high", AgentType.INTERACTIVE, input_tokens=1, priority=10)
    low.state = object()
    active = [low]

    class FailingOffload:
        def offload(self, state):
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        sched.preempt_for(high, active_requests=active, offload_manager=FailingOffload())
    assert active == [low]
