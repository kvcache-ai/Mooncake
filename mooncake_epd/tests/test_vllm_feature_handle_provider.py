from __future__ import annotations

import os

import pytest
import torch

from mooncake_epd.core.state import (
    FeatureBundle,
    FeatureHandle,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    FeatureHandleRegistry,
    MMStore,
    clear_feature_handle_resolved_cache,
    close_default_feature_handle_provider,
    get_default_feature_handle_provider,
    maybe_inject_feature_handle_kwargs,
    publish_feature_bundle_to_dir,
    register_feature_handle_registry,
    resolve_feature_handles_for_vllm,
    unregister_feature_handle_registry,
)


def _bundle(feature_id: str, base: float) -> FeatureBundle:
    return FeatureBundle(
        image_hash=feature_id,
        last_hidden=torch.tensor([[base, base + 1.0], [base + 2.0, base + 3.0]]),
        intermediates=[(3, torch.tensor([[base + 10.0], [base + 11.0]]))],
        grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
        metadata={"model_fingerprint": "model-a", "processor_fingerprint": "processor-a"},
    )


def test_file_backed_feature_handles_resolve_and_merge(tmp_path, monkeypatch):
    monkeypatch.setenv("MOONCAKE_EPD_VLLM_MM_HIDDEN_CACHE", "1")
    monkeypatch.setenv("MOONCAKE_EPD_CONNECTOR_METRICS_DIR", str(tmp_path / "metrics"))
    store_dir = tmp_path / "feature_store"
    h1 = publish_feature_bundle_to_dir(_bundle("img-a", 1.0), store_dir)
    h2 = publish_feature_bundle_to_dir(_bundle("img-b", 5.0), store_dir)

    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            worker_id="prefill-test",
            device="cpu",
            store_dirs=(store_dir,),
            expected_model_fingerprint="model-a",
            expected_processor_fingerprint="processor-a",
        )
    )
    resolved = provider.resolve_from_sources(
        {"kv_transfer_params": {"mm_feature_handles": [h1.as_control_payload(), h2.as_control_payload()]}}
    )

    assert resolved is not None
    assert resolved.count == 2
    # vLLM Qwen3-VL consumes packed image embeds: main hidden || deepstack hidden.
    assert torch.equal(
        resolved.image_embeds,
        torch.tensor([[1.0, 2.0, 11.0], [3.0, 4.0, 12.0], [5.0, 6.0, 15.0], [7.0, 8.0, 16.0]]),
    )
    assert torch.equal(resolved.image_grid_thw, torch.tensor([[1, 2, 2], [1, 2, 2]]))
    assert len(resolved.deepstack_image_embeds) == 1
    assert resolved.deepstack_image_embeds[0][0] == 3
    assert torch.equal(resolved.deepstack_image_embeds[0][1], torch.tensor([[11.0], [12.0], [15.0], [16.0]]))


def test_in_process_registry_feature_handle_resolution():
    mm_store = MMStore()
    registry = FeatureHandleRegistry(mm_store, store_id="unit-mm-store")
    bundle = _bundle("img-reg", 2.0)
    handle = registry.publish_bundle(bundle)
    register_feature_handle_registry(registry)
    try:
        resolved = resolve_feature_handles_for_vllm(
            {"mm_feature_handles": [handle.as_control_payload()]},
            device="cpu",
            provider=FeatureHandleProvider(FeatureHandleProviderConfig(worker_id="prefill", device="cpu")),
        )
    finally:
        unregister_feature_handle_registry("unit-mm-store")
        mm_store.stop()

    assert resolved is not None
    assert torch.equal(resolved.image_embeds, torch.cat([bundle.last_hidden, bundle.intermediates[0][1]], dim=-1))


def test_feature_handle_provider_is_fail_open_unless_strict(tmp_path):
    clear_feature_handle_resolved_cache()
    missing = publish_feature_bundle_to_dir(_bundle("missing", 1.0), tmp_path)
    os.remove(tmp_path / "missing.pt")
    fail_open = FeatureHandleProvider(
        FeatureHandleProviderConfig(device="cpu", store_dirs=(tmp_path,), strict=False)
    )
    assert fail_open.resolve_from_sources({"mm_feature_handles": [missing.as_control_payload()]}) is None

    strict = FeatureHandleProvider(
        FeatureHandleProviderConfig(device="cpu", store_dirs=(tmp_path,), strict=True)
    )
    with pytest.raises(Exception):
        strict.resolve_from_sources({"mm_feature_handles": [missing.as_control_payload()]})


class _ReadingMooncakeEngine:
    def __init__(self):
        self.registered = []
        self.unregistered = []
        self.read_calls = []

    def register_memory(self, ptr, nbytes):
        self.registered.append((int(ptr), int(nbytes)))
        return 0

    def unregister_memory(self, ptr):
        self.unregistered.append(int(ptr))
        return 0

    def transfer_sync_read(self, remote_session, local_ptr, remote_ptr, length):
        import ctypes

        self.read_calls.append((remote_session, [int(local_ptr)], [int(remote_ptr)], [int(length)]))
        ctypes.memmove(int(local_ptr), int(remote_ptr), int(length))
        return 0

    def batch_transfer_sync_read(self, remote_session, local_ptrs, remote_ptrs, lengths):
        import ctypes

        self.read_calls.append((remote_session, list(local_ptrs), list(remote_ptrs), list(lengths)))
        for local_ptr, remote_ptr, length in zip(local_ptrs, remote_ptrs, lengths):
            ctypes.memmove(int(local_ptr), int(remote_ptr), int(length))
        return 0


def _direct_handle(bundle: FeatureBundle, *, handle_id: str = "remote-direct") -> FeatureHandle:
    from mooncake_epd.core.transfer import TransferEngine

    return FeatureHandle(
        handle_id=handle_id,
        feature_id=bundle.image_hash,
        store_id="direct-store",
        uri=f"epd-direct://direct-store/{bundle.image_hash}",
        descriptor=bundle.descriptor(),
        metadata={
            "backend": "direct_engine",
            "direct_remote_session": "api-prefill-session",
            "direct_plan": {
                "feature_id": bundle.image_hash,
                "targets": [
                    {
                        "name": name,
                        "remote_pointer": int(tensor.data_ptr()),
                        "nbytes": int(tensor.nelement() * tensor.element_size()),
                    }
                    for name, tensor in TransferEngine.feature_bundle_tensor_items(bundle)
                ],
            },
        },
    )


def test_epd_direct_remote_handle_reads_into_final_tensors(monkeypatch):
    import mooncake_epd.core.state.vllm_feature_handle_provider as provider_mod
    from mooncake_epd.core.transfer import TransferEngine

    clear_feature_handle_resolved_cache()
    bundle = _bundle("remote-direct", 9.0)
    engine = TransferEngine(protocol="tcp")
    fake = _ReadingMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    monkeypatch.setattr(provider_mod, "_DIRECT_READ_ENGINE", engine)
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_READ_MODE", "registered_tensor")

    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            worker_id="no-registry",
            device="cpu",
            strict=True,
            resolved_cache_entries=0,
        )
    )
    resolved = provider.resolve_from_sources(
        {"mm_feature_handles": [_direct_handle(bundle).as_control_payload()]},
        device="cpu",
        dtype=torch.float32,
    )

    assert resolved is not None
    assert torch.equal(
        resolved.image_embeds,
        torch.cat([bundle.last_hidden, bundle.intermediates[0][1]], dim=-1),
    )
    assert torch.equal(resolved.image_grid_thw, bundle.grid_thw)
    assert fake.read_calls and fake.read_calls[0][0] == "api-prefill-session"
    # grid_thw is authoritative semantic control metadata and is rebuilt
    # locally; only the bandwidth-bearing hidden tensors use direct reads.
    assert len(fake.registered) == 2
    assert fake.unregistered == [ptr for ptr, _ in fake.registered]
    assert bundle.grid_thw.data_ptr() not in fake.read_calls[0][2]


def test_epd_direct_remote_handle_ignores_poisoned_grid_buffer(monkeypatch):
    import mooncake_epd.core.state.vllm_feature_handle_provider as provider_mod
    from mooncake_epd.core.transfer import TransferEngine

    clear_feature_handle_resolved_cache()
    bundle = _bundle("remote-direct-poisoned-grid", 11.0)
    handle = _direct_handle(bundle)
    grid_target = next(
        target
        for target in handle.metadata["direct_plan"]["targets"]
        if target["name"] == "grid_thw"
    )
    grid_target["remote_pointer"] = 0

    engine = TransferEngine(protocol="tcp")
    fake = _ReadingMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    monkeypatch.setattr(provider_mod, "_DIRECT_READ_ENGINE", engine)
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_READ_MODE", "registered_tensor")
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            worker_id="no-registry",
            device="cpu",
            strict=True,
            resolved_cache_entries=0,
        )
    )

    resolved = provider.resolve_from_sources(
        {"mm_feature_handles": [handle.as_control_payload()]},
        device="cpu",
        dtype=torch.float32,
    )

    assert resolved is not None
    assert torch.equal(resolved.image_grid_thw, bundle.grid_thw)
    assert 0 not in fake.read_calls[0][2]


def test_epd_direct_remote_handle_rejects_invalid_semantic_grid(monkeypatch):
    from dataclasses import replace

    bundle = _bundle("remote-direct-invalid-grid", 12.0)
    handle = _direct_handle(bundle)
    descriptor = replace(
        handle.descriptor,
        metadata={**handle.descriptor.metadata, "grid_thw_values": [[0, 2, 2]]},
    )
    handle = replace(handle, descriptor=descriptor)
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            worker_id="no-registry",
            device="cpu",
            strict=True,
            resolved_cache_entries=0,
        )
    )

    with pytest.raises(Exception, match="grid_thw values must be positive"):
        provider.resolve_from_sources(
            {"mm_feature_handles": [handle.as_control_payload()]},
            device="cpu",
            dtype=torch.float32,
        )


def test_adaptive_direct_read_policy_uses_hysteresis_without_same_request_fallback():
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            direct_read_mode="adaptive",
            direct_read_slow_ms=100.0,
            direct_read_managed_cooldown_reads=2,
        )
    )

    requested, selected, transition = provider._select_direct_read_mode()
    assert (requested, selected, transition) == ("adaptive", "registered_tensor", None)
    transition, remaining = provider._observe_direct_read(
        requested_mode=requested,
        selected_mode=selected,
        timings_ms={"total_ms": 101.0},
    )
    assert transition == "adaptive_registered_slow_to_managed"
    assert remaining == 2

    assert provider._select_direct_read_mode() == (
        "adaptive",
        "managed_buffer",
        "adaptive_managed_cooldown",
    )
    assert provider._select_direct_read_mode() == (
        "adaptive",
        "managed_buffer",
        "adaptive_managed_cooldown",
    )
    assert provider._select_direct_read_mode() == (
        "adaptive",
        "registered_tensor",
        "adaptive_registered_probe",
    )


def test_adaptive_direct_read_policy_keeps_fast_registered_path():
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            direct_read_mode="adaptive",
            direct_read_slow_ms=100.0,
            direct_read_managed_cooldown_reads=2,
        )
    )
    requested, selected, _ = provider._select_direct_read_mode()
    transition, remaining = provider._observe_direct_read(
        requested_mode=requested,
        selected_mode=selected,
        timings_ms={"total_ms": 99.0},
    )
    assert transition is None
    assert remaining == 0
    assert provider._select_direct_read_mode()[1] == "registered_tensor"


def test_adaptive_direct_read_policy_uses_payload_normalized_ewma_after_warmup():
    mib = 1024 * 1024
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            direct_read_mode="adaptive",
            direct_read_slow_ms=10_000.0,
            direct_read_managed_cooldown_reads=2,
            direct_read_adaptive_min_samples=2,
            direct_read_adaptive_ewma_alpha=0.5,
            direct_read_adaptive_slow_ratio=2.0,
            direct_read_adaptive_min_observation_ms=0.0,
        )
    )
    key = provider._direct_read_policy_key(
        remote_session="encoder-a",
        nbytes=100 * mib,
    )

    for nbytes, total_ms in ((100 * mib, 100.0), (120 * mib, 120.0)):
        requested, selected, _ = provider._select_direct_read_mode(key)
        transition, remaining, observation = provider._observe_direct_read_detailed(
            requested_mode=requested,
            selected_mode=selected,
            timings_ms={"total_ms": total_ms, "nbytes": float(nbytes)},
            policy_key=key,
        )
        assert transition is None
        assert remaining == 0
        assert observation["baseline_ms_per_mib"] == pytest.approx(1.0)

    requested, selected, _ = provider._select_direct_read_mode(key)
    transition, remaining, observation = provider._observe_direct_read_detailed(
        requested_mode=requested,
        selected_mode=selected,
        timings_ms={"total_ms": 300.0, "nbytes": float(120 * mib)},
        policy_key=key,
    )

    assert transition == "adaptive_registered_relative_slow_to_managed"
    assert remaining == 2
    assert observation["slow_reason"] == "relative_ewma"
    assert observation["effective_mib_per_s"] == pytest.approx(400.0)
    # The slow sample must not poison the fast-path baseline.
    assert observation["baseline_ms_per_mib"] == pytest.approx(1.0)
    assert observation["baseline_samples"] == 2


def test_adaptive_direct_read_policy_isolates_remote_session_and_payload_bucket():
    mib = 1024 * 1024
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            direct_read_mode="adaptive",
            direct_read_slow_ms=100.0,
            direct_read_managed_cooldown_reads=1,
        )
    )
    slow_key = provider._direct_read_policy_key(
        remote_session="encoder-a",
        nbytes=100 * mib,
    )
    other_peer_key = provider._direct_read_policy_key(
        remote_session="encoder-b",
        nbytes=100 * mib,
    )
    other_bucket_key = provider._direct_read_policy_key(
        remote_session="encoder-a",
        nbytes=200 * mib,
    )
    assert len({slow_key, other_peer_key, other_bucket_key}) == 3

    requested, selected, _ = provider._select_direct_read_mode(slow_key)
    transition, remaining = provider._observe_direct_read(
        requested_mode=requested,
        selected_mode=selected,
        timings_ms={"total_ms": 101.0, "nbytes": float(100 * mib)},
        policy_key=slow_key,
    )
    assert transition == "adaptive_registered_slow_to_managed"
    assert remaining == 1

    assert provider._select_direct_read_mode(slow_key)[1] == "managed_buffer"
    assert provider._select_direct_read_mode(other_peer_key)[1] == "registered_tensor"
    assert provider._select_direct_read_mode(other_bucket_key)[1] == "registered_tensor"


def test_resolved_direct_handle_cache_ignores_ephemeral_transfer_plan(monkeypatch):
    import mooncake_epd.core.state.vllm_feature_handle_provider as provider_mod
    from mooncake_epd.core.transfer import TransferEngine

    clear_feature_handle_resolved_cache()
    bundle = _bundle("direct-cache", 13.0)
    engine = TransferEngine(protocol="tcp")
    fake = _ReadingMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    monkeypatch.setattr(provider_mod, "_DIRECT_READ_ENGINE", engine)
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_READ_MODE", "registered_tensor")
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            worker_id="no-registry",
            device="cpu",
            strict=True,
            resolved_cache_entries=4,
            resolved_cache_max_bytes=1024 * 1024,
        )
    )

    first_handle = _direct_handle(bundle, handle_id="first")
    first = provider.resolve_from_sources(
        {"mm_feature_handles": [first_handle.as_control_payload()]},
        device="cpu",
        dtype=torch.float32,
    )
    assert first is not None
    assert len(fake.read_calls) == 1

    second_handle = _direct_handle(bundle, handle_id="second")
    second_handle.metadata["direct_plan"]["targets"][0]["remote_pointer"] = 0
    second = provider.resolve_from_sources(
        {"mm_feature_handles": [second_handle.as_control_payload()]},
        device="cpu",
        dtype=torch.float32,
    )

    assert second is first
    assert len(fake.read_calls) == 1


def test_default_feature_handle_provider_is_process_singleton():
    close_default_feature_handle_provider()
    first = get_default_feature_handle_provider()
    assert get_default_feature_handle_provider() is first
    close_default_feature_handle_provider()
    assert get_default_feature_handle_provider() is not first
    close_default_feature_handle_provider()


def test_maybe_inject_feature_handle_kwargs_preserves_existing_embeds(tmp_path):
    handle = publish_feature_bundle_to_dir(_bundle("img-a", 1.0), tmp_path)
    existing = torch.ones(1, 2)
    provider = FeatureHandleProvider(FeatureHandleProviderConfig(device="cpu", store_dirs=(tmp_path,)))

    unchanged = maybe_inject_feature_handle_kwargs(
        {"image_embeds": existing, "kv_transfer_params": {"mm_feature_handles": [handle.as_control_payload()]}},
        provider=provider,
    )
    assert unchanged["image_embeds"] is existing

    injected = maybe_inject_feature_handle_kwargs(
        {"kv_transfer_params": {"mm_feature_handles": [handle.as_control_payload()]}},
        provider=provider,
    )
    expected_bundle = _bundle("img-a", 1.0)
    assert torch.equal(injected["image_embeds"], torch.cat([expected_bundle.last_hidden, expected_bundle.intermediates[0][1]], dim=-1))
    assert "image_grid_thw" in injected


def test_vllm_mm_kwargs_injection_replaces_pixels_when_vllm_available(tmp_path):
    pytest.importorskip("vllm")
    from types import SimpleNamespace

    from vllm.multimodal.inputs import (  # type: ignore
        MultiModalFieldConfig,
        MultiModalFieldElem,
        MultiModalKwargsItem,
    )

    from mooncake_epd.core.state import inject_feature_handles_into_vllm_mm_kwargs

    bundle = _bundle("stable-mm-hash", 3.0)
    handle = publish_feature_bundle_to_dir(
        bundle,
        tmp_path,
        metadata={"source_mm_hash": "stable-mm-hash"},
    )
    flat_field = MultiModalFieldConfig.flat("image", [slice(0, 4)]).field
    batched_field = MultiModalFieldConfig.batched("image").field
    original = MultiModalKwargsItem(
        {
            "pixel_values": MultiModalFieldElem(data=torch.zeros(4, 2), field=flat_field),
            "image_grid_thw": MultiModalFieldElem(data=torch.tensor([1, 2, 2]), field=batched_field),
        }
    )
    req = SimpleNamespace(
        kv_transfer_params={"mm_feature_handles": [handle.as_control_payload()]}
    )
    provider = FeatureHandleProvider(FeatureHandleProviderConfig(device="cpu", store_dirs=(tmp_path,)))

    _, converted_kwargs, _ = inject_feature_handles_into_vllm_mm_kwargs(
        mm_hashes=["stable-mm-hash"],
        mm_kwargs=[("image", original)],
        mm_lora_refs=[("req-1", object())],
        requests={"req-1": req},
        device="cpu",
        provider=provider,
    )

    converted = converted_kwargs[0][1]
    assert "pixel_values" not in converted
    assert "image_embeds" in converted
    assert torch.equal(converted["image_embeds"].data, torch.cat([bundle.last_hidden, bundle.intermediates[0][1]], dim=-1))
    assert torch.equal(converted["image_grid_thw"].data, torch.tensor([1, 2, 2]))
