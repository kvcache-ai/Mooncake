from __future__ import annotations

import os

import pytest
import torch

from mooncake_epd.core.state import (
    DirectFeatureBufferRegistry,
    FeatureBundle,
    FeatureHandle,
    FeatureHandleError,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    FeatureHandleRegistry,
    MMStore,
    clear_feature_handle_bundle_cache,
    close_default_feature_handle_provider,
    maybe_inject_feature_handle_kwargs,
    publish_feature_bundle_to_dir,
    register_direct_feature_buffer_registry,
    register_feature_handle_registry,
    resolve_feature_handles_for_vllm,
    unregister_direct_feature_buffer_registry,
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


def test_single_direct_feature_handle_merge_avoids_cat_copy():
    bundle = FeatureBundle(
        image_hash="direct-single-no-copy",
        last_hidden=torch.arange(8, dtype=torch.float32).reshape(2, 4),
        intermediates=[],
        grid_thw=torch.tensor([[1, 2, 1]], dtype=torch.long),
        metadata={"model_fingerprint": "model-a", "processor_fingerprint": "processor-a"},
    )
    registry = DirectFeatureBufferRegistry(worker_id="prefill-single", device="cpu")
    allocation = registry.allocate_for_descriptor(bundle.descriptor())
    allocation.tensors["last_hidden"].copy_(bundle.last_hidden)
    allocation.tensors["grid_thw"].copy_(bundle.grid_thw)
    register_direct_feature_buffer_registry(registry)
    try:
        handle = FeatureHandle(
            handle_id="direct-single",
            feature_id=bundle.image_hash,
            store_id="direct-store",
            uri=f"epd-direct://direct-store/{bundle.image_hash}",
            descriptor=bundle.descriptor(),
            metadata={
                "backend": "direct_engine",
                "direct_plan": {
                    "feature_id": bundle.image_hash,
                    "targets": [
                        {
                            "name": name,
                            "remote_pointer": int(tensor.data_ptr()),
                            "nbytes": int(tensor.nelement() * tensor.element_size()),
                        }
                        for name, tensor in allocation.tensors.items()
                    ],
                },
            },
        )
        provider = FeatureHandleProvider(
            FeatureHandleProviderConfig(worker_id="prefill-single", device="cpu", strict=True)
        )
        resolved = provider.resolve_from_sources(
            {"mm_feature_handles": [handle.as_control_payload()]},
            device="cpu",
            dtype=torch.float32,
        )

        assert resolved is not None
        assert torch.equal(resolved.image_embeds, bundle.last_hidden)
        assert resolved.image_embeds.data_ptr() == allocation.tensors["last_hidden"].data_ptr()
        assert resolved.image_grid_thw is not None
        assert resolved.image_grid_thw.data_ptr() == allocation.tensors["grid_thw"].data_ptr()
    finally:
        unregister_direct_feature_buffer_registry("prefill-single")


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
    clear_feature_handle_bundle_cache()
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


def test_epd_direct_handle_is_fail_closed_unless_explicitly_enabled():
    clear_feature_handle_bundle_cache()
    bundle = _bundle("missing-direct", 1.0)
    direct = FeatureHandle(
        handle_id="missing-direct",
        feature_id=bundle.image_hash,
        store_id="direct-store",
        uri=f"epd-direct://direct-store/{bundle.image_hash}",
        descriptor=bundle.descriptor(),
        metadata={"backend": "direct_engine"},
    )
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(device="cpu", strict=False)
    )
    with pytest.raises(FeatureHandleError):
        provider.resolve_from_sources(
            {"mm_feature_handles": [direct.as_control_payload()]}
        )

    opt_in = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            strict=False,
            allow_direct_feature_fallback=True,
        )
    )
    assert opt_in.resolve_from_sources(
        {"mm_feature_handles": [direct.as_control_payload()]}
    ) is None


def test_feature_handle_provider_reuses_bundle_cache_for_repeated_handles(tmp_path):
    clear_feature_handle_bundle_cache()
    handle = publish_feature_bundle_to_dir(_bundle("cached", 4.0), tmp_path)
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            store_dirs=(tmp_path,),
            strict=True,
            bundle_cache_entries=4,
            bundle_cache_max_bytes=1024 * 1024,
        )
    )

    first = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})
    os.remove(tmp_path / "cached.pt")
    second = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})

    assert first is not None
    assert second is not None
    assert torch.equal(first.image_embeds, second.image_embeds)




def test_feature_handle_provider_reuses_resolved_cache_for_repeated_handles(tmp_path):
    clear_feature_handle_bundle_cache()
    handle = publish_feature_bundle_to_dir(_bundle("resolved-cached", 6.0), tmp_path)
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(
            device="cpu",
            store_dirs=(tmp_path,),
            strict=True,
            bundle_cache_entries=0,
            resolved_cache_entries=4,
            resolved_cache_max_bytes=1024 * 1024,
        )
    )

    first = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})
    os.remove(tmp_path / "resolved-cached.pt")
    second = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]})

    assert first is not None
    assert second is not None
    assert torch.equal(first.image_embeds, second.image_embeds)


def test_default_feature_handle_provider_can_be_closed_and_recreated():
    close_default_feature_handle_provider()
    from mooncake_epd.core.state import get_default_feature_handle_provider

    p1 = get_default_feature_handle_provider()
    p2 = get_default_feature_handle_provider()
    assert p1 is p2
    close_default_feature_handle_provider()
    p3 = get_default_feature_handle_provider()
    assert p3 is not p1
    close_default_feature_handle_provider()


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


class _BorrowedRegistrationMooncakeEngine(_ReadingMooncakeEngine):
    def register_memory(self, ptr, nbytes):
        self.registered.append((int(ptr), int(nbytes)))
        return -600


def test_direct_read_does_not_unregister_borrowed_registration(monkeypatch):
    import mooncake_epd.core.state.vllm_feature_handle_provider as provider_mod
    from mooncake_epd.core.transfer import TransferEngine

    engine = TransferEngine(protocol="tcp")
    fake = _BorrowedRegistrationMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    monkeypatch.setattr(provider_mod, "_DIRECT_READ_ENGINE", engine)

    bundle = _bundle("borrowed-direct", 4.0)
    handle = FeatureHandle(
        handle_id="borrowed-direct",
        feature_id=bundle.image_hash,
        store_id="direct-store",
        uri=f"epd-direct://direct-store/{bundle.image_hash}",
        descriptor=bundle.descriptor(),
        metadata={
            "backend": "direct_engine",
            "direct_remote_session": "api-prefill-session",
            "direct_plan": {
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
    provider = FeatureHandleProvider(
        FeatureHandleProviderConfig(worker_id="no-registry", device="cpu", strict=True)
    )
    resolved = provider.resolve_from_sources(
        {"mm_feature_handles": [handle.as_control_payload()]},
        device="cpu",
        dtype=torch.float32,
    )

    assert resolved is not None
    assert fake.registered
    assert fake.unregistered == []


def test_epd_direct_remote_handle_reads_into_target_tensors(monkeypatch):
    import mooncake_epd.core.state.vllm_feature_handle_provider as provider_mod
    from mooncake_epd.core.transfer import TransferEngine

    bundle = _bundle("remote-direct", 9.0)
    engine = TransferEngine(protocol="tcp")
    fake = _ReadingMooncakeEngine()
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    monkeypatch.setattr(provider_mod, "_DIRECT_READ_ENGINE", engine)
    monkeypatch.setenv("MOONCAKE_EPD_DIRECT_READ_MODE", "registered_tensor")
    handle = FeatureHandle(
        handle_id="remote-direct-handle",
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

    provider = FeatureHandleProvider(FeatureHandleProviderConfig(worker_id="no-registry", device="cpu", strict=True))
    resolved = provider.resolve_from_sources({"mm_feature_handles": [handle.as_control_payload()]}, device="cpu", dtype=torch.float32)

    assert resolved is not None
    assert torch.equal(resolved.image_embeds, torch.cat([bundle.last_hidden, bundle.intermediates[0][1]], dim=-1))
    assert torch.equal(resolved.image_grid_thw, bundle.grid_thw)
    assert fake.read_calls and fake.read_calls[0][0] == "api-prefill-session"
    assert len(fake.registered) == 3
    assert fake.unregistered == [ptr for ptr, _ in fake.registered]


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
