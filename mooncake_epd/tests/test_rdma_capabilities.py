from __future__ import annotations

import os

import pytest

from mooncake_epd.core.transfer import (
    HwCaps,
    RegisteredRegion,
    default_rdma_bind_address,
    detect_rdma_capabilities,
    rdmacm_cuda_staging_smoke,
    rdmacm_listener_smoke,
    resolve_rdma_protocol,
)


def test_rdma_capability_probe_is_transport_specific():
    capabilities = detect_rdma_capabilities()

    assert capabilities.devices
    assert capabilities.has_rdma
    assert any(device.active for device in capabilities.devices)
    # This host is Intel X722 iWARP. The regression protects against the old
    # false claim that every verbs HCA is Mooncake/GPU-Direct compatible.
    active_iwarp = [
        device for device in capabilities.devices if device.active and device.is_iwarp
    ]
    if active_iwarp:
        assert capabilities.rdmacm_compatible
        assert not capabilities.mooncake_compatible
        assert resolve_rdma_protocol("auto", capabilities) == "rdmacm"
        assert resolve_rdma_protocol("rdma", capabilities) == "rdmacm"


def test_same_host_auto_never_routes_through_the_rnic():
    capabilities = detect_rdma_capabilities()
    assert resolve_rdma_protocol(
        "auto",
        capabilities,
        same_host=True,
    ) == "local"


def test_hwcaps_does_not_infer_gpudirect_from_loaded_module_only():
    capabilities = detect_rdma_capabilities()
    hw = HwCaps.detect()

    assert hw.has_rdma == capabilities.has_rdma
    assert hw.has_gpudirect == capabilities.has_gpudirect
    if capabilities.preferred_device is not None:
        assert hw.rdma_device == capabilities.preferred_device.name
        assert hw.rdma_transport == capabilities.preferred_device.transport


def test_registered_region_bounds_are_strict():
    region = RegisteredRegion(0x1000, 0x100, "cuda")

    assert region.contains(0x1000, 0x100)
    assert region.contains(0x1080, 0x80)
    assert not region.contains(0x0FFF, 1)
    assert not region.contains(0x1080, 0x81)
    assert not region.contains(0x1000, -1)


def test_real_rdmacm_listener_smoke_uses_active_hardware_endpoint():
    capabilities = detect_rdma_capabilities()
    if not capabilities.rdmacm_compatible:
        pytest.skip("no active rdma_cm-compatible device")
    address = default_rdma_bind_address(capabilities)
    if not address or ":" in address:
        pytest.skip("an IPv4 RDMA address is required by the current rsocket backend")

    # Use a process-specific port to avoid collisions with concurrently running
    # real-model suites. This is a real rbind/rlisten operation, not a mock.
    port = 48000 + (os.getpid() % 1000)
    result = rdmacm_listener_smoke(
        bind_address=address,
        port=port,
        capabilities=capabilities,
    )

    assert result["ok"] is True
    assert result["backend"] == "rdmacm_rsocket"
    assert result["bind_address"] == address


def test_real_cuda_staging_path_on_gpu3():
    import torch

    if not torch.cuda.is_available() or torch.cuda.device_count() <= 3:
        pytest.skip("physical GPU 3 is unavailable")
    result = rdmacm_cuda_staging_smoke(
        cuda_device="cuda:3",
        size_bytes=8192,
    )

    assert result == {
        "ok": True,
        "device": "cuda:3",
        "nbytes": 8192,
        "backend": "cuda_host_staging",
    }
