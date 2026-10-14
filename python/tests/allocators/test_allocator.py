from __future__ import annotations

from types import ModuleType
from typing import Callable

import pytest

from conftest import FakePluggableAllocator


@pytest.mark.parametrize(
    ("probe_value", "expected_name"),
    [
        (0, "USE_CUDAMALLOC"),
        (1, "USE_CUMEMCREATE"),
        (-2, "UNSUPPORTED"),
        (37, "UNKNOWN"),
    ],
)
def test_nvlink_probe_maps_backend_values(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    probe_value: int,
    expected_name: str,
) -> None:
    allocator = import_allocator_module("mooncake.allocator")

    def probe_backend(*_args: object) -> int:
        return probe_value

    monkeypatch.setattr(allocator, "probe_allocator_backend", probe_backend)

    backend = allocator.NVLinkAllocator._probe_fabric_memory_support("fake.so")

    assert backend.name == expected_name


def test_nvlink_detection_probes_once(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator")
    allocator_type = allocator.NVLinkAllocator
    allocator_type._probe_done = False
    allocator_type._supports_fabric = allocator.MemoryBackend.UNKNOWN
    calls = 0

    def get_path(_cls: type[object]) -> str:
        return "nvlink_allocator.so"

    def probe(_cls: type[object], so_path: str) -> object:
        nonlocal calls
        calls += 1
        assert so_path == "nvlink_allocator.so"
        return allocator.MemoryBackend.USE_CUMEMCREATE

    monkeypatch.setattr(allocator_type, "_get_so_path", classmethod(get_path))
    monkeypatch.setattr(
        allocator_type, "_probe_fabric_memory_support", classmethod(probe)
    )

    assert (
        allocator_type.detect_mem_backend() is allocator.MemoryBackend.USE_CUMEMCREATE
    )
    assert (
        allocator_type.detect_mem_backend() is allocator.MemoryBackend.USE_CUMEMCREATE
    )
    assert calls == 1


def test_nvlink_detection_caches_setup_failure(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator")
    allocator_type = allocator.NVLinkAllocator
    allocator_type._probe_done = False
    allocator_type._supports_fabric = allocator.MemoryBackend.UNKNOWN
    calls = 0

    def get_path(_cls: type[object]) -> str:
        nonlocal calls
        calls += 1
        raise ImportError("allocator unavailable")

    monkeypatch.setattr(allocator_type, "_get_so_path", classmethod(get_path))

    assert allocator_type.detect_mem_backend() is allocator.MemoryBackend.UNSUPPORTED
    assert allocator_type.detect_mem_backend() is allocator.MemoryBackend.UNSUPPORTED
    assert calls == 1


def test_nvlink_allocator_instance_is_cached_per_device(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator")
    allocator_type = allocator.NVLinkAllocator
    allocator_type._instances = {}
    monkeypatch.setattr(
        allocator_type,
        "_get_so_path",
        classmethod(lambda _cls: "nvlink_allocator.so"),
    )
    device = object()

    first = allocator_type.get_allocator(device)
    second = allocator_type.get_allocator(device)

    assert first is second
    assert FakePluggableAllocator.created == [
        ("nvlink_allocator.so", "mc_allocator_malloc", "mc_allocator_free")
    ]
