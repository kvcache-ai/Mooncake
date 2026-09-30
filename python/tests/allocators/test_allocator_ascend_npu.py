from __future__ import annotations

from types import ModuleType
from typing import Callable

import pytest

from conftest import FakePluggableAllocator


@pytest.mark.parametrize(("probe_value", "expected"), [(0, False), (1, True)])
def test_ubshmem_probe_maps_capability(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    probe_value: int,
    expected: bool,
) -> None:
    allocator = import_allocator_module("mooncake.allocator_ascend_npu")

    def probe_backend(*_args: object) -> int:
        return probe_value

    monkeypatch.setattr(allocator, "probe_allocator_backend", probe_backend)

    assert (
        allocator.UBShmemAllocator._probe_fabric_memory_support("fake.so") is expected
    )


def test_ubshmem_detection_probes_once(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator_ascend_npu")
    allocator_type = allocator.UBShmemAllocator
    allocator_type._probe_done = False
    allocator_type._supports_fabric = False
    calls = 0

    def get_path(_cls: type[object]) -> str:
        return "ubshmem_fabric_allocator.so"

    def probe(_cls: type[object], so_path: str) -> bool:
        nonlocal calls
        calls += 1
        assert so_path == "ubshmem_fabric_allocator.so"
        return True

    monkeypatch.setattr(allocator_type, "_get_so_path", classmethod(get_path))
    monkeypatch.setattr(
        allocator_type, "_probe_fabric_memory_support", classmethod(probe)
    )

    assert allocator_type.detect_mem_backend() is True
    assert allocator_type.detect_mem_backend() is True
    assert calls == 1


def test_ubshmem_detection_caches_setup_failure(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator_ascend_npu")
    allocator_type = allocator.UBShmemAllocator
    allocator_type._probe_done = False
    allocator_type._supports_fabric = True
    calls = 0

    def get_path(_cls: type[object]) -> str:
        nonlocal calls
        calls += 1
        raise ImportError("allocator unavailable")

    monkeypatch.setattr(allocator_type, "_get_so_path", classmethod(get_path))

    assert allocator_type.detect_mem_backend() is False
    assert allocator_type.detect_mem_backend() is False
    assert calls == 1


def test_ubshmem_allocator_instance_is_cached_per_device(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allocator = import_allocator_module("mooncake.allocator_ascend_npu")
    allocator_type = allocator.UBShmemAllocator
    allocator_type._instances = {}
    monkeypatch.setattr(
        allocator_type,
        "_get_so_path",
        classmethod(lambda _cls: "ubshmem_fabric_allocator.so"),
    )
    device = object()

    first = allocator_type.get_allocator(device)
    second = allocator_type.get_allocator(device)

    assert first is second
    assert FakePluggableAllocator.created == [
        (
            "ubshmem_fabric_allocator.so",
            "mc_allocator_malloc",
            "mc_allocator_free",
        )
    ]
