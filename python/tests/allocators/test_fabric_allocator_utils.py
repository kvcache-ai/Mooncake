from __future__ import annotations

from types import ModuleType
from typing import Callable

import pytest


class FakeProbe:
    def __init__(self, result: int):
        self.result = result
        self.argtypes: list[object] | None = None
        self.restype: object | None = None

    def __call__(self, device_id: int) -> int:
        assert device_id == 0
        return self.result


def test_probe_allocator_backend_calls_the_exported_symbol(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utilities = import_allocator_module("mooncake.fabric_allocator_utils")
    probe = FakeProbe(7)
    library = ModuleType("allocator_library")
    library.mc_allocator_probe = probe
    monkeypatch.setattr(utilities.ctypes, "CDLL", lambda _path: library)

    result = utilities.probe_allocator_backend(
        "allocator.so", "mc_allocator_probe", utilities.ctypes.c_int, -2
    )

    assert result == 7
    assert probe.argtypes == [utilities.ctypes.c_int]
    assert probe.restype is utilities.ctypes.c_int


def test_probe_allocator_backend_uses_fallback_for_missing_symbol(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utilities = import_allocator_module("mooncake.fabric_allocator_utils")
    monkeypatch.setattr(utilities.ctypes, "CDLL", lambda _path: object())

    assert (
        utilities.probe_allocator_backend(
            "allocator.so", "mc_allocator_probe", utilities.ctypes.c_int, -2
        )
        == -2
    )


def test_probe_allocator_backend_uses_fallback_for_loader_failure(
    import_allocator_module: Callable[[str], ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utilities = import_allocator_module("mooncake.fabric_allocator_utils")

    def fail_to_load(_path: str) -> object:
        raise OSError("not loadable")

    monkeypatch.setattr(utilities.ctypes, "CDLL", fail_to_load)

    assert (
        utilities.probe_allocator_backend(
            "allocator.so", "mc_allocator_probe", utilities.ctypes.c_int, False
        )
        is False
    )
