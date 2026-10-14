from __future__ import annotations

import importlib
from importlib.machinery import ModuleSpec
from pathlib import Path
import sys
from types import ModuleType
from typing import Callable

import pytest


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "mooncake"


class FakePluggableAllocator:
    created: list[tuple[str, str, str]] = []

    def __init__(self, so_path: str, malloc_symbol: str, free_symbol: str):
        self.arguments = (so_path, malloc_symbol, free_symbol)
        self.created.append(self.arguments)


@pytest.fixture
def import_allocator_module(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[str], ModuleType]:
    package = ModuleType("mooncake")
    package.__path__ = [str(PACKAGE_ROOT)]
    package_spec = ModuleSpec("mooncake", loader=None, is_package=True)
    package_spec.submodule_search_locations = package.__path__
    package.__spec__ = package_spec

    class FakeDevice:
        pass

    torch = ModuleType("torch")
    torch.__path__ = []
    torch.device = FakeDevice
    torch_cuda = ModuleType("torch.cuda")
    torch_cuda.__path__ = []
    torch_cuda_memory = ModuleType("torch.cuda.memory")
    torch_cuda_memory.CUDAPluggableAllocator = FakePluggableAllocator

    torch_npu = ModuleType("torch_npu")
    torch_npu.__path__ = []
    torch_npu_npu = ModuleType("torch_npu.npu")
    torch_npu_npu.__path__ = []
    torch_npu_memory = ModuleType("torch_npu.npu.memory")
    torch_npu_memory.NPUPluggableAllocator = FakePluggableAllocator

    modules = {
        "mooncake": package,
        "torch": torch,
        "torch.cuda": torch_cuda,
        "torch.cuda.memory": torch_cuda_memory,
        "torch_npu": torch_npu,
        "torch_npu.npu": torch_npu_npu,
        "torch_npu.npu.memory": torch_npu_memory,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    FakePluggableAllocator.created.clear()

    def load(module_name: str) -> ModuleType:
        for name in (
            "mooncake.allocator",
            "mooncake.allocator_ascend_npu",
            "mooncake.fabric_allocator_utils",
        ):
            monkeypatch.delitem(sys.modules, name, raising=False)
        return importlib.import_module(module_name)

    return load
