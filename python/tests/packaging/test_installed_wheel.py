from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from zipfile import ZipFile

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
ALLOCATOR_MODULES = (
    "allocator.py",
    "allocator_ascend_npu.py",
    "fabric_allocator_utils.py",
)


def _project_version() -> str:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib

    project = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())
    return project["project"]["version"]


def test_wheel_imports_outside_the_repository(tmp_path: Path) -> None:
    wheel_value = os.environ.get("MOONCAKE_TEST_WHEEL")
    if not wheel_value:
        pytest.skip("set MOONCAKE_TEST_WHEEL to run the installed-wheel smoke test")

    wheel = Path(wheel_value).resolve()
    assert wheel.is_file(), f"wheel does not exist: {wheel}"
    with ZipFile(wheel) as archive:
        members = set(archive.namelist())
    for module in ALLOCATOR_MODULES:
        assert f"mooncake/{module}" in members

    environment = tmp_path / "environment"
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    python = environment / "bin" / "python"
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        check=True,
    )

    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    clean_environment["PYTHONNOUSERSITE"] = "1"
    smoke_script = f"""
from importlib import metadata
from importlib.util import find_spec
from pathlib import Path
import sys
from types import ModuleType
import mooncake
import mooncake.engine
import mooncake.fabric_allocator_utils
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version()!r}
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.engine.TransferEngine is not None
for module in {ALLOCATOR_MODULES!r}:
    spec = find_spec(f"mooncake.{{module.removesuffix('.py')}}")
    assert spec is not None and spec.origin is not None
    assert Path(spec.origin).resolve().parent == package_path.parent
assert "torch" not in sys.modules
assert "torch_npu" not in sys.modules

class FakePluggableAllocator:
    pass

torch = ModuleType("torch")
torch.__path__ = []
torch.device = object
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
sys.modules.update({{
    "torch": torch,
    "torch.cuda": torch_cuda,
    "torch.cuda.memory": torch_cuda_memory,
    "torch_npu": torch_npu,
    "torch_npu.npu": torch_npu_npu,
    "torch_npu.npu.memory": torch_npu_memory,
}})

import mooncake.allocator
import mooncake.allocator_ascend_npu

assert mooncake.allocator.NVLinkAllocator is not None
assert mooncake.allocator_ascend_npu.UBShmemAllocator is not None
"""
    subprocess.run(
        [str(python), "-I", "-c", smoke_script],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )
