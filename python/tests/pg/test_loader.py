from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PG_LOADER = REPOSITORY_ROOT / "python" / "mooncake" / "pg.py"


def _load_pg(monkeypatch: pytest.MonkeyPatch, torch_version: str) -> ModuleType:
    package = ModuleType("mooncake")
    package.__path__ = []
    monkeypatch.setitem(sys.modules, "mooncake", package)

    torch = ModuleType("torch")
    torch.__version__ = torch_version
    monkeypatch.setitem(sys.modules, "torch", torch)

    spec = importlib.util.spec_from_file_location("mooncake.pg", PG_LOADER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_loader_selects_the_torch_abi_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = ModuleType("mooncake.pg_2_7_1")
    exported = object()
    backend.exported = exported
    backend._private = object()
    monkeypatch.setitem(sys.modules, backend.__name__, backend)

    loader = _load_pg(monkeypatch, "2.7.1+cu128")

    assert loader.torch_version == "2.7.1"
    assert loader.version_suffix == "_2_7_1"
    assert loader.exported is exported
    assert not hasattr(loader, "_private")


def test_loader_reports_an_unsupported_torch_abi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(
        ImportError,
        match=r"Mooncake PG was not built against torch==2\.8\.0",
    ):
        _load_pg(monkeypatch, "2.8.0")
