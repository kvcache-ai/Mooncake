from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import stat
import sys
from types import ModuleType, SimpleNamespace

import pytest


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "python" / "mooncake"


@pytest.fixture
def mooncake_pkg(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load the canonical wrappers without requiring an installed native backend."""
    pkg = ModuleType("mooncake")
    pkg.__path__ = [str(PACKAGE_ROOT)]
    pkg.__package__ = "mooncake"
    monkeypatch.setitem(sys.modules, "mooncake", pkg)
    return pkg


def _load_module(filename: str) -> ModuleType:
    module_path = PACKAGE_ROOT / filename
    spec = importlib.util.spec_from_file_location(
        f"_mooncake_cli_test_{module_path.stem}", module_path
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CLI_CASES = [
    ("cli.py", "mooncake_master"),
    ("cli_client.py", "mooncake_client"),
]


@pytest.mark.parametrize(("filename", "binary_name"), _CLI_CASES)
def test_cli_wrappers_forward_arguments_via_launcher(
    monkeypatch: pytest.MonkeyPatch,
    mooncake_pkg: ModuleType,
    filename: str,
    binary_name: str,
) -> None:
    module = _load_module(filename)
    located = f"/pkg/mooncake/{binary_name}"
    locate_calls: list[str] = []
    runner_calls: list[tuple[str, list[str]]] = []

    def fake_locate(name: str) -> str:
        locate_calls.append(name)
        return located

    monkeypatch.setattr(module, "locate", fake_locate)
    monkeypatch.setattr(module.sys, "argv", [filename, "--flag", "value"])

    expected_arguments = [located, "--flag", "value"]
    monkeypatch.setattr(
        module.os, "execv", lambda path, args: runner_calls.append((path, args))
    )
    assert module.main() is None

    assert locate_calls == [binary_name]
    assert runner_calls == [(located, expected_arguments)]


def test_launcher_locates_binary_and_sets_exec_bit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = _load_module("_launcher.py")

    class FakePath:
        def __init__(self, path: str) -> None:
            self.path = path

        def __truediv__(self, name: str) -> "FakePath":
            return FakePath(os.path.join(self.path, name))

        def is_file(self) -> bool:
            return True

        def __str__(self) -> str:
            return self.path

        def __fspath__(self) -> str:
            return self.path

    chmod_calls: list[tuple[str, int]] = []
    mode = stat.S_IFREG | 0o640
    monkeypatch.setattr(launcher, "files", lambda package: FakePath("/pkg/mooncake"))
    monkeypatch.setattr(launcher.os, "access", lambda _path, _mode: False)
    monkeypatch.setattr(
        launcher.os, "stat", lambda _path: SimpleNamespace(st_mode=mode)
    )
    monkeypatch.setattr(
        launcher.os, "chmod", lambda path, m: chmod_calls.append((path, m))
    )

    assert launcher.locate("mooncake_master") == "/pkg/mooncake/mooncake_master"
    expected_bits = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    assert chmod_calls == [("/pkg/mooncake/mooncake_master", mode | expected_bits)]


def test_launcher_raises_when_binary_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = _load_module("_launcher.py")

    class FakePath:
        def __truediv__(self, name: str) -> "FakePath":
            return FakePath()

        def is_file(self) -> bool:
            return False

    monkeypatch.setattr(launcher, "files", lambda package: FakePath())

    with pytest.raises(FileNotFoundError):
        launcher.locate("mooncake_master")
