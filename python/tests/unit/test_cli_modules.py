from __future__ import annotations

import importlib.util
from pathlib import Path
import stat
import subprocess
import sys
from types import ModuleType, SimpleNamespace

import pytest


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "mooncake"


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


@pytest.mark.parametrize(
    ("filename", "binary_name", "uses_exec"),
    [
        ("cli.py", "mooncake_master", True),
        ("cli_client.py", "mooncake_client", True),
        ("cli_bench.py", "transfer_engine_bench", False),
    ],
)
def test_binary_wrappers_forward_arguments_and_restore_execute_bits(
    monkeypatch: pytest.MonkeyPatch,
    filename: str,
    binary_name: str,
    uses_exec: bool,
) -> None:
    module = _load_module(filename)
    expected_binary = str(PACKAGE_ROOT / binary_name)
    original_mode = stat.S_IFREG | 0o640
    chmod_calls: list[tuple[str, int]] = []
    runner_calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(module.sys, "argv", [filename, "--flag", "value"])
    monkeypatch.setattr(module.os, "access", lambda _path, _mode: False)
    monkeypatch.setattr(
        module.os, "stat", lambda _path: SimpleNamespace(st_mode=original_mode)
    )
    monkeypatch.setattr(
        module.os,
        "chmod",
        lambda path, mode: chmod_calls.append((path, mode)),
    )

    expected_arguments = [expected_binary, "--flag", "value"]
    if uses_exec:
        monkeypatch.setattr(
            module.os,
            "execv",
            lambda path, args: runner_calls.append((path, args)),
        )
        assert module.main() is None
    else:
        monkeypatch.setattr(
            module.subprocess,
            "call",
            lambda args: runner_calls.append((args[0], args)) or 17,
        )
        assert module.main() == 17

    execute_bits = stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    assert chmod_calls == [(expected_binary, original_mode | execute_bits)]
    assert runner_calls == [(expected_binary, expected_arguments)]


def test_topology_cli_import_keeps_native_backends_lazy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delitem(sys.modules, "mooncake.engine", raising=False)
    monkeypatch.delitem(sys.modules, "tent", raising=False)

    module = _load_module("transfer_engine_topology_dump.py")

    assert module.resolve_backend("te") == "te"
    assert "mooncake.engine" not in sys.modules
    assert "tent" not in sys.modules


def test_topology_cli_runs_from_the_canonical_source_tree() -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(PACKAGE_ROOT / "transfer_engine_topology_dump.py"),
            "--help",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Dump device topology" in result.stdout
    assert "--custom-topo-json" in result.stdout
