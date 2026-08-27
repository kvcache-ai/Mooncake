from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


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
from pathlib import Path
import sys
import mooncake

cli_modules = (
    "mooncake.cli",
    "mooncake.cli_client",
    "mooncake.cli_bench",
    "mooncake.transfer_engine_topology_dump",
)
assert all(module not in sys.modules for module in cli_modules)

import mooncake.cli
import mooncake.cli_bench
import mooncake.cli_client
import mooncake.transfer_engine_topology_dump

assert "mooncake.engine" not in sys.modules

import mooncake.engine
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version()!r}
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.engine.TransferEngine is not None
entry_points = {{
    entry_point.name: entry_point.value
    for entry_point in metadata.entry_points(group="console_scripts")
}}
expected_entry_points = {{
    "mooncake_master": "mooncake.cli:main",
    "mooncake_client": "mooncake.cli_client:main",
    "transfer_engine_bench": "mooncake.cli_bench:main",
    "transfer_engine_topology_dump": "mooncake.transfer_engine_topology_dump:main",
}}
assert {{name: entry_points[name] for name in expected_entry_points}} == expected_entry_points
"""
    subprocess.run(
        [str(python), "-I", "-c", smoke_script],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )

    package_query = "from pathlib import Path; import mooncake; print(Path(mooncake.__file__).parent)"
    package_result = subprocess.run(
        [str(python), "-I", "-c", package_query],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
        capture_output=True,
        text=True,
    )
    package_directory = Path(package_result.stdout.strip())
    fake_binary = "#!/bin/sh\nprintf '%s\\n' \"$*\"\n"
    for binary_name in ("mooncake_master", "mooncake_client", "transfer_engine_bench"):
        binary = package_directory / binary_name
        binary.write_text(fake_binary)
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    module_commands = {
        "mooncake_master": "mooncake.cli",
        "mooncake_client": "mooncake.cli_client",
        "transfer_engine_bench": "mooncake.cli_bench",
    }
    for command_name, module_name in module_commands.items():
        command_result = subprocess.run(
            [str(environment / "bin" / command_name), "console-script-smoke"],
            cwd=tmp_path,
            env=clean_environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert command_result.stdout.strip() == "console-script-smoke"

        module_result = subprocess.run(
            [str(python), "-I", "-m", module_name, "module-smoke"],
            cwd=tmp_path,
            env=clean_environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert module_result.stdout.strip() == "module-smoke"

    topology_commands = (
        [str(environment / "bin" / "transfer_engine_topology_dump"), "--help"],
        [
            str(python),
            "-I",
            "-m",
            "mooncake.transfer_engine_topology_dump",
            "--help",
        ],
    )
    for command in topology_commands:
        result = subprocess.run(
            command,
            cwd=tmp_path,
            env=clean_environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert "Dump device topology" in result.stdout
