from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _project_version(project_file: str) -> str:
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python 3.10
        import tomli as tomllib

    project = tomllib.loads((REPOSITORY_ROOT / project_file).read_text())
    return project["project"]["version"]


@pytest.mark.parametrize(
    ("wheel_variable", "project_file"),
    [
        ("MOONCAKE_TEST_WHEEL", "pyproject.toml"),
        ("MOONCAKE_TEST_LEGACY_WHEEL", "mooncake-wheel/pyproject.toml"),
    ],
    ids=["unified", "legacy"],
)
def test_wheel_imports_outside_the_repository(
    tmp_path: Path, wheel_variable: str, project_file: str
) -> None:
    wheel_value = os.environ.get(wheel_variable)
    if not wheel_value:
        pytest.skip(f"set {wheel_variable} to run the installed-wheel smoke test")

    wheel = Path(wheel_value).resolve()
    assert wheel.is_file(), f"wheel does not exist: {wheel}"

    environment = tmp_path / "environment"
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    python = environment / "bin" / "python"
    subprocess.run(
        [str(python), "-m", "pip", "install", "aiohttp"],
        check=True,
    )
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        check=True,
    )

    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    clean_environment["PYTHONNOUSERSITE"] = "1"
    smoke_script = f"""
from importlib import import_module, metadata, util
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

import mooncake.async_store
import mooncake.engine
import mooncake.http_metadata_server
import mooncake.mooncake_config
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version(project_file)!r}
assert "administration" in metadata.metadata("mooncake-transfer-engine").get_all("Provides-Extra", [])
assert mooncake.BufferPool is mooncake.store.BufferPool
assert issubclass(
    mooncake.async_store.MooncakeDistributedStoreAsync,
    mooncake.store.MooncakeDistributedStore,
)
assert Path(mooncake.async_store.__file__).resolve().parent == package_path.parent
assert mooncake.engine.TransferEngine is not None
assert mooncake.http_metadata_server.KVBootstrapServer is not None
assert mooncake.mooncake_config.MooncakeConfig is not None
for ep_module in (
    "ep.py",
    "mooncake_ep_buffer.py",
    "mooncake_elastic_buffer.py",
):
    assert (package_path.parent / ep_module).is_file(), ep_module
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

for module in (
    "mooncake.mooncake_ssd_register",
    "mooncake.mooncake_ssd_unregister",
    "mooncake.spdk_tgt_create",
):
    import_module(module)
assert util.find_spec("paramiko") is None
assert "paramiko" not in sys.modules

installed_files = {{str(path) for path in metadata.files("mooncake-transfer-engine") or []}}
assert {{
    "mooncake/_administration.py",
    "mooncake/mooncake_ssd_register.py",
    "mooncake/mooncake_ssd_unregister.py",
    "mooncake/spdk_tgt_create.py",
}} <= installed_files
"""
    subprocess.run(
        [str(python), "-I", "-c", smoke_script],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )
    subprocess.run(
        [str(environment / "bin" / "mooncake_http_metadata_server"), "--help"],
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

    def check_help():
        for module in (
            "mooncake.mooncake_ssd_register",
            "mooncake.mooncake_ssd_unregister",
            "mooncake.spdk_tgt_create",
        ):
            result = subprocess.run(
                [str(python), "-I", "-m", module, "--help"],
                cwd=tmp_path,
                env=clean_environment,
                check=True,
                capture_output=True,
                text=True,
            )
            assert "usage:" in result.stdout

    check_help()
    subprocess.run(
        [str(python), "-m", "pip", "install", f"{wheel}[administration]"],
        check=True,
    )
    subprocess.run(
        [str(python), "-I", "-c", "import paramiko; assert paramiko.SSHClient"],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )
    check_help()
