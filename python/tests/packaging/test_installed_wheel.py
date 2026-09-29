from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys
import zipfile

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

    with zipfile.ZipFile(wheel) as archive:
        wheel_files = set(archive.namelist())
    assert "mooncake/store/__init__.py" in wheel_files
    assert not any(name.endswith("mooncake_store_rs.pth") for name in wheel_files)
    assert not any(name.startswith("mooncake_store_rs/") for name in wheel_files)
    assert not any("_shim.py" in name or "meta_path" in name for name in wheel_files)
    cpp_extensions = [
        name
        for name in wheel_files
        if name.startswith("mooncake/_store.") and name.endswith(".so")
    ]
    assert len(cpp_extensions) == 1
    rs_extensions = [
        name
        for name in wheel_files
        if name.startswith("mooncake/_store_rs.") and name.endswith(".so")
    ]
    assert len(rs_extensions) <= 1

    environment = tmp_path / "environment"
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    python = environment / "bin" / "python"
    subprocess.run(
        [str(python), "-m", "pip", "install", "aiohttp", "msgpack", "requests"],
        check=True,
    )
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        check=True,
    )

    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    clean_environment["PYTHONNOUSERSITE"] = "1"
    clean_environment.pop("MOONCAKE_STORE_BACKEND", None)
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
import mooncake._store as private_store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version(project_file)!r}
assert "administration" in metadata.metadata("mooncake-transfer-engine").get_all("Provides-Extra", [])
assert mooncake.store._BACKEND == "cpp"
assert mooncake.store.MooncakeDistributedStore is private_store.MooncakeDistributedStore
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.RegisteredBufferPool is mooncake.store.RegisteredBufferPool
from mooncake.buffer_pool import BufferPool, RegisteredBufferPool
assert BufferPool is mooncake.store.BufferPool
assert RegisteredBufferPool is mooncake.store.RegisteredBufferPool
assert not hasattr(mooncake.store, "_serialize_tensor")
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
if {project_file == "pyproject.toml"!r}:
    expected_entry_points.update({{
        "mooncake-store-client": "mooncake._launcher:store_rs_client",
        "mooncake-store-admin": "mooncake._launcher:store_rs_admin",
        "mooncake-store-bench": "mooncake._launcher:store_rs_bench",
    }})
assert {{name: entry_points[name] for name in expected_entry_points}} == expected_entry_points

for module in (
    "mooncake.mooncake_ssd_register",
    "mooncake.mooncake_ssd_unregister",
    "mooncake.spdk_tgt_create",
):
    import_module(module)
assert util.find_spec("paramiko") is None
assert "paramiko" not in sys.modules
for name, module in tuple(sys.modules.items()):
    if (name == "mooncake" or name.startswith("mooncake.")) and getattr(module, "__file__", None):
        assert Path(module.__file__).resolve().is_relative_to(package_path.parent), (
            name,
            module.__file__,
            package_path.parent,
        )

installed_files = {{str(path) for path in metadata.files("mooncake-transfer-engine") or []}}
assert {{
    "mooncake/_administration.py",
    "mooncake/mooncake_ssd_register.py",
    "mooncake/mooncake_ssd_unregister.py",
    "mooncake/spdk_tgt_create.py",
}} <= installed_files
    """
    for backend in (None, "cpp"):
        if backend is None:
            clean_environment.pop("MOONCAKE_STORE_BACKEND", None)
        else:
            clean_environment["MOONCAKE_STORE_BACKEND"] = backend
        subprocess.run(
            [str(python), "-I", "-c", smoke_script],
            cwd=tmp_path,
            env=clean_environment,
            check=True,
        )

    if rs_extensions:
        rs_environment = clean_environment.copy()
        rs_environment["MOONCAKE_STORE_BACKEND"] = "rs"
        rs_smoke = """
from pathlib import Path
import sys
import mooncake
import mooncake.store as store
import mooncake._store_rs as native
assert store._BACKEND == "rs"
assert store.MooncakeDistributedStore is store.rs.store.MooncakeDistributedStore
assert store.BufferPool is native.BufferPool
package = Path(mooncake.__file__).resolve().parent
for name, module in tuple(sys.modules.items()):
    if (name == "mooncake" or name.startswith("mooncake.")) and getattr(module, "__file__", None):
        assert Path(module.__file__).resolve().is_relative_to(package), (name, module.__file__)
"""
        subprocess.run(
            [str(python), "-I", "-c", rs_smoke],
            cwd=tmp_path,
            env=rs_environment,
            check=True,
        )
        cli = environment / "bin" / "mooncake-store-client"
        result = subprocess.run(
            [str(cli), "--help"],
            cwd=tmp_path,
            env=rs_environment,
            check=True,
            capture_output=True,
            text=True,
        )
        assert "Usage:" in result.stdout or "Usage:" in result.stderr
    else:
        missing_rs = clean_environment.copy()
        missing_rs["MOONCAKE_STORE_BACKEND"] = "rs"
        result = subprocess.run(
            [str(python), "-I", "-c", "import mooncake.store"],
            cwd=tmp_path,
            env=missing_rs,
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "requires a Mooncake wheel built with WITH_STORE_RS=ON" in result.stderr
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


def test_store_rs_backend_from_installed_root_wheel(tmp_path: Path) -> None:
    wheel_value = os.environ.get("MOONCAKE_TEST_STORE_RS_WHEEL")
    if not wheel_value:
        pytest.skip("set MOONCAKE_TEST_STORE_RS_WHEEL to a WITH_STORE_RS=ON root wheel")

    wheel = Path(wheel_value).resolve()
    assert wheel.is_file(), f"wheel does not exist: {wheel}"
    with zipfile.ZipFile(wheel) as archive:
        wheel_files = set(archive.namelist())
    rs_extensions = [
        name
        for name in wheel_files
        if name.startswith("mooncake/_store_rs.") and name.endswith(".so")
    ]
    assert len(rs_extensions) == 1
    assert not any(name.endswith("mooncake_store_rs.pth") for name in wheel_files)
    assert not any(name.startswith("mooncake_store_rs/") for name in wheel_files)

    environment = tmp_path / "rs-environment"
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    python = environment / "bin" / "python"
    subprocess.run(
        [str(python), "-m", "pip", "install", f"{wheel}[structured]"],
        check=True,
    )

    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    clean_environment["PYTHONNOUSERSITE"] = "1"
    clean_environment["MOONCAKE_STORE_BACKEND"] = "rs"
    smoke = """
import importlib
from importlib import metadata
import sys

import mooncake
import mooncake.store as store
import mooncake._store_rs as private_store
import mooncake.structured_object_store as public_structured
import mooncake.store.rs.structured_object_store as rs_structured
from mooncake.buffer_pool import BufferPool as module_buffer_pool

assert store._BACKEND == "rs"
assert store.MooncakeDistributedStore is store.rs.store.MooncakeDistributedStore
assert store.MooncakeDistributedStore is not private_store.MooncakeDistributedStore
assert store.BufferPool is private_store.BufferPool
assert mooncake.BufferPool is store.BufferPool
assert module_buffer_pool is store.BufferPool
assert store.RegisteredBufferPool is store.BufferPool
assert not hasattr(store, "EngramStore")
assert public_structured.MooncakeBundleTransfer is not rs_structured.MooncakeBundleTransfer
assert public_structured.MooncakeBundleTransfer.__module__ == "mooncake.structured_object_store"
assert "mooncake-store-client" in {
    item.name for item in metadata.entry_points(group="console_scripts")
}
"""
    subprocess.run(
        [str(python), "-I", "-c", smoke],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )

    cli = environment / "bin" / "mooncake-store-client"
    result = subprocess.run(
        [str(cli), "--help"],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Usage:" in result.stdout or "Usage:" in result.stderr

    for backend in ("", "other"):
        invalid_environment = clean_environment.copy()
        invalid_environment["MOONCAKE_STORE_BACKEND"] = backend
        result = subprocess.run(
            [str(python), "-I", "-c", "import mooncake.store"],
            cwd=tmp_path,
            env=invalid_environment,
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "must be exactly 'cpp' or 'rs'" in result.stderr

    engine_environment = clean_environment.copy()
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import sys, mooncake.engine; assert 'mooncake.store' not in sys.modules",
        ],
        cwd=tmp_path,
        env=engine_environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
