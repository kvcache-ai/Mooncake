from __future__ import annotations

import os
from pathlib import Path
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
import mooncake.engine
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version(project_file)!r}
assert "administration" in metadata.metadata("mooncake-transfer-engine").get_all("Provides-Extra", [])
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.engine.TransferEngine is not None
for ep_module in (
    "ep.py",
    "mooncake_ep_buffer.py",
    "mooncake_elastic_buffer.py",
):
    assert (package_path.parent / ep_module).is_file(), ep_module
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
