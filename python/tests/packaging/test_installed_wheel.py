from __future__ import annotations

import os
from pathlib import Path
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
from importlib import metadata
from pathlib import Path
import mooncake
import mooncake.engine
import mooncake.http_metadata_server
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version()!r}
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.engine.TransferEngine is not None
assert mooncake.http_metadata_server.KVBootstrapServer is not None
for ep_module in (
    "ep.py",
    "mooncake_ep_buffer.py",
    "mooncake_elastic_buffer.py",
):
    assert (package_path.parent / ep_module).is_file(), ep_module
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
