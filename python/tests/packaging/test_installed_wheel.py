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
        [str(python), "-m", "pip", "install", "--no-deps", str(wheel)],
        check=True,
    )

    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    clean_environment["PYTHONNOUSERSITE"] = "1"
    smoke_script = f"""
import importlib.util
from importlib import metadata
from pathlib import Path
import sys
import mooncake
import mooncake.engine
import mooncake.reshard
import mooncake.store

package_path = Path(mooncake.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
distribution_files = {{str(path) for path in metadata.files("mooncake-transfer-engine") or ()}}
assert not package_path.is_relative_to(repository_path), (package_path, repository_path)
assert metadata.version("mooncake-transfer-engine") == {_project_version()!r}
assert mooncake.BufferPool is mooncake.store.BufferPool
assert mooncake.engine.TransferEngine is not None
assert importlib.util.find_spec("numpy") is None
assert importlib.util.find_spec("PIL") is None
assert importlib.util.find_spec("mooncake.structured_object_store") is not None
assert "mooncake.structured_object_store" not in sys.modules
assert "mooncake/structured_object_store.py" in distribution_files
assert any(
    path.startswith("mooncake/_fast_copy.") and path.endswith(".so")
    for path in distribution_files
)
"""
    subprocess.run(
        [str(python), "-I", "-c", smoke_script],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )

    structured_requirement = f"mooncake-transfer-engine[structured] @ {wheel.as_uri()}"
    subprocess.run(
        [str(python), "-m", "pip", "install", structured_requirement],
        check=True,
    )
    structured_smoke_script = f"""
import ctypes
from pathlib import Path
import numpy as np
from PIL import Image
import mooncake.structured_object_store as structured_object_store
from mooncake._fast_copy import concat_arrays_into

module_path = Path(structured_object_store.__file__).resolve()
repository_path = Path({str(REPOSITORY_ROOT)!r}).resolve()
assert not module_path.is_relative_to(repository_path), (module_path, repository_path)
assert structured_object_store.MooncakeBundleTransfer is not None
assert Image is not None

source = np.arange(8, dtype=np.uint8)
destination = ctypes.create_string_buffer(source.nbytes)
copied = concat_arrays_into(
    [source], ctypes.addressof(destination), len(destination)
)
assert copied == source.nbytes
assert destination.raw == source.tobytes()
"""
    subprocess.run(
        [str(python), "-I", "-c", structured_smoke_script],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
    )
