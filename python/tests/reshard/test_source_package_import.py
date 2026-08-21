from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def _repository_root() -> Path:
    for directory in Path(__file__).resolve().parents:
        if (directory / "pyproject.toml").is_file() and (
            directory / "python" / "mooncake" / "reshard"
        ).is_dir():
            return directory
    raise AssertionError("repository root not found")


def test_source_tree_package_coexists_with_installed_mooncake() -> None:
    repo_root = _repository_root()
    source_root = repo_root / "python"
    wheel_root = repo_root / "mooncake-wheel"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(wheel_root), str(source_root)))
    env["PYTHONNOUSERSITE"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import mooncake; import mooncake.reshard; "
                "assert len(tuple(mooncake.__path__)) >= 2; "
                f"assert mooncake.reshard.__file__.startswith({str(source_root)!r})"
            ),
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
