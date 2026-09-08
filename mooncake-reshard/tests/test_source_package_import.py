from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def test_source_tree_package_coexists_with_installed_mooncake() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    source_root = repo_root / "mooncake-reshard" / "python"
    wheel_root = repo_root / "mooncake-wheel"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join((str(source_root), str(wheel_root)))

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import mooncake; import mooncake.reshard; "
                "assert len(tuple(mooncake.__path__)) >= 2"
            ),
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
