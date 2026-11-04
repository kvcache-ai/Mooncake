from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

# Stub for the Go toolchain: the behavior under test is how the CMake wrapper
# recipes drive the shell, not the Go build itself. ``go mod tidy`` creates
# go.sum (as the real command does) and ``go build`` writes the shared library
# plus its generated header at the ``-o`` path.
STUB_GO = """#!/bin/bash
if [ "$1" = "mod" ]; then
  [ -f go.sum ] || : > go.sum
  exit 0
fi
if [ "$1" = "build" ]; then
  shift
  out=""
  while [ $# -gt 0 ]; do
    if [ "$1" = "-o" ]; then out="$2"; shift 2; continue; fi
    shift
  done
  : > "$out"
  : > "${out%.so}.h"
fi
exit 0
"""

WRAPPER_CASES = (
    ("etcd", "build_etcd_wrapper", "libetcd_wrapper.h"),
    ("k8s-lease", "build_k8s_lease_wrapper", "libk8s_lease_wrapper.h"),
)


@pytest.mark.skipif(shutil.which("cmake") is None, reason="cmake not available")
@pytest.mark.parametrize("subdir,target,header", WRAPPER_CASES)
def test_go_wrapper_header_copied_to_source_dir(
    tmp_path: Path, subdir: str, target: str, header: str
) -> None:
    """Building a Go wrapper target must copy the generated header into the
    source directory; downstream CMake code includes it from there."""
    source_dir = tmp_path / subdir
    shutil.copytree(REPOSITORY_ROOT / "mooncake-common" / subdir, source_dir)
    (source_dir / header).unlink(missing_ok=True)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "go"
    stub.write_text(STUB_GO)
    stub.chmod(0o755)

    (tmp_path / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.16)\n"
        "project(wrapper_recipe_test NONE)\n"
        f"add_subdirectory({subdir})\n"
    )

    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    subprocess.run(
        [
            "cmake",
            "-S",
            str(tmp_path),
            "-B",
            str(tmp_path / "build"),
            "-G",
            "Unix Makefiles",
        ],
        check=True,
        env=env,
    )
    subprocess.run(
        ["cmake", "--build", str(tmp_path / "build"), "--target", target],
        check=True,
        env=env,
    )

    assert (
        source_dir / header
    ).is_file(), f"{header} was not copied into {source_dir} after building {target}"
