"""Keep release CI on the backend-owned configure/build/install path."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import subprocess

import pytest


yaml = pytest.importorskip("yaml")


ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github/workflows"
RELEASE_BUILD_WORKFLOWS = (
    "_build-wheel.yaml",
    "_build-efa-wheel.yaml",
    "release-musa.yaml",
    "release-npu.yaml",
    "release-rocm.yaml",
    "ci_rocm.yml",
)


def build_steps(name):
    workflow = yaml.safe_load((WORKFLOWS / name).read_text())
    return next(job["steps"] for job in workflow["jobs"].values() if "steps" in job)


@pytest.mark.parametrize("name", RELEASE_BUILD_WORKFLOWS)
def test_release_build_is_owned_by_scikit_build(name):
    commands = "\n".join(step.get("run", "") for step in build_steps(name))
    assert "scripts/build_release_wheel.py" in commands
    assert not re.search(r"(?m)^\s*(?:sudo\s+)?(?:cmake|make)\s", commands)
    assert "nvlink-allocator" not in commands
    assert "-DPython3_EXECUTABLE" not in commands


@pytest.mark.parametrize("architecture", ["x86_64", "arm64"])
@pytest.mark.parametrize("variant", ["cuda", "cuda13", "non-cuda"])
def test_standard_release_profiles(tmp_path, architecture, variant):
    step = next(
        step
        for step in build_steps("_build-wheel.yaml")
        if step["name"] == "Configure build profile"
    )
    output = tmp_path / "environment"
    env = dict(
        os.environ,
        BUILD_ARCHITECTURE=architecture,
        BUILD_VARIANT=variant,
        GITHUB_ENV=str(output),
    )
    subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", step["run"]], env=env, check=True
    )
    profile = dict(line.split("=", 1) for line in output.read_text().splitlines())
    defines = dict(
        arg.removeprefix("-D").split("=", 1)
        for arg in shlex.split(profile["CMAKE_ARGS"])
    )
    assert defines["USE_HTTP"] == "ON"
    assert defines["ENABLE_SCCACHE"] == "ON"
    assert (
        profile["VARIANT_FLAG"]
        == {"cuda": "", "cuda13": "CU13_BUILD", "non-cuda": "NON_CUDA_BUILD"}[variant]
    )
    if variant == "non-cuda":
        assert not profile["EP_TORCH_VERSIONS"]
        assert not profile["TORCH_CUDA_ARCH_LIST"]
        assert "USE_CUDA" not in defines  # Backend default is OFF.
    else:
        assert defines["USE_CUDA"] == defines["WITH_EP"] == "ON"
        assert profile["EP_TORCH_VERSIONS"] == "2.11.0;2.12.0;2.12.1;2.13.0;2.14.0"
        expected_arches = "8.0;9.0" if architecture == "x86_64" else "9.0"
        if variant == "cuda13":
            expected_arches += ";10.3"
        assert profile["TORCH_CUDA_ARCH_LIST"] == expected_arches
        feature = "USE_INTRA_NVLINK" if architecture == "x86_64" else "USE_MNNVL"
        assert defines[feature] == "ON"
    if architecture == "x86_64" or variant == "non-cuda":
        assert defines["USE_ETCD"] == defines["STORE_USE_ETCD"] == "ON"
    else:
        assert "USE_ETCD" not in defines


@pytest.mark.parametrize(
    "architecture,variant", [("riscv64", "cuda"), ("x86_64", "unknown")]
)
def test_unknown_release_profile_is_rejected(tmp_path, architecture, variant):
    step = next(
        step
        for step in build_steps("_build-wheel.yaml")
        if step["name"] == "Configure build profile"
    )
    env = dict(
        os.environ,
        BUILD_ARCHITECTURE=architecture,
        BUILD_VARIANT=variant,
        GITHUB_ENV=str(tmp_path / "environment"),
    )
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", step["run"]],
        env=env,
        capture_output=True,
    )
    assert result.returncode != 0


def test_efa_validation_accepts_soabi_extension_names():
    step = next(
        step
        for step in build_steps("_build-efa-wheel.yaml")
        if step["name"] == "Verify CUDA runtime dependency"
    )
    assert "'mooncake/engine*.so'" in step["run"]
    assert 'readelf -d "${engines[0]}"' in step["run"]
