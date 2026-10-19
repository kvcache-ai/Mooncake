"""Exercise wheel dependency selection without a GPU build host."""

import os
from pathlib import Path
import re
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/_build-wheel.yaml").read_text())
STEPS = WORKFLOW["jobs"]["build"]["steps"]


@pytest.mark.parametrize("backend", ["OFF", "CUDA", "HIP"])
def test_store_staging_follows_selected_backend(tmp_path, backend):
    source = (ROOT / "mooncake-store/src/CMakeLists.txt").read_text()
    blocks = re.findall(
        r"if\((?:USE_CUDA|CUDAToolkit_FOUND)\).*?endif\(\)", source, re.S
    )
    assert len(blocks) == 4
    for name, target in [("CUDAToolkit", "CUDA::cudart"), ("hip", "hip::host")]:
        (tmp_path / f"Find{name}.cmake").write_text(
            f"set({name}_FOUND TRUE)\n" f"add_library({target} INTERFACE IMPORTED)\n"
        )
    (tmp_path / "empty.cpp").write_text("int main() { return 0; }\n")
    targets = ["mooncake_store_client_objects", "mooncake_store", "mooncake_client"]
    project = (
        "cmake_minimum_required(VERSION 3.20)\nproject(Staging CXX)\n"
        'list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")\n'
        # The builder has both SDKs available, even for the CPU variant.
        "set(CUDAToolkit_FOUND TRUE)\nset(hip_FOUND TRUE)\n"
        + "\n".join(f"add_library({target} STATIC empty.cpp)" for target in targets)
        + "\n"
        + "\n".join(blocks)
    )
    for target in targets:
        project += (
            f"\nget_target_property(defs {target} COMPILE_DEFINITIONS)\n"
            f"get_target_property(libs {target} LINK_LIBRARIES)\n"
            f'file(WRITE "${{CMAKE_BINARY_DIR}}/{target}.txt" "${{defs}}\n${{libs}}")\n'
        )
    (tmp_path / "CMakeLists.txt").write_text(project)
    subprocess.run(
        [
            "cmake",
            "-S",
            str(tmp_path),
            "-B",
            str(tmp_path / "build"),
            f"-DUSE_CUDA={'ON' if backend == 'CUDA' else 'OFF'}",
            f"-DUSE_HIP={'ON' if backend == 'HIP' else 'OFF'}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = "\n".join((tmp_path / "build" / f"{t}.txt").read_text() for t in targets)
    assert ("USE_CUDA" in metadata) == (backend == "CUDA")
    assert ("CUDA::cudart" in metadata) == (backend == "CUDA")
    assert ("USE_HIP" in metadata) == (backend == "HIP")
    assert ("hip::host" in metadata) == (backend == "HIP")


@pytest.mark.parametrize("architecture", ["x86_64", "arm64"])
@pytest.mark.parametrize("variant", ["cuda", "cuda13", "non-cuda"])
def test_ep_profiles_cover_current_torch_abi(tmp_path, architecture, variant):
    profile = next(
        step for step in STEPS if step.get("name") == "Configure build profile"
    )
    environment_file = tmp_path / "environment"
    subprocess.run(
        ["bash", "-e", "-c", profile["run"]],
        env={
            **os.environ,
            "BUILD_ARCHITECTURE": architecture,
            "BUILD_VARIANT": variant,
            "GITHUB_ENV": str(environment_file),
        },
        check=True,
        capture_output=True,
        text=True,
    )
    environment = environment_file.read_text()
    assert ("2.14.0" in environment) == (variant != "non-cuda")
    if variant == "non-cuda":
        assert "-DUSE_CUDA=OFF" in environment
        assert "-DWITH_EP=OFF" in environment
