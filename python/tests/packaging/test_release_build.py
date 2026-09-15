from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "release_build", ROOT / "scripts/build_release_wheel.py"
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
PROJECT_TEXT = (ROOT / "pyproject.toml").read_text()


@pytest.mark.parametrize("flag", [None, *release.VARIANTS])
def test_release_metadata_uses_root_project(flag):
    env = {flag: "1"} if flag else {}
    text, defines = release.release_metadata(PROJECT_TEXT, env)
    project = tomllib.loads(text)
    suffix = f"-{release.VARIANTS[flag][0]}" if flag else ""
    assert project["project"]["name"] == f"mooncake-transfer-engine{suffix}"
    assert project["build-system"]["build-backend"] == "scikit_build_core.build"
    assert project["tool"] == tomllib.loads(PROJECT_TEXT)["tool"]
    assert (
        project["project"]["scripts"]
        == tomllib.loads(PROJECT_TEXT)["project"]["scripts"]
    )
    if flag in {"NPU_BUILD", "MUSA_BUILD"}:
        assert project["project"]["requires-python"] == ">=3.9"
        assert (
            "typing_extensions>=4.0; python_version < '3.10'"
            in project["project"]["dependencies"]
        )
    if suffix in {"-non-cuda", "-efa-non-cuda", "-npu", "-musa", "-rocm"}:
        assert defines["USE_CUDA"] == "OFF"
        assert (
            "Environment :: GPU :: NVIDIA CUDA" not in project["project"]["classifiers"]
        )


def test_output_requires_explicit_overwrite(tmp_path):
    wheel = tmp_path / "previous.whl"
    wheel.write_bytes(b"previous or failed-repair wheel")
    unrelated = tmp_path / "build.log"
    unrelated.write_text("keep")
    with pytest.raises(ValueError, match="--overwrite"):
        release.prepare_output_dir(tmp_path)
    assert wheel.exists()
    release.prepare_output_dir(tmp_path, overwrite=True)
    assert not wheel.exists()
    assert unrelated.read_text() == "keep"
    release.prepare_output_dir(tmp_path)


def test_conflicting_variants_fail():
    with pytest.raises(ValueError, match="Only one"):
        release.release_metadata(PROJECT_TEXT, {"HIP_BUILD": "1", "CU13_BUILD": "1"})


def test_explicit_version_override():
    text, _ = release.release_metadata(
        PROJECT_TEXT, {"MOONCAKE_WHEEL_VERSION": "0.3.14.dev20260601"}
    )
    assert tomllib.loads(text)["project"]["version"] == "0.3.14.dev20260601"
    text, _ = release.release_metadata(PROJECT_TEXT, {"VERSION": "main"})
    assert text == PROJECT_TEXT


def test_metadata_restored_on_build_failure(tmp_path):
    path = tmp_path / "pyproject.toml"
    original = PROJECT_TEXT.encode()
    path.write_bytes(original)

    def failing_build() -> None:
        with release.metadata_override(path, "temporary metadata"):
            assert path.read_text() == "temporary metadata"
            raise RuntimeError("build failed")

    with pytest.raises(RuntimeError):
        failing_build()
    assert path.read_bytes() == original


@pytest.mark.parametrize("build_type", [None, "", "Debug", "Release"])
def test_cached_build_type_preserves_backend_default(tmp_path, build_type):
    if build_type is not None:
        (tmp_path / "CMakeCache.txt").write_text(
            f"CMAKE_BUILD_TYPE:STRING={build_type}\n"
        )
    settings = release.build_settings(tomllib.loads(PROJECT_TEXT), tmp_path, {})
    build_type_settings = [
        setting
        for setting in settings
        if setting.startswith("--config-setting=cmake.build-type=")
    ]
    assert build_type_settings == (
        [f"--config-setting=cmake.build-type={build_type}"] if build_type else []
    )


def test_configured_native_profile_is_preserved(tmp_path):
    (tmp_path / "CMakeCache.txt").write_text(
        "USE_CUDA:BOOL=ON\nWITH_EP:BOOL=ON\nWITH_STORE_RUST:BOOL=ON\n"
        "CMAKE_GENERATOR:INTERNAL=Unix Makefiles\nCMAKE_BUILD_TYPE:STRING=Release\n"
    )
    settings = release.build_settings(
        tomllib.loads(PROJECT_TEXT), tmp_path, {"USE_CUDA": "OFF"}
    )
    assert "--config-setting=cmake.define.WITH_EP=ON" in settings
    assert "--config-setting=cmake.define.WITH_STORE_RUST=ON" in settings
    assert "--config-setting=cmake.define.USE_CUDA=OFF" in settings
    assert "--config-setting=cmake.args=-GUnix Makefiles" in settings
    assert "--config-setting=cmake.build-type=Release" in settings


@pytest.mark.parametrize("build_fails", [False, True])
def test_release_invokes_root_backend_and_restores_metadata(
    tmp_path, monkeypatch, build_fails
):
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text(PROJECT_TEXT)
    build_dir = tmp_path / "native"
    build_dir.mkdir()
    (build_dir / "CMakeCache.txt").write_text("USE_CUDA:BOOL=OFF\n")
    monkeypatch.setattr(release, "ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_release_wheel.py",
            "--build-dir",
            str(build_dir),
            "--output-dir",
            str(tmp_path / "dist"),
        ],
    )
    for flag in release.VARIANTS:
        monkeypatch.delenv(flag, raising=False)
    monkeypatch.setenv("NON_CUDA_BUILD", "1")
    monkeypatch.setenv("CMAKE_ARGS", "-DUSE_CUDA=OFF")
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert kwargs["env"]["CMAKE_ARGS"] == "-DUSE_CUDA=OFF"
        if command[1:3] == ["-m", "build"]:
            assert command[-1] == str(tmp_path)
            assert "--no-isolation" in command
            assert tomllib.loads(metadata.read_text())["project"]["name"].endswith(
                "-non-cuda"
            )
            if build_fails:
                raise subprocess.CalledProcessError(1, command)
        if command[0] == "bash":
            assert metadata.read_text() == PROJECT_TEXT
            assert command == [
                "bash",
                str(tmp_path / "scripts/repair_wheel.sh"),
                sys.executable,
                str(tmp_path / "dist"),
                str(build_dir),
            ]
            assert kwargs["cwd"] == tmp_path
            assert int(kwargs["env"]["CMAKE_BUILD_PARALLEL_LEVEL"]) > 0

    monkeypatch.setattr(release.subprocess, "run", run)
    if build_fails:
        with pytest.raises(subprocess.CalledProcessError):
            release.main()
        assert len(calls) == 3  # Never repair a failed build.
    else:
        release.main()
        assert len(calls) == 4
        assert calls[-1][0] == "bash"
    assert calls[0][1:4] == ["-m", "pip", "install"]
    assert "auditwheel" in calls[0] and "patchelf" in calls[0]
    assert "wheel>=0.45.1" in calls[0]
    assert calls[1][4:] == tomllib.loads(PROJECT_TEXT)["build-system"]["requires"]
    assert metadata.read_text() == PROJECT_TEXT


@pytest.mark.parametrize("output", [None, "dist-py313", "/tmp/release-output-test"])
def test_release_environment_paths_are_independent_of_cwd(
    tmp_path, monkeypatch, output
):
    (tmp_path / "pyproject.toml").write_text(PROJECT_TEXT)
    monkeypatch.setattr(release, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["build_release_wheel.py"])
    monkeypatch.setenv("BUILD_DIR", "native")
    monkeypatch.setenv("CMAKE_BUILD_PARALLEL_LEVEL", "2")
    monkeypatch.delenv("CMAKE_ARGS", raising=False)
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    for flag in release.VARIANTS:
        monkeypatch.delenv(flag, raising=False)
    if output:
        # Keep the absolute-path case isolated inside pytest's temporary tree.
        if output.startswith("/"):
            output = str(tmp_path / "absolute-output")
        monkeypatch.setenv("OUTPUT_DIR", output)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert kwargs["env"]["CMAKE_BUILD_PARALLEL_LEVEL"] == "2"

    monkeypatch.setattr(release.subprocess, "run", run)
    release.main()
    expected_output = tmp_path / "mooncake-wheel" / (output or "dist")
    assert calls[-1][-2:] == [str(expected_output), str(tmp_path / "native")]
    assert expected_output.is_dir()
    assert not any("cmake.define.USE_CUDA=ON" in arg for arg in calls[2])


@pytest.mark.parametrize("succeeds", [False, True])
def test_fresh_build_forwards_backend_options_and_retries(
    tmp_path, monkeypatch, succeeds
):
    metadata = tmp_path / "pyproject.toml"
    metadata.write_text(PROJECT_TEXT)
    monkeypatch.setattr(release, "ROOT", tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["build_release_wheel.py", "--build-attempts", "3"]
    )
    monkeypatch.delenv("OUTPUT_DIR", raising=False)
    monkeypatch.delenv("BUILD_DIR", raising=False)
    cmake_args = '-DUSE_HTTP=ON -DEP_TORCH_VERSIONS="2.11.0;2.12.0" -DCMAKE_PREFIX_PATH="/path with spaces"'
    monkeypatch.setenv("CMAKE_ARGS", cmake_args)
    monkeypatch.setenv("CMAKE_GENERATOR", "Ninja")
    attempts = []
    repairs = []
    sleeps = []

    def run(command, **kwargs):
        assert kwargs["env"]["CMAKE_ARGS"] == cmake_args
        assert kwargs["env"]["CMAKE_GENERATOR"] == "Ninja"
        if command[1:3] == ["-m", "build"]:
            attempts.append(command)
            if not succeeds or len(attempts) < 3:
                raise subprocess.CalledProcessError(1, command)
        elif command[0] == "bash":
            repairs.append(command)

    monkeypatch.setattr(release.subprocess, "run", run)
    monkeypatch.setattr(release.time, "sleep", sleeps.append)
    assert not (tmp_path / "build/CMakeCache.txt").exists()
    if succeeds:
        release.main()
    else:
        with pytest.raises(subprocess.CalledProcessError):
            release.main()
    assert len(attempts) == 3
    assert len(repairs) == int(succeeds)
    assert sleeps == [15, 15]
    assert metadata.read_text() == PROJECT_TEXT


def test_legacy_backend_is_removed():
    assert not (ROOT / "mooncake-wheel/setup.py").exists()
    assert not (ROOT / "mooncake-wheel/pyproject.toml").exists()
    assert not (ROOT / "scripts/build_wheel.sh").exists()


def test_repair_keeps_cuda_payload_out_of_auditwheel(tmp_path):
    pytest.importorskip("wheel")
    package = tmp_path / "package"
    (package / "mooncake").mkdir(parents=True)
    payload = b"CUDA fatbin: must remain byte-identical"
    (package / "mooncake/libmooncake_ep_device.so").write_bytes(payload)
    (package / "mooncake/__init__.py").write_text("")
    executable = package / "mooncake/mooncake_master"
    executable.write_text("#!/bin/sh\necho smoke-test\n")
    executable.chmod(0o755)
    metadata = package / "mooncake_transfer_engine-0.0.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: mooncake-transfer-engine\nVersion: 0.0.0\n"
    )
    (metadata / "WHEEL").write_text(
        "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: false\nTag: cp310-cp310-linux_x86_64\n"
    )
    output = tmp_path / "dist"
    output.mkdir()
    subprocess.run(
        [sys.executable, "-m", "wheel", "pack", str(package), "-d", str(output)],
        check=True,
    )
    staging = tmp_path / "build/ep_pg_staging"
    staging.mkdir(parents=True)
    (staging / "libmooncake_ep_device.so").write_bytes(payload)
    # Only mock auditwheel; wheel unpack/pack and RECORD regeneration are real.
    python = tmp_path / "python"
    python.write_text(
        f"#!{sys.executable}\n"
        "import os, pathlib, shutil, sys, zipfile\n"
        "if sys.argv[1:3] == ['-m', 'auditwheel']:\n"
        "    wheel = pathlib.Path(sys.argv[4])\n"
        "    with zipfile.ZipFile(wheel) as archive:\n"
        "        assert not any('device.so' in name for name in archive.namelist())\n"
        "    assert 'libmooncake_ep_device.so*' in sys.argv\n"
        "    assert '/opt/conda/lib' in os.environ['LD_LIBRARY_PATH'].split(':')\n"
        "    shutil.copy(wheel, sys.argv[sys.argv.index('-w') + 1])\n"
        "else:\n"
        f"    os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])\n"
    )
    python.chmod(0o755)
    env = os.environ.copy()
    env["NPU_BUILD"] = "0"
    subprocess.run(
        [
            "bash",
            str(ROOT / "scripts/repair_wheel.sh"),
            str(python),
            str(output),
            str(staging.parent),
        ],
        check=True,
        env=env,
    )
    wheels = list(output.glob("*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as archive:
        assert archive.read("mooncake/libmooncake_ep_device.so") == payload
        assert archive.getinfo("mooncake/mooncake_master").external_attr >> 16 & 0o111
    subprocess.run(
        [
            sys.executable,
            "-m",
            "wheel",
            "unpack",
            str(wheels[0]),
            "-d",
            str(tmp_path / "verified"),
        ],
        check=True,
    )
    installed = tmp_path / "installed"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(installed),
            str(wheels[0]),
            "--platform",
            "linux_x86_64",
            "--python-version",
            "3.10",
            "--implementation",
            "cp",
            "--abi",
            "cp310",
            "--only-binary=:all:",
        ],
        check=True,
    )
    result = subprocess.run(
        [str(installed / "mooncake/mooncake_master")],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout == "smoke-test\n"
