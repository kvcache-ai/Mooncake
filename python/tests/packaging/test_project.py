from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def test_scikit_build_core_is_the_only_build_backend() -> None:
    project = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())

    assert project["build-system"]["build-backend"] == "scikit_build_core.build"
    assert project["tool"]["scikit-build"]["wheel"]["packages"] == ["python/mooncake"]
    assert project["tool"]["scikit-build"]["install"]["components"] == ["python"]
    assert project["tool"]["scikit-build"]["sdist"]["include"] == [
        "extern/yalantinglibs/**",
        "!extern/yalantinglibs/.git",
    ]
    assert project["tool"]["scikit-build"]["sdist"]["exclude"] == [
        "extern/yalantinglibs/.git"
    ]
    assert "setuptools>=61" in project["build-system"]["requires"]
    assert "pip>=23" in project["build-system"]["requires"]
    assert project["tool"]["scikit-build"]["cmake"]["define"]["USE_CUDA"] is False
    assert project["tool"]["scikit-build"]["cmake"]["define"]["WITH_EP"] is False


def test_dependency_boundaries_are_declared() -> None:
    project = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text())
    metadata = project["project"]

    assert set(metadata["dependencies"]) == {"aiohttp", "msgpack", "requests"}
    assert set(metadata["optional-dependencies"]) == {
        "administration",
        "dev",
        "hardware",
        "structured",
        "vllm",
    }


def test_tracked_source_roots_contain_no_generated_native_artifacts() -> None:
    package_root = REPOSITORY_ROOT / "python" / "mooncake"

    assert (package_root / "__init__.py").is_file()
    assert not list(package_root.rglob("*.so"))
    assert not list((REPOSITORY_ROOT / "mooncake-pg" / "torch").rglob("*.so"))


def test_lightweight_modules_have_one_authoritative_source() -> None:
    package_root = REPOSITORY_ROOT / "python" / "mooncake"
    legacy_package_root = REPOSITORY_ROOT / "mooncake-wheel" / "mooncake"

    for module in ("buffer_pool.py", "mooncake_config.py"):
        assert (package_root / module).is_file()
        assert not (legacy_package_root / module).exists()

    assert (
        REPOSITORY_ROOT / "python" / "tests" / "unit" / "test_mooncake_config.py"
    ).is_file()
    assert (
        REPOSITORY_ROOT / "python" / "tests" / "store" / "test_buffer_pool_native.py"
    ).is_file()
    assert not (
        REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "test_mooncake_config.py"
    ).exists()
    assert not (
        REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "test_buffer_pool_native.py"
    ).exists()


def test_pg_extension_build_stages_outside_the_source_tree(
    tmp_path: Path,
) -> None:
    cmake = shutil.which("cmake")
    if cmake is None:
        pytest.skip("CMake is required to exercise the PG staging script")

    source = tmp_path / "source" / "mooncake-pg" / "torch"
    common = tmp_path / "source" / "mooncake-common"
    source.mkdir(parents=True)
    common.mkdir(parents=True)
    (common / "SetupPyTorchEnv.cmake").write_text("")
    (source / "setup.py").write_text(
        """\
from pathlib import Path
import sys

build_lib = Path(sys.argv[sys.argv.index("--build-lib") + 1])
package = build_lib / "mooncake"
package.mkdir(parents=True, exist_ok=True)
(package / "pg_fake.so").write_bytes(b"extension")
"""
    )

    core = tmp_path / "libmooncake_pg.so"
    device = tmp_path / "libmooncake_pg_device.so"
    core.write_bytes(b"core")
    device.write_bytes(b"device")
    staging = tmp_path / "staging"
    build = tmp_path / "build"

    subprocess.run(
        [
            cmake,
            f"-DSOURCE_DIR={source}",
            "-DEP_TORCH_VERSIONS=",
            f"-DSTAGING_DIR={staging}",
            f"-DBUILD_DIR={build}",
            f"-DPG_CORE_SO_PATH={core}",
            f"-DPG_DEVICE_SO_PATH={device}",
            f"-DPython3_EXECUTABLE={sys.executable}",
            "-P",
            str(REPOSITORY_ROOT / "mooncake-pg" / "torch" / "BuildPgExt.cmake"),
        ],
        check=True,
    )

    assert (staging / "pg_fake.so").read_bytes() == b"extension"
    assert (staging / device.name).read_bytes() == b"device"
    assert (build / "current" / "lib" / "mooncake" / "pg_fake.so").is_file()
    assert not list(source.rglob("*.so"))
