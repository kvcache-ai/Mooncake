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


def test_ep_modules_have_one_authoritative_source() -> None:
    package_root = REPOSITORY_ROOT / "python" / "mooncake"
    legacy_package_root = REPOSITORY_ROOT / "mooncake-wheel" / "mooncake"

    for module in (
        "ep.py",
        "mooncake_ep_buffer.py",
        "mooncake_elastic_buffer.py",
    ):
        assert (package_root / module).is_file()
        assert not (legacy_package_root / module).exists()

    test_root = REPOSITORY_ROOT / "python" / "tests" / "ep"
    for test_file in (
        "ep_test_utils.py",
        "test_elastic_buffer.py",
        "test_ep_grid.py",
        "test_mooncake_ep.py",
        "test_regmr_overhead.py",
    ):
        assert (test_root / test_file).is_file()

    for legacy_test in (
        REPOSITORY_ROOT / "mooncake-ep" / "tests" / "test_elastic_buffer.py",
        REPOSITORY_ROOT / "mooncake-ep" / "tests" / "test_ep_grid.py",
        REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "ep_test_utils.py",
        REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "test_mooncake_ep.py",
        REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "test_regmr_overhead.py",
    ):
        assert not legacy_test.exists()


@pytest.mark.parametrize(
    "artifact,defines,expected",
    [
        ("libetcd_wrapper.so", {"STORE_USE_ETCD": "ON"}, True),
        ("libetcd_wrapper.so", {"USE_ETCD": "ON"}, True),
        ("libetcd_wrapper.so", {"USE_ETCD": "ON", "USE_ETCD_LEGACY": "ON"}, False),
        ("libetcd_wrapper.so", {"STORE_USE_ETCD": "ON", "USE_ETCD_LEGACY": "ON"}, True),
        ("libetcd_wrapper.so", {}, False),
        ("allocator.py", {"WITH_TE": "ON"}, True),
        ("fabric_allocator_utils.py", {"WITH_TE": "ON"}, True),
    ],
)
def test_release_runtime_install_conditions(tmp_path, artifact, defines, expected):
    cmake = shutil.which("cmake")
    if cmake is None:
        pytest.skip("CMake is required to evaluate install conditions")
    source = (REPOSITORY_ROOT / "mooncake-integration/CMakeLists.txt").read_text()
    # Execute the actual, self-contained conditional install rule without
    # configuring native dependencies. Capture install() arguments in script mode.
    marker = source.index(f'/{artifact}"')
    start = source.rfind("if(", 0, marker)
    end = source.index("endif()", marker) + len("endif()")
    result = tmp_path / "installed.txt"
    script = tmp_path / "check.cmake"
    script.write_text(
        "\n".join(f"set({key} {value})" for key, value in defines.items())
        + f'\nmacro(install)\nfile(APPEND "{result}" "${{ARGV}}\\n")\nendmacro()\n'
        + source[start:end]
    )
    subprocess.run([cmake, "-P", str(script)], check=True)
    assert result.exists() is expected
    if expected:
        assert artifact in result.read_text()
        assert "COMPONENT;python" in result.read_text()


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
