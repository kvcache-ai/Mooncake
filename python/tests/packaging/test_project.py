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
CLI_MODULES = (
    "cli.py",
    "cli_client.py",
    "cli_bench.py",
    "transfer_engine_topology_dump.py",
)
CLI_ENTRY_POINTS = {
    "mooncake_master": "mooncake.cli:main",
    "mooncake_client": "mooncake.cli_client:main",
    "transfer_engine_bench": "mooncake.cli_bench:main",
    "transfer_engine_topology_dump": "mooncake.transfer_engine_topology_dump:main",
}


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


def test_cli_has_one_authoritative_source_and_test_location() -> None:
    package_root = REPOSITORY_ROOT / "python" / "mooncake"
    legacy_package_root = REPOSITORY_ROOT / "mooncake-wheel" / "mooncake"

    for module in CLI_MODULES:
        assert (package_root / module).is_file()
        assert not (legacy_package_root / module).exists()

    assert (
        REPOSITORY_ROOT / "python" / "tests" / "integration" / "test_cli.py"
    ).is_file()
    assert (
        REPOSITORY_ROOT / "python" / "tests" / "unit" / "test_cli_modules.py"
    ).is_file()
    assert not (REPOSITORY_ROOT / "mooncake-wheel" / "tests" / "test_cli.py").exists()


def test_scikit_build_consumes_unified_python_sources() -> None:
    """Lock the Phase-2 cut-over invariants for the scikit-build wheel path.

    These are read-only source checks. The scikit-build backend uses the
    repository-root CMakeLists.txt, which must pull in python/CMakeLists.txt
    (the \"unified\" Python sources) while mooncake-integration installs the
    CLI binaries next to the package so the console scripts resolve them.

    The legacy mooncake-wheel tree still supplies the not-yet-migrated pure-
    Python modules at install time (Python/CMakeLists.txt DIRECTORY rule);
    Phase 2 should move those into python/mooncake and drop the rule.
    """
    root_cmake = (REPOSITORY_ROOT / "CMakeLists.txt").read_text()
    # SKBUILD must route through the unified python/CMakeLists.txt.
    assert "if(SKBUILD)" in root_cmake
    assert "add_subdirectory(python)" in root_cmake

    python_cmake = (REPOSITORY_ROOT / "python" / "CMakeLists.txt").read_text()
    assert "../mooncake-wheel/mooncake/" in python_cmake
    assert 'PATTERN "__init__.py" EXCLUDE' in python_cmake
    # Modules still supplied from the legacy tree at build time. Phase 2 should
    # migrate these into python/mooncake and drop the DIRECTORY rule above.
    for module in (
        "buffer_pool.py",
        "http_metadata_server.py",
        "mooncake_config.py",
        "mooncake_store_service.py",
        "mooncake_connector_v1.py",
        "vllm_v1_proxy_server.py",
        "pg.py",
        "structured_object_store.py",
        "dataproto_catalog.py",
        "mooncake_ssd_register.py",
        "mooncake_ssd_unregister.py",
        "spdk_tgt_create.py",
    ):
        assert (REPOSITORY_ROOT / "mooncake-wheel" / "mooncake" / module).is_file()

    integration_cmake = (
        REPOSITORY_ROOT / "mooncake-integration" / "CMakeLists.txt"
    ).read_text()
    # The store CLI binaries ship next to the package under SKBUILD so the
    # mooncake.cli / mooncake.cli_client wrappers resolve them in place.
    assert "install(TARGETS mooncake_master mooncake_client" in integration_cmake
    assert "RUNTIME DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}" in integration_cmake
    # Known gap: transfer_engine_bench install is guarded by BUILD_EXAMPLES,
    # which the scikit-build cmake.define does not enable today. Its console
    # script therefore has no backing binary in a scikit-build wheel; tracked
    # as a Phase-2 cut-over item.
    assert "if(WITH_TE AND BUILD_EXAMPLES)" in integration_cmake


def test_python_package_init_keeps_namespace_extension() -> None:
    """The unified python/mooncake package must behave like the legacy wheel
    package: both extend the namespace so independently installed mooncake
    subpackages (e.g. mooncake-reshard / mooncake-pg) remain importable."""
    init = (REPOSITORY_ROOT / "python" / "mooncake" / "__init__.py").read_text()
    assert "extend_path" in init
    assert "__path__ = extend_path(__path__, __name__)" in init

    legacy_init = (
        REPOSITORY_ROOT / "mooncake-wheel" / "mooncake" / "__init__.py"
    ).read_text()
    assert "extend_path" in legacy_init


def test_cli_entry_points_remain_stable_across_build_interfaces() -> None:
    for project_file in (
        REPOSITORY_ROOT / "pyproject.toml",
        REPOSITORY_ROOT / "mooncake-wheel" / "pyproject.toml",
    ):
        project = tomllib.loads(project_file.read_text())
        scripts = project["project"]["scripts"]
        assert {name: scripts[name] for name in CLI_ENTRY_POINTS} == CLI_ENTRY_POINTS


def test_cli_build_inputs_use_the_canonical_sources() -> None:
    legacy_build = (REPOSITORY_ROOT / "scripts" / "build_wheel.sh").read_text()
    assert 'MIGRATED_PYTHON_SOURCE_DIR="python/mooncake"' in legacy_build
    for module in CLI_MODULES:
        assert module in legacy_build
    # The shared launcher must also be staged so the setuptools wheel has it.
    assert "_launcher.py" in legacy_build

    integration_cmake = (
        REPOSITORY_ROOT / "mooncake-integration" / "CMakeLists.txt"
    ).read_text()
    for module in (
        "cli.py",
        "cli_client.py",
        "cli_bench.py",
        "transfer_engine_topology_dump.py",
    ):
        assert f"../python/mooncake/{module}" in integration_cmake
        assert f"../mooncake-wheel/mooncake/{module}" not in integration_cmake


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
