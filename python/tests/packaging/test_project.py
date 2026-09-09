from __future__ import annotations

from pathlib import Path


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
