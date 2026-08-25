"""Focused tests for the TestPyPI pre-release wheel gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from zipfile import ZipFile

import pytest
from packaging.version import Version


def _load_gate_module():
    repository = Path(__file__).resolve().parents[2]
    script = repository / "scripts" / "ci" / "testpypi_wheel_gate.py"
    spec = importlib.util.spec_from_file_location("testpypi_wheel_gate", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = _load_gate_module()


def _wheel(package: str, version: str, python_tag: str, architecture: str) -> str:
    distribution = package.replace("-", "_")
    return (
        f"{distribution}-{version}-{python_tag}-{python_tag}-"
        f"manylinux_2_28_{architecture}.whl"
    )


def _complete_matrix(version: str) -> list[str]:
    return [
        _wheel(package, version, python_tag, architecture)
        for package in gate.CORE_PACKAGES
        for python_tag in gate.PYTHON_TAGS
        for architecture in gate.ARCHITECTURES
    ]


def _write_wheel(
    directory: Path,
    package: str,
    filename_version: str,
    python_tag: str,
    architecture: str,
    *,
    metadata_version: str | None = None,
) -> Path:
    path = directory / _wheel(package, filename_version, python_tag, architecture)
    dist_info = f"{package.replace('-', '_')}-{filename_version}.dist-info"
    with ZipFile(path, "w") as wheel:
        wheel.writestr(
            f"{dist_info}/METADATA",
            "\n".join(
                (
                    "Metadata-Version: 2.1",
                    f"Name: {package}",
                    f"Version: {metadata_version or filename_version}",
                    "",
                )
            ),
        )
    return path


def test_complete_matrix_requires_exact_version_and_all_24_targets() -> None:
    filenames = _complete_matrix("1.2.0rc1")

    gate.validate_wheel_set(filenames, Version("1.2.0-rc1"))

    with pytest.raises(gate.GateError, match="missing targets"):
        gate.validate_wheel_set(filenames[:-1], Version("1.2.0rc1"))

    wrong_version = [*filenames[:-1], filenames[-1].replace("1.2.0rc1", "1.2.0rc2")]
    with pytest.raises(gate.GateError, match="has version 1.2.0rc2"):
        gate.validate_wheel_set(wrong_version, Version("1.2.0rc1"))

    equivalent_version = _complete_matrix("1.2rc1")
    with pytest.raises(gate.GateError, match="has version 1.2rc1"):
        gate.validate_wheel_set(equivalent_version, Version("1.2.0rc1"))


def test_local_validation_reads_each_wheel_metadata(tmp_path: Path) -> None:
    wheels = [
        _write_wheel(tmp_path, package, "1.2.0rc1", python_tag, architecture)
        for package in gate.CORE_PACKAGES
        for python_tag in gate.PYTHON_TAGS
        for architecture in gate.ARCHITECTURES
    ]
    gate.validate_local(tmp_path, Version("1.2.0rc1"))

    mismatched = wheels[-1]
    package = gate.CORE_PACKAGES[-1]
    _write_wheel(
        tmp_path,
        package,
        "1.2.0rc1",
        gate.PYTHON_TAGS[-1],
        gate.ARCHITECTURES[-1],
        metadata_version="1.2.0rc2",
    )
    assert mismatched.exists()
    with pytest.raises(gate.GateError, match="metadata version 1.2.0rc2"):
        gate.validate_local(tmp_path, Version("1.2.0rc1"))

    _write_wheel(
        tmp_path,
        package,
        "1.2.0rc1",
        gate.PYTHON_TAGS[-1],
        gate.ARCHITECTURES[-1],
        metadata_version="1.2rc1",
    )
    with pytest.raises(gate.GateError, match="metadata version 1.2rc1"):
        gate.validate_local(tmp_path, Version("1.2.0rc1"))


def test_existing_testpypi_equivalent_version_is_a_hard_collision() -> None:
    existing = _complete_matrix("1.2rc1")

    def fetcher(_index_url: str, package: str) -> list[str]:
        prefix = package.replace("-", "_") + "-"
        return [filename for filename in existing if filename.startswith(prefix)]

    with pytest.raises(gate.GateError, match="refusing to mix"):
        gate.ensure_version_absent(
            gate.DEFAULT_INDEX_URL,
            Version("1.2.0rc1"),
            fetcher=fetcher,
        )


def test_index_wait_retries_until_the_complete_matrix_is_visible() -> None:
    complete = _complete_matrix("1.2.0rc1")
    attempts = 0
    sleeps = []

    def fetcher(_index_url: str, package: str) -> list[str]:
        nonlocal attempts
        if package == gate.CORE_PACKAGES[0]:
            attempts += 1
        visible = complete[:-1] if attempts <= 1 else complete
        prefix = package.replace("-", "_") + "-"
        return [filename for filename in visible if filename.startswith(prefix)]

    gate.wait_for_index(
        gate.DEFAULT_INDEX_URL,
        Version("1.2.0rc1"),
        attempts=3,
        initial_delay=1,
        max_delay=4,
        fetcher=fetcher,
        sleeper=sleeps.append,
    )

    assert sleeps == [1]


def test_index_wait_is_bounded() -> None:
    with pytest.raises(gate.GateError, match="after 3 attempts"):
        gate.wait_for_index(
            gate.DEFAULT_INDEX_URL,
            Version("1.2.0rc1"),
            attempts=3,
            initial_delay=1,
            max_delay=2,
            fetcher=lambda _index_url, _package: [],
            sleeper=lambda _delay: None,
        )
