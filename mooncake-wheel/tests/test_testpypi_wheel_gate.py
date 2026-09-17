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


def _write_complete_matrix(directory: Path, version: str) -> list[Path]:
    return [
        _write_wheel(directory, package, version, python_tag, architecture)
        for package in gate.CORE_PACKAGES
        for python_tag in gate.PYTHON_TAGS
        for architecture in gate.ARCHITECTURES
    ]


def _artifacts(
    wheels: list[Path],
    hashes: dict[str, str | None] | None = None,
) -> list[gate.IndexedArtifact]:
    hashes = hashes or {}
    return [
        gate.IndexedArtifact(
            wheel.name,
            hashes[wheel.name] if wheel.name in hashes else gate._sha256(wheel),
        )
        for wheel in wheels
    ]


def test_prerelease_tag_normalization_rejects_non_public_versions() -> None:
    assert str(gate.normalize_prerelease_tag("v1.2.0-rc1")) == "1.2.0rc1"

    for tag in ("1.2.0-rc1", "v1.2.0", "v1.2.0rc1+local", "vnot-a-version"):
        with pytest.raises(gate.GateError):
            gate.normalize_prerelease_tag(tag)


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
    _write_complete_matrix(tmp_path, "1.2.0rc1")
    gate.validate_local(tmp_path, Version("1.2.0rc1"))

    package = gate.CORE_PACKAGES[-1]
    for bad_version in ("1.2.0rc2", "1.2rc1"):
        _write_wheel(
            tmp_path,
            package,
            "1.2.0rc1",
            gate.PYTHON_TAGS[-1],
            gate.ARCHITECTURES[-1],
            metadata_version=bad_version,
        )
        with pytest.raises(gate.GateError, match=f"metadata version {bad_version}"):
            gate.validate_local(tmp_path, Version("1.2.0rc1"))


def test_upload_state_accepts_only_hash_matching_partial_uploads(
    tmp_path: Path,
) -> None:
    wheels = _write_complete_matrix(tmp_path, "1.2.0rc1")
    existing = wheels[:7]

    gate.validate_upload_state(
        tmp_path,
        Version("1.2.0rc1"),
        fetcher=lambda _index_url: _artifacts(existing),
    )

    changed = existing[0]
    with pytest.raises(gate.GateError, match="refusing to mix builds"):
        gate.validate_upload_state(
            tmp_path,
            Version("1.2.0rc1"),
            fetcher=lambda _index_url: _artifacts([changed], {changed.name: "0" * 64}),
        )


def test_upload_state_rejects_unverifiable_or_unexpected_artifacts(
    tmp_path: Path,
) -> None:
    wheels = _write_complete_matrix(tmp_path, "1.2.0rc1")
    existing = wheels[0]

    with pytest.raises(gate.GateError, match="did not advertise a SHA-256"):
        gate.validate_upload_state(
            tmp_path,
            Version("1.2.0rc1"),
            fetcher=lambda _index_url: _artifacts([existing], {existing.name: None}),
        )

    unexpected = existing.name.replace("x86_64", "ppc64le")
    with pytest.raises(gate.GateError, match="unexpected artifact"):
        gate.validate_upload_state(
            tmp_path,
            Version("1.2.0rc1"),
            fetcher=lambda _index_url: [gate.IndexedArtifact(unexpected, "0" * 64)],
        )


def test_upload_state_waits_for_the_complete_hash_matching_matrix(
    tmp_path: Path,
) -> None:
    wheels = _write_complete_matrix(tmp_path, "1.2.0rc1")
    responses = iter((_artifacts(wheels[:-1]), _artifacts(wheels)))
    sleeps = []

    gate.wait_for_upload_state(
        tmp_path,
        gate.DEFAULT_INDEX_URL,
        Version("1.2.0rc1"),
        attempts=3,
        initial_delay=1,
        max_delay=4,
        fetcher=lambda _index_url: next(responses),
        sleeper=sleeps.append,
    )

    assert sleeps == [1]
