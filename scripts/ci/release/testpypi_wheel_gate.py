#!/usr/bin/env python3
"""Validate the core Mooncake pre-release wheel set on disk or TestPyPI."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import NamedTuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

from packaging.metadata import InvalidMetadata, Metadata
from packaging.utils import (
    InvalidSdistFilename,
    InvalidWheelFilename,
    canonicalize_name,
    parse_sdist_filename,
    parse_wheel_filename,
)
from packaging.version import InvalidVersion, Version

CORE_PACKAGES = (
    "mooncake-transfer-engine",
    "mooncake-transfer-engine-cuda13",
    "mooncake-transfer-engine-non-cuda",
)
PYTHON_TAGS = ("cp310", "cp311", "cp312", "cp313")
ARCHITECTURES = ("x86_64", "aarch64")
DEFAULT_INDEX_URL = "https://test.pypi.org/simple"

WheelTarget = tuple[str, str, str]


class IndexedArtifact(NamedTuple):
    """One artifact advertised by a package's Simple API page."""

    filename: str
    sha256: str | None


ArtifactFetcher = Callable[[str], list[IndexedArtifact]]


class GateError(RuntimeError):
    """A release gate invariant was not satisfied."""


def expected_targets() -> set[WheelTarget]:
    """Return every package/Python/architecture target in the core matrix."""
    return {
        (canonicalize_name(package), python_tag, architecture)
        for package in CORE_PACKAGES
        for python_tag in PYTHON_TAGS
        for architecture in ARCHITECTURES
    }


def normalize_prerelease_tag(tag: str) -> Version:
    """Normalize a public PEP 440 pre-release tag starting with ``v``."""
    if not tag.startswith("v"):
        raise GateError(f"pre-release tag must start with v: {tag!r}")
    try:
        version = Version(tag[1:])
    except InvalidVersion as error:
        raise GateError(f"invalid PEP 440 version in tag {tag!r}: {error}") from error
    if not version.is_prerelease or version.local is not None:
        raise GateError(f"tag {tag!r} must identify a public PEP 440 pre-release")
    return version


def _target_from_wheel(filename: str, expected_version: Version) -> WheelTarget:
    try:
        package, version, _build, tags = parse_wheel_filename(filename)
    except InvalidWheelFilename as error:
        raise GateError(f"invalid wheel filename {filename!r}: {error}") from error

    if str(version) != str(expected_version):
        raise GateError(
            f"wheel {filename!r} has version {version}, expected {expected_version}"
        )

    package = canonicalize_name(package)
    candidates = {
        (package, tag.interpreter, architecture)
        for tag in tags
        if tag.interpreter in PYTHON_TAGS
        for architecture in ARCHITECTURES
        if tag.platform.endswith(f"_{architecture}")
    }
    if len(candidates) != 1:
        raise GateError(
            f"wheel {filename!r} does not identify exactly one expected "
            f"Python/architecture target: {sorted(candidates)}"
        )
    return candidates.pop()


def validate_wheel_set(filenames: Iterable[str], version: Version) -> None:
    """Require exactly one wheel for each target at ``version``."""
    files_by_target: dict[WheelTarget, str] = {}
    for filename in filenames:
        target = _target_from_wheel(filename, version)
        if target in files_by_target:
            raise GateError(
                f"duplicate wheels for {target}: "
                f"{files_by_target[target]!r} and {filename!r}"
            )
        files_by_target[target] = filename

    actual = set(files_by_target)
    expected = expected_targets()
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing targets: {missing}")
        if unexpected:
            details.append(f"unexpected targets: {unexpected}")
        raise GateError("; ".join(details))


def _validate_wheel_metadata(path: Path, expected_version: Version) -> None:
    expected_package = _target_from_wheel(path.name, expected_version)[0]
    try:
        with ZipFile(path) as wheel:
            metadata_files = [
                name
                for name in wheel.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_files) != 1:
                raise GateError(
                    f"wheel {path.name!r} contains {len(metadata_files)} "
                    ".dist-info/METADATA files, expected exactly one"
                )
            metadata = Metadata.from_email(
                wheel.read(metadata_files[0]), validate=False
            )
            metadata_package = canonicalize_name(metadata.name)
            metadata_version = metadata.version
    except (BadZipFile, InvalidMetadata, OSError) as error:
        raise GateError(f"cannot read wheel {path.name!r}: {error}") from error

    if metadata_package != expected_package:
        raise GateError(
            f"wheel {path.name!r} has metadata name {metadata.name!r}, "
            f"expected {expected_package!r}"
        )
    if str(metadata_version) != str(expected_version):
        raise GateError(
            f"wheel {path.name!r} has metadata version {metadata_version}, "
            f"expected {expected_version}"
        )


def validate_local(directory: Path, version: Version) -> None:
    """Validate all wheels collected for publication."""
    wheels = sorted(directory.glob("*.whl"))
    if not wheels:
        raise GateError(f"no wheels found in {directory}")
    validate_wheel_set((path.name for path in wheels), version)
    for path in wheels:
        _validate_wheel_metadata(path, version)
    print(f"Validated {len(wheels)} local wheels for {version}")


def _fetch_package_artifacts(index_url: str, package: str) -> list[IndexedArtifact]:
    """Read artifact filenames and hashes from a Simple API project page."""
    project_url = f"{index_url.rstrip('/')}/{canonicalize_name(package)}/"
    request = Request(
        project_url,
        headers={
            "Accept": "application/vnd.pypi.simple.v1+json",
            "User-Agent": "Mooncake-TestPyPI-release-gate/1",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return []
        raise
    return [
        IndexedArtifact(item["filename"], item.get("hashes", {}).get("sha256"))
        for item in payload.get("files", [])
    ]


def fetch_index_artifacts(index_url: str) -> list[IndexedArtifact]:
    return [
        artifact
        for package in CORE_PACKAGES
        for artifact in _fetch_package_artifacts(index_url, package)
    ]


def _artifact_version(filename: str) -> Version | None:
    try:
        if filename.endswith(".whl"):
            return parse_wheel_filename(filename)[1]
        return parse_sdist_filename(filename)[1]
    except (InvalidSdistFilename, InvalidWheelFilename, InvalidVersion):
        return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as wheel:
        for chunk in iter(lambda: wheel.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _local_wheel_hashes(directory: Path, version: Version) -> dict[str, str]:
    validate_local(directory, version)
    return {path.name: _sha256(path) for path in sorted(directory.glob("*.whl"))}


def _indexed_version_artifacts(
    index_url: str,
    version: Version,
    fetcher: ArtifactFetcher,
) -> dict[str, str | None]:
    indexed: dict[str, str | None] = {}
    for artifact in fetcher(index_url):
        if _artifact_version(artifact.filename) != version:
            continue
        if artifact.filename in indexed:
            raise GateError(f"TestPyPI lists {artifact.filename!r} more than once")
        indexed[artifact.filename] = artifact.sha256
    return indexed


def _validate_indexed_hashes(
    local_hashes: dict[str, str],
    indexed: dict[str, str | None],
    version: Version,
    *,
    require_complete: bool,
) -> None:
    """Require every visible artifact to match and optionally require all files."""
    if require_complete:
        missing = sorted(set(local_hashes) - set(indexed))
        if missing:
            raise GateError(
                f"TestPyPI is missing {len(missing)} artifacts for {version}: {missing}"
            )

    for filename, indexed_hash in sorted(indexed.items()):
        local_hash = local_hashes.get(filename)
        if local_hash is None:
            raise GateError(
                f"TestPyPI contains unexpected artifact {filename!r} for {version}"
            )
        if not indexed_hash:
            raise GateError(
                f"TestPyPI did not advertise a SHA-256 hash for {filename!r}"
            )
        if indexed_hash.lower() != local_hash:
            raise GateError(
                f"TestPyPI artifact {filename!r} has SHA-256 {indexed_hash}, "
                f"but the local wheel has {local_hash}; refusing to mix builds"
            )


def validate_upload_state(
    directory: Path,
    version: Version,
    fetcher: ArtifactFetcher = fetch_index_artifacts,
    *,
    index_url: str = DEFAULT_INDEX_URL,
) -> None:
    """Allow an empty index or an exact, hash-matching partial upload.

    TestPyPI filenames do not identify build contents. Comparing the hashes
    advertised by the Simple API prevents a retry from combining wheels from
    two different builds under one immutable version.
    """
    local_hashes = _local_wheel_hashes(directory, version)
    indexed = _indexed_version_artifacts(index_url, version, fetcher)
    _validate_indexed_hashes(
        local_hashes,
        indexed,
        version,
        require_complete=False,
    )

    if indexed:
        print(
            f"Validated {len(indexed)} matching existing TestPyPI artifacts; "
            "the upload can safely resume"
        )
    else:
        print(f"TestPyPI has no existing core artifacts for {version}")


def wait_for_upload_state(
    directory: Path,
    index_url: str,
    version: Version,
    attempts: int,
    initial_delay: float,
    max_delay: float,
    fetcher: ArtifactFetcher = fetch_index_artifacts,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    """Wait until every published wheel is visible with its local SHA-256."""
    if attempts < 1:
        raise GateError("attempts must be at least 1")

    local_hashes = _local_wheel_hashes(directory, version)
    delay = initial_delay
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            indexed = _indexed_version_artifacts(index_url, version, fetcher)
            _validate_indexed_hashes(
                local_hashes,
                indexed,
                version,
                require_complete=True,
            )
        except (GateError, OSError) as error:
            last_error = error
            if attempt == attempts:
                break
            print(
                "TestPyPI hashes are not ready "
                f"(attempt {attempt}/{attempts}): {error}; "
                f"retrying in {delay:g}s",
                flush=True,
            )
            sleeper(delay)
            delay = min(delay * 2, max_delay)
        else:
            print(
                f"Validated all {len(indexed)} published TestPyPI hashes for {version}"
            )
            return

    raise GateError(
        f"TestPyPI did not expose the complete, hash-matching wheel set for "
        f"{version} after {attempts} attempts: {last_error}"
    )


def _version(value: str) -> Version:
    try:
        return Version(value)
    except InvalidVersion as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    normalize = subparsers.add_parser("normalize-version")
    normalize.add_argument("--tag", required=True)

    upload = subparsers.add_parser("validate-upload-state")
    upload.add_argument("--directory", type=Path, required=True)
    upload.add_argument("--index-url", default=DEFAULT_INDEX_URL)
    upload.add_argument("--version", type=_version, required=True)

    wait_upload = subparsers.add_parser("wait-upload-state")
    wait_upload.add_argument("--directory", type=Path, required=True)
    wait_upload.add_argument("--index-url", default=DEFAULT_INDEX_URL)
    wait_upload.add_argument("--version", type=_version, required=True)
    wait_upload.add_argument("--attempts", type=int, default=8)
    wait_upload.add_argument("--initial-delay", type=float, default=5)
    wait_upload.add_argument("--max-delay", type=float, default=30)

    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "normalize-version":
            print(normalize_prerelease_tag(args.tag))
        elif args.command == "validate-upload-state":
            validate_upload_state(
                args.directory,
                args.version,
                index_url=args.index_url,
            )
        elif args.command == "wait-upload-state":
            wait_for_upload_state(
                args.directory,
                args.index_url,
                args.version,
                args.attempts,
                args.initial_delay,
                args.max_delay,
            )
    except GateError as error:
        print(f"release gate failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
