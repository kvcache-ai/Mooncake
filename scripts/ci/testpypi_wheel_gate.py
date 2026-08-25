#!/usr/bin/env python3
"""Validate the core Mooncake pre-release wheel set on disk or TestPyPI."""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Iterable
from email.parser import BytesParser
from email.policy import default
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen
from zipfile import BadZipFile, ZipFile

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
IndexFetcher = Callable[[str, str], list[str]]


class GateError(RuntimeError):
    """A release gate invariant was not satisfied."""


class _SimpleLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for name, value in attrs:
            if name == "href" and value:
                self.hrefs.append(value)


def expected_targets() -> set[WheelTarget]:
    """Return every package/Python/architecture target in the core matrix."""
    return {
        (canonicalize_name(package), python_tag, architecture)
        for package in CORE_PACKAGES
        for python_tag in PYTHON_TAGS
        for architecture in ARCHITECTURES
    }


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
    files_by_target: dict[WheelTarget, list[str]] = {}
    for filename in filenames:
        target = _target_from_wheel(filename, version)
        files_by_target.setdefault(target, []).append(filename)

    duplicate_targets = {
        target: files for target, files in files_by_target.items() if len(files) > 1
    }
    if duplicate_targets:
        details = "; ".join(
            f"{target}: {files}" for target, files in sorted(duplicate_targets.items())
        )
        raise GateError(f"duplicate wheels for release targets: {details}")

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
            metadata = BytesParser(policy=default).parsebytes(
                wheel.read(metadata_files[0])
            )
    except (BadZipFile, OSError) as error:
        raise GateError(f"cannot read wheel {path.name!r}: {error}") from error

    names = metadata.get_all("Name", [])
    versions = metadata.get_all("Version", [])
    if len(names) != 1 or len(versions) != 1:
        raise GateError(
            f"wheel {path.name!r} must contain exactly one Name and Version "
            "metadata field"
        )

    metadata_package = canonicalize_name(names[0])
    if metadata_package != expected_package:
        raise GateError(
            f"wheel {path.name!r} has metadata name {names[0]!r}, "
            f"expected {expected_package!r}"
        )
    try:
        metadata_version = Version(versions[0])
    except InvalidVersion as error:
        raise GateError(
            f"wheel {path.name!r} has invalid metadata version {versions[0]!r}"
        ) from error
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


def fetch_simple_filenames(index_url: str, package: str) -> list[str]:
    """Read artifact filenames from one PEP 503 Simple API project page."""
    project_url = f"{index_url.rstrip('/')}/{canonicalize_name(package)}/"
    request = Request(
        project_url,
        headers={
            "Accept": "text/html",
            "User-Agent": "Mooncake-TestPyPI-release-gate/1",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            content = response.read().decode("utf-8")
    except HTTPError as error:
        if error.code == 404:
            return []
        raise

    parser = _SimpleLinks()
    parser.feed(content)
    return [
        Path(unquote(urlparse(urljoin(project_url, href)).path)).name
        for href in parser.hrefs
    ]


def _artifact_version(filename: str) -> Version | None:
    try:
        if filename.endswith(".whl"):
            return parse_wheel_filename(filename)[1]
        return parse_sdist_filename(filename)[1]
    except (InvalidSdistFilename, InvalidWheelFilename, InvalidVersion):
        return None


def ensure_version_absent(
    index_url: str,
    version: Version,
    fetcher: IndexFetcher = fetch_simple_filenames,
) -> None:
    """Fail before upload if TestPyPI already has any artifact at ``version``."""
    collisions = []
    for package in CORE_PACKAGES:
        collisions.extend(
            f"{package}: {filename}"
            for filename in fetcher(index_url, package)
            if _artifact_version(filename) == version
        )
    if collisions:
        raise GateError(
            f"TestPyPI already contains artifacts for {version}; refusing to mix "
            f"this build with an existing upload: {collisions}"
        )
    print(f"TestPyPI has no existing core artifacts for {version}")


def _indexed_wheels(
    index_url: str,
    version: Version,
    fetcher: IndexFetcher,
) -> list[str]:
    filenames = []
    for package in CORE_PACKAGES:
        filenames.extend(
            filename
            for filename in fetcher(index_url, package)
            if filename.endswith(".whl") and _artifact_version(filename) == version
        )
    return filenames


def wait_for_index(
    index_url: str,
    version: Version,
    attempts: int,
    initial_delay: float,
    max_delay: float,
    fetcher: IndexFetcher = fetch_simple_filenames,
    sleeper: Callable[[float], None] = time.sleep,
) -> None:
    """Wait a bounded amount of time for the complete wheel set to propagate."""
    if attempts < 1:
        raise GateError("attempts must be at least 1")

    delay = initial_delay
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            filenames = _indexed_wheels(index_url, version, fetcher)
            validate_wheel_set(filenames, version)
        except (GateError, OSError) as error:
            last_error = error
            if attempt == attempts:
                break
            print(
                f"TestPyPI wheel set is not ready (attempt {attempt}/{attempts}): "
                f"{error}; retrying in {delay:g}s",
                flush=True,
            )
            sleeper(delay)
            delay = min(delay * 2, max_delay)
        else:
            print(f"Validated {len(filenames)} indexed TestPyPI wheels for {version}")
            return

    raise GateError(
        f"TestPyPI did not expose the complete wheel set for {version} after "
        f"{attempts} attempts: {last_error}"
    )


def _version(value: str) -> Version:
    try:
        return Version(value)
    except InvalidVersion as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    local = subparsers.add_parser("validate-local")
    local.add_argument("--directory", type=Path, required=True)
    local.add_argument("--version", type=_version, required=True)

    absent = subparsers.add_parser("ensure-version-absent")
    absent.add_argument("--index-url", default=DEFAULT_INDEX_URL)
    absent.add_argument("--version", type=_version, required=True)

    wait = subparsers.add_parser("wait-index")
    wait.add_argument("--index-url", default=DEFAULT_INDEX_URL)
    wait.add_argument("--version", type=_version, required=True)
    wait.add_argument("--attempts", type=int, default=8)
    wait.add_argument("--initial-delay", type=float, default=5)
    wait.add_argument("--max-delay", type=float, default=30)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "validate-local":
            validate_local(args.directory, args.version)
        elif args.command == "ensure-version-absent":
            ensure_version_absent(args.index_url, args.version)
        else:
            wait_for_index(
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
