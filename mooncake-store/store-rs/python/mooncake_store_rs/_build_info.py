from __future__ import annotations

import json
import pathlib
import subprocess
from types import MappingProxyType

from ._runtime import package_dir, repo_root

_BUILD_INFO_FILE = "build-info.json"
_UNKNOWN = "unknown"


def _normalize_build_info(payload: dict[str, object] | None) -> dict[str, str]:
    info = {
        "branch": _UNKNOWN,
        "commit": _UNKNOWN,
        "build_time": _UNKNOWN,
    }
    if payload is None:
        return info
    for key in info:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            info[key] = text
    return info


def _read_packaged_build_info() -> dict[str, str] | None:
    path = package_dir() / _BUILD_INFO_FILE
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return _normalize_build_info(payload)


def _git_output(*args: str) -> str | None:
    repository = repo_root()
    if not (repository / ".git").exists():
        return None
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError, OSError):
        return None
    output = completed.stdout.strip()
    return output or None


def _read_repo_build_info() -> dict[str, str] | None:
    branch = _git_output("rev-parse", "--abbrev-ref", "HEAD")
    commit = _git_output("rev-parse", "HEAD")
    if branch is None and commit is None:
        return None
    return _normalize_build_info(
        {
            "branch": branch,
            "commit": commit,
            "build_time": None,
        }
    )


def load_build_info() -> dict[str, str]:
    return (
        _read_packaged_build_info()
        or _read_repo_build_info()
        or _normalize_build_info(None)
    )


BUILD_INFO = MappingProxyType(load_build_info())


def format_version_output(
    program_name: str,
    version: str,
    edition: str,
    *,
    verbose: bool,
) -> str:
    lines = [f"{pathlib.Path(program_name).name} {version} ({edition})"]
    if verbose:
        lines.extend(
            [
                f"build_branch: {BUILD_INFO['branch']}",
                f"build_commit: {BUILD_INFO['commit']}",
                f"build_time: {BUILD_INFO['build_time']}",
            ]
        )
    return "\n".join(lines)
