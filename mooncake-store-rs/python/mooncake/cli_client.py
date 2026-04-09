from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile


def _package_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent


def _repo_root(package_dir: pathlib.Path) -> pathlib.Path:
    return package_dir.parent.parent


def _resolve_first(candidates: list[pathlib.Path]) -> pathlib.Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _binary_path(package_dir: pathlib.Path) -> pathlib.Path:
    repo_root = _repo_root(package_dir)
    candidates = [
        package_dir / "mooncake-store-client",
        repo_root / "dist" / "bin" / "mooncake-store-client",
        repo_root / "target" / "release" / "mooncake-store-client",
    ]
    binary = _resolve_first(candidates)
    if binary is None:
        raise FileNotFoundError("cannot find packaged mooncake-store-client binary")
    return binary


def _library_dir(package_dir: pathlib.Path) -> pathlib.Path | None:
    repo_root = _repo_root(package_dir)
    candidates = [
        package_dir.parent / "mooncake_store_rs.libs",
        repo_root
        / "third_party"
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "tent"
        / "src",
        repo_root
        / "third_party"
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "src",
    ]
    return _resolve_first(candidates)


def _hashed_library(library_dir: pathlib.Path, prefix: str) -> pathlib.Path | None:
    matches = sorted(library_dir.glob(f"{prefix}-*.so*"))
    if matches:
        return matches[0]
    original = library_dir / f"{prefix}.so"
    if original.exists():
        return original
    return None


def _prepare_env(
    package_dir: pathlib.Path,
) -> tuple[dict[str, str], tempfile.TemporaryDirectory[str] | None]:
    library_dir = _library_dir(package_dir)
    if library_dir is None:
        return os.environ.copy(), None

    env = os.environ.copy()
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    search_paths = [str(library_dir)]

    if library_dir.name == "mooncake_store_rs.libs":
        temp_dir = tempfile.TemporaryDirectory(prefix="mooncake-store-client-")
        compat_dir = pathlib.Path(temp_dir.name)
        for alias, prefix in (
            ("libtent_shared.so", "libtent_shared"),
            ("libtransfer_engine.so", "libtransfer_engine"),
        ):
            target = _hashed_library(library_dir, prefix)
            if target is not None:
                (compat_dir / alias).symlink_to(target)
        search_paths.insert(0, str(compat_dir))

    current = env.get("LD_LIBRARY_PATH")
    env["LD_LIBRARY_PATH"] = ":".join(search_paths + ([current] if current else []))
    return env, temp_dir


def main() -> int:
    package_dir = _package_dir()
    binary = _binary_path(package_dir)
    binary.chmod(0o755)
    env, temp_dir = _prepare_env(package_dir)
    try:
        return subprocess.call([str(binary), *sys.argv[1:]], env=env)
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
