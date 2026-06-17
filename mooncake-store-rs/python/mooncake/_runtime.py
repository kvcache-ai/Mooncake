from __future__ import annotations

import ctypes
import os
import pathlib
import subprocess
import sys
import tempfile

_WHEEL_LIB_DIRS = ("mooncake.libs", "mooncake_store_rs.libs")
_NATIVE_LIBRARIES = (
    "libasio.so",
    "libtent_shared.so",
    "libmooncake_classic_shim.so",
    "libmooncake_tent_shim.so",
)
_NATIVE_LIBRARY_ENV_VARS = {
    "libtent_shared.so": "MOONCAKE_TENT_SHARED_LIB_PATH",
    "libmooncake_classic_shim.so": "MOONCAKE_CLASSIC_SHIM_LIB_PATH",
    "libmooncake_tent_shim.so": "MOONCAKE_TENT_SHIM_LIB_PATH",
}


def package_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent


def repo_root(package_root: pathlib.Path | None = None) -> pathlib.Path:
    root = package_root if package_root is not None else package_dir()
    return root.parent.parent


def resolve_first(candidates: list[pathlib.Path]) -> pathlib.Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def safe_resolve(path: pathlib.Path) -> pathlib.Path | None:
    try:
        return path.expanduser().resolve()
    except (OSError, RuntimeError):
        return None


def library_dirs(package_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    root = package_root if package_root is not None else package_dir()
    repository = repo_root(root)
    env_candidates: list[pathlib.Path] = []
    upstream_build = os.environ.get("MOONCAKE_UPSTREAM_BUILD_DIR")
    if upstream_build:
        build_root = pathlib.Path(upstream_build).expanduser()
        if build_root.exists():
            env_candidates.extend(
                [
                    build_root / "mooncake-asio",
                    build_root / "mooncake-transfer-engine" / "src",
                    build_root / "mooncake-transfer-engine" / "tent" / "src",
                ]
            )
    upstream_root = os.environ.get("MOONCAKE_UPSTREAM_DIR")
    if upstream_root:
        root_path = pathlib.Path(upstream_root).expanduser()
        if root_path.exists():
            env_candidates.extend(
                [
                    root_path / "build-wheel-compat" / "mooncake-asio",
                    root_path
                    / "build-wheel-compat"
                    / "mooncake-transfer-engine"
                    / "src",
                    root_path
                    / "build-wheel-compat"
                    / "mooncake-transfer-engine"
                    / "tent"
                    / "src",
                    root_path / "build-rust" / "mooncake-asio",
                    root_path / "build-rust" / "mooncake-transfer-engine" / "src",
                    root_path
                    / "build-rust"
                    / "mooncake-transfer-engine"
                    / "tent"
                    / "src",
                ]
            )
    candidates = env_candidates + [
        root,
        root / "lib",
        root.parent / "mooncake.libs",
        root.parent / "mooncake_store_rs.libs",
        repository / "third_party" / "Mooncake" / "build-rust" / "mooncake-asio",
        repository
        / "third_party"
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "src",
        repository
        / "third_party"
        / "Mooncake"
        / "build-rust"
        / "mooncake-transfer-engine"
        / "tent"
        / "src",
        repository
        / "third_party"
        / "Mooncake"
        / "build-wheel-compat"
        / "mooncake-asio",
        repository
        / "third_party"
        / "Mooncake"
        / "build-wheel-compat"
        / "mooncake-transfer-engine"
        / "src",
        repository
        / "third_party"
        / "Mooncake"
        / "build-wheel-compat"
        / "mooncake-transfer-engine"
        / "tent"
        / "src",
    ]
    resolved: list[pathlib.Path] = []
    seen: set[pathlib.Path] = set()
    for candidate in candidates:
        path = candidate.resolve()
        if path in seen or not path.is_dir():
            continue
        seen.add(path)
        resolved.append(path)
    return resolved


def hashed_library(library_dir: pathlib.Path, prefix: str) -> pathlib.Path | None:
    matches = sorted(library_dir.glob(f"{prefix}-*.so*"))
    if matches:
        return matches[0]
    original = library_dir / f"{prefix}.so"
    if original.exists():
        return original
    return None


def native_library_candidates(
    package_root: pathlib.Path | None = None,
) -> list[pathlib.Path]:
    selected: list[pathlib.Path] = []
    for library_name in _NATIVE_LIBRARIES:
        env_override = _NATIVE_LIBRARY_ENV_VARS.get(library_name)
        if env_override:
            configured = os.environ.get(env_override)
            if configured:
                resolved = safe_resolve(pathlib.Path(configured))
                if resolved is not None and resolved.exists():
                    selected.append(resolved)
                    continue
        candidate_path: pathlib.Path | None = None
        for library_dir in library_dirs(package_root):
            if library_dir.name in _WHEEL_LIB_DIRS:
                candidate = hashed_library(library_dir, library_name[:-3])
            else:
                candidate = library_dir / library_name
            if candidate is None:
                continue
            resolved = safe_resolve(candidate)
            if resolved is None or not resolved.exists():
                continue
            candidate_path = resolved
            break
        if candidate_path is not None:
            selected.append(candidate_path)
    return selected


def preload_native_libraries(package_root: pathlib.Path | None = None) -> None:
    root = package_root if package_root is not None else package_dir()
    if any((root.parent / name).is_dir() for name in _WHEEL_LIB_DIRS):
        return
    for library in native_library_candidates(package_root):
        try:
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
        except OSError as exc:
            print(
                f"warning: failed to preload optional native library {library}: {exc}",
                file=sys.stderr,
            )


def binary_path(
    binary_name: str, package_root: pathlib.Path | None = None
) -> pathlib.Path:
    root = package_root if package_root is not None else package_dir()
    repository = repo_root(root)
    candidates = [
        root / binary_name,
        repository / "dist" / "bin" / binary_name,
        repository / "target" / "release" / binary_name,
        repository
        / "third_party"
        / "Mooncake"
        / "build-wheel-compat"
        / "mooncake-store"
        / "src"
        / binary_name,
        repository
        / "third_party"
        / "Mooncake"
        / "build-wheel-compat"
        / "mooncake-transfer-engine"
        / "example"
        / binary_name,
        repository
        / "third_party"
        / "Mooncake"
        / "build"
        / "mooncake-store"
        / "src"
        / binary_name,
        repository
        / "third_party"
        / "Mooncake"
        / "build"
        / "mooncake-transfer-engine"
        / "example"
        / binary_name,
    ]
    resolved = resolve_first(candidates)
    if resolved is None:
        raise FileNotFoundError(f"cannot find packaged {binary_name} binary")
    return resolved


def prepare_library_env(
    package_root: pathlib.Path | None = None,
) -> tuple[dict[str, str], tempfile.TemporaryDirectory[str] | None]:
    root = package_root if package_root is not None else package_dir()
    env = os.environ.copy()
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    search_paths: list[str] = []

    for library_dir in library_dirs(root):
        search_paths.append(str(library_dir))
        if library_dir.name not in _WHEEL_LIB_DIRS:
            continue
        if temp_dir is None:
            temp_dir = tempfile.TemporaryDirectory(prefix="mooncake-cli-")
        compat_dir = pathlib.Path(temp_dir.name)
        for library_name in _NATIVE_LIBRARIES:
            target = hashed_library(library_dir, library_name[:-3])
            if target is None:
                continue
            alias = compat_dir / library_name
            if alias.exists():
                continue
            alias.symlink_to(target)
        search_paths.insert(0, str(compat_dir))

    current = env.get("LD_LIBRARY_PATH")
    env["LD_LIBRARY_PATH"] = ":".join(search_paths + ([current] if current else []))
    return env, temp_dir


def execute_packaged_binary(binary_name: str, argv: list[str]) -> int:
    root = package_dir()
    binary = binary_path(binary_name, root)
    binary.chmod(0o755)
    env, temp_dir = prepare_library_env(root)
    try:
        return subprocess.call([str(binary), *argv], env=env)
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def invoked_binary_name() -> str:
    program_name = pathlib.Path(sys.argv[0]).name
    if program_name in {
        "transfer_engine_bench",
        "mooncake-store-client",
        "mooncake-store-admin",
        "mooncake-store-bench",
    }:
        return program_name
    return os.environ.get("MOONCAKE_CLI_TARGET", "mooncake-store-client")
