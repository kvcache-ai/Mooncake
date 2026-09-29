from __future__ import annotations

import ctypes
import os
import pathlib
import subprocess
import sys

_NATIVE_LIBRARIES = (
    "libasio.so",
    "libmooncake_common.so",
    "libtransfer_engine.so",
    "libtent_shared.so",
    "libmooncake_classic_shim.so",
    "libmooncake_tent_shim.so",
)
_NATIVE_LIBRARY_ENV_VARS = {
    "libtransfer_engine.so": "MOONCAKE_CLASSIC_TE_LIB_PATH",
    "libtent_shared.so": "MOONCAKE_TENT_SHARED_LIB_PATH",
    "libmooncake_classic_shim.so": "MOONCAKE_CLASSIC_SHIM_LIB_PATH",
    "libmooncake_tent_shim.so": "MOONCAKE_TENT_SHIM_LIB_PATH",
}

# Artefact locations within the CMake build directory.
_BUILD_LIB_SUBDIRS = (
    # upstream relocated libasio.so from mooncake-asio/ to mooncake-common/
    ("mooncake-common",),
    ("mooncake-asio",),
    ("mooncake-transfer-engine", "src"),
    ("mooncake-transfer-engine", "tent", "src"),
)


_BUILD_BIN_SUBDIRS = (("mooncake-transfer-engine", "example"),)


def package_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent


def resolve_first(candidates: list[pathlib.Path]) -> pathlib.Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def explicit_path(name: str) -> pathlib.Path | None:
    value = os.environ.get(name)
    if not value:
        return None
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path: {path}")
    return path


def safe_resolve(path: pathlib.Path) -> pathlib.Path | None:
    try:
        return path.expanduser().resolve()
    except (OSError, RuntimeError):
        return None


def source_tree_root() -> pathlib.Path | None:
    """Store-RS checkout explicitly configured for development resources."""
    return explicit_path("MOONCAKE_STORE_RS_DIR")


def library_dirs(package_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    root = package_root if package_root is not None else package_dir()
    candidates: list[pathlib.Path] = []

    build_root = explicit_path("MOONCAKE_BUILD_DIR")
    if build_root is not None:
        candidates.extend(
            build_root.joinpath(*parts) for parts in _BUILD_LIB_SUBDIRS
        )

    candidates.extend(
        [
            root,
            root / "lib",
        ]
    )

    resolved: list[pathlib.Path] = []
    seen: set[pathlib.Path] = set()
    for candidate in candidates:
        path = candidate.resolve()
        if path in seen or not path.is_dir():
            continue
        seen.add(path)
        resolved.append(path)
    return resolved


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
            candidate = library_dir / library_name
            resolved = safe_resolve(candidate)
            if resolved is None or not resolved.exists():
                continue
            candidate_path = resolved
            break
        if candidate_path is not None:
            selected.append(candidate_path)
    return selected


def preload_native_libraries(package_root: pathlib.Path | None = None) -> None:
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
    candidates = [root / binary_name]

    source_root = source_tree_root()
    if source_root is not None:
        candidates.append(source_root / "target" / "release" / binary_name)

    build_root = explicit_path("MOONCAKE_BUILD_DIR")
    if build_root is not None:
        candidates.extend(
            build_root.joinpath(*parts) / binary_name for parts in _BUILD_BIN_SUBDIRS
        )

    resolved = resolve_first(candidates)
    if resolved is None:
        raise FileNotFoundError(f"cannot find {binary_name} in configured source/build paths")
    return resolved


def prepare_library_env(
    package_root: pathlib.Path | None = None,
) -> dict[str, str]:
    root = package_root if package_root is not None else package_dir()
    env = os.environ.copy()
    search_paths = [str(path) for path in library_dirs(root)]
    current = env.get("LD_LIBRARY_PATH")
    env["LD_LIBRARY_PATH"] = ":".join(search_paths + ([current] if current else []))
    return env


def execute_packaged_binary(binary_name: str, argv: list[str]) -> int:
    root = package_dir()
    binary = binary_path(binary_name, root)
    binary.chmod(0o755)
    env = prepare_library_env(root)
    return subprocess.call([str(binary), *argv], env=env)


def invoked_binary_name() -> str:
    program_name = pathlib.Path(sys.argv[0]).name
    # Must stay in sync with the Store-RS console scripts.
    if program_name in {
        "mooncake-store-client",
        "mooncake-store-admin",
        "mooncake-store-bench",
    }:
        return program_name
    return os.environ.get("MOONCAKE_CLI_TARGET", "mooncake-store-client")
