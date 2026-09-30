from __future__ import annotations

import ctypes
import os
import pathlib
import subprocess
import sys
import tempfile

# Directories that follow auditwheel's vendoring layout, where libraries carry
# a `libfoo-<hash>.so` suffix. The upstream `mooncake-transfer-engine` wheel is
# listed too: when it is co-installed its vendored transfer-engine libraries are
# usable, so they are worth probing.
_VENDORED_LIB_DIRS = ("mooncake_store_rs.libs", "mooncake.libs")

# Only *this* package's vendored directory means "auditwheel already wired up
# our libraries via RPATH, so preloading is unnecessary". Checking the upstream
# directory here would suppress our own preload the moment the upstream wheel is
# installed alongside us -- the co-installation this package is built for.
_OWN_VENDORED_LIB_DIRS = ("mooncake_store_rs.libs",)
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

# Build directories an upstream Mooncake checkout may have been configured into,
# and the artefact locations within each.
_UPSTREAM_BUILD_DIRS = ("build", "build-rust", "build-wheel-compat")
_UPSTREAM_LIB_SUBDIRS = (
    # upstream relocated libasio.so from mooncake-asio/ to mooncake-common/
    ("mooncake-common",),
    ("mooncake-asio",),
    ("mooncake-transfer-engine", "src"),
    ("mooncake-transfer-engine", "tent", "src"),
)
_UPSTREAM_BIN_SUBDIRS = (
    ("mooncake-store", "src"),
    ("mooncake-transfer-engine", "example"),
)


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


def source_tree_root(package_root: pathlib.Path | None = None) -> pathlib.Path | None:
    """The checkout this package was imported from, or None when installed.

    `repo_root` answers unconditionally, which for a wheel install points at
    whatever happens to sit two levels above ``site-packages``. Build-tree
    lookups must not follow it there, so they ask this instead.
    """
    candidate = repo_root(package_root)
    if (candidate / "Cargo.toml").is_file() and (candidate / "crates").is_dir():
        return candidate
    return None


def upstream_roots(package_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    """Checkouts of upstream Mooncake that may hold built artefacts.

    Both supported layouts are probed without configuration: a standalone
    checkout vendors upstream at ``third_party/Mooncake``, while inside the
    Mooncake monorepo the upstream tree *is* an ancestor directory. A candidate
    only counts if it actually looks like Mooncake, which keeps walking upwards
    from finding false positives.
    """
    candidates: list[pathlib.Path] = []

    configured = os.environ.get("MOONCAKE_UPSTREAM_DIR")
    if configured:
        candidates.append(pathlib.Path(configured).expanduser())

    source_root = source_tree_root(package_root)
    if source_root is not None:
        candidates.append(source_root / "third_party" / "Mooncake")
        candidates.extend(list(source_root.parents)[:3])

    return [
        candidate
        for candidate in candidates
        if (candidate / "mooncake-transfer-engine").is_dir()
    ]


def library_dirs(package_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    root = package_root if package_root is not None else package_dir()
    candidates: list[pathlib.Path] = []

    configured_build = os.environ.get("MOONCAKE_UPSTREAM_BUILD_DIR")
    if configured_build:
        build_root = pathlib.Path(configured_build).expanduser()
        candidates.extend(
            build_root.joinpath(*parts) for parts in _UPSTREAM_LIB_SUBDIRS
        )

    candidates.extend(
        [
            root,
            root / "lib",
            # Our own vendored copy first: a co-installed upstream wheel pins
            # its own transfer-engine build, which need not match ours.
            root.parent / "mooncake_store_rs.libs",
            root.parent / "mooncake.libs",
        ]
    )

    for upstream in upstream_roots(root):
        for build_dir in _UPSTREAM_BUILD_DIRS:
            candidates.extend(
                (upstream / build_dir).joinpath(*parts)
                for parts in _UPSTREAM_LIB_SUBDIRS
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
            if library_dir.name in _VENDORED_LIB_DIRS:
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
    if any((root.parent / name).is_dir() for name in _OWN_VENDORED_LIB_DIRS):
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
    candidates = [root / binary_name]

    source_root = source_tree_root(root)
    if source_root is not None:
        candidates.append(source_root / "dist" / "bin" / binary_name)
        candidates.append(source_root / "target" / "release" / binary_name)

    for upstream in upstream_roots(root):
        for build_dir in _UPSTREAM_BUILD_DIRS:
            candidates.extend(
                (upstream / build_dir).joinpath(*parts) / binary_name
                for parts in _UPSTREAM_BIN_SUBDIRS
            )

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
        if library_dir.name not in _VENDORED_LIB_DIRS:
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
    # Must stay in sync with [project.scripts]. `transfer_engine_bench` is
    # deliberately absent: it belongs to the upstream wheel.
    if program_name in {
        "mooncake-store-client",
        "mooncake-store-admin",
        "mooncake-store-bench",
    }:
        return program_name
    return os.environ.get("MOONCAKE_CLI_TARGET", "mooncake-store-client")
