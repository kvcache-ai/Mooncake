from __future__ import annotations

import ctypes
import importlib.machinery
import importlib.util
import pathlib
import sys


def _load_native():
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    _preload_upstream_libraries(repo_root)
    try:
        from . import _store_rs as native  # type: ignore

        return native
    except Exception:
        target_root = repo_root / "target"
        suffixes = list(importlib.machinery.EXTENSION_SUFFIXES) + [".so"]
        patterns = []
        for suffix in suffixes:
            patterns.extend([f"_store_rs{suffix}", f"lib_store_rs{suffix}"])

        for profile in ("debug", "release"):
            for profile_dir in (target_root / profile, target_root / profile / "deps"):
                for pattern in patterns:
                    candidate = profile_dir / pattern
                    if not candidate.exists():
                        continue
                    spec = importlib.util.spec_from_file_location(
                        "mooncake._store_rs", candidate
                    )
                    if spec is None or spec.loader is None:
                        continue
                    module = importlib.util.module_from_spec(spec)
                    sys.modules["mooncake._store_rs"] = module
                    spec.loader.exec_module(module)
                    return module

        raise ImportError(
            "cannot find native mooncake store module; run `cargo build -p mooncake-store-py` first"
        )


def _preload_upstream_libraries(repo_root: pathlib.Path) -> None:
    build_root = repo_root / "third_party" / "Mooncake" / "build-rust" / "mooncake-transfer-engine"
    libraries = [
        build_root / "src" / "libtransfer_engine.so",
        build_root / "tent" / "src" / "libtent_shared.so",
    ]
    for library in libraries:
        if library.exists():
            ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)


_native = _load_native()

MooncakeDistributedStore = _native.MooncakeDistributedStore
init_tracing = _native.init_tracing
metrics_text = _native.metrics_text

__all__ = ["MooncakeDistributedStore", "init_tracing", "metrics_text"]
