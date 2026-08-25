"""Mooncake ProcessGroup Torch adapter loaded through runtime JIT."""

import contextlib
import fcntl
import hashlib
import os
import traceback
from pathlib import Path

import torch


_SOURCE_NAMES = ("pg_py.cpp", "mooncake_backend.cpp", "work_handles.cpp")
_HEADER_NAMES = (
    "mooncake_backend.h",
    "work_handles.h",
    "torch_utils.h",
    "adapter_backoff.h",
    "mooncake_pg.h",
)


def _source_dir() -> Path:
    return Path(__file__).with_name("_pg_jit")


def _cache_root() -> Path:
    configured = os.environ.get("MOONCAKE_PG_JIT_DIR")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".cache" / "mooncake" / "pg_jit"


def _cache_key(source_dir: Path, core_path: Path, build_path: str) -> str:
    digest = hashlib.sha256()
    digest.update(torch.__version__.encode())
    digest.update(str(torch.version.cuda).encode())
    digest.update(str(torch._C._GLIBCXX_USE_CXX11_ABI).encode())
    digest.update(str(torch.version.git_version).encode())
    digest.update(build_path.encode())
    digest.update(core_path.read_bytes())
    for name in (*_SOURCE_NAMES, *_HEADER_NAMES):
        digest.update(name.encode())
        digest.update((source_dir / name).read_bytes())
    return digest.hexdigest()[:20]


@contextlib.contextmanager
def _build_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _build_adapter(source_dir: Path, core_path: Path, build_dir: Path):
    from torch.utils.cpp_extension import library_paths, load

    verbose = os.environ.get("MOONCAKE_PG_JIT_VERBOSE", "0") == "1"
    return load(
        name="_mooncake_pg_jit_cuda",
        sources=[str(source_dir / name) for name in _SOURCE_NAMES],
        extra_cflags=[
            "-std=c++20",
            "-O3",
            "-g0",
            f"-D_GLIBCXX_USE_CXX11_ABI={int(torch._C._GLIBCXX_USE_CXX11_ABI)}",
        ],
        extra_include_paths=[str(source_dir)],
        extra_ldflags=[
            f"-Wl,-rpath,{core_path.parent}",
            *[f"-Wl,-rpath,{path}" for path in library_paths()],
            str(core_path),
            "-lc10_cuda",
            "-ltorch_cuda",
        ],
        build_directory=str(build_dir),
        verbose=verbose,
        with_cuda=True,
    )


def _failure_marker(build_dir: Path) -> Path:
    return build_dir / ".failed"


def _read_failure(build_dir: Path):
    marker = _failure_marker(build_dir)
    if marker.is_file():
        return marker.read_text(errors="replace")
    return None


def _write_failure(build_dir: Path, exc: BaseException):
    _failure_marker(build_dir).write_text(
        "The previous Mooncake PG JIT attempt failed.\n"
        + "".join(traceback.format_exception(exc)),
    )


def _load_jit_adapter():
    source_dir = _source_dir()
    core_path = Path(__file__).with_name("libmooncake_pg.so")
    if not core_path.is_file():
        raise ImportError(
            "Mooncake PG JIT requires libmooncake_pg.so beside mooncake.pg"
        )
    missing = [
        str(source_dir / name)
        for name in (*_SOURCE_NAMES, *_HEADER_NAMES)
        if not (source_dir / name).is_file()
    ]
    if missing:
        raise ImportError(
            "Mooncake PG JIT source bundle is incomplete: " + ", ".join(missing)
        )

    from torch.utils.cpp_extension import CUDA_HOME

    if not CUDA_HOME or not (Path(CUDA_HOME) / "bin" / "nvcc").is_file():
        raise ImportError(
            "Mooncake PG Torch adapter JIT requires a CUDA toolkit with nvcc; "
            "install a CUDA toolkit or use a compatible runtime environment"
        )
    toolkit_identity = f"cuda:{CUDA_HOME}:{Path(CUDA_HOME) / 'bin' / 'nvcc'}"
    key = _cache_key(source_dir, core_path, toolkit_identity)
    build_dir = _cache_root() / key
    build_dir.mkdir(parents=True, exist_ok=True)
    lock_path = build_dir.with_suffix(".lock")
    try:
        with _build_lock(lock_path):
            previous_failure = _read_failure(build_dir)
            if previous_failure:
                raise RuntimeError(previous_failure)
            return _build_adapter(source_dir, core_path, build_dir)
    except Exception as exc:
        with _build_lock(lock_path):
            if not _failure_marker(build_dir).exists():
                _write_failure(build_dir, exc)
        raise ImportError(
            "Mooncake PG Torch adapter JIT compilation failed: " + str(exc)
        ) from exc


backend_module = _load_jit_adapter()
globals().update(
    {key: value for key, value in backend_module.__dict__.items()
     if not key.startswith("_")}
)
