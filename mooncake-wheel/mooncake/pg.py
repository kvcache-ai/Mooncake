"""Mooncake ProcessGroup Torch adapter loaded through runtime JIT."""

import contextlib
import fcntl
import hashlib
import os
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


def _build_adapter(source_dir: Path, core_path: Path, build_dir: Path,
                   with_cuda: bool):
    from torch.utils.cpp_extension import load

    verbose = os.environ.get("MOONCAKE_PG_JIT_VERBOSE", "0") == "1"
    return load(
        name="_mooncake_pg_jit_cuda" if with_cuda else "_mooncake_pg_jit_cpp",
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
            str(core_path),
            "-lc10_cuda",
            "-ltorch_cuda",
        ],
        build_directory=str(build_dir),
        verbose=verbose,
        with_cuda=with_cuda,
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

    force_cpp = os.environ.get("MOONCAKE_PG_JIT_FORCE_CPP") == "1"
    force_cuda = os.environ.get("MOONCAKE_PG_JIT_FORCE_NVCC") == "1"
    if force_cpp and force_cuda:
        raise ImportError(
            "MOONCAKE_PG_JIT_FORCE_CPP and MOONCAKE_PG_JIT_FORCE_NVCC "
            "cannot both be set"
        )

    from torch.utils.cpp_extension import CUDA_HOME

    cuda_toolkit_available = bool(CUDA_HOME and (Path(CUDA_HOME) / "bin" / "nvcc").is_file())
    if force_cuda and not cuda_toolkit_available:
        raise ImportError(
            "MOONCAKE_PG_JIT_FORCE_NVCC=1 requires CUDA_HOME and nvcc"
        )
    paths = (True,) if force_cuda else (False,) if force_cpp else (False, True)
    failures = []
    for with_cuda in paths:
        if with_cuda and not cuda_toolkit_available:
            failures.append("Path B unavailable: CUDA_HOME/nvcc not found")
            continue
        path_name = "cuda" if with_cuda else "cpp"
        key = _cache_key(source_dir, core_path, path_name)
        build_dir = _cache_root() / key
        build_dir.mkdir(parents=True, exist_ok=True)
        lock_path = build_dir.with_suffix(".lock")
        try:
            with _build_lock(lock_path):
                return _build_adapter(source_dir, core_path, build_dir, with_cuda)
        except Exception as exc:
            failures.append(f"Path {path_name.upper()} failed: {exc}")
            if force_cpp or force_cuda:
                break

    details = "; ".join(failures)
    raise ImportError(
        "Mooncake PG Torch adapter JIT compilation failed: " + details
    )


backend_module = _load_jit_adapter()
globals().update(
    {key: value for key, value in backend_module.__dict__.items()
     if not key.startswith("_")}
)
