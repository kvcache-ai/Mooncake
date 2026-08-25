"""Mooncake ProcessGroup Torch adapter loaded through runtime JIT."""

import contextlib
import fcntl
import hashlib
import os
import shutil
import traceback
from pathlib import Path

import torch


_SOURCE_NAMES = ("pg_py.cpp", "mooncake_backend.cpp", "work_handles.cpp")
_HEADER_NAMES = (
    "mooncake_backend.h",
    "work_handles.h",
    "torch_utils.h",
    "pg_utils.h",
    "mooncake_pg.h",
)


def _env_enabled(name: str) -> bool:
    return os.environ.get(name, "").upper() in {"1", "ON", "TRUE", "YES"}


def _using_musa() -> bool:
    return _env_enabled("MOONCAKE_EP_USE_MUSA")


def _load_torchada() -> None:
    """Install torchada's MUSA mappings before loading cpp_extension."""
    if not _using_musa():
        return
    try:
        import torchada  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "Mooncake PG MUSA JIT requires torchada. "
            "Install it with `python -m pip install torchada`."
        ) from exc


def _source_dir() -> Path:
    installed = Path(__file__).with_name("_pg_jit")
    if installed.is_dir():
        return installed
    # In a source checkout, consume the authoritative adapter sources directly.
    return Path(__file__).parents[2] / "mooncake-pg" / "torch"


def _source_path(source_dir: Path, name: str) -> Path:
    candidate = source_dir / name
    if candidate.is_file():
        return candidate
    subdir = "src" if name.endswith(".cpp") else "include"
    if name in {"mooncake_pg.h", "pg_utils.h"}:
        return source_dir.parent / "include" / name
    return source_dir / subdir / name


def _cache_root() -> Path:
    configured = os.environ.get("MOONCAKE_PG_JIT_DIR")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".cache" / "mooncake" / "pg_jit"


def _cache_key(source_dir: Path, core_path: Path, build_path: str) -> str:
    digest = hashlib.sha256()
    digest.update(torch.__version__.encode())
    digest.update(str(torch.version.cuda).encode())
    digest.update(str(getattr(torch.version, "musa", None)).encode())
    digest.update(str(torch._C._GLIBCXX_USE_CXX11_ABI).encode())
    digest.update(str(torch.version.git_version).encode())
    digest.update(build_path.encode())
    digest.update(core_path.read_bytes())
    for name in (*_SOURCE_NAMES, *_HEADER_NAMES):
        digest.update(name.encode())
        digest.update(_source_path(source_dir, name).read_bytes())
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
    _load_torchada()
    verbose = os.environ.get("MOONCAKE_PG_JIT_VERBOSE", "0") == "1"
    is_musa = _using_musa()
    extra_cflags = [
        "-std=c++20",
        "-O3",
        "-g0",
        f"-D_GLIBCXX_USE_CXX11_ABI={int(torch._C._GLIBCXX_USE_CXX11_ABI)}",
    ]
    if is_musa:
        # torchada exposes the MUSA-aware runtime loader; importing the
        # similarly named torch loader would fall back to CUDA arch detection.
        from torchada.utils.cpp_extension import library_paths, load
    else:
        from torch.utils.cpp_extension import library_paths, load

    extra_ldflags = [
        f"-Wl,-rpath,{core_path.parent}",
        *[f"-Wl,-rpath,{path}" for path in library_paths()],
        str(core_path),
    ]
    if is_musa:
        # MUSA Torch builds report no CUDA SM list. An explicit MUSA target
        # prevents PyTorch's CUDA helper from calling max([]) before mcc runs.
        musa_target = os.environ.get("MTGPU_TARGET", "mp_31")
        if musa_target not in {"mp_22", "mp_31"}:
            raise ImportError(
                "Unsupported MUSA target {!r}; set MTGPU_TARGET to mp_22 or mp_31".format(
                    musa_target
                )
            )
        extra_cuda_cflags = [
            f"--cuda-gpu-arch={musa_target}",
            "-x",
            "musa",
            "-mtgpu",
            "-DUSE_MUSA",
            "-DMOONCAKE_EP_USE_MUSA=1",
        ]
    else:
        # Retain the CUDA link contract validated by the fresh-wheel smoke.
        extra_ldflags += ["-lc10_cuda", "-ltorch_cuda"]
        extra_cuda_cflags = None

    return load(
        name="_mooncake_pg_jit_musa" if is_musa else "_mooncake_pg_jit_cuda",
        sources=[str(_source_path(source_dir, name)) for name in _SOURCE_NAMES],
        extra_cflags=extra_cflags,
        extra_cuda_cflags=extra_cuda_cflags,
        extra_include_paths=[
            str(source_dir),
            str(source_dir / "include"),
            str(source_dir.parent / "include"),
            str(source_dir.parent.parent / "include"),
        ],
        extra_ldflags=extra_ldflags,
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
        if not _source_path(source_dir, name).is_file()
    ]
    if missing:
        raise ImportError(
            "Mooncake PG JIT source bundle is incomplete: " + ", ".join(missing)
        )

    if shutil.which("ninja") is None:
        raise ImportError(
            "Mooncake PG JIT requires Ninja to compile the Torch adapter. "
            "Install it with `python -m pip install ninja`."
        )

    if _using_musa():
        _load_torchada()
        musa_home = os.environ.get("MUSA_HOME")
        mcc = (
            Path(musa_home) / "bin" / "mcc"
            if musa_home
            else shutil.which("mcc")
        )
        if not mcc or not Path(mcc).is_file():
            raise ImportError(
                "Mooncake PG MUSA JIT requires the MUSA compiler (mcc). "
                "Set MUSA_HOME or add mcc to PATH."
            )
        toolkit_identity = f"musa:{musa_home or 'PATH'}:{Path(mcc).resolve()}"
    else:
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
