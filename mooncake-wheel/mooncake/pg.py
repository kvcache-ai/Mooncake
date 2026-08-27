"""Mooncake ProcessGroup Torch adapter loaded through runtime JIT."""

import argparse
import contextlib
import fcntl
import hashlib
import os
import shutil
import sys
import time
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
    configured = os.environ.get("MOONCAKE_EP_USE_MUSA")
    if configured is not None:
        return _env_enabled("MOONCAKE_EP_USE_MUSA")
    # MUSA PyTorch builds expose a version while regular CUDA builds do not.
    # Prefer this runtime signal so MUSA wheels work without a packaging-only
    # environment variable.
    return bool(getattr(getattr(torch, "version", None), "musa", None))


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


def _mcc_path() -> Path | None:
    musa_home = os.environ.get("MUSA_HOME")
    if musa_home:
        candidate = Path(musa_home) / "bin" / "mcc"
        return candidate if candidate.is_file() else None
    mcc = shutil.which("mcc")
    return Path(mcc) if mcc else None


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

        # torchada's MUSA JIT loader does not run SimplePorting for C++ source
        # files. Port a private copy first so CUDA namespace/header references
        # in the unchanged adapter sources are translated for MUSA.
        from torchada.utils.cpp_extension import BuildExtension

        musa_input = build_dir / "musa_input"
        if not musa_input.exists():
            shutil.copytree(source_dir, musa_input)
            external_include = source_dir.parent / "include"
            if external_include.is_dir():
                target_include = musa_input / "include"
                target_include.mkdir(parents=True, exist_ok=True)
                for header in ("pg_utils.h", "mooncake_pg.h"):
                    source_header = external_include / header
                    if source_header.is_file():
                        shutil.copy2(source_header, target_include / header)
        ported_dir = musa_input.with_name(musa_input.name + "_musa")
        if not ported_dir.exists():
            ported_dir = Path(
                object.__new__(BuildExtension)._port_directory(str(musa_input))
            )
        adapter_source_dir = ported_dir
        source_paths = []
        for name in _SOURCE_NAMES:
            candidates = (
                ported_dir / name,
                ported_dir / "src_musa" / name,
                ported_dir / "src" / name,
            )
            source_paths.append(next(path for path in candidates if path.is_file()))
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
        extra_cflags += ["-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"]
    else:
        # Retain the CUDA link contract validated by the fresh-wheel smoke.
        extra_ldflags += ["-lc10_cuda", "-ltorch_cuda"]
        extra_cuda_cflags = None
        adapter_source_dir = source_dir
        source_paths = [_source_path(source_dir, name) for name in _SOURCE_NAMES]

    return load(
        name="_mooncake_pg_jit_musa" if is_musa else "_mooncake_pg_jit_cuda",
        sources=[str(path) for path in source_paths],
        extra_cflags=extra_cflags,
        extra_cuda_cflags=extra_cuda_cflags,
        extra_include_paths=[
            str(adapter_source_dir),
            str(adapter_source_dir / "include"),
            str(adapter_source_dir.parent / "include"),
        ],
        extra_ldflags=extra_ldflags,
        build_directory=str(build_dir),
        verbose=verbose,
        with_cuda=True,
    )


def _has_built_extension(build_dir: Path) -> bool:
    return any(build_dir.glob("*.so"))


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
        mcc = _mcc_path()
        if mcc is None:
            raise ImportError(
                "Mooncake PG MUSA JIT requires the MUSA compiler (mcc). "
                "Set MUSA_HOME or add mcc to PATH."
            )
        toolkit_identity = f"musa:{musa_home or 'PATH'}:{mcc.resolve()}"
    else:
        from torch.utils.cpp_extension import CUDA_HOME

        if not CUDA_HOME or not (Path(CUDA_HOME) / "bin" / "nvcc").is_file():
            raise ImportError(
                "Mooncake PG Torch adapter JIT requires a CUDA toolkit with nvcc; "
                "install a CUDA toolkit or use a compatible runtime environment"
            )
        toolkit_identity = f"cuda:{CUDA_HOME}:{Path(CUDA_HOME) / 'bin' / 'nvcc'}"
        if not os.environ.get("TORCH_CUDA_ARCH_LIST") and not torch.cuda.is_available():
            raise ImportError(
                "Mooncake PG CUDA JIT is running without a visible CUDA device. "
                "Set TORCH_CUDA_ARCH_LIST to a compatible architecture (for example, 8.0) "
                "before importing mooncake.pg."
            )
    key = _cache_key(source_dir, core_path, toolkit_identity)
    build_dir = _cache_root() / key
    build_dir.mkdir(parents=True, exist_ok=True)
    lock_path = build_dir.with_suffix(".lock")
    try:
        with _build_lock(lock_path):
            cold_build = not _has_built_extension(build_dir)
            started = time.monotonic()
            if cold_build:
                print(
                    "[mooncake.pg] Building the Torch adapter with JIT; "
                    "the first import may take a few minutes.",
                    file=sys.stderr,
                    flush=True,
                )
            module = _build_adapter(source_dir, core_path, build_dir)
            if cold_build:
                elapsed = time.monotonic() - started
                print(
                    f"[mooncake.pg] Torch adapter JIT build completed in {elapsed:.1f}s.",
                    file=sys.stderr,
                    flush=True,
                )
            return module
    except Exception as exc:
        raise ImportError(
            "Mooncake PG Torch adapter JIT compilation failed: " + str(exc)
        ) from exc


def _compatibility_report() -> int:
    is_musa = _using_musa()
    ready = True
    print("Mooncake PG JIT compatibility report")
    print(f"  torch: {torch.__version__}")
    print(f"  backend: {'MUSA' if is_musa else 'CUDA'}")
    print(f"  torch.version.cuda: {torch.version.cuda}")
    print(f"  torch.version.musa: {getattr(torch.version, 'musa', None)}")
    ninja = shutil.which("ninja")
    print(f"  ninja: {ninja or 'missing'}")
    ready &= ninja is not None
    print(f"  TORCH_CUDA_ARCH_LIST: {os.environ.get('TORCH_CUDA_ARCH_LIST', '<unset>')}")
    if is_musa:
        mcc = _mcc_path()
        print(f"  mcc: {mcc or 'missing'}")
        ready &= mcc is not None
        print(f"  MTGPU_TARGET: {os.environ.get('MTGPU_TARGET', 'mp_31')}")
    else:
        try:
            from torch.utils.cpp_extension import CUDA_HOME
        except Exception:
            CUDA_HOME = None
        nvcc = Path(CUDA_HOME) / "bin" / "nvcc" if CUDA_HOME else None
        cuda_available = torch.cuda.is_available()
        print(f"  CUDA_HOME: {CUDA_HOME or 'missing'}")
        print(f"  nvcc: {nvcc if nvcc and nvcc.is_file() else 'missing'}")
        print(f"  CUDA available: {cuda_available}")
        ready &= nvcc is not None and nvcc.is_file()
        if not cuda_available and not os.environ.get("TORCH_CUDA_ARCH_LIST"):
            ready = False
    source_dir = _source_dir()
    print(f"  source bundle: {source_dir if source_dir.is_dir() else 'missing'}")
    ready &= source_dir.is_dir()
    core_path = Path(__file__).with_name("libmooncake_pg.so")
    print(f"  PG core library: {core_path if core_path.is_file() else 'missing'}")
    ready &= core_path.is_file()
    print(f"  cache: {_cache_root()}")
    print(f"  status: {'ready' if ready else 'not ready'}")
    return 0 if ready else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Mooncake PG Torch JIT utility")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--prebuild", action="store_true", help="build the PG adapter now")
    action.add_argument("--report", action="store_true", help="print JIT compatibility information")
    args = parser.parse_args(argv)
    if args.report:
        return _compatibility_report()
    if args.prebuild:
        _load_jit_adapter()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
else:
    backend_module = _load_jit_adapter()
    globals().update(
        {key: value for key, value in backend_module.__dict__.items()
         if not key.startswith("_")}
    )
