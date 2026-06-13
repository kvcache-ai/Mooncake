import os
import re

from setuptools import setup
import torch

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}
if use_musa:
    try:
        import torchada  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "torchada is required to build the MUSA PG extension. "
            "Please install it first using 'pip install torchada'."
        ) from e


from torch.utils.cpp_extension import (  # noqa: E402
    BuildExtension,
    CUDAExtension,
    CUDA_HOME,
)


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.pg" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))

abi_define = f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}"
cxx_args = [abi_define, "-std=c++20", "-O3", "-g0"]

cuda_libraries = ["ibverbs", "mlx5"]
cuda_library_dirs = []
use_maca = hasattr(torch.version, "maca") and torch.version.maca is not None

if use_musa:
    musa_defines = ["-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"]
    cxx_args += musa_defines
    # torchada maps the "nvcc" key to "mcc".
    device_args = [
        abi_define,
        *musa_defines,
        "-std=c++20",
        "--cuda-gpu-arch=mp_21",
        "--cuda-gpu-arch=mp_31",
        "-O3",
    ]
else:
    if use_maca:
        cxx_args.append("-DUSE_MACA")
    device_args = [
        abi_define,
        "-std=c++20",
        "-Xcompiler",
        "-O3",
        "-Xcompiler",
        "-g0",
    ]
    if use_maca:
        device_args.append("-DUSE_MACA")
    # Link against the CUDA driver stub library if available.
    # Same approach as mooncake-ep/setup.py.
    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            cuda_libraries.insert(0, "cuda")
            cuda_library_dirs.append(cuda_stub_dir)

setup(
    name=module_name,
    ext_modules=[
        CUDAExtension(
            name=module_name,
            include_dirs=[
                os.path.join(current_dir, "include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/include"),
            ],
            sources=[
                "src/pg_py.cpp",
                "src/mooncake_backend.cpp",
                "src/p2p_proxy.cpp",
                "src/mooncake_worker.cu",
                "src/mooncake_worker_host.cpp",
                "src/mooncake_worker_thread.cpp",
                "src/connection_poller.cpp",
                "src/work_handles.cpp",
            ],
            extra_compile_args={"cxx": cxx_args, "nvcc": device_args},
            libraries=cuda_libraries,
            library_dirs=cuda_library_dirs,
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
                "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
                "-Wl,--push-state,--no-as-needed",
                "-l:engine.so",
                "-Wl,--pop-state",
            ],
        )
    ],
    cmdclass={"build_ext": BuildExtension},
)
