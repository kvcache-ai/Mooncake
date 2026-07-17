import os
import re

from setuptools import setup
import torch

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}
use_maca = (
    os.getenv("MOONCAKE_EP_USE_MACA", "").upper() in {"1", "ON", "TRUE", "YES"}
    or (hasattr(torch.version, "maca") and torch.version.maca is not None)
)
use_nccl_device = os.getenv("MOONCAKE_EP_USE_NCCL_DEVICE", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}
use_b300_perf_shapes_only = os.getenv(
    "MOONCAKE_EP_ELASTIC_B300_PERF_SHAPES_ONLY", ""
).upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}
if use_musa:
    try:
        import torchada  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "torchada is required to build the MUSA EP extension. "
            "Please install it first using 'pip install torchada'."
        ) from e


from torch.utils.cpp_extension import (  # noqa: E402
    BuildExtension,
    CUDAExtension,
    CUDA_HOME,
)


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.ep" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))
repo_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sysroot_dir = os.path.join(repo_dir, ".deps", "sysroot", "usr")


def existing_dirs(*paths):
    return [path for path in paths if os.path.isdir(path)]


sysroot_include_dirs = existing_dirs(
    os.path.join(sysroot_dir, "include"),
    os.path.join(sysroot_dir, "include", "jsoncpp"),
    os.path.join(sysroot_dir, "include", "libnl3"),
)
sysroot_library_dirs = existing_dirs(
    os.path.join(sysroot_dir, "lib", "x86_64-linux-gnu"),
    os.path.join(sysroot_dir, "lib"),
)

abi_define = f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}"
cxx_args = [abi_define, "-std=c++20", "-O3", "-g0"]

cuda_libraries = ["ibverbs", "mlx5"]
cuda_library_dirs = []
include_dirs = [
    os.path.join(current_dir, "include"),
    os.path.join(current_dir, "../mooncake-transfer-engine/include"),
]

if use_musa:
    cuda_libraries = []
    musa_defines = [
        "-DUSE_MUSA",
        "-DMOONCAKE_EP_USE_MUSA=1",
    ]
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
elif use_maca:
    cuda_libraries = []
    cuda_library_dirs = sysroot_library_dirs.copy()
    include_dirs += sysroot_include_dirs
    maca_defines = ["-DUSE_MACA", "-DMOONCAKE_EP_USE_MACA=1"]
    cxx_args += maca_defines
    device_args = [
        abi_define,
        *maca_defines,
        "-std=c++20",
        "-O3",
    ]
else:
    cxx_args.append("-DUSE_CUDA")
    device_args = [
        abi_define,
        "-std=c++20",
        "-DUSE_CUDA",
        "-Xcompiler",
        "-O3",
        "-Xcompiler",
        "-g0",
    ]
    # Link against the CUDA driver stub library if available.
    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            cuda_libraries.insert(0, "cuda")
        cuda_library_dirs.append(cuda_stub_dir)

if use_nccl_device:
    nccl_root = os.getenv("NCCL_ROOT")
    if not nccl_root:
        raise RuntimeError(
            "MOONCAKE_EP_USE_NCCL_DEVICE requires NCCL_ROOT to point to "
            "an NCCL 2.30.4+ installation"
        )
    nccl_include = os.path.join(nccl_root, "include")
    nccl_lib = os.path.join(nccl_root, "lib")
    if not os.path.exists(os.path.join(nccl_include, "nccl_device.h")):
        raise RuntimeError(f"NCCL device header not found under {nccl_include}")
    if not os.path.exists(os.path.join(nccl_lib, "libnccl.so.2")):
        raise RuntimeError(f"NCCL library not found under {nccl_lib}")
    include_dirs.append(nccl_include)
    cuda_library_dirs.append(nccl_lib)
    cuda_libraries.append("nccl")
    nccl_defines = [
        "-DUSE_NCCL_DEVICE",
        "-DNCCL_DEVICE_PERMIT_EXPERIMENTAL_CODE=1",
    ]
    cxx_args += nccl_defines
    device_args += nccl_defines

if use_b300_perf_shapes_only:
    shape_defines = ["-DMOONCAKE_EP_ELASTIC_B300_PERF_SHAPES_ONLY=1"]
    cxx_args += shape_defines
    device_args += shape_defines

setup(
    name=module_name,
    ext_modules=[
        CUDAExtension(
            name=module_name,
            include_dirs=include_dirs,
            sources=[
                "src/ep_py.cpp",
                "src/mooncake_ep_buffer.cpp",
                "src/mooncake_ep_elastic_buffer.cpp",
                "src/mooncake_ep_kernel.cu",
                "src/mooncake_ep_elastic_kernel.cu",
            ],
            extra_compile_args={"cxx": cxx_args, "nvcc": device_args},
            libraries=cuda_libraries,
            library_dirs=cuda_library_dirs,
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
                *(
                    ["-Wl,-rpath," + os.path.join(os.getenv("NCCL_ROOT"), "lib")]
                    if use_nccl_device
                    else []
                ),
                "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
                "-Wl,--push-state,--no-as-needed",
                "-l:engine.so",
                "-Wl,--pop-state",
            ],
        )
    ],
    cmdclass={"build_ext": BuildExtension},
)
