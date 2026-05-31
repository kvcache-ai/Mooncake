import os
import re

from setuptools import setup
import torch
from torch.utils.cpp_extension import BuildExtension, CUDAExtension, CUDA_HOME


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.ep" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}

if use_musa:
    from torch_musa.utils.musa_extension import MUSAExtension
    from torch_musa.utils.musa_extension import BuildExtension as MUSABuildExtension
    ExtensionClass = MUSAExtension
    BuildClass = MUSABuildExtension
    libraries = ["glog"]
    library_dirs = []
    sources = [
        "src/ep_py.cpp",
        "src/mooncake_ep_buffer.cpp",
        "src/mooncake_ep_kernel.mu",
    ]
    cxx_flags = [
        f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
        "-DUSE_MUSA",
        "-DMOONCAKE_EP_USE_MUSA=1",
        "-std=c++20",
        "-O3",
        "-g0",
    ]
    musa_flags = [
        f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
        "-DUSE_MUSA",
        "-DMOONCAKE_EP_USE_MUSA=1",
        "-std=c++20",
        "--cuda-gpu-arch=mp_21",
        "--cuda-gpu-arch=mp_31",
        "-O3",
    ]
    extra_compile_args = {"cxx": cxx_flags, "mcc": musa_flags}
    # Device transport symbols come from engine.so at runtime
    extra_link_args = [
        "-Wl,-rpath,$ORIGIN",
        "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
        "-l:engine.so",
    ]
else:
    ExtensionClass = CUDAExtension
    BuildClass = BuildExtension
    libraries = ["ibverbs", "mlx5"]
    library_dirs = []
    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            libraries.insert(0, "cuda")
            library_dirs.append(cuda_stub_dir)
    sources = [
        "src/ep_py.cpp",
        "src/mooncake_ep_buffer.cpp",
        "src/mooncake_ep_kernel.cu",
    ]
    extra_compile_args = {
        "cxx": [f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}", "-DUSE_CUDA", "-std=c++20", "-O3", "-g0"],
        "nvcc": [f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}", "-DUSE_CUDA", "-std=c++20", "-Xcompiler", "-O3", "-Xcompiler", "-g0"],
    }
    extra_link_args = [
        "-Wl,-rpath,$ORIGIN",
        "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
        "-l:engine.so",
    ]

setup(
    name=module_name,
    ext_modules=[
        ExtensionClass(
            name=module_name,
            include_dirs=[
                os.path.join(current_dir, "include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/include"),
            ],
            sources=sources,
            extra_compile_args=extra_compile_args,
            libraries=libraries,
            library_dirs=library_dirs,
            extra_link_args=extra_link_args,
        )
    ],
    cmdclass={"build_ext": BuildClass},
)
