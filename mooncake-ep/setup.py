import os
import re

import torch
from setuptools import setup
from torch.utils.cpp_extension import BuildExtension, CUDAExtension, CUDA_HOME

torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.ep" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}
use_tent = os.getenv("MOONCAKE_EP_USE_TENT", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}

sources = [
    "src/ep_py.cpp",
    "src/mooncake_ep_buffer.cpp",
    "src/mooncake_ep_kernel.cu",
]

if use_musa:
    # MUSA: no IB verbs, no mlx5, use MUSAExtension
    cuda_libraries = []
    cuda_library_dirs = []
    from torch_musa.utils.musa_extension import MUSAExtension
    ExtensionClass = MUSAExtension
    if use_tent:
        sources.append("../mooncake-transfer-engine/tent/src/transport/mtlink/mtlink_device.cpp")
else:
    # CUDA: link IB verbs and mlx5
    cuda_libraries = ["ibverbs", "mlx5"]
    cuda_library_dirs = []
    ExtensionClass = CUDAExtension
    if not use_tent:
        sources.append("../mooncake-transfer-engine/tent/src/transport/ibgda/detail/mlx5gda.cpp")

    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            cuda_libraries.insert(0, "cuda")
            cuda_library_dirs.append(cuda_stub_dir)

defines = []
if use_tent:
    defines.append("MOONCAKE_EP_USE_TENT=1")
if use_musa:
    defines.append("MOONCAKE_EP_USE_MUSA=1")

setup(
    name=module_name,
    ext_modules=[
        ExtensionClass(
            name=module_name,
            include_dirs=[
                os.path.join(current_dir, "include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/tent/include"),
            ],
            sources=sources,
            extra_compile_args={
                "cxx": [
                    f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
                    *[f"-D{d}" for d in defines],
                    "-std=c++20",
                    "-O3",
                    "-g0",
                ],
                "nvcc": [
                    f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
                    *[f"-D{d}" for d in defines],
                    "-std=c++20",
                    "-Xcompiler",
                    "-O3",
                    "-Xcompiler",
                    "-g0",
                ],
            },
            libraries=cuda_libraries,
            library_dirs=cuda_library_dirs,
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
                "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
                "-l:engine.so",
            ],
        )
    ],
    cmdclass={"build_ext": BuildExtension},
)
