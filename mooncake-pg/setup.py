import os
import re

from setuptools import setup
import torch
from torch.utils.cpp_extension import BuildExtension, CUDAExtension


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.pg" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))


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
                "src/p2p_proxy.cc",
                "src/mooncake_worker.cu",
                "src/mooncake_worker_thread.cpp",
            ],
            extra_compile_args={
                "cxx": [
                    f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
                    "-std=c++20",
                    "-O3",
                    "-g0",
                ],
                "nvcc": [
                    f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
                    "-std=c++20",
                    "-Xcompiler",
                    "-O3",
                    "-Xcompiler",
                    "-g0",
                ],
            },
            libraries=["ibverbs", "mlx5"],
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
                "-L" + os.path.join(current_dir, "../mooncake-wheel/mooncake"),
                "-l:engine.so",
            ],
        )
    ],
    cmdclass={"build_ext": BuildExtension},
)
