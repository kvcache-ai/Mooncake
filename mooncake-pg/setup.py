import os
import re

from setuptools import setup
import torch
from torch.utils.cpp_extension import BuildExtension, CUDAExtension, CUDA_HOME


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.pg" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))

# Link against the CUDA driver stub library if available.
# Same approach as mooncake-ep/setup.py.
cuda_libraries = ["ibverbs", "mlx5"]
cuda_library_dirs = []

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
                "src/mooncake_worker_thread.cpp",
                "src/connection_poller.cpp",
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
