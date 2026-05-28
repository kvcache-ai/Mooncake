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

# MUSA builds must use TENT (mtlink); the raw IBGDA/mlx5gda path requires CUDA
if use_musa:
    use_tent = True

sources = [
    "src/ep_py.cpp",
    "src/mooncake_ep_buffer.cpp",
    "src/mooncake_ep_kernel.mu" if use_musa else "src/mooncake_ep_kernel.cu",
]

if use_musa:
    # MUSA: link glog, use MUSAExtension
    # All builds now use TENT DeviceTransport — include transport sources.
    cuda_libraries = ["glog"]
    cuda_library_dirs = []
    from torch_musa.utils.musa_extension import MUSAExtension, BuildExtension as MUSABuildExtension
    ExtensionClass = MUSAExtension
    BuildClass = MUSABuildExtension
    sources.append("../mooncake-transfer-engine/tent/src/transport/mtlink/mtlink_device.cpp")
    sources.append("../mooncake-transfer-engine/tent/src/common/status.cpp")
else:
    # CUDA: link IB verbs, mlx5, and TENT static lib
    # All builds now use TENT DeviceTransport — include transport sources.
    cuda_libraries = ["ibverbs", "mlx5", "glog"]
    cuda_library_dirs = []
    ExtensionClass = CUDAExtension
    BuildClass = BuildExtension
    sources.append("../mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_device.cpp")
    sources.append("../mooncake-transfer-engine/tent/src/transport/ibgda/ibgda_device.cpp")
    sources.append("../mooncake-transfer-engine/tent/src/common/status.cpp")

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

# Link against the TENT shared library built from source.  This provides
# IbGdaTransport, NvLinkDeviceTransport, and all their dependencies
# (RPC, metastore, etc.) without needing the old engine.so Python module.
tent_build_dir = os.path.join(
    current_dir, "../build-phase2/mooncake-transfer-engine/tent/src"
)
tent_lib = os.path.join(tent_build_dir, "libtent_shared.so")
if not os.path.exists(tent_lib):
    raise FileNotFoundError(
        f"TENT shared library not found at {tent_lib}. "
        "Build the transfer engine first: cmake --build build-phase2"
    )

extra_link_args = [
    "-Wl,-rpath,$ORIGIN",
    "-Wl,-rpath," + tent_build_dir,
    "-L" + tent_build_dir,
    "-ltent_shared",
]
if not use_musa:
    # CUDA builds also need asio_shared
    asio_lib_dir = os.path.join(
        current_dir, "../build-phase2/mooncake-common"
    )
    extra_link_args.extend([
        "-L" + asio_lib_dir,
        "-lasio",
        "-Wl,-rpath," + asio_lib_dir,
    ])

setup(
    name=module_name,
    ext_modules=[
        ExtensionClass(
            name=module_name,
            include_dirs=[
                os.path.join(current_dir, "include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/tent/include"),
                os.path.join(current_dir, "../mooncake-common/include"),
                "/root/ylt-install/include",
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
                ("mcc" if use_musa else "nvcc"): [
                    f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}",
                    *[f"-D{d}" for d in defines],
                    "-std=c++20",
                    *([] if use_musa else ["-Xcompiler"]),
                    "-O3",
                    *([] if use_musa else ["-Xcompiler"]),
                    "-g0",
                ],
            },
            libraries=cuda_libraries,
            library_dirs=cuda_library_dirs,
            extra_link_args=extra_link_args,
        )
    ],
    cmdclass={"build_ext": BuildClass},
)
