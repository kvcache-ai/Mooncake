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


def _env_path(name: str, fallback: str) -> str:
    return os.path.abspath(os.getenv(name, fallback))


def _tent_paths():
    tent_build_dir = _env_path(
        "MOONCAKE_TENT_LIB_DIR",
        os.path.join(current_dir, "../build-phase2/mooncake-transfer-engine/tent/src"),
    )
    tent_lib = os.path.abspath(
        os.getenv(
            "MOONCAKE_TENT_SHARED_SO_PATH",
            os.path.join(tent_build_dir, "libtent_shared.so"),
        )
    )
    if not os.path.exists(tent_lib):
        raise FileNotFoundError(
            f"TENT shared library not found at {tent_lib}. "
            "Build the transfer engine first, or set "
            "MOONCAKE_TENT_SHARED_SO_PATH/MOONCAKE_TENT_LIB_DIR."
        )
    return tent_lib, tent_build_dir


def _env_paths(name: str) -> list[str]:
    value = os.getenv(name, "")
    return [os.path.abspath(path) for path in value.split("|") if path]


use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}
use_tent_device_api = os.getenv("MOONCAKE_PG_USE_TENT_DEVICE_API", "").upper() in {
    "1",
    "ON",
    "TRUE",
    "YES",
}

# MUSA builds must use TENT (mtlink); the raw IBGDA/mlx5gda path requires CUDA
if use_musa:
    use_tent_device_api = True

defines = []
if use_tent_device_api:
    defines.append("MOONCAKE_PG_HAS_TENT_DEVICE_API=1")
if use_musa:
    defines.append("MOONCAKE_EP_USE_MUSA=1")

if use_musa:
    cuda_libraries = ["glog"]
    cuda_library_dirs = []
    from torch_musa.utils.musa_extension import MUSAExtension, BuildExtension as MUSABuildExtension
    ExtensionClass = MUSAExtension
    BuildClass = MUSABuildExtension
else:
    # Link against the CUDA driver stub library if available.
    cuda_libraries = ["ibverbs", "mlx5", "glog", "jsoncpp"]
    cuda_library_dirs = []
    if CUDA_HOME is not None:
        cuda_stub_dir = os.path.join(CUDA_HOME, "lib64", "stubs")
        cuda_stub_lib = os.path.join(cuda_stub_dir, "libcuda.so")
        if os.path.exists(cuda_stub_lib):
            cuda_libraries.insert(0, "cuda")
            cuda_library_dirs.append(cuda_stub_dir)
    ExtensionClass = CUDAExtension
    BuildClass = BuildExtension

# Build link args: link against libtent_shared.so instead of engine.so
extra_link_args = ["-Wl,-rpath,$ORIGIN"]
if not use_musa:
    tent_lib, tent_build_dir = _tent_paths()
    asio_lib_dir = _env_path(
        "MOONCAKE_ASIO_LIB_DIR",
        os.path.join(current_dir, "../build-phase2/mooncake-common"),
    )
    mc_common_dir = _env_path(
        "MOONCAKE_COMMON_LIB_DIR",
        os.path.join(current_dir, "../build-phase2/mooncake-common/src"),
    )
    te_build_dir = _env_path(
        "MOONCAKE_TRANSFER_ENGINE_LIB_DIR",
        os.path.join(current_dir, "../build-phase2/mooncake-transfer-engine/src"),
    )
    te_common_dir = _env_path(
        "MOONCAKE_TE_COMMON_LIB_DIR",
        os.path.join(current_dir, "../build-phase2/mooncake-transfer-engine/src/common/base"),
    )
    extra_link_args.extend([
        "-Wl,-rpath," + tent_build_dir,
        "-L" + tent_build_dir,
        "-ltent_shared",
        "-L" + asio_lib_dir,
        "-lasio",
        "-Wl,-rpath," + asio_lib_dir,
        "-L" + te_build_dir,
        "-ltransfer_engine",
        "-L" + te_common_dir,
        "-lbase",
        "-L" + mc_common_dir,
        "-lmooncake_common",
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
            ] + _env_paths("MOONCAKE_YLT_INCLUDE_DIRS")
              + _env_paths("MOONCAKE_PG_EXTRA_INCLUDE"),
            sources=[
                "src/pg_py.cpp",
                "src/mooncake_backend.cpp",
                "src/p2p_proxy.cpp",
                "src/mooncake_worker.cu" if not use_musa else "src/mooncake_worker.mu",
                "src/mooncake_worker_thread.cpp",
                "src/connection_poller.cpp",
            ],
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
