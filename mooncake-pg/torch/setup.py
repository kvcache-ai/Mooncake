import os
import re

from setuptools import setup
import torch

use_musa = os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {"1", "ON", "TRUE", "YES"}
if use_musa:
    try:
        import importlib

        importlib.import_module("torchada")
    except ImportError as e:
        raise ImportError(
            "torchada is required to build the MUSA PG extension. "
            "Please install it first using 'pip install torchada'."
        ) from e


from torch.utils.cpp_extension import (  # noqa: E402
    BuildExtension,
    CUDAExtension,
)


torch_version = re.match(r"\d+(?:\.\d+)*", torch.__version__).group()
version_suffix = "_" + torch_version.replace(".", "_")
module_name = "mooncake.pg" + version_suffix

abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
current_dir = os.path.abspath(os.path.dirname(__file__))

abi_define = f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}"

pg_core_so_path = os.getenv("MOONCAKE_PG_CORE_SO_PATH", "")
if not os.path.isfile(pg_core_so_path):
    raise RuntimeError(
        "MOONCAKE_PG_CORE_SO_PATH is unset or does not name "
        "libmooncake_pg.so"
    )

cxx_args = [
    abi_define,
    "-std=c++20",
    "-O3",
    "-g0",
]

include_dirs = [
    os.path.join(current_dir, "include"),
    os.path.join(current_dir, "../include"),
    os.path.join(current_dir, "../../mooncake-transfer-engine/include"),
]
use_maca = (
    os.getenv("MOONCAKE_EP_USE_MACA", "").upper() in {"1", "ON", "TRUE", "YES"}
    or (hasattr(torch.version, "maca") and torch.version.maca is not None)
)

if use_musa:
    musa_defines = ["-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"]
    cxx_args += musa_defines
else:
    if use_maca:
        cxx_args += ["-DUSE_MACA", "-DMOONCAKE_EP_USE_MACA=1"]

setup(
    name=module_name,
    ext_modules=[
        CUDAExtension(
            name=module_name,
            include_dirs=include_dirs,
            sources=[
                "src/pg_py.cpp",
                "src/mooncake_backend.cpp",
                "src/work_handles.cpp",
            ],
            extra_compile_args={"cxx": cxx_args},
            extra_objects=[pg_core_so_path],
            extra_link_args=[
                "-Wl,-rpath,$ORIGIN",
            ],
        )
    ],
    cmdclass={"build_ext": BuildExtension},
)
