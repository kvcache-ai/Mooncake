from setuptools import setup
from torch.utils.cpp_extension import BuildExtension, CUDAExtension
import os
import sys

# Try to locate mooncake directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# Assuming the directory structure is mooncake-root/mooncake-async-pylib
mooncake_root = os.path.dirname(current_dir)
mooncake_engine_root = os.path.join(mooncake_root, "mooncake-transfer-engine")
include_dir = os.path.join(mooncake_engine_root, "include")

# Locate the static library
# Based on your file listing, it is in build/mooncake-transfer-engine/src/libtransfer_engine.a
static_lib_path = os.path.join(mooncake_root, "build", "mooncake-transfer-engine", "src", "libtransfer_engine.a")
common_lib_path = os.path.join(mooncake_root, "build", "mooncake-common", "src", "libmooncake_common.a")

if not os.path.exists(static_lib_path):
    print(f"Error: Could not find static library at {static_lib_path}")
    # Fallback or exit, but let's try to proceed to see if user has it elsewhere
    # sys.exit(1)

extra_objs = []
if os.path.exists(static_lib_path):
    extra_objs.append(static_lib_path)
if os.path.exists(common_lib_path):
    extra_objs.append(common_lib_path)

# Add status.cpp to sources to solve undefined symbol issue
status_cpp_path = os.path.join(mooncake_engine_root, "src", "common", "base", "status.cpp")

setup(
    name='mooncake_async',
    ext_modules=[
        CUDAExtension('mooncake_async', [
            'src/async_binding.cu',
            status_cpp_path,
        ],
        include_dirs=[include_dir],
        # Remove 'libraries' and use 'extra_objects' for static linking
        # libraries=['mooncake_transfer_engine', 'cuda'], 
        # library_dirs=valid_lib_dirs,
        extra_objects=extra_objs, 
        libraries=['ibverbs', 'cuda', 'numa', 'glog', 'jsoncpp', 'curl'], # Link against RDMA verbs and CUDA
        extra_compile_args={'cxx': ['-std=c++17'],
                            'nvcc': ['-O2', '-std=c++17']}
        )
    ],
    cmdclass={
        'build_ext': BuildExtension
    })
