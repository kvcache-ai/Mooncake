# file name: setup.py
import os
import sys
import torch
from setuptools import setup
from torch.utils import cpp_extension

sources = ["src/dummy.cpp"]
include_dirs = [f"{os.path.dirname(os.path.abspath(__file__))}/include/"]

if torch.cuda.is_available():
    module = cpp_extension.CUDAExtension(
        name = "dummy_collectives",
        sources = sources,
        include_dirs = include_dirs,
    )
else:
    module = cpp_extension.CppExtension(
        name = "dummy_collectives",
        sources = sources,
        include_dirs = include_dirs,
    )

setup(
    name = "Dummy-Collectives",
    version = "0.0.1",
    ext_modules = [module],
    cmdclass={'build_ext': cpp_extension.BuildExtension}
)