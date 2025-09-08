# file name: setup.py
import os
import sys
import torch
from setuptools import setup
from torch.utils import cpp_extension

sources = ["dummy.cpp"]

if torch.cuda.is_available():
    module = cpp_extension.CUDAExtension(
        name = "dummy_collectives",
        sources = sources,
    )
else:
    module = cpp_extension.CppExtension(
        name = "dummy_collectives",
        sources = sources,
    )

setup(
    name = "Dummy-Collectives",
    version = "0.0.1",
    ext_modules = [module],
    cmdclass={'build_ext': cpp_extension.BuildExtension}
)