import os
import subprocess
import setuptools
from torch.utils.cpp_extension import BuildExtension, CUDAExtension


if __name__ == '__main__':
    include_dirs = ['csrc/']
    sources = ['csrc/mlx5gda.cpp', 'csrc/mxa_ep.cpp', 'csrc/mxa_kernel.cu']
    libraries = ['ibverbs', 'mlx5']
    extra_link_args = ['-libverbs', '-Wl,--no-as-needed', '-L/usr/lib/aarch64-linux-gnu', '-lcuda']

    # noinspection PyBroadException
    try:
        cmd = ['git', 'rev-parse', '--short', 'HEAD']
        revision = '+' + subprocess.check_output(cmd).decode('ascii').rstrip()
    except Exception as _:
        revision = ''

    setuptools.setup(
        name='mxa_ep',
        version='1.0.0' + revision,
        packages=setuptools.find_packages(
            include=['mxa_ep']
        ),
        ext_modules=[
            CUDAExtension(
                name='mxa_ep_cpp',
                include_dirs=include_dirs,
                sources=sources,
                extra_compile_args={
                    'cxx': ['-DTORCH_USE_CUDA_DSA'],
                    'nvcc': ['-DTORCH_USE_CUDA_DSA'],
                },
                extra_link_args=extra_link_args,
                libraries=libraries
            )
        ],
        cmdclass={
            'build_ext': BuildExtension
        }
    )
