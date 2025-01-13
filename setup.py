import os
from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext
from setuptools_scm import get_version
import subprocess
import shutil


def get_moooncake_version():
    version = get_version()
    if os.environ.get('USE_CUDA'):
        nvcc_output = subprocess.check_output(['/usr/local/cuda/bin/nvcc', '-V'],
            universal_newlines=True)
        output = nvcc_output.split()
        release_idx = output.index("release") + 1
        nvcc_cuda_version = output[release_idx].split(",")[0]
        version += nvcc_cuda_version
    elif os.environ.get('WITH_P2P_STORE'):
        version += '-p2p'
    return version

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):

    def build_extension(self, ext: Extension) -> None:
        # check cmake version
        try:
            output = subprocess.check_output(['cmake', '--version'])
            if output.decode() < 'cmake version 3.16':
                raise RuntimeError('Require cmake version 3.16+')
        except OSError as e:
            raise RuntimeError('Cannot find CMake executable') from e

        # configure
        cmake_args = ['-DUSE_CXL=ON', '-DUSE_REDIS=ON', '-DUSE_HTTP=ON']
        if os.environ.get('USE_CUDA'):
            cmake_args += [ '-DUSE_CUDA=ON' ]
        elif os.environ.get('WITH_P2P_STORE'):
            cmake_args += [ '-DWITH_P2P_STORE=ON' ]
        
        #build
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.run(
            ['cmake', ext.sourcedir, *cmake_args], cwd=self.build_temp, check=True
        )
        subprocess.run(
            ['cmake', '--build', '.'], cwd=self.build_temp, check=True
        )
        # install
        build_install = os.path.join(self.build_lib, ext.name)
        if not os.path.exists(build_install):
            os.makedirs(build_install)
        for root, dirs, files in os.walk(self.build_temp):
            for file in files:
                if file.endswith('.so') and ext.name in file:
                    source_file = os.path.join(root, file)
                    target_file = os.path.join(build_install, file)
                    shutil.copy2(source_file, target_file)

setup(
    name='mooncake',
    version=get_moooncake_version(),
    description='Mooncake features a KVCache-centric disaggregated architecture that separates the prefill and decoding clusters.\
        It also leverages the underutilized CPU, DRAM, and SSD resources of the GPU cluster to implement a disaggregated cache of KVCache',
    url='https://github.com/kvcache-ai/Mooncake',
    author='Mooncake Team',
    packages=find_packages(),
    python_requires='>=3.10',
    ext_modules=[CMakeExtension('mooncake_vllm_adaptor')],
    cmdclass={'build_ext': CMakeBuild},
)