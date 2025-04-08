import sys
import os
from setuptools import setup, find_packages
from setuptools.dist import Distribution
from wheel.bdist_wheel import bdist_wheel

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        bdist_wheel.finalize_options(self)
        self.root_is_pure = False
        self.plat_name_supplied = True
        self.plat_name = "manylinux2014_x86_64"

python_version = f">={sys.version_info.major}.{sys.version_info.minor}"

VERSION = os.environ.get("VERSION", "0.1.0")

setup(
    name="mooncake-transfer-engine",
    version=VERSION,
    packages=find_packages(),
    package_data={
        "mooncake": [
            "*.so",
            "mooncake_master",
            "lib_so/*.so",
        ],
        "mooncake.transfer": [
            "*.so",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    distclass=BinaryDistribution,
    cmdclass={
        'bdist_wheel': CustomBdistWheel,
    },
    author="Mooncake Authors",
    description="Python binding of a Mooncake library using pybind11",
    url="https://github.com/kvcache-ai/Mooncake",
    project_urls={
        "Documentation": "https://github.com/kvcache-ai/Mooncake/tree/main/doc",
        "Source": "https://github.com/kvcache-ai/Mooncake",
        "Issues": "https://github.com/kvcache-ai/Mooncake/issues",
    },
    keywords=["mooncake", "data transfer", "kv cache", "llm inference"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: C++",
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=python_version,
)
