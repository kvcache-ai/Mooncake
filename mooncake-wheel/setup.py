import sys
from setuptools import setup, find_packages
from setuptools.dist import Distribution

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

python_version = f">={sys.version_info.major}.{sys.version_info.minor}"

setup(
    name="mooncake",
    version="0.1.0",
    packages=find_packages(),
    package_data={"mooncake": [
        "mooncake_vllm_adaptor.cpython-310-x86_64-linux-gnu.so",
        "mooncake_master",
        "lib/libetcd-cpp-api.so",
    ]},
    include_package_data=True,
    zip_safe=False,
    distclass=BinaryDistribution,
    author="Mooncake",
    description="Python binding of a Mooncake library using pybind11",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: C++",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires=python_version,
)
