import sys
import os
from setuptools import setup, find_packages
from setuptools.dist import Distribution

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

python_version = f">={sys.version_info.major}.{sys.version_info.minor}"

VERSION = os.environ.get("VERSION", "0.1.0")

setup(
    name="mooncake",
    version=VERSION,
    packages=find_packages(),
    package_data={"mooncake": [
        "*.so",
        "mooncake_master",
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