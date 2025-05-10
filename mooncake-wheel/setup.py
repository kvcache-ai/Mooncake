import sys
import platform
from setuptools import setup, Distribution
from wheel.bdist_wheel import bdist_wheel

import sys
import platform

# Fail gracefully on unsupported platforms
unsupported_platforms = ["win32", "darwin"]
if sys.platform in unsupported_platforms:
    sys.exit(f"Error: mooncake does not support {platform.system()} at this time. Please use a supported Linux distribution.")

def get_arch():
    """
    Returns the system architecture for the current system.
    """
    if sys.platform.startswith("linux"):
        if platform.machine() == "x86_64":
            return "x86_64"
        if platform.machine() in ("arm64", "aarch64"):
            return "aarch64"
    elif sys.platform == "darwin":
        mac_version = ".".join(platform.mac_ver()[0].split(".")[:2])
        arch = "x86_64" if platform.machine() == "x86_64" else platform.machine()
        return f"macosx_{mac_version}_{arch}"
    elif sys.platform == "win32":
        return "win_amd64"
    else:
        raise ValueError(f"Unsupported platform: {sys.platform}")


def get_system():
    """
    Returns the system name as used in wheel filenames.
    """
    if platform.system() == "Windows":
        return "win"
    elif platform.system() == "Darwin":
        mac_version = ".".join(platform.mac_ver()[0].split(".")[:1])
        return f"macos_{mac_version}"
    elif platform.system() == "Linux":
        return "manylinux_2_35"
    else:
        raise ValueError(f"Unsupported system: {platform.system()}")


def get_platform():
    """
    Returns the platform name as used in wheel filenames.
    """
    return f"{get_system()}_{get_arch()}"


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False
        self.plat_name_supplied = True
        self.plat_name = get_platform()


setup(
    distclass=BinaryDistribution,
    cmdclass={
        'bdist_wheel': CustomBdistWheel,
    },
)
