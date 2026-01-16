import sys
import platform
from setuptools import setup, Distribution
from wheel.bdist_wheel import bdist_wheel

# ---------------------------------------------------------------------------
# Platform guard
# ---------------------------------------------------------------------------
unsupported_platforms = ["win32", "darwin"]  # Still blocking non-Linux builds
if sys.platform in unsupported_platforms:
    sys.exit(
        f"Error: mooncake does not support {platform.system()} at this time. "
        "Please use a supported Linux distribution."
    )

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
try:
    # packaging ≥20 provides a robust glibc detector used by pip/build
    from packaging.tags import glibc_version_string
except ImportError:
    glibc_version_string = None


def _detect_manylinux_tag() -> str:
    """
    Return a PEP 600-style manylinux tag matching *this* build host (e.g. 'manylinux_2_31').
    Falls back to 'manylinux_2_17' if detection fails for maximum compatibility.
    """
    if glibc_version_string is not None:
        ver = glibc_version_string()
    else:
        import ctypes

        try:
            libc = ctypes.CDLL("libc.so.6")
            # gnu_get_libc_version returns a const char* (C string)
            # Set the return type to c_char_p to get the string properly
            libc.gnu_get_libc_version.restype = ctypes.c_char_p
            version_bytes = libc.gnu_get_libc_version()
            ver = version_bytes.decode("ascii", "replace")
        except Exception:
            ver = "2.17"  # conservative baseline

    major, minor, *_ = ver.split(".")
    return f"manylinux_{major}_{minor}"


# ---------------------------------------------------------------------------
# wheel tag builders
# ---------------------------------------------------------------------------
def get_arch() -> str:
    """
    CPU architecture component of the wheel tag.
    """
    if sys.platform.startswith("linux"):
        if platform.machine() == "x86_64":
            return "x86_64"
        if platform.machine() in ("arm64", "aarch64"):
            return "aarch64"
    elif sys.platform == "darwin":
        # Return only the arch; macOS deployment target lives in get_system()
        return "aarch64" if platform.machine() in ("arm64", "aarch64") else "x86_64"
    elif sys.platform == "win32":
        return "win_amd64"

    raise ValueError(f"Unsupported platform: {sys.platform}")


def get_system() -> str:
    """
    OS component of the wheel tag (PEP 425 / PEP 600 compliant).
    """
    sys_name = platform.system()
    if sys_name == "Windows":
        return "win"
    elif sys_name == "Darwin":
        # Use macosx_{major}_{minor} layout (underscored) per PEP 425
        major, minor, *_ = (platform.mac_ver()[0] or "11.0").split(".")
        return f"macosx_{major}_{minor}"
    elif sys_name == "Linux":
        return _detect_manylinux_tag()

    raise ValueError(f"Unsupported system: {sys_name}")


def get_platform() -> str:
    """Full `{system}_{arch}` tag used by bdist_wheel."""
    return f"{get_system()}_{get_arch()}"


# ---------------------------------------------------------------------------
# dist / cmd hooks
# ---------------------------------------------------------------------------
class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False
        self.plat_name_supplied = True
        self.plat_name = get_platform()


# ---------------------------------------------------------------------------
# setup()
# ---------------------------------------------------------------------------
setup(
    distclass=BinaryDistribution,
    cmdclass={"bdist_wheel": CustomBdistWheel},
)
