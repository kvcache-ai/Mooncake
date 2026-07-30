import sys
import os
import platform
from setuptools import setup, Distribution
from wheel.bdist_wheel import bdist_wheel
import subprocess
from typing import Optional

pwd = os.path.dirname(os.path.abspath(__file__))
add_git_version = False
if int(os.environ.get('ADD_GIT_VERSION', '0')) == 1:
    add_git_version = True

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
        import subprocess
        import shutil

        # Try getconf first (POSIX standard, most reliable, no hardcoded libc name)
        ver = None
        if shutil.which("getconf"):
            try:
                result = subprocess.run(
                    ["getconf", "GNU_LIBC_VERSION"],
                    capture_output=True,
                    text=True,
                    timeout=1,
                )
                if result.returncode == 0 and result.stdout.strip():
                    # Output format: "glibc X.Y" or "X.Y"
                    ver_str = result.stdout.strip()
                    ver = ver_str.split()[-1] if "glibc" in ver_str else ver_str
            except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                pass

        # Fallback: use ctypes with dynamic libc detection
        if ver is None:
            import ctypes

            try:
                # Try to find libc dynamically instead of hardcoding "libc.so.6"
                libc = None
                # First try the standard name (works for 99.9% of Linux systems)
                for name in ["libc.so.6", "libc.so"]:
                    try:
                        libc = ctypes.CDLL(name)
                        break
                    except OSError:
                        continue

                # If standard names don't work, try to find via ldconfig
                if libc is None:
                    try:
                        result = subprocess.run(
                            ["ldconfig", "-p"],
                            capture_output=True,
                            text=True,
                            timeout=1,
                        )
                        if result.returncode == 0:
                            for line in result.stdout.split("\n"):
                                if "libc.so" in line:
                                    # Extract path from ldconfig output
                                    # Format: "libc.so.6 (libc6,x86-64) => /lib/x86_64-linux-gnu/libc.so.6"
                                    parts = line.split("=>")
                                    if len(parts) == 2:
                                        libc_path = parts[1].strip()
                                        libc = ctypes.CDLL(libc_path)
                                        break
                    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
                        pass

                if libc is not None:
                    # gnu_get_libc_version returns a const char* (C string)
                    # Set the return type to c_char_p to get the string properly
                    libc.gnu_get_libc_version.restype = ctypes.c_char_p
                    version_bytes = libc.gnu_get_libc_version()
                    ver = version_bytes.decode("ascii", "replace")
                else:
                    raise OSError("libc not found")
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


def get_version_add(sha: Optional[str] = None) -> str:
    command = "git config --global --add safe.directory " + pwd
    result = subprocess.run(command, shell=True, capture_output=False, text=True)
    deepep_root = os.path.dirname(os.path.abspath(__file__))
    add_version_path = os.path.join(os.path.join(deepep_root, "mooncake"), "version.py")
    version = "das.opt1"

    if add_git_version:
        if sha != 'Unknown':
            if sha is None:
                sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=deepep_root).decode('ascii').strip()
            version = f"{version}.{sha[:7]}"

    if os.getenv("ROCM_PATH"):
        rocm_path = os.getenv('ROCM_PATH', "")
        rocm_version_path = os.path.join(rocm_path, '.info', "rocm_version")
        with open(rocm_version_path, 'r',encoding='utf-8') as file:
            lines = file.readlines()
        rocm_version=lines[0].replace(".", "")
        version += ".dtk" + rocm_version

    new_version_content = f"""
try:
    __version__ = "0.3.12.post1"
    __version_tuple__ = (0, 3, 12, 'post1')
    __hcu_version__ = f'0.3.12.post1+{version}'

    from mooncake.version import __version__, __version_tuple__, __hcu_version__
except Exception as e:
    import warnings
    warnings.warn(f"Failed to read commit hash:\\n + str(e)",
                  RuntimeWarning, stacklevel=2)
    __version__ = "dev"
    __version_tuple__ = (0, 0, __version__)

def _prev_minor_version_was(version_str):
    '''Check whether a given version matches the previous minor version.

    Return True if version_str matches the previous minor version.

    For example - return True if the current version is 0.7.4 and the
    supplied version_str is '0.6'.

    Used for --show-hidden-metrics-for-version.
    '''
    # Match anything if this is a dev tree
    if __version_tuple__[0:2] == (0, 0):
        return True

    # Note - this won't do the right thing when we release 1.0!
    # assert __version_tuple__[0] == 0
    assert isinstance(__version_tuple__[1], int)
    return version_str == f"{{__version_tuple__[0]}}.{{__version_tuple__[1] - 1}}"

def _prev_minor_version():
    '''For the purpose of testing, return a previous minor version number.'''
    # In dev tree, this will return "0.-1", but that will work fine"
    assert isinstance(__version_tuple__[1], int)
    return f"{{__version_tuple__[0]}}.{{__version_tuple__[1] - 1}}"
"""

    with open(add_version_path, encoding="utf-8",mode="w") as file:
        file.write(new_version_content)
    file.close()


def get_version() -> str:
    get_version_add()
    version_file = 'mooncake/version.py'
    with open(version_file, encoding='utf-8') as f:
        exec(compile(f.read(), version_file, 'exec'))
    return locals()['__hcu_version__']


def get_mooncake_version() -> str:
    version = get_version()
    return version


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
    version=get_mooncake_version(),
    distclass=BinaryDistribution,
    cmdclass={"bdist_wheel": CustomBdistWheel},
)
