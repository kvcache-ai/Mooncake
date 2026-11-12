import argparse
from importlib.machinery import EXTENSION_SUFFIXES
import sys
import platform
from pathlib import Path
import subprocess


def read_architecture(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as exc:
        sys.exit(f"Error: cannot read Store architecture from {command}: {exc}")
    if result.returncode:
        sys.exit(
            f"Error: Store identity command failed ({result.returncode}): {command}\n"
            f"{result.stdout}{result.stderr}"
        )
    values = [
        line.partition("=")[2]
        for line in result.stdout.splitlines()
        if line.startswith("architecture=")
    ]
    if len(values) != 1 or values[0] not in ("centralized", "p2p"):
        sys.exit(
            f"Error: invalid Store architecture from {command}:\n"
            f"{result.stdout}{result.stderr}"
        )
    return values[0]


def check_store_artifacts(package_dir, with_store=None, expected_architecture=None):
    package_dir = Path(package_dir).resolve()
    binaries = [package_dir / name for name in ("mooncake_master", "mooncake_client")]
    extensions = list(package_dir.glob("_store.*so"))
    legacy_extensions = list(package_dir.glob("store.*so"))
    present = extensions + legacy_extensions + [p for p in binaries if p.exists()]

    # A standalone legacy sdist has no CMake cache; any Store artifact requires
    # the complete trio. The two packaging entry points pass the actual option.
    enabled = with_store == "ON" if with_store else bool(present)
    if not enabled:
        if present:
            sys.exit(f"Error: Store artifacts remain with WITH_STORE=OFF: {present}")
        return
    if legacy_extensions:
        sys.exit(f"Error: obsolete Store extensions in {package_dir}: {legacy_extensions}")
    if len(extensions) != 1 or extensions[0].name not in {
        "_store" + suffix for suffix in EXTENSION_SUFFIXES
    }:
        sys.exit(
            f"Error: expected one _store extension for {sys.executable} in "
            f"{package_dir}, found {extensions}"
        )
    for binary in binaries:
        if not binary.is_file():
            sys.exit(f"Error: missing required Store binary: {binary}")

    architectures = {
        binary.name: read_architecture([str(binary), "--version"])
        for binary in binaries
    }
    # Load exactly the staged extension without importing a source-tree facade
    # or consulting MOONCAKE_STORE_BACKEND.
    architectures["_store"] = read_architecture(
        [
            sys.executable,
            "-I",
            "-c",
            "import importlib.util, sys; "
            "spec = importlib.util.spec_from_file_location('_store', sys.argv[1]); "
            "module = importlib.util.module_from_spec(spec); "
            "spec.loader.exec_module(module); "
            "print('architecture=' + module._store_architecture)",
            str(extensions[0]),
        ]
    )
    if len(set(architectures.values())) != 1 or (
        expected_architecture
        and architectures["_store"] != expected_architecture
    ):
        sys.exit(
            f"Error: Store architecture mismatch in {package_dir}: "
            f"expected={expected_architecture}, actual={architectures}"
        )


# CMake and the release script share this check without starting setuptools.
if sys.argv[1:2] == ["--check-store-artifacts"]:
    parser = argparse.ArgumentParser(description="Check native Store build identities")
    parser.add_argument("package_dir", type=Path)
    parser.add_argument("--with-store", choices=("ON", "OFF"))
    parser.add_argument("--expected-architecture", choices=("centralized", "p2p"))
    args = parser.parse_args(sys.argv[2:])
    check_store_artifacts(args.package_dir, args.with_store, args.expected_architecture)
    sys.exit(0)


from setuptools import setup, Distribution  # noqa: E402
from wheel.bdist_wheel import bdist_wheel  # noqa: E402

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
check_store_artifacts(Path(__file__).resolve().parent / "mooncake")

setup(
    distclass=BinaryDistribution,
    cmdclass={"bdist_wheel": CustomBdistWheel},
)
