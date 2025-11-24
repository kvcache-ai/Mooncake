import os
import subprocess
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
            ver = (
                ctypes.CDLL("libc.so.6")
                .gnu_get_libc_version()
                .decode("ascii", "replace")
            )
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
if int(os.getenv("BUILD_WITH_EP", "0")):
    import torch
    from torch.utils.cpp_extension import BuildExtension, CUDAExtension
    abi_flag = int(torch._C._GLIBCXX_USE_CXX11_ABI)
    current_dir = os.path.abspath(os.path.dirname(__file__))

    class MooncakeBuildExt(BuildExtension):
        def __init__(self, *args, **kwargs):
            kwargs["force"] = True   # force rebuild for each torch version
            super().__init__(*args, **kwargs)

        def build_extension(self, ext):
            if hasattr(ext, "torch_version"):
                print(f"Installing torch=={ext.torch_version} for '{ext.name}'")
                subprocess.check_call([sys.executable, "-m", "pip", "install", f"torch=={ext.torch_version}"])
            super().build_extension(ext)

    ext_modules = []
    torch_versions_str = os.getenv("EP_TORCH_VERSIONS", "")
    for torch_version in torch_versions_str.split(';'):
        version_suffix = "_" + torch_version.replace(".", "_")
        ext = CUDAExtension(
            name="mooncake.ep" + version_suffix,
            include_dirs=[
                os.path.join(current_dir, "../mooncake-ep/include"),
                os.path.join(current_dir, "../mooncake-transfer-engine/include"),
            ],
            sources=[
                "../mooncake-integration/ep/ep_py.cpp",
                "../mooncake-ep/src/mooncake_backend.cpp",
                "../mooncake-ep/src/mooncake_ep_buffer.cpp",
                "../mooncake-ep/src/mooncake_ep_kernel.cu",
                "../mooncake-ep/src/mooncake_worker.cu",
                "../mooncake-ep/src/mooncake_worker_thread.cpp",
                "../mooncake-ep/src/mooncake_ibgda/mlx5gda.cpp",
            ],
            extra_compile_args={
                "cxx": [f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}", "-std=c++20", "-O3", "-g0"],
                "nvcc": [f"-D_GLIBCXX_USE_CXX11_ABI={abi_flag}", "-std=c++20"],
            },
            libraries=["ibverbs", "mlx5"],
            extra_objects=[
                os.path.join(current_dir, "mooncake/engine.so"),
            ],
        )
        ext.torch_version = torch_version
        ext_modules.append(ext)
    setup(
        distclass=BinaryDistribution,
        cmdclass={
            "bdist_wheel": CustomBdistWheel,
            "build_ext": MooncakeBuildExt,
        },
        ext_modules=ext_modules,
    )
else:
    setup(
        distclass=BinaryDistribution,
        cmdclass={"bdist_wheel": CustomBdistWheel},
    )
