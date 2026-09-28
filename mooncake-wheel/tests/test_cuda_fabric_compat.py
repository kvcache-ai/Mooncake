"""Compile the compatibility declarations without requiring a CUDA toolkit."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "mooncake-transfer-engine"
COMPILER = shutil.which("c++")


@unittest.skipUnless(COMPILER, "C++ compiler is unavailable")
class CudaFabricCompatTest(unittest.TestCase):
    def test_old_and_new_toolkits_and_both_include_orders(self):
        for version in (12000, 12030, 13000):
            for tent_first in (False, True):
                with self.subTest(version=version, tent_first=tent_first):
                    headers = [
                        '"cuda_fabric_compat.h"',
                        '"tent/common/cuda_fabric_compat.h"',
                    ]
                    if tent_first:
                        headers.reverse()
                    source = f"""#define USE_CUDA 1
#define CUDA_VERSION {version}
enum CUdevice_attribute : unsigned int {{}};
enum CUmemAllocationHandleType : unsigned int {{}};
"""
                    if version >= 12030:
                        source += """
#define CU_IPC_HANDLE_SIZE 64
struct CUmemFabricHandle { unsigned char data[64]; };
constexpr auto CU_MEM_HANDLE_TYPE_FABRIC = (CUmemAllocationHandleType)8;
constexpr auto CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED =
    (CUdevice_attribute)128;
"""
                    source += "\n".join(f"#include {header}" for header in headers)
                    source += """
static_assert(sizeof(CUmemFabricHandle) == 64);
static_assert((int)CU_MEM_HANDLE_TYPE_FABRIC == 8);
static_assert((int)CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED == 128);
"""
                    subprocess.run(
                        [
                            COMPILER,
                            "-std=c++17",
                            "-fsyntax-only",
                            "-x",
                            "c++",
                            "-I",
                            str(ROOT / "include"),
                            "-I",
                            str(ROOT / "tent/include"),
                            "-",
                        ],
                        input=source,
                        text=True,
                        check=True,
                        capture_output=True,
                    )

    def test_standalone_mnnvl_header_does_not_need_classic_include_path(self):
        with tempfile.TemporaryDirectory() as directory:
            stubs = Path(directory)
            # Isolate the include boundary from unrelated toolkit/RPC headers.
            for name in (
                "cuda.h",
                "cuda_runtime.h",
                "tent/runtime/control_plane.h",
                "tent/runtime/transport.h",
                "tent/platform/cuda.h",
            ):
                path = stubs / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("")
            subprocess.run(
                [
                    COMPILER,
                    "-E",
                    "-x",
                    "c++",
                    "-I",
                    str(stubs),
                    "-I",
                    str(ROOT / "tent/include"),
                    "-",
                ],
                input='#include "tent/transport/mnnvl/mnnvl_transport.h"\n',
                text=True,
                check=True,
                capture_output=True,
            )


if __name__ == "__main__":
    unittest.main()
