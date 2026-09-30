# SPDX-License-Identifier: Apache-2.0
"""Host ELF regression for the Ascend GE boundary; no SDK/device is required."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(
    sys.platform == "linux"
    and all(shutil.which(x) for x in ("cmake", "c++", "nm", "c++filt", "readelf")),
    "Linux ELF build tools required",
)
class AscendGeBoundaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.source = Path(cls.temp.name)
        (cls.source / "fixture.cpp").write_text(
            r"""
#include <string>
namespace ge {
struct StatusFactory {
    std::string value;
    static StatusFactory &Instance() {
        static StatusFactory instance;
        return instance;
    }
    __attribute__((noinline)) void RegisterErrorNo(unsigned, const char *s) {
        value = s;
    }
    __attribute__((noinline)) ~StatusFactory() { value.clear(); }
};
struct ErrorNoRegisterar {
    __attribute__((noinline)) ErrorNoRegisterar() {
        StatusFactory::Instance().RegisterErrorNo(1, "fixture");
    }
};
}
extern "C" void *registry_address() {
    ge::ErrorNoRegisterar registration;
    return &ge::StatusFactory::Instance();
}
extern "C" int retained_api() { return 37; }
"""
        )
        (cls.source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.16)\n"
            "project(ge_boundary CXX)\n"
            f'include("{ROOT / "cmake/AscendGeBoundary.cmake"}")\n'
            "add_library(sdk SHARED fixture.cpp)\n"
            "foreach(target transfer_engine ascend_transport)\n"
            "  add_library(${target} SHARED fixture.cpp)\n"
            "  mooncake_isolate_ascend_ge(${target})\n"
            "endforeach()\n"
        )
        for mode in ("ON", "OFF"):
            build = cls.source / mode
            subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(cls.source),
                    "-B",
                    str(build),
                    f"-DUSE_ASCEND_DIRECT={mode}",
                ],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                ["cmake", "--build", str(build)],
                check=True,
                capture_output=True,
            )

    def symbols(self, path):
        text = subprocess.check_output(
            ["nm", "-D", "--defined-only", "--format=posix", str(path)],
            text=True,
        )
        return {row.split()[0] for row in text.splitlines()}

    def test_only_error_registry_exports_are_localized(self):
        for target in ("transfer_engine", "ascend_transport"):
            name = f"lib{target}.so"
            before = self.symbols(self.source / "OFF" / name)
            after = self.symbols(self.source / "ON" / name)
            names = sorted(before)
            demangled = subprocess.check_output(
                ["c++filt", *names],
                text=True,
            ).splitlines()
            expected = {
                symbol
                for symbol, readable in zip(names, demangled)
                if "ge::StatusFactory" in readable
                or "ge::ErrorNoRegisterar" in readable
            }
            self.assertTrue(expected)
            self.assertTrue(any("guard variable" in s for s in demangled))
            self.assertEqual(before - after, expected)
            self.assertEqual(after - before, set())
            self.assertIn("retained_api", after)

    def test_sdk_and_mooncake_registries_exit_independently(self):
        code = r"""
import ctypes, json, sys
paths = json.loads(sys.argv[1])
handles = [ctypes.CDLL(p, mode=ctypes.RTLD_GLOBAL) for p in paths]
addresses = []
for handle in handles:
    handle.registry_address.restype = ctypes.c_void_p
    addresses.append(handle.registry_address())
    assert handle.retained_api() == 37
assert len(set(addresses)) == len(addresses), addresses
"""
        paths = [
            str(self.source / "ON" / name)
            for name in ("libsdk.so", "libtransfer_engine.so", "libascend_transport.so")
        ]
        for order in (paths, paths[::-1]):
            result = subprocess.run(
                [sys.executable, "-I", "-c", code, json.dumps(order)],
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
