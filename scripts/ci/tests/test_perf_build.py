"""Exercise workload freezing against changes to CMake targets and parent flags."""

import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "performance/freeze_build.py"
SPEC = importlib.util.spec_from_file_location("freeze_build", SCRIPT)
build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build)


class PerformanceBuildTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.baseline, self.candidate = self.root / "base", self.root / "candidate"
        for root, value in ((self.baseline, 11), (self.candidate, 23)):
            (root / "bench").mkdir(parents=True)
            (root / "cmake").mkdir()
            (root / "CMakeLists.txt").write_text("""cmake_minimum_required(VERSION 3.16)
project(frozen_workload LANGUAGES CXX)
file(GLOB recipes "${CMAKE_CURRENT_SOURCE_DIR}/cmake/*.cmake")
foreach(recipe IN LISTS recipes)
  include("${recipe}")
endforeach()
add_subdirectory(bench)
""")
            (root / "cmake/flags.cmake").write_text(
                "add_compile_definitions(WORKLOAD=7)\n"
            )
            (root / "bench/CMakeLists.txt").write_text(
                "add_executable(bench main.cpp ../implementation.cpp)\n"
            )
            (root / "bench/main.cpp").write_text(
                "#include <iostream>\nint implementation();\nint main() { std::cout << WORKLOAD + implementation(); }\n"
            )
            (root / "implementation.cpp").write_text(
                f"int implementation() {{ return {value}; }}\n"
            )
            self.git(root, "init", "-q")
            self.git(root, "add", ".")

    def git(self, root, *args):
        subprocess.run(
            ["git", "-C", str(root), *args], check=True, capture_output=True, timeout=10
        )

    def redirect_candidate(self):
        (self.candidate / "CMakeLists.txt").write_text(
            'message(FATAL_ERROR "candidate project used")\n'
        )
        (self.candidate / "bench/CMakeLists.txt").write_text(
            'message(FATAL_ERROR "candidate target used")\n'
        )
        (self.candidate / "cmake/flags.cmake").write_text(
            "add_compile_definitions(WORKLOAD=999)\n"
        )
        (self.candidate / "cmake/extra.cmake").write_text(
            'message(FATAL_ERROR "new recipe used")\n'
        )
        self.git(self.candidate, "add", ".")

    def test_freezes_parent_flags_targets_and_removes_added_recipes(self):
        self.redirect_candidate()
        implementation = (self.candidate / "implementation.cpp").read_bytes()
        manifest = build.freeze(self.baseline, self.candidate)
        self.assertEqual(len(manifest["sha256"]), 64)
        self.assertEqual(
            set(manifest["files"]),
            {"CMakeLists.txt", "bench/CMakeLists.txt", "cmake/flags.cmake"},
        )
        for name in manifest["files"]:
            self.assertEqual(
                (self.baseline / name).read_bytes(),
                (self.candidate / name).read_bytes(),
            )
        self.assertFalse((self.candidate / "cmake/extra.cmake").exists())
        self.assertEqual(
            (self.candidate / "implementation.cpp").read_bytes(), implementation
        )

    def test_refuses_symlinked_build_files_or_parent_directories(self):
        for name in ("CMakeLists.txt", "cmake"):
            with self.subTest(name=name):
                path = self.candidate / name
                saved = path.with_name(path.name + ".saved")
                path.rename(saved)
                path.symlink_to(self.baseline / name)
                try:
                    with self.assertRaisesRegex(ValueError, "symlink"):
                        build.freeze(self.baseline, self.candidate)
                finally:
                    path.unlink()
                    saved.rename(path)

    @unittest.skipUnless(
        shutil.which("cmake") and shutil.which("c++"), "CMake and C++ are required"
    )
    def test_native_build_uses_frozen_workload_and_candidate_implementation(self):
        self.redirect_candidate()
        build.freeze(self.baseline, self.candidate)
        for root, expected in ((self.baseline, "18"), (self.candidate, "30")):
            directory = self.root / (root.name + "-build")
            for command in (
                [
                    "cmake",
                    "-S",
                    str(root),
                    "-B",
                    str(directory),
                    "-DCMAKE_BUILD_TYPE=Release",
                    "-DCMAKE_CXX_COMPILER=c++",
                ],
                ["cmake", "--build", str(directory), "--target", "bench"],
            ):
                process = subprocess.run(
                    command, capture_output=True, text=True, timeout=60
                )
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
            self.assertEqual(
                subprocess.check_output(
                    [str(directory / "bench/bench")], text=True, timeout=10
                ),
                expected,
            )


if __name__ == "__main__":
    unittest.main()
