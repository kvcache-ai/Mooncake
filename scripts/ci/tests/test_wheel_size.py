"""Exercise wheel stripping and size limits without a native Mooncake build."""

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
BUILD_SCRIPT = (ROOT / "scripts/build_wheel.sh").read_text()


def script_block(start, end):
    return BUILD_SCRIPT.split(start, 1)[1].split(end, 1)[0]


class WheelSizeTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.package = self.directory / "mooncake-wheel/mooncake"
        self.package.mkdir(parents=True)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        # The builder runs on Linux. Use GNU stat for its size gate on macOS.
        stat = shutil.which("gstat")
        if stat:
            (self.bin / "stat").symlink_to(stat)

    def run_block(self, source, **environment):
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", source],
            cwd=self.directory,
            env={
                **os.environ,
                "PATH": f"{self.bin}:{os.environ['PATH']}",
                "CI": "true",
                "NPU_BUILD": "0",
                "KEEP_DEBUG_SYMBOLS": "0",
                "WHEEL_PACKAGE_DIR": str(self.package),
                "OUTPUT_DIR": str(self.directory),
                **environment,
            },
            capture_output=True,
            text=True,
            timeout=20,
        )

    def strip(self, **environment):
        return self.run_block(
            script_block(
                'WHEEL_PACKAGE_DIR="$(pwd)/mooncake-wheel/mooncake"',
                "# Make the post-repair device library discoverable",
            ),
            **environment,
        )

    def test_local_build_and_explicit_opt_out_preserve_debug_files(self):
        library = self.package / "engine.so"
        library.write_bytes(b"developer build")
        for options in ({"CI": "false"}, {"KEEP_DEBUG_SYMBOLS": "1"}):
            with self.subTest(options=options):
                result = self.strip(STRIP="missing-strip-tool", **options)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(library.read_bytes(), b"developer build")

    def test_missing_strip_tool_fails_ci(self):
        result = self.strip(STRIP="missing-strip-tool")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not found", result.stdout)

    def test_real_elf_loses_debug_but_keeps_code_and_symbols(self):
        compiler = shutil.which("clang")
        strip = shutil.which("llvm-strip") or shutil.which("strip")
        readelf = shutil.which("llvm-readelf") or shutil.which("readelf")
        if not all((compiler, strip, readelf)):
            self.skipTest("clang, ELF strip and readelf are required")
        library = self.package / "engine.so"
        subprocess.run(
            [
                compiler,
                "--target=x86_64-linux-gnu",
                "-g",
                "-c",
                "-x",
                "c",
                "-o",
                str(library),
                "-",
            ],
            input="int mooncake_test(int value) { return value + 7; }\n",
            text=True,
            check=True,
            capture_output=True,
        )

        def inspect(*arguments):
            return subprocess.check_output(
                [readelf, *arguments, str(library)], text=True
            )

        self.assertIn(".debug_info", inspect("-S"))
        before_size = library.stat().st_size
        before_code = inspect("--hex-dump=.text")
        script = self.package / "launcher"
        script.write_text("#!/bin/sh\necho launch\n")
        script.chmod(0o755)
        result = self.strip(STRIP=strip)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(".debug_info", inspect("-S"))
        self.assertIn(".symtab", inspect("-S"))
        self.assertIn("mooncake_test", inspect("-s"))
        self.assertEqual(inspect("--hex-dump=.text"), before_code)
        self.assertLess(library.stat().st_size, before_size)
        self.assertEqual(script.read_text(), "#!/bin/sh\necho launch\n")

        # A strip failure must stop packaging rather than upload an oversized wheel.
        failing_strip = self.bin / "failing-strip"
        failing_strip.write_text("#!/bin/sh\nexit 23\n")
        failing_strip.chmod(0o755)
        self.assertNotEqual(self.strip(STRIP=str(failing_strip)).returncode, 0)

    def test_non_npu_keeps_dependency_search_paths_for_auditwheel(self):
        library = self.package / "engine.so"
        original = b"\x7fELF" + bytes(60)
        library.write_bytes(original)
        patchelf = self.bin / "patchelf"
        called = self.directory / "patchelf-called"
        patchelf.write_text(f"#!/bin/sh\ntouch {shlex.quote(str(called))}\nexit 1\n")
        patchelf.chmod(0o755)
        result = self.run_block(
            script_block(
                "# Run after installing patchelf above so the supported version is used.\n",
                "# Create directory for repaired wheels",
            ),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(called.exists())
        self.assertEqual(library.read_bytes(), original)

    def check_size(self, **environment):
        return self.run_block(
            "wheels=(" + script_block("wheels=(", "\ncd .."), **environment
        )

    def test_empty_output_is_rejected(self):
        result = self.check_size()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no wheel found", result.stdout)

    def test_size_limit_is_checked_without_rounding(self):
        wheel = self.directory / "test.whl"
        limit = 100 * 1024 * 1024
        for size, expected in ((limit - 1, 0), (limit, 0), (limit + 1, 1)):
            with self.subTest(size=size):
                with wheel.open("wb") as output:
                    output.truncate(size)
                result = self.check_size()
                self.assertEqual(result.returncode, expected, result.stderr)

        result = self.check_size(CI="false")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Warning:", result.stdout)


if __name__ == "__main__":
    unittest.main()
