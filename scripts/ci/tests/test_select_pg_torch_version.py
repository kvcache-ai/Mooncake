# Copyright 2026 Mooncake contributors
# SPDX-License-Identifier: Apache-2.0

"""Regression coverage for wheel-specific PyTorch version selection."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


SCRIPT = Path(__file__).resolve().parents[1] / "select_pg_torch_version.py"
SPEC = importlib.util.spec_from_file_location("select_pg_torch_version", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
SELECTOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SELECTOR)


class SelectPgTorchVersionTests(unittest.TestCase):
    def test_latest_bundled_patch_is_selected(self):
        members = [
            "mooncake/pg_2_13_0.cpython-312-x86_64-linux-gnu.so",
            "mooncake/pg_2_14_0.cpython-312-x86_64-linux-gnu.so",
        ]
        self.assertEqual(SELECTOR.select_torch_requirement(members), "torch==2.14.0")

    def test_versions_are_sorted_numerically(self):
        members = ["mooncake/pg_2_9_1.so", "mooncake/pg_2_11_0.so"]
        self.assertEqual(SELECTOR.select_torch_requirement(members), "torch==2.11.0")

    def test_patch_versions_are_sorted_numerically(self):
        members = ["mooncake/pg_2_12_9.so", "mooncake/pg_2_12_10.so"]
        self.assertEqual(SELECTOR.select_torch_requirement(members), "torch==2.12.10")

    def test_a_new_bundled_patch_is_picked_up_automatically(self):
        members = ["mooncake/pg_2_14_0.so", "mooncake/pg_2_14_1.so"]
        self.assertEqual(SELECTOR.select_torch_requirement(members), "torch==2.14.1")

    def test_unrelated_files_do_not_change_the_requirement(self):
        members = [
            "mooncake/pg.py",
            "mooncake/pg_2_99_0.py",
            "other/pg_2_99_0.so",
            "mooncake/.libs/pg_2_99_0.so",
            "mooncake/pg_2_14_0.cpython-312-x86_64-linux-gnu.so",
        ]
        self.assertEqual(SELECTOR.select_torch_requirement(members), "torch==2.14.0")

    def test_non_pg_wheel_preserves_unpinned_torch(self):
        self.assertEqual(
            SELECTOR.select_torch_requirement(["mooncake/engine.so", "mooncake/pg.py"]),
            "torch",
        )

    def test_cli_reads_the_wheel_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "mooncake.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("mooncake/pg_2_13_0.so", b"")
                archive.writestr("mooncake/pg_2_14_0.so", b"")
            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(wheel)],
                capture_output=True,
                text=True,
                check=True,
            )
            self.assertEqual(result.stdout.strip(), "torch==2.14.0")

    def test_invalid_archive_fails_instead_of_silently_falling_back(self):
        with tempfile.TemporaryDirectory() as directory:
            wheel = Path(directory) / "mooncake.whl"
            wheel.write_bytes(b"not a wheel archive")
            result = subprocess.run(
                [sys.executable, str(SCRIPT), str(wheel)],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("BadZipFile", result.stderr)


if __name__ == "__main__":
    unittest.main()
