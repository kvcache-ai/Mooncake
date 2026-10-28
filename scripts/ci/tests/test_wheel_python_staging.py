"""Regression coverage for the combined wheel's migrated Python modules."""

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class WheelPythonStagingTest(unittest.TestCase):
    def test_structured_ranges_is_staged_with_its_consumer(self):
        script = (ROOT / "scripts/build_wheel.sh").read_text()
        array_start = script.index("MIGRATED_PYTHON_MODULES=(")
        array_end = script.index("\n)", array_start) + 2
        loop_start = script.index(
            'for module in "${MIGRATED_PYTHON_MODULES[@]}"; do',
            script.index('cp "${MIGRATED_PYTHON_STAGING_DIR}/__init__.py"'),
        )
        loop_end = script.index("\ndone", loop_start) + len("\ndone")
        with tempfile.TemporaryDirectory(dir=ROOT) as staging:
            subprocess.run(
                [
                    "bash",
                    "-eu",
                    "-c",
                    'MIGRATED_PYTHON_SOURCE_DIR="$1"\n'
                    'MIGRATED_PYTHON_STAGING_DIR="$2"\n'
                    + script[array_start:array_end]
                    + "\n"
                    + script[loop_start:loop_end],
                    "wheel-staging",
                    str(ROOT / "python/mooncake"),
                    staging,
                ],
                check=True,
            )
            for module in ("structured_object_store.py", "_structured_ranges.py"):
                self.assertTrue((Path(staging) / module).is_file(), module)
                self.assertEqual(
                    (Path(staging) / module).read_bytes(),
                    (ROOT / "python/mooncake" / module).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
