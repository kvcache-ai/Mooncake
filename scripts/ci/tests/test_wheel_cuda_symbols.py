import os
from pathlib import Path
import subprocess
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]


class WheelCudaSymbolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = yaml.safe_load(
            (REPOSITORY / ".github/workflows/_build-wheel.yaml").read_text()
        )
        script = next(
            step["run"]
            for step in workflow["jobs"]["build"]["steps"]
            if step["name"] == "Smoke test repaired wheel"
        )
        cls.symbol_check = script.split(
            'cuda_symbols=$(nm -D --undefined-only "$file")', 1
        )[1].split('if [ -n "$cuda_symbols" ]; then', 1)[0]

    def filter_symbols(self, symbols):
        script = (
            'cuda_symbols="$SYMBOLS"\n'
            + self.symbol_check
            + 'printf "%s" "$cuda_symbols"\n'
        )
        result = subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", script],
            env={**os.environ, "SYMBOLS": symbols},
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        return result.stdout.splitlines()

    def test_cufile_symbols_do_not_trigger_driver_check(self):
        self.assertEqual(
            self.filter_symbols(
                "                 U cuFileBatchIOCancel\n"
                "                 U cuFileDriverOpen\n"
                "                 U cuFileHandleRegister@LIBCUFILE_1.0"
            ),
            [],
        )

    def test_driver_symbols_are_still_reported(self):
        symbols = (
            "                 U cuInit\n"
            "                 U cuMemCreate@libcuda.so.1\n"
            "                 w cuCtxCreate_v2\n"
            "                 U cuFileDriverOpen"
        )
        self.assertEqual(self.filter_symbols(symbols), symbols.splitlines()[:3])


if __name__ == "__main__":
    unittest.main()
