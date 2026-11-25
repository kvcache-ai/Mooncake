from pathlib import Path
import subprocess
import sys
import unittest


class BenchmarkEntrypointTest(unittest.TestCase):
    def test_cli_help(self) -> None:
        benchmark_dir = Path(__file__).resolve().parent
        for directory, args in (
            (benchmark_dir.parents[1], ["-m", "benchmarks.storage_benchmark_v1"]),
            (benchmark_dir, ["benchmark.py"]),
        ):
            with self.subTest(args=args):
                result = subprocess.run(
                    [sys.executable, *args, "--help"],
                    cwd=directory,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--trace-dir", result.stdout)
