import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ACTIONS = Path(__file__).resolve().parents[1]


class SharedActionsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "build").mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(self.root),
            "GITHUB_ENV": str(self.root / "github-env"),
            "RESERVE_RPC_PORT": "false",
            "JUNIT_REPORT": "",
        }

    def command(self, name, body):
        path = self.bin / name
        path.write_text("#!/usr/bin/env bash\n" + body)
        path.chmod(0o755)

    def run_action(self, name):
        return subprocess.run(
            ["bash", str(ACTIONS / name / "run.sh")],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_ctest_arguments_environment_and_failure(self):
        self.command("nproc", "echo 4\n")
        self.command(
            "ctest",
            'printf "%s\\n" "$PWD" "$@" "$MC_METADATA_SERVER" '
            '"$DEFAULT_KV_LEASE_TTL" "$LD_LIBRARY_PATH"\nexit 23\n',
        )
        result = self.run_action("run-ctest")
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn(str(self.root / "build"), result.stdout)
        self.assertIn("--parallel\n4\n--output-on-failure", result.stdout)
        self.assertIn("http://127.0.0.1:8080/metadata\n500\n", result.stdout)
        self.assertNotIn("--output-junit", result.stdout)
        self.assertIn("/usr/local/lib", result.stdout)

    def test_ctest_junit_and_port_reservation(self):
        self.env.update(RESERVE_RPC_PORT="true", JUNIT_REPORT="test results/ctest.xml")
        self.command("sudo", 'exec "$@"\n')
        self.command(
            "sysctl",
            'if [ "$1" = -n ]; then echo 1234; else echo "$*"; fi\n',
        )
        self.command("ctest", 'printf "%s\\n" "$@"\n')
        result = self.run_action("run-ctest")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("net.ipv4.ip_local_reserved_ports=1234,50052", result.stdout)
        self.assertIn(
            f"--output-junit\n{self.root}/test results/ctest.xml", result.stdout
        )
        self.assertTrue((self.root / "test results").is_dir())

    def test_cuda_driver_missing_fails(self):
        (self.root / "build/CMakeCache.txt").write_text("")
        result = self.run_action("setup-cuda-runtime")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CMake did not resolve", result.stdout)

    def test_cuda_driver_link_and_environment_are_idempotent(self):
        driver = self.root / "cuda stubs/libcuda.so"
        driver.parent.mkdir()
        driver.touch()
        (self.root / "build/CMakeCache.txt").write_text(
            f"CUDA_cuda_driver_LIBRARY:FILEPATH={driver}\n"
        )
        self.command("sudo", 'exec "$@"\n')
        for _ in range(2):
            result = self.run_action("setup-cuda-runtime")
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((driver.parent / "libcuda.so.1").resolve(), driver)
        exports = (self.root / "github-env").read_text()
        self.assertIn(f"LIBRARY_PATH={driver.parent}:", exports)
        self.assertIn(f"LD_LIBRARY_PATH={driver.parent}:", exports)


if __name__ == "__main__":
    unittest.main()
