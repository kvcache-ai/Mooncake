import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[3] / ".github/actions/run-ctest/run.sh"


class RunCTestTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        (self.root / "build").mkdir()

    def run_suite(self, **overrides):
        env = {
            **os.environ,
            "SCRIPT": str(SCRIPT),
            "SUITE_TMP": str(self.root),
            "GITHUB_WORKSPACE": str(self.root),
            "RESERVE_RPC_PORT": "false",
            "JUNIT_REPORT": "",
            "LD_LIBRARY_PATH": "/test/lib",
            "EXISTING_PORTS": "",
            "CTEST_STATUS": "0",
        }
        # An inherited workflow setting must not affect the unset-value case.
        env.pop("CTEST_LABEL_EXCLUDE", None)
        env.pop("CTEST_EXCLUDE_REGEX", None)
        env.pop("CTEST_TIMEOUT", None)
        env.update(overrides)
        return subprocess.run(
            [
                "bash",
                "-c",
                """
nproc() { echo 6; }
sysctl() {
  printf '%s\\0' "$@" >> "$SUITE_TMP/sysctl.args"
  if [ "$1" = -n ]; then printf '%s\\n' "$EXISTING_PORTS"; fi
}
sudo() {
  printf '%s\\0' "$@" >> "$SUITE_TMP/sudo.args"
  "$@"
}
ctest() {
  printf '%s\\0' "$@" > "$SUITE_TMP/ctest.args"
  printf '%s\\0' "$PWD" "$LD_LIBRARY_PATH" "$MC_METADATA_SERVER" \\
    "$DEFAULT_KV_LEASE_TTL" > "$SUITE_TMP/ctest.env"
  return "$CTEST_STATUS"
}
source "$SCRIPT"
""",
            ],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def recorded(self, name):
        return (self.root / name).read_bytes().decode().split("\0")[:-1]

    def test_unset_or_empty_filter_preserves_default_sweep(self):
        for overrides in ({}, {"CTEST_LABEL_EXCLUDE": ""}):
            with self.subTest(overrides=overrides):
                result = self.run_suite(**overrides)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    self.recorded("ctest.args"),
                    ["--parallel", "6", "--output-on-failure"],
                )
                self.assertEqual(
                    self.recorded("ctest.env"),
                    [
                        str(self.root / "build"),
                        "/test/lib:/usr/local/lib",
                        "http://127.0.0.1:8080/metadata",
                        "500",
                    ],
                )
                self.assertFalse((self.root / "sudo.args").exists())
                self.assertFalse((self.root / "sysctl.args").exists())

    def test_label_regex_is_one_literal_argument(self):
        regex = "^(nvlink_gpu|other hardware[0-9]*)$"
        result = self.run_suite(CTEST_LABEL_EXCLUDE=regex)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.recorded("ctest.args"),
            ["--parallel", "6", "--output-on-failure", "--label-exclude", regex],
        )

    def test_junit_report_and_filter_coexist(self):
        report = "build/test results/ctest.xml"
        result = self.run_suite(JUNIT_REPORT=report, CTEST_LABEL_EXCLUDE="^nvlink_gpu$")
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.recorded("ctest.args")
        self.assertEqual(args[:3], ["--parallel", "6", "--output-on-failure"])
        self.assertEqual(len(args), 7)
        self.assertEqual(
            args[args.index("--output-junit") + 1], str(self.root / report)
        )
        self.assertEqual(args[args.index("--label-exclude") + 1], "^nvlink_gpu$")
        self.assertTrue((self.root / report).parent.is_dir())

    def test_port_reservation_preserves_existing_ports(self):
        for existing in ("", "12345,23456-23460"):
            with self.subTest(existing=existing):
                for name in ("sysctl.args", "sudo.args"):
                    (self.root / name).unlink(missing_ok=True)
                result = self.run_suite(
                    RESERVE_RPC_PORT="true", EXISTING_PORTS=existing
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                value = f"{existing},50052" if existing else "50052"
                assignment = f"net.ipv4.ip_local_reserved_ports={value}"
                self.assertEqual(
                    self.recorded("sysctl.args"),
                    ["-n", "net.ipv4.ip_local_reserved_ports", "-w", assignment],
                )
                self.assertEqual(
                    self.recorded("sudo.args"), ["sysctl", "-w", assignment]
                )

    def test_ctest_failure_status_propagates(self):
        result = self.run_suite(CTEST_STATUS="23", CTEST_LABEL_EXCLUDE="nvlink_gpu")
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertIn("--label-exclude", self.recorded("ctest.args"))

    def test_exclude_regex_and_timeout_coexist_with_label_exclude_and_junit(self):
        regex = "^efa_(transport|c_api|gpu_loopback)_test$"
        report = "build/test-results/ctest.xml"
        result = self.run_suite(
            JUNIT_REPORT=report,
            CTEST_LABEL_EXCLUDE="^nvlink_gpu$",
            CTEST_EXCLUDE_REGEX=regex,
            CTEST_TIMEOUT="600",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.recorded("ctest.args")
        self.assertEqual(args[args.index("--label-exclude") + 1], "^nvlink_gpu$")
        self.assertEqual(args[args.index("--exclude-regex") + 1], regex)
        self.assertEqual(args[args.index("--timeout") + 1], "600")
        self.assertEqual(
            args[args.index("--output-junit") + 1], str(self.root / report)
        )

    def test_cost_data_seeded_only_when_absent(self):
        cost_file = self.root / "build/Testing/Temporary/CTestCostData.txt"
        result = self.run_suite()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(cost_file.is_file())
        seeded = cost_file.read_text()
        self.assertIn("pybind_client_test 1 390", seeded)
        self.assertIn("master_service_ha_test 1 217", seeded)
        self.assertTrue(seeded.rstrip().endswith("---"))

        custom = "custom_test 1 42\n---\n"
        cost_file.write_text(custom)
        result = self.run_suite()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(cost_file.read_text(), custom)


if __name__ == "__main__":
    unittest.main()
