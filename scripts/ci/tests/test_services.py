import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SERVICES = Path(__file__).resolve().parents[1] / "services.sh"


class ServicesTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        os.mkfifo(self.root / "ready")
        os.mkfifo(self.root / "hold")

    def run_suite(self, body):
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", body],
            env={**os.environ, "SERVICES": str(SERVICES), "SUITE_TMP": str(self.root)},
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_failure_preserves_status_logs_and_cleans_all_services(self):
        result = self.run_suite(
            """
source "$SERVICES"
trap 'ci_cleanup_services "$?"' EXIT
for name in metadata master; do
  ci_start_service "$name" "$SUITE_TMP/$name.log" bash -c '
    echo "startup: $1"
    echo ready > "$2/ready"
    exec cat "$2/hold"
  ' _ "$name" "$SUITE_TMP"
  echo "${CI_SERVICE_PIDS[$name]}" >> "$SUITE_TMP/pids"
  read -r ready < "$SUITE_TMP/ready"
done
exit 23
"""
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIn("startup: metadata", result.stdout)
        self.assertIn("startup: master", result.stdout)
        for pid in (self.root / "pids").read_text().splitlines():
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid), 0)

    def test_wait_requires_every_port_and_stop_is_idempotent(self):
        result = self.run_suite(
            """
source "$SERVICES"
trap 'ci_cleanup_services "$?"' EXIT
ci_start_service master "$SUITE_TMP/master.log" cat "$SUITE_TMP/hold"
ss() {
  echo "$*" >> "$SUITE_TMP/probes"
  if [[ "$*" == *50051 ]]; then
    echo listening
  elif [ -f "$SUITE_TMP/retried" ]; then
    echo listening
  fi
}
sleep() { touch "$SUITE_TMP/retried"; }
ci_wait_service master 50051 8080
ci_stop_service master
ci_stop_service master
"""
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        probes = (self.root / "probes").read_text()
        self.assertEqual(probes.count(":8080"), 2)
        self.assertEqual(probes.count(":50051"), 2)

    def test_startup_exit_is_reported_with_server_log(self):
        result = self.run_suite(
            """
source "$SERVICES"
trap 'ci_cleanup_services "$?"' EXIT
ci_start_service master "$SUITE_TMP/master.log" bash -c 'echo startup-failed; exit 7'
wait "${CI_SERVICE_PIDS[master]}" || true
ci_wait_service master 50051
"""
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("exited before becoming ready", result.stdout)
        self.assertIn("startup-failed", result.stdout)

    def test_readiness_timeout_fails_without_wall_clock_delay(self):
        result = self.run_suite(
            """
source "$SERVICES"
trap 'ci_cleanup_services "$?"' EXIT
ci_start_service master "$SUITE_TMP/master.log" cat "$SUITE_TMP/hold"
ss() { return 0; }
sleep() { :; }
ci_wait_service master 50051
"""
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("did not listen", result.stdout)


if __name__ == "__main__":
    unittest.main()
