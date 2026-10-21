"""Exercise nightly gate decisions and shell failure handling without publishing."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]


class NightlyCITest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.jobs = yaml.safe_load(
            (REPOSITORY / ".github/workflows/nightly.yml").read_text()
        )["jobs"]

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.summary = self.directory / "summary.md"

    def step(self, job, name):
        return next(
            step for step in self.jobs[job]["steps"] if step.get("name") == name
        )

    def run_step(self, job, name, **environment):
        return subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", self.step(job, name)["run"]],
            env={
                **os.environ,
                "GITHUB_STEP_SUMMARY": str(self.summary),
                **environment,
            },
            cwd=self.directory,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def run_gate(self, results=None, *, allow_rocm=False, allow_publish=False):
        needs = {
            job: {"result": "success"} for job in self.jobs["nightly-gate"]["needs"]
        }
        for job, result in (results or {}).items():
            needs[job]["result"] = result
        return self.run_step(
            "nightly-gate",
            "Check results",
            NEEDS_JSON=json.dumps(needs),
            ALLOW_ROCM_SKIP=str(allow_rocm).lower(),
            ALLOW_PUBLISH_SKIP=str(allow_publish).lower(),
        )

    def test_all_required_jobs_pass(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_disabled_rocm_is_reported_as_untested(self):
        result = self.run_gate({"rocm-integration-test": "skipped"}, allow_rocm=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("coverage is absent", self.summary.read_text())
        self.assertIn("rocm-integration-test: skipped", self.summary.read_text())

    def test_enabled_rocm_must_succeed(self):
        for status in ("skipped", "failure", "cancelled"):
            with self.subTest(status=status):
                result = self.run_gate({"rocm-integration-test": status})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"rocm-integration-test: {status}", result.stdout)

    def test_allowing_rocm_skip_does_not_hide_failure_or_cancellation(self):
        for status in ("failure", "cancelled"):
            with self.subTest(status=status):
                result = self.run_gate(
                    {"rocm-integration-test": status}, allow_rocm=True
                )
                self.assertNotEqual(result.returncode, 0)

    def test_other_jobs_remain_required_even_when_both_skips_are_allowed(self):
        for job in self.jobs["nightly-gate"]["needs"]:
            if job in {"rocm-integration-test", "publish-testpypi"}:
                continue
            for status in ("skipped", "failure", "cancelled"):
                with self.subTest(job=job, status=status):
                    result = self.run_gate(
                        {job: status}, allow_rocm=True, allow_publish=True
                    )
                    self.assertNotEqual(result.returncode, 0)

    def test_publish_skip_requires_explicit_request(self):
        for allowed in (False, True):
            with self.subTest(allowed=allowed):
                result = self.run_gate(
                    {"publish-testpypi": "skipped"}, allow_publish=allowed
                )
                self.assertEqual(result.returncode == 0, allowed)

    def test_publish_failure_is_never_accepted_as_an_intentional_skip(self):
        for status in ("failure", "cancelled"):
            with self.subTest(status=status):
                result = self.run_gate({"publish-testpypi": status}, allow_publish=True)
                self.assertNotEqual(result.returncode, 0)

    def test_hardware_selection_records_the_request_and_missing_coverage(self):
        for enabled in ("false", "true"):
            with self.subTest(enabled=enabled):
                output = self.directory / f"output-{enabled}"
                self.summary.unlink(missing_ok=True)
                result = self.run_step(
                    "version-stamp",
                    "Select hardware tests",
                    GITHUB_OUTPUT=str(output),
                    RUN_ROCM_INTEGRATION=enabled,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    output.read_text(), f"run_rocm_integration={enabled}\n"
                )
                self.assertEqual(
                    "will not run" in self.summary.read_text(), enabled == "false"
                )

    def test_rocm_job_and_gate_use_the_same_recorded_selection(self):
        integration = self.jobs["rocm-integration-test"]
        self.assertIn("version-stamp", integration["needs"])
        self.assertIn("build-wheel-rocm", integration["needs"])
        self.assertEqual(
            integration["if"],
            "${{ needs.version-stamp.outputs.run_rocm_integration == 'true' }}",
        )
        self.assertEqual(
            self.step("nightly-gate", "Check results")["env"]["ALLOW_ROCM_SKIP"],
            "${{ needs.version-stamp.outputs.run_rocm_integration == 'false' }}",
        )
        self.assertNotIn("if", self.jobs["build-wheel-rocm"])

    def test_missing_upload_credentials_fail_without_prompting_or_leaking(self):
        for token in ("", "not-a-real-api-token"):
            with self.subTest(token_present=bool(token)):
                result = self.run_step(
                    "publish-testpypi",
                    "Require TestPyPI credentials",
                    TESTPYPI_API_TOKEN=token,
                )
                self.assertEqual(result.returncode == 0, bool(token))
                self.assertNotIn("not-a-real-api-token", result.stdout + result.stderr)

    def test_docker_build_uses_configured_builder_and_preserves_failure(self):
        bin_directory = self.directory / "bin"
        bin_directory.mkdir()
        stubs = {
            "free": "exit 0",
            "df": "exit 0",
            "vmstat": "exec sleep 30",
            "docker": 'printf "%s\\n" "$@" > "$DOCKER_ARGS_FILE"\nexit "$DOCKER_EXIT_CODE"',
        }
        for name, body in stubs.items():
            stub = bin_directory / name
            stub.write_text(f"#!/bin/bash\n{body}\n")
            stub.chmod(0o755)
        arguments = self.directory / "docker-args"
        for code in (0, 23):
            with self.subTest(exit_code=code):
                result = self.run_step(
                    "build-docker",
                    "Build Docker image",
                    PATH=f"{bin_directory}:{os.environ['PATH']}",
                    DOCKER_ARGS_FILE=str(arguments),
                    DOCKER_EXIT_CODE=str(code),
                )
                self.assertEqual(result.returncode, code, result.stderr)
                args = arguments.read_text().splitlines()
                self.assertEqual(args[:2], ["buildx", "build"])
                self.assertIn("--output=type=cacheonly", args)
                self.assertIn("--progress=plain", args)
                self.assertIn("BUILD_JOBS=2", args)


if __name__ == "__main__":
    unittest.main()
