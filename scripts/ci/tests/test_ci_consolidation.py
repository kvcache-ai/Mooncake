"""Regression tests for PR CI consolidation and CPU TENT profile runner."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


REPOSITORY = Path(__file__).resolve().parents[3]
WORKFLOWS = REPOSITORY / ".github" / "workflows"
PROFILE_SCRIPT = REPOSITORY / "scripts" / "ci" / "run_tent_cpu_profile.sh"
SERVICES_SCRIPT = REPOSITORY / "scripts" / "ci" / "services.sh"


def load_workflow(name):
    with (WORKFLOWS / name).open() as workflow_file:
        return yaml.safe_load(workflow_file)


def workflow_triggers(workflow):
    return workflow.get("on", workflow.get(True))


class TentCpuProfileScriptTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()
        self.workspace = self.root / "workspace"
        (self.workspace / "scripts" / "ci").mkdir(parents=True)
        shutil.copy2(SERVICES_SCRIPT, self.workspace / "scripts" / "ci" / "services.sh")
        (
            self.workspace
            / "mooncake-transfer-engine"
            / "example"
            / "http-metadata-server-python"
        ).mkdir(parents=True)
        (
            self.workspace
            / "mooncake-transfer-engine"
            / "example"
            / "http-metadata-server-python"
            / "bootstrap_server.py"
        ).write_text("# stub\n")
        self.runner_temp = self.root / "runner-temp"
        self.runner_temp.mkdir()
        self.log_dir = self.root / "logs"
        self.log_dir.mkdir()
        self.ready_fifo = self.runner_temp / "ready"
        os.mkfifo(self.ready_fifo)

        self._write_stubs()

    def _write_stubs(self):
        cmake_stub = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

log_file = Path(os.environ["STUB_LOG_DIR"]) / "cmake.jsonl"
args = sys.argv[1:]
entry = {{"args": args, "destdir": os.environ.get("DESTDIR", "")}}
with log_file.open("a") as f:
    f.write(json.dumps(entry) + "\\n")

if "-B" in args:
    build_dir = Path(args[args.index("-B") + 1])
    build_dir.mkdir(parents=True, exist_ok=True)
if len(args) >= 2 and args[0] == "--install" and os.environ.get("DESTDIR"):
    install_root = Path(os.environ["DESTDIR"])
    install_root.mkdir(parents=True, exist_ok=True)
"""
        ctest_stub = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

log_file = Path(os.environ["STUB_LOG_DIR"]) / "ctest.jsonl"
args = sys.argv[1:]
with log_file.open("a") as f:
    f.write(json.dumps({{"args": args, "cwd": os.getcwd()}}) + "\\n")

fail_on = os.environ.get("STUB_CTEST_FAIL_CONTAINS", "")
if fail_on and any(fail_on in arg for arg in args):
    sys.exit(1)
if not fail_on and os.environ.get("STUB_CTEST_FAIL_ALWAYS") == "1":
    sys.exit(1)
"""
        python3_stub = f"""#!{sys.executable}
import os
from pathlib import Path
import signal
import sys

root = Path(os.environ["RUNNER_TEMP"])
(root / "server.pid").write_text(str(os.getpid()))
with (root / "ready").open("w") as pipe:
    pipe.write("listening\\n")
signal.pause()
"""
        ss_stub = '#!/bin/sh\ncat "$RUNNER_TEMP/ready"\n'
        df_stub = "#!/bin/sh\nexit 0\n"
        rust_smoke_stub = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

log_file = Path(os.environ["STUB_LOG_DIR"]) / "rust_smoke.json"
log_file.write_text(
    json.dumps(
        {{
            "MOONCAKE_BUILD_DIR": os.environ.get("MOONCAKE_BUILD_DIR", ""),
            "MOONCAKE_TE_RUST_LINK_ASAN": os.environ.get(
                "MOONCAKE_TE_RUST_LINK_ASAN", ""
            ),
        }}
    )
)
if os.environ.get("STUB_RUST_SMOKE_FAIL") == "1":
    sys.exit(1)
"""

        stubs = {
            self.bin_dir / "cmake": cmake_stub,
            self.bin_dir / "ctest": ctest_stub,
            self.bin_dir / "python3": python3_stub,
            self.bin_dir / "ss": ss_stub,
            self.bin_dir / "df": df_stub,
            self.root / "rust_smoke.sh": rust_smoke_stub,
        }
        for path, body in stubs.items():
            path.write_text(body)
            path.chmod(0o755)

    def _run_profile(self, *profile_args, extra_env=None):
        env = {
            **os.environ,
            "PATH": f"{self.bin_dir}:{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(self.workspace),
            "RUNNER_TEMP": str(self.runner_temp),
            "STUB_LOG_DIR": str(self.log_dir),
            "MOONCAKE_TE_RUST_SMOKE_SCRIPT": str(self.root / "rust_smoke.sh"),
        }
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["bash", str(PROFILE_SCRIPT), *profile_args],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def _read_jsonl(self, name):
        path = self.log_dir / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    def _assert_no_leaked_server_pid(self):
        pid_file = self.runner_temp / "server.pid"
        self.assertTrue(pid_file.exists(), "metadata server was never started")
        pid = int(pid_file.read_text())
        try:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass

    def test_invalid_arguments_exit_2(self):
        for args in ((), ("unknown-profile",), ("cuda-off", "extra")):
            with self.subTest(args=args):
                result = self._run_profile(*args)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Usage:", result.stderr)

    def test_build_flags_profile(self):
        result = self._run_profile("build-flags")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        cmake_calls = self._read_jsonl("cmake.jsonl")
        self.assertEqual(len(cmake_calls), 3)
        configure_args = cmake_calls[0]["args"]
        self.assertIn(str(self.workspace / "mooncake-transfer-engine"), configure_args)
        for flag in ("-DUSE_ETCD=OFF", "-DUSE_CXL=ON", "-DWITH_METRICS=ON"):
            self.assertIn(flag, configure_args)
        self.assertEqual(cmake_calls[1]["args"][0], "--build")
        self.assertEqual(cmake_calls[2]["args"][0], "--install")
        self.assertEqual(
            cmake_calls[2]["destdir"], str(self.runner_temp / "install-build-flags")
        )

        self.assertEqual(self._read_jsonl("ctest.jsonl"), [])
        self.assertFalse((self.log_dir / "rust_smoke.json").exists())
        self.assertFalse(
            (self.workspace / "mooncake-transfer-engine" / "build-flags").exists()
        )
        self.assertFalse((self.runner_temp / "install-build-flags").exists())

    def test_tent_cpu_profiles_execute_expected_builds_and_tests(self):
        cases = [
            {
                "profile": "cuda-off",
                "required_flags": [
                    "-DUSE_TENT=ON",
                    "-DUSE_CUDA=OFF",
                    "-DUSE_EVENT_DRIVEN_COMPLETION=ON",
                ],
                "targets": [
                    "mooncake-transfer-engine/all",
                    "transfer_task_test",
                    "standalone_client_test",
                ],
                "expect_install": False,
                "expected_ctest_regexes": [
                    "^(transfer_task_tent_completion_test|standalone_client_tent_test)$",
                    "^tent_compat_notify_test$",
                    "^transfer_task_tent_shared_segment_test$",
                ],
            },
            {
                "profile": "cuda-off-metrics-on",
                "required_flags": [
                    "-DUSE_TENT=ON",
                    "-DUSE_CUDA=OFF",
                    "-DWITH_STORE=OFF",
                    "-DWITH_STORE_RUST=OFF",
                    "-DTENT_METRICS_ENABLED=ON",
                ],
                "targets": None,
                "expect_install": True,
                "expected_ctest_regexes": [],
            },
            {
                "profile": "ub-mock",
                "required_flags": [
                    "-DUSE_TENT=ON",
                    "-DUSE_CUDA=OFF",
                    "-DUSE_UB=ON",
                    "-DCMAKE_BUILD_TYPE=Debug",
                    "-DWITH_STORE=OFF",
                    "-DWITH_STORE_RUST=OFF",
                ],
                "targets": None,
                "expect_install": True,
                "expected_ctest_regexes": [],
            },
            {
                "profile": "memfabric-mock",
                "required_flags": [
                    "-DUSE_TENT=ON",
                    "-DUSE_CUDA=OFF",
                    "-DUSE_MEMFABRIC=ON",
                    "-DWITH_STORE=OFF",
                    "-DWITH_STORE_RUST=OFF",
                    "-DCMAKE_BUILD_TYPE=Debug",
                ],
                "targets": ["tent_memfabric_transport_test", "transfer_engine"],
                "expect_install": False,
                "expected_ctest_regexes": ["^tent_memfabric_transport_test$"],
            },
        ]

        for case in cases:
            with self.subTest(profile=case["profile"]):
                for log in self.log_dir.iterdir():
                    log.unlink()
                pid_file = self.runner_temp / "server.pid"
                pid_file.unlink(missing_ok=True)

                profile = case["profile"]
                expected_build_dir = str(self.workspace / f"build-tent-{profile}")

                result = self._run_profile(profile)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

                cmake_calls = self._read_jsonl("cmake.jsonl")
                configure_args = cmake_calls[0]["args"]
                self.assertIn(expected_build_dir, configure_args)
                for flag in case["required_flags"]:
                    self.assertIn(flag, configure_args)

                build_args = cmake_calls[1]["args"]
                self.assertEqual(build_args[:2], ["--build", expected_build_dir])
                if case["targets"]:
                    target_idx = build_args.index("--target")
                    self.assertEqual(build_args[target_idx + 1 :], case["targets"])
                else:
                    self.assertNotIn("--target", build_args)

                if case["expect_install"]:
                    self.assertEqual(len(cmake_calls), 3)
                    self.assertEqual(
                        cmake_calls[2]["args"], ["--install", expected_build_dir]
                    )
                    self.assertEqual(
                        cmake_calls[2]["destdir"],
                        str(self.runner_temp / f"install-tent-{profile}"),
                    )
                else:
                    self.assertEqual(len(cmake_calls), 2)

                ctest_calls = self._read_jsonl("ctest.jsonl")
                tent_ctest = ctest_calls[-1]["args"]
                self.assertIn("mooncake-transfer-engine/tent/tests", tent_ctest)
                self.assertIn(
                    f"{expected_build_dir}/test-results/ctest.xml", tent_ctest
                )

                all_ctest_args = [arg for call in ctest_calls for arg in call["args"]]
                for regex in case["expected_ctest_regexes"]:
                    self.assertIn(regex, all_ctest_args)

                rust_env = json.loads((self.log_dir / "rust_smoke.json").read_text())
                self.assertEqual(rust_env["MOONCAKE_BUILD_DIR"], expected_build_dir)
                self.assertEqual(rust_env["MOONCAKE_TE_RUST_LINK_ASAN"], "0")

                self._assert_no_leaked_server_pid()
                self.assertFalse(Path(expected_build_dir).exists())
                self.assertFalse(
                    (self.runner_temp / f"install-tent-{profile}").exists()
                )

    def test_ctest_failure_preserves_build_dir_for_diagnostics(self):
        result = self._run_profile(
            "memfabric-mock", extra_env={"STUB_CTEST_FAIL_ALWAYS": "1"}
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((self.workspace / "build-tent-memfabric-mock").is_dir())
        self.assertFalse((self.log_dir / "rust_smoke.json").exists())

    def test_rust_smoke_failure_cleans_up_metadata_server(self):
        result = self._run_profile("ub-mock", extra_env={"STUB_RUST_SMOKE_FAIL": "1"})
        self.assertNotEqual(result.returncode, 0)
        self._assert_no_leaked_server_pid()
        self.assertTrue((self.workspace / "build-tent-ub-mock").is_dir())


class WorkflowConsolidationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci = load_workflow("ci.yml")
        cls.macos = load_workflow("ci-macos.yml")
        cls.e2e = load_workflow("e2e-ci.yml")
        cls.nightly = load_workflow("nightly.yml")

    def test_removed_redundant_jobs(self):
        jobs = self.ci["jobs"]
        for removed in (
            "check-paths",
            "clang-format",
            "engram-cpu-contracts",
            "build-flags",
        ):
            self.assertNotIn(removed, jobs)

    def test_fast_checks_unifies_stage0_with_positive_filters(self):
        fast = self.ci["jobs"]["fast-checks"]
        self.assertEqual(fast["permissions"]["pull-requests"], "read")
        self.assertEqual(set(fast["outputs"]), {"src", "tent", "store_rs", "docs"})

        setup_python = next(
            step
            for step in fast["steps"]
            if step.get("uses", "").startswith("actions/setup-python@")
        )
        versions = [
            line.strip()
            for line in setup_python["with"]["python-version"].splitlines()
            if line.strip()
        ]
        self.assertEqual(versions, ["3.10", "3.12"])

        tools_step = next(
            step
            for step in fast["steps"]
            if step.get("name") == "Install Python check and format tools"
        )
        self.assertIn("clang-format==20.1.8", tools_step["run"])

        filter_step = next(step for step in fast["steps"] if step.get("id") == "filter")
        filters = yaml.safe_load(filter_step["with"]["filters"])
        for group, patterns in filters.items():
            for pattern in patterns:
                self.assertFalse(
                    pattern.startswith("!"),
                    f"Negated pattern {pattern!r} in {group} breaks paths-filter",
                )
        self.assertNotIn("mooncake-store-rs/**", filters["src"])
        self.assertNotIn("mooncake-*/**", filters["src"])
        self.assertIn("scripts/ci/run_tent_cpu_profile.sh", filters["tent"])
        self.assertIn("scripts/ci/services.sh", filters["tent"])

    def test_rocm_wheel_is_single_python_on_pr_and_default_elsewhere(self):
        self.assertEqual(
            self.ci["jobs"]["build-wheel-rocm"]["with"]["python_versions"],
            '["3.12"]',
        )
        self.assertNotIn(
            "python_versions", self.e2e["jobs"]["build-wheel-rocm"].get("with", {})
        )
        self.assertNotIn(
            "python_versions",
            self.nightly["jobs"]["build-wheel-rocm"].get("with", {}),
        )

    def test_tent_ci_consolidates_cpu_profiles_and_diagnostics(self):
        tent = self.ci["jobs"]["tent-ci"]
        self.assertEqual(tent["needs"], ["fast-checks"])
        self.assertEqual(
            [entry["name"] for entry in tent["strategy"]["matrix"]["include"]],
            ["cuda-on", "cpu"],
        )

        steps = tent["steps"]
        run_commands = "\n".join(step.get("run", "") for step in steps)
        for profile in (
            "build-flags",
            "cuda-off",
            "cuda-off-metrics-on",
            "ub-mock",
            "memfabric-mock",
        ):
            self.assertIn(
                f"./scripts/ci/run_tent_cpu_profile.sh {profile}", run_commands
            )

        for profile in ("cuda-off", "cuda-off-metrics-on", "ub-mock", "memfabric-mock"):
            diag_step = next(
                step
                for step in steps
                if step.get("name") == f"Preserve TENT CTest diagnostics ({profile})"
            )
            self.assertEqual(
                diag_step["with"]["junit-report"],
                f"build-tent-{profile}/test-results/ctest.xml",
            )
            self.assertEqual(
                diag_step["with"]["failed-tests-log"],
                f"build-tent-{profile}/mooncake-transfer-engine/tent/tests/Testing/Temporary/LastTestsFailed.log",
            )
            self.assertEqual(
                diag_step["with"]["last-test-log"],
                f"build-tent-{profile}/mooncake-transfer-engine/tent/tests/Testing/Temporary/LastTest.log",
            )

    def test_macos_workflow_narrows_path_filter(self):
        triggers = workflow_triggers(self.macos)
        for event in ("push", "pull_request"):
            paths = triggers[event]["paths"]
            self.assertNotIn("mooncake-*/**", paths)
            for required in (
                "mooncake-common/**",
                "mooncake-transfer-engine/**",
                "mooncake-store/**",
                "mooncake-integration/**",
            ):
                self.assertIn(required, paths)

    def test_ci_gate_covers_all_jobs_and_rejects_unselected_or_failed_jobs(self):
        jobs = self.ci["jobs"]
        gate = jobs["ci-gate"]
        self.assertEqual(set(gate["needs"]), set(jobs) - {"ci-gate"})

        steps = {step["name"]: step for step in gate["steps"]}
        unconditional_script = steps["Check unconditional jobs"]["run"]
        for passed_expr, expected_rc in (("true", 0), ("false", 1)):
            result = subprocess.run(
                ["bash", "-ec", unconditional_script],
                env={**os.environ, "NEEDS_JSON": "{}", "PASSED": passed_expr},
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, expected_rc)

        tent_script = steps["Check TENT jobs"]["run"]
        for expected, actual, passes in (
            ("success", "success", True),
            ("skipped", "skipped", True),
            ("success", "skipped", False),
            ("success", "failure", False),
            ("skipped", "failure", False),
        ):
            result = subprocess.run(
                ["bash", "-ec", tent_script],
                env={**os.environ, "EXPECTED": expected, "RESULT": actual},
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(result.returncode == 0, passes, (expected, actual))

    def test_wheel_ubuntu_configures_cuda_stub_and_pg_jit_headers(self):
        steps = self.ci["jobs"]["test-wheel-ubuntu"]["steps"]
        names = [step.get("name") for step in steps]
        cuda_idx = names.index("Install CUDA Toolkit")
        stub_idx = names.index("Configure CUDA stub and PG JIT headers")
        install_idx = names.index("Run installation test script")
        self.assertLess(cuda_idx, stub_idx)
        self.assertLess(stub_idx, install_idx)

        stub_run = steps[stub_idx]["run"]
        self.assertIn("libcuda.so.1", stub_run)
        self.assertIn("LD_LIBRARY_PATH=", stub_run)
        for pkg in (
            "libcublas-dev-12-8",
            "libcusparse-dev-12-8",
            "libcusolver-dev-12-8",
            "libcurand-dev-12-8",
        ):
            self.assertIn(pkg, stub_run)


if __name__ == "__main__":
    unittest.main()
