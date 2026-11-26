"""Regression tests for PR CI runner consolidation and build optimizations."""

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
ACTIONS = REPOSITORY / ".github" / "actions"
PROFILE_SCRIPT = REPOSITORY / "scripts" / "ci" / "run_tent_cpu_profile.sh"
SERVICES_SCRIPT = REPOSITORY / "scripts" / "ci" / "services.sh"
LLD_CMAKE = REPOSITORY / "scripts" / "ci" / "cmake_lld.cmake"


def load_yaml(path):
    with path.open() as handle:
        return yaml.safe_load(handle)


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
        shutil.copy2(LLD_CMAKE, self.workspace / "scripts" / "ci" / "cmake_lld.cmake")
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
"""
        python3_stub = f"""#!{sys.executable}
import signal
import sys
import time

if any("bootstrap_server.py" in arg for arg in sys.argv[1:]):
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        time.sleep(0.1)
"""
        ss_stub = "#!/bin/sh\necho 'LISTEN 0 128 127.0.0.1:8080 0.0.0.0:*'\n"
        sudo_stub = f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

log_file = Path(os.environ["STUB_LOG_DIR"]) / "sudo.jsonl"
with log_file.open("a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
"""
        rust_smoke_stub = f"""#!{sys.executable}
import json
import os
from pathlib import Path

log_file = Path(os.environ["STUB_LOG_DIR"]) / "rust.jsonl"
with log_file.open("a") as f:
    f.write(json.dumps({{
        "build_dir": os.environ.get("MOONCAKE_BUILD_DIR", ""),
        "asan": os.environ.get("MOONCAKE_TE_RUST_LINK_ASAN", ""),
    }}) + "\\n")
"""
        stubs = {
            "cmake": cmake_stub,
            "ctest": ctest_stub,
            "python3": python3_stub,
            "ss": ss_stub,
            "sudo": sudo_stub,
            "df": "#!/bin/sh\nexit 0\n",
            "rust-smoke.sh": rust_smoke_stub,
        }
        for name, content in stubs.items():
            path = self.bin_dir / name
            path.write_text(content)
            path.chmod(0o755)

    def run_profile(self, profile, extra_env=None):
        env = {
            **os.environ,
            "PATH": f"{self.bin_dir}:{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(self.workspace),
            "RUNNER_TEMP": str(self.runner_temp),
            "STUB_LOG_DIR": str(self.log_dir),
            "MOONCAKE_TE_RUST_SMOKE_SCRIPT": str(self.bin_dir / "rust-smoke.sh"),
        }
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["bash", str(PROFILE_SCRIPT), profile],
            cwd=self.workspace,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def read_jsonl(self, name):
        path = self.log_dir / name
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    def test_build_flags_profile_uses_lld_without_cuda_and_cleans_up(self):
        result = self.run_profile("build-flags")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        cmake_calls = self.read_jsonl("cmake.jsonl")
        self.assertEqual(len(cmake_calls), 3)
        configure_args = cmake_calls[0]["args"]
        self.assertIn("-DUSE_CUDA=OFF", configure_args)
        self.assertIn("-DBUILD_BENCHMARK=OFF", configure_args)
        self.assertIn(
            f"-DCMAKE_PROJECT_INCLUDE={self.workspace / 'scripts/ci/cmake_lld.cmake'}",
            configure_args,
        )
        self.assertEqual(
            cmake_calls[2]["destdir"], str(self.runner_temp / "install-build-flags")
        )
        self.assertFalse(
            (self.workspace / "mooncake-transfer-engine/build-flags").exists()
        )

    def test_cuda_off_profile_runs_store_and_tent_tests_and_rust_smoke(self):
        result = self.run_profile("cuda-off")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        cmake_calls = self.read_jsonl("cmake.jsonl")
        self.assertIn("-DUSE_EVENT_DRIVEN_COMPLETION=ON", cmake_calls[0]["args"])
        self.assertIn("transfer_task_test", cmake_calls[1]["args"])
        self.assertIn("standalone_client_test", cmake_calls[1]["args"])

        ctest_calls = self.read_jsonl("ctest.jsonl")
        self.assertEqual(len(ctest_calls), 4)
        self.assertIn("tent_compat_notify_test", " ".join(ctest_calls[1]["args"]))
        self.assertIn(
            "transfer_task_tent_shared_segment_test", " ".join(ctest_calls[2]["args"])
        )
        self.assertIn("mooncake-transfer-engine/tent/tests", ctest_calls[3]["args"])

        rust_calls = self.read_jsonl("rust.jsonl")
        self.assertEqual(len(rust_calls), 1)
        self.assertEqual(
            rust_calls[0]["build_dir"], str(self.workspace / "build-tent-cuda-off")
        )
        self.assertFalse((self.workspace / "build-tent-cuda-off").exists())

    def test_ub_mock_profile_builds_and_runs_store_tent_ub_mock_test(self):
        result = self.run_profile("ub-mock")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        cmake_calls = self.read_jsonl("cmake.jsonl")
        configure_args = cmake_calls[0]["args"]
        self.assertIn("-DUSE_UB=ON", configure_args)
        self.assertIn("-DUSE_UB_MOCK=ON", configure_args)
        self.assertIn("-DCMAKE_BUILD_TYPE=Debug", configure_args)
        self.assertIn("store_tent_ub_mock_test", cmake_calls[1]["args"])

        sudo_calls = self.read_jsonl("sudo.jsonl")
        self.assertEqual(sudo_calls, [["sysctl", "-w", "vm.nr_hugepages=64"]])

        ctest_calls = self.read_jsonl("ctest.jsonl")
        self.assertEqual(len(ctest_calls), 2)
        self.assertIn("^store_tent_ub_mock_test$", ctest_calls[0]["args"])
        self.assertIn("mooncake-transfer-engine/tent/tests", ctest_calls[1]["args"])
        self.assertFalse((self.workspace / "build-tent-ub-mock").exists())

    def test_memfabric_mock_profile_runs_targeted_ctest(self):
        result = self.run_profile("memfabric-mock")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        cmake_calls = self.read_jsonl("cmake.jsonl")
        self.assertIn("-DUSE_MEMFABRIC=ON", cmake_calls[0]["args"])
        self.assertIn("tent_memfabric_transport_test", cmake_calls[1]["args"])
        ctest_calls = self.read_jsonl("ctest.jsonl")
        self.assertEqual(len(ctest_calls), 1)
        self.assertIn("^tent_memfabric_transport_test$", ctest_calls[0]["args"])

    def test_failed_ctest_preserves_build_dir_for_diagnostics(self):
        result = self.run_profile(
            "cuda-off-metrics-on",
            extra_env={
                "STUB_CTEST_FAIL_CONTAINS": "mooncake-transfer-engine/tent/tests"
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((self.workspace / "build-tent-cuda-off-metrics-on").is_dir())
        self.assertEqual(self.read_jsonl("rust.jsonl"), [])


class CIWorkflowOptimizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci = load_yaml(WORKFLOWS / "ci.yml")
        cls.build_wheel = load_yaml(WORKFLOWS / "_build-wheel.yaml")
        cls.rocm = load_yaml(WORKFLOWS / "ci_rocm.yml")
        cls.macos = load_yaml(WORKFLOWS / "ci-macos.yml")
        cls.pr_size = load_yaml(WORKFLOWS / "pr-size.yml")
        cls.free_disk = load_yaml(ACTIONS / "free-disk-space" / "action.yml")

    def test_stage0_consolidated_into_fast_checks_and_gates_heavy_jobs(self):
        jobs = self.ci["jobs"]
        self.assertIn("fast-checks", jobs)
        for removed in (
            "check-paths",
            "spell-check",
            "clang-format",
            "python-lint",
            "reshard-type-check",
            "ci-script-tests",
            "tone-script-tests",
            "engram-cpu-contracts",
            "efa-unit-tests",
            "build-flags",
        ):
            self.assertNotIn(removed, jobs)

        for gated in (
            "build-wheel",
            "unit-tests",
            "docs-check",
            "build-wheel-cu13",
            "build-wheel-cu13-arm64",
            "build-wheel-rocm",
            "build-wheel-xpu",
            "tent-ci",
            "store-rs-ci",
        ):
            self.assertEqual(jobs[gated]["needs"], ["fast-checks"])

        filter_step = next(
            s for s in jobs["fast-checks"]["steps"] if s.get("id") == "filter"
        )
        filters = yaml.safe_load(filter_step["with"]["filters"])
        self.assertNotIn("mooncake-*/**", filters["src"])
        self.assertNotIn("mooncake-store-rs/**", filters["src"])
        self.assertNotIn("scripts/**", filters["src"])
        self.assertNotIn(".github/workflows/**", filters["src"])
        self.assertIn("scripts/ci/cmake_lld.cmake", filters["src"])
        self.assertIn("scripts/ci/cmake_lld.cmake", filters["tent"])
        self.assertIn("scripts/ci/run_tent_cpu_profile.sh", filters["tent"])
        self.assertIn("scripts/ci/cmake_lld.cmake", filters["store_rs"])

    def test_unit_tests_folds_efa_preserves_oom_cap_and_uses_lld(self):
        steps = self.ci["jobs"]["unit-tests"]["steps"]
        configure = next(
            s["run"]
            for s in steps
            if s.get("name") == "Configure project with unit tests"
        )
        self.assertIn("-DUSE_EFA=ON", configure)
        self.assertIn("-DBUILD_EXAMPLES=OFF", configure)
        self.assertIn("scripts/ci/cmake_lld.cmake", configure)
        self.assertIn("-DCMAKE_CUDA_COMPILER_LAUNCHER=sccache", configure)

        build = next(
            s["run"] for s in steps if s.get("name") == "Build project and unit tests"
        )
        self.assertIn("cmake --build build --parallel 2", build)

        ctest = next(s for s in steps if s.get("name") == "Run all CTest unit tests")
        self.assertEqual(ctest["with"]["label-exclude"], "^nvlink_gpu$")
        self.assertEqual(
            ctest["with"]["exclude-regex"], "^efa_(transport|c_api|gpu_loopback)_test$"
        )

    def test_tent_ci_splits_into_three_balanced_legs_with_diagnostics(self):
        tent = self.ci["jobs"]["tent-ci"]
        names = [entry["name"] for entry in tent["strategy"]["matrix"]["include"]]
        self.assertEqual(names, ["cuda-on", "cpu-core", "cpu-variants"])

        steps = {s.get("name"): s for s in tent["steps"]}
        for profile in ("cuda-off", "memfabric-mock", "cuda-off-metrics-on", "ub-mock"):
            diag = steps[f"Preserve TENT CTest diagnostics ({profile})"]
            self.assertEqual(
                diag["with"]["junit-report"],
                f"build-tent-{profile}/test-results/ctest.xml",
            )
            self.assertEqual(
                diag["with"]["failed-tests-log"],
                f"build-tent-{profile}/mooncake-transfer-engine/tent/tests/Testing/Temporary/LastTestsFailed.log",
            )

    def test_wheel_builds_disable_benchmarks_and_rocm_disables_debug_symbols(self):
        profile_step = next(
            s["run"]
            for s in self.build_wheel["jobs"]["build"]["steps"]
            if s.get("name") == "Configure build profile"
        )
        self.assertIn("-DBUILD_BENCHMARK=OFF", profile_step)
        self.assertIn("-DCMAKE_CUDA_COMPILER_LAUNCHER=sccache", profile_step)

        self.assertEqual(
            self.ci["jobs"]["build-wheel-rocm"]["with"]["python_versions"], '["3.12"]'
        )
        rocm_configure = next(
            s["run"]
            for s in self.rocm["jobs"]["build-wheel-rocm"]["steps"]
            if s.get("name") == "Configure project"
        )
        self.assertIn("-DENABLE_DEBUG_SYMBOLS=OFF", rocm_configure)
        self.assertIn("-DBUILD_BENCHMARK=OFF", rocm_configure)
        self.assertIn("scripts/ci/cmake_lld.cmake", rocm_configure)
        rocm_build = next(
            s
            for s in self.rocm["jobs"]["build-wheel-rocm"]["steps"]
            if s.get("name") == "Build project"
        )
        self.assertEqual(rocm_build["env"]["MAX_JOBS"], "2")

    def test_wheel_ubuntu_slims_cuda_and_uses_cpu_torch_on_2404(self):
        job = self.ci["jobs"]["test-wheel-ubuntu"]
        steps = job["steps"]
        names = [s.get("name") for s in steps]
        self.assertNotIn("Free up disk space", names)
        cuda_step = steps[names.index("Install CUDA Toolkit")]
        self.assertEqual(cuda_step["with"]["sub-packages"], '["nvcc"]')

        stub_idx = names.index("Configure CUDA stub and PG JIT headers")
        install_idx = names.index("Run installation test script")
        self.assertLess(stub_idx, install_idx)
        for pkg in (
            "libcublas-dev-12-8",
            "libcusparse-dev-12-8",
            "libcusolver-dev-12-8",
            "libcurand-dev-12-8",
        ):
            self.assertIn(pkg, steps[stub_idx]["run"])

        matrix = {
            entry["ubuntu-version"]: entry["torch-pip-extra-args"]
            for entry in job["strategy"]["matrix"]["include"]
        }
        self.assertEqual(matrix["ubuntu-22.04"], "")
        self.assertEqual(
            matrix["ubuntu-24.04"], "--index-url https://download.pytorch.org/whl/cpu"
        )

    def test_auxiliary_workflows_avoid_unnecessary_triggers(self):
        for event in ("push", "pull_request"):
            paths = workflow_triggers(self.macos)[event]["paths"]
            self.assertNotIn("mooncake-*/**", paths)
            for required in (
                "mooncake-common/**",
                "mooncake-transfer-engine/**",
                "mooncake-store/**",
                "mooncake-integration/**",
            ):
                self.assertIn(required, paths)

        self.assertIn(
            "github.event.changes.base != null",
            self.pr_size["jobs"]["label"]["if"],
        )

    def test_ci_gate_covers_all_jobs_strictly(self):
        jobs = self.ci["jobs"]
        gate = jobs["ci-gate"]
        self.assertEqual(set(gate["needs"]), set(jobs) - {"ci-gate"})
        steps = {s["name"]: s for s in gate["steps"]}
        self.assertEqual(
            steps["Check documentation job"]["env"]["RESULT"],
            "${{ needs.docs-check.result }}",
        )
        for expected, actual, passes in (
            ("success", "success", True),
            ("skipped", "skipped", True),
            ("success", "skipped", False),
            ("success", "failure", False),
            ("skipped", "failure", False),
        ):
            result = subprocess.run(
                ["bash", "-ec", steps["Check TENT jobs"]["run"]],
                env={**os.environ, "EXPECTED": expected, "RESULT": actual},
                capture_output=True,
                timeout=5,
            )
            self.assertEqual(result.returncode == 0, passes, (expected, actual))

    def test_free_disk_space_skips_when_sufficient_and_cleans_when_low(self):
        script = self.free_disk["runs"]["steps"][0]["run"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            sudo_log = root / "sudo.log"
            (bin_dir / "sudo").write_text(
                f'#!/bin/sh\nprintf "%s\\n" "$*" >> "{sudo_log}"\n'
            )
            (bin_dir / "sudo").chmod(0o755)

            for avail, should_clean in (("80G", False), ("15G", True)):
                with self.subTest(avail=avail):
                    sudo_log.unlink(missing_ok=True)
                    (bin_dir / "df").write_text(
                        f"#!/bin/sh\nif [ \"$1\" = -BG ]; then printf 'Avail\\n{avail}\\n'; fi\n"
                    )
                    (bin_dir / "df").chmod(0o755)
                    result = subprocess.run(
                        ["bash", "-c", script],
                        env={
                            **os.environ,
                            "PATH": f"{bin_dir}:{os.environ['PATH']}",
                            "MIN_FREE_GB": "40",
                        },
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(sudo_log.exists(), should_clean)
                    if should_clean:
                        calls = sudo_log.read_text()
                        self.assertIn("/usr/share/dotnet", calls)
                        self.assertIn("/usr/local/lib/android", calls)

    def test_cmake_lld_include_configures_with_real_cmake_and_lld(self):
        if not (shutil.which("cmake") and shutil.which("ld.lld")):
            self.skipTest("cmake and ld.lld are required")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "main.c").write_text("int main(void) { return 0; }\n")
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.16)\n"
                "project(lld_probe C)\n"
                "add_executable(probe main.c)\n"
            )
            build = root / "build"
            result = subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(root),
                    "-B",
                    str(build),
                    f"-DCMAKE_PROJECT_INCLUDE={LLD_CMAKE}",
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("[ci] Using lld linker:", result.stdout)
            build_res = subprocess.run(
                ["cmake", "--build", str(build)],
                capture_output=True,
                text=True,
                timeout=20,
            )
            self.assertEqual(
                build_res.returncode, 0, build_res.stdout + build_res.stderr
            )


if __name__ == "__main__":
    unittest.main()
