"""CPU-only regressions for XPU packaging and hardware CI orchestration."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from importlib import metadata
import xml.etree.ElementTree as ET
import zipfile

import yaml


ROOT = Path(__file__).resolve().parents[3]


def load_script(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/ci_xpu.yml").read_text())
E2E_WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/e2e-ci.yml").read_text())
INTEGRATION = yaml.safe_load(
    (ROOT / ".github/workflows/integration-test-xpu.yml").read_text()
)


def workflow_step(name):
    return next(
        step
        for step in WORKFLOW["jobs"]["build-wheel-xpu"]["steps"]
        if step.get("name") == name
    )["run"]


def hardware_step(name):
    return next(
        step
        for step in INTEGRATION["jobs"]["core"]["steps"]
        if step.get("name") == name
    )["run"]


ROCM = load_script("verify_rocm_wheel", "scripts/e2e/python/verify_rocm_wheel.py")
with patch.dict(sys.modules, {"verify_rocm_wheel": ROCM}):
    VERIFY = load_script("verify_xpu_wheel", "scripts/e2e/python/verify_xpu_wheel.py")


class HostReportTest(unittest.TestCase):
    def test_requires_exact_count_and_completed_non_skipped_cases(self):
        with tempfile.TemporaryDirectory() as directory:
            reports = Path(directory) / "xpu-ci-output/reports"
            reports.mkdir(parents=True)
            code = (
                workflow_step("Run host-side unit tests")
                .split("python - <<'PY'\n", 1)[1]
                .rsplit("\nPY", 1)[0]
            )
            for body, valid in (
                ('<testcase status="run" result="completed"/>', True),
                ("", False),
                ('<testcase status="notrun" result="suppressed"/>', False),
                (
                    '<testcase status="run" result="skipped"><skipped/></testcase>',
                    False,
                ),
                (
                    '<testcase status="run" result="completed"><failure/></testcase>',
                    False,
                ),
            ):
                with self.subTest(body=body):
                    for name, count in (("config", 19), ("ep", 6), ("crc32c", 5)):
                        (reports / f"{name}.xml").write_text(
                            f"<testsuites><testsuite>{body * count}</testsuite></testsuites>"
                        )
                    result = subprocess.run(
                        [sys.executable, "-c", code],
                        cwd=directory,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode == 0, valid, result.stderr)
            (reports / "ep.xml").unlink()
            self.assertNotEqual(
                subprocess.run(
                    [sys.executable, "-c", code],
                    cwd=directory,
                    capture_output=True,
                    timeout=10,
                ).returncode,
                0,
            )


class BuildDirectoryTest(unittest.TestCase):
    def test_cleanup_defaults_and_explicit_override(self):
        script = (ROOT / "scripts/build_wheel.sh").read_text()
        code = script[
            script.index('CUDA_EP_STAGING_TEMP=""') : script.index(
                "# Make the post-repair device library discoverable"
            )
        ]
        wheel_step = next(
            step
            for step in WORKFLOW["jobs"]["build-wheel-xpu"]["steps"]
            if step.get("name") == "Build Python wheel"
        )
        self.assertEqual(wheel_step["env"]["FREE_BUILD_DIR"], "0")
        for ci, free_build_dir, preserved in (
            ("true", None, False),
            ("true", "1", False),
            ("true", wheel_step["env"]["FREE_BUILD_DIR"], True),
            ("false", None, True),
            ("false", "1", False),
            ("false", "0", True),
        ):
            with self.subTest(ci=ci, free_build_dir=free_build_dir):
                with tempfile.TemporaryDirectory() as directory:
                    build = Path(directory) / "build-xpu-ci"
                    build.mkdir()
                    library = build / "libtent_metrics.so"
                    library.write_bytes(b"shared dependency")
                    binary = build / "tent_xpu_platform_test"
                    binary.write_bytes(b"native test")
                    environment = {**os.environ, "CI": ci, "BUILD_DIR": str(build)}
                    environment.pop("FREE_BUILD_DIR", None)
                    if free_build_dir is not None:
                        environment["FREE_BUILD_DIR"] = free_build_dir
                    environment["CUDA_EP_STAGING_DIR"] = str(build / "ep_pg_staging")
                    environment["CUDA_EP_JIT_DIR"] = str(build / "mooncake_ep_jit")
                    result = subprocess.run(
                        ["bash", "-ec", code],
                        env=environment,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(build.exists(), preserved)
                    if preserved:
                        self.assertEqual(library.read_bytes(), b"shared dependency")
                        self.assertEqual(binary.read_bytes(), b"native test")


class WheelVariantTest(unittest.TestCase):
    def test_xpu_metadata_and_mutual_exclusion(self):
        script = (ROOT / "scripts/build_wheel.sh").read_text()
        block = script[
            script.index('BUILD_VARIANTS="') : script.index(
                'echo "Cleaning up previous build artifacts'
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "pyproject.toml"
            original = (ROOT / "mooncake-wheel/pyproject.toml").read_text()
            project.write_text(original)
            environment = {
                key: value
                for key, value in os.environ.items()
                if not key.endswith("_BUILD")
            }
            environment["XPU_BUILD"] = "1"
            result = subprocess.run(
                ["bash", "-ec", block],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            modified = project.read_text()
            self.assertIn('name = "mooncake-transfer-engine-xpu"', modified)
            self.assertNotIn("NVIDIA CUDA", modified)
            self.assertIn('"intel", "xpu", "sycl"', modified)
            self.assertEqual(
                (Path(directory) / "pyproject.toml.backup").read_text(), original
            )
            project.write_text(original)
            environment["HIP_BUILD"] = "1"
            result = subprocess.run(
                ["bash", "-ec", block],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("only one", result.stdout)
            self.assertEqual(project.read_text(), original)


class WorkflowTest(unittest.TestCase):
    def test_xpu_selection_and_strict_gate_are_wired(self):
        workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
        jobs = workflow["jobs"]
        filters = yaml.safe_load(jobs["check-paths"]["steps"][1]["with"]["filters"])
        for path in (
            "python/**",
            ".github/workflows/ci_xpu.yml",
        ):
            self.assertIn(path, filters["xpu"])
        self.assertIn("workflow_dispatch", jobs["check-paths"]["outputs"]["xpu"])
        self.assertIn("outputs.xpu == 'true'", jobs["build-wheel-xpu"]["if"])
        gate = jobs["ci-gate"]
        self.assertIn("build-wheel-xpu", gate["needs"])
        step = next(
            s for s in gate["steps"] if s["name"] == "Check XPU build and packaging"
        )
        for selected, result, passes in (
            (True, "success", True),
            (False, "skipped", True),
            (True, "skipped", False),
            (True, "failure", False),
            (True, "cancelled", False),
            (False, "failure", False),
        ):
            status = subprocess.run(
                ["bash", "-ec", step["run"]],
                env={
                    **os.environ,
                    "EXPECTED": "success" if selected else "skipped",
                    "RESULT": result,
                },
                capture_output=True,
                timeout=5,
            ).returncode
            self.assertEqual(status == 0, passes, (selected, result))
        nightly = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())[
            "jobs"
        ]
        self.assertIn("build-wheel-xpu", nightly["nightly-gate"]["needs"])
        self.assertNotIn("build-wheel-xpu", nightly["publish-testpypi"]["needs"])

    def test_uses_rocm_style_container_without_docker_or_orchestration_scripts(self):
        job = WORKFLOW["jobs"]["build-wheel-xpu"]
        self.assertIn("@sha256:", job["container"])
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        self.assertNotIn("docker ", commands)
        self.assertIn("bash scripts/build_wheel.sh", commands)
        for name in ("build_xpu.sh", "package_xpu_tests.sh", "check_xpu_host_tests.py"):
            self.assertFalse((ROOT / "scripts/ci" / name).exists())
            self.assertNotIn(name, commands)
        self.assertLess(
            [step.get("name") for step in job["steps"]].index(
                "Install git (pre-checkout)"
            ),
            [step.get("name") for step in job["steps"]].index("Check out source"),
        )

    def test_hosted_wheelhouse_is_hashed_inside_the_native_bundle(self):
        script = workflow_step("Package Python dependencies for hardware")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = root / "xpu-ci-output"
            payload.mkdir()
            wheel = payload / "fixture.whl"
            wheel.write_bytes(b"wheel")
            python = root / "python"
            python.write_text("""#!/bin/bash
set -eu
[ "$*" = "-m pip download --only-binary=:all: --dest xpu-ci-output/python-deps xpu-ci-output/fixture.whl" ]
cp xpu-ci-output/fixture.whl xpu-ci-output/python-deps/
echo dependency > xpu-ci-output/python-deps/dependency.whl
""")
            python.chmod(0o755)
            result = subprocess.run(
                ["bash", "-ec", script],
                cwd=root,
                env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}"},
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                [path.name for path in (payload / "python-deps").iterdir()],
                ["dependency.whl"],
            )
        self.assertIn(
            'cp -R xpu-ci-output/python-deps "$out/"',
            workflow_step("Package native tests"),
        )

    def test_unified_trigger_and_matching_artifact_handoff(self):
        jobs = E2E_WORKFLOW["jobs"]
        event = E2E_WORKFLOW.get("on", E2E_WORKFLOW.get(True))
        self.assertEqual(event["pull_request_target"]["types"], ["labeled"])
        self.assertNotIn("synchronize", event["pull_request_target"]["types"])
        self.assertIn("run-e2e-ci", jobs["xpu-authorize"]["if"])
        self.assertIn("workflow_dispatch", jobs["xpu-authorize"]["if"])
        self.assertTrue(E2E_WORKFLOW["concurrency"]["cancel-in-progress"])
        self.assertFalse((ROOT / ".github/workflows/xpu-core.yml").exists())
        self.assertEqual(
            jobs["xpu-authorize"]["steps"][0]["env"],
            {
                "PR_NUMBER": "${{ inputs.pr_number }}",
                "CHECKOUT_SHA": "${{ inputs.pr_sha }}",
            },
        )
        self.assertEqual(jobs["build-wheel-xpu"]["needs"], "xpu-authorize")
        self.assertEqual(
            jobs["build-wheel-xpu"]["with"]["checkout_ref"],
            "${{ needs.xpu-authorize.outputs.sha }}",
        )
        self.assertEqual(
            jobs["xpu-integration-test"]["needs"],
            ["xpu-authorize", "build-wheel-xpu"],
        )
        self.assertEqual(
            jobs["xpu-integration-test"]["with"]["checkout_sha"],
            "${{ needs.xpu-authorize.outputs.sha }}",
        )
        for name in ("artifact_id", "artifact_digest", "source_sha"):
            self.assertEqual(
                jobs["xpu-integration-test"]["with"][name],
                "${{ needs.build-wheel-xpu.outputs." + name + " }}",
            )
        self.assertEqual(
            set(jobs["e2e-gate"]["needs"]),
            {
                "ascend-test",
                "build-wheel-rocm",
                "integration-test",
                "rocm-integration-test",
                "integration-test-tent",
                "xpu-authorize",
                "build-wheel-xpu",
                "xpu-integration-test",
            },
        )
        self.assertIn("always()", jobs["e2e-gate"]["if"])
        self.assertEqual(
            jobs["e2e-gate"]["permissions"]["checks"],
            "write",
        )
        self.assertNotIn("continue-on-error", jobs["e2e-gate"]["steps"][0])
        self.assertEqual(jobs["cleanup-label"]["needs"], ["e2e-gate"])
        integration = yaml.safe_load(
            (ROOT / ".github/workflows/integration-test-xpu.yml").read_text()
        )
        job = integration["jobs"]["core"]
        self.assertEqual(job["runs-on"], "xpu")
        build = yaml.safe_load((ROOT / ".github/workflows/ci_xpu.yml").read_text())
        self.assertEqual(
            job["env"]["XPU_IMAGE"], build["jobs"]["build-wheel-xpu"]["container"]
        )
        self.assertIn("github.workflow_sha", job["steps"][0]["with"]["ref"])
        self.assertTrue(
            all(
                not step.get("with", {}).get("persist-credentials", False)
                for step in job["steps"]
            )
        )

    def run_javascript(self, job, environment, state):
        script = next(
            step["with"]["script"]
            for step in E2E_WORKFLOW["jobs"][job]["steps"]
            if "script" in step.get("with", {})
        )
        harness = r"""
        const state = JSON.parse(process.env.TEST_STATE);
        const context = {
          repo: {owner: 'owner', repo: 'repo'}, actor: 'maintainer',
          ref: state.ref || 'refs/heads/main', eventName: state.event || 'workflow_dispatch',
          sha: state.sha || 'c'.repeat(40),
          payload: state.payload || {}, serverUrl: 'https://github.com', runId: 123
        };
        const outputs = {};
        const core = {
          setOutput: (key, value) => outputs[key] = value,
          setFailed: message => { throw new Error(message); },
          summary: {addRaw() {return this;}, async write() {}}
        };
        const github = {rest: {
          repos: {
            getCollaboratorPermissionLevel: async () => ({data: {permission: state.permission || 'write'}}),
            getCommit: async ({ref}) => ({data: {sha: state.commit || ref}})
          },
          pulls: {get: async () => ({data: {
            state: state.pullState || 'open', base: {ref: state.base || 'main'},
            head: {sha: state.head}
          }})},
          checks: {
            create: async request => { outputs.created = request; return {data: {id: 456}}; },
            update: async request => { outputs.check = request; }
          }
        }};
        (async () => {
        SCRIPT
        })().then(() => console.log(JSON.stringify(outputs)))
          .catch(error => {
            console.log(JSON.stringify(outputs));
            console.error(error.message);
            process.exitCode = 1;
          });
        """
        return subprocess.run(
            ["node", "-e", harness.replace("SCRIPT", script)],
            env={**os.environ, **environment, "TEST_STATE": json.dumps(state)},
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_authorization_and_stale_head(self):
        sha = "a" * 40
        environment = {"PR_NUMBER": "1", "CHECKOUT_SHA": sha}
        for state, valid in (
            ({"head": sha}, True),
            ({"head": sha, "permission": "maintain"}, True),
            ({"head": sha, "permission": "admin"}, True),
            ({"head": sha, "permission": "read"}, False),
            ({"head": "b" * 40}, False),
            ({"head": sha, "ref": "refs/heads/untrusted"}, False),
            ({"head": sha, "pullState": "closed"}, False),
            ({"head": sha, "base": "release"}, False),
            ({"head": sha, "commit": "b" * 40}, False),
            (
                {
                    "head": sha,
                    "event": "pull_request_target",
                    "payload": {"pull_request": {"number": 1, "head": {"sha": sha}}},
                },
                True,
            ),
            (
                {
                    "head": sha,
                    "event": "pull_request_target",
                    "payload": {
                        "pull_request": {"number": 1, "head": {"sha": "b" * 40}}
                    },
                },
                False,
            ),
        ):
            with self.subTest(state=state):
                result = self.run_javascript("xpu-authorize", environment, state)
                self.assertEqual(result.returncode == 0, valid, result.stderr)
                if valid:
                    outputs = json.loads(result.stdout)
                    self.assertEqual(outputs["sha"], sha)
                    self.assertEqual(outputs["created"]["head_sha"], sha)

    def test_dispatch_revision_selection(self):
        sha = "a" * 40
        for environment, state, expected in (
            ({"PR_NUMBER": "", "CHECKOUT_SHA": ""}, {"sha": sha}, sha),
            ({"PR_NUMBER": "", "CHECKOUT_SHA": sha}, {}, sha),
            ({"PR_NUMBER": "1", "CHECKOUT_SHA": ""}, {"head": sha}, sha),
            ({"PR_NUMBER": "", "CHECKOUT_SHA": "main"}, {}, None),
            ({"PR_NUMBER": "01", "CHECKOUT_SHA": sha}, {}, None),
            (
                {"PR_NUMBER": "", "CHECKOUT_SHA": ""},
                {"ref": "refs/heads/untrusted", "sha": sha},
                None,
            ),
        ):
            with self.subTest(environment=environment, state=state):
                result = self.run_javascript("xpu-authorize", environment, state)
                self.assertEqual(
                    result.returncode == 0, expected is not None, result.stderr
                )
                if expected is not None:
                    self.assertEqual(json.loads(result.stdout)["sha"], expected)

    def test_gate_requires_success_and_current_head(self):
        sha = "a" * 40
        for job in ("xpu-authorize", "build-wheel-xpu", "xpu-integration-test"):
            for status in ("success", "failure", "cancelled", "skipped", "missing"):
                needs = {
                    "xpu-authorize": {
                        "result": "success",
                        "outputs": {"sha": sha, "pr": "1", "check_id": "456"},
                    },
                    "build-wheel-xpu": {
                        "result": "success",
                        "outputs": {"source_sha": sha},
                    },
                    "xpu-integration-test": {"result": "success"},
                }
                if status == "missing":
                    del needs[job]
                else:
                    needs[job]["result"] = status
                for head in (sha, "b" * 40):
                    with self.subTest(job=job, status=status, head=head):
                        completed = self.run_javascript(
                            "e2e-gate",
                            {"NEEDS_JSON": json.dumps(needs)},
                            {"head": head},
                        )
                        valid = status == "success" and head == sha
                        self.assertEqual(
                            completed.returncode == 0, valid, completed.stderr
                        )
                        if job != "xpu-authorize" or status == "success":
                            check = json.loads(completed.stdout)["check"]
                            self.assertEqual(
                                check["conclusion"], "success" if valid else "failure"
                            )

    def test_gate_rejects_mismatched_or_missing_build_sha(self):
        sha = "a" * 40
        for build_outputs in ({}, {"source_sha": "b" * 40}):
            needs = {
                "xpu-authorize": {
                    "result": "success",
                    "outputs": {"sha": sha, "pr": "1", "check_id": "456"},
                },
                "build-wheel-xpu": {"result": "success", "outputs": build_outputs},
                "xpu-integration-test": {"result": "success"},
            }
            with self.subTest(build_outputs=build_outputs):
                result = self.run_javascript(
                    "e2e-gate", {"NEEDS_JSON": json.dumps(needs)}, {"head": sha}
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("build SHA differs", result.stderr)

    def test_shared_gate_still_checks_other_platforms(self):
        steps = E2E_WORKFLOW["jobs"]["e2e-gate"]["steps"]
        self.assertEqual(steps[0]["uses"], "actions/github-script@v7")
        step = next(step for step in steps if step["name"] == "Check E2E results")
        script = re.sub(r"\$\{\{.*?\}\}", "fixture", step["run"])
        for status in ("success", "failure", "cancelled", "skipped"):
            with self.subTest(status=status):
                result = subprocess.run(
                    ["bash", "-ec", script],
                    env={
                        **os.environ,
                        "NEEDS_JSON": json.dumps(
                            {
                                "rocm-integration-test": {"result": status},
                                "xpu-integration-test": {"result": "success"},
                            }
                        ),
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(
                    result.returncode == 0,
                    status in ("success", "skipped"),
                    result.stderr,
                )


class CommandLineTest(unittest.TestCase):
    def test_no_arguments_preserve_host_only_wheel_checks(self):
        with (
            patch.object(sys, "argv", ["verify_xpu_wheel.py"]),
            patch.object(VERIFY, "main") as verify,
        ):
            VERIFY.cli()
            verify.assert_called_once_with()

    def test_hardware_subcommands_and_unknown_command(self):
        for arguments, callback, expected in (
            (
                ["unpack", "dir", "sha", "42", "digest"],
                "unpack",
                [Path("dir"), "sha", "42", "digest"],
            ),
            (
                ["run", "bundle", "source", "output"],
                "run",
                [Path("bundle"), Path("source"), Path("output")],
            ),
            (
                ["suite", "native", "source", "output"],
                "suite",
                [Path("native"), Path("source"), Path("output")],
            ),
            (
                ["store", "source", "report"],
                "store_test",
                [Path("source"), Path("report")],
            ),
            (["seccomp", "profile"], "prepare_seccomp", [Path("profile")]),
        ):
            with (
                self.subTest(arguments=arguments),
                patch.object(sys, "argv", ["verify_xpu_wheel.py", *arguments]),
                patch.object(VERIFY, callback) as command,
            ):
                VERIFY.cli()
                command.assert_called_once_with(*expected)
        with patch.object(sys, "argv", ["verify_xpu_wheel.py", "unknown"]):
            with self.assertRaisesRegex(RuntimeError, "Unknown XPU verification"):
                VERIFY.cli()


class InstalledWheelTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.package = self.root / "mooncake"
        self.package.mkdir()
        self.modules = {
            "mooncake": SimpleNamespace(__file__=str(self.package / "__init__.py"))
        }
        self.records = []
        for filename in (
            "engine.so",
            "_store.so",
            "_ep.so",
            "_fast_copy.so",
            "mooncake_master",
        ):
            path = self.package / filename
            path.write_bytes(filename.encode())
            record = metadata.PackagePath(f"mooncake/{filename}")
            record.hash = metadata.FileHash(
                f"sha256={ROCM.record_digest(path, 'sha256')}"
            )
            self.records.append(record)
            if filename.endswith(".so"):
                self.modules[f"mooncake.{filename[:-3]}"] = SimpleNamespace(
                    __file__=str(path)
                )
        self.modules["mooncake.engine"].SUPPORT_XPU = True
        self.modules["mooncake.engine"].SUPPORT_TENT = True
        self.modules["mooncake.engine"].SUPPORT_CUDA = False
        self.modules["mooncake.ep"] = SimpleNamespace(DEVICE_TYPE="xpu")
        store_class = object()
        self.modules["mooncake._store"].MooncakeDistributedStore = store_class
        self.modules["mooncake.store"] = SimpleNamespace(
            _BACKEND="cpp", MooncakeDistributedStore=store_class
        )
        distribution = SimpleNamespace(
            files=self.records,
            version="0.0.0.dev0",
            locate_file=lambda record: self.root / str(record),
        )
        for patcher in (
            patch.object(VERIFY.metadata, "distribution", return_value=distribution),
            patch.object(
                VERIFY.importlib, "import_module", side_effect=self.modules.__getitem__
            ),
            patch.object(VERIFY.subprocess, "run"),
            patch.object(VERIFY.subprocess, "Popen"),
            patch.object(VERIFY.socket, "create_connection"),
            patch.dict(os.environ),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.popen = VERIFY.subprocess.Popen.return_value.__enter__.return_value
        self.popen.poll.return_value = None

    def test_accepts_exact_wheel_and_stops_master(self):
        VERIFY.main()
        self.popen.terminate.assert_called_once()

    def test_rejects_modified_extension(self):
        (self.package / "engine.so").write_bytes(b"different build")
        with self.assertRaisesRegex(RuntimeError, "RECORD mismatch"):
            VERIFY.main()

    def test_rejects_shadowed_import(self):
        self.modules["mooncake.engine"].__file__ = "/tmp/another-engine.so"
        with self.assertRaisesRegex(RuntimeError, "outside wheel"):
            VERIFY.main()

    def test_rejects_missing_xpu_or_tent_capability(self):
        for capability in ("SUPPORT_XPU", "SUPPORT_TENT"):
            with self.subTest(capability=capability):
                setattr(self.modules["mooncake.engine"], capability, False)
                with self.assertRaisesRegex(RuntimeError, "native XPU/TENT"):
                    VERIFY.main()
                setattr(self.modules["mooncake.engine"], capability, True)

    def test_rejects_non_xpu_ep(self):
        self.modules["mooncake.ep"].DEVICE_TYPE = "cuda"
        with self.assertRaisesRegex(RuntimeError, "native XPU EP"):
            VERIFY.main()

    def test_rejects_failed_master(self):
        self.popen.poll.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "master exited"):
            VERIFY.main()
        self.popen.terminate.assert_called_once()

    def test_rejects_master_readiness_timeout_and_cleans_up(self):
        with (
            patch.object(VERIFY.socket, "create_connection", side_effect=OSError),
            patch.object(VERIFY.time, "sleep"),
        ):
            with self.assertRaisesRegex(RuntimeError, "did not start"):
                VERIFY.main()
        self.popen.terminate.assert_called_once()


class NativeBundleTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.build = self.root / "build-xpu-ci"
        self.out = self.root / "xpu-ci-output" / "native-tests"
        self.out.parent.mkdir()
        (self.out.parent / "provenance.txt").write_text("source_sha=test\n")
        (self.out.parent / "fixture.whl").write_bytes(b"wheel artifact")
        (self.out.parent / "python-deps").mkdir()
        (self.out.parent / "python-deps" / "fixture-dep.whl").write_bytes(b"dependency")
        self.build.mkdir()
        (self.build / "CMakeCache.txt").write_text("USE_XPU:BOOL=ON\n")
        self.library = self.root / "libfixture.so"
        self.library.write_bytes(b"shared dependency")
        self.tools = self.root / "tools"
        self.tools.mkdir()
        self.ldd = self.tools / "ldd"
        self.ldd.write_text(
            f"#!/bin/sh\necho 'libfixture.so => {self.library} (0x1234)'\n"
        )
        self.ldd.chmod(0o755)
        for name in (
            "tent_xpu_platform_test",
            "tent_xpu_transport_test",
            "tent_xpu_engine_test",
            "tent_xpu_ipc_process_test",
            "pybind_client_test",
        ):
            directory = (
                "mooncake-store/tests"
                if name == "pybind_client_test"
                else "mooncake-transfer-engine/tent/tests"
            )
            binary = self.build / directory / name
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.write_text(
                "#!/bin/sh\nprintf 'RealClientTest.\\n  XpuDeviceBufferPutGetRoundTrips\\n'\n"
            )
            binary.chmod(0o755)

    def package(self):
        code = workflow_step("Package native tests").replace(
            "source /opt/intel/oneapi/setvars.sh >/dev/null", ":"
        )
        return subprocess.run(
            ["bash", "-ec", code],
            cwd=self.root,
            env={
                **os.environ,
                "PATH": f"{self.tools}:{os.environ['PATH']}",
                "GITHUB_WORKSPACE": str(self.root),
                "BUILD_DIR": self.build.name,
            },
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_packages_dependencies_inventories_and_hashes(self):
        result = self.package()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(list(self.out.glob("*.list"))), 5)
        self.assertEqual(
            (self.out / "lib/libfixture.so").read_bytes(), self.library.read_bytes()
        )
        self.assertIn("provenance.txt", (self.out / "SHA256SUMS").read_text())
        result = subprocess.run(
            ["sha256sum", "--check", "SHA256SUMS"],
            cwd=self.out,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_rejects_unresolved_dependency(self):
        self.ldd.write_text("#!/bin/sh\necho 'libfixture.so => not found'\n")
        result = self.package()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not found", result.stderr)

    def test_rejects_missing_required_executable(self):
        (self.build / "mooncake-store/tests/pybind_client_test").unlink()
        self.assertNotEqual(self.package().returncode, 0)

    def test_rejects_zero_test_inventory(self):
        binary = (
            self.build / "mooncake-transfer-engine/tent/tests/tent_xpu_platform_test"
        )
        binary.write_text("#!/bin/sh\necho 'Running main from gtest_main.cc'\n")
        result = self.package()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Empty GoogleTest inventory", result.stderr)


def write_sums(root, excluded=()):
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{VERIFY.digest(path)}  ./{path.relative_to(root)}\n"
            for path in sorted(root.rglob("*"))
            if path.is_file() and path.name not in (*excluded, "SHA256SUMS")
        )
    )


class ArtifactTest(unittest.TestCase):
    def test_exact_artifact_identity_digest_sha_and_complete_manifest(self):
        sha = "a" * 40
        image = "intel/pytorch@sha256:" + "b" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            provenance = f"source_sha={sha}\nimage={image}\nci_run=123/2\n"
            wheel = "mooncake_transfer_engine_xpu-0.0.0-cp312-cp312-linux_x86_64.whl"
            payload = {wheel: b"wheel", "native-tests.tar.gz": b"native"}
            sums = "".join(
                f"{hashlib.sha256(data).hexdigest()}  ./{name}\n"
                for name, data in payload.items()
            )
            payload.update(
                {"provenance.txt": provenance.encode(), "SHA256SUMS": sums.encode()}
            )
            with zipfile.ZipFile(root / "artifact.zip", "w") as archive:
                for name, data in payload.items():
                    archive.writestr(name, data)
            checksum = VERIFY.digest(root / "artifact.zip")
            original = {
                "id": 42,
                "name": f"mooncake-xpu-ci-py312-{sha}-123-2",
                "expired": False,
                "workflow_run": {"id": 123},
                "digest": f"sha256:{checksum}",
            }
            environment = {
                "GITHUB_RUN_ID": "123",
                "GITHUB_RUN_ATTEMPT": "2",
                "XPU_IMAGE": image,
            }
            for change in (
                {"id": 43},
                {"name": "latest"},
                {"expired": True},
                {"workflow_run": {"id": 122}},
                {"digest": "sha256:" + "0" * 64},
            ):
                with self.subTest(change=change), patch.dict(os.environ, environment):
                    (root / "artifact.json").write_text(
                        json.dumps({**original, **change})
                    )
                    with self.assertRaises(RuntimeError):
                        VERIFY.unpack(root, sha, "42", checksum)
            (root / "artifact.json").write_text(json.dumps(original))
            with patch.dict(os.environ, environment):
                with self.assertRaises(RuntimeError):
                    VERIFY.unpack(root, sha, "42", "0" * 64)
                VERIFY.unpack(root, sha, "42", checksum)
            bundle = root / "bundle"
            with self.assertRaises(RuntimeError):
                VERIFY.verify_provenance(bundle, "c" * 40, image, "123/2")
            with self.assertRaises(RuntimeError):
                VERIFY.verify_provenance(bundle, sha, image + "changed", "123/2")
            (bundle / wheel).write_bytes(b"corrupted")
            with self.assertRaises(RuntimeError):
                VERIFY.verify_hashes(bundle, {"provenance.txt"})

    def test_zip_rejects_traversal_and_duplicate_entries(self):
        for names in (
            ["../escape", "a", "b", "c"],
            ["/escape", "a", "b", "c"],
            ["nested/file", "a", "b", "c"],
        ):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with zipfile.ZipFile(root / "artifact.zip", "w") as archive:
                    for name in names:
                        archive.writestr(name, b"data")
                checksum = VERIFY.digest(root / "artifact.zip")
                (root / "artifact.json").write_text(
                    json.dumps(
                        {
                            "id": 1,
                            "name": f"mooncake-xpu-ci-py312-{'a' * 40}-1-1",
                            "expired": False,
                            "workflow_run": {"id": 1},
                            "digest": f"sha256:{checksum}",
                        }
                    )
                )
                with patch.dict(
                    os.environ, {"GITHUB_RUN_ID": "1", "GITHUB_RUN_ATTEMPT": "1"}
                ):
                    with self.assertRaises(RuntimeError):
                        VERIFY.unpack(root, "a" * 40, "1", checksum)

    def test_hash_inventory_rejects_unhashed_missing_duplicate_and_unsafe_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "a").write_bytes(b"a")
            write_sums(root)
            valid = (root / "SHA256SUMS").read_text()
            VERIFY.verify_hashes(root)
            for content in (
                "",
                valid * 2,
                valid.replace("./a", "../a"),
                valid + "invalid\n",
            ):
                with self.subTest(content=content):
                    (root / "SHA256SUMS").write_text(content)
                    with self.assertRaises(RuntimeError):
                        VERIFY.verify_hashes(root)
            (root / "SHA256SUMS").write_text(valid)
            (root / "unhashed").write_text("extra")
            with self.assertRaises(RuntimeError):
                VERIFY.verify_hashes(root)

    def test_native_tar_roundtrip_and_rejection_of_links_and_traversal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            native = root / "native-tests"
            native.mkdir()
            (native / "provenance.txt").write_text("source provenance")
            (native / "test").write_bytes(b"executable")
            (native / "test").chmod(0o755)
            write_sums(native)
            (root / "provenance.txt").write_bytes(
                (native / "provenance.txt").read_bytes()
            )
            with tarfile.open(root / "native-tests.tar.gz", "w:gz") as archive:
                archive.add(native, arcname="native-tests")
            output = root / "valid"
            output.mkdir()
            extracted = VERIFY.extract_native(root, output)
            self.assertEqual((extracted / "test").stat().st_mode & 0o777, 0o755)
            for index, (name, kind) in enumerate(
                (
                    ("../escape", tarfile.REGTYPE),
                    ("native-tests/link", tarfile.SYMTYPE),
                    ("native-tests/link", tarfile.LNKTYPE),
                    ("native-tests/device", tarfile.CHRTYPE),
                )
            ):
                with self.subTest(name=name, kind=kind):
                    entry = tarfile.TarInfo(name)
                    entry.type = kind
                    with tarfile.open(root / "native-tests.tar.gz", "w:gz") as archive:
                        archive.addfile(entry, io.BytesIO())
                    output = root / str(index)
                    output.mkdir()
                    with self.assertRaises(RuntimeError):
                        VERIFY.extract_native(root, output)


class ReportTest(unittest.TestCase):
    def test_suite_requires_store_report_and_ep_completion_before_success(self):
        import importlib.metadata

        def write_report(path, identities):
            root = ET.Element("testsuites")
            suite = ET.SubElement(root, "testsuite")
            for identity in identities:
                classname, name = identity.split(".", 1)
                ET.SubElement(
                    suite,
                    "testcase",
                    classname=classname,
                    name=name,
                    status="run",
                    result="completed",
                )
            ET.ElementTree(root).write(path)

        def inventory_text(identities):
            return "".join(
                f"{identity.split('.', 1)[0]}.\n  {identity.split('.', 1)[1]}\n"
                for identity in sorted(identities)
            )

        for store_report, ep_complete, valid in (
            (True, True, True),
            (False, True, False),
            (True, False, False),
        ):
            with self.subTest(store_report=store_report, ep_complete=ep_complete):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    for name, (inventory, _) in VERIFY.SUITES.items():
                        (root / f"{name}.list").write_text(inventory_text(inventory))

                    def run_command(args, log, timeout=600):
                        if log.name == "wheel.log":
                            self.assertEqual(args, [sys.executable, VERIFY.__file__])
                            self.assertTrue(Path(args[1]).is_file())
                        if "--gtest_list_tests" in args:
                            return inventory_text(VERIFY.SUITES[Path(args[0]).name][0])
                        for argument in args:
                            if argument.startswith("--gtest_output=xml:"):
                                write_report(
                                    Path(argument.split("xml:", 1)[1]),
                                    VERIFY.SUITES[Path(args[0]).name][1],
                                )
                        if len(args) > 2 and args[2] == "store" and store_report:
                            write_report(
                                Path(args[4]), {"StorePython.test_xpu_local_copy_paths"}
                            )
                        if "test_mooncake_ep_xpu.py" in args[1]:
                            if not ep_complete:
                                return ""
                            return "".join(
                                f"round {i}: tokens=0 ok\n" for i in range(8)
                            ) + (
                                "PASSED: 8 rounds + 2 empty-rank rounds + 4 two-batch rounds"
                                " + duplicate experts + reused src_info + rejected handle"
                                " + cross-stream + fallback agreement"
                                " on 2 ranks (hidden=1024, experts=8, topk=2)\n"
                            )
                        return ""

                    with (
                        patch.object(VERIFY, "preflight"),
                        patch.object(VERIFY, "command", side_effect=run_command),
                        patch.object(VERIFY.socket, "create_connection"),
                        patch.object(VERIFY.subprocess, "Popen") as process,
                        patch.object(
                            importlib.metadata, "distribution"
                        ) as distribution,
                        patch.dict(os.environ, {"XPU_SOURCE_SHA": "a" * 40}),
                    ):
                        distribution.return_value.locate_file.return_value = root
                        process.return_value.__enter__.return_value.poll.return_value = None
                        if valid:
                            VERIFY.suite(root, ROOT, root)
                            result = json.loads((root / "result.json").read_text())
                            self.assertEqual(result["cpp_cases"], 149)
                            self.assertEqual(result["ep_rounds"], 8)
                        else:
                            with self.assertRaises((RuntimeError, FileNotFoundError)):
                                VERIFY.suite(root, ROOT, root)
                            self.assertFalse((root / "result.json").exists())

    def test_exact_case_identity_count_and_no_skips_or_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.xml"
            for attributes, child, valid in (
                ({"status": "run", "result": "completed"}, None, True),
                ({"status": "notrun", "result": "suppressed"}, None, False),
                ({"status": "run", "result": "skipped"}, "skipped", False),
                ({"status": "run", "result": "completed"}, "failure", False),
                ({"status": "run", "result": "completed"}, "error", False),
            ):
                root = ET.Element("testsuites")
                suite = ET.SubElement(root, "testsuite")
                case = ET.SubElement(
                    suite, "testcase", classname="Suite", name="Case", **attributes
                )
                if child:
                    ET.SubElement(case, child)
                ET.ElementTree(root).write(report)
                with self.subTest(attributes=attributes, child=child):
                    if valid:
                        VERIFY.validate_report(report, {"Suite.Case"})
                        for expected in (
                            set(),
                            {"Suite.Other"},
                            {"Suite.Case", "Suite.Missing"},
                        ):
                            with self.assertRaises(RuntimeError):
                                VERIFY.validate_report(report, expected)
                        suite.append(ET.fromstring(ET.tostring(case)))
                        ET.ElementTree(root).write(report)
                        with self.assertRaises(RuntimeError):
                            VERIFY.validate_report(report, {"Suite.Case"})
                    else:
                        with self.assertRaises(RuntimeError):
                            VERIFY.validate_report(report, {"Suite.Case"})

    def test_inventory_parser(self):
        self.assertEqual(
            VERIFY.parse_inventory(
                "noise\nModes/Suite.\n  Case/0  # GetParam() = false\n  Case/1\n"
            ),
            {"Modes/Suite.Case/0", "Modes/Suite.Case/1"},
        )
        for text in ("", "noise\n", "  Case\n", "Suite.\n  Case\n  Case\n"):
            with self.subTest(text=text), self.assertRaises(RuntimeError):
                VERIFY.parse_inventory(text)

    def test_required_inventory_matches_cpp_sources(self):
        tent = ROOT / "mooncake-transfer-engine/tent/tests/xpu"
        for filename, names in (
            ("xpu_platform_test.cpp", VERIFY.PLATFORM),
            ("xpu_transport_test.cpp", VERIFY.TRANSPORT),
            ("xpu_engine_test.cpp", VERIFY.ENGINE | VERIFY.RDMA),
            ("xpu_ipc_process_test.cpp", VERIFY.IPC),
        ):
            declared = set(
                re.findall(
                    r"TEST_[FP]\(\s*(\w+),\s*(\w+)\s*\)", (tent / filename).read_text()
                )
            )
            selected = {
                (
                    identity.split(".")[0].split("/")[-1],
                    identity.split(".")[1].split("/")[0],
                )
                for identity in names
            }
            self.assertEqual(selected, declared, filename)
        self.assertEqual(len(VERIFY.ENGINE), 124)
        self.assertEqual(len(VERIFY.INAPPLICABLE), 10)
        self.assertEqual(
            sum(len(selected) for _, selected in VERIFY.SUITES.values()), 149
        )
        selected_engine = VERIFY.SUITES["tent_xpu_engine_test"][1]
        self.assertFalse(selected_engine & VERIFY.RDMA)
        self.assertFalse(selected_engine & VERIFY.INAPPLICABLE.keys())

    def test_command_exit_and_timeout_propagate(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "command.log"
            diagnostics = io.StringIO()
            with (
                patch.object(VERIFY.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                VERIFY.command(["bash", "-c", "echo failed; exit 2"], log)
            self.assertIn("failed", log.read_text())
            self.assertIn(str(log), diagnostics.getvalue())
            self.assertIn("failed", diagnostics.getvalue())
            diagnostics = io.StringIO()
            with (
                patch.object(VERIFY.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.TimeoutExpired),
            ):
                VERIFY.command(["sleep", "2"], log, timeout=0.01)
            self.assertIn(str(log), diagnostics.getvalue())

    def test_command_failure_diagnostics_are_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "command.log"
            diagnostics = io.StringIO()
            with (
                patch.object(VERIFY.sys, "stderr", diagnostics),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                VERIFY.command(["bash", "-c", "seq 1 100; exit 2"], log)
            lines = diagnostics.getvalue().splitlines()
            self.assertEqual(len(lines), 41)
            self.assertEqual(lines[1:], [str(i) for i in range(61, 101)])

    def test_device_preflight_rejects_missing_wrong_and_unexecuted_devices(self):
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            for count, name, total, valid in (
                (0, "Intel Arc Pro B60", 2080, False),
                (1, "Intel Arc Pro B60", 2080, False),
                (2, "CPU fallback", 2080, False),
                (2, "Intel Arc Pro B60", 0, False),
                (2, "Intel Arc Pro B60", 2080, True),
            ):
                tensor = unittest.mock.MagicMock()
                tensor.__add__.return_value.sum.return_value.cpu.return_value = total
                torch = SimpleNamespace(
                    __version__="2.13.0+xpu",
                    xpu=SimpleNamespace(
                        is_available=lambda: count > 0,
                        device_count=lambda: count,
                        get_device_properties=lambda index: SimpleNamespace(
                            name=name, total_memory=24 << 30
                        ),
                        synchronize=lambda index: None,
                    ),
                    arange=lambda *args, **kwargs: tensor,
                )
                read_text = Path.read_text

                def runtime_text(path, *args, **kwargs):
                    if str(path) == "/proc/self/status":
                        return "CapEff:\t00000000\n"
                    if str(path) == "/proc/sys/kernel/yama/ptrace_scope":
                        return "1"
                    return read_text(path, *args, **kwargs)

                with (
                    self.subTest(count=count, name=name, total=total),
                    patch.dict("sys.modules", {"torch": torch}),
                    patch.dict(
                        os.environ,
                        {
                            "ONEAPI_DEVICE_SELECTOR": "level_zero:gpu",
                            "ZE_AFFINITY_MASK": "0,1",
                            "XPU_IMAGE": "image",
                            "XPU_SOURCE_SHA": "a" * 40,
                        },
                    ),
                    patch.object(Path, "read_text", runtime_text),
                ):
                    if valid:
                        VERIFY.preflight(output)
                    else:
                        with self.assertRaises(RuntimeError):
                            VERIFY.preflight(output)


class ShellLifecycleTest(unittest.TestCase):
    def test_inline_hardware_shell_syntax_and_removed_launchers(self):
        for step in INTEGRATION["jobs"]["core"]["steps"]:
            if "run" in step:
                with self.subTest(step=step["name"]):
                    result = subprocess.run(
                        ["bash", "-n"],
                        input=step["run"],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
        for name in ("run_xpu_core.sh", "xpu_core.py"):
            self.assertFalse((ROOT / "scripts/ci" / name).exists())
        self.assertIn(
            "scripts/e2e/python/verify_xpu_wheel.py unpack",
            hardware_step("Download exact build artifact"),
        )

    def prepare_run(self, root):
        output = root / "github-output"
        subprocess.run(
            ["bash", "-ec", hardware_step("Allocate run directory")],
            env={
                **os.environ,
                "RUNNER_TEMP": str(root),
                "GITHUB_OUTPUT": str(output),
            },
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return Path(output.read_text().strip().removeprefix("directory="))

    def test_network_isolation_sets_store_and_ep_loopback_interfaces(self):
        script = hardware_step("Execute isolated two-B60 core")
        start = script.index("container_id=$(docker create")
        stop = script.index("timeout --signal=TERM", start)
        docker_command = script[start:stop]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docker = root / "docker"
            docker.write_text("""#!/bin/bash
set -eu
printf '%s\\n' "$@" > "$DOCKER_ARGUMENTS"
echo container-id
""")
            docker.chmod(0o755)
            arguments = root / "arguments"
            result = subprocess.run(
                ["bash", "-ec", "device_args=()\n" + docker_command],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "DOCKER_ARGUMENTS": str(arguments),
                    "GITHUB_RUN_ID": "123",
                    "GITHUB_RUN_ATTEMPT": "1",
                    "XPU_IMAGE": "image",
                    "ZE_AFFINITY_MASK": "4,5",
                    "CHECKOUT_SHA": "a" * 40,
                    "run_directory": str(root),
                    "source_directory": str(ROOT),
                    "control_directory": str(ROOT / "scripts"),
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            args = arguments.read_text().splitlines()
            self.assertEqual(args[args.index("--network") + 1], "none")
            for variable in (
                "MOONCAKE_LOCAL_HOSTNAME=127.0.0.1",
                "CCL_KVS_IFACE=lo",
                "GLOO_SOCKET_IFNAME=lo",
                "LOCAL_HOSTNAME=127.0.0.1",
                "MASTER_SERVER=127.0.0.1:50051",
                "ZE_AFFINITY_MASK=4,5",
            ):
                self.assertIn(variable, args)
                self.assertEqual(args[args.index(variable) - 1], "--env")
            self.assertNotIn("--privileged", args)
            self.assertIn(
                "python3 /control/e2e/python/verify_xpu_wheel.py run /bundle /source /output",
                "\n".join(args),
            )

    def test_fixed_runner_masks_need_no_lock_or_profile_variables(self):
        script = hardware_step("Execute isolated two-B60 core")
        start = script.index(': "${ZE_AFFINITY_MASK:')
        stop = script.index("# Preserve host GPU ordinals")
        for mask, valid in (
            ("0,1", True),
            ("2,3", True),
            ("4,5", True),
            ("6,7", True),
            ("0,0", False),
            ("00,0", False),
            ("0", False),
            ("0.0,0.1", False),
            ("", False),
        ):
            environment = {
                key: value
                for key, value in os.environ.items()
                if key
                not in {
                    "XPU_LOCK_DIRECTORY",
                    "XPU_SECCOMP_PROFILE",
                    "XPU_RENDER_DEVICES",
                }
            }
            environment.update(
                {
                    "ZE_AFFINITY_MASK": mask,
                    "XPU_IMAGE": "image@sha256:" + "a" * 64,
                    "CHECKOUT_SHA": "b" * 40,
                }
            )
            with self.subTest(mask=mask):
                result = subprocess.run(
                    [
                        "bash",
                        "-ec",
                        'fail() { echo "$*" >&2; exit 1; }\n' + script[start:stop],
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode == 0, valid, result.stderr)
        self.assertNotIn("XPU_LOCK_DIRECTORY", script)
        self.assertNotIn("XPU_SECCOMP_PROFILE", script)
        self.assertNotIn("XPU_RENDER_DEVICES", script)
        self.assertNotIn("flock", script)
        self.assertIn("devices=(/dev/dri/renderD*)", script)
        self.assertIn('--env "ZE_AFFINITY_MASK=$ZE_AFFINITY_MASK"', script)
        self.assertIn(
            '--security-opt "seccomp=$run_directory/output/seccomp.json"', script
        )

    def test_cleanup_recheck_propagates_docker_query_failure(self):
        for query_status in (0, 2):
            with self.subTest(
                query_status=query_status
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                run_directory = self.prepare_run(root)
                docker = root / "docker"
                docker.write_text("""#!/bin/bash
set -eu
case "$1" in
  ps) exit "$QUERY_STATUS" ;;
  info) exit 0 ;;
  *) exit 99 ;;
esac
""")
                docker.chmod(0o755)
                result = subprocess.run(
                    [
                        "bash",
                        "-ec",
                        hardware_step("Recheck current-run container cleanup"),
                    ],
                    env={
                        **os.environ,
                        "PATH": f"{root}:{os.environ['PATH']}",
                        "GITHUB_RUN_ID": "123",
                        "GITHUB_RUN_ATTEMPT": "1",
                        "QUERY_STATUS": str(query_status),
                        "RUN_DIRECTORY": str(run_directory),
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, query_status, result.stderr)
                self.assertEqual(
                    (run_directory / "output/cleanup-recheck.json").exists(),
                    query_status == 0,
                )

    def test_owned_container_cleanup_and_failures_propagate(self):
        script = hardware_step("Execute isolated two-B60 core")
        cleanup = (
            'source "$RUN_DIRECTORY/cleanup.sh"\n'
            + script[
                script.index("container_id=\n") : script.index(
                    'docker pull "$XPU_IMAGE"'
                )
            ]
        )
        for suite_status, cleanup_failure, expected in (
            (0, "no", 0),
            (2, "no", 2),
            (0, "yes", 1),
        ):
            with self.subTest(
                suite_status=suite_status, cleanup_failure=cleanup_failure
            ):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    run_directory = self.prepare_run(root)
                    (run_directory / "output").mkdir()
                    docker = root / "docker"
                    docker.write_text("""#!/bin/bash
set -eu
echo "$*" >> "$RUN_DIRECTORY/docker.commands"
case "$1" in
  info) echo daemon-ready ;;
  stop) echo owned-container ;;
  inspect)
    [ ! -e "$RUN_DIRECTORY/removed" ] || exit 1
    if [ "${2:-}" = --format ]; then echo false; else echo '{}'; fi ;;
  rm)
    [ "$CLEANUP_FAILURE" = no ] || exit 1
    touch "$RUN_DIRECTORY/removed" ;;
  *) exit 99 ;;
esac
""")
                    docker.chmod(0o755)
                    result = subprocess.run(
                        [
                            "bash",
                            "-ec",
                            f"{cleanup}\ncontainer_id=owned-container\nexit {suite_status}",
                        ],
                        env={
                            **os.environ,
                            "PATH": f"{root}:{os.environ['PATH']}",
                            "run_directory": str(run_directory),
                            "RUN_DIRECTORY": str(run_directory),
                            "CLEANUP_FAILURE": cleanup_failure,
                        },
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(
                        result.returncode, expected, result.stdout + result.stderr
                    )
                    commands = (run_directory / "docker.commands").read_text()
                    self.assertIn("stop --time 10 owned-container", commands)
                    self.assertIn("rm owned-container", commands)
                    self.assertNotIn("prune", commands)
                    report = json.loads(
                        (run_directory / "output/cleanup.json").read_text()
                    )
                    self.assertEqual(
                        report["cleanup_status"] == 0, cleanup_failure == "no"
                    )

    def test_cleanup_recheck_removes_only_current_run_container(self):
        for container_id, valid in (("a" * 64, True), ("invalid-id", False)):
            with self.subTest(
                container_id=container_id
            ), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                run_directory = self.prepare_run(root)
                docker = root / "docker"
                docker.write_text("""#!/bin/bash
set -eu
echo "$*" >> "$RUN_DIRECTORY/docker.commands"
case "$1" in
  ps) echo "$OWNED_CONTAINER" ;;
  info|stop) exit 0 ;;
  inspect)
    [ ! -e "$RUN_DIRECTORY/removed" ] || exit 1
    if [ "${2:-}" = --format ]; then echo false; else echo '{}'; fi ;;
  rm) touch "$RUN_DIRECTORY/removed" ;;
  *) exit 99 ;;
esac
""")
                docker.chmod(0o755)
                result = subprocess.run(
                    [
                        "bash",
                        "-ec",
                        hardware_step("Recheck current-run container cleanup"),
                    ],
                    env={
                        **os.environ,
                        "PATH": f"{root}:{os.environ['PATH']}",
                        "RUN_DIRECTORY": str(run_directory),
                        "OWNED_CONTAINER": container_id,
                        "GITHUB_RUN_ID": "123",
                        "GITHUB_RUN_ATTEMPT": "1",
                    },
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode == 0, valid, result.stderr)
                commands = (run_directory / "docker.commands").read_text()
                self.assertIn("--filter label=mooncake.xpu.run=123-1", commands)
                self.assertEqual((run_directory / "removed").exists(), valid)
                self.assertEqual(
                    (run_directory / "output/cleanup-recheck.json").exists(), valid
                )
                if valid:
                    self.assertIn(f"rm {container_id}", commands)
                else:
                    self.assertNotIn("stop ", commands)
                    self.assertNotIn("rm ", commands)
                self.assertNotIn("prune", commands)

    def test_host_preflight_rejects_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "output").mkdir()
            fake_id = root / "id"
            fake_id.write_text("#!/bin/sh\necho 0\n")
            fake_id.chmod(0o755)
            script = hardware_step("Execute isolated two-B60 core")
            result = subprocess.run(
                [
                    "bash",
                    "-ec",
                    script[script.index("exec >") :],
                ],
                env={
                    **os.environ,
                    "PATH": f"{root}:{os.environ['PATH']}",
                    "run_directory": str(root),
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must not run as root", result.stdout + result.stderr)


class SeccompTest(unittest.TestCase):
    def test_only_required_syscalls_are_unconditionally_allowed(self):
        original = {
            "defaultAction": "SCMP_ACT_ERRNO",
            "defaultErrnoRet": 1,
            "archMap": [{"architecture": "SCMP_ARCH_X86_64", "subArchitectures": []}],
            "syscalls": [
                {"names": ["read", "write", "pidfd_open"], "action": "SCMP_ACT_ALLOW"},
                {
                    "names": ["pidfd_getfd", "ptrace"],
                    "action": "SCMP_ACT_ALLOW",
                    "includes": {"caps": ["CAP_SYS_PTRACE"]},
                },
                {
                    "names": ["io_uring_setup"],
                    "action": "SCMP_ACT_ERRNO",
                    "errnoRet": 1,
                },
                {
                    "names": ["mount"],
                    "action": "SCMP_ACT_ALLOW",
                    "includes": {"caps": ["CAP_SYS_ADMIN"]},
                },
            ],
        }
        content = json.dumps(original).encode()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "seccomp.json"
            with (
                patch.object(VERIFY.urllib.request, "urlopen") as download,
                patch.object(
                    VERIFY, "SECCOMP_SHA256", hashlib.sha256(content).hexdigest()
                ),
            ):
                download.return_value.__enter__.return_value.read.return_value = content
                VERIFY.prepare_seccomp(output)
                download.assert_called_once_with(VERIFY.SECCOMP_URL, timeout=30)
            actual = json.loads(output.read_text())
            self.assertEqual(actual["defaultAction"], original["defaultAction"])
            self.assertEqual(actual["archMap"], original["archMap"])
            self.assertEqual(
                actual["syscalls"][-1],
                {
                    "names": sorted(VERIFY.XPU_SYSCALLS),
                    "action": "SCMP_ACT_ALLOW",
                },
            )
            remaining = []
            for rule in original["syscalls"]:
                names = [
                    name for name in rule["names"] if name not in VERIFY.XPU_SYSCALLS
                ]
                if names:
                    remaining.append({**rule, "names": names})
            self.assertEqual(actual["syscalls"][:-1], remaining)

    def test_download_checksum_and_default_action_failures_abort(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "seccomp.json"
            with patch.object(
                VERIFY.urllib.request, "urlopen", side_effect=OSError("download failed")
            ):
                with self.assertRaisesRegex(OSError, "download failed"):
                    VERIFY.prepare_seccomp(output)
            with patch.object(VERIFY.urllib.request, "urlopen") as download:
                download.return_value.__enter__.return_value.read.return_value = (
                    b"corrupted"
                )
                with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                    VERIFY.prepare_seccomp(output)
                content = b'{"defaultAction":"SCMP_ACT_ALLOW"}'
                download.return_value.__enter__.return_value.read.return_value = content
                with patch.object(
                    VERIFY, "SECCOMP_SHA256", hashlib.sha256(content).hexdigest()
                ):
                    with self.assertRaisesRegex(RuntimeError, "deny-by-default"):
                        VERIFY.prepare_seccomp(output)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
