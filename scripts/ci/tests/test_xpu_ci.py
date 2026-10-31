"""Host-only regression checks for XPU CI gates and packaging."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from importlib import metadata

import yaml


ROOT = Path(__file__).resolve().parents[3]


def load_script(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/ci_xpu.yml").read_text())


def workflow_step(name):
    return next(
        step
        for step in WORKFLOW["jobs"]["build-wheel-xpu"]["steps"]
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


if __name__ == "__main__":
    unittest.main()
