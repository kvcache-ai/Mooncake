"""Regression checks for wheel-gate failures and client loader isolation."""

import importlib.util
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import yaml

SPEC = importlib.util.spec_from_file_location(
    "smoke_conductor_wheel",
    Path(__file__).resolve().parents[3] / "scripts/smoke_conductor_wheel.py",
)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class ConductorWheelGateTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.wheel = self.root / "conductor.whl"
        self.server = self.root / "mooncake_conductor"
        self.server.write_text("#!/bin/sh\nexit 0\n")
        self.server.chmod(0o755)
        self.write_wheel()

    def write_wheel(self, omit=None):
        with zipfile.ZipFile(self.wheel, "w") as archive:
            for name in ("mooncake/conductor.py", "mooncake/_conductor.cp312.so"):
                if name != omit:
                    archive.writestr(name, b"")

    def invoke(self, *args):
        with patch.object(sys, "argv", ["smoke", str(self.wheel), *map(str, args)]):
            SMOKE.main()

    def test_missing_package_members_fail_before_install(self):
        for member in ("mooncake/conductor.py", "mooncake/_conductor.cp312.so"):
            with self.subTest(member=member):
                self.write_wheel(omit=member)
                with patch.object(SMOKE.subprocess, "run") as run:
                    with self.assertRaisesRegex(SystemExit, "missing Conductor"):
                        self.invoke()
                    run.assert_not_called()

    def test_missing_server_is_fatal(self):
        self.server.unlink()
        with patch.object(SMOKE.subprocess, "run") as run:
            with self.assertRaises(SystemExit):
                self.invoke("--server", self.server)
            run.assert_not_called()

    def test_e2e_keeps_build_libraries_out_of_client(self):
        overrides = {
            "PYTHONPATH": "/build/python",
            "PYTHONHOME": "/build/python-home",
            "LD_LIBRARY_PATH": "/build/lib",
            "LD_PRELOAD": "/build/libasio.so",
        }
        completed = subprocess.CompletedProcess([], 0)
        with patch.dict(os.environ, overrides):
            with patch.object(SMOKE.subprocess, "run", return_value=completed) as run:
                self.invoke("--server", self.server)
        calls = run.call_args_list
        for call in calls:
            command = call.args[0]
            self.assertIn("-I", command)
            if "env" in call.kwargs:
                self.assertTrue(overrides.keys().isdisjoint(call.kwargs["env"]))
        test_call = next(call for call in calls if "pytest" in call.args[0])
        self.assertEqual(test_call.kwargs["env"]["CONDUCTOR_BINARY"], str(self.server))
        self.assertEqual(
            test_call.kwargs["env"]["CONDUCTOR_SERVER_LD_LIBRARY_PATH"], "/build/lib"
        )
        for name in ("test_client_validation.py", "test_client_e2e.py"):
            self.assertTrue(any(arg.endswith(name) for arg in test_call.args[0]))

    def test_import_or_test_failure_fails_gate(self):
        for failure in ("import", "pytest"):
            with self.subTest(failure=failure):

                def run(command, **kwargs):
                    is_import = any("import mooncake.conductor" in s for s in command)
                    if (failure == "import" and is_import) or (
                        failure == "pytest" and "pytest" in command
                    ):
                        self.assertTrue(kwargs["check"])
                        raise subprocess.CalledProcessError(1, command)
                    return subprocess.CompletedProcess(command, 0)

                with patch.object(SMOKE.subprocess, "run", side_effect=run):
                    with self.assertRaises(subprocess.CalledProcessError):
                        self.invoke("--test-api")

    def test_preserved_server_libraries_are_only_passed_to_server(self):
        completed = subprocess.CompletedProcess([], 0)
        with patch.dict(
            os.environ,
            {
                "LD_LIBRARY_PATH": "/build/lib",
                "CONDUCTOR_SERVER_LD_LIBRARY_PATH": "/preserved/server/lib",
            },
        ):
            with patch.object(SMOKE.subprocess, "run", return_value=completed) as run:
                self.invoke("--server", self.server)
        test_call = next(
            call for call in run.call_args_list if "pytest" in call.args[0]
        )
        self.assertNotIn("LD_LIBRARY_PATH", test_call.kwargs["env"])
        self.assertEqual(
            test_call.kwargs["env"]["CONDUCTOR_SERVER_LD_LIBRARY_PATH"],
            "/preserved/server/lib",
        )

    def test_declared_external_runtime_reaches_client(self):
        runtime = "/opt/intel/oneapi/compiler/lib"
        seen = []

        def run(command, **kwargs):
            if "env" in kwargs:
                seen.append((command, dict(kwargs["env"])))
            return subprocess.CompletedProcess(command, 0)

        with patch.dict(
            os.environ,
            {
                "LD_LIBRARY_PATH": "/build/lib",
                "CONDUCTOR_CLIENT_LD_LIBRARY_PATH": runtime,
            },
        ):
            with patch.object(SMOKE.subprocess, "run", side_effect=run):
                self.invoke("--server", self.server)
        client_calls = [
            env
            for command, env in seen
            if "pip" in command
            or "pytest" in command
            or any("import mooncake.conductor" in arg for arg in command)
        ]
        self.assertTrue(client_calls)
        for env in client_calls:
            self.assertEqual(env["LD_LIBRARY_PATH"], runtime)


class IntelRuntimeLibraryPathTest(unittest.TestCase):
    """XPU wheels exclude the Intel runtime, so the smoke client must find it."""

    def resolve(self, ldd_output):
        script_path = Path(SMOKE.__file__).parent / "build_wheel.sh"
        with tempfile.TemporaryDirectory() as temporary:
            binary_dir = Path(temporary)
            ldd = binary_dir / "ldd"
            ldd.write_text(f"#!/bin/sh\ncat <<'EOF'\n{ldd_output}EOF\n")
            ldd.chmod(0o755)
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    f"source {shlex.quote(str(script_path))}\n"
                    "intel_runtime_library_path /build/_conductor.so",
                ],
                env=dict(os.environ, PATH=f"{binary_dir}:{os.environ['PATH']}"),
                capture_output=True,
                text=True,
                check=True,
            )
        return result.stdout.strip()

    def test_only_intel_runtime_directories_are_exposed(self):
        oneapi = "/opt/intel/oneapi/compiler/2026.1/lib"
        output = (
            "\tlinux-vdso.so.1 (0x0000)\n"
            "\tlibasio.so => /build/mooncake-common/libasio.so (0x0000)\n"
            f"\tlibsycl.so.9 => {oneapi}/libsycl.so.9 (0x0000)\n"
            f"\tlibsvml.so => {oneapi}/libsvml.so (0x0000)\n"
            f"\tlibimf.so => {oneapi}/libimf.so (0x0000)\n"
            "\tlibirng.so => /opt/intel/oneapi/umf/lib/libirng.so (0x0000)\n"
            f"\tlibintlc.so.5 => {oneapi}/libintlc.so.5 (0x0000)\n"
            "\tlibze_loader.so.1 => /lib/x86_64-linux-gnu/libze_loader.so.1 (0x0000)\n"
            "\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x0000)\n"
        )
        self.assertEqual(self.resolve(output), f"{oneapi}:/opt/intel/oneapi/umf/lib")

    def test_unresolved_or_unrelated_libraries_expose_nothing(self):
        output = (
            "\tlibasio.so => /build/mooncake-common/libasio.so (0x0000)\n"
            "\tlibsvml.so => not found\n"
            "\tlibc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x0000)\n"
        )
        self.assertEqual(self.resolve(output), "")


class ConductorServerPreservationTest(unittest.TestCase):
    def test_server_survives_build_cleanup_and_is_removed_on_exit(self):
        script_path = Path(SMOKE.__file__).parent / "build_wheel.sh"
        script = script_path.read_text()
        # Execute the actual staging/cleanup block, without building a GPU wheel.
        start = script.index("# Preserve the Conductor server")
        end = script.index("# Release builds still compile with -g", start)
        for cleanup_flag in ("CI", "FREE_BUILD_DIR"):
            for absolute in (False, True):
                for exit_code in (0, 23):
                    with self.subTest(
                        cleanup_flag=cleanup_flag,
                        absolute=absolute,
                        exit_code=exit_code,
                    ), tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary).resolve()
                        build = root / "build with spaces"
                        server = build / "mooncake-conductor/mooncake_conductor"
                        server.parent.mkdir(parents=True)
                        server.write_text(
                            '#!/bin/sh\ntest -f "$(dirname "$0")/libasio.so"\n'
                        )
                        server.chmod(0o755)
                        library = build / "mooncake-common/libasio.so"
                        library.parent.mkdir()
                        library.write_bytes(b"server runtime dependency")
                        staging = build / "ep_pg_staging"
                        staging.mkdir()
                        (staging / "libmooncake_ep_device.so").write_bytes(b"EP")
                        (staging / "libmooncake_pg_device.so").write_bytes(b"PG")
                        (build / "mooncake_ep_jit").mkdir()
                        build_arg = str(build) if absolute else build.name
                        command = (
                            "set -e\n"
                            f"source {shlex.quote(str(script_path))}\n"
                            f"BUILD_DIR={shlex.quote(build_arg)}\n"
                            f"BUILD_DIR_ABS={shlex.quote(str(build))}\n"
                            'DEVICE_STAGING_DIR="$BUILD_DIR_ABS/ep_pg_staging"\n'
                            "DEVICE_LIBRARY_BASENAMES=(libmooncake_ep_device.so)\n"
                            "WITH_EP_BUILD=1\nUSE_XPU_BUILD=0\n"
                            + script[start:end]
                            + '\ntest ! -d "$BUILD_DIR_ABS"\n'
                            + '"$CONDUCTOR_SMOKE_DIR/mooncake_conductor"\n'
                            + 'test -f "$DEVICE_STAGING_TEMP/libmooncake_ep_device.so"\n'
                            + 'test ! -e "$DEVICE_STAGING_TEMP/libmooncake_pg_device.so"\n'
                            + 'test -d "$CUDA_EP_JIT_DIR"\n'
                            + 'rm -rf "$DEVICE_STAGING_TEMP"\n'
                            + 'printf "%s" "$CONDUCTOR_SMOKE_DIR" > preserved-path\n'
                            + f"exit {exit_code}\n"
                        )
                        env = dict(os.environ, CI="false")
                        env.pop("FREE_BUILD_DIR", None)
                        env[cleanup_flag] = "true" if cleanup_flag == "CI" else "1"
                        result = subprocess.run(
                            ["bash", "-c", command],
                            cwd=root,
                            env=env,
                            capture_output=True,
                            text=True,
                        )
                        self.assertEqual(result.returncode, exit_code, result.stderr)
                        preserved = Path((root / "preserved-path").read_text())
                        self.assertFalse(preserved.exists())


class WheelBuildCallerTest(unittest.TestCase):
    """build_wheel.sh requires _conductor, so every direct caller must build it."""

    ROOT = Path(SMOKE.__file__).resolve().parents[1]

    @staticmethod
    def code(text):
        # A flag mentioned only in a comment must not satisfy the check.
        return "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )

    def callers(self):
        found = {}
        # Workflows are checked per job: the configure step must share a job
        # with the build_wheel.sh step. Reusable-workflow callers are covered
        # by the reusable workflow's own job.
        for path in sorted(self.ROOT.glob(".github/workflows/*.y*ml")):
            jobs = yaml.safe_load(path.read_text()).get("jobs") or {}
            for name, job in jobs.items():
                steps = job.get("steps") or []
                script = self.code("\n".join(step.get("run", "") for step in steps))
                if "build_wheel.sh" in script:
                    found[f"{path.relative_to(self.ROOT)}:{name}"] = script
        for pattern in ("docker/*Dockerfile*", "*.sh", "scripts/**/*.sh"):
            for path in sorted(self.ROOT.glob(pattern)):
                script = self.code(path.read_text())
                if path.name != "build_wheel.sh" and "build_wheel.sh" in script:
                    found[str(path.relative_to(self.ROOT))] = script
        return found

    def test_every_wheel_build_caller_enables_conductor(self):
        callers = self.callers()
        # Entry points that PR CI does not build; guard the discovery itself.
        for known in (
            "docker/mooncake.Dockerfile",
            "docker/musa.Dockerfile",
            ".github/workflows/pre-release.yaml:build-npu",
            ".github/workflows/_build-wheel.yaml:build",
        ):
            self.assertIn(known, callers)
        for caller, script in callers.items():
            with self.subTest(caller=caller):
                self.assertIn("-DWITH_CONDUCTOR=ON", script)


class StoreRsWheelConductorTest(unittest.TestCase):
    def test_wrapper_builds_conductor_before_packaging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scripts = root / "scripts"
            scripts.mkdir()
            shutil.copy2(
                Path(SMOKE.__file__).parent / "build_store_rs_wheel.sh", scripts
            )
            build = root / "build"
            build.mkdir()
            cache = build / "CMakeCache.txt"
            options = (
                "WITH_STORE_RS:BOOL=ON\n" "WITH_STORE:BOOL=ON\n" "USE_REDIS:BOOL=ON\n"
            )
            binary_dir = root / "bin"
            binary_dir.mkdir()
            cmake = binary_dir / "cmake"
            cmake.write_text(
                "#!/bin/bash\n"
                'if [ "$1" = --build ]; then\n'
                '  mkdir -p "$2/mooncake-transfer-engine/src"\n'
                '  touch "$2/mooncake-transfer-engine/src/libtransfer_engine.so"\n'
                '  for target in "$@"; do\n'
                '    case "$target" in\n'
                '      _conductor|mooncake_conductor) touch "$2/$target" ;;\n'
                "    esac\n"
                "  done\n"
                "fi\n"
            )
            cmake.chmod(0o755)
            packager = scripts / "build_wheel.sh"
            packager.write_text(
                "#!/bin/bash\n"
                'test -f "$BUILD_DIR/_conductor" && '
                'test -f "$BUILD_DIR/mooncake_conductor"\n'
            )
            packager.chmod(0o755)
            env = {
                **os.environ,
                "PATH": f"{binary_dir}:{os.environ['PATH']}",
                "BUILD_DIR": "build",
                "BUILD_JOBS": "1",
                "PYTHON_VERSION": "3.12",
                "STORE_RS_PYTHON_PREFIX": str(root / "prefix"),
            }
            for enabled in (False, True):
                with self.subTest(enabled=enabled):
                    cache.write_text(
                        options + f"WITH_CONDUCTOR:BOOL={'ON' if enabled else 'OFF'}\n"
                    )
                    result = subprocess.run(
                        ["bash", str(scripts / "build_store_rs_wheel.sh")],
                        cwd=root,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if enabled:
                        self.assertEqual(result.returncode, 0, result.stderr)
                    else:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("-DWITH_CONDUCTOR=ON", result.stderr)
                        self.assertFalse((build / "mooncake-transfer-engine").exists())


if __name__ == "__main__":
    unittest.main()
