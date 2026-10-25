"""Regression checks for wheel-gate failures and client loader isolation."""

import importlib.util
import os
import shlex
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

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


class ConductorServerPreservationTest(unittest.TestCase):
    def test_server_survives_build_cleanup_and_is_removed_on_exit(self):
        script = (Path(SMOKE.__file__).parent / "build_wheel.sh").read_text()
        # Execute the actual staging/cleanup block, without building a GPU wheel.
        start = script.index("# Preserve the Conductor server")
        end = script.index("# Make the post-repair device library discoverable", start)
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
                        build_arg = str(build) if absolute else build.name
                        command = (
                            "set -e\n"
                            f"BUILD_DIR={shlex.quote(build_arg)}\n"
                            f"BUILD_DIR_ABS={shlex.quote(str(build))}\n"
                            'CUDA_EP_STAGING_DIR="$BUILD_DIR_ABS/ep_pg_staging"\n'
                            + script[start:end]
                            + '\ntest ! -d "$BUILD_DIR_ABS"\n'
                            + '"$CONDUCTOR_SMOKE_DIR/mooncake_conductor"\n'
                            + 'printf "%s" "$CONDUCTOR_SMOKE_DIR" > preserved-path\n'
                            + f"exit {exit_code}\n"
                        )
                        env = dict(os.environ, CI="false", FREE_BUILD_DIR="0")
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


if __name__ == "__main__":
    unittest.main()
