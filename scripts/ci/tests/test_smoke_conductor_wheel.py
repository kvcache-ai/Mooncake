"""Regression checks for wheel-gate failures and client loader isolation."""

import importlib.util
import os
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


if __name__ == "__main__":
    unittest.main()
