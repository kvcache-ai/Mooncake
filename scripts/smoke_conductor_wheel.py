#!/usr/bin/env python3
"""Check Conductor in an isolated wheel install, optionally against a server."""

import argparse
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--test-api", action="store_true", help="Run native API tests")
    parser.add_argument(
        "--server", type=Path, help="Also run E2E against this built Conductor server"
    )
    args = parser.parse_args()
    wheel = args.wheel.resolve()
    server = args.server.resolve() if args.server else None
    if server is not None and (not server.is_file() or not os.access(server, os.X_OK)):
        parser.error(f"Conductor server is missing or not executable: {server}")
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if "mooncake/conductor.py" not in names or not any(
            name.startswith("mooncake/_conductor.") and name.endswith(".so")
            for name in names
        ):
            raise SystemExit(f"{wheel.name}: missing Conductor wrapper or extension")
    env = os.environ.copy()
    for key in ("PYTHONPATH", "PYTHONHOME", "LD_LIBRARY_PATH", "LD_PRELOAD"):
        env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix="conductor-wheel-smoke-") as temporary:
        # Resolve venv symlinks: older Python versions otherwise write the
        # parent venv's bin directory as "home", breaking copied executables.
        interpreter = str(Path(sys.executable).resolve())
        subprocess.run(
            [interpreter, "-I", "-m", "venv", "--copies", temporary], check=True
        )
        python = str(Path(temporary) / "bin" / "python")
        probe = subprocess.run(
            [python, "-I", "-c", "pass"], env=env, capture_output=True, text=True
        )
        if probe.returncode != 0:
            if "libpython" not in probe.stderr or "cannot open" not in probe.stderr:
                raise RuntimeError(probe.stderr)
            # setup-python binaries in containers may rely on LD_LIBRARY_PATH
            # for libpython. Repair only the copied test interpreter, keeping
            # loader overrides absent when testing the installed wheel.
            subprocess.run(
                ["patchelf", "--add-rpath", str(Path(sys.base_prefix) / "lib"), python],
                check=True,
            )
            subprocess.run([python, "-I", "-c", "pass"], env=env, check=True)
        subprocess.run(
            [
                python,
                "-I",
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                str(wheel),
            ],
            env=env,
            check=True,
        )
        subprocess.run(
            [
                python,
                "-I",
                "-c",
                "import sys; from pathlib import Path; "
                "import mooncake.conductor, mooncake._conductor; "
                "assert all(Path(m.__file__).resolve().is_relative_to(sys.prefix) "
                "for m in (mooncake.conductor, mooncake._conductor)); "
                "client = mooncake.conductor.ConductorClient(); "
                "assert client.health_check() == mooncake.conductor.HEALTH_NOT_INITIALIZED; "
                "assert client.close() == mooncake.conductor.OK; "
                "print(mooncake.conductor.__file__); print(mooncake._conductor.__file__)",
            ],
            cwd=temporary,
            env=env,
            check=True,
        )
        if args.test_api or server is not None:
            dependencies = ["pytest>=7,<9", "pytest-timeout>=2,<3"]
            tests_dir = (
                Path(__file__).resolve().parents[1] / "mooncake-conductor/tests/python"
            )
            tests = [str(tests_dir / "test_client_validation.py")]
            if server is not None:
                dependencies += ["msgpack>=1,<2", "pyzmq>=25,<28"]
                tests.append(str(tests_dir / "test_client_e2e.py"))
                env["CONDUCTOR_BINARY"] = str(server)
                # Only the server subprocess may use build-time library paths.
                # The client must resolve every dependency from its wheel.
                env["CONDUCTOR_SERVER_LD_LIBRARY_PATH"] = os.environ.get(
                    "LD_LIBRARY_PATH", ""
                )
            subprocess.run(
                [python, "-I", "-m", "pip", "install", *dependencies],
                env=env,
                check=True,
            )
            env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
            env.pop("PYTEST_ADDOPTS", None)
            env.pop("PYTEST_PLUGINS", None)
            subprocess.run(
                [
                    python,
                    "-I",
                    "-m",
                    "pytest",
                    "-p",
                    "pytest_timeout",
                    "--timeout=60",
                    "--import-mode=importlib",
                    f"--confcutdir={tests_dir}",
                    "-q",
                    *tests,
                ],
                cwd=temporary,
                env=env,
                check=True,
            )


if __name__ == "__main__":
    main()
