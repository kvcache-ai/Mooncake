"""Run each loader scenario in a fresh process; no NVIDIA driver or GPU needed."""

import argparse
import os
import platform
import re
import subprocess
import tempfile
from pathlib import Path


def run_case(args, scenario, library_dir, work_dir):
    marker = work_dir / f"{scenario}.loaded"
    env = os.environ.copy()
    env["MOONCAKE_TEST_CUDA_LOAD_MARKER"] = str(marker)
    env["LD_LIBRARY_PATH"] = str(library_dir)
    env["DYLD_LIBRARY_PATH"] = str(library_dir)
    command = [args.probe, scenario]
    if scenario == "import":
        command.append(args.import_library)
    result = subprocess.run(command, env=env, text=True, capture_output=True)
    if result.returncode:
        raise AssertionError(f"{scenario}: {result.stdout}\n{result.stderr}")
    loads = marker.read_text().splitlines() if marker.exists() else []
    if scenario in ("complete", "missing"):
        assert loads == ["loaded"], (scenario, loads)
    else:
        assert not loads, (scenario, loads)
    if scenario == "unavailable":
        assert result.stderr.count("Mooncake: cannot load libcuda.so.1:") == 1
    if scenario == "missing":
        assert result.stderr.count("symbol cuMemGetAddressRange_v2:") == 1
    print(f"PASS: {scenario}")


def check_dependencies(binary):
    if platform.system() == "Darwin":
        symbols = subprocess.check_output(["nm", "-u", binary], text=True)
        dependencies = subprocess.check_output(["otool", "-L", binary], text=True)
    else:
        symbols = subprocess.check_output(
            ["nm", "-D", "--undefined-only", binary], text=True
        )
        dependencies = subprocess.check_output(["readelf", "-d", binary], text=True)
    assert not re.search(r"\b_?cu[A-Z]\w*(?:@\S+)?$", symbols, re.MULTILINE), symbols
    assert not re.search(r"libcuda(?:\.so|\.dylib)", dependencies), dependencies
    print(f"PASS: no direct CUDA symbols or driver dependency in {Path(binary).name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("probe", "import-library", "complete", "missing"):
        parser.add_argument(f"--{option}", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="mooncake-cuda-driver-") as directory:
        work_dir = Path(directory)
        unavailable = work_dir / "unavailable"
        unavailable.mkdir()
        # An invalid first candidate makes load failure deterministic even when
        # the test host has a real driver in its default library search path.
        (unavailable / "libcuda.so.1").write_text("not a shared library\n")
        run_case(args, "import", args.complete, work_dir)
        run_case(args, "unavailable", unavailable, work_dir)
        run_case(args, "missing", args.missing, work_dir)
        run_case(args, "complete", args.complete, work_dir)
    check_dependencies(args.probe)
    check_dependencies(args.import_library)


if __name__ == "__main__":
    main()
