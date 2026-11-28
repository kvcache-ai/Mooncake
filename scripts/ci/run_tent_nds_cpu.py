#!/usr/bin/env python3
"""Build, test and install TENT with NDS ON/OFF without NPU hardware."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "mooncake-transfer-engine/tent/tests"
HEADER_SHA256 = "c2d7e7b5f255fca3d654e3c725831be15e0db3cce7d75cc650c5df75fc85414c"
TARGETS = [
    "tent_shared",
    "tentpy",
    "tent_cli",
    "tent_diagnostic_test",
    "tent_transport_selector_test",
    "thread_local_storage_test",
    "segment_manager_test",
    "config_lifecycle_test",
    "transfer_engine_config_override_test",
    "tent_metrics_recording_test",
]


def run(args, **kwargs):
    print("+", " ".join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), check=True, **kwargs)


def check_default_config():
    """Keep the shipped configuration capable of selecting NDS."""
    config_path = ROOT / "mooncake-transfer-engine/tent/config/transfer-engine.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    expected = {"enable": False}
    if config.get("transports", {}).get("nds") != expected:
        raise RuntimeError("Default NDS transport configuration is missing or stale")
    file_policy = next(
        (
            policy
            for policy in config.get("policy", [])
            if policy.get("name") == "file_storage"
        ),
        None,
    )
    if file_policy is None or "nds" not in file_policy.get("transports", []):
        raise RuntimeError("The default file_storage policy must authorize NDS")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("on", "off"), required=True)
    parser.add_argument("--build-root", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--cmake-arg", action="append", default=[])
    args = parser.parse_args()
    check_default_config()
    work = args.build_root.resolve()
    work.mkdir(parents=True, exist_ok=True)
    build, prefix = work / "build", work / "install"
    sdk = work / "mock-sdk"
    flags = []
    if args.mode == "on":
        header = TESTS / "thirdparty/nds/ascend_nds.h"
        header_bytes = header.read_bytes().replace(b"\r\n", b"\n")
        if hashlib.sha256(header_bytes).hexdigest() != HEADER_SHA256:
            raise RuntimeError("The third-party NDS header fixture changed")
        (sdk / "include").mkdir(parents=True, exist_ok=True)
        (sdk / "lib").mkdir(exist_ok=True)
        shutil.copyfile(header, sdk / "include/ascend_nds.h")
        run(
            [
                os.environ.get("CXX", "c++"),
                "-shared",
                "-fPIC",
                "-std=c++17",
                "-Wl,-soname,libascend_nds.so",
                "-I",
                sdk / "include",
                TESTS / "nds_test_backend.cpp",
                "-o",
                sdk / "lib/libascend_nds.so",
            ]
        )
        flags = [f"-DNDS_ROOT={sdk}"]
    run(
        [
            "cmake",
            "-S",
            ROOT,
            "-B",
            build,
            "-G",
            "Ninja",
            "-DCMAKE_BUILD_TYPE=Release",
            "-DENABLE_DEBUG_SYMBOLS=OFF",
            f"-DCMAKE_INSTALL_PREFIX={prefix}",
            "-DUSE_TENT=ON",
            f"-DUSE_NDS={args.mode.upper()}",
            "-DUSE_CUDA=OFF",
            "-DUSE_ASCEND=OFF",
            "-DUSE_ASCEND_DIRECT=OFF",
            "-DWITH_STORE=OFF",
            "-DWITH_STORE_RUST=OFF",
            "-DBUILD_UNIT_TESTS=ON",
            "-DBUILD_EXAMPLES=OFF",
            "-DBUILD_BENCHMARK=OFF",
            "-DTENT_METRICS_ENABLED=ON",
            "-DYLT_ENABLE_IBV=OFF",
            *flags,
            *args.cmake_arg,
        ]
    )
    targets = TARGETS + (["tent_nds_transport_test"] if args.mode == "on" else [])
    run(["cmake", "--build", build, "--parallel", args.jobs, "--target", *targets])
    names = [t for t in targets if t.endswith("_test")]
    names += ["tent_python_guards_test", "tent_cli_test"]
    run(
        [
            "ctest",
            "--test-dir",
            build,
            "--output-on-failure",
            "--no-tests=error",
            "--timeout",
            "180",
            "--output-junit",
            work / "ctest.xml",
            "-R",
            "^(" + "|".join(names) + ")$",
        ]
    )
    # Install the TENT subtree and its common shared-library dependencies.
    run(["cmake", "--install", build / "mooncake-common/src"])
    run(["cmake", "--install", build / "mooncake-transfer-engine/tent"])
    if list(prefix.rglob("libascend_nds*")) or list(prefix.rglob("ascend_nds.h")):
        raise RuntimeError("The test-only NDS SDK must not be installed with TENT")
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = str(prefix / "lib")
    if args.mode == "on":
        env["LD_LIBRARY_PATH"] += ":" + str(sdk / "lib")
    env["PYTHONPATH"] = str(build / "mooncake-transfer-engine/tent/src/python")
    # An installed consumer must resolve every symbol without build-tree libs.
    # Per the documented usage, C/C++ consumers take the public headers from
    # the source tree and link against the installed libraries.
    run(
        [
            os.environ.get("CXX", "c++"),
            "-std=c++20",
            "-I",
            ROOT / "mooncake-transfer-engine/tent/include",
            TESTS / "nds_installed_consumer.cpp",
            "-L",
            prefix / "lib",
            "-ltent_shared",
            "-o",
            work / "installed-consumer",
            f"-Wl,-rpath-link,{prefix / 'lib'}",
            *([f"-Wl,-rpath-link,{sdk / 'lib'}"] if args.mode == "on" else []),
        ],
        env=env,
    )
    run([work / "installed-consumer", args.mode], env=env)
    linked = subprocess.check_output(
        ["ldd", str(work / "installed-consumer")], env=env, text=True
    )
    (work / "installed-libraries.txt").write_text(linked)
    if "not found" in linked or str(build) in linked:
        raise RuntimeError("Installed consumer depends on missing/build-tree libraries")
    run([sys.executable, TESTS / "test_nds_bindings.py", args.mode], env=env)
    # Missing production SDK must fail at configure time with an actionable error.
    missing = subprocess.run(
        [
            "cmake",
            "-S",
            ROOT,
            "-B",
            work / "missing-sdk",
            "-G",
            "Ninja",
            "-DUSE_TENT=ON",
            "-DUSE_NDS=ON",
            "-DWITH_STORE=OFF",
            "-DBUILD_UNIT_TESTS=OFF",
            "-DNDS_INCLUDE_DIR=NDS_INCLUDE_DIR-NOTFOUND",
            "-DNDS_LIBRARY=NDS_LIBRARY-NOTFOUND",
            "-DYLT_ENABLE_IBV=OFF",
            *args.cmake_arg,
        ],
        capture_output=True,
        text=True,
    )
    (work / "missing-sdk.log").write_text(missing.stdout + missing.stderr)
    if (
        missing.returncode == 0
        or "USE_NDS requires ascend_nds.h" not in missing.stdout + missing.stderr
    ):
        raise RuntimeError("Missing-SDK configure check did not fail as expected")
    (work / "result.json").write_text(
        json.dumps(
            {
                "mode": args.mode,
                "result": "PASS",
                "hardware": False,
                "tests": names,
                "installed_consumer": "PASS",
                "python_binding": "PASS",
                "missing_sdk_rejected": True,
                "mock_sdk_installed": False,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
