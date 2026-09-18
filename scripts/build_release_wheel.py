"""Release metadata/configuration adapter; scikit-build-core owns wheel assembly.

CI may supply an already configured CMake tree. Preserve its profile rather than
letting the source-install defaults (notably USE_CUDA=OFF) override it. No native
artifact paths or Python package file lists belong here. Run this script with the
Python interpreter targeted by the wheel; it prepares build tools and repairs the
result. BUILD_DIR and OUTPUT_DIR retain the release workflows' legacy defaults.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import sysconfig
import time


ROOT = Path(__file__).resolve().parents[1]
VARIANTS = {
    "NON_CUDA_BUILD": ("non-cuda", {"USE_CUDA": "OFF", "WITH_EP": "OFF"}),
    "CU13_BUILD": ("cuda13", {"USE_CUDA": "ON"}),
    "NPU_BUILD": (
        "npu",
        {"USE_CUDA": "OFF", "USE_ASCEND_DIRECT": "ON", "WITH_EP": "OFF"},
    ),
    "EFA_BUILD": ("efa", {"USE_CUDA": "ON", "USE_EFA": "ON"}),
    "EFA_CU13_BUILD": ("efa-cuda13", {"USE_CUDA": "ON", "USE_EFA": "ON"}),
    "EFA_NON_CUDA_BUILD": (
        "efa-non-cuda",
        {"USE_CUDA": "OFF", "WITH_EP": "OFF", "USE_EFA": "ON"},
    ),
    "MUSA_BUILD": ("musa", {"USE_CUDA": "OFF", "USE_MUSA": "ON", "WITH_EP": "OFF"}),
    "HIP_BUILD": ("rocm", {"USE_CUDA": "OFF", "USE_HIP": "ON", "WITH_EP": "OFF"}),
}


def release_metadata(text: str, environ: dict[str, str]) -> tuple[str, dict[str, str]]:
    selected = [flag for flag in VARIANTS if environ.get(flag) == "1"]
    if len(selected) > 1:
        raise ValueError("Only one release variant may be selected")
    # Without a naming variant, retain the configured hardware profile (including
    # MUSA/Ascend callers that intentionally use the standard distribution name).
    suffix, defines = VARIANTS[selected[0]] if selected else ("", {})
    if suffix:
        text = re.sub(
            r'^name = "mooncake-transfer-engine"$',
            f'name = "mooncake-transfer-engine-{suffix}"',
            text,
            flags=re.MULTILINE,
        )
    if suffix in {"npu", "musa"}:
        text = text.replace('requires-python = ">=3.10"', 'requires-python = ">=3.9"')
        text = text.replace(
            '    "requests",',
            '    "requests",\n    "typing_extensions>=4.0; python_version < \'3.10\'",',
        )
        text = text.replace(
            '    "Programming Language :: Python :: 3.10",',
            '    "Programming Language :: Python :: 3.9",\n'
            '    "Programming Language :: Python :: 3.10",',
        )
    if suffix in {"non-cuda", "efa-non-cuda", "npu", "musa", "rocm"}:
        text = text.replace('    "Environment :: GPU :: NVIDIA CUDA",\n', "")
    # VERSION in older workflows can be a branch name. Only an explicit override
    # changes metadata; tag builds keep the checked-in release version.
    if version := environ.get("MOONCAKE_WHEEL_VERSION"):
        text = re.sub(
            r"^version = .*$",
            f"version = {json.dumps(version)}",
            text,
            flags=re.MULTILINE,
        )
    return text, defines


def build_settings(
    project: dict, build_dir: Path, variant_defines: dict[str, str]
) -> list[str]:
    settings = [f"--config-setting=build-dir={build_dir}"]
    cache = {}
    cache_path = build_dir / "CMakeCache.txt"
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            match = re.match(r"([^:#/][^:]*):[^=]+=(.*)", line)
            if match:
                cache[match[1]] = match[2]
    # Preserve explicit CI configuration when adopting a native build tree.
    defaults = project["tool"]["scikit-build"]["cmake"]["define"]
    defines = {key: cache[key] for key in defaults if key in cache}
    defines.update(variant_defines)
    defines.update(
        Python3_EXECUTABLE=sys.executable,
    )
    for key, value in defines.items():
        settings.append(f"--config-setting=cmake.define.{key}={value}")
    if "CMAKE_GENERATOR" in cache:
        settings.append(f"--config-setting=cmake.args=-G{cache['CMAKE_GENERATOR']}")
    if cache.get("CMAKE_BUILD_TYPE"):
        settings.append(
            f"--config-setting=cmake.build-type={cache['CMAKE_BUILD_TYPE']}"
        )
    return settings


@contextmanager
def metadata_override(path: Path, text: str):
    original = path.read_bytes()
    try:
        path.write_text(text)
        yield
    finally:
        path.write_bytes(original)


def prepare_output_dir(output_dir: Path, *, overwrite: bool = False) -> None:
    wheels = list(output_dir.glob("*.whl"))
    if wheels and not overwrite:
        raise ValueError(
            f"Output directory already contains wheels: {output_dir}. "
            "Use --overwrite to remove existing wheels, or choose --output-dir."
        )
    for wheel in wheels:
        wheel.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--build-dir",
        type=Path,
        default=os.environ.get("BUILD_DIR", "build"),
        help="CMake tree, relative to the repository root (default: BUILD_DIR or build)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "mooncake-wheel" / os.environ.get("OUTPUT_DIR", "dist"),
        help="Output path, relative to the repository root (default: mooncake-wheel/$OUTPUT_DIR or mooncake-wheel/dist)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove existing *.whl files in the output directory before building",
    )
    parser.add_argument(
        "--build-attempts",
        type=int,
        default=1,
        help="Backend build attempts for transient dependency download failures (default: 1)",
    )
    args = parser.parse_args()
    if args.build_attempts < 1:
        parser.error("--build-attempts must be positive")
    args.build_dir = (ROOT / args.build_dir).resolve()
    args.output_dir = (ROOT / args.output_dir).resolve()
    metadata_path = ROOT / "pyproject.toml"
    text, defines = release_metadata(metadata_path.read_text(), os.environ)
    env = os.environ.copy()
    env.setdefault("CMAKE_BUILD_PARALLEL_LEVEL", str(len(os.sched_getaffinity(0))))
    env["PATH"] = sysconfig.get_path("scripts") + os.pathsep + env.get("PATH", "")
    # Pass CMAKE_ARGS/CMAKE_GENERATOR through to scikit-build-core directly.
    # Its shell-aware parser preserves quoted paths and semicolon-valued options;
    # CI no longer needs a separately configured native tree.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prepare_output_dir(args.output_dir, overwrite=args.overwrite)
    # Bootstrap TOML support here, not at module import time, so Python 3.9/3.10
    # release images can invoke this entry point without preinstalled build tools.
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "build",
            # Older wheel unpack drops executable permissions (Ubuntu ships 0.37.1).
            "wheel>=0.45.1",
            "auditwheel",
            "patchelf",
            'tomli; python_version < "3.11"',
        ],
        check=True,
        env=env,
        cwd=ROOT,
    )
    try:
        import tomllib
    except ModuleNotFoundError:  # Python 3.9/3.10 release images
        import tomli as tomllib

    project = tomllib.loads(text)
    settings = build_settings(project, args.build_dir, defines)
    # Keep dependency declarations authoritative in the root pyproject, too.
    subprocess.run(
        [sys.executable, "-m", "pip", "install", *project["build-system"]["requires"]],
        check=True,
        env=env,
    )
    with metadata_override(metadata_path, text):
        build_command = [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(args.output_dir),
            *settings,
            str(ROOT),
        ]
        for attempt in range(1, args.build_attempts + 1):
            try:
                subprocess.run(build_command, check=True, env=env, cwd=ROOT)
                break
            except subprocess.CalledProcessError:
                if attempt == args.build_attempts:
                    raise
                print(
                    f"Backend build attempt {attempt} failed; retrying in 15s...",
                    flush=True,
                )
                time.sleep(15)
    subprocess.run(
        [
            "bash",
            str(ROOT / "scripts/repair_wheel.sh"),
            sys.executable,
            str(args.output_dir),
            str(args.build_dir),
        ],
        check=True,
        env=env,
        cwd=ROOT,
    )
    print(f"Wheel package built and repaired successfully: {args.output_dir}")


if __name__ == "__main__":
    main()
