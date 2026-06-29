#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import glob
import hashlib
import pathlib
import shutil
import tempfile
import zipfile


PYTHON_ASSETS = {
    "http_metadata_server.py",
    "mooncake_config.py",
    "mooncake_connector_v1.py",
    "mooncake_ep_buffer.py",
    "mooncake_store_service.py",
    "transfer_engine_topology_dump.py",
    "vllm_v1_proxy_server.py",
    "ep.py",
    "pg.py",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replace the packaged mooncake.engine runtime assets in a Store-RS "
            "wheel with assets extracted from an upstream Mooncake release wheel."
        )
    )
    parser.add_argument(
        "--target-wheel",
        help="Store-RS runtime mooncake wheel to rewrite.",
    )
    parser.add_argument(
        "--target-wheel-glob",
        default="dist/wheels/mooncake-*.whl",
        help="Glob used when --target-wheel is omitted.",
    )
    parser.add_argument(
        "--release-wheel",
        required=True,
        help="Downloaded upstream Mooncake release wheel.",
    )
    return parser.parse_args()


def pick_target_wheel(pattern: str) -> pathlib.Path:
    candidates = [
        pathlib.Path(path)
        for path in glob.glob(pattern)
        if pathlib.Path(path).name.startswith("mooncake-")
    ]
    if not candidates:
        raise FileNotFoundError(f"no Store-RS runtime wheel matched {pattern!r}")
    candidates.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)
    return candidates[0]


def extract_wheel(wheel_path: pathlib.Path, destination: pathlib.Path) -> None:
    with zipfile.ZipFile(wheel_path) as wheel:
        wheel.extractall(destination)


def copy_release_assets(
    release_root: pathlib.Path, target_root: pathlib.Path
) -> list[str]:
    release_package = release_root / "mooncake"
    target_package = target_root / "mooncake"
    if not release_package.is_dir():
        raise FileNotFoundError(f"{release_package} not found in release wheel")
    if not target_package.is_dir():
        raise FileNotFoundError(f"{target_package} not found in target wheel")

    copied: list[str] = []

    for source in sorted(release_package.glob("engine*.so")):
        target = target_package / source.name
        shutil.copy2(source, target)
        copied.append(target.relative_to(target_root).as_posix())

    if not any((target_package / name).exists() for name in ("engine.so",)):
        raise FileNotFoundError("release wheel did not provide mooncake/engine.so")

    for source in sorted(release_package.glob("lib*.so*")):
        target = target_package / source.name
        shutil.copy2(source, target)
        copied.append(target.relative_to(target_root).as_posix())

    for name in sorted(PYTHON_ASSETS):
        source = release_package / name
        if not source.exists():
            continue
        target = target_package / name
        shutil.copy2(source, target)
        copied.append(target.relative_to(target_root).as_posix())

    for source_dir in sorted(release_root.glob("*.libs")):
        target_dir = target_root / source_dir.name
        if target_dir.exists():
            shutil.rmtree(target_dir)
        shutil.copytree(source_dir, target_dir)
        for path in sorted(target_dir.rglob("*")):
            if path.is_file():
                copied.append(path.relative_to(target_root).as_posix())

    return copied


def rebuild_record(root: pathlib.Path) -> None:
    dist_infos = sorted(root.glob("*.dist-info"))
    if len(dist_infos) != 1:
        raise RuntimeError(
            f"expected one *.dist-info directory, found {len(dist_infos)}"
        )
    record_path = dist_infos[0] / "RECORD"

    rows: list[tuple[str, str, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if path == record_path:
            rows.append((relative, "", ""))
            continue
        payload = path.read_bytes()
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
            .decode()
            .rstrip("=")
        )
        rows.append((relative, f"sha256={digest}", str(len(payload))))

    with record_path.open("w", newline="") as record_file:
        csv.writer(record_file, lineterminator="\n").writerows(rows)


def rewrite_wheel(root: pathlib.Path, wheel_path: pathlib.Path) -> None:
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as wheel:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo.from_file(path, arcname=relative)
            info.compress_type = zipfile.ZIP_DEFLATED
            wheel.writestr(info, path.read_bytes())


def main() -> None:
    args = parse_args()
    target_wheel = (
        pathlib.Path(args.target_wheel)
        if args.target_wheel
        else pick_target_wheel(args.target_wheel_glob)
    )
    release_wheel = pathlib.Path(args.release_wheel)

    if not target_wheel.exists():
        raise FileNotFoundError(target_wheel)
    if not release_wheel.exists():
        raise FileNotFoundError(release_wheel)

    with tempfile.TemporaryDirectory(
        prefix="store-rs-wheel-"
    ) as target_tmp, tempfile.TemporaryDirectory(
        prefix="mooncake-release-wheel-"
    ) as release_tmp:
        target_root = pathlib.Path(target_tmp)
        release_root = pathlib.Path(release_tmp)
        extract_wheel(target_wheel, target_root)
        extract_wheel(release_wheel, release_root)
        copied = copy_release_assets(release_root, target_root)
        rebuild_record(target_root)
        rewrite_wheel(target_root, target_wheel)

    print(f"rewrote {target_wheel}")
    print(f"copied {len(copied)} release assets")
    for path in copied:
        print(f"  {path}")


if __name__ == "__main__":
    main()
