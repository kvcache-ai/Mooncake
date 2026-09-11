#!/usr/bin/env python3

"""Verify that the active Mooncake native modules belong to the ROCm wheel."""

import base64
import hashlib
import importlib
import importlib.metadata as metadata
from pathlib import Path


def record_digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm, path.read_bytes()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def main() -> None:
    distribution = metadata.distribution("mooncake-transfer-engine-rocm")
    package = importlib.import_module("mooncake")
    package_dir = Path(package.__file__).resolve().parent
    records = {str(item): item for item in distribution.files or ()}
    expected_files = {
        "mooncake/engine.so": "mooncake.engine",
        "mooncake/store.so": "mooncake.store",
        "mooncake/mooncake_master": None,
    }

    print("Mooncake package:", package.__file__)
    print("Mooncake ROCm distribution:", distribution.version)
    for relative_path, module_name in expected_files.items():
        record = records.get(relative_path)
        if record is None or record.hash is None:
            raise RuntimeError(f"Missing hashed wheel record: {relative_path}")

        installed_path = Path(distribution.locate_file(record)).resolve()
        package_path = (package_dir / Path(relative_path).name).resolve()
        if installed_path != package_path:
            raise RuntimeError(
                f"{relative_path} resolves outside the active Mooncake package: "
                f"{installed_path} != {package_path}"
            )

        digest = record_digest(installed_path, record.hash.mode)
        if digest != record.hash.value:
            raise RuntimeError(f"Installed file does not match wheel: {installed_path}")

        if module_name is not None:
            module = importlib.import_module(module_name)
            if Path(module.__file__).resolve() != installed_path:
                raise RuntimeError(
                    f"{module_name} loaded from an unexpected path: {module.__file__}"
                )

        print(relative_path, installed_path, f"{record.hash.mode}={digest}")


if __name__ == "__main__":
    main()
