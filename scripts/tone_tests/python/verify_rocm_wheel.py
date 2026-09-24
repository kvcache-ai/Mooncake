#!/usr/bin/env python3

"""Verify ROCm wheel provenance and required compiled capabilities."""

import base64
import hashlib
import importlib
import importlib.metadata as metadata
from pathlib import Path


LEGACY_MULTI_PROTOCOL_MARKER = b"MC_DISABLE_HIP"


def record_digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm, path.read_bytes()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def file_contains(path: Path, needle: bytes, chunk_size: int = 1024 * 1024) -> bool:
    """Search a binary file without loading the entire artifact into memory."""
    if not needle:
        raise ValueError("needle must not be empty")
    overlap = b""
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            data = overlap + chunk
            if needle in data:
                return True
            overlap = data[-(len(needle) - 1) :] if len(needle) > 1 else b""
    return False


def verify_multi_protocol_support(engine_module: object, engine_path: Path) -> None:
    """Require the routing support needed by a combined RDMA and HIP wheel."""
    advertised = getattr(engine_module, "SUPPORT_MULTI_PROTOCOL", None)
    if advertised is True:
        print("Mooncake multi-protocol support: enabled")
        return
    if advertised is False:
        raise RuntimeError(
            "ROCm wheel was built without ENABLE_MULTI_PROTOCOL; cross-host GPU "
            "targets can be routed through host-local HIP IPC"
        )

    # Historical ROCm tags predate the explicit Python capability bit. The
    # multi-protocol implementation in those trees contains this runtime
    # selector only when ENABLE_MULTI_PROTOCOL is compiled, so retain a narrow
    # compatibility check for workflow_dispatch backfills of those tags.
    if file_contains(engine_path, LEGACY_MULTI_PROTOCOL_MARKER):
        print("Mooncake multi-protocol support: enabled (legacy binary marker)")
        return
    raise RuntimeError(
        "ROCm wheel does not advertise multi-protocol support and its engine "
        "binary lacks the ENABLE_MULTI_PROTOCOL marker"
    )


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
            if module_name == "mooncake.engine":
                verify_multi_protocol_support(module, installed_path)

        print(relative_path, installed_path, f"{record.hash.mode}={digest}")


if __name__ == "__main__":
    main()
