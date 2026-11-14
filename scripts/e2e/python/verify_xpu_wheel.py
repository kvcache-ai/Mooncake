"""Check an installed XPU wheel without a GPU or a source-tree native build."""

import importlib
from importlib import metadata
import os
from pathlib import Path
import socket
import subprocess
import time

from verify_rocm_wheel import find_extension_record, record_digest


def main() -> None:
    os.environ["MC_USE_TENT"] = "1"
    os.environ["MOONCAKE_STORE_BACKEND"] = "cpp"
    distribution = metadata.distribution("mooncake-transfer-engine-xpu")
    package = importlib.import_module("mooncake")
    package_dir = Path(package.__file__).resolve().parent
    records = {str(item): item for item in distribution.files or ()}
    for stem in ("engine.", "_store.", "_ep."):
        relative = find_extension_record(records, stem)
        record = records[relative]
        installed = Path(distribution.locate_file(record)).resolve()
        if installed != package_dir / Path(relative).name:
            raise RuntimeError(
                f"Extension resolves outside installed package: {installed}"
            )
        if (
            record.hash is None
            or record_digest(installed, record.hash.mode) != record.hash.value
        ):
            raise RuntimeError(f"Wheel RECORD mismatch: {installed}")
        module = importlib.import_module(f"mooncake.{stem.rstrip('.')}")
        if Path(module.__file__).resolve() != installed:
            raise RuntimeError(f"Extension loaded outside wheel: {module.__file__}")

    engine = importlib.import_module("mooncake.engine")
    if not engine.SUPPORT_XPU or not engine.SUPPORT_TENT or engine.SUPPORT_CUDA:
        raise RuntimeError("Wheel must provide native XPU/TENT, not CUDA")
    ep = importlib.import_module("mooncake.ep")
    if ep.DEVICE_TYPE != "xpu":
        raise RuntimeError("Wheel did not load the native XPU EP implementation")
    store = importlib.import_module("mooncake.store")
    private_store = importlib.import_module("mooncake._store")
    if (
        store._BACKEND != "cpp"
        or store.MooncakeDistributedStore is not private_store.MooncakeDistributedStore
    ):
        raise RuntimeError("Store facade did not select the installed C++ extension")

    master = package_dir / "mooncake_master"
    record = records.get("mooncake/mooncake_master")
    if (
        record is None
        or record.hash is None
        or record_digest(master, record.hash.mode) != record.hash.value
    ):
        raise RuntimeError("Packaged master does not match wheel RECORD")
    subprocess.run([str(master), "--version"], check=True, timeout=15)
    with subprocess.Popen([str(master), "--port=50051"]) as process:
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError(f"Packaged master exited: {process.returncode}")
                try:
                    with socket.create_connection(("127.0.0.1", 50051), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Packaged master did not start within 10 seconds")
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise
    print(
        f"Verified mooncake-transfer-engine-xpu {distribution.version}; host-only coverage"
    )


if __name__ == "__main__":
    main()
