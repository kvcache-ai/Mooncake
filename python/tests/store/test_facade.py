from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest


PYTHON_SOURCE = Path(__file__).resolve().parents[2]


def _run_python(tmp_path: Path, source: str, backend: str | None) -> subprocess.CompletedProcess:
    environment = os.environ.copy()
    environment.pop("MOONCAKE_STORE_BACKEND", None)
    environment.pop("PYTHONPATH", None)
    environment["PYTHONPATH"] = str(PYTHON_SOURCE)
    if backend is not None:
        environment["MOONCAKE_STORE_BACKEND"] = backend
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result


_CPP_PUBLIC_NAMES = {
    "RangedReadSnapshot",
    "ObjectDataType",
    "SoftPinAction",
    "ReplicateConfig",
    "ReplicaStatus",
    "MemoryDescriptor",
    "DiskDescriptor",
    "ReplicaDescriptor",
    "Descriptor",
    "TaskType",
    "TaskStatus",
    "QueryTaskResponse",
    "BufferHandle",
    "BufferLeaseView",
    "BufferLease",
    "BufferPool",
    "RegisteredBufferPool",
    "RegisteredBufferLease",
    "RegisteredBufferLeaseView",
    "MooncakeHostMemAllocator",
    "MooncakeDistributedNoFRegister",
    "MooncakeDistributedStore",
    "EngramStoreConfig",
    "EngramStore",
    "bind_to_numa_node",
    "get_alloc_func_addr",
    "get_free_func_addr",
    "UNKNOWN",
    "KVCACHE",
    "TENSOR",
    "WEIGHT",
    "SAMPLE",
    "ACTIVATION",
    "GRADIENT",
    "OPTIMIZER_STATE",
    "METADATA",
    "GENERAL",
    "UNDEFINED",
    "INITIALIZED",
    "PROCESSING",
    "COMPLETE",
    "REMOVED",
    "FAILED",
    "REPLICA_COPY",
    "REPLICA_MOVE",
    "PENDING",
    "SUCCESS",
}


@pytest.mark.parametrize("backend", [None, "cpp"], ids=["default", "explicit-cpp"])
def test_cpp_keeps_the_native_public_api(tmp_path: Path, backend: str | None) -> None:
    _run_python(
        tmp_path,
        f"""
import importlib
import sys
import types

native = types.ModuleType("mooncake._store")
expected = {sorted(_CPP_PUBLIC_NAMES)!r}
for name in expected:
    setattr(native, name, object())
native.RegisteredBufferPool = native.BufferPool
sys.modules[native.__name__] = native

store = importlib.import_module("mooncake.store")
assert store._BACKEND == "cpp"
assert set(expected) == set(store.__all__)
assert all(getattr(store, name) is getattr(native, name) for name in expected)

import mooncake
from mooncake.buffer_pool import BufferPool, RegisteredBufferPool
assert mooncake.BufferPool is store.BufferPool
assert BufferPool is store.BufferPool
assert RegisteredBufferPool is store.RegisteredBufferPool
assert store.RegisteredBufferPool is store.BufferPool
assert "_serialize_tensor" not in store.__all__
""",
        backend=backend,
    )


@pytest.mark.parametrize("backend", ["", "store-rs", "CPP"])
def test_invalid_backend_fails_when_facade_is_imported(
    tmp_path: Path, backend: str
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PYTHON_SOURCE)
    environment["MOONCAKE_STORE_BACKEND"] = backend
    result = subprocess.run(
        [sys.executable, "-c", "import mooncake.store"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "must be exactly 'cpp' or 'rs'" in result.stderr


def test_cpp_selection_requires_the_cpp_component(tmp_path: Path) -> None:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["PYTHONPATH"] = str(PYTHON_SOURCE)
    environment["MOONCAKE_STORE_BACKEND"] = "cpp"
    result = subprocess.run(
        [sys.executable, "-c", "import mooncake.store"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "requires a Mooncake wheel built with WITH_STORE=ON" in result.stderr


def test_rs_selection_uses_private_native_and_rejects_cpp_only_apis(
    tmp_path: Path,
) -> None:
    _run_python(
        tmp_path,
        """
import importlib
import sys
import types

native = types.ModuleType("mooncake._store_rs")
native.MooncakeDistributedStore = type("RsStore", (), {})
native.MooncakeHostMemAllocator = type("RsAllocator", (), {})
native.BufferPool = type("RsBufferPool", (), {})
native.ReplicateConfig = type("RsReplicateConfig", (), {})
native.ParallelAxis = type("ParallelAxis", (), {})
native.TensorParallelism = type("TensorParallelism", (), {})
native.ReadTarget = type("ReadTarget", (), {})
native.init_tracing = lambda *args: None
native.metrics_text = lambda: ""
native.start_metrics_server = lambda *args: "127.0.0.1:1"
native.stop_metrics_server = lambda: None
native.metrics_server_address = lambda: None
for name, value in {
    "AXIS_DP": 1, "AXIS_TP": 2, "AXIS_EP": 3, "AXIS_PP": 4,
    "READ_MODE_AS_STORED": 1, "READ_MODE_SHARD": 2, "READ_MODE_FULL": 3,
}.items():
    setattr(native, name, lambda value=value: value)
sys.modules[native.__name__] = native

store = importlib.import_module("mooncake.store")
implementation = importlib.import_module("mooncake.store.rs.store")
assert store._BACKEND == "rs"
assert store.MooncakeDistributedStore is implementation.MooncakeDistributedStore
assert store.ReplicateConfig is implementation.ReplicateConfig
assert store.BufferPool is implementation.BufferPool
assert store.RegisteredBufferPool is store.BufferPool
import mooncake
from mooncake.buffer_pool import BufferPool, RegisteredBufferPool
assert mooncake.BufferPool is store.BufferPool
assert BufferPool is store.BufferPool
assert RegisteredBufferPool is store.BufferPool
try:
    store.EngramStore
except AttributeError as error:
    assert "C++ Store API" in str(error)
else:
    raise AssertionError("Store-RS must mark C++-only APIs unavailable")
""",
        backend="rs",
    )


def test_rs_selection_without_private_extension_has_a_feature_error(
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PYTHON_SOURCE)
    environment["MOONCAKE_STORE_BACKEND"] = "rs"
    result = subprocess.run(
        [sys.executable, "-c", "import mooncake.store"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "requires a Mooncake wheel built with WITH_STORE_RS=ON" in result.stderr


def test_importing_mooncake_does_not_load_a_store_backend(tmp_path: Path) -> None:
    _run_python(
        tmp_path,
        """
import sys
import types

sys.modules["mooncake.engine"] = types.ModuleType("mooncake.engine")
import mooncake.engine
assert "mooncake.store" not in sys.modules
assert "mooncake._store" not in sys.modules
assert "mooncake._store_rs" not in sys.modules
""",
        backend="rs",
    )
