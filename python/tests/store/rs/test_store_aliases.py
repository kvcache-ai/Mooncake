from __future__ import annotations

import importlib
import sys
import types

import pytest


@pytest.fixture
def store_module(monkeypatch: pytest.MonkeyPatch):
    import mooncake

    monkeypatch.setenv("MOONCAKE_STORE_BACKEND", "rs")
    for name in list(sys.modules):
        if name == "mooncake.store" or name.startswith("mooncake.store."):
            monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "mooncake._store_rs", raising=False)
    monkeypatch.delattr(mooncake, "store", raising=False)

    native = types.ModuleType("mooncake._store_rs")
    native.MooncakeDistributedStore = type("NativeStore", (), {})
    native.MooncakeHostMemAllocator = type("NativeAllocator", (), {})
    native.BufferPool = type("NativeBufferPool", (), {})
    native.ParallelAxis = type("ParallelAxis", (), {})
    native.TensorParallelism = type("TensorParallelism", (), {})
    native.ReadTarget = type("ReadTarget", (), {})
    native.ReplicateConfig = type("NativeReplicateConfig", (), {})
    native.init_tracing = lambda trace_filter=None: None
    native.metrics_text = lambda: ""
    native.start_metrics_server = lambda bind_addr="127.0.0.1:0": bind_addr
    native.stop_metrics_server = lambda: None
    native.metrics_server_address = lambda: None
    for name, value in {
        "AXIS_DP": 1,
        "AXIS_TP": 2,
        "AXIS_EP": 3,
        "AXIS_PP": 4,
        "READ_MODE_AS_STORED": 1,
        "READ_MODE_SHARD": 2,
        "READ_MODE_FULL": 3,
    }.items():
        setattr(native, name, lambda value=value: value)

    monkeypatch.setitem(sys.modules, native.__name__, native)
    return importlib.import_module("mooncake.store.rs.store")


def test_rs_source_module_exports_native_buffer_pool(store_module) -> None:
    assert store_module.BufferPool is not None
    assert store_module.RegisteredBufferPool is store_module.BufferPool


def test_put_batch_delegates_to_batch_put(store_module) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    calls: list[tuple[str, tuple, dict]] = []

    def fake_invoke(name, *args, **kwargs):
        calls.append((name, args, kwargs))
        return 0

    store._invoke = fake_invoke  # type: ignore[attr-defined]
    store._track_keys = lambda keys, tenant=None: None  # type: ignore[attr-defined]

    result = store.put_batch(["alpha"], [b"one"], tenant="tenant-a")

    assert result == 0
    assert len(calls) == 1
    assert calls[0][0] == "batch_put"


def test_get_batch_delegates_to_batch_get(store_module) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    calls: list[tuple[str, tuple, dict]] = []

    def fake_invoke(name, *args, **kwargs):
        calls.append((name, args, kwargs))
        return [b"one", b"two"]

    store._invoke = fake_invoke  # type: ignore[attr-defined]

    result = store.get_batch(["alpha", "beta"], tenant="tenant-a")

    assert result == [b"one", b"two"]
    assert len(calls) == 1
    assert calls[0][0] == "batch_get"
