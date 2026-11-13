from __future__ import annotations

import importlib
import sys
import types
import weakref

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


def test_remove_all_real_path_returns_success_count(store_module, caplog) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    calls = []

    class Native:
        def remove_all(self, **kwargs):
            calls.append(kwargs)
            return 7

    store._native = Native()
    store._worker = None
    assert store.remove_all(force=True) == 7
    assert calls == [{"force": True}]


def test_remove_all_worker_path_returns_success_count(store_module, caplog) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    calls = []

    class Worker:
        def call(self, name, *args, **kwargs):
            calls.append((name, args, kwargs))
            return 3

    store._native = object()
    store._worker = Worker()
    assert store.remove_all() == 3
    assert calls == [("remove_all", (), {"force": False})]


def test_remove_all_worker_failure_keeps_worker_usable(store_module, caplog) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    results = iter([RuntimeError("worker remove_all timed out"), 2])

    class Worker:
        def call(self, name, *args, **kwargs):
            result = next(results)
            if isinstance(result, BaseException):
                raise result
            return result

    store._native = object()
    store._worker = Worker()
    with caplog.at_level("ERROR"):
        assert store.remove_all() == -1
    assert store.remove_all() == 2


def test_remove_all_native_failure_returns_soft_failure_and_logs(
    store_module, caplog
) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)

    class Native:
        def remove_all(self, **kwargs):
            raise RuntimeError("real remove_all did not complete (evicted=3)")

    store._native = Native()
    store._worker = None
    with caplog.at_level("ERROR"):
        assert store.remove_all() == -1
    assert "remove_all raised an exception" in caplog.text


def test_remove_all_timeout_keeps_wrapper_usable(store_module, caplog) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)
    results = iter([TimeoutError("evict-all timed out"), 5])

    class Native:
        def remove_all(self, **kwargs):
            result = next(results)
            if isinstance(result, BaseException):
                raise result
            return result

    store._native = Native()
    store._worker = None
    with caplog.at_level("ERROR"):
        assert store.remove_all() == -1
    assert store.remove_all() == 5


def test_remove_all_preserves_direct_negative_status(store_module, caplog) -> None:
    store = object.__new__(store_module.MooncakeDistributedStore)

    class Native:
        def remove_all(self, **kwargs):
            return -1

    store._native = Native()
    store._worker = None
    assert store.remove_all() == -1


def test_pending_publication_env_defaults_and_explicit_precedence(
    store_module, monkeypatch
):
    monkeypatch.setenv("MC_STORE_RS_MAX_PENDING_PUBLICATIONS", "17")
    monkeypatch.setenv("MC_STORE_RS_MAX_PENDING_PUBLICATION_BYTES", "4096")
    defaults = store_module._apply_setup_env_defaults({})
    assert defaults["max_pending_publications"] == 17
    assert defaults["max_pending_publication_bytes"] == 4096
    explicit = store_module._apply_setup_env_defaults(
        {
            "max_pending_publications": 3,
            "max_pending_publication_bytes": 512,
        }
    )
    assert explicit["max_pending_publications"] == 3
    assert explicit["max_pending_publication_bytes"] == 512


def test_batch_put_preserves_per_item_statuses(store_module):
    store = object.__new__(store_module.MooncakeDistributedStore)
    store._invoke = lambda *args, **kwargs: [0, -1, 0]
    assert store.batch_put([("first", b"a"), ("duplicate", b"b"), ("third", b"c")]) == [
        0,
        -1,
        0,
    ]


def test_successful_writes_release_key_references(store_module):
    class Key(str):
        def __str__(self):
            return self

    store = store_module.MooncakeDistributedStore()
    store._invoke = (
        lambda name, *args, **kwargs: [0] if name.startswith("batch_") else 0
    )
    writes = [
        lambda key: store.put(key, b"x"),
        lambda key: store.put_from(key, 1, 1),
        lambda key: store.batch_put([(key, b"x")]),
        lambda key: store.batch_put_from([(key, 1, 1)]),
        lambda key: store.batch_put_from([key], [1], [1]),
    ]
    for index, write in enumerate(writes):
        key = Key(f"unique-{index}")
        reference = weakref.ref(key)
        write(key)
        del key
        assert reference() is None
    assert "_tracked_keys" not in vars(store)
