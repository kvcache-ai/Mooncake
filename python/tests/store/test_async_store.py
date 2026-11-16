from __future__ import annotations

import asyncio
from collections.abc import Iterator
import importlib
from pathlib import Path
import sys
import threading
from types import ModuleType

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PYTHON_ROOT = REPOSITORY_ROOT / "python"
CANONICAL_MODULE = PYTHON_ROOT / "mooncake" / "async_store.py"
_MISSING = object()


class MockMooncakeDistributedStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, int, int]] = []
        self.non_callable = 42

    def put(self, key: str, value: bytes, *, ttl: int = 0) -> tuple[str, int]:
        self.calls.append((key, value, ttl, threading.get_ident()))
        return key, ttl


@pytest.fixture()
def async_store_module() -> Iterator[ModuleType]:
    module_names = (
        "mooncake",
        "mooncake.async_store",
        "mooncake.store",
    )
    saved_modules = {name: sys.modules.get(name, _MISSING) for name in module_names}
    for name in module_names:
        sys.modules.pop(name, None)

    package = ModuleType("mooncake")
    package.__path__ = [str(PYTHON_ROOT / "mooncake")]
    store = ModuleType("mooncake.store")
    store.MooncakeDistributedStore = MockMooncakeDistributedStore
    sys.modules["mooncake"] = package
    sys.modules["mooncake.store"] = store

    try:
        yield importlib.import_module("mooncake.async_store")
    finally:
        for name in module_names:
            sys.modules.pop(name, None)
            saved = saved_modules[name]
            if saved is not _MISSING:
                sys.modules[name] = saved


def test_source_tree_import_uses_canonical_module(
    async_store_module: ModuleType,
) -> None:
    assert Path(async_store_module.__file__).resolve() == CANONICAL_MODULE
    assert (
        async_store_module.MooncakeDistributedStoreAsync.__module__
        == "mooncake.async_store"
    )


def test_async_wrapper_forwards_arguments_and_is_cached(
    async_store_module: ModuleType,
) -> None:
    store = async_store_module.MooncakeDistributedStoreAsync()
    calling_thread = threading.get_ident()

    async_put = store.async_put
    assert async_put is store.async_put
    assert async_put.__name__ == "put"
    assert asyncio.run(async_put("key", b"value", ttl=7)) == ("key", 7)
    assert store.calls[0][:3] == ("key", b"value", 7)
    assert store.calls[0][3] != calling_thread


def test_async_wrapper_rejects_invalid_attributes(
    async_store_module: ModuleType,
) -> None:
    store = async_store_module.MooncakeDistributedStoreAsync()

    with pytest.raises(AttributeError, match="has no attribute 'missing'"):
        getattr(store, "missing")
    with pytest.raises(AttributeError, match="nor 'missing'"):
        getattr(store, "async_missing")
    with pytest.raises(AttributeError, match="'non_callable' is not callable"):
        getattr(store, "async_non_callable")
