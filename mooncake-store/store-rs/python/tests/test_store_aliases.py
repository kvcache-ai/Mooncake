from __future__ import annotations

import importlib
import sys
import types
import unittest

_NATIVE = "mooncake_store_rs._store_rs"


def _purge() -> None:
    for name in list(sys.modules):
        if name == "mooncake_store_rs" or name.startswith("mooncake_store_rs."):
            del sys.modules[name]


def _load_store_module():
    """Import `mooncake_store_rs.store` without a built native extension.

    Seeding `sys.modules` is enough: `_load_native` reaches the extension via
    `from . import _store_rs`, which resolves out of `sys.modules` first.
    """
    _purge()

    runtime = importlib.import_module("mooncake_store_rs._runtime")
    runtime.preload_native_libraries = lambda package_root=None: None

    sys.modules[_NATIVE] = types.ModuleType(_NATIVE)
    return importlib.import_module("mooncake_store_rs.store")


STORE_MODULE = _load_store_module()
MooncakeDistributedStore = STORE_MODULE.MooncakeDistributedStore


class StoreAliasTests(unittest.TestCase):
    def test_store_import_tolerates_native_without_buffer_pool(self) -> None:
        self.assertIsNone(STORE_MODULE.BufferPool)

    def test_package_import_does_not_eagerly_load_store_rs_native(self) -> None:
        _purge()

        importlib.import_module("mooncake_store_rs")

        self.assertNotIn("mooncake_store_rs.store", sys.modules)
        self.assertNotIn("mooncake_store_rs._runtime", sys.modules)
        self.assertNotIn(_NATIVE, sys.modules)

    def test_put_batch_delegates_to_batch_put(self) -> None:
        store = object.__new__(MooncakeDistributedStore)
        calls: list[tuple[str, tuple, dict]] = []

        def fake_invoke(name, *args, **kwargs):
            calls.append((name, args, kwargs))
            return 0

        store._invoke = fake_invoke  # type: ignore[attr-defined]
        store._track_keys = lambda keys, tenant=None: None  # type: ignore[attr-defined]

        result = store.put_batch(["alpha"], [b"one"], tenant="tenant-a")

        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "batch_put")

    def test_get_batch_delegates_to_batch_get(self) -> None:
        store = object.__new__(MooncakeDistributedStore)
        calls: list[tuple[str, tuple, dict]] = []

        def fake_invoke(name, *args, **kwargs):
            calls.append((name, args, kwargs))
            return [b"one", b"two"]

        store._invoke = fake_invoke  # type: ignore[attr-defined]

        result = store.get_batch(["alpha", "beta"], tenant="tenant-a")

        self.assertEqual(result, [b"one", b"two"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "batch_get")


if __name__ == "__main__":
    unittest.main()
