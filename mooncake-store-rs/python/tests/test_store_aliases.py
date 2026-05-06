from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import unittest


def _load_store_module():
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    package_dir = repo_root / "python" / "mooncake"

    fake_package = types.ModuleType("mooncake")
    fake_package.__path__ = [str(package_dir)]
    sys.modules["mooncake"] = fake_package

    fake_runtime = types.ModuleType("mooncake._runtime")
    fake_runtime.package_dir = lambda: package_dir
    fake_runtime.preload_native_libraries = lambda root: None
    sys.modules["mooncake._runtime"] = fake_runtime

    fake_native = types.ModuleType("mooncake._store_rs")
    sys.modules["mooncake._store_rs"] = fake_native

    spec = importlib.util.spec_from_file_location(
        "mooncake.store",
        package_dir / "store.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to create mooncake.store module spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules["mooncake.store"] = module
    spec.loader.exec_module(module)
    return module


STORE_MODULE = _load_store_module()
MooncakeDistributedStore = STORE_MODULE.MooncakeDistributedStore


class StoreAliasTests(unittest.TestCase):
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
