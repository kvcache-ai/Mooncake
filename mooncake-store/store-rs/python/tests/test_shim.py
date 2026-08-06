"""Coverage for the four install combinations of the ``mooncake.*`` redirect.

The redirect has to behave correctly whether or not the upstream
``mooncake-transfer-engine`` wheel is present and whether or not the operator
selected this backend, so all four are exercised here.
"""

from __future__ import annotations

import importlib
import os
import pathlib
import re
import sys
import types
import unittest
from importlib.machinery import ModuleSpec, PathFinder
from unittest import mock

from mooncake_store_rs import _shim

_NATIVE = "mooncake_store_rs._store_rs"
_REAL_FIND_SPEC = PathFinder.find_spec


def _patch_upstream(spec: ModuleSpec | None) -> mock._patch:
    """Make only ``mooncake`` resolve to `spec`, leaving other names real.

    Patching `PathFinder.find_spec` wholesale would also hide
    ``mooncake_store_rs`` from the import system, which the redirect itself
    depends on.
    """

    def find_spec(name, path=None, target=None):
        if name == "mooncake":
            return spec
        return _REAL_FIND_SPEC(name, path, target)

    return mock.patch(
        "mooncake_store_rs._shim.PathFinder.find_spec", side_effect=find_spec
    )


def _seed_native() -> None:
    """Let `mooncake_store_rs.store` import without a built extension."""
    runtime = importlib.import_module("mooncake_store_rs._runtime")
    runtime.preload_native_libraries = lambda package_root=None: None
    sys.modules.setdefault(_NATIVE, types.ModuleType(_NATIVE))


def _purge_mooncake() -> None:
    for name in list(sys.modules):
        if name == "mooncake" or name.startswith("mooncake."):
            del sys.modules[name]


class ShimTestCase(unittest.TestCase):
    def setUp(self) -> None:
        _seed_native()
        _shim.uninstall()
        self._saved = {
            name: module
            for name, module in sys.modules.items()
            if name == "mooncake" or name.startswith("mooncake.")
        }
        _purge_mooncake()
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        _shim.uninstall()
        _purge_mooncake()
        sys.modules.update(self._saved)

    @staticmethod
    def _env(value: str | None) -> mock._patch_dict:
        if value is None:
            return mock.patch.dict(os.environ, {}, clear=False)
        return mock.patch.dict(os.environ, {_shim.BACKEND_ENV: value})


class BackendSelectionTests(ShimTestCase):
    def test_inert_without_env(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_shim.BACKEND_ENV, None)
            self.assertFalse(_shim.backend_selected())
            self.assertFalse(_shim.install())
        self.assertEqual(_shim.active_backend(), "upstream")

    def test_inert_when_env_names_upstream(self) -> None:
        with self._env("cpp"):
            self.assertFalse(_shim.backend_selected())
            self.assertFalse(_shim.install())
        self.assertEqual(_shim.active_backend(), "upstream")

    def test_selected_by_each_accepted_alias(self) -> None:
        for value in ("rs", "rust", "store-rs", "store_rs", "masterless", "RS", " rs "):
            with self.subTest(value=value), self._env(value):
                self.assertTrue(_shim.backend_selected())

    def test_install_is_idempotent(self) -> None:
        with self._env("rs"):
            self.assertTrue(_shim.install())
            self.assertTrue(_shim.install())
        installed = [f for f in sys.meta_path if isinstance(f, _shim.StoreRsFinder)]
        self.assertEqual(len(installed), 1)


class RedirectTests(ShimTestCase):
    def test_redirect_binds_upstream_name_to_this_backend(self) -> None:
        with self._env("rs"):
            self.assertTrue(_shim.install())
            store = importlib.import_module("mooncake.store")

        canonical = importlib.import_module("mooncake_store_rs.store")
        self.assertIs(store, canonical)
        self.assertIs(sys.modules["mooncake.store"], canonical)

    def test_alias_does_not_rewrite_the_canonical_module_identity(self) -> None:
        canonical = importlib.import_module("mooncake_store_rs.store")
        name_before = canonical.__name__
        spec_before = canonical.__spec__

        with self._env("rs"):
            _shim.install()
            importlib.import_module("mooncake.store")

        self.assertEqual(canonical.__name__, name_before)
        self.assertIs(canonical.__spec__, spec_before)

    def test_every_divergent_module_is_redirected(self) -> None:
        # Where the two implementations genuinely differ, serving one half from
        # upstream and the other from here would not work.
        for name in (
            "mooncake.store",
            "mooncake.buffer_pool",
            "mooncake.cli",
            "mooncake.cli_client",
            "mooncake.structured_object_store",
        ):
            with self.subTest(module=name):
                self.assertIn(name, _shim._REDIRECTS)

    def test_engine_is_redirected(self) -> None:
        # SGLang imports `mooncake.engine`. Without this the wheel cannot stand
        # alone -- it would need the upstream wheel installed alongside it.
        self.assertIn("mooncake.engine", _shim._REDIRECTS)

    def test_redirect_targets_all_live_in_this_package(self) -> None:
        for source, target in _shim._REDIRECTS.items():
            with self.subTest(module=source):
                self.assertTrue(
                    target.startswith("mooncake_store_rs."),
                    f"{source} -> {target} escapes this package",
                )

    def test_vendored_assets_are_all_redirected(self) -> None:
        """Every upstream module the wheel vendors must also be redirected.

        The two lists live apart -- `build-wheel.sh` stages the files, this shim
        exposes them under the upstream import path -- so they drift silently:
        a module added to the wheel but not here is simply unreachable through
        `mooncake.*`, and a stale entry here redirects to nothing.
        """
        build_wheel = (
            pathlib.Path(__file__).resolve().parents[2]
            / "scripts"
            / "build"
            / "build-wheel.sh"
        )
        if not build_wheel.is_file():
            self.skipTest(f"{build_wheel} not present")

        text = build_wheel.read_text()
        block = re.search(r"python_assets = \[(.*?)\]", text, re.S)
        self.assertIsNotNone(block, "could not locate python_assets in build-wheel.sh")
        assert block is not None
        staged = {
            f"mooncake.{name[: -len('.py')]}"
            for name in re.findall(r'"([^"]+\.py)"', block.group(1))
        }
        self.assertTrue(staged, "python_assets parsed as empty")

        missing = staged - set(_shim._REDIRECTS)
        self.assertFalse(
            missing, f"vendored but not redirected, unreachable as mooncake.*: {missing}"
        )

    def test_unlisted_submodules_are_left_to_upstream(self) -> None:
        finder = _shim.StoreRsFinder()
        # Real upstream modules this wheel neither implements nor vendors; they
        # have to keep resolving to the upstream wheel.
        for name in (
            "mooncake.cli_bench",
            "mooncake.mooncake_elastic_buffer",
            "mooncake.mooncake_ssd_register",
            "mooncake.spdk_tgt_create",
        ):
            with self.subTest(module=name):
                self.assertIsNone(finder.find_spec(name))
        self.assertIsNone(finder.find_spec("mooncake_store_rs.store"))
        self.assertIsNone(finder.find_spec("json"))


class ParentPackageTests(ShimTestCase):
    def test_parent_is_synthesised_when_upstream_absent(self) -> None:
        finder = _shim.StoreRsFinder()
        with _patch_upstream(None):
            spec = finder.find_spec("mooncake")

        self.assertIsNotNone(spec)
        assert spec is not None
        self.assertEqual(spec.submodule_search_locations, [])

    def test_upstream_parent_wins_when_present(self) -> None:
        finder = _shim.StoreRsFinder()
        upstream = ModuleSpec(
            "mooncake", None, origin="/somewhere/mooncake/__init__.py"
        )
        with _patch_upstream(upstream):
            self.assertIsNone(finder.find_spec("mooncake"))

    def test_import_works_with_only_this_backend_installed(self) -> None:
        with self._env("rs"), _patch_upstream(None):
            _shim.install()
            store = importlib.import_module("mooncake.store")

        self.assertIs(store, importlib.import_module("mooncake_store_rs.store"))


if __name__ == "__main__":
    unittest.main()
