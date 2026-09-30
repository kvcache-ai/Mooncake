"""Runtime library discovery when the upstream wheel is co-installed.

The upstream ``mooncake-transfer-engine`` wheel ships an auditwheel-vendored
``mooncake.libs/`` directory on every non-NPU build. Since this package is
designed to sit alongside it, nothing here may treat that directory as evidence
about *our* installation.
"""

from __future__ import annotations

import pathlib
import tempfile
import unittest

from mooncake_store_rs import _runtime


class _Layout:
    """A throwaway site-packages tree."""

    def __init__(self, stack: tempfile.TemporaryDirectory[str]) -> None:
        self.site = pathlib.Path(stack.name)
        self.package = self.site / "mooncake_store_rs"
        self.package.mkdir()

    def add_vendored(self, name: str, *libraries: str) -> pathlib.Path:
        vendored = self.site / name
        vendored.mkdir(exist_ok=True)
        for library in libraries:
            (vendored / f"{library[:-3]}-abc123.so").touch()
        return vendored


class PreloadSuppressionTests(unittest.TestCase):
    def setUp(self) -> None:
        stack = tempfile.TemporaryDirectory()
        self.addCleanup(stack.cleanup)
        self.layout = _Layout(stack)
        self.preloaded: list[str] = []

    def _run_preload(self) -> None:
        original = _runtime.ctypes.CDLL
        _runtime.ctypes.CDLL = lambda path, mode=0: self.preloaded.append(str(path))
        try:
            _runtime.preload_native_libraries(self.layout.package)
        finally:
            _runtime.ctypes.CDLL = original

    def test_upstream_vendored_dir_does_not_suppress_our_preload(self) -> None:
        # The regression: installing the upstream wheel used to make this
        # package skip preloading, so a working install broke the moment the
        # other wheel showed up.
        self.layout.add_vendored("mooncake.libs", "libtransfer_engine.so")

        self._run_preload()

        self.assertTrue(
            self.preloaded,
            "preload must still run when only the upstream wheel is vendored",
        )

    def test_our_own_vendored_dir_suppresses_preload(self) -> None:
        self.layout.add_vendored("mooncake_store_rs.libs", "libtransfer_engine.so")

        self._run_preload()

        self.assertEqual(self.preloaded, [])


class LibraryPrecedenceTests(unittest.TestCase):
    def setUp(self) -> None:
        stack = tempfile.TemporaryDirectory()
        self.addCleanup(stack.cleanup)
        self.layout = _Layout(stack)

    def test_our_vendored_dir_is_probed_before_upstream(self) -> None:
        # Both wheels vendor a transfer engine, and they need not be the same
        # build, so ours has to win.
        self.layout.add_vendored("mooncake.libs", "libtransfer_engine.so")
        self.layout.add_vendored("mooncake_store_rs.libs", "libtransfer_engine.so")

        dirs = [d.name for d in _runtime.library_dirs(self.layout.package)]

        self.assertIn("mooncake_store_rs.libs", dirs)
        self.assertIn("mooncake.libs", dirs)
        self.assertLess(
            dirs.index("mooncake_store_rs.libs"), dirs.index("mooncake.libs")
        )

    def test_upstream_vendored_libraries_are_still_reachable(self) -> None:
        # Reusing the upstream wheel's transfer engine is a supported fallback
        # when this package has no vendored copy of its own.
        self.layout.add_vendored("mooncake.libs", "libtransfer_engine.so")

        found = _runtime.native_library_candidates(self.layout.package)

        self.assertTrue(
            any("libtransfer_engine-abc123.so" in str(path) for path in found),
            f"expected the upstream vendored library to be probed, got {found}",
        )


class _Checkout:
    """A throwaway source checkout of this package."""

    def __init__(self, stack: tempfile.TemporaryDirectory[str]) -> None:
        self.base = pathlib.Path(stack.name)

    def store_rs(self, *parents: str) -> pathlib.Path:
        """Create a store-rs checkout nested under `parents`."""
        root = self.base.joinpath(*parents) if parents else self.base / "store-rs"
        (root / "crates").mkdir(parents=True)
        (root / "Cargo.toml").touch()
        package = root / "python" / "mooncake_store_rs"
        package.mkdir(parents=True)
        return package

    def upstream(self, root: pathlib.Path, build_dir: str) -> pathlib.Path:
        """Mark `root` as an upstream Mooncake tree with a configured build.

        Returns the resolved artefact directory, since `library_dirs` resolves
        its results and temp dirs sit behind a symlink on macOS.
        """
        (root / "mooncake-transfer-engine").mkdir(parents=True, exist_ok=True)
        built = root / build_dir / "mooncake-transfer-engine" / "src"
        built.mkdir(parents=True)
        return built.resolve()


class LayoutDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        stack = tempfile.TemporaryDirectory()
        self.addCleanup(stack.cleanup)
        self.checkout = _Checkout(stack)

    def test_source_tree_is_recognised(self) -> None:
        package = self.checkout.store_rs()

        root = _runtime.source_tree_root(package)

        self.assertIsNotNone(root)
        assert root is not None
        self.assertTrue((root / "Cargo.toml").is_file())

    def test_installed_layout_is_not_mistaken_for_a_checkout(self) -> None:
        # site-packages/mooncake_store_rs -- two levels up is an arbitrary
        # directory that must not be probed for build artefacts.
        installed = self.checkout.base / "lib" / "python3.99" / "site-packages"
        package = installed / "mooncake_store_rs"
        package.mkdir(parents=True)

        self.assertIsNone(_runtime.source_tree_root(package))
        self.assertEqual(_runtime.upstream_roots(package), [])

    def test_standalone_checkout_finds_upstream_submodule(self) -> None:
        package = self.checkout.store_rs()
        source_root = package.parent.parent
        built = self.checkout.upstream(
            source_root / "third_party" / "Mooncake", "build-rust"
        )

        self.assertIn(built, _runtime.library_dirs(package))

    def test_monorepo_layout_finds_enclosing_upstream(self) -> None:
        # The layout after the move: <mooncake>/mooncake-store/store-rs.
        package = self.checkout.store_rs("mooncake", "mooncake-store", "store-rs")
        monorepo = self.checkout.base / "mooncake"
        built = self.checkout.upstream(monorepo, "build")

        self.assertIn(built, _runtime.library_dirs(package))

    def test_unrelated_ancestors_are_not_treated_as_upstream(self) -> None:
        package = self.checkout.store_rs("mooncake", "mooncake-store", "store-rs")
        # No mooncake-transfer-engine anywhere above, so nothing qualifies.
        self.assertEqual(_runtime.upstream_roots(package), [])


if __name__ == "__main__":
    unittest.main()
