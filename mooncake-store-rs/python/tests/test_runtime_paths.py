from __future__ import annotations

import pathlib
import tempfile
import unittest
from unittest.mock import patch

from mooncake_store_rs import _runtime


class _Checkout:
    """A throwaway source checkout of this package."""

    def __init__(self, stack: tempfile.TemporaryDirectory[str]) -> None:
        self.base = pathlib.Path(stack.name)

    def store_rs(self, *parents: str) -> pathlib.Path:
        """Create a Store-RS checkout nested under `parents`."""
        root = self.base.joinpath(*parents) if parents else self.base / "store-rs"
        (root / "crates").mkdir(parents=True)
        package = root / "python" / "mooncake_store_rs"
        package.mkdir(parents=True)
        return package

    def build(self, root: pathlib.Path) -> pathlib.Path:
        """Create the explicitly configured CMake build's TE artefacts."""
        built = root / "mooncake-transfer-engine" / "src"
        built.mkdir(parents=True)
        return built.resolve()


class RuntimePathTests(unittest.TestCase):
    def setUp(self) -> None:
        stack = tempfile.TemporaryDirectory()
        self.addCleanup(stack.cleanup)
        self.checkout = _Checkout(stack)

    def test_source_tree_root_comes_from_explicit_environment(self) -> None:
        package = self.checkout.store_rs()
        root = package.parent.parent

        with patch.dict("os.environ", {"MOONCAKE_STORE_RS_DIR": str(root)}):
            source_root = _runtime.source_tree_root()
            self.assertEqual(source_root, root)
            assert source_root is not None
            self.assertTrue((source_root / "crates").is_dir())

    def test_source_tree_is_not_inferred_without_environment(self) -> None:
        self.checkout.store_rs()

        with patch.dict("os.environ", {"MOONCAKE_STORE_RS_DIR": ""}):
            self.assertIsNone(_runtime.source_tree_root())

    def test_cmake_build_libraries_require_explicit_build_directory(self) -> None:
        package = self.checkout.store_rs()
        build_dir = self.checkout.base / "build-rust"
        built = self.checkout.build(build_dir)

        with patch.dict(
            "os.environ",
            {"MOONCAKE_BUILD_DIR": str(build_dir), "MOONCAKE_STORE_RS_DIR": ""},
        ):
            self.assertIn(built, _runtime.library_dirs(package))

    def test_old_submodule_build_is_not_inferred(self) -> None:
        package = self.checkout.store_rs()
        old_build = self.checkout.base / "third_party" / "Mooncake" / "build-rust"
        built = self.checkout.build(old_build)

        with patch.dict(
            "os.environ",
            {
                "MOONCAKE_STORE_RS_DIR": str(package.parent.parent),
                "MOONCAKE_BUILD_DIR": "",
            },
        ):
            self.assertNotIn(built, _runtime.library_dirs(package))

    def test_transfer_engine_benchmark_uses_explicit_build_directory(self) -> None:
        package = self.checkout.store_rs()
        build_dir = self.checkout.base / "build"
        binary = (
            build_dir
            / "mooncake-transfer-engine"
            / "example"
            / "transfer_engine_bench"
        )
        binary.parent.mkdir(parents=True)
        binary.touch()

        with patch.dict(
            "os.environ",
            {"MOONCAKE_BUILD_DIR": str(build_dir), "MOONCAKE_STORE_RS_DIR": ""},
        ):
            self.assertEqual(
                _runtime.binary_path("transfer_engine_bench", package), binary
            )

    def test_cpp_store_binary_is_not_a_runtime_candidate(self) -> None:
        package = self.checkout.store_rs()
        with patch.dict(
            "os.environ",
            {"MOONCAKE_STORE_RS_DIR": "", "MOONCAKE_BUILD_DIR": ""},
        ):
            with self.assertRaises(FileNotFoundError):
                _runtime.binary_path("mooncake-store", package)


if __name__ == "__main__":
    unittest.main()
