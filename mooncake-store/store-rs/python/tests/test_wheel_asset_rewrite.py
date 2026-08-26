import importlib.util
import pathlib
import tempfile
import unittest


SCRIPT = (
    pathlib.Path(__file__).parents[2]
    / "scripts"
    / "build"
    / "replace-wheel-mooncake-release.py"
)
SPEC = importlib.util.spec_from_file_location("wheel_asset_rewrite", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class WheelAssetRewriteTests(unittest.TestCase):
    def test_release_assets_keep_store_rs_package_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            release_root = root / "release"
            target_root = root / "target"
            release_package = release_root / "mooncake"
            target_package = target_root / "mooncake_store_rs"
            release_libraries = release_root / "mooncake.libs"

            release_package.mkdir(parents=True)
            target_package.mkdir(parents=True)
            release_libraries.mkdir(parents=True)
            (release_package / "engine.so").write_bytes(b"engine")
            (release_package / "mooncake_config.py").write_text("VALUE = 1\n")
            (release_libraries / "libtransfer_engine.so").write_bytes(b"library")

            copied = MODULE.copy_release_assets(release_root, target_root)

            self.assertEqual(
                (target_package / "engine.so").read_bytes(), b"engine"
            )
            self.assertTrue(
                (target_package / "mooncake_config.py").is_file()
            )
            self.assertEqual(
                (
                    target_root
                    / "mooncake_store_rs.libs"
                    / "libtransfer_engine.so"
                ).read_bytes(),
                b"library",
            )
            self.assertFalse((target_root / "mooncake").exists())
            self.assertIn("mooncake_store_rs/engine.so", copied)
            self.assertIn(
                "mooncake_store_rs.libs/libtransfer_engine.so", copied
            )


if __name__ == "__main__":
    unittest.main()
