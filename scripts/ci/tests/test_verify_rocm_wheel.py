"""Unit tests for the ROCm wheel release capability guard."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "verify_rocm_wheel", ROOT / "scripts/tone_tests/python/verify_rocm_wheel.py"
)
VERIFY = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(VERIFY)


class VerifyMultiProtocolSupportTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.engine_path = Path(self.temp_dir.name) / "engine.so"

    def test_accepts_explicit_capability(self):
        self.engine_path.write_bytes(b"no legacy marker needed")
        VERIFY.verify_multi_protocol_support(
            SimpleNamespace(SUPPORT_MULTI_PROTOCOL=True), self.engine_path
        )

    def test_rejects_explicitly_disabled_capability(self):
        self.engine_path.write_bytes(VERIFY.LEGACY_MULTI_PROTOCOL_MARKER)
        with self.assertRaisesRegex(RuntimeError, "without ENABLE_MULTI_PROTOCOL"):
            VERIFY.verify_multi_protocol_support(
                SimpleNamespace(SUPPORT_MULTI_PROTOCOL=False), self.engine_path
            )

    def test_accepts_legacy_binary_marker_across_chunks(self):
        self.engine_path.write_bytes(b"abc" + VERIFY.LEGACY_MULTI_PROTOCOL_MARKER)
        self.assertTrue(
            VERIFY.file_contains(
                self.engine_path, VERIFY.LEGACY_MULTI_PROTOCOL_MARKER, chunk_size=5
            )
        )
        VERIFY.verify_multi_protocol_support(SimpleNamespace(), self.engine_path)

    def test_rejects_legacy_binary_without_marker(self):
        self.engine_path.write_bytes(b"multi-protocol support is absent")
        with self.assertRaisesRegex(RuntimeError, "lacks the ENABLE_MULTI_PROTOCOL"):
            VERIFY.verify_multi_protocol_support(SimpleNamespace(), self.engine_path)


if __name__ == "__main__":
    unittest.main()
