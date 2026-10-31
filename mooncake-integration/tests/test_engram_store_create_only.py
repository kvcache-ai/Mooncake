"""Check strict creation and legacy Put against an isolated real master."""

import sys
import unittest
from pathlib import Path

import engram_store_test_client as native

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import test_engram_store as harness  # noqa: E402


class CreateOnlyTests(unittest.TestCase):
    def test_existing_key_is_not_success_and_data_is_preserved(self):
        config = harness.start_local_master()
        try:
            initial, legacy, strict, unchanged = native.verify_real_create_only(
                config.master_server_address, config.metadata_server
            )
            self.assertEqual(initial, 0)
            self.assertEqual(legacy, 0)
            self.assertEqual(strict, native.OBJECT_ALREADY_EXISTS)
            self.assertTrue(unchanged)
        finally:
            harness.stop_local_master()


if __name__ == "__main__":
    unittest.main(verbosity=2)
