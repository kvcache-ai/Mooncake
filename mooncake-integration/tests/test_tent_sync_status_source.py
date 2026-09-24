import unittest
from pathlib import Path


class TentSyncStatusSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_path = (
            Path(__file__).resolve().parents[1]
            / "transfer_engine"
            / "transfer_engine_py.cpp"
        )
        cls.source = source_path.read_text()

    def test_tent_sync_uses_native_single_attempt(self):
        self.assertIn(
            "engine_->isUsingTent() ? 1 : engine_->numContexts() + 1",
            self.source,
        )

    def test_sync_paths_handle_canceled_and_poll_errors(self):
        self.assertIn("TransferStatusEnum::CANCELED", self.source)
        self.assertIn("invalidateCachedSegment", self.source)
        self.assertNotIn("LOG_ASSERT(s.ok())", self.source)


if __name__ == "__main__":
    unittest.main()
