from pathlib import Path
import unittest


class TransferEngineFlagCxSourceTest(unittest.TestCase):
    def test_python_transfer_engine_installs_flagcx_transport(self):
        source_path = (
            Path(__file__).resolve().parents[2]
            / "mooncake-integration"
            / "transfer_engine"
            / "transfer_engine_py.cpp"
        )
        source = source_path.read_text()

        self.assertIn('proto == "flagcx"', source)
        self.assertIn('installTransport("flagcx", nullptr)', source)


if __name__ == "__main__":
    unittest.main()
