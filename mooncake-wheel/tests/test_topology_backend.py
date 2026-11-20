import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mooncake.engine import TransferEngine, _is_tent_selected
from mooncake.transfer_engine_topology_dump import resolve_backend


class TopologyBackendTest(unittest.TestCase):
    def test_disabled_flags_return_the_classic_priority_matrix(self):
        # Empty NIC lists keep this test independent of the runner's RNICs.
        matrix = {"cpu:0": [[], []]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "topology.json"
            path.write_text(json.dumps(matrix))
            with patch.dict(
                os.environ,
                {
                    "MC_USE_TENT": "0",
                    "MC_USE_TEV1": "off",
                    "MC_FORCE_TCP": "1",
                    "MC_CUSTOM_TOPO_JSON": str(path),
                },
            ):
                self.assertFalse(_is_tent_selected())
                self.assertEqual(resolve_backend("auto"), "te")
                actual = TransferEngine().get_local_topology()
                self.assertEqual(json.loads(actual), matrix)


if __name__ == "__main__":
    unittest.main()
