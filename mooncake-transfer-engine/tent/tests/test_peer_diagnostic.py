"""Controller-only fixtures: these records are not RDMA measurements."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "diagnostic", Path(__file__).parents[1] / "benchmark/analyze_peer_diagnostic.py"
)
diagnostic = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic)


class CalibrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.run = Path(self.temp.name)
        self.summary = {
            "measurement_origin_ns": 10000000,
            "measurement_ns": 10000000,
            "variant": "v1",
            "evidence": "rdma_diagnostic",
            "data_verification_complete": True,
            "all": {"success": 3},
        }
        requests = [{"detail": {"peer": 0, "request_key": i}} for i in range(3)]
        (self.run / "requests.jsonl").write_text(
            "\n".join(json.dumps(r) for r in requests)
        )
        self.lines = []
        for i in range(3):
            for dev in range(2):
                samples = 0 if i == 0 and dev == 1 else 1
                age = 1000000 * i + dev
                self.lines.append(
                    f"peer_allocation peer=0 request_key={i} now_ns=15000000 "
                    f"num_slices=32 candidates=2 dev={dev} samples={samples} "
                    f"last_sample_ns={15000000-age}"
                )

    def analyze(self):
        (self.run / "summary.json").write_text(json.dumps(self.summary))
        log = self.run / "diagnostic.log"
        log.write_text("\n".join(self.lines))
        return diagnostic.analyze(self.run, log)

    def test_max_candidate_age_excludes_cold_and_rounds_up_without_multiplier(self):
        result = self.analyze()
        self.assertTrue(result["calibration_complete"])
        self.assertEqual(result["cold_decisions"], 1)
        self.assertEqual(result["noncold_decisions"], 2)
        self.assertEqual(result["maximum_candidate_age_p99_ns"], 2000001)
        self.assertEqual(result["proposed_frozen_ttl_ns"], 3000000)

    def test_incomplete_candidate_records_cannot_freeze_ttl(self):
        self.lines.pop()
        result = self.analyze()
        self.assertFalse(result["calibration_complete"])
        self.assertIsNone(result["proposed_frozen_ttl_ns"])

    def test_tcp_or_failed_evidence_cannot_freeze_ttl(self):
        self.summary["evidence"] = "tcp_control_only"
        self.assertIsNone(self.analyze()["proposed_frozen_ttl_ns"])
        self.summary["evidence"] = "rdma_diagnostic"
        self.summary["all"]["success"] = 2
        self.assertIsNone(self.analyze()["proposed_frozen_ttl_ns"])


if __name__ == "__main__":
    unittest.main()
