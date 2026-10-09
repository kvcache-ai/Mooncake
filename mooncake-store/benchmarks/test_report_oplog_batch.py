import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("report_oplog_batch.py")
SPEC = importlib.util.spec_from_file_location("report_oplog_batch", MODULE_PATH)
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)


class ReportTest(unittest.TestCase):
    def test_summarizes_repeats_and_stage_limits(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            samples = root / "samples.jsonl"
            rows = []
            for repeat, entries_per_sec in enumerate((9000, 10000, 11000)):
                rows.append(
                    {
                        "schema_version": 1,
                        "case": "normal-e64-b8",
                        "repeat": repeat,
                        "entries": 8000,
                        "entries_per_sec": entries_per_sec,
                        "transactions_per_sec": entries_per_sec / 8,
                        "cpu_cores_used": 0.75 + repeat * 0.25,
                        "batch_entries_mean": 8,
                        "encoded_bytes": 800000,
                        "stage_latency_us": {
                            "queue": 100,
                            "encode": 200,
                            "txn": 500,
                            "callback": 200,
                        },
                    }
                )
            samples.write_text("".join(json.dumps(row) + "\n" for row in rows))

            summary = report.generate_report(samples, root / "out")

            case = summary["cases"][0]
            self.assertEqual(case["entries_per_sec"]["median"], 10000)
            self.assertEqual(case["entries_per_sec"]["min"], 9000)
            self.assertEqual(case["entries_per_sec"]["max"], 11000)
            self.assertAlmostEqual(case["bytes_per_entry"], 100.0)
            self.assertEqual(case["cpu_cores_used"]["median"], 1.0)
            self.assertAlmostEqual(case["stage_share"]["txn"], 0.5)
            self.assertEqual(case["theoretical_entries_per_sec"], 16000)
            self.assertAlmostEqual(case["observed_efficiency"], 0.625)
            self.assertEqual(case["bottleneck_stage"], "txn")
            self.assertTrue((root / "out" / "report.md").is_file())
            with (root / "out" / "cases.csv").open(newline="") as stream:
                self.assertEqual(
                    list(csv.DictReader(stream))[0]["case"], "normal-e64-b8"
                )

    def test_marks_missing_measurements_as_insufficient(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            samples = root / "samples.jsonl"
            samples.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "case": "partial",
                        "repeat": 0,
                        "entries_per_sec": 42,
                    }
                )
                + "\n"
            )

            case = report.generate_report(samples, root / "out")["cases"][0]

            self.assertEqual(case["status"], "insufficient_data")
            self.assertIn("at least 3 repeats", case["missing"])
            self.assertIn("encoded_bytes and entries", case["missing"])
            self.assertIn("stage_latency_us.txn", case["missing"])
            self.assertNotIn("theoretical_entries_per_sec", case)


if __name__ == "__main__":
    unittest.main()
