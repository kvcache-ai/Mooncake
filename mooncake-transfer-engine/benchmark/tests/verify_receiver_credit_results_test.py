#!/usr/bin/env python3
"""Regression tests for the receiver-credit JSONL verifier."""

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


VERIFIER = pathlib.Path(__file__).with_name("verify_receiver_credit_results.py")


def record(mode, senders, condition, repetition):
    credit = mode == "credit"
    return {
        "schema_version": 1,
        "run_id": f"{mode}-{senders}-{condition}-{repetition}",
        "mode": mode,
        "condition": condition,
        "sender_count": senders,
        "repetition": repetition,
        "offered": 10,
        "completed": 10,
        "data_errors": 0,
        "throughput_gbps": 9.6 if credit else 10.0,
        "p99_us": 100.0,
        "oracle_throughput_gbps": 10.0,
        "receiver": {
            "capacity_bytes": 100,
            "capacity_slots": 2,
            "current_bytes": 0,
            "current_slots": 0,
            "peak_bytes": 100 if credit else 120,
            "peak_slots": 2,
            "admitted_total": 10,
            "stalled_total": 1 if credit else 0,
            "capacity_violation_total": 0 if credit else 1,
            "invalid_release_total": 0,
        },
    }


class ReceiverCreditVerifierTest(unittest.TestCase):
    def run_verifier(self, records, *arguments):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "results.jsonl"
            path.write_text(
                "".join(json.dumps(item) + "\n" for item in records),
                encoding="utf-8",
            )
            return subprocess.run(
                [sys.executable, str(VERIFIER), "--input", str(path), *arguments],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_accepts_complete_safe_matrix(self):
        records = [
            record(mode, senders, "normal", repetition)
            for mode in ("fixed", "credit")
            for senders in (1, 4)
            for repetition in (1, 2)
        ]
        result = self.run_verifier(
            records,
            "--require-modes=fixed,credit",
            "--require-senders=1,4",
            "--require-conditions=normal",
            "--min-repetitions=2",
            "--require-fixed-overload-evidence",
            "--min-credit-oracle-utilization-ratio=0.95",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("status=PASS", result.stdout)

    def test_rejects_credit_capacity_violation(self):
        item = record("credit", 4, "overload", 1)
        item["receiver"]["capacity_violation_total"] = 1
        result = self.run_verifier(item for item in [item])
        self.assertEqual(result.returncode, 1)
        self.assertIn("credit_capacity_violation", result.stdout)

    def test_rejects_missing_matrix_cell(self):
        result = self.run_verifier(
            [record("credit", 1, "normal", 1)],
            "--require-modes=fixed,credit",
            "--require-senders=1",
            "--require-conditions=normal",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing:fixed:1:normal", result.stdout)

    def test_rejects_completion_mismatch_and_invalid_release(self):
        item = record("fixed", 4, "overload", 1)
        item["completed"] = 9
        item["receiver"]["invalid_release_total"] = 1
        result = self.run_verifier([item])
        self.assertEqual(result.returncode, 1)
        self.assertIn("completion_mismatch", result.stdout)
        self.assertIn("invalid_release", result.stdout)


if __name__ == "__main__":
    unittest.main()
