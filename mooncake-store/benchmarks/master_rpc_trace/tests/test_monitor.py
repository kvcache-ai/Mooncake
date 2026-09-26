# Copyright 2026 Alibaba Cloud and its affiliates
# Licensed under the Apache License, Version 2.0.
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_benchmark import process_summary, traffic_summary  # noqa: E402


class MonitorTest(unittest.TestCase):
    def test_workload_rates_exclude_lifecycle_and_skipped_calls(self):
        def row(phase, sent, due, start, finish):
            return {
                "phase": phase,
                "rpc_sent": sent,
                "phase_origin_us": 500_000,
                "scheduled_us": due + 500_000,
                "start_us": start + 500_000,
                "finish_us": finish + 500_000,
                "planned_keys": 4,
                "key_status": {"skipped": 1 if sent else 4},
            }

        result = traffic_summary(
            [
                row("setup", True, 0, 0, 1),
                row("workload", True, 0, 1, 2_000_000),
                row("workload", False, 1, 2_000_000, 2_000_000),
                row("teardown", True, 0, 0, 1),
            ]
        )
        self.assertEqual(
            result["totals"],
            {
                "offered_calls": 2,
                "sent_calls": 1,
                "completed_calls": 1,
                "issued_keys": 3,
            },
        )
        self.assertEqual(result["mean_per_second"]["completed_calls"], 0.5)
        self.assertEqual(result["peak_client_calls_inflight"], 1)

    def test_process_stats_use_workload_window(self):
        rows = [
            {"monotonic_s": t, "master": {"cpu_seconds": cpu, "rss_bytes": rss}}
            for t, cpu, rss in ((0, 0, 999), (1, 10, 100), (2, 12, 200), (3, 99, 999))
        ]
        result = process_summary(rows, 1, 2)
        self.assertEqual(result["master"]["mean_cpu_cores"], 2)
        self.assertEqual(result["master"]["peak_rss_bytes"], 200)
        self.assertEqual(result["replayer"]["samples"], 0)


if __name__ == "__main__":
    unittest.main()
