#!/usr/bin/env python3
"""Unit tests for the HugeTLB sizing helper."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_hicache_hugepage_requirements as helper  # noqa: E402


class ParseSizeTest(unittest.TestCase):
    def test_parses_human_readable_units(self) -> None:
        self.assertEqual(helper.parse_size("8gb"), 8 * 1024**3)
        self.assertEqual(helper.parse_size("2MB"), 2 * 1024**2)
        self.assertEqual(helper.parse_size("1.5tb"), int(1.5 * 1024**4))

    def test_rejects_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            helper.parse_size("eight-gigabytes")


class EvaluateBudgetTest(unittest.TestCase):
    def test_detects_baseline_failure(self) -> None:
        summary = helper.evaluate_budget(
            tp_size=1,
            hicache_size_bytes=80 * 1024**3,
            global_segment_size_bytes=48 * 1024**3,
            arena_pool_size_bytes=0,
            available_bytes=96 * 1024**3,
        )
        self.assertEqual(summary.status, "insufficient_for_baseline")
        self.assertEqual(summary.exit_code, 2)

    def test_detects_partial_arena_budget(self) -> None:
        summary = helper.evaluate_budget(
            tp_size=4,
            hicache_size_bytes=64 * 1024**3,
            global_segment_size_bytes=8 * 1024**3,
            arena_pool_size_bytes=56 * 1024**3,
            available_bytes=320 * 1024**3,
        )
        self.assertEqual(summary.status, "baseline_fits_arena_may_fallback")
        self.assertEqual(summary.exit_code, 1)

    def test_detects_clean_arena_budget(self) -> None:
        summary = helper.evaluate_budget(
            tp_size=4,
            hicache_size_bytes=64 * 1024**3,
            global_segment_size_bytes=8 * 1024**3,
            arena_pool_size_bytes=56 * 1024**3,
            available_bytes=512 * 1024**3,
        )
        self.assertEqual(summary.status, "clean_arena_budget_available")
        self.assertEqual(summary.exit_code, 0)

    def test_supports_planning_only_mode(self) -> None:
        summary = helper.evaluate_budget(
            tp_size=2,
            hicache_size_bytes=56 * 1024**3,
            global_segment_size_bytes=8 * 1024**3,
            arena_pool_size_bytes=56 * 1024**3,
            available_bytes=None,
        )
        self.assertEqual(summary.status, "planning_only")
        self.assertEqual(summary.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
