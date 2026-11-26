#!/usr/bin/env python3
import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
GATE_PATH = BENCH_DIR / "master_trace_gate.py"
RUNNER_PATH = BENCH_DIR / "master_trace_runner.py"
PROFILE_DIR = BENCH_DIR / "profiles"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


RUNNER = load_module("master_trace_runner", RUNNER_PATH)


def sample_report(**overrides):
    report = {
        "benchmark": "master_trace",
        "model": "qwen3.5-9b",
        "profile_hash": "63018421c2224086",
        "seed": 42,
        "amplify": 2,
        "object_bytes": 24576,
        "key_bytes": 64,
        "keys_per_clone": 1073077,
        "speed": 0,
        "clone_spread_us": 1000,
        "segment_bytes": 64 * 1024**3,
        "allow_eviction": False,
        "planned_ops": 19380,
        "completed_ops": 19380,
        "skipped_ops": 0,
        "keys_planned": 2146154,
        "keys_total": 2146154,
        "failed_ops": 0,
        "errors": 0,
        "missing": 0,
        "evicted_misses": 0,
        "missed_deadlines": 0,
        "seconds": 15.0,
        "ops_per_second": 1292.0,
        "keys_per_second": 143076.0,
        "put_ops": 8894,
        "put_keys": 2146154,
        "get_ops": 488,
        "get_keys": 119080,
        "exist_ops": 9998,
        "exist_keys": 6936306,
        "exist_not_found": 2859112,
        "put_latency_us": {
            "count": 8894,
            "mean": 2150.0,
            "p50": 2100.0,
            "p95": 3000.0,
            "p99": 3400.0,
        },
        "get_latency_us": {
            "count": 488,
            "mean": 570.0,
            "p50": 540.0,
            "p95": 800.0,
            "p99": 880.0,
        },
        "exist_latency_us": {
            "count": 9998,
            "mean": 990.0,
            "p50": 370.0,
            "p95": 7100.0,
            "p99": 11800.0,
        },
    }
    report.update(overrides)
    return report


def run_gate(baseline, current, extra_args=()):
    with tempfile.TemporaryDirectory() as td:
        bp = Path(td) / "baseline.json"
        cp = Path(td) / "current.json"
        bp.write_text(json.dumps(baseline), encoding="utf-8")
        cp.write_text(json.dumps(current), encoding="utf-8")
        return subprocess.run(
            [
                sys.executable,
                str(GATE_PATH),
                "--baseline",
                str(bp),
                "--current",
                str(cp),
                *extra_args,
            ],
            capture_output=True,
            text=True,
            check=False,
        )


class TestProfiles(unittest.TestCase):
    def test_qwen_profile_valid(self):
        profile = json.loads(
            (PROFILE_DIR / "qwen3.5-9b.json").read_text(encoding="utf-8")
        )
        self.assertEqual(profile["format"], "mooncake-master-profile-v1")
        self.assertEqual(profile["model"], "qwen3.5-9b")
        self.assertTrue(profile["desensitized"])
        self.assertGreaterEqual(profile["key_bytes"], 24)
        self.assertGreater(profile["object_bytes"], 0)
        self.assertGreater(profile["keys_per_clone"], 0)
        self.assertLess(profile["read_hit_ratio"] + profile["prefix_probe_ratio"], 0.95)
        for field in ("put_batch_pmf", "get_batch_pmf", "prefix_probe_pmf"):
            self.assertTrue(profile[field])
            for entry in profile[field]:
                self.assertGreater(entry["size"], 0)
                self.assertGreater(entry["weight"], 0)


class TestGate(unittest.TestCase):
    def test_strict_pass_gates_all_three_ops(self):
        base = sample_report()
        proc = run_gate(base, copy.deepcopy(base))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertTrue(out["passed"])
        self.assertEqual(out["gated_p50_ops"], ["put", "get", "exist"])
        self.assertEqual(out["advisory_p50_ops"], [])
        self.assertEqual(out["baseline_ops_per_second"], 1292.0)
        self.assertEqual(out["op_metrics"]["put"]["baseline_p50_us"], 2100.0)

    def test_fails_on_rps_drop_or_p50_regression(self):
        base = sample_report()
        slow_rps = sample_report(ops_per_second=base["ops_per_second"] * 0.85)
        self.assertEqual(run_gate(base, slow_rps).returncode, 1)

        slow_get = sample_report()
        slow_get["get_latency_us"]["p50"] *= 1.15
        self.assertEqual(run_gate(base, slow_get).returncode, 1)

    def test_low_sample_op_is_advisory(self):
        base = sample_report(get_ops=5)
        cur = sample_report(get_ops=5)
        cur["get_latency_us"]["p50"] *= 1.5
        proc = run_gate(base, cur)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["gated_p50_ops"], ["put", "exist"])
        self.assertEqual(out["advisory_p50_ops"], ["get"])

    def test_rejects_plan_mismatch_and_malformed_fields(self):
        base = sample_report()
        self.assertEqual(run_gate(base, sample_report(seed=99)).returncode, 2)
        self.assertEqual(run_gate(base, sample_report(errors=False)).returncode, 2)
        bad_skip = sample_report(completed_ops=19370, skipped_ops=10)
        self.assertEqual(run_gate(base, bad_skip).returncode, 2)

    def test_eviction_mode_allows_skipped_gets_and_forbids_mixing(self):
        ev_base = sample_report(
            allow_eviction=True,
            completed_ops=19300,
            skipped_ops=80,
            get_ops=408,
            evicted_misses=12,
        )
        ev_cur = sample_report(
            allow_eviction=True,
            completed_ops=19290,
            skipped_ops=90,
            get_ops=398,
            evicted_misses=18,
        )
        proc = run_gate(ev_base, ev_cur)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(run_gate(sample_report(), ev_base).returncode, 2)


class TestRunnerHelpers(unittest.TestCase):
    def test_aggregate_preserves_min_completed_and_max_skipped(self):
        r0 = sample_report(
            allow_eviction=True,
            completed_ops=19300,
            skipped_ops=80,
            get_ops=408,
            ops_per_second=1200.0,
        )
        r1 = sample_report(
            allow_eviction=True,
            completed_ops=19280,
            skipped_ops=100,
            get_ops=388,
            ops_per_second=1300.0,
        )
        r2 = sample_report(
            allow_eviction=True,
            completed_ops=19290,
            skipped_ops=90,
            get_ops=398,
            ops_per_second=1250.0,
        )
        agg = RUNNER.aggregate([r0, r1, r2])
        self.assertEqual(agg["completed_ops"], 19280)
        self.assertEqual(agg["skipped_ops"], 100)
        self.assertEqual(agg["completed_ops"] + agg["skipped_ops"], 19380)
        self.assertEqual(agg["ops_per_second"], 1250.0)
        self.assertEqual(agg["get_ops"], 388)

    def test_validate_single_run_catches_short_run(self):
        bad = sample_report(completed_ops=1000)
        with self.assertRaises(RuntimeError):
            RUNNER.validate_single_run(bad, "base", 0)


if __name__ == "__main__":
    unittest.main()
