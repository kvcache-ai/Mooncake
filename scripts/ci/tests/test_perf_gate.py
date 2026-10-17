import argparse
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch


DIRECTORY = Path(__file__).resolve().parents[1] / "performance"
SPEC = importlib.util.spec_from_file_location("perf_gate", DIRECTORY / "perf_gate.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
BASE_SHA = "a" * 40
CANDIDATE_SHA = "b" * 40


class PerformanceGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.policy = gate.load_policy(DIRECTORY / "policy.json")
        self.policy_path = self.root / "policy.json"
        self.baseline = self.result(BASE_SHA)
        self.candidate = self.result(CANDIDATE_SHA)

    def result(self, sha):
        return {
            "schema_version": 1,
            "revision": sha,
            "environment": {"hostname": "test-host", "cpu_affinity": [0, 1]},
            "policy_sha256": gate.policy_digest(self.policy),
            "binary_sha256": "c" * 64,
            "errors": [],
            "cases": {
                name: {
                    key: {"unit": metric["unit"], "samples": [1000.0] * 5}
                    for key, metric in self.policy["metrics"].items()
                }
                for name in self.policy["cases"]
            },
        }

    def set_samples(self, metric, values, side=None):
        result = self.candidate if side is None else side
        result["cases"][next(iter(self.policy["cases"]))][metric]["samples"] = values

    def run_gate(self):
        for name, value in (
            ("baseline", self.baseline),
            ("candidate", self.candidate),
            ("policy", self.policy),
        ):
            (self.root / f"{name}.json").write_text(json.dumps(value))
        return subprocess.run(
            [
                sys.executable,
                str(DIRECTORY / "perf_gate.py"),
                "compare",
                "--policy",
                str(self.policy_path),
                "--baseline",
                str(self.root / "baseline.json"),
                "--candidate",
                str(self.root / "candidate.json"),
                "--baseline-sha",
                BASE_SHA,
                "--candidate-sha",
                CANDIDATE_SHA,
                "--output",
                str(self.root / "report"),
                "--summary",
                str(self.root / "job-summary.md"),
            ],
            text=True,
            capture_output=True,
            timeout=10,
        )

    def test_pass_publishes_all_metrics_and_artifacts(self):
        process = self.run_gate()
        self.assertEqual(process.returncode, 0, process.stderr)
        report = json.loads((self.root / "report/comparison.json").read_text())
        self.assertEqual(report["status"], "PASSED")
        self.assertEqual(len(report["rows"]), len(self.policy["cases"]) * 3)
        summary = (self.root / "job-summary.md").read_text()
        self.assertIn(BASE_SHA, summary)
        self.assertIn(CANDIDATE_SHA, summary)
        self.assertIn("ops/s", summary)
        self.assertNotIn("::error", process.stdout)

    def test_threshold_equality_passes_in_both_directions(self):
        self.set_samples("qps", [950.0] * 5)
        self.set_samples("p50_ns", [1050.0] * 5)
        self.set_samples("p99_ns", [1100.0] * 5)
        self.assertEqual(self.run_gate().returncode, 0)

    def test_one_regression_fails_despite_other_improvements(self):
        self.set_samples("qps", [949.0] * 5)
        self.set_samples("p50_ns", [500.0] * 5)
        self.set_samples("p99_ns", [500.0] * 5)
        process = self.run_gate()
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertIn("::error title=Performance Gate::", process.stdout)
        self.assertIn("regressed 5.10%", process.stdout)
        self.assertIn("FAILED", (self.root / "job-summary.md").read_text())
        self.assertTrue((self.root / "report/comparison.json").is_file())

    def test_latency_regression_fails(self):
        for metric, value in (("p50_ns", 1051), ("p99_ns", 1101)):
            with self.subTest(metric=metric):
                self.candidate = self.result(CANDIDATE_SHA)
                self.set_samples(metric, [value] * 5)
                self.assertEqual(self.run_gate().returncode, 1)

    def test_case_override_only_changes_its_own_threshold(self):
        name = next(iter(self.policy["cases"]))
        self.policy["cases"][name]["max_regression_percent"] = {"qps": 8}
        self.baseline = self.result(BASE_SHA)
        self.candidate = self.result(CANDIDATE_SHA)
        self.set_samples("qps", [925] * 5)
        self.assertEqual(self.run_gate().returncode, 0)
        self.set_samples("p99_ns", [1110] * 5)
        self.assertEqual(self.run_gate().returncode, 1)

    def test_negative_or_nonfinite_threshold_is_rejected(self):
        for value in (-0.1, float("nan"), True, 10**400):
            with self.subTest(value=value):
                self.policy["metrics"]["qps"]["max_regression_percent"] = value
                self.assertEqual(self.run_gate().returncode, 2)

    def test_improvements_and_one_outlier_pass(self):
        self.set_samples("qps", [1200, 1200, 1200, 1200, 100])
        self.set_samples("p50_ns", [800] * 5)
        self.set_samples("p99_ns", [800] * 5)
        self.assertEqual(self.run_gate().returncode, 0)

    def test_noise_fails_as_error_in_either_revision(self):
        for side in (self.baseline, self.candidate):
            with self.subTest(revision=side["revision"]):
                self.set_samples("qps", [700, 850, 1000, 1150, 1300], side)
                process = self.run_gate()
                self.assertEqual(process.returncode, 2)
                self.assertIn("unstable measurements", process.stdout)
                self.set_samples("qps", [1000] * 5, side)

    def test_invalid_results_fail_closed_with_report(self):
        name = next(iter(self.policy["cases"]))
        mutations = {
            "missing case": lambda r: r["cases"].pop(name),
            "extra case": lambda r: r["cases"].update(extra={}),
            "missing metric": lambda r: r["cases"][name].pop("qps"),
            "unit mismatch": lambda r: r["cases"][name]["qps"].update(unit="GB/s"),
            "missing sample": lambda r: r["cases"][name]["qps"]["samples"].pop(),
            "extra sample": lambda r: r["cases"][name]["qps"]["samples"].append(1000),
            "wrong SHA": lambda r: r.update(revision=BASE_SHA),
            "wrong version": lambda r: r.update(schema_version=2),
            "wrong policy": lambda r: r.update(policy_sha256="bad"),
            "wrong host": lambda r: r["environment"].update(hostname="another-host"),
            "benchmark error": lambda r: r["errors"].append(
                "unexpected allocation result"
            ),
        }
        for description, mutate in mutations.items():
            with self.subTest(description=description):
                self.candidate = self.result(CANDIDATE_SHA)
                mutate(self.candidate)
                process = self.run_gate()
                self.assertEqual(process.returncode, 2, process.stdout + process.stderr)
                self.assertIn(
                    "Performance Gate: ERROR",
                    (self.root / "report/summary.md").read_text(),
                )

    def test_nonfinite_zero_negative_and_boolean_samples_fail(self):
        for value in (float("nan"), float("inf"), -float("inf"), 0, -1, True, "1000"):
            with self.subTest(value=value):
                self.set_samples("qps", [1000, 1000, value, 1000, 1000])
                self.assertEqual(self.run_gate().returncode, 2)

    def test_missing_input_still_writes_summary(self):
        args = argparse.Namespace(
            policy=DIRECTORY / "policy.json",
            baseline=self.root / "missing.json",
            candidate=self.root / "missing-candidate.json",
            baseline_sha=BASE_SHA,
            candidate_sha=CANDIDATE_SHA,
            output=self.root / "report",
            summary=None,
        )
        with redirect_stdout(io.StringIO()):
            self.assertEqual(gate.report(args), 2)
        self.assertIn("missing.json", (self.root / "report/summary.md").read_text())

    def test_malformed_json_still_writes_summary(self):
        self.policy_path.write_text("{")
        args = argparse.Namespace(
            policy=self.policy_path,
            baseline="unused",
            candidate="unused",
            baseline_sha=BASE_SHA,
            candidate_sha=CANDIDATE_SHA,
            output=self.root / "report",
            summary=None,
        )
        with redirect_stdout(io.StringIO()):
            self.assertEqual(gate.report(args), 2)
        self.assertTrue((self.root / "report/comparison.json").is_file())

    def test_annotation_escapes_untrusted_line_breaks(self):
        output = io.StringIO()
        with redirect_stdout(output):
            gate.annotation("bad%\r\n::notice::forged annotation")
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertIn("%25%0D%0A", output.getvalue())

    def test_invalid_policy_is_rejected(self):
        for field, value in (
            ("repetitions", 0),
            ("repetitions", True),
            ("cpu_count", 0),
            ("timeout_seconds", -1),
            ("cases", {}),
            ("metrics", {}),
        ):
            with self.subTest(field=field):
                invalid = {**self.policy, field: value}
                self.policy_path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    gate.load_policy(self.policy_path)

    def test_malformed_policy_containers_preserve_error_reports(self):
        valid = copy.deepcopy(self.policy)
        paths = (
            (),
            ("arguments",),
            ("cases",),
            ("metrics",),
            ("cases", "offset-success-1t"),
            ("cases", "offset-success-1t", "max_regression_percent"),
            ("metrics", "qps"),
        )
        for path in paths:
            for invalid in (None, [], ["not-an-object"], "not-an-object", 1):
                with self.subTest(path=path, invalid=invalid):
                    self.policy = copy.deepcopy(valid)
                    if path:
                        parent = self.policy
                        for key in path[:-1]:
                            parent = parent[key]
                        parent[path[-1]] = invalid
                    else:
                        self.policy = invalid
                    process = self.run_gate()
                    self.assertEqual(process.returncode, 2, process.stderr)
                    comparison = json.loads(
                        (self.root / "report/comparison.json").read_text()
                    )
                    self.assertEqual(comparison["status"], "ERROR")
                    self.assertIn(
                        "Performance Gate: ERROR",
                        (self.root / "report/summary.md").read_text(),
                    )

    def fixture_binary(self, side, behavior="ok"):
        path = self.root / side
        path.write_text(f"""#!{sys.executable}
import csv
import os
import sys
import time
from pathlib import Path
args = dict(value[2:].split("=", 1) for value in sys.argv[1:])
with Path(os.environ["PERF_TEST_ORDER"]).open("a") as order:
    order.write({side!r} + "\\n")
if {behavior!r} == "timeout":
    time.sleep(10)
if {behavior!r} == "exit":
    sys.exit(7)
row = {{
    "layer": args["layer"], "scenario": "successful_allocate_free",
    "threads": args["threads"], "iterations_per_thread": args["iterations"],
    "operations": int(args["threads"]) * int(args["iterations"]),
    "throughput_seconds": 1, "qps": 1000, "p50_ns": 100, "p99_ns": 200,
    "throughput_unexpected_results": int({behavior!r} == "incorrect"),
    "latency_unexpected_results": 0,
}}
if {behavior!r} == "malformed":
    print("not a benchmark result")
else:
    writer = csv.DictWriter(sys.stdout, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
""")
        path.chmod(0o755)
        return path

    def collect_fixture(self, behavior="ok", budget=900):
        self.policy["cases"] = {
            "offset-success-1t": self.policy["cases"]["offset-success-1t"]
        }
        self.policy["arguments"]["iterations"] = 1000
        if behavior == "timeout":
            self.policy["timeout_seconds"] = 1
        self.policy_path.write_text(json.dumps(self.policy))
        args = argparse.Namespace(
            policy=self.policy_path,
            baseline=self.fixture_binary("baseline"),
            candidate=self.fixture_binary("candidate", behavior),
            baseline_sha=BASE_SHA,
            candidate_sha=CANDIDATE_SHA,
            output=self.root / "samples",
            collection_timeout_seconds=budget,
        )
        with patch.object(
            gate, "environment", return_value={"hostname": "fixture"}
        ), patch.dict(
            os.environ, {"PERF_TEST_ORDER": str(self.root / "order")}
        ), redirect_stdout(io.StringIO()):
            code = gate.collect(args)
        return code, args

    def test_collection_alternates_pairs_and_preserves_raw_results(self):
        code, args = self.collect_fixture()
        self.assertEqual(code, 0)
        self.assertEqual(
            (self.root / "order").read_text().splitlines(),
            [
                "baseline",
                "candidate",
                "candidate",
                "baseline",
                "baseline",
                "candidate",
                "candidate",
                "baseline",
                "baseline",
                "candidate",
            ],
        )
        baseline = json.loads((args.output / "baseline.json").read_text())
        candidate = json.loads((args.output / "candidate.json").read_text())
        self.assertEqual(len(list(args.output.glob("*.csv"))), 10)
        self.assertEqual(len(list(args.output.glob("*.log"))), 10)
        rows = gate.compare(self.policy, baseline, candidate, BASE_SHA, CANDIDATE_SHA)
        self.assertTrue(all(row["status"] == "PASS" for row in rows))
        with self.assertRaisesRegex(ValueError, "already contains results"):
            gate.collect(args)

    def test_process_failures_and_corrupt_output_are_not_passes(self):
        for behavior in ("exit", "incorrect", "malformed", "timeout"):
            with self.subTest(behavior=behavior):
                with tempfile.TemporaryDirectory() as directory:
                    self.root = Path(directory)
                    self.policy_path = self.root / "policy.json"
                    code, args = self.collect_fixture(behavior)
                    self.assertEqual(code, 2)
                    candidate = json.loads((args.output / "candidate.json").read_text())
                    self.assertEqual(len(candidate["errors"]), 1)
                    self.assertEqual(candidate["cases"], {})
                    self.assertEqual(
                        (self.root / "order").read_text().splitlines(),
                        ["baseline", "candidate"],
                    )
                    self.assertEqual(len(list(args.output.glob("*.csv"))), 2)
                    args.baseline, args.candidate = (
                        args.output / "baseline.json",
                        args.output / "candidate.json",
                    )
                    args.summary = None
                    with redirect_stdout(io.StringIO()):
                        self.assertEqual(gate.report(args), 2)
                    self.assertEqual(
                        json.loads((args.output / "comparison.json").read_text())[
                            "status"
                        ],
                        "ERROR",
                    )

    def test_collection_budget_bounds_total_runtime(self):
        start = time.monotonic()
        # The first baseline consumes the budget; no subsequent sample may start.
        with patch.object(gate.time, "monotonic", side_effect=[0, 0, 2]):
            code, args = self.collect_fixture(budget=1)
        self.assertEqual(code, 2)
        self.assertLess(time.monotonic() - start, 5)
        self.assertEqual((self.root / "order").read_text().splitlines(), ["baseline"])
        candidate = json.loads((args.output / "candidate.json").read_text())
        self.assertIn("collection time budget exhausted", candidate["errors"][0])


if __name__ == "__main__":
    unittest.main()
