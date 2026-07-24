import importlib.util
import json
import os
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


store_module = types.ModuleType("mooncake.store")
store_module.MooncakeDistributedStore = object
store_module.ReplicateConfig = type("ReplicateConfig", (), {})
store_module.get_alloc_func_addr = lambda: None
store_module.get_free_func_addr = lambda: None
sys.modules.setdefault("mooncake", types.ModuleType("mooncake"))
sys.modules["mooncake.store"] = store_module

MODULE_PATH = Path(__file__).with_name("store_kv_bench.py")
SPEC = importlib.util.spec_from_file_location("store_kv_bench_under_test", MODULE_PATH)
bench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bench
SPEC.loader.exec_module(bench)


class MetadataWorkloadModelTest(unittest.TestCase):
    def test_percentages_and_lane_choices_are_deterministic(self):
        weights = {"put": 40, "get": 30, "exist": 20, "remove": 10}
        bench.validate_operation_percentages(weights)
        first = bench.choose_metadata_operations(7, 3, 20, weights)
        second = bench.choose_metadata_operations(7, 3, 20, weights)
        self.assertEqual(first, second)
        self.assertNotEqual(first, bench.choose_metadata_operations(7, 4, 20, weights))
        with self.assertRaisesRegex(ValueError, "sum to 100"):
            bench.validate_operation_percentages({**weights, "remove": 9})

    def test_model_tracks_successful_transitions(self):
        model = bench.MetadataLaneModel()
        self.assertFalse(model.is_present(4))
        model.record(4, "put", True)
        self.assertTrue(model.is_present(4))
        model.record(4, "remove", False)
        self.assertTrue(model.is_present(4))
        model.record(4, "remove", True)
        self.assertFalse(model.is_present(4))


class OutputTest(unittest.TestCase):
    def test_journal_replay_summary_and_sampling(self):
        root = Path(f"/tmp/mooncake-kv-bench-test-{os.getpid()}")
        root.mkdir(parents=True, exist_ok=True)
        records = [
            {
                "schema_version": 1,
                "op_id": 1,
                "lane": 0,
                "op": "put",
                "object_id": 9,
                "key": "k9",
                "invoke_ns": 10,
                "return_ns": 20,
                "result": 0,
            }
        ]
        journal = root / "journal.jsonl"
        bench.write_jsonl(journal, records)
        self.assertEqual(bench.read_replay(journal), records)

        summary = root / "summary.json"
        bench.write_json_atomic(summary, {"ok": True, "requests": 1})
        self.assertEqual(json.loads(summary.read_text()), {"ok": True, "requests": 1})

        sampler = bench.LatencySampler(3)
        for value in range(10):
            sampler.add(float(value))
        self.assertEqual(sampler.samples, [0.0, 3.0, 6.0, 9.0])
        self.assertEqual(sampler.seen, 10)


class StoreSessionMetadataTest(unittest.TestCase):
    def test_put_get_exist_remove(self):
        class FakeStore:
            def __init__(self):
                self.values = {}

            def put(self, key, value, _config):
                self.values[key] = value
                return 0

            def get(self, key):
                return self.values.get(key, b"")

            def isExist(self, key):
                return int(key in self.values)

            def remove(self, key):
                return 0 if self.values.pop(key, None) is not None else -1

        args = SimpleNamespace(
            memory_replica_num=1,
            nof_replica_num=0,
            key_prefix="metadata",
            key_size=24,
            value_size=512,
            io_api="plain",
            batch_size=1,
        )
        store = FakeStore()
        session = bench.StoreSession(args, 0, bench.PayloadFactory(512, b"x"), store)

        for operation, expected_before, expected_after in [
            ("put", False, True),
            ("exist", True, True),
            ("get", True, True),
            ("remove", True, False),
            ("exist", False, False),
        ]:
            result, actual_present, _ = session.metadata_operation(
                operation, 1, expected_before
            )
            self.assertTrue(result.request_ok, operation)
            self.assertEqual(actual_present, expected_after, operation)

    def test_metadata_smoke_writes_replayable_outputs(self):
        class FakeStore:
            def __init__(self):
                self.values = {}

            def put(self, key, value, _config):
                self.values[key] = value
                return 0

            def get(self, key):
                return self.values.get(key, b"")

            def isExist(self, key):
                return int(key in self.values)

            def remove(self, key):
                return 0 if self.values.pop(key, None) is not None else -1

        output_dir = Path(f"/tmp/mooncake-kv-smoke-test-{os.getpid()}")
        args = bench.build_parser().parse_args(
            [
                "--scenario=metadata_smoke",
                "--nr-objects=1",
                "--value-size=512",
                "--key-size=24",
                "--pattern=x",
                f"--output-dir={output_dir}",
                "--journal=all",
            ]
        )
        runner = bench.BenchmarkRunner(args)
        runner._sessions = [
            bench.StoreSession(args, 0, runner.payload_factory, FakeStore())
        ]
        phases = runner.run()
        runner.write_outputs(phases)

        self.assertEqual(phases[0].requests, 5)
        self.assertEqual(phases[0].failed_kvs, 0)
        records = bench.read_replay(output_dir / "journal.jsonl")
        self.assertEqual(
            [record["op"] for record in records],
            ["put", "exist", "get", "remove", "exist"],
        )
        summary = json.loads((output_dir / "summary.json").read_text())
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["journal_records"], 5)


if __name__ == "__main__":
    unittest.main()
