import ctypes
import importlib.util
import json
import os
import sys
import threading
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch


class FakeBufferLease:
    def __init__(self, size):
        self._buffer = (ctypes.c_ubyte * size)()
        self.ptr = ctypes.addressof(self._buffer)
        self.released = False

    def release(self):
        self.released = True


class FakeBufferPool:
    instances = []
    acquire_error = None

    def __init__(self, store, **kwargs):
        self.store = store
        self.kwargs = kwargs
        self.lease = None
        self.closed = False
        self.instances.append(self)

    def acquire(self, size, block=True):
        if self.acquire_error is not None:
            raise self.acquire_error
        self.lease = FakeBufferLease(size)
        self.acquire_args = (size, block)
        return self.lease

    def close(self):
        self.closed = True


store_module = types.ModuleType("mooncake.store")
store_module.MooncakeDistributedStore = object
store_module.ReplicateConfig = type(
    "ReplicateConfig",
    (),
    {"replica_num": 1, "nof_replica_num": 0, "dfs_replica_num": 0},
)
store_module.BufferPool = FakeBufferPool
sys.modules.setdefault("mooncake", types.ModuleType("mooncake"))
sys.modules["mooncake.store"] = store_module

MODULE_PATH = Path(__file__).with_name("store_kv_bench.py")
SPEC = importlib.util.spec_from_file_location("store_kv_bench_under_test", MODULE_PATH)
bench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bench
SPEC.loader.exec_module(bench)


class StoreModuleCompatibilityTest(unittest.TestCase):
    def test_import_does_not_require_buffer_pool(self):
        del store_module.BufferPool
        module_name = "store_kv_bench_without_buffer_pool"
        spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            store_module.BufferPool = FakeBufferPool
            sys.modules.pop(module_name, None)


class ReplicaConfigTest(unittest.TestCase):
    def test_rejects_all_replica_counts_zero(self):
        args = bench.build_parser().parse_args(
            [
                "--scenario=read_perf",
                "--memory-replica-num=0",
                "--nof-replica-num=0",
                "--dfs-replica-num=0",
            ]
        )
        with self.assertRaisesRegex(ValueError, "at least one.*replica"):
            bench.BenchmarkRunner(args)

    def test_dfs_only_preparation_and_reads(self):
        payload = b"x" * 512

        def read_into(keys, ptrs, sizes):
            for ptr in ptrs:
                ctypes.memmove(ptr, payload, len(payload))
            return [len(payload)] * len(keys)

        for scenario in ("read_perf", "verify_write", "mixed_rw"):
            for io_api in ("plain", "zcopy"):
                for batch_size in (1, 2):
                    with self.subTest(
                        scenario=scenario, io_api=io_api, batch_size=batch_size
                    ):
                        args = bench.build_parser().parse_args(
                            [
                                f"--scenario={scenario}",
                                f"--io-api={io_api}",
                                f"--batch-size={batch_size}",
                                "--nr-objects=3",
                                "--value-size=512",
                                "--memory-replica-num=0",
                                "--nof-replica-num=0",
                                "--dfs-replica-num=1",
                                "--enable-ssd-offload",
                                "--verify",
                                "--pattern=x",
                            ]
                            + (["--runtime=1"] if scenario == "mixed_rw" else [])
                        )
                        store = Mock()
                        store.setup.return_value = 0
                        store.put.return_value = 0
                        store.put_batch.return_value = 0
                        store.put_from.return_value = 0
                        store.batch_put_from.side_effect = (
                            lambda keys, ptrs, sizes, config: [0] * len(keys)
                        )
                        store.get.return_value = payload
                        store.get_batch.side_effect = lambda keys: [payload] * len(keys)
                        store.get_into.side_effect = lambda key, ptr, size: read_into(
                            [key], [ptr], [size]
                        )[0]
                        store.batch_get_into.side_effect = read_into
                        runner = bench.BenchmarkRunner(args)
                        fake_module = types.SimpleNamespace(
                            MooncakeDistributedStore=Mock(return_value=store),
                            ReplicateConfig=store_module.ReplicateConfig,
                            BufferPool=FakeBufferPool,
                        )
                        with patch.object(
                            bench, "load_store_module", return_value=fake_module
                        ):
                            try:
                                phases = runner.run()
                            finally:
                                runner.close()

                        self.assertEqual(phases[0].successful_kvs, 3)
                        self.assertGreater(phases[1].successful_kvs, 0)
                        if scenario != "mixed_rw":
                            self.assertEqual(phases[1].successful_kvs, 3)
                        write_api, read_api = {
                            ("plain", 1): ("put", "get"),
                            ("plain", 2): ("put_batch", "get_batch"),
                            ("zcopy", 1): ("put_from", "get_into"),
                            ("zcopy", 2): ("batch_put_from", "batch_get_into"),
                        }[io_api, batch_size]
                        getattr(store, read_api).assert_called()
                        for phase in phases:
                            self.assertEqual(phase.failed_kvs, 0)
                            self.assertEqual(phase.verify_failures, 0)
                        writes = getattr(store, write_api).call_args_list
                        self.assertTrue(writes)
                        for call in writes:
                            config = call.kwargs["config"]
                            self.assertEqual(
                                (
                                    config.replica_num,
                                    config.nof_replica_num,
                                    config.dfs_replica_num,
                                ),
                                (0, 0, 1),
                            )
                        store.setup.assert_called_once()
                        self.assertTrue(
                            store.setup.call_args.kwargs["enable_ssd_offload"]
                        )
                        store.close.assert_called_once()


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


class ReadMixTest(unittest.TestCase):
    def _args(self, *extra):
        return bench.build_parser().parse_args(
            [
                "--scenario=read_mix",
                "--io-api=zcopy",
                "--read-hit-pct=60",
                "--value-size=512",
                "--verify",
                "--pattern=x",
                *extra,
            ]
        )

    def test_plan_is_seeded_and_reserves_disjoint_missing_keys(self):
        for hit_pct, expected_hits in ((0, 0), (60, 6), (100, 10)):
            with self.subTest(hit_pct=hit_pct):
                plan = bench.build_read_mix_plan(20, 4, 10, hit_pct, 17)
                self.assertEqual(
                    plan, bench.build_read_mix_plan(20, 4, 10, hit_pct, 17)
                )
                present = [key for key, hit in plan if hit]
                missing = [key for key, hit in plan if not hit]
                self.assertEqual(len(present), expected_hits)
                self.assertTrue(set(present) <= set(range(20, 24)))
                self.assertEqual(set(missing), set(range(24, 34 - expected_hits)))
                self.assertEqual(len(plan), 10)
        self.assertEqual(
            sum(hit for _, hit in bench.build_read_mix_plan(20, 4, 7, 60, 17)), 4
        )

    def test_strict_per_key_status_distinguishes_miss_and_transfer_error(self):
        args = self._args("--batch-size=7")
        store, zcopy = Mock(), Mock()
        payload = bench.PayloadFactory(512, b"x")
        zcopy.read_bytes.side_effect = [payload.build(0), b"y" * 512]
        session = bench.StoreSession(
            args, 0, payload, store, SimpleNamespace(replica_num=1), zcopy
        )
        plan = [
            (0, True),
            (1, False),
            (2, False),
            (3, True),
            (4, True),
            (5, False),
            (6, True),
        ]
        with patch.object(
            session,
            "_get_lengths_zcopy",
            return_value=[512, -704, -800, -704, 511, 512, 512],
        ):
            result = session.get_expected_ids(plan, verify=True)
        self.assertEqual((result.expected_hits, result.expected_misses), (4, 3))
        self.assertEqual((result.matched_hits, result.matched_misses), (1, 1))
        self.assertEqual((result.kv_successes, result.kv_failures), (2, 5))
        self.assertEqual(result.misses, 2)
        self.assertEqual(result.bytes_processed, 512)
        self.assertEqual(result.verify_failures, 2)
        self.assertEqual(
            result.error_counts,
            {
                -800: 1,
                "UNEXPECTED_MISS": 1,
                "VERIFY_SIZE_MISMATCH": 1,
                "UNEXPECTED_HIT": 1,
                "VERIFY_FAIL": 1,
            },
        )
        store.is_exist.assert_not_called()
        self.assertEqual(
            [call.args for call in zcopy.read_bytes.call_args_list],
            [(0, 512), (6, 512)],
        )

    def test_short_status_vector_does_not_consume_destination_bytes(self):
        args = self._args("--batch-size=2")
        store, zcopy = Mock(), Mock()
        session = bench.StoreSession(
            args,
            0,
            bench.PayloadFactory(512, b"x"),
            store,
            SimpleNamespace(replica_num=1),
            zcopy,
        )
        with patch.object(session, "_get_lengths_zcopy", return_value=[512]):
            result = session.get_expected_ids([(0, True), (1, False)], verify=True)
        self.assertFalse(result.request_ok)
        self.assertEqual(result.kv_failures, 2)
        self.assertEqual(result.error_counts, {"READ_RESULT_COUNT_MISMATCH": 2})
        zcopy.read_bytes.assert_not_called()

    def _fake_store(self, contents):
        store = Mock()
        store.setup.return_value = 0
        lock = threading.Lock()

        def put(keys, ptrs, sizes, config):
            with lock:
                for key, ptr, size in zip(keys, ptrs, sizes):
                    contents[key] = ctypes.string_at(ptr, size)
            return [0] * len(keys)

        def get(keys, ptrs, sizes):
            with lock:
                results = []
                for key, ptr, size in zip(keys, ptrs, sizes):
                    if key not in contents:
                        results.append(-704)
                        continue
                    value = contents[key]
                    self.assertLessEqual(len(value), size)
                    ctypes.memmove(ptr, value, len(value))
                    results.append(len(value))
                return results

        store.put_from.side_effect = lambda key, ptr, size, config: put(
            [key], [ptr], [size], config
        )[0]
        store.batch_put_from.side_effect = put
        store.get_into.side_effect = lambda key, ptr, size: get([key], [ptr], [size])[0]
        store.batch_get_into.side_effect = get
        return store

    def _run_cli(self, store, extra):
        with TemporaryDirectory() as directory:
            argv = [
                "store_kv_bench.py",
                "--scenario=read_mix",
                "--io-api=zcopy",
                "--nr-objects=10",
                "--prepare-objects=4",
                "--object-id-start=20",
                "--value-size=512",
                "--verify",
                "--pattern=x",
                "--numjobs=2",
                "--output-dir",
                directory,
                *extra,
            ]
            fake_module = types.SimpleNamespace(
                MooncakeDistributedStore=Mock(return_value=store),
                ReplicateConfig=store_module.ReplicateConfig,
                BufferPool=FakeBufferPool,
            )
            with (
                patch.object(bench, "load_store_module", return_value=fake_module),
                patch.object(sys, "argv", argv),
            ):
                code = bench.main()
            return code, json.loads((Path(directory) / "summary.json").read_text())

    def test_cli_runs_all_hit_all_miss_and_mixed_single_or_batched(self):
        for hit_pct, batch_size, external in (
            (100, 1, False),
            (0, 3, False),
            (60, 3, False),
            (60, 3, True),
        ):
            with self.subTest(
                hit_pct=hit_pct, batch_size=batch_size, external=external
            ):
                contents = {}
                if external:
                    contents = {
                        bench.make_key("kvbench", 20, object_id): b"x" * 512
                        for object_id in range(20, 24)
                    }
                store = self._fake_store(contents)
                extra = [f"--read-hit-pct={hit_pct}", f"--batch-size={batch_size}"]
                if external:
                    extra.append("--prepare-mode=none")
                code, summary = self._run_cli(store, extra)
                stats = summary["phases"]["read_mix"]
                self.assertEqual(code, 0)
                self.assertTrue(summary["ok"])
                expected_hits = hit_pct // 10
                self.assertEqual(stats["expected_hits"], expected_hits)
                self.assertEqual(stats["expected_misses"], 10 - expected_hits)
                self.assertEqual(stats["matched_hits"], expected_hits)
                self.assertEqual(stats["matched_misses"], 10 - expected_hits)
                self.assertEqual(stats["kvs"], 10)
                self.assertEqual(stats["successful_kvs"], 10)
                self.assertEqual(stats["failed_kvs"], 0)
                self.assertEqual(stats["misses"], 10 - expected_hits)
                self.assertEqual(stats["bytes"], expected_hits * 512)
                self.assertEqual(stats["error_counts"], {})
                store.is_exist.assert_not_called()
                if external:
                    self.assertNotIn("prepare_write", summary["phases"])
                    store.put_from.assert_not_called()
                    store.batch_put_from.assert_not_called()
                else:
                    self.assertEqual(
                        summary["phases"]["prepare_write"]["successful_kvs"], 4
                    )

    def test_cli_fails_a_transport_error_on_an_expected_missing_key(self):
        store = self._fake_store({})
        store.batch_get_into.side_effect = lambda keys, ptrs, sizes: [-800] * len(keys)
        code, summary = self._run_cli(
            store, ["--read-hit-pct=0", "--batch-size=3", "--prepare-mode=none"]
        )
        self.assertEqual(code, 22)
        self.assertFalse(summary["ok"])
        stats = summary["phases"]["read_mix"]
        self.assertEqual(stats["matched_misses"], 0)
        self.assertEqual(stats["misses"], 0)
        self.assertEqual(stats["failed_kvs"], 10)
        self.assertEqual(stats["error_counts"], {"-800": 10})

    def test_rejects_backends_without_unambiguous_status_and_timed_mix(self):
        for extra in (
            "--store-backend=rs",
            "--io-api=plain",
            "--runtime=1",
            "--read-hit-pct=101",
        ):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                bench.BenchmarkRunner(self._args(extra))


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

            def put(self, key, value, config=None):
                self.values[key] = value
                return 0

            def get(self, key):
                return self.values.get(key, b"")

            def is_exist(self, key):
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
        session = bench.StoreSession(
            args,
            0,
            bench.PayloadFactory(512, b"x"),
            store,
            SimpleNamespace(replica_num=1),
        )

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

            def put(self, key, value, config=None):
                self.values[key] = value
                return 0

            def get(self, key):
                return self.values.get(key, b"")

            def is_exist(self, key):
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
            bench.StoreSession(
                args,
                0,
                runner.payload_factory,
                FakeStore(),
                SimpleNamespace(replica_num=1),
            )
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


class RemoveExistenceErrorTest(unittest.TestCase):
    def test_remove_requires_a_successful_absence_check(self):
        args = bench.build_parser().parse_args(["--scenario=mixed_metadata"])
        store = Mock()
        session = bench.StoreSession(
            args,
            0,
            bench.PayloadFactory(4096, b"x"),
            store,
            SimpleNamespace(replica_num=1),
        )
        for expected, removed, exists, ok in (
            (True, 0, -1, False),
            (False, -1, -1, False),
            (False, 0, -1, False),
            (True, 0, 0, True),
            (False, -1, 0, True),
            (True, 0, 1, False),
        ):
            with self.subTest(expected=expected, removed=removed, exists=exists):
                store.remove.return_value = removed
                store.is_exist.return_value = exists
                result, _, code = session.metadata_operation("remove", 1, expected)
                self.assertEqual(result.request_ok, ok)
                if exists < 0:
                    self.assertEqual(code, exists)
                    self.assertEqual(result.error_counts[exists], 1)

    def test_cli_records_failed_probe_in_summary_and_journal(self):
        store = Mock()
        store.setup.return_value = 0
        store.remove.return_value = -1
        store.is_exist.return_value = -1
        with TemporaryDirectory() as directory:
            argv = [
                "store_kv_bench.py",
                "--scenario=mixed_metadata",
                "--nr-objects=1",
                "--put-pct=0",
                "--get-pct=0",
                "--exist-pct=0",
                "--remove-pct=100",
                "--output-dir",
                directory,
            ]
            fake_module = types.SimpleNamespace(
                MooncakeDistributedStore=Mock(return_value=store),
                ReplicateConfig=store_module.ReplicateConfig,
            )
            with (
                patch.object(bench, "load_store_module", return_value=fake_module),
                patch.object(sys, "argv", argv),
            ):
                code = bench.main()
            summary = json.loads((Path(directory) / "summary.json").read_text())
            journal = bench.read_replay(Path(directory) / "journal.jsonl")
            self.assertEqual(code, 22)
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["overall"]["failed_kvs"], 1)
            self.assertEqual(len(journal), 1)
            self.assertFalse(journal[0]["ok"])
            self.assertEqual(journal[0]["result"], -1)
            store.close.assert_called_once()


class WorkerFailureTest(unittest.TestCase):
    def setUp(self):
        args = bench.build_parser().parse_args(["--scenario=metadata_smoke"])
        self.runner = bench.BenchmarkRunner(args)
        self.runner._sessions = [Mock()]
        hook = patch.object(threading, "excepthook")
        hook.start()
        self.addCleanup(hook.stop)

    def test_worker_and_builder_errors_reach_caller(self):
        for during_build in (False, True):
            for error in (ValueError("store failed"), SystemExit(0)):
                with self.subTest(during_build=during_build, error=type(error)):

                    def build(session, lane):
                        if during_build:
                            raise error

                        def work(stats):
                            raise error

                        return work

                    with self.assertRaisesRegex(
                        RuntimeError, "probe.*lane 0"
                    ) as caught:
                        self.runner._run_threads("probe", build)
                    self.assertIs(caught.exception.__cause__, error)

    def test_failure_joins_every_lane_before_returning(self):
        self.runner.lane_count = 2
        self.runner._sessions = [Mock(), Mock()]
        joined = []
        finished = threading.Event()
        original_join = threading.Thread.join

        def join(thread, *args, **kwargs):
            original_join(thread, *args, **kwargs)
            joined.append(thread.name)

        def build(session, lane):
            def work(stats):
                if lane == 0:
                    raise RuntimeError("first lane failed")
                finished.set()
                stats.requests = 1

            return work

        with patch.object(threading.Thread, "join", join):
            with self.assertRaises(RuntimeError):
                self.runner._run_threads("probe", build)
        self.assertEqual(joined, ["probe-lane0", "probe-lane1"])
        self.assertTrue(finished.is_set())

    def test_healthy_lanes_keep_their_statistics(self):
        self.runner.lane_count = 2
        self.runner._sessions = [Mock(), Mock()]

        def build(session, lane):
            def work(stats):
                stats.requests = lane + 1
                stats.successful_requests = lane + 1

            return work

        stats = self.runner._run_threads("probe", build)
        self.assertEqual(stats.requests, 3)
        self.assertEqual(stats.successful_requests, 3)
        self.assertEqual(stats.failed_requests, 0)

    def test_cli_fails_without_writing_success_and_closes_store(self):
        for scenario, operation in (("metadata_smoke", "put"), ("verify_write", "get")):
            with self.subTest(scenario=scenario), TemporaryDirectory() as directory:
                store = Mock()
                store.setup.return_value = 0
                store.put.return_value = 0
                getattr(store, operation).side_effect = RuntimeError("store failed")
                argv = [
                    "store_kv_bench.py",
                    "--scenario",
                    scenario,
                    "--nr-objects=1",
                    "--output-dir",
                    directory,
                ]
                fake_module = types.SimpleNamespace(
                    MooncakeDistributedStore=Mock(return_value=store),
                    ReplicateConfig=store_module.ReplicateConfig,
                )
                with (
                    patch.object(bench, "load_store_module", return_value=fake_module),
                    patch.object(sys, "argv", argv),
                    patch.object(bench.LOG, "exception") as log_error,
                ):
                    self.assertEqual(bench.main(), 1)
                log_error.assert_called_once()
                store.close.assert_called_once()
                self.assertFalse((Path(directory) / "summary.json").exists())


class StoreBackendTest(unittest.TestCase):
    def _args(self, extra):
        return bench.build_parser().parse_args(
            [
                "--scenario=write_perf",
                "--nr-objects=1",
                "--value-size=512",
                "--key-size=24",
                *extra,
            ]
        )

    def _fake_module(self, store):
        return types.SimpleNamespace(
            MooncakeDistributedStore=Mock(return_value=store),
            ReplicateConfig=store_module.ReplicateConfig,
        )

    def test_cpp_setup_uses_master_and_metadata_server(self):
        store = Mock()
        store.setup.return_value = 0
        args = self._args(["--store-backend=cpp"])
        with patch.object(
            bench, "load_store_module", return_value=self._fake_module(store)
        ):
            bench.StoreRuntime(args, lane_count=1)
        call = store.setup.call_args
        self.assertEqual(
            call.args,
            (
                "127.0.0.1:50071",
                "http://127.0.0.1:8080/metadata",
                64 * 1024 * 1024,
                32 * 1024 * 1024,
                "tcp",
                "",
                "127.0.0.1:50051",
            ),
        )
        self.assertEqual(call.kwargs, {"enable_ssd_offload": False})
        summary = bench.summarize_backend(args)
        self.assertEqual(summary["store"], "cpp")
        self.assertEqual(summary["routing"], "master")

    def test_rs_setup_uses_store_rs_arguments(self):
        store = Mock()
        store.setup.return_value = 0
        args = self._args(
            [
                "--store-backend=rs",
                "--rs-metadata-url=redis://127.0.0.1:6380",
                "--rs-keyspace=mc/test",
                "--rs-route-control=metadata_only",
            ]
        )
        with patch.object(
            bench, "load_store_module", return_value=self._fake_module(store)
        ):
            bench.StoreRuntime(args, lane_count=1)
        call = store.setup.call_args
        self.assertEqual(
            call.args,
            (
                "127.0.0.1:50071",
                "P2PHANDSHAKE",
                64 * 1024 * 1024,
                32 * 1024 * 1024,
                "tcp",
                "",
                "redis://127.0.0.1:6380",
            ),
        )
        self.assertEqual(
            call.kwargs,
            {
                "tenant": "default",
                "replica_count": 1,
                "keyspace": "mc/test",
                "route_control": "metadata_only",
            },
        )
        summary = bench.summarize_backend(args)
        self.assertEqual(summary["store"], "rs")
        self.assertEqual(summary["routing"], "metadata_only")
        self.assertEqual(summary["keyspace"], "mc/test")

    def test_rs_backend_rejects_nof_replica_num(self):
        args = self._args(["--store-backend=rs", "--nof-replica-num=1"])
        with self.assertRaisesRegex(ValueError, "does not support --nof-replica-num"):
            bench.BenchmarkRunner(args)


class ZcopyBufferPoolTest(unittest.TestCase):
    def setUp(self):
        FakeBufferPool.instances.clear()
        FakeBufferPool.acquire_error = None

    def test_uses_native_buffer_pool_without_nof_allocators(self):
        store = object()
        pool = bench.ZcopyBufferPool(
            store, FakeBufferPool, value_size=16, slots=2, local_buffer_size=64
        )
        native_pool = FakeBufferPool.instances[0]

        self.assertIs(native_pool.store, store)
        self.assertEqual(native_pool.kwargs, {"max_bytes": 64, "max_regions": 1})
        self.assertEqual(native_pool.acquire_args, (32, False))
        self.assertEqual(pool.slot_ptr(1), pool.base_ptr + 16)

        view = bench.ZcopyBufferView(pool, slot_offset=0, slots=2)
        self.assertEqual(view.fill_write_buffers([b"abc"]), [pool.base_ptr])
        self.assertEqual(view.read_bytes(0, 3), b"abc")

        lease = native_pool.lease
        pool.close()
        self.assertTrue(lease.released)
        self.assertTrue(native_pool.closed)
        self.assertEqual(pool.base_ptr, 0)

        pool.close()
        self.assertTrue(lease.released)
        self.assertTrue(native_pool.closed)

    def test_external_buffer_registers_and_unregisters(self):
        store = Mock()
        store.register_buffer.return_value = 0
        pool = bench.ZcopyBufferPool(
            store,
            FakeBufferPool,
            value_size=16,
            slots=2,
            local_buffer_size=64,
            external=True,
        )

        self.assertEqual(FakeBufferPool.instances, [])
        ptr = pool.base_ptr
        store.register_buffer.assert_called_once_with(ptr, 32)
        self.assertEqual(pool.slot_ptr(1), ptr + 16)

        pool.close()
        store.unregister_buffer.assert_called_once_with(ptr)
        self.assertEqual(pool.base_ptr, 0)

    def test_external_buffer_rejects_unregistered_buffer(self):
        store = Mock()
        store.register_buffer.return_value = -1
        with self.assertRaisesRegex(RuntimeError, "register_buffer failed"):
            bench.ZcopyBufferPool(
                store,
                FakeBufferPool,
                value_size=16,
                slots=2,
                local_buffer_size=64,
                external=True,
            )

    def test_rejects_pool_larger_than_store_local_buffer(self):
        with self.assertRaisesRegex(ValueError, "increase --local-buffer-size"):
            bench.ZcopyBufferPool(
                object(), FakeBufferPool, value_size=32, slots=3, local_buffer_size=64
            )
        self.assertEqual(FakeBufferPool.instances, [])

    def test_closes_native_pool_when_acquire_fails(self):
        FakeBufferPool.acquire_error = RuntimeError("buffer pool is exhausted")

        with self.assertRaisesRegex(RuntimeError, "buffer pool is exhausted"):
            bench.ZcopyBufferPool(
                object(), FakeBufferPool, value_size=16, slots=2, local_buffer_size=64
            )

        self.assertEqual(len(FakeBufferPool.instances), 1)
        self.assertTrue(FakeBufferPool.instances[0].closed)


if __name__ == "__main__":
    unittest.main()
