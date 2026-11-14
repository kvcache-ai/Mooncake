import ctypes
import importlib.util
import io
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
    def test_rejects_negative_replica_counts(self):
        for kind in ("memory", "nof", "dfs"):
            with self.subTest(kind=kind):
                args = bench.build_parser().parse_args(
                    ["--scenario=read_perf", f"--{kind}-replica-num=-1"]
                )
                with self.assertRaisesRegex(ValueError, "replica counts must be >= 0"):
                    bench.BenchmarkRunner(args)
                with self.assertRaisesRegex(ValueError, "replica counts must be >= 0"):
                    bench.report_capabilities(args)

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

    def test_rs_backend_skips_unconfigured_nof_replica_num(self):
        args = self._args(["--store-backend=rs", "--nof-replica-num=1"])
        runner = bench.BenchmarkRunner(args)
        with patch.object(bench, "load_store_module") as load:
            self.assertEqual(runner.run(), [])
        load.assert_not_called()
        self.assertEqual(runner.capability["result"], "SKIP_NOT_CONFIGURED")


class NofCapabilityTest(unittest.TestCase):
    def _args(self, *extra):
        return bench.build_parser().parse_args(["--scenario=capabilities", *extra])

    def test_rs_matrix_distinguishes_unwired_copy_from_missing_operations(self):
        with patch.object(bench, "load_store_module") as load:
            capabilities = bench.probe_nof_capabilities(
                self._args("--store-backend=rs", "--nof-replica-num=1")
            )
        load.assert_not_called()
        self.assertEqual(
            capabilities["nof_replica_count"]["result"], "SKIP_NOT_CONFIGURED"
        )
        for name in ("nof_promotion", "nof_zero_copy_read", "nof_zero_copy_write"):
            self.assertEqual(capabilities[name]["capability"], "not_implemented")
            self.assertEqual(capabilities[name]["result"], "SKIP_NOT_IMPLEMENTED")
            self.assertTrue(capabilities[name]["reason"])

    def test_cpp_default_report_does_not_need_a_native_module_or_target(self):
        with patch.object(bench, "load_store_module") as load:
            capabilities = bench.probe_nof_capabilities(self._args())
        load.assert_not_called()
        self.assertEqual(
            capabilities["nof_replica_count"]["result"], "SKIP_NOT_CONFIGURED"
        )
        self.assertEqual(
            capabilities["nof_zero_copy_write"]["result"], "SKIP_NOT_IMPLEMENTED"
        )

    def test_cpp_build_flag_is_required_even_when_config_exposes_nof(self):
        args = self._args("--nof-replica-num=1", "--io-api=zcopy")
        for enabled in (True, False, None):
            with self.subTest(enabled=enabled):
                module = SimpleNamespace(
                    NOF_ENABLED=enabled,
                    ReplicateConfig=store_module.ReplicateConfig,
                    MooncakeDistributedStore=Mock(),
                )
                with patch.object(bench, "load_store_module", return_value=module):
                    capabilities = bench.probe_nof_capabilities(args)
                module.MooncakeDistributedStore.assert_not_called()
                for name in (
                    "nof_replica_count",
                    "nof_promotion",
                    "nof_zero_copy_read",
                ):
                    self.assertEqual(
                        capabilities[name]["capability"],
                        "supported" if enabled else "not_configured",
                    )
                    # Discovering support is not a successful benchmark run.
                    self.assertEqual(
                        capabilities[name]["result"],
                        None if enabled else "SKIP_NOT_CONFIGURED",
                    )

    def test_cpp_promotion_and_registered_reads_require_their_configuration(self):
        args = self._args("--nof-replica-num=1", "--memory-replica-num=0")
        with patch.object(
            bench, "load_store_module", return_value=SimpleNamespace(NOF_ENABLED=True)
        ):
            capabilities = bench.probe_nof_capabilities(args)
        self.assertEqual(capabilities["nof_replica_count"]["capability"], "supported")
        self.assertIn("memory-replica-num", capabilities["nof_promotion"]["reason"])
        self.assertIn("io-api=zcopy", capabilities["nof_zero_copy_read"]["reason"])

    def test_report_cli_writes_only_capabilities_without_performance_results(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "summary.json"
            with (
                patch.object(
                    sys,
                    "argv",
                    [
                        "store_kv_bench.py",
                        "--scenario=capabilities",
                        "--store-backend=rs",
                        f"--output-dir={directory}",
                    ],
                ),
                patch.object(bench, "load_store_module") as load,
                patch.object(sys, "stdout", new_callable=io.StringIO) as output,
            ):
                self.assertEqual(bench.main(), 0)
            load.assert_not_called()
            summary = json.loads(path.read_text())
            self.assertEqual(json.loads(output.getvalue()), summary)
            self.assertEqual(len(summary["capabilities"]), 4)
            self.assertNotIn("overall", summary)
            self.assertNotIn("ok", summary)

    def test_skipped_cli_writes_no_throughput_or_success_and_never_sets_up(self):
        for backend, enabled, io_api, expected in (
            ("rs", None, "plain", "SKIP_NOT_CONFIGURED"),
            ("rs", None, "zcopy", "SKIP_NOT_IMPLEMENTED"),
            ("cpp", False, "plain", "SKIP_NOT_CONFIGURED"),
        ):
            with self.subTest(
                backend=backend, io_api=io_api
            ), TemporaryDirectory() as root:
                module = SimpleNamespace(
                    NOF_ENABLED=enabled, MooncakeDistributedStore=Mock()
                )
                with (
                    patch.object(
                        sys,
                        "argv",
                        [
                            "store_kv_bench.py",
                            "--scenario=read_perf",
                            f"--store-backend={backend}",
                            f"--io-api={io_api}",
                            "--nof-replica-num=1",
                            "--memory-replica-num=0",
                            f"--summary-json={root}/result.json",
                        ],
                    ),
                    patch.object(bench, "load_store_module", return_value=module),
                ):
                    self.assertEqual(bench.main(), 0)
                module.MooncakeDistributedStore.assert_not_called()
                summary = json.loads((Path(root) / "result.json").read_text())
                self.assertEqual(summary["result"], expected)
                self.assertIsNone(summary["ok"])
                self.assertIsNone(summary["overall"])
                self.assertEqual(summary["phases"], {})
                self.assertTrue(summary["capability"]["reason"])

    def test_configured_cpp_executes_io_and_reports_real_success_or_failure(self):
        for put_result in (0, -800):
            with self.subTest(put_result=put_result), TemporaryDirectory() as root:
                store = Mock()
                store.setup.return_value = 0
                store.put.return_value = put_result
                module = SimpleNamespace(
                    NOF_ENABLED=True,
                    MooncakeDistributedStore=Mock(return_value=store),
                    ReplicateConfig=store_module.ReplicateConfig,
                )
                with (
                    patch.object(
                        sys,
                        "argv",
                        [
                            "store_kv_bench.py",
                            "--scenario=write_perf",
                            "--nr-objects=1",
                            "--nof-replica-num=1",
                            f"--output-dir={root}",
                        ],
                    ),
                    patch.object(bench, "load_store_module", return_value=module),
                ):
                    self.assertEqual(bench.main(), 0)
                store.put.assert_called_once()
                self.assertEqual(
                    store.put.call_args.kwargs["config"].nof_replica_num, 1
                )
                store.close.assert_called_once()
                summary = json.loads((Path(root) / "summary.json").read_text())
                self.assertEqual(
                    summary["result"], "PASS" if put_result == 0 else "FAIL"
                )
                self.assertEqual(summary["overall"]["requests"], 1)

    def test_broken_native_module_is_not_hidden_as_a_skip(self):
        args = self._args("--nof-replica-num=1")
        with (
            patch.object(
                bench, "load_store_module", side_effect=ImportError("libasio")
            ),
            self.assertRaisesRegex(ImportError, "libasio"),
        ):
            bench.probe_nof_capabilities(args)


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
