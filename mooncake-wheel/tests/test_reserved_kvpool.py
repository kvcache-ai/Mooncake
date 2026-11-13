import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/ascend/reserved_kvpool.py"
spec = importlib.util.spec_from_file_location("reserved_kvpool", SCRIPT)
pool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pool)


def series(name, size):
    return f"segment_total_capacity_bytes{{segment={json.dumps(name)}}} {size}\n"


class ReservationTests(unittest.TestCase):
    def setUp(self):
        pool.STOP.clear()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "reservation.json"
        self.state = dict(
            ready=True,
            controller_pid=1,
            controller_token="parent",
            devices=2,
            per_device_bytes=30 * pool.GIB,
            owners=[
                dict(pid=2, token="a", segment="owner-a"),
                dict(pid=3, token="b", segment="owner-b"),
            ],
            metrics_url="http://master/metrics",
        )
        self.full = series("owner-a", 30 * pool.GIB) + series("owner-b", 30 * pool.GIB)
        self.consumer = dict(
            master_server_address="master:50051", global_segment_size=0
        )
        (self.path.parent / "consumer.json").write_text(json.dumps(self.consumer))
        self.state["consumer_config"] = self.consumer

    def test_unrelated_capacity_cannot_satisfy_reservation(self):
        text = (
            "master_total_capacity_bytes 999999999999\n"
            + series("unrelated", 800 * pool.GIB)
            + series("owner-a", 15 * pool.GIB)
            + series("owner-b", 15 * pool.GIB)
        )
        with self.assertRaisesRegex(RuntimeError, "short"):
            pool.require_capacity(self.state, text)

    def test_aggregate_cannot_hide_missing_owner(self):
        with self.assertRaisesRegex(RuntimeError, "short"):
            pool.require_capacity(self.state, series("owner-a", 60 * pool.GIB))

    def test_exact_owned_capacity_and_higher_floor(self):
        self.assertEqual(pool.require_capacity(self.state, self.full), 60 * pool.GIB)
        with self.assertRaisesRegex(RuntimeError, "below required"):
            pool.require_capacity(self.state, self.full, 61 * pool.GIB)

    def test_duplicate_owners_rejected(self):
        self.state["owners"][1]["segment"] = "owner-a"
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            pool.require_capacity(self.state, self.full)

    def test_duplicate_series_does_not_double_count(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            pool.require_capacity(
                self.state, self.full + series("owner-a", 30 * pool.GIB)
            )

    def test_escaped_label_and_extra_labels(self):
        name = 'host\\name"suffix'
        text = f'segment_total_capacity_bytes{{tenant="test",segment={json.dumps(name)}}} 3.0e1\n'
        self.assertEqual(pool.owned_capacities(text, [name]), {name: 30})

    def test_invalid_capacity_fails_closed(self):
        for value in ("NaN", "Infinity", "-1", "1.5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                pool.owned_capacities(series("owner-a", value), ["owner-a"])

    def test_identity_change_blocks_stale_ready_record(self):
        pool.write_state(self.path, self.state)
        with mock.patch.object(
            pool,
            "process_token",
            side_effect=lambda pid: {1: "parent", 2: "recycled", 3: "b"}[pid],
        ), mock.patch.object(pool, "fetch_metrics", return_value=self.full):
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                pool.check_admission(self.path.parent, 0)

    def test_admission_rechecks_live_capacity(self):
        pool.write_state(self.path, self.state)
        with mock.patch.object(
            pool,
            "process_token",
            side_effect=lambda pid: {1: "parent", 2: "a", 3: "b"}[pid],
        ), mock.patch.object(
            pool, "fetch_metrics", return_value=series("owner-a", 15 * pool.GIB)
        ):
            with self.assertRaisesRegex(RuntimeError, "short"):
                pool.check_admission(self.path.parent, 0)

    def test_stopping_never_admits(self):
        self.state["stopping"] = True
        pool.write_state(self.path, self.state)
        with self.assertRaisesRegex(RuntimeError, "not ready"):
            pool.check_admission(self.path.parent, 0)

    def test_modified_consumer_cannot_use_a_different_pool(self):
        pool.write_state(self.path, self.state)
        changed = dict(self.consumer, master_server_address="another-master:50051")
        (self.path.parent / "consumer.json").write_text(json.dumps(changed))
        with self.assertRaisesRegex(RuntimeError, "configuration differs"):
            pool.check_admission(self.path.parent, 0)

    def test_metrics_failure_recovers_without_terminating_owners(self):
        process = mock.Mock()
        process.poll.return_value = None
        states = []
        write = pool.write_state

        def capture(path, state):
            states.append(state["ready"])
            write(path, state)

        def tick(_):
            if len(states) == 2:
                pool.STOP.set()

        with mock.patch.object(
            pool,
            "fetch_metrics",
            side_effect=[OSError("metrics unavailable"), self.full],
        ), mock.patch.object(
            pool, "write_state", side_effect=capture
        ), mock.patch.object(pool.STOP, "wait", side_effect=tick):
            pool.monitor(self.state, self.path, [process], 0, float("inf"))
        self.assertEqual(states, [False, True])
        process.terminate.assert_not_called()
        self.assertTrue(json.loads(self.path.read_text())["ready"])

    def test_owner_exit_closes_monitor(self):
        process = mock.Mock()
        process.poll.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "owner exited"):
            pool.monitor(self.state, self.path, [process], 0, float("inf"))

    def test_initial_short_capacity_times_out(self):
        process = mock.Mock()
        process.poll.return_value = None
        with mock.patch.object(
            pool, "fetch_metrics", return_value=series("owner-a", 15 * pool.GIB)
        ):
            with self.assertRaisesRegex(TimeoutError, "full reservation"):
                pool.monitor(self.state, self.path, [process], 0, 0)
        self.assertFalse(json.loads(self.path.read_text())["ready"])

    def test_warmup_returns_only_after_full_owned_capacity(self):
        process = mock.Mock()
        process.poll.return_value = None
        self.state["ready"] = False
        with mock.patch.object(pool, "fetch_metrics", return_value=self.full):
            pool.monitor(
                self.state,
                self.path,
                [process],
                0,
                float("inf"),
                release_after_ready=True,
            )
        self.assertTrue(json.loads(self.path.read_text())["ready"])
        self.assertEqual(self.state["capacity_bytes"], 60 * pool.GIB)

    def test_warmup_release_checks_only_its_owners(self):
        process = mock.Mock()
        process.poll.return_value = 0
        unrelated = series("unrelated", 800 * pool.GIB)
        own_zero = series("owner-a", 0) + series("owner-b", 0)
        with mock.patch.object(
            pool, "fetch_metrics", side_effect=[self.full, unrelated + own_zero]
        ) as fetch, mock.patch.object(pool.time, "sleep"):
            pool.confirm_released(self.state, [process])
        self.assertEqual(fetch.call_count, 2)

    def test_warmup_rejects_unclean_owner_or_mounted_capacity(self):
        process = mock.Mock()
        process.poll.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "did not exit cleanly"):
            pool.confirm_released(self.state, [process])
        process.poll.return_value = 0
        with mock.patch.object(pool, "fetch_metrics", return_value=self.full):
            with self.assertRaisesRegex(RuntimeError, "release not confirmed"):
                pool.confirm_released(self.state, [process], timeout=0)

    def test_application_gets_zero_contribution_not_inherited_budget(self):
        args = argparse.Namespace(
            state_dir=str(self.path.parent),
            minimum_gib=60,
            timeout=0,
            command=["--", "application", "argument"],
        )
        with mock.patch.object(
            pool, "check_admission", return_value=60 * pool.GIB
        ), mock.patch.dict(
            pool.os.environ,
            MOONCAKE_GLOBAL_SEGMENT_SIZE="30GB",
            MOONCAKE_MASTER="master:50051",
        ), mock.patch.object(pool.os, "execvpe") as execute:
            pool.admit(args)
        executable, command, env = execute.call_args.args
        self.assertEqual(
            (executable, command), ("application", ["application", "argument"])
        )
        self.assertEqual(env["MOONCAKE_GLOBAL_SEGMENT_SIZE"], "0")
        self.assertEqual(env["MOONCAKE_MASTER"], "master:50051")
        self.assertEqual(
            env["MOONCAKE_CONFIG_PATH"],
            str(self.path.parent.resolve() / "consumer.json"),
        )

    def test_conflicting_inherited_master_never_executes_application(self):
        args = argparse.Namespace(
            state_dir=str(self.path.parent),
            minimum_gib=60,
            timeout=0,
            command=["application"],
        )
        with mock.patch.object(
            pool, "check_admission", return_value=60 * pool.GIB
        ), mock.patch.dict(
            pool.os.environ, MOONCAKE_MASTER="another-master:50051"
        ), mock.patch.object(pool.os, "execvpe") as execute:
            with self.assertRaisesRegex(RuntimeError, "MOONCAKE_MASTER conflicts"):
                pool.admit(args)
        execute.assert_not_called()

    @unittest.skipUnless(Path("/proc/self/stat").exists(), "Linux process identity")
    def test_actual_process_token_is_stable(self):
        self.assertEqual(
            pool.process_token(pool.os.getpid()), pool.process_token(pool.os.getpid())
        )


if __name__ == "__main__":
    unittest.main()
