# SPDX-License-Identifier: Apache-2.0
"""CPU-only checks for the fixed elastic test launcher; no CUDA calls."""

import inspect
import os
import shlex
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import launch_elastic
import rank_server
from plan import Plan
from mooncake_adapter import MooncakeAdapter
from nixl_adapter import NixlAdapter


HERE = Path(__file__).resolve().parent


class FakeProcess:
    def __init__(self, returncode):
        self.returncode = returncode
        self.pid = 4242

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        return self.returncode


class LauncherCpuTests(unittest.TestCase):
    def make_launcher(self, directory, phases):
        args = SimpleNamespace(
            phase_timeout_s=1.0,
            ssh="ssh",
            worker_hosts=["local", "remote"],
            gpu_lists=(("4", "5"), ("0", "1")),
            backend="mooncake",
            worker_pythonpath="",
            nixl_pythonpath="",
        )
        coordinator = launch_elastic.RunCoordinator(phases)
        return launch_elastic.ElasticLauncher(
            args, HERE / "single_rank.json", phases, Path(directory), coordinator
        )

    def test_round_robin_gpu_and_host_mapping(self):
        with tempfile.TemporaryDirectory() as tmp:
            launcher = self.make_launcher(tmp, [[0, 1, 2, 3]])
            self.assertEqual(
                [launcher._host_for_rank(rank) for rank in range(4)],
                ["local", "remote", "local", "remote"],
            )
            self.assertEqual(launcher._gpu_ids_for_rank(2), ("4", "5"))
            self.assertEqual(
                launch_elastic._parse_gpu_lists("4,5;0,1", "", 2),
                (("4", "5"), ("0", "1")),
            )

    def test_barriers_joiners_and_clean_removal(self):
        phases = [[0, 1], [0, 1, 2, 3]]
        coordinator = launch_elastic.RunCoordinator(phases)
        members = [set(phase) for phase in phases]
        for phase, ranks in enumerate(members):
            coordinator.configure("ready", phase, ranks)
            coordinator.configure("done", phase, ranks)
        errors = []
        threads = []

        def worker(rank, start_phase):
            try:
                for phase in range(start_phase, len(phases)):
                    if rank not in members[phase]:
                        return
                    coordinator.arrive_and_wait("ready", phase, rank)
                    coordinator.arrive_and_wait("done", phase, rank)
            except Exception as exc:  # surfaced in the test thread
                errors.append(exc)

        for rank in sorted(members[0]):
            thread = threading.Thread(target=worker, args=(rank, 0), daemon=True)
            thread.start()
            threads.append(thread)
        coordinator.wait_arrivals("ready", 0, 1.0)
        coordinator.release("ready", 0)
        coordinator.wait_arrivals("done", 0, 1.0)
        for rank in sorted(members[1] - members[0]):
            thread = threading.Thread(target=worker, args=(rank, 1), daemon=True)
            thread.start()
            threads.append(thread)
        coordinator.release("done", 0)
        coordinator.wait_arrivals("ready", 1, 1.0)
        coordinator.release("ready", 1)
        coordinator.wait_arrivals("done", 1, 1.0)
        coordinator.release("done", 1)
        for thread in threads:
            thread.join(1.0)
        self.assertFalse(errors)
        self.assertTrue(all(not thread.is_alive() for thread in threads))

        # Removed ranks finish phase 0 and do not join phase 1.
        phases = [[0, 1, 2, 3], [0, 1]]
        coordinator = launch_elastic.RunCoordinator(phases)
        members = [set(phase) for phase in phases]
        for phase, ranks in enumerate(members):
            coordinator.configure("ready", phase, ranks)
            coordinator.configure("done", phase, ranks)
        threads = []
        for rank in sorted(members[0]):
            thread = threading.Thread(target=worker, args=(rank, 0), daemon=True)
            thread.start()
            threads.append(thread)
        coordinator.wait_arrivals("ready", 0, 1.0)
        coordinator.release("ready", 0)
        coordinator.wait_arrivals("done", 0, 1.0)
        coordinator.release("done", 0)
        coordinator.wait_arrivals("ready", 1, 1.0)
        coordinator.release("ready", 1)
        coordinator.wait_arrivals("done", 1, 1.0)
        coordinator.release("done", 1)
        for thread in threads:
            thread.join(1.0)
        self.assertTrue(all(not thread.is_alive() for thread in threads))

    def test_rank_reentry_and_lowest_free_global_id(self):
        handler = rank_server.RankServerHandler
        handler._counts.clear()
        handler._rank_to_host.clear()
        handler._user_context.clear()
        handler._all_global_ranks.clear()
        handler._removed_global_ranks.clear()
        server = rank_server.ReusableTCPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        address, port = server.server_address
        try:
            clients = [rank_server.RankClient(address, port) for _ in range(4)]
            assigned = [client.get_rank() for client in clients]
            self.assertEqual([row[1] for row in assigned], [0, 1, 2, 3])
            clients[1].release_rank(user_context=2)
            clients[3].release_rank(user_context=2)
            replacement_a = rank_server.RankClient(address, port).get_rank()
            replacement_b = rank_server.RankClient(address, port).get_rank()
            self.assertEqual(replacement_a[1:], (1, 2))
            self.assertEqual(replacement_b[1:], (3, 2))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(1.0)
            handler._counts.clear()
            handler._rank_to_host.clear()
            handler._user_context.clear()
            handler._all_global_ranks.clear()
            handler._removed_global_ranks.clear()

        plan_path = HERE / "expansion_contraction.json"
        self.assertEqual(Plan(str(plan_path), 5, start_phase=2).current_phase, 3)
        self.assertEqual(Plan(str(plan_path), 6, start_phase=2).current_phase, 3)

    def test_planned_vs_unexpected_sigterm_and_clean_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            phases = [[0, 1], [0, -1], [0, 1]]
            launcher = self.make_launcher(tmp, phases)
            fault = launch_elastic.Worker(
                1,
                1,
                "local",
                FakeProcess(-signal.SIGTERM),
                Path(tmp) / "fault.log",
                is_local=True,
            )
            launcher.current_by_rank[1] = fault
            launcher.workers.append(fault)
            launcher.coordinator.mark_expected_fault(1, 1)
            launcher._wait_expected_faults(1)
            launcher._wait_removed({1})
            launcher._wait_all()
            self.assertIn(id(fault), launcher.expected_signal_workers)

            with self.assertRaises(RuntimeError):
                launcher.coordinator.mark_expected_fault(7, 1)

            unexpected = self.make_launcher(tmp, [[0]])
            unexpected.workers.append(
                launch_elastic.Worker(
                    0, 0, "local", FakeProcess(-signal.SIGTERM), Path(tmp) / "bad.log"
                )
            )
            with self.assertRaisesRegex(RuntimeError, "unexpected worker exits"):
                unexpected._wait_all()

            clean = self.make_launcher(tmp, [[0, 1], [0]])
            removed = launch_elastic.Worker(
                1, 0, "local", FakeProcess(0), Path(tmp) / "removed.log"
            )
            clean.current_by_rank[1] = removed
            clean._wait_removed({1})

    def test_remote_planned_sigterm_15_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            phases = [[0, 1], [0, -1], [0, 1]]
            remote_launcher = self.make_launcher(tmp, phases)
            remote = launch_elastic.Worker(
                1,
                1,
                "remote",
                FakeProcess(signal.SIGTERM),
                Path(tmp) / "planned-remote.log",
                is_local=False,
            )
            remote_launcher.current_by_rank[1] = remote
            remote_launcher.workers.append(remote)
            remote_launcher.coordinator.mark_expected_fault(1, 1)
            remote_launcher._wait_expected_faults(1)
            remote_launcher._wait_removed({1})
            remote_launcher._wait_all()

            local_launcher = self.make_launcher(tmp, phases)
            local = launch_elastic.Worker(
                1,
                1,
                "local",
                FakeProcess(signal.SIGTERM),
                Path(tmp) / "planned-local-positive-15.log",
                is_local=True,
            )
            local_launcher.current_by_rank[1] = local
            local_launcher.coordinator.mark_expected_fault(1, 1)
            with self.assertRaisesRegex(RuntimeError, "planned fault rank 1"):
                local_launcher._wait_expected_faults(1)

            unplanned_launcher = self.make_launcher(tmp, [[0]])
            unplanned = launch_elastic.Worker(
                0,
                0,
                "remote",
                FakeProcess(signal.SIGTERM),
                Path(tmp) / "unplanned-remote.log",
                is_local=False,
            )
            unplanned_launcher.workers.append(unplanned)
            with self.assertRaisesRegex(RuntimeError, "unexpected worker exits"):
                unplanned_launcher._wait_all()

            clean_remove_launcher = self.make_launcher(tmp, [[0, 1], [0]])
            clean_remove = launch_elastic.Worker(
                1,
                0,
                "remote",
                FakeProcess(signal.SIGTERM),
                Path(tmp) / "clean-remote-remove.log",
                is_local=False,
            )
            clean_remove_launcher.current_by_rank[1] = clean_remove
            with self.assertRaisesRegex(RuntimeError, "cleanly removed rank 1 exited"):
                clean_remove_launcher._wait_removed({1})

    def test_controller_cleanup_handles_keyboard_interrupt(self):
        with tempfile.TemporaryDirectory() as tmp:
            launcher = self.make_launcher(tmp, [[0]])
            previous = signal.getsignal(signal.SIGTERM)
            with (
                patch.object(launcher, "run", side_effect=KeyboardInterrupt),
                patch.object(launcher, "cleanup_workers", return_value=[]) as cleanup,
            ):
                with self.assertRaises(KeyboardInterrupt):
                    launch_elastic._run_with_cleanup(
                        launcher, launcher.coordinator, Path(tmp)
                    )
            cleanup.assert_called_once_with()
            self.assertIn("KeyboardInterrupt", (Path(tmp) / "failure.txt").read_text())
            self.assertIs(signal.getsignal(signal.SIGTERM), previous)

    def test_direct_worker_sigterm_status(self):
        code = (
            "import elastic,os,signal,sys; "
            "elastic.worker=lambda rank,args: os.kill(os.getpid(),signal.SIGTERM); "
            "sys.argv=['elastic.py','--plan','single_rank.json','--num-processes','1',"
            "'--tcp-server','127.0.0.1']; elastic.main()"
        )
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ""
        env["MC_EP_ELASTIC_CONTROL_ADDR"] = "127.0.0.1:1"
        env["PYTHONPATH"] = str(HERE)
        result = subprocess.run(
            [os.sys.executable, "-c", code],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
        self.assertEqual(result.returncode, -signal.SIGTERM)

    def test_targeted_process_group_cleanup_and_remote_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            launcher = self.make_launcher(tmp, [[0]])
            child_pid_file = Path(tmp) / "child.pid"
            command = f"sleep 30 & echo $! > {child_pid_file}; wait"
            process = subprocess.Popen(
                ["bash", "-c", command],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            deadline = time.monotonic() + 2.0
            while not child_pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(child_pid_file.exists())
            child_pid = int(child_pid_file.read_text().strip())
            worker = launch_elastic.Worker(
                0,
                0,
                "local",
                process,
                Path(tmp) / "worker.log",
                is_local=True,
                task_pgid=process.pid,
            )
            launcher.workers.append(worker)
            self.assertEqual(launcher.cleanup_workers(), [])
            self.assertIsNotNone(process.poll())
            for _ in range(100):
                try:
                    state = Path(f"/proc/{child_pid}/stat").read_text().split()[2]
                except FileNotFoundError:
                    break
                if state == "Z":
                    break
                time.sleep(0.01)
            else:
                self.fail("task child survived targeted process-group cleanup")

            remote = launch_elastic.Worker(
                2,
                0,
                "remote",
                FakeProcess(None),
                Path(tmp) / "remote.log",
                is_local=False,
                remote_pid=9002,
                task_pgid=9001,
            )
            launcher.workers = []
            with patch.object(launch_elastic.subprocess, "run") as remote_run:
                launcher._signal_task_group(remote, signal.SIGTERM)
            call = remote_run.call_args
            self.assertEqual(
                call.args[0],
                ["ssh", "remote", "kill -SIGTERM -- -9001 2>/dev/null || true"],
            )
            self.assertEqual(call.kwargs["timeout"], 8.0)

    def test_nixl_remote_command_forwards_explicit_runtime_environment(self):
        values = {
            "NIXL_EP_DEVICE_MODE": "ibgda mode;$(touch /tmp/p1-nixl-env-must-not-run)",
            "NIXL_EP_NUM_CHANNELS": "7",
            "UCX_TLS": "rc,sm,self",
            "NIXL_PLUGIN_DIR": "/tmp/nixl plugins",
            "UCX_MEMTYPE_CACHE": "n",
            "UCX_LOG_LEVEL": "info",
        }
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, values):
            launcher = self.make_launcher(tmp, [[0], [0]])
            launcher.args.backend = "nixl"
            launcher.args.format_set = "common"
            launcher.args.num_tokens = 128
            launcher.args.num_experts_per_rank = 8
            launcher.args.hidden_dim = 2048
            launcher.args.num_topk = 8
            launcher.args.timeout_ms = 1000
            launcher.args.advertise_address = "192.168.0.239"
            launcher.args.control_port = 10001
            launcher.args.phase_timeout_s = 180.0
            launcher.args.conda_sh = "/opt/test-conda/etc/profile.d/conda.sh"
            launcher.args.native_lib_path = ""
            launcher.args.kineto = False
            launcher.args.disable_ll_nvlink = False
            nixl_command = shlex.split(
                launcher._remote_command(1, Path(tmp) / "rank.pid")
            )[2]
            for name, value in values.items():
                self.assertIn(f"export {name}={shlex.quote(value)}", nixl_command)
            self.assertIn(
                "export UCX_NET_DEVICES=mlx5_1:1,mlx5_2:1,mlx5_3:1,mlx5_4:1",
                nixl_command,
            )

            launcher.args.backend = "mooncake"
            mooncake_command = shlex.split(
                launcher._remote_command(1, Path(tmp) / "rank.pid")
            )[2]
            for name in values:
                self.assertNotIn(f"export {name}=", mooncake_command)
            self.assertIn(
                "export MOONCAKE_EP_DEVICE_FILTER=mlx5_1,mlx5_2,mlx5_3,mlx5_4",
                mooncake_command,
            )
            self.assertNotIn("export UCX_NET_DEVICES=", mooncake_command)

    def test_adapter_signatures_bind_suite_and_benchmark_calls(self):
        x, topk_idx, topk_weights, handle, stats, out = [object() for _ in range(6)]
        for adapter in (MooncakeAdapter, NixlAdapter):
            dispatch = inspect.signature(adapter.dispatch)
            dispatch.bind(
                None,
                x,
                topk_idx,
                128,
                64,
                use_fp8=True,
                round_scale=True,
                use_ue8m0=True,
                cumulative_local_expert_recv_stats=stats,
                async_finish=True,
                return_recv_hook=True,
            )
            dispatch.bind(
                None,
                x,
                topk_idx,
                128,
                64,
                cumulative_local_expert_recv_stats=stats,
                use_fp8=True,
                async_finish=False,
                return_recv_hook=False,
            )
            combine = inspect.signature(adapter.combine)
            combine.bind(
                None,
                x,
                topk_idx,
                topk_weights,
                handle,
                use_logfmt=False,
                async_finish=True,
                zero_copy=True,
                return_recv_hook=True,
                out=out,
            )
            combine.bind(
                None,
                x,
                topk_idx,
                topk_weights,
                handle,
                use_logfmt=False,
                return_recv_hook=False,
            )

    def test_exited_ssh_still_cleans_registered_remote_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            launcher = self.make_launcher(tmp, [[0]])
            remote = launch_elastic.Worker(
                0,
                0,
                "remote",
                FakeProcess(143),
                Path(tmp) / "remote.log",
                is_local=False,
                remote_pid=9002,
                task_pgid=9001,
            )
            launcher.workers = [remote]
            with (
                patch.object(launcher, "_signal_task_group") as signal_group,
                patch.object(
                    launcher, "_remote_task_group_alive", return_value=False
                ) as alive,
            ):
                self.assertEqual(launcher.cleanup_workers(), [])
            self.assertEqual(
                [call.args for call in signal_group.call_args_list],
                [(remote, signal.SIGTERM)],
            )
            alive.assert_called()

    def test_remote_cleanup_escalates_only_when_registered_group_is_alive(self):
        with tempfile.TemporaryDirectory() as tmp:
            launcher = self.make_launcher(tmp, [[0]])
            remote = launch_elastic.Worker(
                0,
                0,
                "remote",
                FakeProcess(143),
                Path(tmp) / "remote.log",
                is_local=False,
                remote_pid=9002,
                task_pgid=9001,
            )
            launcher.workers = [remote]
            with (
                patch.object(launcher, "_signal_task_group") as signal_group,
                patch.object(
                    launcher, "_remote_task_group_alive", side_effect=[True, False]
                ),
            ):
                self.assertEqual(launcher.cleanup_workers(), [])
            self.assertEqual(
                [call.args for call in signal_group.call_args_list],
                [(remote, signal.SIGTERM), (remote, signal.SIGKILL)],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
