"""Hardware-independent tests for passive MNNVL owner supervision."""

import argparse
import os
from pathlib import Path
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

import launch_egm_owners as launcher


class OwnerLauncherTest(unittest.TestCase):
    def args(self, **overrides):
        values = dict(
            host="10.0.0.1",
            rack="rack1",
            fabric_domain="fabric1",
            master="10.0.0.2:50051",
            metadata="P2PHANDSHAKE",
            gpus=[3],
            base_port=18060,
            segment_gib=1,
            ready_timeout=1,
        )
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_physical_gpu_environment_precedes_imports(self):
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0,1"}):
            env = launcher.owner_environment(self.args(), 3)
            self.assertEqual(env["CUDA_VISIBLE_DEVICES"], "3")
            self.assertEqual(env["MC_PHYSICAL_GPU"], "3")
            self.assertEqual(env["CUDA_DEVICE_ORDER"], "PCI_BUS_ID")
            self.assertEqual(env["MOONCAKE_FABRIC_DOMAIN"], "fabric1")
            self.assertEqual(env["MOONCAKE_STORE_BACKEND"], "cpp")
            self.assertEqual(os.environ["CUDA_VISIBLE_DEVICES"], "0,1")

    def test_native_readiness_logging_ignores_inherited_file_sink(self):
        inherited = {
            "MC_LOG_DIR": "/tmp/external-logs",
            "GLOG_logtostderr": "0",
            "GLOG_minloglevel": "2",
            "MC_LOG_LEVEL": "ERROR",
        }
        with mock.patch.dict(os.environ, inherited):
            env = launcher.owner_environment(self.args(), 3)
            self.assertNotIn("MC_LOG_DIR", env)
            self.assertEqual(env["GLOG_logtostderr"], "1")
            self.assertEqual(env["GLOG_minloglevel"], "0")
            self.assertEqual(env["MC_LOG_LEVEL"], "INFO")
            for name, value in inherited.items():
                self.assertEqual(os.environ[name], value)

    def test_invalid_gpu_lists(self):
        for value in ("", "0,", "-1", "1,1", "GPU-uuid", "0,x"):
            with self.subTest(value=value), self.assertRaises(
                argparse.ArgumentTypeError
            ):
                launcher.parse_gpus(value)
        self.assertEqual(launcher.parse_gpus("3,7"), [3, 7])

    def test_readiness_requires_mount_and_successful_setup(self):
        process = mock.Mock()
        process.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "owner.log"
            for marker in ("OWNER_READY", "MNNVL_SEGMENT_READY"):
                path.write_text(marker)
                with self.assertRaises(TimeoutError):
                    launcher.wait_ready([(process, path)], 0, threading.Event())
            path.write_text("MNNVL_SEGMENT_READY\nOWNER_READY\n")
            self.assertTrue(
                launcher.wait_ready([(process, path)], 0, threading.Event())
            )

    def test_readiness_rejects_exited_owner_even_with_markers(self):
        process = mock.Mock()
        process.poll.return_value = 1
        path = mock.Mock()
        path.read_text.return_value = "MNNVL_SEGMENT_READY\nOWNER_READY"
        with self.assertRaises(RuntimeError):
            launcher.wait_ready(
                [(process, path)], time.monotonic() + 1, threading.Event()
            )

    def test_readiness_shutdown_is_immediate(self):
        stopped = threading.Event()
        stopped.set()
        self.assertFalse(launcher.wait_ready([(mock.Mock(), mock.Mock())], 0, stopped))

    def test_owner_requires_gpu_pin_before_native_import(self):
        with mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0"}):
            with self.assertRaises(RuntimeError):
                launcher.run_owner(self.args())

    def run_fake_owner(self, setup_result=0, health_result=0):
        store = mock.Mock()
        store.setup.return_value = setup_result
        store.health_check.return_value = health_result
        store.close.return_value = 0
        module = types.ModuleType("mooncake.store")
        module.MooncakeDistributedStore = lambda: store
        stopped = mock.Mock()
        stopped.wait.return_value = True
        with (
            mock.patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "3"}),
            mock.patch.dict("sys.modules", {"mooncake.store": module}),
            mock.patch.object(launcher.signal, "signal"),
            mock.patch.object(launcher.threading, "Event", return_value=stopped),
            mock.patch("builtins.print") as output,
        ):
            if setup_result or health_result:
                with self.assertRaises(RuntimeError):
                    launcher.run_owner(self.args())
                output.assert_not_called()
            else:
                launcher.run_owner(self.args())
                output.assert_called_once_with(
                    "OWNER_READY gpu=3 domain=fabric1", flush=True
                )
        store.close.assert_called_once()
        return store

    def test_owner_is_passive_and_closes_after_readiness(self):
        store = self.run_fake_owner()
        self.assertEqual(store.setup.call_args.kwargs["local_buffer_size"], 0)
        self.assertEqual(store.setup.call_args.kwargs["protocol"], "mnnvl")
        store.put.assert_not_called()
        store.get.assert_not_called()

    def test_failed_setup_never_reports_readiness(self):
        self.run_fake_owner(setup_result=-1)

    def test_failed_health_check_never_reports_readiness(self):
        self.run_fake_owner(health_result=-1)

    def test_supervisor_pins_each_child_and_terminates_only_its_children(self):
        children = [mock.Mock(), mock.Mock()]
        for process in children:
            process.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            args = self.args(gpus=[3, 7], log_dir=Path(directory))
            with (
                mock.patch.object(launcher.Path, "exists", return_value=True),
                mock.patch.object(launcher.signal, "signal"),
                mock.patch.object(
                    launcher.subprocess, "Popen", side_effect=children
                ) as popen,
                mock.patch.object(launcher, "wait_ready", return_value=False),
            ):
                launcher.supervise(args)
            for index, process in enumerate(children):
                call = popen.call_args_list[index]
                self.assertEqual(
                    call.kwargs["env"]["CUDA_VISIBLE_DEVICES"], str(args.gpus[index])
                )
                command = call.args[0]
                self.assertEqual(
                    command[command.index("--base-port") + 1], str(18060 + index)
                )
                process.terminate.assert_called_once()
                process.wait.assert_called_once_with(timeout=60)
                process.kill.assert_not_called()

    def test_missing_imex_never_spawns_owner(self):
        with (
            mock.patch.object(launcher.Path, "exists", return_value=False),
            mock.patch.object(launcher.subprocess, "Popen") as popen,
        ):
            with self.assertRaises(RuntimeError):
                launcher.supervise(self.args())
            popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
