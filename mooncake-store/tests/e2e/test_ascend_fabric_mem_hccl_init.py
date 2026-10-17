"""CPU-only checks for the manual probe's order, result and cleanup contract."""

import contextlib
import io
import json
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

PROBE = Path(__file__).with_name("ascend_fabric_mem_hccl_init.py")


class ProbeTest(unittest.TestCase):
    def run_probe(self, *, early=False, setup=(0, 0), close=(0, 0), hccl_error=False):
        calls = []
        dist = types.ModuleType("torch.distributed")
        dist.active = False

        def init(*args, **kwargs):
            calls.append("hccl")
            if hccl_error:
                raise RuntimeError("HCCL initialization failed")
            dist.active = True

        def destroy():
            calls.append("destroy")
            dist.active = False

        dist.init_process_group = init
        dist.barrier = lambda: calls.append("barrier")
        dist.is_initialized = lambda: dist.active
        dist.destroy_process_group = destroy
        torch = types.ModuleType("torch")
        torch.__version__ = "test"
        torch.npu = types.SimpleNamespace(set_device=lambda _: calls.append("device"))
        torch.distributed = dist
        torch_npu = types.ModuleType("torch_npu")
        torch_npu.__version__ = "test"
        instances = []

        class Store:
            def __init__(self):
                self.index = len(instances)
                instances.append(self)

            def setup(self, **kwargs):
                calls.append(f"setup{self.index}")
                assert kwargs["global_segment_size"] == 0
                assert kwargs["local_buffer_size"] == 0
                assert kwargs["protocol"] == "ascend"
                return setup[self.index]

            def close(self):
                calls.append(f"close{self.index}")
                value = close[self.index]
                if isinstance(value, Exception):
                    raise value
                return value

        store = types.ModuleType("mooncake.store")
        store.MooncakeDistributedStore = Store
        modules = {
            "torch": torch,
            "torch.distributed": dist,
            "torch_npu": torch_npu,
            "mooncake": types.ModuleType("mooncake"),
            "mooncake.store": store,
        }
        argv = [
            str(PROBE),
            "--master",
            "127.0.0.1:50051",
            "--endpoint",
            "127.0.0.1:19101",
            "--device",
            "0",
            "--dist-port",
            "19201",
        ]
        if early:
            argv.append("--hixl-first")
        output = io.StringIO()
        with patch.dict(sys.modules, modules), patch.object(
            sys, "argv", argv
        ), patch.dict("os.environ"), contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as exit_result:
                runpy.run_path(str(PROBE), run_name="__main__")
        return exit_result.exception.code, json.loads(output.getvalue()), calls

    def test_hccl_first(self):
        code, result, calls = self.run_probe()
        self.assertEqual((code, result["status"]), (0, "INIT_PASS"))
        self.assertLess(calls.index("hccl"), calls.index("setup0"))
        self.assertEqual(calls[-2:], ["close0", "destroy"])

    def test_hixl_first_retains_first_client_until_after_secondary(self):
        code, result, calls = self.run_probe(early=True)
        self.assertEqual((code, result["status"]), (0, "INIT_PASS"))
        self.assertLess(calls.index("setup0"), calls.index("hccl"))
        self.assertLess(calls.index("hccl"), calls.index("setup1"))
        self.assertEqual(calls[-3:], ["close1", "close0", "destroy"])

    def test_primary_setup_failure_does_not_initialize_hccl_or_secondary(self):
        code, result, calls = self.run_probe(early=True, setup=(-600, 0))
        self.assertEqual((code, result["status"]), (1, "INIT_FAIL"))
        self.assertNotIn("hccl", calls)
        self.assertEqual(calls[-1], "close0")

    def test_secondary_setup_failure(self):
        code, result, calls = self.run_probe(early=True, setup=(0, -600))
        self.assertEqual((code, result["status"]), (1, "INIT_FAIL"))
        self.assertEqual(calls[-3:], ["close1", "close0", "destroy"])

    def test_hccl_failure_closes_prepared_client(self):
        code, result, calls = self.run_probe(early=True, hccl_error=True)
        self.assertEqual((code, result["status"]), (1, "INIT_FAIL"))
        self.assertIn("HCCL initialization failed", result["error"])
        self.assertEqual(calls[-1], "close0")

    def test_close_failure_is_not_reported_as_pass(self):
        for value in (-1, RuntimeError("close failed")):
            with self.subTest(value=value):
                code, result, calls = self.run_probe(early=True, close=(0, value))
                self.assertEqual((code, result["status"]), (1, "CLOSE_FAIL"))
                self.assertEqual(calls[-3:], ["close1", "close0", "destroy"])


if __name__ == "__main__":
    unittest.main()
