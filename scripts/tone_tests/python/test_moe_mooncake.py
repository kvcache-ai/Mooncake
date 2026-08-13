"""
this test case is from https://github.com/HanHan009527/sglang/blob/a100-ci/test/manual/ep/test_moe_mooncake.py
End-to-End Integration Test for SGLang with Mooncake Elastic EP Backend.
"""

import os
import subprocess
import unittest
from types import SimpleNamespace

from sglang.srt.environ import envs
from sglang.srt.utils import kill_process_tree
from sglang.test.run_eval import run_eval
from sglang.test.server_fixtures.disaggregation_fixture import get_rdma_devices_args
from sglang.test.test_utils import (
    DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
    DEFAULT_URL_FOR_TEST,
    CustomTestCase,
    popen_launch_server,
)

ib_devices = os.getenv("MOONCAKE_DEVICE") or get_rdma_devices_args()


def dump_server_failure_diagnostics(test_class, process=None):
    print("\n===== MoE server failure diagnostics =====")
    print(f"test_class={test_class.__name__} model={test_class.model}")
    if process is not None:
        print(f"server_pid={process.pid} returncode={process.poll()}")
        status_path = f"/proc/{process.pid}/status"
        if os.path.isfile(status_path):
            try:
                with open(status_path) as status_file:
                    print(f"\n--- {status_path} ---")
                    print(status_file.read().rstrip())
            except OSError as error:
                print(f"Unable to read {status_path}: {error}")

    for path in (
        "/proc/meminfo",
        "/proc/pressure/memory",
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory.peak",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.events",
        "/sys/fs/cgroup/memory.events.local",
    ):
        if not os.path.isfile(path):
            continue
        print(f"\n--- {path} ---")
        try:
            with open(path) as diagnostic_file:
                print(diagnostic_file.read().rstrip())
        except OSError as error:
            print(f"Unable to read {path}: {error}")

    for label, command in (
        (
            "largest processes",
            ["ps", "-eo", "pid,ppid,pgid,stat,rss,vsz,comm,args", "--sort=-rss"],
        ),
        ("ROCm VRAM", ["rocm-smi", "--showmeminfo", "vram", "--json"]),
        ("ROCm utilization", ["rocm-smi", "--showuse", "--json"]),
    ):
        print(f"\n--- {label}: {' '.join(command)} ---")
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (FileNotFoundError, subprocess.SubprocessError) as error:
            print(f"Unable to run diagnostic command: {error}")
            continue
        output = "\n".join(
            part.rstrip() for part in (result.stdout, result.stderr) if part
        )
        if label == "largest processes":
            output = "\n".join(output.splitlines()[:25])
        print(output or "<no output>")
        print(f"returncode={result.returncode}")
    print("===== End MoE server failure diagnostics =====\n")


class TestMooncakeBackend(CustomTestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = "deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct"
        cls.base_url = DEFAULT_URL_FOR_TEST
        try:
            with envs.SGLANG_ENABLE_JIT_DEEPGEMM.override(False):
                cls.process = popen_launch_server(
                    cls.model,
                    cls.base_url,
                    timeout=DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
                    other_args=[
                        "--trust-remote-code",
                        "--tp",
                        "2",
                        "--elastic-ep-backend",
                        "mooncake",
                        "--mooncake-ib-device",
                        ib_devices,
                        "--mem-fraction-static",
                        "0.8",
                    ],
                )
        except Exception:
            dump_server_failure_diagnostics(cls, getattr(cls, "process", None))
            raise

    @classmethod
    def tearDownClass(cls):
        kill_process_tree(cls.process.pid)

    def test_mmlu(self):
        args = SimpleNamespace(
            base_url=self.base_url,
            model=self.model,
            eval_name="mmlu",
            num_examples=64,
            num_threads=32,
        )

        metrics = run_eval(args)
        self.assertGreater(metrics["score"], 0.5)


if __name__ == "__main__":
    unittest.main()
