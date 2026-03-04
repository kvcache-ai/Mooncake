'''
this test case is from https://github.com/HanHan009527/sglang/blob/a100-ci/test/manual/ep/test_moe_mooncake.py
End-to-End Integration Test for SGLang with Mooncake Elastic EP Backend.
'''

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

ib_devices = get_rdma_devices_args()



class TestDPAttn(CustomTestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = "deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct"
        cls.base_url = DEFAULT_URL_FOR_TEST
        with envs.SGLANG_ENABLE_JIT_DEEPGEMM.override(False):
            cls.process = popen_launch_server(
                cls.model,
                cls.base_url,
                timeout=DEFAULT_TIMEOUT_FOR_SERVER_LAUNCH,
                other_args=[
                    "--trust-remote-code",
                    "--tp",
                    "2",
                    "--dp",
                    "2",
                    "--enable-dp-attention",
                    "--elastic-ep-backend",
                    "mooncake",
                    "--mooncake-ib-device",
                    ib_devices,
                ],
            )

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
