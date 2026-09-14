"""GPU multi-buffer PUT/GET smoke test against an isolated NOF store."""

import os
import time
import unittest
import uuid


def log_progress(message):
    print(f"[e2e] {message}", flush=True)


@unittest.skipUnless(
    os.getenv("MOONCAKE_NOF_STAGING_TEST_MASTER"),
    "Run through run_nof_gpu_staging_e2e.sh",
)
class TestNofGpuStaging(unittest.TestCase):
    def setUp(self):
        import torch
        from store import MooncakeDistributedStore, ReplicateConfig

        self.assertTrue(torch.cuda.is_available(), "A CUDA GPU is required")
        self.torch = torch
        self.buffers = []
        self.store = MooncakeDistributedStore()
        self.addCleanup(self.store.close)
        master = os.environ["MOONCAKE_NOF_STAGING_TEST_MASTER"]
        log_progress(f"Store setup start: master={master}")
        started = time.perf_counter()
        result = self.store.setup(
            os.environ["MOONCAKE_NOF_STAGING_TEST_HOST"],
            "P2PHANDSHAKE",
            0,
            16 * 1024 * 1024,
            "tcp",
            "",
            master,
        )
        log_progress(
            f"Store setup returned: rc={result}, "
            f"elapsed_ms={(time.perf_counter() - started) * 1000:.2f}"
        )
        self.assertEqual(result, 0)
        self.replication = ReplicateConfig()
        self.replication.replica_num = 0
        self.replication.nof_replica_num = 1

    def test_gpu_multi_buffer_nof_roundtrip(self):
        torch = self.torch
        num_keys, num_slices, width, gap = 2, 3, 1024, 128
        log_progress("Preparing and registering GPU buffers")
        source = torch.arange(
            num_keys * num_slices * (width + gap),
            dtype=torch.int32,
            device="cuda",
        ).view(num_keys, num_slices, width + gap)
        destination = torch.full_like(source, -1)
        expected = source[:, :, :width].clone()
        self.buffers = [source, destination]
        for tensor in self.buffers:
            self.assertEqual(
                self.store.register_buffer(
                    tensor.data_ptr(), tensor.numel() * tensor.element_size()
                ),
                0,
            )

        keys = [f"nof-staging-{uuid.uuid4().hex}" for _ in range(num_keys)]
        sizes = [[width * source.element_size()] * num_slices for _ in keys]
        source_ptrs = [[row.data_ptr() for row in item] for item in source]
        destination_ptrs = [[row.data_ptr() for row in item] for item in destination]
        torch.cuda.synchronize()
        total_bytes = sum(sum(key_sizes) for key_sizes in sizes)
        log_progress(f"PUT start: keys={num_keys}, bytes={total_bytes}")
        started = time.perf_counter()
        results = self.store.batch_put_from_multi_buffers(
            keys, source_ptrs, sizes, self.replication
        )
        log_progress(
            f"PUT returned: results={results}, "
            f"elapsed_ms={(time.perf_counter() - started) * 1000:.2f}"
        )
        self.assertEqual(results, [0] * num_keys)
        log_progress("Checking NOF replicas")
        replicas = self.store.batch_get_replica_desc(keys)
        for key in keys:
            self.assertEqual(len(replicas[key]), 1)
            self.assertTrue(replicas[key][0].is_nof_replica())
        log_progress("NOF replicas verified")

        log_progress("Overwriting source GPU buffers")
        source.zero_()
        torch.cuda.synchronize()
        log_progress(f"GET start: keys={num_keys}, bytes={total_bytes}")
        started = time.perf_counter()
        results = self.store.batch_get_into_multi_buffers(keys, destination_ptrs, sizes)
        log_progress(
            f"GET returned: results={results}, "
            f"elapsed_ms={(time.perf_counter() - started) * 1000:.2f}"
        )
        self.assertEqual(results, [sum(sizes[0])] * num_keys)
        log_progress("Waiting for GPU copies to finish")
        torch.cuda.synchronize()
        log_progress("GPU synchronized; checking payload and gaps")
        self.assertTrue(torch.equal(destination[:, :, :width], expected))
        self.assertTrue(torch.all(destination[:, :, width:] == -1).item())
        log_progress("PASS: payload and gaps verified")


if __name__ == "__main__":
    unittest.main()
