import os
import torch
import torch.distributed as dist
import mooncake.ep
import time
import unittest
import torch.multiprocessing as mp


def run_latency_test(rank, world_size, backend, device, collective, data_size, results, num_iterations=100):
    # Initialize the process group in each spawned process
    torch.cuda.set_device(rank)
    dist.init_process_group(backend=backend, rank=rank, world_size=world_size)

    # Create a tensor for the collective operation
    tensor = torch.rand(data_size, device=device)

    # Synchronize before starting the test
    torch.cuda.synchronize()

    start = time.perf_counter()
    for _ in range(num_iterations):

        if collective == 'broadcast':
            dist.broadcast(tensor, src=0)
        elif collective == 'allreduce':
            dist.all_reduce(tensor)

    end = time.perf_counter()

    # Calculate average time
    avg_time = (end - start) / num_iterations

    dist.destroy_process_group()  # Destroy the process group after testing

    # Store the result
    results[rank] = avg_time

class TestMooncakeBackendPerf(unittest.TestCase):
    def setUp(self):
        self.world_size = torch.cuda.device_count()
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        os.environ["MASTER_PORT"] = "29500"

    def tearDown(self):
        pass


if __name__ == '__main__':
    devices = ['cpu', 'cuda']
    collectives = ['broadcast', 'allreduce']
    data_sizes = [2**i for i in range(10, 21)]

    # Generate tests dynamically for each combination
    for device in devices:
        for collective in collectives:
            for data_size in data_sizes:
                def test_function(self):
                    # Use mp.spawn to call the latency test
                    mp_manager = mp.Manager()
                    # Test mooncake
                    mooncake_results = mp_manager.dict()
                    mp.spawn(run_latency_test, args=(self.world_size, 'mooncake-cpu' if device == 'cpu' else 'mooncake', device, collective, data_size, mooncake_results), nprocs=self.world_size, join=True)
                    # Test baseline
                    baseline_results = mp_manager.dict()
                    mp.spawn(run_latency_test, args=(self.world_size, 'gloo' if device == 'cpu' else 'nccl', device, collective, data_size, baseline_results), nprocs=self.world_size, join=True)

                    # After all processes have completed, check the results
                    mooncake_latency = max(mooncake_results[r] for r in mooncake_results)
                    baseline_latency = max(baseline_results[r] for r in baseline_results)
                    self.assertLessEqual(mooncake_latency, 2 * baseline_latency,
                                         f"Latency of mooncake({device}) for {collective} with size {data_size} exceeded twice the baseline.")

                test_name = f"test_{device}_{collective}_{data_size}"
                setattr(TestMooncakeBackendPerf, test_name, test_function)

    unittest.main()
