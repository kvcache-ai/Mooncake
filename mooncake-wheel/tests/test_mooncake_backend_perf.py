import os
import torch
import torch.distributed as dist
import mooncake.pg
import time
import unittest
import torch.multiprocessing as mp


def run_latency_test(rank, world_size, backend, device, collective, data_size, results, num_iterations=100):
    # Initialize the process group in each spawned process
    torch.cuda.set_device(rank)
    dist.init_process_group(backend=backend, rank=rank, world_size=world_size)

    # Create a tensor for the collective operation
    tensor = torch.rand(data_size, device=device)

    gathered = [torch.zeros_like(tensor) for _ in range(world_size)]

    # Warm up
    for _ in range(num_iterations):
        if collective == 'broadcast':
            dist.broadcast(tensor, src=0)
        elif collective == 'allreduce':
            dist.all_reduce(tensor)
        elif collective == 'allgather':
            dist.all_gather(gathered, tensor)

    # Synchronize before starting the test
    torch.cuda.synchronize()

    start = time.perf_counter()
    for _ in range(num_iterations):
        if collective == 'broadcast':
            dist.broadcast(tensor, src=0)
        elif collective == 'allreduce':
            dist.all_reduce(tensor)
        elif collective == 'allgather':
            dist.all_gather(gathered, tensor)

    torch.cuda.synchronize()

    end = time.perf_counter()

    # Calculate average time
    avg_time = (end - start) / num_iterations

    # Store the result
    results[rank] = avg_time

    while len(results) < world_size:
        time.sleep(1)

    dist.destroy_process_group()  # Destroy the process group after testing

class TestMooncakeBackendPerf(unittest.TestCase):
    def setUp(self):
        self.world_size = torch.cuda.device_count()
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        os.environ["MASTER_PORT"] = "29500"

    def tearDown(self):
        pass

    def do_test(self, device, collective, data_size):
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
        self.assertLessEqual(mooncake_latency, 10 * baseline_latency,
                             f"Latency of mooncake({device}) for {collective} with size {data_size} exceeded 10 times the baseline.")

    # cpu + allgather
    def test_cpu_allgather_1024(self):
        self.do_test("cpu", "allgather", 1024)

    # cpu + allreduce
    def test_cpu_allreduce_1024(self):
        self.do_test("cpu", "allreduce", 1024)

    # cpu + broadcast
    def test_cpu_broadcast_1024(self):
        self.do_test("cpu", "broadcast", 1024)

    # cuda + allgather
    def test_cuda_allgather_1024(self):
        self.do_test("cuda", "allgather", 1024)

    # cuda + allreduce
    def test_cuda_allreduce_1024(self):
        self.do_test("cuda", "allreduce", 1024)

    # cuda + broadcast
    def test_cuda_broadcast_1024(self):
        self.do_test("cuda", "broadcast", 1024)


if __name__ == "__main__":
    unittest.main()
