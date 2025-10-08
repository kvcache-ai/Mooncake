import os
import time
import unittest
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import ep

N = 2 ** 32

def worker(rank, world_size, results, collective):
    torch.cuda.set_device(rank)
    dist.init_process_group(
        backend="mooncake",
        rank=rank,
        world_size=world_size,
        pg_options=ep.MooncakeBackendOptions(torch.zeros((world_size,), dtype=torch.int32, device="cuda")),
    )

    if collective == "all_reduce_sum":
        tensor = torch.tensor([rank + 1] * N, dtype=torch.int32, device="cuda")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        results[rank] = tensor[0].item()
        assert torch.all(tensor == tensor[0].item()) 
        print(results[rank])
        
    elif collective == "all_reduce_2d": 
        tensor = torch.tensor([[rank, -rank] for i in range(N)], dtype=torch.int32, device="cuda")
        dist.all_reduce(tensor, op=dist.ReduceOp.MAX)
        results[rank] = [tensor[0][0].item(), tensor[0][1].item()]
        first_row = tensor[0]
        all_same = torch.all(tensor == first_row, dim = 1)
        assert torch.all(all_same).item()

    else:
        raise ValueError(f"Unsupported collective: {collective}")

    while len(results) < world_size:
        time.sleep(1)

    dist.destroy_process_group()


class TestMooncakeBackend(unittest.TestCase):
    def setUp(self):
        self.world_size = torch.cuda.device_count()
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        os.environ["MASTER_PORT"] = "29500"

    def tearDown(self):
        pass

    def _spawn_and_check(self, collective):
        mp_manager = mp.Manager()
        results = mp_manager.dict()
        mp.spawn(
            worker,
            args=(self.world_size, results, collective),
            nprocs=self.world_size,
            join=True,
        )

        expected = 36 if collective == "all_reduce_sum" else [7, 0]
        for r in range(self.world_size):
            self.assertEqual(results[r], expected)

    def test_allreduce_sum(self):
        self._spawn_and_check("all_reduce_sum")
    
    def test_allreduce_2d(self):
        self._spawn_and_check("all_reduce_2d")


if __name__ == "__main__":
    unittest.main()
