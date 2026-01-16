import os
import time
import unittest
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import pg


def worker(rank, world_size, results, collective):
    torch.cuda.set_device(rank)
    dist.init_process_group(
        backend="mooncake",
        rank=rank,
        world_size=world_size,
        pg_options=pg.MooncakeBackendOptions(torch.zeros((world_size,), dtype=torch.int32, device="cuda")),
    )

    if collective == "all_reduce":
        tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cuda")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        results[rank] = tensor.item()

    elif collective == "all_gather":
        tensor = torch.tensor([rank], device="cuda")
        gathered = [torch.zeros_like(tensor) for _ in range(world_size)]
        dist.all_gather(gathered, tensor)
        results[rank] = [t.item() for t in gathered]

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

    def _spawn_and_check(self, collective, expected_fn):
        mp_manager = mp.Manager()
        results = mp_manager.dict()
        mp.spawn(
            worker,
            args=(self.world_size, results, collective),
            nprocs=self.world_size,
            join=True,
        )

        expected = expected_fn(self.world_size)
        for r in range(self.world_size):
            self.assertEqual(results[r], expected)

    def test_allreduce(self):
        # Expected sum = 1 + 2 + 3 + 4 = 10
        self._spawn_and_check("all_reduce", lambda size: sum(range(1, size + 1)))

    def test_allgather(self):
        # Expected gather = [0, 1, 2, 3]
        self._spawn_and_check("all_gather", lambda size: list(range(size)))


if __name__ == "__main__":
    unittest.main()
