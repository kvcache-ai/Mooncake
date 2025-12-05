import os
import time
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import ep


os.environ["MASTER_ADDR"] = "127.0.0.1"
os.environ["MASTER_PORT"] = "19000"


def worker(rank, num_processes, signals):
    assert num_processes % 2 == 0
    if rank < num_processes // 2:
        # Ensure correct operation before extension
        world_size = num_processes // 2
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=world_size,
        )
        tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        assert tensor.item() == sum(range(1, world_size + 1))
        if rank == 0:
            signals["extend"] = 1

        backend = dist.group.WORLD._get_backend(torch.device('cpu'))
        while True:
            num_synced_ranks = ep.get_num_synced_ranks(backend)
            if num_synced_ranks == num_processes:
                print(f"rank {rank} synced")
                break
            # Simulate ongoing operations
            tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
            dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
            assert tensor.item() == sum(range(1, world_size + 1))

        # Extend world
        ep.extend_group_size_to(backend, num_processes)
    else:
        while "extend" not in signals:
            time.sleep(1)
        print(f"Rank {rank} joining")
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )

    # Ensure correct operation after extension
    print(f"Rank {rank} before all_reduce")
    tensor = torch.tensor([rank + 1], dtype=torch.int32, device="cpu")
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    assert tensor.item() == sum(range(1, num_processes + 1)), f"Rank {rank} expected {sum(range(1, num_processes + 1))}, get {tensor.item()}"
    print(f"Rank {rank} after all_reduce")


if __name__ == '__main__':
    num_processes = 4
    mp_manager = mp.Manager()
    signals = mp_manager.dict()
    torch.multiprocessing.spawn(worker, args=(num_processes, signals), nprocs=num_processes)
