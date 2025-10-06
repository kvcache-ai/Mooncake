import os
import time
import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import ep


os.environ["MASTER_ADDR"] = "127.0.0.1"
os.environ["MASTER_PORT"] = "19000"


def worker(rank, num_processes, sync_dict):
    assert num_processes % 2 == 0
    torch.cuda.set_device(rank)
    if rank < num_processes // 2:
        world_size = num_processes // 2
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=world_size,
        )
        dist.barrier()
        print(f"rank {rank} after barrier")
        dist.destroy_process_group()
        time.sleep(1)
        sync_dict[f"sync{rank}"] = 1
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )
        dist.barrier()
        while "done" not in sync_dict:
            time.sleep(1)
    else:
        while "sync0" not in sync_dict:
            time.sleep(1)
        while "sync1" not in sync_dict:
            time.sleep(1)
        print(f"rank {rank} after sync")
        dist.init_process_group(
            backend="mooncake-cpu",
            rank=rank,
            world_size=num_processes,
        )
        dist.barrier()
        if rank == num_processes // 2:
            sync_dict["done"] = 1


if __name__ == '__main__':
    num_processes = 4
    mp_manager = mp.Manager()
    sync_dict = mp_manager.dict()
    torch.multiprocessing.spawn(worker, args=(num_processes, sync_dict), nprocs=num_processes)
