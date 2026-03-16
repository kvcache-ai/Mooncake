"""Test all collective ops in a SINGLE process group (no destroy/recreate)."""
import os, time, torch, torch.distributed as dist, torch.multiprocessing as mp
from mooncake import pg

def worker(rank, world_size, results):
    torch.cuda.set_device(rank)
    dist.init_process_group(
        backend="mooncake", rank=rank, world_size=world_size,
        pg_options=pg.MooncakeBackendOptions(
            torch.zeros((world_size,), dtype=torch.int32, device="cuda")),
    )

    errors = []

    # 1. allreduce SUM
    t = torch.tensor([rank + 1], dtype=torch.int32, device="cuda")
    dist.all_reduce(t, op=dist.ReduceOp.SUM)
    val = t.item()
    expected = sum(range(1, world_size + 1))
    if val != expected:
        errors.append(f"allreduce_sum: got {val}, expected {expected}")
    if rank == 0: print(f"  allreduce_sum: {val} (expected {expected})", flush=True)

    # 2. allreduce MAX
    t = torch.tensor([rank + 10], dtype=torch.int32, device="cuda")
    dist.all_reduce(t, op=dist.ReduceOp.MAX)
    val = t.item()
    expected = 10 + world_size - 1
    if val != expected:
        errors.append(f"allreduce_max: got {val}, expected {expected}")
    if rank == 0: print(f"  allreduce_max: {val} (expected {expected})", flush=True)

    # 3. allreduce MIN
    t = torch.tensor([rank + 10], dtype=torch.int32, device="cuda")
    dist.all_reduce(t, op=dist.ReduceOp.MIN)
    val = t.item()
    if val != 10:
        errors.append(f"allreduce_min: got {val}, expected 10")
    if rank == 0: print(f"  allreduce_min: {val} (expected 10)", flush=True)

    # 4. allreduce PRODUCT
    t = torch.tensor([2], dtype=torch.int32, device="cuda")
    dist.all_reduce(t, op=dist.ReduceOp.PRODUCT)
    val = t.item()
    expected = 2 ** world_size
    if val != expected:
        errors.append(f"allreduce_product: got {val}, expected {expected}")
    if rank == 0: print(f"  allreduce_product: {val} (expected {expected})", flush=True)

    # 5. allgather
    t = torch.tensor([rank], device="cuda")
    gathered = [torch.zeros_like(t) for _ in range(world_size)]
    dist.all_gather(gathered, t)
    vals = [g.item() for g in gathered]
    expected = list(range(world_size))
    if vals != expected:
        errors.append(f"allgather: got {vals}, expected {expected}")
    if rank == 0: print(f"  allgather: {vals} (expected {expected})", flush=True)

    # 6. gather (root=0)
    t = torch.tensor([rank], dtype=torch.int32, device="cuda")
    if rank == 0:
        gather_list = [torch.zeros_like(t) for _ in range(world_size)]
        dist.gather(t, gather_list, dst=0)
        vals = [g.item() for g in gather_list]
        expected = list(range(world_size))
        if vals != expected:
            errors.append(f"gather: got {vals}, expected {expected}")
        print(f"  gather: {vals} (expected {expected})", flush=True)
    else:
        dist.gather(t, dst=0)

    # 7. scatter (root=0)
    t = torch.zeros(1, dtype=torch.int32, device="cuda")
    if rank == 0:
        scatter_list = [torch.tensor([i], dtype=torch.int32, device="cuda")
                        for i in range(world_size)]
        dist.scatter(t, scatter_list, src=0)
    else:
        dist.scatter(t, src=0)
    val = t.item()
    if val != rank:
        errors.append(f"scatter: got {val}, expected {rank}")
    if rank == 0: print(f"  scatter: rank0 got {val} (expected 0)", flush=True)

    # 8. reduce (root=0)
    t = torch.tensor([1], dtype=torch.int32, device="cuda")
    dist.reduce(t, dst=0, op=dist.ReduceOp.SUM)
    if rank == 0:
        val = t.item()
        if val != world_size:
            errors.append(f"reduce: got {val}, expected {world_size}")
        print(f"  reduce: {val} (expected {world_size})", flush=True)

    # Report
    results[rank] = errors

    while len(results) < world_size:
        time.sleep(0.5)
    dist.destroy_process_group()


if __name__ == "__main__":
    world_size = torch.cuda.device_count()
    os.environ["MASTER_ADDR"] = "127.0.0.1"
    import socket; s = socket.socket(); s.bind(("", 0)); os.environ["MASTER_PORT"] = str(s.getsockname()[1]); s.close()
    print(f"Running ALL ops in single process group, world_size={world_size}")

    mp_manager = mp.Manager()
    results = mp_manager.dict()
    mp.spawn(worker, args=(world_size, results), nprocs=world_size, join=True)

    all_ok = True
    for r in range(world_size):
        if results[r]:
            print(f"Rank {r} ERRORS: {results[r]}")
            all_ok = False

    if all_ok:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
        exit(1)
