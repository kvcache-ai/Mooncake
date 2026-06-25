import argparse
import os
import time
from typing import List

import torch
import torch.distributed as dist
import torch.multiprocessing as mp


def _sizes(minbytes: int, maxbytes: int, factor: int) -> List[int]:
    out = []
    size = minbytes
    while size <= maxbytes:
        out.append(size)
        size *= factor
    return out


def _fmt_size(nbytes: int) -> str:
    if nbytes >= 1024 * 1024 and nbytes % (1024 * 1024) == 0:
        return f"{nbytes // (1024 * 1024)}M"
    if nbytes >= 1024 and nbytes % 1024 == 0:
        return f"{nbytes // 1024}K"
    return str(nbytes)


def _wrong_count(output: torch.Tensor, world_size: int, count: int) -> int:
    wrong = 0
    for peer in range(world_size):
        expected = torch.full((count,), float(peer + 1), device=output.device,
                              dtype=output.dtype)
        wrong += int((output[peer * count:(peer + 1) * count] != expected).sum().item())
    return wrong


def _worker(local_rank: int, args) -> None:
    if "RANK" in os.environ:
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        local_rank = int(os.environ.get("LOCAL_RANK", local_rank))
    else:
        rank = local_rank
        world_size = args.ngpus
        os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
        os.environ.setdefault("MASTER_PORT", str(args.master_port))

    torch.cuda.set_device(local_rank)
    dist.init_process_group("gloo", rank=rank, world_size=world_size)

    import mooncake.pg as pg
    pg.ipc_enable_peer_access(world_size)

    dtype = torch.float32
    if rank == 0:
        print("# CUDA IPC direct-output allgather PoC", flush=True)
        print(f"# mode={args.mode}", flush=True)
        print("# size        count         time_us  algbw_GBps  wrong", flush=True)

    for nbytes in _sizes(args.minbytes, args.maxbytes, args.factor):
        count = nbytes // torch.empty((), dtype=dtype).element_size()
        if count <= 0:
            continue

        inp = torch.full((count,), float(rank + 1), device="cuda", dtype=dtype)
        # CUDA IPC opens the base allocation behind an exported handle.  Use a
        # raw cudaMalloc-backed tensor so the exported pointer has no PyTorch
        # caching-allocator suballocation offset.
        out = pg.ipc_alloc_like(count * world_size, inp)

        local_handle = pg.ipc_export_tensor(out)
        handles = [None for _ in range(world_size)]
        dist.all_gather_object(handles, local_handle)
        out_ptrs = pg.ipc_open_output_handles(handles, out, rank)
        out_ptr_tensor = None
        if args.mode in ("store", "store_signal"):
            out_ptr_tensor = pg.ipc_pack_output_ptrs(out_ptrs, out)

        signal_ptrs = None
        signal_ptr_tensor = None
        if args.mode == "store_signal":
            signal = pg.ipc_alloc_int32(world_size, inp)
            local_signal_handle = pg.ipc_export_tensor(signal)
            signal_handles = [None for _ in range(world_size)]
            dist.all_gather_object(signal_handles, local_signal_handle)
            signal_ptrs = pg.ipc_open_output_handles(signal_handles, signal, rank)
            signal_ptr_tensor = pg.ipc_pack_output_ptrs(signal_ptrs, signal)
        dist.barrier()

        seq = 1

        def run_allgather() -> None:
            nonlocal seq
            if args.mode == "store":
                pg.ipc_allgather_store(inp, out_ptr_tensor, rank, nbytes)
            elif args.mode == "store_signal":
                pg.ipc_allgather_store_signal(inp, out_ptr_tensor,
                                              signal_ptr_tensor, rank, seq,
                                              nbytes)
                seq += 1
            else:
                pg.ipc_allgather_memcpy(inp, out_ptrs, rank, nbytes)

        for _ in range(args.warmup_iters):
            run_allgather()
        torch.cuda.synchronize()
        dist.barrier()

        start = time.perf_counter()
        for _ in range(args.iters):
            run_allgather()
        torch.cuda.synchronize()
        elapsed = time.perf_counter() - start

        elapsed_tensor = torch.tensor([elapsed], dtype=torch.float64)
        dist.all_reduce(elapsed_tensor, op=dist.ReduceOp.MAX)
        avg_sec = float(elapsed_tensor.item()) / max(1, args.iters)

        dist.barrier()
        wrong = 0
        if args.check:
            torch.cuda.synchronize()
            wrong = _wrong_count(out, world_size, count)
            wrong_tensor = torch.tensor([wrong], dtype=torch.int64)
            dist.all_reduce(wrong_tensor, op=dist.ReduceOp.SUM)
            wrong = int(wrong_tensor.item())

        if rank == 0:
            algbw = (nbytes / 1e9) / avg_sec if avg_sec > 0 else 0.0
            print(f"{_fmt_size(nbytes):>10} {count:>12} {avg_sec * 1e6:>9.2f} "
                  f"{algbw:>11.2f} {wrong:>6}", flush=True)

        dist.barrier()
        pg.ipc_close_handles(out_ptrs, rank)
        if signal_ptrs is not None:
            pg.ipc_close_handles(signal_ptrs, rank)
        dist.barrier()

    dist.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ngpus", "-g", type=int, default=2)
    parser.add_argument("-b", "--minbytes", type=int, default=16 * 1024)
    parser.add_argument("-e", "--maxbytes", type=int, default=1024 * 1024)
    parser.add_argument("-f", "--factor", type=int, default=8)
    parser.add_argument("-n", "--iters", type=int, default=100)
    parser.add_argument("-w", "--warmup-iters", type=int, default=20)
    parser.add_argument("-c", "--check", type=int, default=1)
    parser.add_argument("--mode", choices=("store", "store_signal", "memcpy"),
                        default="store")
    parser.add_argument("--master-port", type=int, default=29999)
    args = parser.parse_args()

    if "RANK" in os.environ:
        _worker(int(os.environ.get("LOCAL_RANK", 0)), args)
    else:
        mp.spawn(_worker, args=(args,), nprocs=args.ngpus)


if __name__ == "__main__":
    main()
