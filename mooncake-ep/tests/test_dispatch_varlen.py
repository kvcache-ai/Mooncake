#!/usr/bin/env python3
"""Test dispatch with variable tokens per rank (mimics official test)."""
import os
import torch
import torch_musa  # noqa: F401
import torch.distributed as dist
import torch.multiprocessing as mp


def worker(rank, world_size):
    torch.musa.set_device(rank)
    dist.init_process_group(backend="gloo", rank=rank, world_size=world_size)
    group = dist.new_group(backend="gloo")

    from mooncake.mooncake_ep_buffer import Buffer

    hidden = 2048
    num_experts = 288
    num_max_tokens = 256

    num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
        num_max_tokens, hidden, world_size, num_experts)
    buf = Buffer(group, num_ep_buffer_bytes)

    # Same as official test: variable tokens per rank
    scale = 1.0 - 0.05 * (rank / world_size)
    num_tokens = int(num_max_tokens * scale)
    print(f"[Rank {rank}] num_tokens={num_tokens}", flush=True)

    x = torch.randn(num_tokens, hidden, dtype=torch.bfloat16, device=f"musa:{rank}")
    topk_idx = torch.randint(0, num_experts, (num_tokens, 8), dtype=torch.int64, device=f"musa:{rank}")
    active_ranks = torch.ones(world_size, dtype=torch.int32, device=f"musa:{rank}")

    print(f"[Rank {rank}] Starting dispatch...", flush=True)
    try:
        recv_x, recv_count, handle, event, hook = buf.dispatch(
            x, topk_idx, active_ranks, num_max_tokens, num_experts,
            -1, use_fp8=False, async_finish=False, return_recv_hook=False)
        torch.musa.synchronize()
        print(f"[Rank {rank}] Dispatch succeeded!", flush=True)
    except Exception as e:
        print(f"[Rank {rank}] Dispatch FAILED: {e}", flush=True)


if __name__ == "__main__":
    os.environ.setdefault("MOONCAKE_EP_USE_MUSA", "1")
    os.environ.setdefault("MOONCAKE_EP_USE_TENT", "1")
    os.environ.setdefault("MUSA_LAUNCH_BLOCKING", "1")
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29501")

    world_size = torch.musa.device_count()
    mp.spawn(worker, args=(world_size,), nprocs=world_size, join=True)
