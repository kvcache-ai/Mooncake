#!/usr/bin/env python3
"""Minimal EP dispatch+combine test for MTT S5000.

Tests dispatch and combine separately to isolate the illegal memory access.
"""
import os
import sys
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

    # Test dispatch only
    x = torch.randn(num_max_tokens, hidden, dtype=torch.bfloat16, device=f"musa:{rank}")
    topk_idx = torch.randint(0, num_experts, (num_max_tokens, 8), dtype=torch.int64, device=f"musa:{rank}")
    active_ranks = torch.ones(world_size, dtype=torch.int32, device=f"musa:{rank}")

    print(f"[Rank {rank}] Starting dispatch...", flush=True)
    try:
        recv_x, recv_count, handle, event, hook = buf.dispatch(
            x, topk_idx, active_ranks, num_max_tokens, num_experts,
            -1, use_fp8=False, async_finish=False, return_recv_hook=False)
        # MUSA: hook includes barrier + recv phase launch
        if hook is not None:
            hook()
        torch.musa.synchronize()
        print(f"[Rank {rank}] Dispatch succeeded!", flush=True)
    except Exception as e:
        print(f"[Rank {rank}] Dispatch FAILED: {e}", flush=True)
        return

    # Mock expert forward (same as test_ep_grid.py)
    num_local_experts = num_experts // world_size
    recv_bf16 = recv_x  # use_fp8=False
    expert_out = torch.empty_like(recv_bf16)
    for le in range(num_local_experts):
        expert_out[le] = recv_bf16[le]
    expert_out = expert_out.to(torch.bfloat16).contiguous()

    # Test combine
    topk_weights = torch.ones(num_max_tokens, 8, dtype=torch.float32, device=f"musa:{rank}")
    out_tensor = torch.zeros_like(x)
    print(f"[Rank {rank}] Starting combine...", flush=True)
    try:
        combined_x, event, hook = buf.combine(
            expert_out, topk_idx, topk_weights, active_ranks,
            -1, handle, zero_copy=False, async_finish=False, return_recv_hook=False,
            out=out_tensor)
        # MUSA: hook includes barrier + recv phase launch
        if hook is not None:
            hook()
        torch.musa.synchronize()
        print(f"[Rank {rank}] Combine succeeded!", flush=True)
    except Exception as e:
        print(f"[Rank {rank}] Combine FAILED: {e}", flush=True)
        return

    print(f"[Rank {rank}] All tests passed!", flush=True)


if __name__ == "__main__":
    os.environ.setdefault("MOONCAKE_EP_USE_MUSA", "1")
    os.environ.setdefault("MOONCAKE_EP_USE_TENT", "1")
    os.environ.setdefault("MUSA_LAUNCH_BLOCKING", "1")
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29500")

    world_size = torch.musa.device_count()
    print(f"Running with {world_size} GPUs", flush=True)

    mp.spawn(worker, args=(world_size,), nprocs=world_size, join=True)
