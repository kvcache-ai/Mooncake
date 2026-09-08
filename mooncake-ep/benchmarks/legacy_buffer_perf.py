#!/usr/bin/env python3
"""Native-core legacy EP benchmark with Python distributed bootstrap.

Launch with torchrun. Python initializes the Mooncake process group and owns
the fixed tensor storage; the timed dispatch/combine loop executes entirely in
the torch-free C++ EP module.
"""

import argparse
import os

import torch
import torch.distributed as dist

import mooncake._ep as native_ep
import mooncake.pg  # Registers the Mooncake process-group backend for bootstrap.
from mooncake.mooncake_ep_buffer import Buffer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", type=int, default=128)
    parser.add_argument("--hidden", type=int, default=7168)
    parser.add_argument("--experts", type=int, default=288)
    parser.add_argument("--topk", type=int, default=8)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--iterations", type=int, default=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    local_rank = int(os.environ["LOCAL_RANK"])
    torch.cuda.set_device(local_rank)
    dist.init_process_group("mooncake")
    world_size = dist.get_world_size()
    rank = dist.get_rank()
    group = dist.new_group(list(range(world_size)))

    if args.experts % world_size:
        raise ValueError("--experts must be divisible by world size")
    if args.hidden % 128:
        raise ValueError("--hidden must be divisible by 128")

    torch.manual_seed(rank)
    num_local_experts = args.experts // world_size
    buffer_bytes = Buffer.get_ep_buffer_size_hint(
        args.tokens, args.hidden, world_size, args.experts
    )
    buffer = Buffer(group, num_ep_buffer_bytes=buffer_bytes)
    if buffer._use_fallback:
        raise RuntimeError("native-core benchmark requires the EP fast path")

    x = torch.randn(
        (args.tokens, args.hidden), dtype=torch.bfloat16, device="cuda"
    )
    scores = torch.randn(
        (args.tokens, args.experts), dtype=torch.float32, device="cuda"
    )
    topk_idx = torch.topk(scores, args.topk, dim=-1).indices.contiguous()
    topk_weights = torch.rand(
        (args.tokens, args.topk), dtype=torch.float32, device="cuda"
    )
    active_ranks = torch.ones(world_size, dtype=torch.int32, device="cuda")
    recv_tokens = world_size * args.tokens
    expert_x = torch.randn(
        (num_local_experts, recv_tokens, args.hidden),
        dtype=torch.bfloat16,
        device="cuda",
    )
    packed_recv_x = torch.empty_like(expert_x)
    packed_recv_count = torch.empty(
        num_local_experts, dtype=torch.int32, device="cuda"
    )
    packed_recv_src_info = torch.empty(
        (num_local_experts, recv_tokens), dtype=torch.int32, device="cuda"
    )
    packed_recv_layout_range = torch.empty(
        (num_local_experts, world_size), dtype=torch.int64, device="cuda"
    )
    combined_x = torch.empty_like(x)
    cache_flush = torch.empty(int(256e6 // 4), dtype=torch.int32, device="cuda")

    tensors = native_ep._LegacyBufferPerfTensors()
    tensors.x_ptr = x.data_ptr()
    tensors.topk_idx_ptr = topk_idx.data_ptr()
    tensors.topk_weights_ptr = topk_weights.data_ptr()
    tensors.active_ranks_ptr = active_ranks.data_ptr()
    tensors.expert_x_ptr = expert_x.data_ptr()
    tensors.packed_recv_x_ptr = packed_recv_x.data_ptr()
    tensors.packed_recv_count_ptr = packed_recv_count.data_ptr()
    tensors.packed_recv_src_info_ptr = packed_recv_src_info.data_ptr()
    tensors.packed_recv_layout_range_ptr = packed_recv_layout_range.data_ptr()
    tensors.combined_x_ptr = combined_x.data_ptr()

    config = native_ep._LegacyBufferPerfConfig()
    config.num_tokens = args.tokens
    config.hidden = args.hidden
    config.num_topk = args.topk
    config.num_local_experts = num_local_experts
    config.num_max_dispatch_tokens_per_rank = args.tokens
    config.num_experts = args.experts
    config.warmups = args.warmups
    config.iterations = args.iterations
    config.compute_stream_ptr = torch.cuda.current_stream().cuda_stream

    dist.barrier(group=group)
    cache_flush.zero_()
    result = native_ep._benchmark_legacy_buffer(buffer.runtime, tensors, config)
    selections = topk_idx.numel()
    payload_bytes = selections * (args.hidden * 4)
    bandwidth = payload_bytes / result.average_us / 1e3
    print(
        f"[rank {rank}] Native dispatch + combine: {bandwidth:.2f} GB/s, "
        f"avg_t={result.average_us:.2f} us, min_t={result.min_us:.2f} us, "
        f"max_t={result.max_us:.2f} us",
        flush=True,
    )
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
