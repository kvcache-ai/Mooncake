#!/usr/bin/env python3
"""Performance smoke for Mooncake ElasticBuffer dispatch/combine.

The benchmark intentionally keeps the workload simple and reproducible.  It is
not a full system benchmark; it provides a reviewer-friendly way to verify that
the new elastic path runs repeatedly, supports cached handles, and reports
per-rank effective payload bandwidth.

Typical single-node usage:

    MOONCAKE_EP_NUM_LOCAL_RANKS=8 \
    torchrun --standalone --nproc_per_node=8 \
      mooncake-ep/benchmarks/elastic_buffer_perf.py --route alltoall
"""

from __future__ import annotations

import argparse
import contextlib
import os
import time
from dataclasses import dataclass

import torch
import torch.distributed as dist
import torch.testing as testing

from mooncake.mooncake_elastic_buffer import ElasticBuffer


@dataclass(frozen=True)
class RoutePlan:
    topk_idx: torch.Tensor
    expected_recv_tokens: int
    expected_combine_factor: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Mooncake ElasticBuffer")
    parser.add_argument("--num-tokens", type=int, default=128)
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--hidden", type=int, default=4096)
    parser.add_argument("--num-experts", type=int, default=256)
    parser.add_argument("--num-topk", type=int, default=8)
    parser.add_argument("--num-sms", type=int, default=24)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument(
        "--route",
        choices=("alltoall", "local", "cross"),
        default="alltoall",
        help="Expert routing pattern to generate.",
    )
    parser.add_argument(
        "--reuse-handle",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse the first dispatch handle for later iterations.",
    )
    parser.add_argument(
        "--check-correctness",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Check combine output on each iteration.",
    )
    parser.add_argument(
        "--sync-actual-count",
        action="store_true",
        help="Synchronize and verify GPU-side received-token count each iteration.",
    )
    parser.add_argument(
        "--profile-kernels",
        action="store_true",
        help="Report selected CUDA kernel durations across all ranks.",
    )
    parser.add_argument(
        "--profile-rank",
        type=int,
        default=0,
        help="Global rank on which to enable the CUDA profiler.",
    )
    parser.add_argument("--seed", type=int, default=2026)
    return parser.parse_args()


def init_distributed(seed: int) -> tuple[int, int]:
    if not dist.is_initialized():
        dist.init_process_group("nccl")
    rank = dist.get_rank()
    local_rank = int(os.environ.get("LOCAL_RANK", rank % torch.cuda.device_count()))
    torch.cuda.set_device(local_rank)
    torch.set_default_device("cuda")
    torch.set_default_dtype(torch.bfloat16)
    torch.manual_seed(seed + rank)
    return rank, dist.get_world_size()


def make_route_plan(
    *,
    rank: int,
    world_size: int,
    buffer: ElasticBuffer,
    num_tokens: int,
    num_topk: int,
    num_experts: int,
    route: str,
) -> RoutePlan:
    local_experts = num_experts // world_size
    if local_experts <= 0:
        raise ValueError("num_experts must be at least world_size")
    expert_offsets = torch.arange(num_topk, device="cuda", dtype=torch.long) % local_experts

    if route == "cross" and buffer.num_scaleout_ranks > 1:
        dst_scaleout = (buffer.scaleout_rank_idx + 1) % buffer.num_scaleout_ranks
        dst_rank = dst_scaleout * buffer.num_scaleup_ranks + buffer.scaleup_rank_idx
        choices = dst_rank * local_experts + expert_offsets
        return RoutePlan(
            choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
            num_tokens,
            1,
        )

    if route == "local" or (route == "cross" and buffer.num_scaleout_ranks == 1):
        choices = rank * local_experts + expert_offsets
        return RoutePlan(
            choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
            num_tokens,
            1,
        )

    dst_ranks = (rank + torch.arange(num_topk, device="cuda", dtype=torch.long)) % world_size
    choices = dst_ranks * local_experts + expert_offsets
    unique_dst_ranks = int(torch.unique(dst_ranks).numel())
    return RoutePlan(
        choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
        num_tokens * unique_dst_ranks,
        unique_dst_ranks,
    )


def make_input(rank: int, iteration: int, num_tokens: int, hidden: int) -> torch.Tensor:
    base = torch.arange(num_tokens * hidden, device="cuda", dtype=torch.float32)
    base = base.view(num_tokens, hidden)
    return (base + rank * 1_000_000 + iteration * 17).to(torch.bfloat16).contiguous()


def check_output(
    *,
    rank: int,
    route: str,
    combined: torch.Tensor,
    expected: torch.Tensor,
) -> None:
    if route == "local":
        if not torch.equal(combined, expected):
            diff = (combined.float() - expected.float()).abs().max().item()
            raise AssertionError(f"rank={rank}: local-route mismatch, max_diff={diff}")
        return

    testing.assert_close(
        combined,
        expected,
        rtol=1e-2,
        atol=1e-3,
        msg=lambda msg: f"rank={rank}: {route} combine mismatch: {msg}",
    )


def main() -> None:
    args = parse_args()
    rank, world_size = init_distributed(args.seed)
    if args.profile_kernels and not 0 <= args.profile_rank < world_size:
        raise ValueError("profile-rank must identify a rank in the process group")
    max_tokens = args.max_tokens or max(128, args.num_tokens)
    num_experts = args.num_experts
    if num_experts % world_size != 0:
        raise ValueError("num_experts must be divisible by world_size")

    buffer = ElasticBuffer(
        dist.group.WORLD,
        num_max_tokens_per_rank=max_tokens,
        hidden=args.hidden,
        num_topk=args.num_topk,
        use_fp8_dispatch=False,
        deterministic=False,
        allow_hybrid_mode=True,
        allow_multiple_reduction=True,
        num_gpu_timeout_secs=10,
    )
    route_plan = make_route_plan(
        rank=rank,
        world_size=world_size,
        buffer=buffer,
        num_tokens=args.num_tokens,
        num_topk=args.num_topk,
        num_experts=num_experts,
        route=args.route,
    )
    weights = torch.ones((args.num_tokens, args.num_topk), device="cuda", dtype=torch.float32)

    def run_one(iteration: int, cached_handle):
        x = make_input(rank, iteration, args.num_tokens, args.hidden)
        dispatch_start = torch.cuda.Event(enable_timing=True)
        dispatch_end = torch.cuda.Event(enable_timing=True)
        combine_end = torch.cuda.Event(enable_timing=True)

        use_cached = args.reuse_handle and cached_handle is not None
        dispatch_start.record()
        recv_x, _recv_idx, recv_weights, handle, _ = buffer.dispatch(
            x,
            topk_idx=None if use_cached else route_plan.topk_idx,
            topk_weights=None if use_cached else weights,
            num_experts=num_experts,
            num_max_tokens_per_rank=max_tokens,
            expert_alignment=1,
            handle=cached_handle if use_cached else None,
            do_cpu_sync=True if not use_cached else None,
            num_sms=args.num_sms,
            async_with_compute_stream=False,
        )
        dispatch_end.record()

        actual_recv_tokens = route_plan.expected_recv_tokens
        if args.sync_actual_count:
            actual_recv_tokens = int(handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
            if actual_recv_tokens != route_plan.expected_recv_tokens:
                raise AssertionError(
                    f"rank={rank}: got {actual_recv_tokens} received tokens, "
                    f"expected {route_plan.expected_recv_tokens}"
                )

        combined, _combined_weights, _ = buffer.combine(
            recv_x[:actual_recv_tokens].contiguous(),
            handle,
            topk_weights=(
                recv_weights[:actual_recv_tokens].contiguous()
                if recv_weights is not None
                else None
            ),
            num_sms=args.num_sms,
            async_with_compute_stream=False,
        )
        combine_end.record()
        torch.cuda.synchronize()

        if args.check_correctness:
            expected = (x.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
            check_output(rank=rank, route=args.route, combined=combined, expected=expected)

        return (
            handle,
            dispatch_start.elapsed_time(dispatch_end),
            dispatch_end.elapsed_time(combine_end),
            actual_recv_tokens,
        )

    cached_handle = None
    for i in range(args.warmup):
        cached_handle, _dispatch_ms, _combine_ms, _actual = run_one(i, cached_handle)

    dist.barrier()
    torch.cuda.synchronize()
    dispatch_ms = []
    combine_ms = []
    recv_tokens = []
    wall_start = time.time()
    profile_this_rank = args.profile_kernels and rank == args.profile_rank
    profiler_context = (
        torch.profiler.profile(
            activities=[torch.profiler.ProfilerActivity.CUDA],
            record_shapes=False,
            with_stack=False,
            acc_events=True,
        )
        if profile_this_rank
        else contextlib.nullcontext()
    )
    with profiler_context as profiler:
        for i in range(args.iters):
            cached_handle, d_ms, c_ms, actual = run_one(
                args.warmup + i, cached_handle
            )
            dispatch_ms.append(d_ms)
            combine_ms.append(c_ms)
            recv_tokens.append(actual)
    torch.cuda.synchronize()
    dist.barrier()
    wall_seconds = time.time() - wall_start

    profiled_kernel_names = (
        "hybrid_dispatch_impl",
        "dispatch_copy_epilogue_impl",
        "hybrid_combine_impl",
        "combine_reduce_epilogue_impl",
    )
    kernel_profile = torch.zeros(
        (len(profiled_kernel_names), 2), device="cuda", dtype=torch.float64
    )
    if profile_this_rank:
        for event in profiler.key_averages():
            for index, kernel_name in enumerate(profiled_kernel_names):
                if kernel_name not in event.key:
                    continue
                device_time_total = getattr(
                    event,
                    "self_device_time_total",
                    getattr(event, "self_cuda_time_total", 0.0),
                )
                kernel_profile[index, 0] += device_time_total
                kernel_profile[index, 1] += event.count

    stats = torch.tensor(
        [
            sum(dispatch_ms) / len(dispatch_ms),
            sum(combine_ms) / len(combine_ms),
            min(dispatch_ms),
            max(dispatch_ms),
            min(combine_ms),
            max(combine_ms),
            sum(recv_tokens) / len(recv_tokens),
            wall_seconds,
        ],
        device="cuda",
        dtype=torch.float64,
    )
    gathered = [torch.empty_like(stats) for _ in range(world_size)]
    dist.all_gather(gathered, stats)
    gathered_kernel_profile = [
        torch.empty_like(kernel_profile) for _ in range(world_size)
    ]
    dist.all_gather(gathered_kernel_profile, kernel_profile)

    if rank == 0:
        table = torch.stack(gathered).cpu()
        payload_bytes = table[:, 6].mean().item() * args.hidden * 2
        dispatch_avg_ms = table[:, 0].mean().item()
        combine_avg_ms = table[:, 1].mean().item()
        # An EP step advances at its slowest rank, not at the rank-average time.
        dispatch_critical_ms = table[:, 0].max().item()
        combine_critical_ms = table[:, 1].max().item()
        e2e_critical_ms = (table[:, 0] + table[:, 1]).max().item()
        print(
            "MOONCAKE_ELASTIC_PERF_OK",
            f"world={world_size}",
            f"route={args.route}",
            f"reuse_handle={int(args.reuse_handle)}",
            f"tokens={args.num_tokens}",
            f"hidden={args.hidden}",
            f"topk={args.num_topk}",
            f"channels_per_sm={cached_handle.num_channels_per_sm}",
            f"scaleout={buffer.num_scaleout_ranks}",
            f"scaleup={buffer.num_scaleup_ranks}",
            f"dispatch_avg_ms={dispatch_avg_ms:.3f}",
            f"combine_avg_ms={combine_avg_ms:.3f}",
            f"dispatch_critical_ms={dispatch_critical_ms:.3f}",
            f"combine_critical_ms={combine_critical_ms:.3f}",
            f"dispatch_combine_critical_ms={e2e_critical_ms:.3f}",
            f"recv_tokens_avg={table[:, 6].mean().item():.1f}",
            f"effective_payload_MB_per_rank={payload_bytes / 1e6:.1f}",
            f"dispatch_effective_GBps={payload_bytes / dispatch_avg_ms / 1e6:.2f}",
            f"combine_effective_GBps={payload_bytes / combine_avg_ms / 1e6:.2f}",
            flush=True,
        )
        if args.profile_kernels:
            kernel_table = torch.stack(gathered_kernel_profile).cpu()
            for index, kernel_name in enumerate(profiled_kernel_names):
                totals = kernel_table[:, index, 0]
                counts = kernel_table[:, index, 1]
                valid = counts > 0
                per_rank_us = totals[valid] / counts[valid]
                print(
                    "MOONCAKE_ELASTIC_KERNEL_PROFILE",
                    f"kernel={kernel_name}",
                    f"profile_rank={args.profile_rank}",
                    f"avg_us={per_rank_us.mean().item():.2f}",
                    f"critical_us={per_rank_us.max().item():.2f}",
                    f"min_us={per_rank_us.min().item():.2f}",
                    f"count_min={int(counts[valid].min().item())}",
                    f"count_max={int(counts[valid].max().item())}",
                    flush=True,
                )

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
