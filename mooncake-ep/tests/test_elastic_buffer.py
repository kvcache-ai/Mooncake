#!/usr/bin/env python3
"""Correctness smoke for the public Mooncake ElasticBuffer API.

This test is intentionally self-contained: it exercises the PR's elastic
dispatch/combine path through ``mooncake.ElasticBuffer`` and checks the result
against a deterministic PyTorch reference derived from the routing metadata.

Typical single-node usage:

    MOONCAKE_EP_NUM_LOCAL_RANKS=8 \
    torchrun --standalone --nproc_per_node=8 \
      mooncake-ep/tests/test_elastic_buffer.py --quick

For multi-node hybrid validation, launch the same script with ``torchrun
--nnodes`` and set ``MOONCAKE_EP_NUM_LOCAL_RANKS`` to the number of GPUs per
node.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

import torch
import torch.distributed as dist
import torch.testing as testing

from mooncake import ElasticBuffer


@dataclass(frozen=True)
class RoutePlan:
    topk_idx: torch.Tensor
    expected_recv_tokens: int
    expected_combine_factor: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Mooncake ElasticBuffer")
    parser.add_argument("--num-tokens", type=int, default=64)
    parser.add_argument("--max-tokens", type=int, default=0)
    parser.add_argument("--hidden", type=int, default=4096)
    parser.add_argument("--num-experts", type=int, default=256)
    parser.add_argument("--num-topk", type=int, default=8)
    parser.add_argument("--num-sms", type=int, default=24)
    parser.add_argument(
        "--route",
        choices=("alltoall", "local", "cross"),
        default="alltoall",
        help="Expert routing pattern to generate.",
    )
    parser.add_argument(
        "--allow-hybrid-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use smaller defaults suitable for reviewer smoke tests.",
    )
    return parser.parse_args()


def init_distributed(seed: int) -> tuple[int, int, int]:
    if not dist.is_initialized():
        dist.init_process_group("nccl")

    rank = dist.get_rank()
    world_size = dist.get_world_size()
    local_rank = int(os.environ.get("LOCAL_RANK", rank % torch.cuda.device_count()))
    torch.cuda.set_device(local_rank)
    torch.set_default_device("cuda")
    torch.set_default_dtype(torch.bfloat16)
    torch.manual_seed(seed + rank)
    return rank, local_rank, world_size


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


def make_input(
    *, rank: int, num_tokens: int, hidden: int, multiplier: int, addend: int = 0
) -> torch.Tensor:
    base = torch.arange(num_tokens * hidden, device="cuda", dtype=torch.float32)
    base = base.view(num_tokens, hidden)
    return (base + rank * multiplier + addend).to(torch.bfloat16).contiguous()


def check_dispatch_payload(
    *,
    rank: int,
    recv_x: torch.Tensor,
    handle,
    expected_recv_tokens: int,
    max_tokens: int,
    num_tokens: int,
    hidden: int,
    multiplier: int,
    addend: int = 0,
) -> int:
    actual = int(handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
    if actual != expected_recv_tokens:
        raise AssertionError(
            f"rank={rank}: got {actual} received tokens, "
            f"expected {expected_recv_tokens}"
        )

    src_global = handle.recv_src_metadata[:actual, 0].long()
    src_rank = torch.div(src_global, max_tokens, rounding_mode="floor")
    src_token = src_global % max_tokens
    if not bool((src_token < num_tokens).all()):
        raise AssertionError(f"rank={rank}: invalid source token index in metadata")

    base = torch.arange(num_tokens * hidden, device="cuda", dtype=torch.float32)
    base = base.view(num_tokens, hidden)
    expected = (base[src_token] + src_rank.view(-1, 1).float() * multiplier + addend)
    expected = expected.to(torch.bfloat16)
    if not torch.equal(recv_x[:actual], expected):
        diff = (recv_x[:actual].float() - expected.float()).abs().max().item()
        raise AssertionError(f"rank={rank}: dispatch payload mismatch, max_diff={diff}")
    return actual


def check_combined(
    *, rank: int, combined: torch.Tensor, expected: torch.Tensor, label: str
) -> None:
    testing.assert_close(
        combined,
        expected,
        rtol=5e-2,
        atol=1e-3,
        msg=lambda msg: f"rank={rank}: {label} combine mismatch: {msg}",
    )


def main() -> None:
    args = parse_args()
    if args.quick:
        args.num_tokens = min(args.num_tokens, 32)

    rank, _local_rank, world_size = init_distributed(args.seed)
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
        allow_hybrid_mode=args.allow_hybrid_mode,
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

    # CPU-sync dispatch: exact output extent and CPU-side expert counts.
    x0 = make_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=1_000_000,
    )
    recv0, idx0, w0, handle0, _ = buffer.dispatch(
        x0,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=1,
        do_cpu_sync=True,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    actual0 = check_dispatch_payload(
        rank=rank,
        recv_x=recv0,
        handle=handle0,
        expected_recv_tokens=route_plan.expected_recv_tokens,
        max_tokens=max_tokens,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=1_000_000,
    )
    if recv0.shape[0] != actual0:
        raise AssertionError(f"rank={rank}: CPU-sync dispatch returned extra rows")
    if len(handle0.num_recv_tokens_per_expert_list) != num_experts // world_size:
        raise AssertionError(f"rank={rank}: invalid per-expert count length")

    combined0, _, _ = buffer.combine(
        recv0.contiguous(),
        handle0,
        topk_weights=w0[:actual0].contiguous(),
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    expected0 = (x0.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
    check_combined(rank=rank, combined=combined0, expected=expected0, label="sync")

    # Cached-handle dispatch: DeepEP-style path without top-k / weight inputs.
    x1 = make_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=2_000_000,
        addend=17,
    )
    recv1, idx1, _w1, handle1, _ = buffer.dispatch(
        x1,
        handle=handle0,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    actual1 = check_dispatch_payload(
        rank=rank,
        recv_x=recv1,
        handle=handle1,
        expected_recv_tokens=route_plan.expected_recv_tokens,
        max_tokens=max_tokens,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=2_000_000,
        addend=17,
    )
    if not torch.equal(idx1[:actual1], idx0[:actual1]):
        raise AssertionError(f"rank={rank}: cached dispatch top-k metadata mismatch")
    combined1, _, _ = buffer.combine(
        recv1[:actual1].contiguous(),
        handle1,
        topk_weights=None,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    expected1 = (x1.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
    check_combined(rank=rank, combined=combined1, expected=expected1, label="cached")

    # Async no-CPU-sync dispatch: output keeps capacity, metadata provides exact count.
    x2 = make_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=3_000_000,
        addend=31,
    )
    recv2, _idx2, w2, handle2, event2 = buffer.dispatch(
        x2,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=1,
        do_cpu_sync=False,
        num_sms=args.num_sms,
        async_with_compute_stream=True,
    )
    event2.current_stream_wait()
    torch.cuda.synchronize()
    actual2 = check_dispatch_payload(
        rank=rank,
        recv_x=recv2,
        handle=handle2,
        expected_recv_tokens=route_plan.expected_recv_tokens,
        max_tokens=max_tokens,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        multiplier=3_000_000,
        addend=31,
    )
    if handle2.num_recv_tokens_per_expert_list:
        raise AssertionError(f"rank={rank}: no-CPU-sync path should not return CPU counts")
    combined2, _, event3 = buffer.combine(
        recv2[:actual2].contiguous(),
        handle2,
        topk_weights=w2[:actual2].contiguous(),
        num_sms=args.num_sms,
        async_with_compute_stream=True,
    )
    event3.current_stream_wait()
    torch.cuda.synchronize()
    expected2 = (x2.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
    check_combined(rank=rank, combined=combined2, expected=expected2, label="async")

    # Expanded dispatch layout: check output extent and metadata shape.
    expanded_recv, expanded_idx, expanded_w, expanded_handle, _ = buffer.dispatch(
        x0,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=1,
        do_expand=True,
        do_cpu_sync=True,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    expanded_actual = int(expanded_handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
    if expanded_actual != route_plan.expected_recv_tokens:
        raise AssertionError(f"rank={rank}: expanded dispatch received-token mismatch")
    expanded_output = int(expanded_handle.psum_num_recv_tokens_per_expert[-1].item())
    if expanded_recv.shape[0] != expanded_output:
        raise AssertionError(f"rank={rank}: expanded output extent mismatch")
    if expanded_w is not None and expanded_w.shape[0] != expanded_output:
        raise AssertionError(f"rank={rank}: expanded weights extent mismatch")
    if expanded_idx.shape[0] != expanded_actual:
        raise AssertionError(f"rank={rank}: expanded metadata extent mismatch")

    dist.barrier()
    if rank == 0:
        print(
            "MOONCAKE_ELASTIC_TEST_OK",
            f"world={world_size}",
            f"route={args.route}",
            f"recv={route_plan.expected_recv_tokens}",
            f"expanded={expanded_output}",
            f"scaleout={buffer.num_scaleout_ranks}",
            f"scaleup={buffer.num_scaleup_ranks}",
            flush=True,
        )
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
