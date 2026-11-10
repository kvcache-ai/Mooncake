#!/usr/bin/env python3
"""Correctness smoke for the public Mooncake ElasticBuffer API.

This test is intentionally self-contained: it exercises the PR's elastic
dispatch/combine path through the public ElasticBuffer wrapper and checks the
result against a deterministic PyTorch reference derived from the routing
metadata.

Typical single-node usage:

    MOONCAKE_EP_NUM_LOCAL_RANKS=8 \
    torchrun --standalone --nproc_per_node=8 \
      python/tests/ep/test_elastic_buffer.py --quick

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

from mooncake.mooncake_elastic_buffer import ElasticBuffer


def using_musa_backend() -> bool:
    return os.getenv("MOONCAKE_EP_USE_MUSA", "").upper() in {
        "1",
        "ON",
        "TRUE",
        "YES",
    }


def import_torchada_if_needed() -> None:
    if not using_musa_backend():
        return
    import torchada  # noqa: F401 — maps torch.cuda.* to torch.musa.* on MUSA


# Intel GPUs run the same checks through torch.xpu and the xccl backend.
DEVICE = "xpu" if hasattr(torch, "xpu") and torch.xpu.is_available() else "cuda"


def synchronize() -> None:
    getattr(torch, DEVICE).synchronize()


def distributed_barrier() -> None:
    if using_musa_backend():
        dist.barrier(device_ids=[torch.cuda.current_device()])
    else:
        dist.barrier()


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
        "--expert-alignment",
        type=int,
        default=1,
        help="expert_alignment for the expanded dispatch stage.",
    )
    parser.add_argument(
        "--transport",
        choices=("auto", "ibgda", "nccl", "ipc"),
        default=os.getenv("MOONCAKE_EP_TRANSPORT", "auto"),
        help="Device transport; auto prefers NCCL and falls back to IPC + IBGDA. "
        "XPU supports only auto/ipc.",
    )
    parser.add_argument(
        "--route",
        choices=("alltoall", "local", "cross", "empty"),
        default="alltoall",
        help="Expert routing pattern to generate; 'empty' leaves rank 0 with "
        "no received tokens.",
    )
    parser.add_argument(
        "--allow-hybrid-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--reconfigure",
        action="store_true",
        help="Rebuild the NCCL generation and verify post-update correctness.",
    )
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use smaller defaults suitable for reviewer smoke tests.",
    )
    parser.add_argument(
        "--fp8",
        action="store_true",
        help="Also check FP8 (x, sf) dispatch: exact codes and scales, "
        "compact, cached and expanded.",
    )
    return parser.parse_args()


def init_distributed(seed: int) -> tuple[int, int, int]:
    import_torchada_if_needed()
    if not dist.is_initialized():
        dist.init_process_group("xccl" if DEVICE == "xpu" else "nccl")

    rank = dist.get_rank()
    world_size = dist.get_world_size()
    accelerator = getattr(torch, DEVICE)
    local_rank = int(os.environ.get("LOCAL_RANK", rank % accelerator.device_count()))
    accelerator.set_device(local_rank)
    torch.set_default_device(DEVICE)
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

    expert_offsets = (
        torch.arange(num_topk, device=DEVICE, dtype=torch.long) % local_experts
    )

    if route == "cross" and buffer.num_scaleout_ranks > 1:
        dst_scaleout = (buffer.scaleout_rank_idx + 1) % buffer.num_scaleout_ranks
        dst_rank = dst_scaleout * buffer.num_scaleup_ranks + buffer.scaleup_rank_idx
        choices = dst_rank * local_experts + expert_offsets
        return RoutePlan(
            choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
            num_tokens,
            1,
        )

    if route == "empty":
        # Rank 0 receives nothing: it routes to rank 1, every other rank to
        # itself.
        if world_size < 2:
            raise ValueError("route 'empty' needs at least 2 ranks")
        dst_rank = 1 if rank == 0 else rank
        choices = dst_rank * local_experts + expert_offsets
        expected_recv_tokens = {0: 0, 1: 2 * num_tokens}.get(rank, num_tokens)
        return RoutePlan(
            choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
            expected_recv_tokens,
            1,
        )

    if route == "local" or (route == "cross" and buffer.num_scaleout_ranks == 1):
        choices = rank * local_experts + expert_offsets
        return RoutePlan(
            choices.view(1, num_topk).repeat(num_tokens, 1).contiguous(),
            num_tokens,
            1,
        )

    dst_ranks = (
        rank + torch.arange(num_topk, device=DEVICE, dtype=torch.long)
    ) % world_size
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
    base = torch.arange(num_tokens * hidden, device=DEVICE, dtype=torch.float32)
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

    base = torch.arange(num_tokens * hidden, device=DEVICE, dtype=torch.float32)
    base = base.view(num_tokens, hidden)
    expected = base[src_token] + src_rank.view(-1, 1).float() * multiplier + addend
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


def fp8_codes(
    src_rank: torch.Tensor, src_token: torch.Tensor, hidden: int, addend: int
) -> torch.Tensor:
    # Any byte pattern will do: dispatch only copies the codes.
    cols = torch.arange(hidden, device=DEVICE, dtype=torch.int64)
    codes = src_rank.view(-1, 1) * 37 + src_token.view(-1, 1) * 11 + cols + addend
    return (codes % 251).to(torch.uint8)


def fp8_scales(
    src_rank: torch.Tensor, src_token: torch.Tensor, num_packs: int
) -> torch.Tensor:
    # Exact in float32 and unique per (rank, token, pack); see run_fp8_checks.
    packs = torch.arange(num_packs, device=DEVICE, dtype=torch.int64)
    keys = src_rank.view(-1, 1) * 1_000_000 + src_token.view(-1, 1) * 1_000 + packs
    return keys.to(torch.float32)


def make_fp8_input(
    *, rank: int, num_tokens: int, hidden: int, addend: int, column_major_sf: bool
) -> tuple[torch.Tensor, torch.Tensor]:
    tokens = torch.arange(num_tokens, device=DEVICE, dtype=torch.int64)
    ranks = torch.full_like(tokens, rank)
    codes = fp8_codes(ranks, tokens, hidden, addend).view(torch.float8_e4m3fn)
    sf = fp8_scales(ranks, tokens, (hidden + 127) // 128)
    if column_major_sf:
        sf = sf.t().contiguous().t()
    return codes, sf


def check_fp8_rows(
    *,
    rank: int,
    label: str,
    codes: torch.Tensor,
    scales: torch.Tensor,
    src_rank: torch.Tensor,
    src_token: torch.Tensor,
    hidden: int,
    addend: int,
) -> None:
    expected_codes = fp8_codes(src_rank, src_token, hidden, addend)
    if not torch.equal(codes.view(torch.uint8), expected_codes):
        raise AssertionError(f"rank={rank}: {label} FP8 codes mismatch")
    if not torch.equal(scales, fp8_scales(src_rank, src_token, scales.shape[1])):
        raise AssertionError(f"rank={rank}: {label} FP8 scales mismatch")


def run_fp8_checks(
    *,
    args: argparse.Namespace,
    rank: int,
    world_size: int,
    max_tokens: int,
    num_experts: int,
    expert_alignment: int,
    transport_kwargs: dict,
) -> None:
    # Scales encode (rank, token, pack) as rank * 1e6 + token * 1e3 + pack,
    # which float32 holds exactly below 2**24.
    if world_size > 16 or args.num_tokens >= 1000 or args.hidden >= 128 * 1000:
        raise ValueError("--fp8 needs <= 16 ranks, < 1000 tokens, hidden < 128000")
    buffer = ElasticBuffer(
        dist.group.WORLD,
        num_max_tokens_per_rank=max_tokens,
        hidden=args.hidden,
        num_topk=args.num_topk,
        use_fp8_dispatch=True,
        deterministic=False,
        allow_hybrid_mode=args.allow_hybrid_mode,
        allow_multiple_reduction=True,
        num_gpu_timeout_secs=10,
        **transport_kwargs,
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
    weights = torch.ones(
        (args.num_tokens, args.num_topk), device=DEVICE, dtype=torch.float32
    )
    # Every rank's routing, to know which tokens this rank must receive.
    gathered = [torch.empty_like(route_plan.topk_idx) for _ in range(world_size)]
    dist.all_gather(gathered, route_plan.topk_idx)
    all_topk = torch.stack(gathered)
    num_local = num_experts // world_size
    first_local = rank * num_local
    is_local = (all_topk >= first_local) & (all_topk < first_local + num_local)
    exp_rank, exp_token = is_local.any(-1).nonzero(as_tuple=True)
    expected_src = exp_rank * max_tokens + exp_token

    def check_compact(label, recv, handle, addend):
        recv_x, recv_sf = recv
        actual = int(handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
        src = handle.recv_src_metadata[:actual, 0].long()
        if not torch.equal(torch.sort(src).values, expected_src):
            raise AssertionError(f"rank={rank}: {label} FP8 received-token mismatch")
        check_fp8_rows(
            rank=rank,
            label=label,
            codes=recv_x[:actual],
            scales=recv_sf[:actual],
            src_rank=src // max_tokens,
            src_token=src % max_tokens,
            hidden=args.hidden,
            addend=addend,
        )

    # Compact, CPU sync, column-major scales (sf_hidden_stride != 1).
    x = make_fp8_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        addend=0,
        column_major_sf=True,
    )
    recv, _, _, handle, _ = buffer.dispatch(
        x,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=1,
        do_cpu_sync=True,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    synchronize()
    check_compact("sync", recv, handle, 0)

    # Cached handle, row-major scales.
    x = make_fp8_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        addend=5,
        column_major_sf=False,
    )
    recv, _, _, _, _ = buffer.dispatch(
        x,
        handle=handle,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    synchronize()
    check_compact("cached", recv, handle, 5)

    # Expanded: each local expert's block, starting at an expert_alignment
    # boundary, holds exactly the tokens that picked that expert.
    x = make_fp8_input(
        rank=rank,
        num_tokens=args.num_tokens,
        hidden=args.hidden,
        addend=9,
        column_major_sf=True,
    )
    (recv_x, recv_sf), _, _, handle, _ = buffer.dispatch(
        x,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=expert_alignment,
        do_expand=True,
        do_cpu_sync=True,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    synchronize()
    start = 0
    for e, count in enumerate(handle.num_recv_tokens_per_expert_list):
        picked = (all_topk == first_local + e).any(-1)
        exp_rank, exp_token = picked.nonzero(as_tuple=True)
        rows = slice(start, start + count)
        key = recv_sf[rows, 0].long()
        src_rank = key // 1_000_000
        src_token = key % 1_000_000 // 1_000
        src = torch.sort(src_rank * max_tokens + src_token).values
        if not torch.equal(src, exp_rank * max_tokens + exp_token):
            raise AssertionError(
                f"rank={rank}: expanded FP8 rows of local expert {e} mismatch"
            )
        check_fp8_rows(
            rank=rank,
            label=f"expanded expert {e}",
            codes=recv_x[rows],
            scales=recv_sf[rows],
            src_rank=src_rank,
            src_token=src_token,
            hidden=args.hidden,
            addend=9,
        )
        start = (start + count + expert_alignment - 1) // expert_alignment
        start *= expert_alignment
    buffer.destroy()


def main() -> None:
    args = parse_args()
    if args.quick:
        args.num_tokens = min(args.num_tokens, 32)

    rank, _local_rank, world_size = init_distributed(args.seed)
    max_tokens = args.max_tokens or max(128, args.num_tokens)
    num_experts = args.num_experts

    if num_experts % world_size != 0:
        raise ValueError("num_experts must be divisible by world_size")

    transport_kwargs = {} if args.transport == "auto" else {"transport": args.transport}
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
        **transport_kwargs,
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
    weights = torch.ones(
        (args.num_tokens, args.num_topk), device=DEVICE, dtype=torch.float32
    )

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
    synchronize()
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
    synchronize()
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
    synchronize()
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
    synchronize()
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
    synchronize()
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
        raise AssertionError(
            f"rank={rank}: no-CPU-sync path should not return CPU counts"
        )
    combined2, _, event3 = buffer.combine(
        recv2[:actual2].contiguous(),
        handle2,
        topk_weights=w2[:actual2].contiguous(),
        num_sms=args.num_sms,
        async_with_compute_stream=True,
    )
    event3.current_stream_wait()
    synchronize()
    expected2 = (x2.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
    check_combined(rank=rank, combined=combined2, expected=expected2, label="async")

    # Expanded dispatch layout: check output extent, metadata shape, and that
    # combine sums every pick.
    expanded_recv, expanded_idx, expanded_w, expanded_handle, _ = buffer.dispatch(
        x0,
        topk_idx=route_plan.topk_idx,
        topk_weights=weights,
        num_experts=num_experts,
        num_max_tokens_per_rank=max_tokens,
        expert_alignment=args.expert_alignment,
        do_expand=True,
        do_cpu_sync=True,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    synchronize()
    expanded_actual = int(
        expanded_handle.psum_num_recv_tokens_per_scaleup_rank[-1].item()
    )
    if expanded_actual != route_plan.expected_recv_tokens:
        raise AssertionError(f"rank={rank}: expanded dispatch received-token mismatch")
    # The expanded psum holds each expert's end, then the aligned total; the
    # output ends at the last expert's end.
    expanded_output = int(expanded_handle.psum_num_recv_tokens_per_expert[-2].item())
    if expanded_recv.shape[0] != expanded_output:
        raise AssertionError(f"rank={rank}: expanded output extent mismatch")
    if expanded_w is not None and expanded_w.shape[0] != expanded_output:
        raise AssertionError(f"rank={rank}: expanded weights extent mismatch")
    if expanded_idx.shape[0] != expanded_actual:
        raise AssertionError(f"rank={rank}: expanded metadata extent mismatch")
    expanded_combined, _, _ = buffer.combine(
        expanded_recv.contiguous(),
        expanded_handle,
        num_sms=args.num_sms,
        async_with_compute_stream=False,
    )
    synchronize()
    check_combined(
        rank=rank,
        combined=expanded_combined,
        expected=(x0.float() * args.num_topk).to(torch.bfloat16),
        label="expanded",
    )

    if args.reconfigure:
        if buffer.transport != "nccl":
            raise RuntimeError("--reconfigure requires the NCCL transport")

        stale_handle = handle0
        # Exercise retirement of more than one obsolete communicator/window
        # generation before checking the new data path.
        for _ in range(2):
            buffer.update_ep_member()

        try:
            buffer.dispatch(
                x0,
                handle=stale_handle,
                num_experts=num_experts,
                num_max_tokens_per_rank=max_tokens,
                num_sms=args.num_sms,
            )
        except RuntimeError as exc:
            if "obsolete NCCL ElasticBuffer generation" not in str(exc):
                raise
        else:
            raise AssertionError(
                f"rank={rank}: cached dispatch accepted a stale EPHandle"
            )

        x3 = make_input(
            rank=rank,
            num_tokens=args.num_tokens,
            hidden=args.hidden,
            multiplier=4_000_000,
            addend=47,
        )
        recv3, _idx3, w3, handle3, _ = buffer.dispatch(
            x3,
            topk_idx=route_plan.topk_idx,
            topk_weights=weights,
            num_experts=num_experts,
            num_max_tokens_per_rank=max_tokens,
            expert_alignment=1,
            do_cpu_sync=True,
            num_sms=args.num_sms,
            async_with_compute_stream=False,
        )
        synchronize()
        actual3 = check_dispatch_payload(
            rank=rank,
            recv_x=recv3,
            handle=handle3,
            expected_recv_tokens=route_plan.expected_recv_tokens,
            max_tokens=max_tokens,
            num_tokens=args.num_tokens,
            hidden=args.hidden,
            multiplier=4_000_000,
            addend=47,
        )
        combined3, _, _ = buffer.combine(
            recv3[:actual3].contiguous(),
            handle3,
            topk_weights=w3[:actual3].contiguous(),
            num_sms=args.num_sms,
            async_with_compute_stream=False,
        )
        synchronize()
        expected3 = (x3.float() * route_plan.expected_combine_factor).to(torch.bfloat16)
        check_combined(
            rank=rank,
            combined=combined3,
            expected=expected3,
            label="post-reconfiguration",
        )

    if args.fp8:
        run_fp8_checks(
            args=args,
            rank=rank,
            world_size=world_size,
            max_tokens=max_tokens,
            num_experts=num_experts,
            expert_alignment=args.expert_alignment,
            transport_kwargs=transport_kwargs,
        )

    distributed_barrier()
    if rank == 0:
        print(
            "MOONCAKE_ELASTIC_TEST_OK",
            f"world={world_size}",
            f"route={args.route}",
            f"transport={buffer.transport}",
            f"recv={route_plan.expected_recv_tokens}",
            f"expanded={expanded_output}",
            f"scaleout={buffer.num_scaleout_ranks}",
            f"scaleup={buffer.num_scaleup_ranks}",
            f"reconfigured={args.reconfigure}",
            f"fp8={args.fp8}",
            flush=True,
        )
    buffer.destroy()
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
