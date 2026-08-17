#!/usr/bin/env python3
"""Compare Mooncake and the MUSA DeepEP low-latency IBGDA APIs.

Run one backend per process group.  The timed region is deliberately limited
to BF16 dispatch and combine, with a fixed peer-only route and no expert GEMM,
FP8 conversion, or zero-copy path. P2P is selected explicitly with
``--enable-p2p``.
"""

import argparse
import json
import os

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
import torch_musa  # noqa: F401


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("mooncake", "deepep"), required=True)
    parser.add_argument("--world-size", type=int, default=2)
    parser.add_argument("--tokens", type=int, default=64)
    parser.add_argument("--hidden", type=int, default=2048)
    parser.add_argument("--experts", type=int, default=16)
    parser.add_argument("--topk", type=int, default=8)
    parser.add_argument("--warmups", type=int, default=20)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--master-port", type=int, default=29680)
    parser.add_argument("--enable-p2p", action="store_true")
    parser.add_argument("--dispatch-only", action="store_true")
    parser.add_argument("--dispatch-pattern", choices=("token", "random"), default="token")
    parser.add_argument("--dispatch-repeats", type=int, default=1)
    return parser.parse_args()


def wait_result(event, hook):
    if hook is not None:
        hook()
    if event is not None and getattr(event, "event", event) is not None:
        event.current_stream_wait()


def build_peer_route(rank, world_size, tokens, experts, topk):
    local_experts = experts // world_size
    peer = (rank + 1) % world_size
    ids = torch.arange(topk, device="musa", dtype=torch.int64)
    ids = peer * local_experts + (ids % local_experts)
    return ids.unsqueeze(0).expand(tokens, -1).contiguous()


def init_group(rank, args):
    os.environ["MASTER_ADDR"] = "127.0.0.1"
    os.environ["MASTER_PORT"] = str(args.master_port)
    if args.backend == "mooncake":
        import torchada  # noqa: F401
        import mooncake.pg as pg

        device_filter = [
            device for device in os.getenv("DEVICE_FILTER", "").split(",") if device
        ]
        if device_filter:
            pg.set_device_filter(device_filter)
        torch.cuda.set_device(rank)
        dist.init_process_group("mooncake", rank=rank, world_size=args.world_size)
    else:
        torch.musa.set_device(rank)
        dist.init_process_group(
            "mccl",
            init_method=f"tcp://127.0.0.1:{args.master_port}",
            rank=rank,
            world_size=args.world_size,
        )
    return dist.group.WORLD


def make_backend(group, args):
    if args.backend == "mooncake":
        from mooncake.mooncake_ep_buffer import Buffer

        size = Buffer.get_ep_buffer_size_hint(
            args.tokens, args.hidden, args.world_size, args.experts
        )
        buffer = Buffer(group, size, disable_p2p=not args.enable_p2p)
        if buffer._use_fallback:
            raise RuntimeError("Mooncake did not select the IBGDA fast path")
        print(
            f"MUSA_DEEPEP_TRANSPORT backend=mooncake "
            f"p2p_enabled={buffer.runtime.p2p_enabled()}",
            flush=True,
        )
        return buffer

    import deep_ep

    size = deep_ep.Buffer.get_low_latency_rdma_size_hint(
        args.tokens, args.hidden, args.world_size, args.experts
    )
    return deep_ep.Buffer(
        group,
        num_rdma_bytes=size,
        low_latency_mode=True,
        num_qps_per_rank=args.experts // args.world_size,
        allow_nvlink_for_low_latency_mode=args.enable_p2p,
    )


def dispatch(buffer, args, x, topk_idx, active_ranks):
    if args.backend == "mooncake":
        recv_x, _, handle, event, hook = buffer.dispatch(
            x,
            topk_idx,
            active_ranks,
            num_max_dispatch_tokens_per_rank=args.tokens,
            num_experts=args.experts,
            timeout_us=-1,
            use_fp8=False,
            async_finish=False,
            return_recv_hook=False,
        )
    else:
        recv_x, _, handle, event, hook = buffer.low_latency_dispatch(
            x,
            topk_idx,
            args.tokens,
            args.experts,
            use_fp8=False,
            async_finish=False,
            return_recv_hook=False,
        )
    wait_result(event, hook)
    return recv_x, handle


def combine(buffer, args, expert_out, topk_idx, topk_weights, active_ranks, handle, out):
    if args.backend == "mooncake":
        combined, event, hook = buffer.combine(
            expert_out,
            topk_idx,
            topk_weights,
            active_ranks,
            timeout_us=-1,
            handle=handle,
            zero_copy=False,
            async_finish=False,
            return_recv_hook=False,
            out=out,
        )
    else:
        combined, event, hook = buffer.low_latency_combine(
            expert_out,
            topk_idx,
            topk_weights,
            handle,
            zero_copy=False,
            async_finish=False,
            return_recv_hook=False,
            out=out,
        )
    wait_result(event, hook)
    return combined


def worker(rank, args):
    if args.experts % args.world_size or args.topk > args.experts // args.world_size:
        raise ValueError("experts must divide world-size and topk must fit peer experts")
    if args.hidden % 128:
        raise ValueError("hidden must be divisible by 128")

    group = init_group(rank, args)
    torch.manual_seed(20260817 + rank)
    if args.dispatch_only and args.dispatch_pattern == "token":
        token_values = torch.arange(args.tokens, device="musa", dtype=torch.float32)
        x = token_values.view(-1, 1).expand(-1, args.hidden).bfloat16().contiguous()
    else:
        x = torch.randn((args.tokens, args.hidden), dtype=torch.bfloat16, device="musa")
    topk_idx = build_peer_route(rank, args.world_size, args.tokens, args.experts, args.topk)
    topk_weights = torch.full(
        (args.tokens, args.topk), 1.0 / args.topk, dtype=torch.float32, device="musa"
    )
    active_ranks = torch.ones(args.world_size, dtype=torch.int32, device="musa")
    factors = topk_idx.float() * 0.1 + 1.0
    expected = (x * (factors * topk_weights).sum(dim=1, keepdim=True)).bfloat16()

    buffer = make_backend(group, args)
    dist.barrier(group)
    source_x_by_rank = None
    if args.dispatch_only:
        # `src_info` identifies a token at the sender, not at this rank.  The
        # random-input diagnostic must therefore compare against that sender's
        # input tensor rather than local `x`.
        source_x_by_rank = [torch.empty_like(x) for _ in range(args.world_size)]
        dist.all_gather(source_x_by_rank, x, group=group)
    if args.dispatch_only:
        if args.backend != "mooncake":
            raise ValueError("--dispatch-only currently supports Mooncake only")
        for dispatch_round in range(args.dispatch_repeats):
            recv_x, handle = dispatch(buffer, args, x, topk_idx, active_ranks)
            src_info, layout_range, _, _, _ = handle
            local_experts = args.experts // args.world_size
            for local_expert in range(local_experts):
                expert_id = rank * local_experts + local_expert
                for src_rank in range(args.world_size):
                    packed = layout_range[local_expert, src_rank].item()
                    offset = packed >> 32
                    count = packed & 0xFFFFFFFF
                    if count == 0:
                        continue
                    source_ids = src_info[local_expert, offset : offset + count].long()
                    expected_rows = source_x_by_rank[src_rank][source_ids]
                    actual_rows = recv_x[local_expert, offset : offset + count]
                    if not torch.equal(actual_rows, expected_rows):
                        diff = (actual_rows != expected_rows).any(dim=1)
                        bad = int(diff.nonzero()[0].item())
                        print(
                            f"DISPATCH_MISMATCH round={dispatch_round} rank={rank} "
                            f"expert={expert_id} src_rank={src_rank} offset={offset} "
                            f"count={count} slot={bad} src_token={int(source_ids[bad])} "
                            f"actual={actual_rows[bad, :8].tolist()} "
                            f"expected={expected_rows[bad, :8].tolist()}",
                            flush=True,
                        )
                        raise AssertionError("dispatch payload mismatch")
        print(f"MUSA_DEEPEP_DISPATCH_ONLY_PASS rank={rank}", flush=True)
        dist.barrier(group)
        dist.destroy_process_group()
        return
    recv_x, handle = dispatch(buffer, args, x, topk_idx, active_ranks)
    local_experts = args.experts // args.world_size
    expert_ids = torch.arange(
        rank * local_experts, (rank + 1) * local_experts, device="musa", dtype=torch.float32
    ).view(-1, 1, 1)
    expert_out = (recv_x.float() * (expert_ids * 0.1 + 1.0)).bfloat16().contiguous()
    out = torch.empty_like(x)
    combined = combine(buffer, args, expert_out, topk_idx, topk_weights, active_ranks, handle, out)
    torch.musa.synchronize()
    torch.testing.assert_close(combined, expected, rtol=5e-2, atol=1e-3)

    for _ in range(args.warmups):
        _, handle = dispatch(buffer, args, x, topk_idx, active_ranks)
        combine(buffer, args, expert_out, topk_idx, topk_weights, active_ranks, handle, out)
    torch.musa.synchronize()

    starts = [torch.musa.Event(enable_timing=True) for _ in range(args.iterations)]
    dispatch_ends = [torch.musa.Event(enable_timing=True) for _ in range(args.iterations)]
    ends = [torch.musa.Event(enable_timing=True) for _ in range(args.iterations)]
    for start, dispatch_end, end in zip(starts, dispatch_ends, ends):
        start.record()
        _, handle = dispatch(buffer, args, x, topk_idx, active_ranks)
        dispatch_end.record()
        combine(buffer, args, expert_out, topk_idx, topk_weights, active_ranks, handle, out)
        end.record()
    torch.musa.synchronize()
    samples_us = [start.elapsed_time(end) * 1000.0 for start, end in zip(starts, ends)]
    dispatch_samples_us = [
        start.elapsed_time(dispatch_end) * 1000.0
        for start, dispatch_end in zip(starts, dispatch_ends)
    ]
    combine_samples_us = [
        dispatch_end.elapsed_time(end) * 1000.0
        for dispatch_end, end in zip(dispatch_ends, ends)
    ]
    samples_us = samples_us[1:]  # Drop the first post-warmup sample.
    dispatch_samples_us = dispatch_samples_us[1:]
    combine_samples_us = combine_samples_us[1:]
    mean_us = sum(samples_us) / len(samples_us)
    payload_bytes = args.tokens * args.topk * args.hidden * 4
    result = {
        "backend": args.backend,
        "rank": rank,
        "world_size": args.world_size,
        "tokens": args.tokens,
        "hidden": args.hidden,
        "experts": args.experts,
        "topk": args.topk,
        "route": "peer-only",
        "p2p_enabled": args.enable_p2p,
        "iterations": args.iterations,
        "avg_us": mean_us,
        "dispatch_avg_us": sum(dispatch_samples_us) / len(dispatch_samples_us),
        "combine_avg_us": sum(combine_samples_us) / len(combine_samples_us),
        "min_us": min(samples_us),
        "max_us": max(samples_us),
        "effective_gbps": payload_bytes / mean_us / 1e3,
    }
    print("MUSA_DEEPEP_COMPARE " + json.dumps(result, sort_keys=True), flush=True)
    dist.barrier(group)
    dist.destroy_process_group()


def main():
    args = parse_args()
    mp.spawn(worker, args=(args,), nprocs=args.world_size, join=True)


if __name__ == "__main__":
    main()
