#!/usr/bin/env python3
"""Mooncake EP (Expert Parallel) dispatch/combine benchmark.

Measures throughput and tail latency of the Mooncake EP Buffer's
dispatch/combine cycle under uniform, k-hot incast, and Zipfian routing
patterns.
"""

import argparse
import json
import os
import sys

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from metrics import (
    assemble_json_output,
    compute_global_expert_load,
    write_json_output,
)
from routing import ROUTING_MODES

from mooncake.mooncake_ep_buffer import Buffer


def parse_args():
    # Pre-parse --config to load JSON defaults before full parsing
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default=None)
    pre_args, _ = pre_parser.parse_known_args()

    config_defaults = {}
    if pre_args.config:
        with open(pre_args.config) as f:
            config_defaults = json.load(f)

    parser = argparse.ArgumentParser(
        description="Mooncake EP dispatch/combine benchmark"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to JSON config file (CLI flags override config values)",
    )
    parser.add_argument(
        "--backend",
        type=str,
        default="cuda",
        help="Device backend label for output JSON (default: cuda)",
    )
    parser.add_argument(
        "--num-ranks",
        type=int,
        default=8,
        help="Number of EP ranks / GPUs",
    )
    parser.add_argument(
        "--num-experts",
        type=int,
        default=256,
        help="Total number of experts across all ranks",
    )
    parser.add_argument(
        "--hidden-size",
        type=int,
        default=7168,
        help="Hidden dimension",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=8,
        help="Number of experts each token routes to",
    )
    parser.add_argument(
        "--num-tokens",
        type=int,
        default=1024,
        help="Number of tokens per rank",
    )
    parser.add_argument(
        "--dtype",
        type=str,
        default="bf16",
        choices=["bf16", "fp8"],
        help="Data type for dispatch (default: bf16)",
    )
    parser.add_argument(
        "--routing-mode",
        type=str,
        default="uniform",
        choices=["uniform", "k_hot", "zipf"],
        help="Routing pattern (default: uniform)",
    )
    parser.add_argument(
        "--hot-experts",
        type=int,
        default=32,
        help="Number of hot experts for k_hot mode (default: 32)",
    )
    parser.add_argument(
        "--hot-fraction",
        type=float,
        default=0.9,
        help="Fraction of tokens routed to hot experts in k_hot mode (default: 0.9)",
    )
    parser.add_argument(
        "--zipf-alpha",
        type=float,
        default=1.0,
        help="Zipf alpha parameter for zipf mode (default: 1.0)",
    )
    parser.add_argument(
        "--zero-copy",
        action="store_true",
        help="Use zero-copy combine via get_next_combine_buffer",
    )
    parser.add_argument(
        "--async-finish",
        action="store_true",
        help="Use async_finish for dispatch/combine (event-based sync)",
    )
    parser.add_argument(
        "--return-recv-hook",
        action="store_true",
        help="Use return_recv_hook for dispatch/combine (hook-based sync)",
    )
    parser.add_argument(
        "--pg-backend",
        type=str,
        default="nccl",
        choices=["nccl", "mooncake"],
        help="Process group backend (default: nccl; mooncake requires RDMA)",
    )
    parser.add_argument(
        "--warmup-iters",
        type=int,
        default=20,
        help="Number of warmup iterations (default: 20)",
    )
    parser.add_argument(
        "--iters",
        type=int,
        default=100,
        help="Number of measured iterations (default: 100)",
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="Base random seed (default: 0)"
    )
    parser.add_argument(
        "--json-output",
        type=str,
        default=None,
        help="Path to write JSON output (default: stdout on rank 0)",
    )
    parser.add_argument(
        "--master-addr",
        type=str,
        default="127.0.0.1",
        help="Master address for process group (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--master-port",
        type=int,
        default=29500,
        help="Master port for process group (default: 29500)",
    )

    if config_defaults:
        parser.set_defaults(**config_defaults)

    return parser.parse_args()


def validate_args(args):
    if args.num_experts % args.num_ranks != 0:
        raise ValueError(
            f"num_experts ({args.num_experts}) must be divisible by "
            f"num_ranks ({args.num_ranks})"
        )
    if args.async_finish and args.return_recv_hook:
        raise ValueError("async_finish and return_recv_hook are mutually exclusive")
    if args.dtype == "fp8" and args.hidden_size % 128 != 0:
        raise ValueError(
            f"hidden_size ({args.hidden_size}) must be divisible by 128 for fp8 dtype"
        )


def dequantize_fp8(x_fp8, scales):
    """Dequantize FP8 packed data to bfloat16 (pattern from test_ep_grid.py:43-48)."""
    hidden = x_fp8.shape[-1]
    x_view = x_fp8.reshape(-1, hidden // 128, 128).float()
    scales_view = scales.reshape(-1, hidden // 128, 1).float()
    dequantized = (x_view * scales_view).reshape(x_fp8.shape)
    return dequantized.to(torch.bfloat16)


class EPBenchmarkWorker:
    """Per-rank worker that sets up the EP buffer, runs warmup and measured
    iterations, then aggregates and outputs results."""

    def __init__(self, rank, local_rank, args):
        self.rank = rank
        self.args = args
        self.use_fp8 = args.dtype == "fp8"
        self.num_local_experts = args.num_experts // args.num_ranks
        self.timeout_us = -1

        torch.cuda.set_device(local_rank)

        dist.init_process_group(
            backend=args.pg_backend, rank=rank, world_size=args.num_ranks
        )
        self.group = dist.group.WORLD

        num_ep_buffer_bytes = Buffer.get_ep_buffer_size_hint(
            args.num_tokens, args.hidden_size, args.num_ranks, args.num_experts
        )
        self.buf = Buffer(self.group, num_ep_buffer_bytes)

        self.topk_idx, self.topk_weights = ROUTING_MODES[args.routing_mode](
            num_tokens=args.num_tokens,
            num_experts=args.num_experts,
            top_k=args.top_k,
            device=torch.device("cuda"),
            seed=args.seed + rank,
            hot_experts=args.hot_experts,
            hot_fraction=args.hot_fraction,
            zipf_alpha=args.zipf_alpha,
        )

        self.x = torch.randn(
            args.num_tokens, args.hidden_size, dtype=torch.bfloat16, device="cuda"
        )
        self.active_ranks = torch.ones(
            (args.num_ranks,), dtype=torch.int32, device="cuda"
        )
        self.out_tensor = torch.zeros_like(self.x)

    def run_dispatch(self):
        recv_x, recv_count, handle, event, hook = self.buf.dispatch(
            self.x,
            self.topk_idx,
            self.active_ranks,
            num_max_dispatch_tokens_per_rank=self.args.num_tokens,
            num_experts=self.args.num_experts,
            timeout_us=self.timeout_us,
            use_fp8=self.use_fp8,
            async_finish=self.args.async_finish,
            return_recv_hook=self.args.return_recv_hook,
        )
        if self.args.return_recv_hook:
            hook()
        elif self.args.async_finish:
            event.current_stream_wait()
        return recv_x, recv_count, handle

    def prepare_combine(self, expert_out, handle):
        if self.args.zero_copy:
            cb_buf = self.buf.get_next_combine_buffer(handle)
            cb_buf.copy_(expert_out)
            return cb_buf.contiguous(), handle
        else:
            return expert_out.contiguous(), handle

    def run_combine(self, expert_to_pass, handle):
        combined_x, event, hook = self.buf.combine(
            expert_to_pass,
            self.topk_idx,
            self.topk_weights,
            self.active_ranks,
            timeout_us=self.timeout_us,
            handle=handle,
            zero_copy=self.args.zero_copy,
            async_finish=self.args.async_finish,
            return_recv_hook=self.args.return_recv_hook,
            out=self.out_tensor,
        )
        if self.args.return_recv_hook:
            hook()
        elif self.args.async_finish:
            event.current_stream_wait()
        return combined_x

    def mock_expert_forward(self, recv_x):
        if self.use_fp8:
            recv_bf16 = dequantize_fp8(recv_x[0], recv_x[1])
        else:
            recv_bf16 = recv_x

        expert_out = torch.empty_like(recv_bf16)
        for le in range(self.num_local_experts):
            expert_id = self.rank * self.num_local_experts + le
            expert_out[le] = recv_bf16[le] * (expert_id * 0.1 + 1.0)
        return expert_out.to(torch.bfloat16)

    def warmup(self):
        for _ in range(self.args.warmup_iters):
            recv_x, _, handle = self.run_dispatch()
            expert_out = self.mock_expert_forward(recv_x)
            expert_to_pass, handle = self.prepare_combine(expert_out, handle)
            self.run_combine(expert_to_pass, handle)
        torch.cuda.synchronize()

    def run_measured(self):
        n = self.args.iters
        dispatch_starts = [torch.cuda.Event(enable_timing=True) for _ in range(n)]
        dispatch_ends = [torch.cuda.Event(enable_timing=True) for _ in range(n)]
        combine_starts = [torch.cuda.Event(enable_timing=True) for _ in range(n)]
        combine_ends = [torch.cuda.Event(enable_timing=True) for _ in range(n)]

        for i in range(n):
            dispatch_starts[i].record()

            recv_x, _, handle = self.run_dispatch()

            dispatch_ends[i].record()

            expert_out = self.mock_expert_forward(recv_x)
            expert_to_pass, handle = self.prepare_combine(expert_out, handle)

            combine_starts[i].record()

            self.run_combine(expert_to_pass, handle)

            combine_ends[i].record()

        torch.cuda.synchronize()

        dispatch_latencies = [
            dispatch_starts[i].elapsed_time(dispatch_ends[i]) for i in range(n)
        ]
        combine_latencies = [
            combine_starts[i].elapsed_time(combine_ends[i]) for i in range(n)
        ]
        # e2e includes dispatch + mock_expert_forward + combine (full cycle)
        e2e_latencies = [
            dispatch_starts[i].elapsed_time(combine_ends[i]) for i in range(n)
        ]

        return dispatch_latencies, combine_latencies, e2e_latencies

    def aggregate_and_output(
        self, dispatch_latencies, combine_latencies, e2e_latencies
    ):
        dispatch_tensor = torch.tensor(
            dispatch_latencies, dtype=torch.float64, device="cuda"
        )
        combine_tensor = torch.tensor(
            combine_latencies, dtype=torch.float64, device="cuda"
        )
        e2e_tensor = torch.tensor(e2e_latencies, dtype=torch.float64, device="cuda")

        all_dispatch = [
            torch.zeros_like(dispatch_tensor) for _ in range(self.args.num_ranks)
        ]
        all_combine = [
            torch.zeros_like(combine_tensor) for _ in range(self.args.num_ranks)
        ]
        all_e2e = [torch.zeros_like(e2e_tensor) for _ in range(self.args.num_ranks)]

        dist.all_gather(all_dispatch, dispatch_tensor, group=self.group)
        dist.all_gather(all_combine, combine_tensor, group=self.group)
        dist.all_gather(all_e2e, e2e_tensor, group=self.group)

        expert_load = compute_global_expert_load(
            self.topk_idx, self.args.num_experts, group=self.group
        )

        if self.rank == 0:
            all_dispatch_flat = torch.cat(all_dispatch).cpu().tolist()
            all_combine_flat = torch.cat(all_combine).cpu().tolist()
            all_e2e_flat = torch.cat(all_e2e).cpu().tolist()

            result = assemble_json_output(
                backend=self.args.backend,
                world_size=self.args.num_ranks,
                num_experts=self.args.num_experts,
                hidden_size=self.args.hidden_size,
                top_k=self.args.top_k,
                num_tokens=self.args.num_tokens,
                dtype=self.args.dtype,
                routing_mode=self.args.routing_mode,
                zero_copy=self.args.zero_copy,
                async_finish=self.args.async_finish,
                return_recv_hook=self.args.return_recv_hook,
                warmup_iters=self.args.warmup_iters,
                iters=self.args.iters,
                hot_experts=(
                    self.args.hot_experts if self.args.routing_mode == "k_hot" else None
                ),
                hot_fraction=(
                    self.args.hot_fraction
                    if self.args.routing_mode == "k_hot"
                    else None
                ),
                zipf_alpha=(
                    self.args.zipf_alpha if self.args.routing_mode == "zipf" else None
                ),
                dispatch_latencies_ms=all_dispatch_flat,
                combine_latencies_ms=all_combine_flat,
                e2e_latencies_ms=all_e2e_flat,
                expert_load=expert_load,
                pg_backend=self.args.pg_backend,
            )

            if self.args.json_output:
                write_json_output(result, self.args.json_output)
                print(f"Results written to {self.args.json_output}", file=sys.stderr)
            else:
                print(json.dumps(result, indent=2))

    def run(self):
        self.warmup()
        dispatch_latencies, combine_latencies, e2e_latencies = self.run_measured()
        self.aggregate_and_output(dispatch_latencies, combine_latencies, e2e_latencies)
        dist.barrier()
        dist.destroy_process_group()


def _worker_entry(rank, args):
    EPBenchmarkWorker(rank, rank, args).run()


def main():
    args = parse_args()

    if "RANK" in os.environ:
        args.num_ranks = int(os.environ["WORLD_SIZE"])
        validate_args(args)
        rank = int(os.environ["RANK"])
        local_rank = int(os.environ.get("LOCAL_RANK", "0"))
        EPBenchmarkWorker(rank, local_rank, args).run()
        return

    validate_args(args)
    os.environ.setdefault("MASTER_ADDR", args.master_addr)
    os.environ.setdefault("MASTER_PORT", str(args.master_port))
    mp.spawn(_worker_entry, args=(args,), nprocs=args.num_ranks)


if __name__ == "__main__":
    main()
