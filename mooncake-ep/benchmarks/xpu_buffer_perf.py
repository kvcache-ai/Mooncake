"""Latency of Mooncake EP dispatch/combine on Intel XPU.

Each iteration aligns the ranks with a CPU barrier and an idle device, then
times (a) dispatch alone and (b) dispatch followed directly by combine, the
round trip an MoE layer makes. Both are end to end (launch included), median
over iterations, worst rank. Combine is timed as part of the round trip
rather than after a host synchronize: an idle gap between the two inflates
the next peer-to-peer send several-fold, which a busy MoE layer never sees.

    python mooncake-ep/benchmarks/xpu_buffer_perf.py --num-ranks 4 \\
        --hidden 7168 --experts 256 --topk 8 --tokens 1,16,64,128

--pipelined instead issues the iterations back to back with no host sync in
between, as consecutive MoE layers do, and reports the mean per iteration.
--weights-at-dispatch passes the top-k weights to dispatch, so combine
reduces on the expert side. --impl fallback times the torch.distributed
fallback of the same Buffer; --impl alltoall times two XCCL
all_to_all_single calls that move the bytes a dispatch and a combine move,
with no routing work: a lower bound for an EP built on torch.distributed
collectives.
"""

import argparse
import os
import random
import statistics
import time

import torch
import torch.distributed as dist
import torch.multiprocessing as mp


def _device_for(local_rank: int) -> int:
    devices = os.environ.get("MOONCAKE_EP_TEST_DEVICES")
    return int(devices.split(",")[local_rank]) if devices else local_rank


class AllToAllBaseline:
    """Dispatch and combine as one even all_to_all_single each, carrying one
    row per (token, top-k) pick."""

    def __init__(self, group, world):
        self.group, self.world = group, world

    def _exchange(self, rows):
        # Even splits need a row count divisible by the world size.
        pad = -rows.size(0) % self.world
        if pad:
            rows = torch.cat([rows, rows.new_zeros((pad, rows.size(1)))])
        out = torch.empty_like(rows)
        dist.all_to_all_single(out, rows, group=self.group)
        return out

    def dispatch(self, x, topk_idx, active, T, E, timeout_us, use_fp8=False):
        assert not use_fp8, "--impl alltoall is BF16 only"
        rows = x.repeat_interleave(topk_idx.size(1), dim=0)
        return self._exchange(rows), None, x.size(0), None, None

    def combine(self, x, topk_idx, w, active, timeout_us, handle, out=None):
        rows = self._exchange(x)[: handle * topk_idx.size(1)]
        rows = rows.view(handle, topk_idx.size(1), -1).float()
        out.copy_((rows * w.unsqueeze(-1)).sum(1))
        return out, None, None


def make_buffer(impl, group, max_tokens, hidden, world, experts):
    if impl == "alltoall":
        return AllToAllBaseline(group, world)
    from mooncake.mooncake_ep_buffer import Buffer

    size = Buffer.get_ep_buffer_size_hint(max_tokens, hidden, world, experts)
    buffer = Buffer(group, size, disable_p2p=(impl == "fallback"))
    if impl == "native":
        assert not buffer._use_fallback, "native fast path not selected"
    return buffer


def bench(buffer, cpu_group, rank, world, args, num_tokens, use_fp8):
    device = torch.device("xpu", torch.xpu.current_device())
    x = torch.randn((num_tokens, args.hidden), device=device).to(torch.bfloat16)
    scores = torch.rand((num_tokens, args.experts), device=device)
    topk_idx = torch.topk(scores, args.topk, dim=-1).indices
    topk_w = torch.rand((num_tokens, args.topk), device=device)
    active = torch.ones((world,), dtype=torch.int32, device=device)
    out = torch.empty_like(x)

    def dispatch():
        return buffer.dispatch(
            x,
            topk_idx,
            active,
            args.max_tokens,
            args.experts,
            args.timeout_us,
            use_fp8=use_fp8,
            **({"topk_weights": topk_w} if args.weights_at_dispatch else {}),
        )

    def combine(recv, handle):
        recv_x = recv[0] if use_fp8 else recv
        # Stand-in expert output with the combine input's shape and dtype.
        expert_out = recv_x.to(torch.bfloat16) if use_fp8 else recv_x
        buffer.combine(
            expert_out, topk_idx, topk_w, active, args.timeout_us, handle, out=out
        )

    if args.pipelined:
        return bench_pipelined(dispatch, combine, cpu_group, args)
    t_d, t_rt = [], []
    for it in range(args.warmup + args.iters):
        for times, with_combine in ((t_d, False), (t_rt, True)):
            dist.barrier(cpu_group)
            torch.xpu.synchronize()
            t0 = time.perf_counter()
            recv, _, handle, _, _ = dispatch()
            if with_combine:
                combine(recv, handle)
            else:
                # The next dispatch must not overwrite this one's buffer
                # half before it is consumed: run combine untimed.
                torch.xpu.synchronize()
                t1 = time.perf_counter()
                combine(recv, handle)
            torch.xpu.synchronize()
            if it >= args.warmup:
                times.append((time.perf_counter() if with_combine else t1) - t0)
    stats = torch.tensor(
        [statistics.median(t_d), statistics.median(t_rt)], dtype=torch.float64
    )
    dist.all_reduce(stats, op=dist.ReduceOp.MAX, group=cpu_group)
    return stats.tolist()


def bench_pipelined(dispatch, combine, cpu_group, args):
    def run(with_combine, iters):
        for _ in range(iters):
            recv, _, handle, _, _ = dispatch()
            if with_combine:
                combine(recv, handle)

    stats = []
    for with_combine in (False, True):
        run(with_combine, args.warmup)
        torch.xpu.synchronize()
        dist.barrier(cpu_group)
        t0 = time.perf_counter()
        run(with_combine, args.iters)
        torch.xpu.synchronize()
        stats.append((time.perf_counter() - t0) / args.iters)
    stats = torch.tensor(stats, dtype=torch.float64)
    dist.all_reduce(stats, op=dist.ReduceOp.MAX, group=cpu_group)
    return stats.tolist()


def worker(local_rank, world, args, port):
    torch.xpu.set_device(_device_for(local_rank))
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ["MASTER_PORT"] = str(port)
    dist.init_process_group("xccl", rank=local_rank, world_size=world)
    group = dist.new_group(list(range(world)))
    cpu_group = dist.new_group(list(range(world)), backend="gloo")
    torch.manual_seed(local_rank)
    buffer = make_buffer(
        args.impl, group, args.max_tokens, args.hidden, world, args.experts
    )

    if local_rank == 0:
        print(
            f"impl={args.impl} ranks={world} hidden={args.hidden} experts={args.experts} "
            f"topk={args.topk}  ({'mean' if args.pipelined else 'median'} us, "
            "worst rank)"
        )
        print(f"{'tokens':>6} {'dtype':>5} {'dispatch':>10} {'dispatch+combine':>17}")
    for n in [int(t) for t in args.tokens.split(",")]:
        for use_fp8 in [False, True] if args.fp8 else [False]:
            d, rt = bench(buffer, cpu_group, local_rank, world, args, n, use_fp8)
            if local_rank == 0:
                print(
                    f"{n:>6} {'fp8' if use_fp8 else 'bf16':>5} {d*1e6:>10.1f} {rt*1e6:>17.1f}",
                    flush=True,
                )
    torch.xpu.synchronize()
    dist.barrier(cpu_group)
    del buffer
    torch.xpu.synchronize()
    dist.barrier(cpu_group)
    dist.destroy_process_group()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--num-ranks", type=int, default=2)
    p.add_argument(
        "--impl", choices=["native", "fallback", "alltoall"], default="native"
    )
    p.add_argument("--hidden", type=int, default=7168)
    p.add_argument("--experts", type=int, default=256)
    p.add_argument("--topk", type=int, default=8)
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--tokens", default="1,8,32,64,128")
    p.add_argument("--fp8", action="store_true")
    p.add_argument("--weights-at-dispatch", action="store_true")
    p.add_argument("--pipelined", action="store_true")
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--iters", type=int, default=30)
    p.add_argument("--timeout-us", type=int, default=30_000_000)
    args = p.parse_args()
    mp.spawn(
        worker,
        args=(args.num_ranks, args, 29500 + random.randint(0, 1000)),
        nprocs=args.num_ranks,
    )


if __name__ == "__main__":
    main()
