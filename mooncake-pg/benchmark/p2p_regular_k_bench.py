#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import time
from typing import List, Tuple

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from pgbench_utils import parse_size, resolve_dtype


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="P2P regular-k benchmark for mooncake-pg"
    )
    parser.add_argument(
        "--backend", choices=["mooncake", "mooncake-cpu", "nccl", "gloo"], default=None
    )
    parser.add_argument("--device", choices=["cuda", "cpu"], default=None)
    parser.add_argument("-g", "--ngpus", type=int, default=1)
    parser.add_argument("--dtype", type=str, default="float")
    parser.add_argument("--tensor-bytes", type=str, default="32M")
    parser.add_argument("-k", type=int, default=1)
    parser.add_argument("-w", "--warmup-iters", type=int, default=10)
    parser.add_argument("-n", "--iters", type=int, default=50)
    parser.add_argument("-c", "--check", type=int, default=1)
    parser.add_argument("-C", "--report-cputime", action="store_true")
    parser.add_argument("--master-addr", type=str, default="127.0.0.1")
    parser.add_argument("--master-port", type=str, default="29500")
    return parser.parse_args()


def _init_backend_device(args: argparse.Namespace) -> None:
    if args.backend is None:
        if args.device is None:
            args.device = "cuda" if torch.cuda.is_available() else "cpu"
        args.backend = "mooncake" if args.device == "cuda" else "mooncake-cpu"
    else:
        if args.device is None:
            args.device = "cuda" if args.backend in ("mooncake", "nccl") else "cpu"

    if args.device == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but not available")

    if args.device == "cuda" and args.backend not in ("mooncake", "nccl"):
        raise ValueError("--device=cuda requires --backend=mooncake or nccl")
    if args.device == "cpu" and args.backend not in ("mooncake-cpu", "gloo"):
        raise ValueError("--device=cpu requires --backend=mooncake-cpu or gloo")

    if args.backend in ("mooncake", "mooncake-cpu"):
        try:
            import mooncake.pg as pg  # noqa: F401

            pg.set_device_filter(["mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4"])
        except Exception as exc:
            raise RuntimeError(
                "Failed to import mooncake.pg; ensure PYTHONPATH includes mooncake-pg"
            ) from exc


def _resolve_rank(local_rank: int, world_size_from_args: int) -> Tuple[int, int, int]:
    if "RANK" in os.environ:
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        resolved_local_rank = int(os.environ.get("LOCAL_RANK", local_rank))
        return rank, world_size, resolved_local_rank
    return local_rank, world_size_from_args, local_rank


def _sync_ranks(device: torch.device) -> None:
    if device.type == "cpu":
        dist.barrier()
        return
    token = torch.zeros(1, device=device, dtype=torch.int32)
    dist.all_reduce(token, op=dist.ReduceOp.SUM)
    torch.cuda.synchronize(device)


def _sync_device_if_needed(device: torch.device, report_cputime: bool) -> None:
    if device.type == "cuda" and not report_cputime:
        torch.cuda.synchronize(device)


def _build_regular_k_peers(
    rank: int, world_size: int, k: int
) -> Tuple[List[int], List[int]]:
    send_peers = [((rank + hop) % world_size) for hop in range(1, k + 1)]
    recv_peers = [((rank - hop + world_size) % world_size) for hop in range(1, k + 1)]
    return send_peers, recv_peers


def _run_iteration(
    send_tensor: torch.Tensor,
    recv_tensors: List[torch.Tensor],
    send_peers: List[int],
    recv_peers: List[int],
) -> None:
    ops: List[dist.P2POp] = []
    for index, src_rank in enumerate(recv_peers):
        ops.append(dist.P2POp(dist.irecv, recv_tensors[index], src_rank))
    for dst_rank in send_peers:
        ops.append(dist.P2POp(dist.isend, send_tensor, dst_rank))

    requests = dist.batch_isend_irecv(ops)
    for request in requests:
        request.wait()


def _count_wrong(actual: torch.Tensor, expected: torch.Tensor) -> int:
    if actual.numel() == 0:
        return 0
    if actual.is_floating_point() or actual.dtype in (torch.bfloat16,):
        close = torch.isclose(actual, expected, rtol=1e-3, atol=1e-3)
        return int((~close).sum().item())
    return int((actual != expected).sum().item())


def _percentile_us(lat_us: torch.Tensor, p: float) -> float:
    return float(torch.quantile(lat_us, p / 100.0).item())


def _run_worker(local_rank: int, args: argparse.Namespace) -> None:
    _init_backend_device(args)
    rank, world_size, local_rank = _resolve_rank(local_rank, args.ngpus)

    if world_size < 2:
        raise ValueError("regular-k benchmark requires world size >= 2")
    if args.k <= 0 or args.k >= world_size:
        raise ValueError(
            f"invalid k={args.k}; require 1 <= k < world_size({world_size})"
        )
    if args.iters <= 0:
        raise ValueError("--iters must be > 0")
    if args.warmup_iters < 0:
        raise ValueError("--warmup-iters must be >= 0")
    if args.check < 0:
        raise ValueError("--check must be >= 0")

    if args.device == "cuda":
        if torch.cuda.device_count() < world_size:
            raise RuntimeError("Requested more ranks than available CUDA devices")
        torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank)
    else:
        device = torch.device("cpu")

    dist.init_process_group(backend=args.backend, rank=rank, world_size=world_size)

    dtype = resolve_dtype(args.dtype, device)
    requested_bytes = parse_size(args.tensor_bytes)
    elt_size = torch.tensor([], dtype=dtype).element_size()
    elem_count = requested_bytes // elt_size
    if elem_count <= 0:
        raise ValueError(
            f"tensor-bytes too small for dtype={args.dtype}; got {requested_bytes} bytes"
        )
    tensor_bytes = elem_count * elt_size

    send_peers, recv_peers = _build_regular_k_peers(rank, world_size, args.k)
    send_tensor = torch.full(
        (elem_count,),
        fill_value=float(rank + 1),
        dtype=dtype,
        device=device,
    )
    recv_tensors = [torch.empty_like(send_tensor) for _ in range(args.k)]

    for _ in range(args.warmup_iters):
        _run_iteration(send_tensor, recv_tensors, send_peers, recv_peers)
        _sync_device_if_needed(device, args.report_cputime)

    _sync_ranks(device)

    iter_latencies = torch.empty(args.iters, device=device, dtype=torch.float64)
    for index in range(args.iters):
        _sync_device_if_needed(device, args.report_cputime)
        t0 = time.perf_counter()
        _run_iteration(send_tensor, recv_tensors, send_peers, recv_peers)
        _sync_device_if_needed(device, args.report_cputime)
        iter_latencies[index] = time.perf_counter() - t0

    dist.all_reduce(iter_latencies, op=dist.ReduceOp.MAX)

    wrong = 0
    if args.check > 0:
        for _ in range(args.check):
            _run_iteration(send_tensor, recv_tensors, send_peers, recv_peers)
            _sync_device_if_needed(device, args.report_cputime)
            for recv_tensor, src_rank in zip(recv_tensors, recv_peers):
                expected = torch.full_like(recv_tensor, float(src_rank + 1))
                wrong += _count_wrong(recv_tensor, expected)
        wrong_tensor = torch.tensor([wrong], device=device, dtype=torch.int64)
        dist.all_reduce(wrong_tensor, op=dist.ReduceOp.SUM)
        wrong = int(wrong_tensor.item())

    lat_us = iter_latencies.mul(1e6).cpu()
    mean_us = float(lat_us.mean().item())
    p50_us = _percentile_us(lat_us, 50.0)
    p95_us = _percentile_us(lat_us, 95.0)
    p99_us = _percentile_us(lat_us, 99.0)

    step_time_sec = mean_us / 1e6
    per_rank_bidir_bytes = 2.0 * args.k * tensor_bytes
    cluster_bidir_bytes = per_rank_bidir_bytes * world_size
    per_rank_bw_gbps = (
        (per_rank_bidir_bytes / 1e9) / step_time_sec if step_time_sec > 0 else 0.0
    )
    cluster_bw_gbps = (
        (cluster_bidir_bytes / 1e9) / step_time_sec if step_time_sec > 0 else 0.0
    )

    if rank == 0:
        print("# p2p regular-k benchmark", flush=True)
        print(
            "# backend={backend} device={device} world_size={world_size} k={k} dtype={dtype}".format(
                backend=args.backend,
                device=args.device,
                world_size=world_size,
                k=args.k,
                dtype=args.dtype,
            ),
            flush=True,
        )
        print(
            "# tensor_bytes={tensor_bytes} element_count={elem_count} warmup={warmup} iters={iters}".format(
                tensor_bytes=tensor_bytes,
                elem_count=elem_count,
                warmup=args.warmup_iters,
                iters=args.iters,
            ),
            flush=True,
        )
        print(
            "lat_mean_us={:.3f} lat_p50_us={:.3f} lat_p95_us={:.3f} lat_p99_us={:.3f} "
            "rank_bidir_GBps={:.3f} cluster_bidir_GBps={:.3f} wrong={}".format(
                mean_us,
                p50_us,
                p95_us,
                p99_us,
                per_rank_bw_gbps,
                cluster_bw_gbps,
                wrong,
            ),
            flush=True,
        )

    _sync_ranks(device)
    dist.destroy_process_group()


def _launch(args: argparse.Namespace) -> None:
    if "RANK" in os.environ:
        _run_worker(int(os.environ.get("LOCAL_RANK", "0")), args)
        return

    os.environ.setdefault("MASTER_ADDR", args.master_addr)
    os.environ.setdefault("MASTER_PORT", args.master_port)
    mp.spawn(_run_worker, args=(args,), nprocs=args.ngpus, join=True)


def main() -> None:
    args = _parse_args()
    _launch(args)


if __name__ == "__main__":
    main()
