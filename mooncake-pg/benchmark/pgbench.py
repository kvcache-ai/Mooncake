#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import time
from typing import List, Optional, Tuple

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
import mooncake.pg as pg

from pgbench_utils import (
    busbw_factor,
    compute_counts,
    format_header,
    format_result_line,
    list_supported_dtypes,
    parse_size,
    resolve_dtype,
    resolve_reduce_op,
)

pg.set_device_filter(["mlx5_1", "mlx5_2", "mlx5_3", "mlx5_4"])

COLLECTIVES = {
    "all_reduce",
    "all_gather",
    "broadcast",
    "reduce_scatter",
    "alltoall",
    "sendrecv",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="mooncake-pg pgbench (nccl-tests style)"
    )
    parser.add_argument("--collective", required=True, choices=sorted(COLLECTIVES))
    parser.add_argument(
        "--backend", choices=["mooncake", "mooncake-cpu", "nccl", "gloo"]
    )
    parser.add_argument("--device", choices=["cuda", "cpu"], default=None)
    parser.add_argument("-b", "--minbytes", type=str, default="32M")
    parser.add_argument("-e", "--maxbytes", type=str, default="32M")
    parser.add_argument("-i", "--stepbytes", type=str, default="1M")
    parser.add_argument("-f", "--stepfactor", type=int, default=1)
    parser.add_argument("-n", "--iters", type=int, default=20)
    parser.add_argument("-w", "--warmup_iters", type=int, default=1)
    parser.add_argument("-m", "--agg_iters", type=int, default=1)
    parser.add_argument("-c", "--check", type=int, default=1)
    parser.add_argument("-a", "--average", type=int, choices=[0, 1, 2, 3], default=1)
    parser.add_argument("-d", "--datatype", default="float")
    parser.add_argument("-o", "--op", default="sum")
    parser.add_argument("-r", "--root", type=int, default=0)
    parser.add_argument("-C", "--report_cputime", action="store_true")
    parser.add_argument("-S", "--report_timestamps", action="store_true")
    parser.add_argument("-t", "--nthreads", type=int, default=1)
    parser.add_argument("-g", "--ngpus", type=int, default=1)
    parser.add_argument("-J", "--output_file", default=None)
    return parser.parse_args()


def _iter_sizes(
    minbytes: int, maxbytes: int, stepbytes: int, stepfactor: int
) -> List[int]:
    sizes = []
    size = minbytes
    while size <= maxbytes:
        sizes.append(size)
        size = size * stepfactor if stepfactor > 1 else size + stepbytes
    return sizes


def _init_dist(rank: int, world_size: int, backend: str) -> None:
    dist.init_process_group(backend=backend, rank=rank, world_size=world_size)


def _sync_if_needed(device: torch.device, report_cputime: bool) -> None:
    if device.type == "cuda" and not report_cputime:
        torch.cuda.synchronize(device)


def _sync_ranks(device: torch.device) -> None:
    if device.type == "cpu":
        dist.barrier()
        return
    # mooncake GPU backend does not support barrier; use all_reduce as a sync primitive.
    token = torch.zeros(1, device=device, dtype=torch.int32)
    dist.all_reduce(token, op=dist.ReduceOp.SUM)
    torch.cuda.synchronize(device)


def _alloc_tensor(count: int, dtype: torch.dtype, device: torch.device) -> torch.Tensor:
    if count <= 0:
        return torch.empty(0, dtype=dtype, device=device)
    return torch.empty(count, dtype=dtype, device=device)


def _fill_tensor(tensor: torch.Tensor, value: float) -> None:
    if tensor.numel() > 0:
        tensor.fill_(value)


def _compute_expected_value(op: str, rank: int, world_size: int) -> float:
    if op == "sum":
        return sum(range(1, world_size + 1))
    if op == "max":
        return float(world_size)
    if op == "min":
        return 1.0
    if op == "avg":
        return sum(range(1, world_size + 1)) / float(world_size)
    if op == "prod":
        return 1.0
    raise ValueError(f"unsupported op for expected value: {op}")


def _count_wrong(actual: torch.Tensor, expected: torch.Tensor) -> int:
    if actual.numel() == 0:
        return 0
    if actual.is_floating_point() or actual.dtype in (torch.bfloat16,):
        close = torch.isclose(actual, expected, rtol=1e-3, atol=1e-3)
        return int((~close).sum().item())
    return int((actual != expected).sum().item())


def _run_all_reduce(
    send: torch.Tensor,
    recv: torch.Tensor,
    op: dist.ReduceOp,
    post_scale: Optional[float],
) -> None:
    if send is not recv:
        recv.copy_(send)
    dist.all_reduce(recv, op=op)
    if post_scale is not None:
        recv.div_(dist.get_world_size())


def _run_broadcast(buffer: torch.Tensor, root: int) -> None:
    dist.broadcast(buffer, src=root)


def _run_all_gather(out_list: List[torch.Tensor], inp: torch.Tensor) -> None:
    dist.all_gather(out_list, inp)


def _run_reduce_scatter(
    output: torch.Tensor,
    input_buf: torch.Tensor,
    op: dist.ReduceOp,
    post_scale: Optional[float],
) -> None:
    if hasattr(dist, "reduce_scatter_tensor"):
        dist.reduce_scatter_tensor(output, input_buf, op=op)
    else:
        chunks = list(input_buf.chunk(dist.get_world_size()))
        dist.reduce_scatter(output, chunks, op=op)
    if post_scale is not None:
        output.div_(dist.get_world_size())


def _run_alltoall(output: torch.Tensor, input_buf: torch.Tensor) -> None:
    if hasattr(dist, "all_to_all_single"):
        dist.all_to_all_single(output, input_buf)
    else:
        in_chunks = list(input_buf.chunk(dist.get_world_size()))
        out_chunks = list(output.chunk(dist.get_world_size()))
        dist.all_to_all(out_chunks, in_chunks)


def _run_sendrecv(
    send: torch.Tensor, recv: torch.Tensor, rank: int, world_size: int
) -> None:
    send_peer = (rank + 1) % world_size
    recv_peer = (rank - 1 + world_size) % world_size
    ops = [
        dist.P2POp(dist.isend, send, send_peer),
        dist.P2POp(dist.irecv, recv, recv_peer),
    ]
    reqs = dist.batch_isend_irecv(ops)
    for req in reqs:
        req.wait()


def _bench_once(
    args: argparse.Namespace,
    device: torch.device,
    dtype_name: str,
    dtype: torch.dtype,
    size_bytes: int,
    sendcount: int,
    recvcount: int,
    paramcount: int,
    in_place: bool,
) -> Tuple[float, float, float, int]:
    rank = dist.get_rank()
    world_size = dist.get_world_size()

    op_name = args.op if args.collective in ("all_reduce", "reduce_scatter") else "none"
    reduce_op, post_scale = (
        resolve_reduce_op(op_name) if op_name != "none" else (dist.ReduceOp.SUM, None)
    )

    if args.collective == "all_reduce":
        send = _alloc_tensor(sendcount, dtype, device)
        recv = send if in_place else _alloc_tensor(sendcount, dtype, device)
        init_val = 1.0 if op_name == "prod" else float(rank + 1)
        _fill_tensor(send, init_val)
        _run_all_reduce(send, recv, reduce_op, post_scale if op_name == "avg" else None)
        result = recv
    elif args.collective == "broadcast":
        send = _alloc_tensor(sendcount, dtype, device)
        recv = send if in_place else _alloc_tensor(sendcount, dtype, device)
        if rank == args.root:
            _fill_tensor(send, 7.0)
        else:
            _fill_tensor(send, 0.0)
        if send is not recv:
            recv.copy_(send)
        _run_broadcast(recv, args.root)
        result = recv
    elif args.collective == "all_gather":
        out_buf = _alloc_tensor(recvcount, dtype, device)
        out_list = [
            out_buf[i * sendcount : (i + 1) * sendcount] for i in range(world_size)
        ]
        if in_place:
            inp = out_list[rank]
        else:
            inp = _alloc_tensor(sendcount, dtype, device)
        _fill_tensor(inp, float(rank + 1))
        _run_all_gather(out_list, inp)
        result = out_buf
    elif args.collective == "reduce_scatter":
        input_buf = _alloc_tensor(sendcount, dtype, device)
        if in_place:
            output = input_buf[rank * recvcount : (rank + 1) * recvcount]
        else:
            output = _alloc_tensor(recvcount, dtype, device)
        init_val = 1.0 if op_name == "prod" else float(rank + 1)
        _fill_tensor(input_buf, init_val)
        _run_reduce_scatter(
            output, input_buf, reduce_op, post_scale if op_name == "avg" else None
        )
        result = output
    elif args.collective == "alltoall":
        input_buf = _alloc_tensor(sendcount, dtype, device)
        output = input_buf if in_place else _alloc_tensor(recvcount, dtype, device)
        # Layout: [peer, chunk]
        chunk = sendcount // world_size if world_size else 0
        if chunk > 0:
            view = input_buf.view(world_size, chunk)
            for peer in range(world_size):
                view[peer].fill_(float(rank * 1000 + peer))
        _run_alltoall(output, input_buf)
        result = output
    elif args.collective == "sendrecv":
        send = _alloc_tensor(sendcount, dtype, device)
        recv = send if in_place else _alloc_tensor(recvcount, dtype, device)
        _fill_tensor(send, float(rank + 1))
        _run_sendrecv(send, recv, rank, world_size)
        result = recv
    else:
        raise ValueError(f"unsupported collective: {args.collective}")

    # Warmup phase
    for _ in range(args.warmup_iters):
        for _ in range(args.agg_iters):
            if args.collective == "all_reduce":
                _run_all_reduce(
                    send, recv, reduce_op, post_scale if op_name == "avg" else None
                )
            elif args.collective == "broadcast":
                _run_broadcast(recv, args.root)
            elif args.collective == "all_gather":
                _run_all_gather(out_list, inp)
            elif args.collective == "reduce_scatter":
                _run_reduce_scatter(
                    output,
                    input_buf,
                    reduce_op,
                    post_scale if op_name == "avg" else None,
                )
            elif args.collective == "alltoall":
                _run_alltoall(output, input_buf)
            elif args.collective == "sendrecv":
                _run_sendrecv(send, recv, rank, world_size)

    _sync_ranks(device)

    # Timing phase
    _sync_if_needed(device, args.report_cputime)
    start = time.perf_counter()
    for _ in range(args.iters):
        for _ in range(args.agg_iters):
            if args.collective == "all_reduce":
                _run_all_reduce(
                    send, recv, reduce_op, post_scale if op_name == "avg" else None
                )
            elif args.collective == "broadcast":
                _run_broadcast(recv, args.root)
            elif args.collective == "all_gather":
                _run_all_gather(out_list, inp)
            elif args.collective == "reduce_scatter":
                _run_reduce_scatter(
                    output,
                    input_buf,
                    reduce_op,
                    post_scale if op_name == "avg" else None,
                )
            elif args.collective == "alltoall":
                _run_alltoall(output, input_buf)
            elif args.collective == "sendrecv":
                _run_sendrecv(send, recv, rank, world_size)
    _sync_if_needed(device, args.report_cputime)
    elapsed = time.perf_counter() - start

    avg_time_sec = elapsed / max(1, args.iters * args.agg_iters)

    # Correctness check (after timing to avoid skewing measurements)
    wrong = 0
    if args.check > 0 and not (
        in_place and args.collective in ("alltoall", "sendrecv")
    ):
        for _ in range(args.check):
            if args.collective == "all_reduce":
                _fill_tensor(send, init_val)
                if send is not recv:
                    recv.copy_(send)
                _run_all_reduce(
                    send, recv, reduce_op, post_scale if op_name == "avg" else None
                )
                expected_val = _compute_expected_value(op_name, rank, world_size)
                expected = torch.full_like(result, expected_val)
                wrong += _count_wrong(result, expected)
            elif args.collective == "broadcast":
                if rank == args.root:
                    _fill_tensor(send, 7.0)
                else:
                    _fill_tensor(send, 0.0)
                if send is not recv:
                    recv.copy_(send)
                _run_broadcast(recv, args.root)
                expected = torch.full_like(result, 7.0)
                wrong += _count_wrong(result, expected)
            elif args.collective == "all_gather":
                _fill_tensor(inp, float(rank + 1))
                _run_all_gather(out_list, inp)
                if sendcount > 0:
                    expected = torch.empty_like(result)
                    for peer in range(world_size):
                        expected[peer * sendcount : (peer + 1) * sendcount].fill_(
                            float(peer + 1)
                        )
                    wrong += _count_wrong(result, expected)
            elif args.collective == "reduce_scatter":
                _fill_tensor(input_buf, init_val)
                _run_reduce_scatter(
                    output,
                    input_buf,
                    reduce_op,
                    post_scale if op_name == "avg" else None,
                )
                expected_val = _compute_expected_value(op_name, rank, world_size)
                expected = torch.full_like(result, expected_val)
                wrong += _count_wrong(result, expected)
            elif args.collective == "alltoall":
                chunk = sendcount // world_size if world_size else 0
                if chunk > 0:
                    view_in = input_buf.view(world_size, chunk)
                    for peer in range(world_size):
                        view_in[peer].fill_(float(rank * 1000 + peer))
                _run_alltoall(output, input_buf)
                if sendcount > 0:
                    expected = torch.empty_like(result)
                    view = expected.view(world_size, chunk)
                    for peer in range(world_size):
                        view[peer].fill_(float(peer * 1000 + rank))
                    wrong += _count_wrong(result, expected)
            elif args.collective == "sendrecv":
                _fill_tensor(send, float(rank + 1))
                _run_sendrecv(send, recv, rank, world_size)
                expected = torch.full_like(
                    result, float((rank - 1 + world_size) % world_size + 1)
                )
                wrong += _count_wrong(result, expected)

    if in_place and args.collective in ("alltoall", "sendrecv"):
        wrong = -1

    if wrong >= 0 and args.check > 0:
        wrong_tensor = torch.tensor([wrong], device=device, dtype=torch.int64)
        dist.all_reduce(wrong_tensor, op=dist.ReduceOp.SUM)
        wrong = int(wrong_tensor.item())

    time_us = avg_time_sec * 1e6
    algbw = (size_bytes / 1e9) / avg_time_sec if avg_time_sec > 0 else 0.0
    busbw = algbw * busbw_factor(args.collective, world_size)
    return time_us, algbw, busbw, wrong


def _maybe_print_header(args: argparse.Namespace, rank: int) -> None:
    if rank != 0:
        return
    print(format_header(args.report_cputime, args.report_timestamps), flush=True)


def _gather_time(args: argparse.Namespace, device: torch.device, value: float) -> float:
    if args.average == 0:
        return value
    tensor = torch.tensor([value], device=device, dtype=torch.float64)
    if args.average == 1:
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        tensor /= dist.get_world_size()
    elif args.average == 2:
        dist.all_reduce(tensor, op=dist.ReduceOp.MIN)
    elif args.average == 3:
        dist.all_reduce(tensor, op=dist.ReduceOp.MAX)
    return tensor.item()


def _run_worker(local_rank: int, args: argparse.Namespace) -> None:
    if args.nthreads != 1:
        raise ValueError("--nthreads must be 1 in this version")
    if args.output_file:
        raise ValueError("--output_file is not supported yet")

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

    backend = args.backend
    if backend in ("mooncake", "mooncake-cpu"):
        try:
            import mooncake.pg as pg  # noqa: F401
        except (
            Exception
        ) as exc:  # pragma: no cover - import-time failure should be explicit
            raise RuntimeError(
                "Failed to import mooncake.pg; ensure PYTHONPATH includes mooncake-pg"
            ) from exc

    if "RANK" in os.environ:
        rank = int(os.environ["RANK"])
        world_size = int(os.environ["WORLD_SIZE"])
        local_rank = int(os.environ.get("LOCAL_RANK", local_rank))
    else:
        rank = local_rank
        world_size = args.ngpus

    if args.device == "cuda":
        if torch.cuda.device_count() < world_size:
            raise RuntimeError("Requested more ranks than available CUDA devices")
        torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank)
    else:
        device = torch.device("cpu")

    _init_dist(rank, world_size, backend)

    if args.datatype == "all":
        dtype_list = list_supported_dtypes(device)
    else:
        dtype_list = [(args.datatype, resolve_dtype(args.datatype, device))]

    if not dtype_list:
        raise RuntimeError("No supported dtypes available for this device/runtime")

    sizes = _iter_sizes(args.minbytes, args.maxbytes, args.stepbytes, args.stepfactor)
    _maybe_print_header(args, rank)

    for dtype_name, dtype in dtype_list:
        elt_size = torch.tensor([], dtype=dtype).element_size()
        for size in sizes:
            sendcount, recvcount, paramcount, _, _ = compute_counts(
                args.collective, size, elt_size, world_size
            )
            send_bytes = sendcount * elt_size
            recv_bytes = recvcount * elt_size
            size_bytes = max(send_bytes, recv_bytes)

            # Skip degenerate sizes
            if sendcount == 0 and recvcount == 0:
                continue

            oop = _bench_once(
                args,
                device,
                dtype_name,
                dtype,
                size_bytes,
                sendcount,
                recvcount,
                paramcount,
                in_place=False,
            )
            inp = _bench_once(
                args,
                device,
                dtype_name,
                dtype,
                size_bytes,
                sendcount,
                recvcount,
                paramcount,
                in_place=True,
            )

            # Aggregate times as requested
            oop_time_us = _gather_time(args, device, oop[0])
            inp_time_us = _gather_time(args, device, inp[0])
            oop_algbw = (
                (size_bytes / 1e9) / (oop_time_us / 1e6) if oop_time_us > 0 else 0.0
            )
            inp_algbw = (
                (size_bytes / 1e9) / (inp_time_us / 1e6) if inp_time_us > 0 else 0.0
            )
            oop_busbw = oop_algbw * busbw_factor(args.collective, world_size)
            inp_busbw = inp_algbw * busbw_factor(args.collective, world_size)
            oop = (oop_time_us, oop_algbw, oop_busbw, oop[3])
            inp = (inp_time_us, inp_algbw, inp_busbw, inp[3])

            if rank == 0:
                op_name = (
                    args.op
                    if args.collective in ("all_reduce", "reduce_scatter")
                    else "none"
                )
                print(
                    format_result_line(
                        size_bytes,
                        paramcount,
                        dtype_name,
                        op_name,
                        args.root if args.collective == "broadcast" else -1,
                        oop,
                        inp,
                        args.report_timestamps,
                    ),
                    flush=True,
                )

            _sync_ranks(device)

    dist.destroy_process_group()


def _launch(args: argparse.Namespace) -> None:
    if "RANK" in os.environ:
        _run_worker(int(os.environ.get("LOCAL_RANK", "0")), args)
        return

    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29500")
    world_size = args.ngpus
    mp.spawn(_run_worker, args=(args,), nprocs=world_size, join=True)


def main() -> None:
    args = _parse_args()
    args.minbytes = parse_size(args.minbytes)
    args.maxbytes = parse_size(args.maxbytes)
    args.stepbytes = parse_size(args.stepbytes)
    if args.stepfactor <= 1 and args.stepbytes <= 0:
        raise ValueError("--stepbytes must be > 0 when --stepfactor <= 1")
    _launch(args)


if __name__ == "__main__":
    main()
