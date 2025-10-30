#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import time
import socket
import threading
from typing import Tuple, List

import numpy as np
import torch
import zmq

from mooncake.engine import TransferEngine


# =========================
# Utilities
# =========================
def get_local_ip() -> str:
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def _aligned_offset(offset: int, align: int) -> int:
    return ((offset + align - 1) // align) * align


def prepare_noncontiguous_blocks(
    src_base: int,
    dst_base: int,
    nope_block_size: int,
    rope_block_size: int,
    num_blocks: int,
    src_buf_size: int,
    dst_buf_size: int,
    align: int = 4096,
    gap_ratio: float = 0.10,
) -> Tuple[List[int], List[int], List[int]]:
    buffers, peer_buffers, lengths = [], [], []
    local_ptr = src_base
    remote_ptr = dst_base
    src_limit = src_base + src_buf_size
    dst_limit = dst_base + dst_buf_size

    def add_block(block_size: int):
        nonlocal local_ptr, remote_ptr
        if local_ptr + block_size > src_limit or remote_ptr + block_size > dst_limit:
            raise ValueError(
                f"Prepared block exceeds buffer capacity: "
                f"(local {local_ptr:#x} + {block_size} > {src_limit:#x}) or "
                f"(remote {remote_ptr:#x} + {block_size} > {dst_limit:#x}). "
                f"Consider increasing --buf_size or reducing --num_blocks / --gap_ratio."
            )
        buffers.append(local_ptr)
        peer_buffers.append(remote_ptr)
        lengths.append(block_size)
        gap = int(block_size * gap_ratio)
        local_ptr = _aligned_offset(local_ptr + block_size + gap, align)
        remote_ptr = _aligned_offset(remote_ptr + block_size + gap, align)

    for _ in range(num_blocks):
        add_block(nope_block_size)
    for _ in range(num_blocks):
        add_block(rope_block_size)

    return buffers, peer_buffers, lengths


# =========================
# ZMQ Handshake (REQ/REP)
# =========================


def send_handshake(info: dict, port: int = 5555):
    context = zmq.Context()
    socket_ = context.socket(zmq.REP)
    socket_.bind(f"tcp://*:{port}")
    print(f"[ZMQ] Handshake server started on tcp://*:{port}")

    try:
        while True:
            try:
                msg = socket_.recv_string(flags=0)
                print(f"[ZMQ] Handshake request: {msg}")
                socket_.send_json(info)
            except zmq.error.ZMQError as e:
                print(f"[ZMQ] Error during handshake: {e}")
                time.sleep(0.25)
    except KeyboardInterrupt:
        print("\n[ZMQ] Handshake server exiting.")
    finally:
        socket_.close(linger=0)
        context.term()


def _parse_remote_and_port(target_addr: str, default_port: int) -> Tuple[str, int]:
    if not target_addr:
        return "127.0.0.1", default_port
    if ":" in target_addr:
        host, port_str = target_addr.split(":")
        return host.strip(), int(port_str)
    return target_addr.strip(), default_port


def recv_handshake(
    target_addr: str, default_port: int = 5555, timeout: int = 10
) -> dict:
    host, port = _parse_remote_and_port(target_addr, default_port)
    context = zmq.Context()
    socket_ = context.socket(zmq.REQ)
    socket_.connect(f"tcp://{host}:{port}")
    socket_.RCVTIMEO = timeout * 1000  # ms
    try:
        socket_.send_string("HELLO")
        info = socket_.recv_json()
        print(f"[ZMQ] Received handshake info from {host}:{port}: {info}")
        return info
    except zmq.error.Again:
        raise TimeoutError(
            f"[ZMQ] Handshake timed out after {timeout}s ({host}:{port})"
        )
    finally:
        socket_.close()
        context.term()


# =========================
# Server
# =========================


def run_server(buf_size: int, num_gpus: int, zmq_port: int):
    te = TransferEngine()
    rc = te.initialize(get_local_ip(), "P2PHANDSHAKE", "rdma", "")
    if rc != 0:
        raise RuntimeError(f"[SERVER INIT] TransferEngine.initialize failed: {rc}")

    buf_addrs = []
    local_ip = get_local_ip()
    local_port = te.get_rpc_port()

    for dev in range(num_gpus):
        torch.cuda.set_device(dev)
        buf = torch.zeros(buf_size // 4, dtype=torch.float32, device=f"cuda:{dev}")
        addr = buf.data_ptr()
        te.register_memory(addr, buf_size)
        buf_addrs.append(addr)
        print(
            f"[SERVER] cuda:{dev} registered buffer {buf_size/1024/1024:.2f} MB @ {addr:#x}"
        )

    info = {
        "remote": f"{local_ip}:{local_port}",
        "buf_addrs": buf_addrs,  # 每个 GPU 的远端基址
        "buf_size": buf_size,
        "num_gpus": num_gpus,
    }

    print(f"[SERVER] RPC listening at {local_ip}:{local_port} (GPUs={num_gpus})")
    print(f"[SERVER] ZMQ handshake on tcp://*:{zmq_port}  (Ctrl+C to stop)")
    send_handshake(info, port=zmq_port)  # 阻塞循环
    print("[SERVER] Handshake loop terminated.")


# =========================
# Client (Shared TE, Multi-Thread)
# =========================


def _worker_rounds_shared_te(
    te: TransferEngine,
    tp_id: int,
    target_addr: str,
    dst_ptr_base: int,
    buf_size: int,
    nope_block_size: int,
    rope_block_size: int,
    num_blocks: int,
    num_layers: int,
    align: int,
    gap_ratio: float,
    repeat: int,
    warmup: int,
    barrier: threading.Barrier,
    durations_out: List[List[float]],
):
    torch.cuda.set_device(tp_id)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
    src_ptr_base = buf.data_ptr()
    te.register_memory(src_ptr_base, buf_size)

    buffers, peer_buffers, lengths = prepare_noncontiguous_blocks(
        src_ptr_base,
        dst_ptr_base,
        nope_block_size,
        rope_block_size,
        num_blocks,
        src_buf_size=buf_size,
        dst_buf_size=buf_size,
        align=align,
        gap_ratio=gap_ratio,
    )

    for _ in range(max(0, warmup)):
        _ = te.batch_transfer_sync_write(target_addr, buffers, peer_buffers, lengths)
        torch.cuda.synchronize()

    durs = []
    for r in range(repeat):
        barrier.wait()
        start = time.time()
        for l in range(num_layers):
            rc = te.batch_transfer_sync_write(target_addr, buffers, peer_buffers, lengths)
            if rc != 0:
                raise RuntimeError(f"[TP{tp_id}] batch_transfer_sync_write failed: {rc}")
        torch.cuda.synchronize()
        end = time.time()
        durs.append((end - start) * 1e6)  # us

    durations_out[tp_id] = durs


def run_client_shared(
    remote_for_zmq: str,
    zmq_port: int,
    num_gpus: int,
    buf_size: int,
    nope_block_size: int,
    rope_block_size: int,
    num_blocks: int,
    num_layers: int,
    repeat: int,
    warmup: int,
    align: int,
    gap_ratio: float,
):
    info = recv_handshake(remote_for_zmq, default_port=zmq_port)
    target_addr = info["remote"]
    server_num_gpus = int(info.get("num_gpus", 1))
    buf_addrs = info["buf_addrs"]
    assert isinstance(buf_addrs, list) and len(buf_addrs) >= server_num_gpus

    tp_degree = min(num_gpus, server_num_gpus)

    total_bytes_per_tp = (nope_block_size + rope_block_size) * num_blocks * num_layers
    total_bytes_all = total_bytes_per_tp * tp_degree

    print(f"[CLIENT] remote={target_addr}, server GPUs={server_num_gpus}")
    print(
        f"[CLIENT] Using TP degree={tp_degree}, per-TP local buf={buf_size/1024/1024:.2f} MB"
    )
    print(
        f"[CLIENT] Blocks: NOPE={nope_block_size}B x{num_blocks}, "
        f"ROPE={rope_block_size}B x{num_blocks}, repeat={repeat}, warmup={warmup}"
    )
    print(f"[CLIENT] Align={align}B, gap_ratio={gap_ratio:.2f}")

    te = TransferEngine()
    rc = te.initialize(get_local_ip(), "P2PHANDSHAKE", "rdma", "")
    if rc != 0:
        raise RuntimeError(f"[CLIENT INIT] TransferEngine.initialize failed: {rc}")

    barrier = threading.Barrier(tp_degree)
    durations_by_tp: List[List[float]] = [None] * tp_degree
    threads = []

    for tp_id in range(tp_degree):
        t = threading.Thread(
            target=_worker_rounds_shared_te,
            args=(
                te,
                tp_id,
                target_addr,
                buf_addrs[tp_id],
                buf_size,
                nope_block_size,
                rope_block_size,
                num_blocks,
                num_layers,
                align,
                gap_ratio,
                repeat,
                warmup,
                barrier,
                durations_by_tp,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    per_round_max = [
        max(durations_by_tp[tp][r] for tp in range(tp_degree)) for r in range(repeat)
    ]
    arr = np.array(per_round_max)  # us
    mean_ms = arr.mean() / 1000
    std_ms = arr.std() / 1000
    p95, p99 = np.percentile(arr, [95, 99]) / 1000

    print("\n[Summary / Multi-TP (makespan per round)]")
    print(f"TP degree: {tp_degree}, Rounds: {repeat}")
    print(f"Mean: {mean_ms:.3f} ms, Std: {std_ms:.3f} ms")
    print(f"P95:  {p95:.3f} ms, P99: {p99:.3f} ms")
    print(f"Avg throughput: {(total_bytes_all/1e6) / (mean_ms/1000):.2f} MB/s")

    for tp_id in range(tp_degree):
        arr_tp = np.array(durations_by_tp[tp_id])  # us
        mean_tp = arr_tp.mean() / 1000
        p95_tp, p99_tp = np.percentile(arr, [95, 99]) / 1000
        print(
            f"[TP{tp_id}] mean {mean_tp:.3f} ms, P99 {p99_tp:.3f} ms    "
            f"({(total_bytes_per_tp/1e6) / (mean_tp/1000):.2f} MB/s)"
        )


def main():
    parser = argparse.ArgumentParser("kvbench")
    parser.add_argument(
        "--role",
        type=str,
        required=True,
        choices=["server", "client"],
        help="server or client",
    )
    parser.add_argument(
        "--remote",
        type=str,
        default="",
        help="ZMQ handshake address: 'ip' or 'ip:port' (client only). "
        "Empty = 127.0.0.1",
    )
    parser.add_argument(
        "--zmq_port",
        type=int,
        default=5555,
        help="ZMQ handshake port (server bind / client default)",
    )
    parser.add_argument(
        "--num_gpus", type=int, default=8, help="TP degree = number of GPUs to use"
    )
    parser.add_argument(
        "--buf_size",
        type=int,
        default=512 * 1024 * 1024,
        help="Per-GPU buffer size in bytes (server & client)",
    )
    parser.add_argument(
        "--nope", type=int, default=256 * 1024, help="Nope block size in bytes"
    )
    parser.add_argument(
        "--rope", type=int, default=64 * 1024, help="Rope block size in bytes"
    )
    parser.add_argument(
        "--num_blocks", type=int, default=128, help="Number of blocks per round"
    )
    parser.add_argument(
        "--num_layers", type=int, default=20, help="Number of layers per blocks"
    )
    parser.add_argument(
        "--retries", type=int, default=1000, help="Number of repeated rounds"
    )
    parser.add_argument(
        "--warmup", type=int, default=1, help="Warmup rounds per thread before timing"
    )
    parser.add_argument(
        "--align", type=int, default=4096, help="Block start alignment in bytes"
    )
    parser.add_argument(
        "--gap_ratio",
        type=float,
        default=0.10,
        help="Gap size as ratio of block size to ensure non-contiguity",
    )
    args = parser.parse_args()

    if args.role == "server":
        run_server(
            buf_size=args.buf_size, num_gpus=args.num_gpus, zmq_port=args.zmq_port
        )
    else:
        run_client_shared(
            remote_for_zmq=args.remote,
            zmq_port=args.zmq_port,
            num_gpus=args.num_gpus,
            buf_size=args.buf_size,
            nope_block_size=args.nope,
            rope_block_size=args.rope,
            num_blocks=args.num_blocks,
            num_layers=args.num_layers,
            repeat=args.retries,
            warmup=args.warmup,
            align=args.align,
            gap_ratio=args.gap_ratio,
        )


if __name__ == "__main__":
    main()
