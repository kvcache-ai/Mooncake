#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import random
import socket
import time
from dataclasses import dataclass
from typing import Tuple, List, Dict, Optional
import queue
import statistics as stats
import multiprocessing as mp

import numpy as np
import torch
import zmq
from mooncake.engine import TransferEngine


# ==============================
# Utilities
# ==============================
def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
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


# ==============================
# ZMQ Handshake
# ==============================
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


def recv_handshake(host: str, port: int = 5555, timeout: int = 10) -> dict:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.connect(f"tcp://{host}:{port}")
    sock.RCVTIMEO = timeout * 1000
    sock.send_string("HELLO")
    info = sock.recv_json()
    sock.close()
    ctx.term()
    print(f"[ZMQ] Received handshake keys: {list(info.keys())}")
    # 期望字段：{"remote": "IP:PORT", "buf_addrs": [int...], "num_gpus": int}
    return info


# ==============================
# Task & Metrics
# ==============================
@dataclass
class XferTask:
    due_time: float      # 该事件的目标起始时刻（mono time, seconds）
    nbytes: int
    layers: int
    dst_rtp: int         # 远端 GPU id
    hid: Optional[int]   # 可用于 hash 分配
    ev_ts: int           # 原始 trace timestamp，便于日志
    src_tp: int          # 本地 TP（GPU id）
    idx: int             # 事件序号（调试用）


@dataclass
class XferStat:
    idx: int
    tp: int
    hid: Optional[int]
    due_time: float
    start_time: float
    end_time: float
    nbytes: int
    dst_rtp: int
    ev_ts: int

    @property
    def latency_us(self) -> float:
        # 含排队：从 due_time 到完成
        return (self.end_time - self.due_time) * 1e6


# ==============================
# Per-TP worker process entry
# ==============================
def tp_worker_entry(
    tp_id: int,
    target_addr: str,
    dst_buf_addrs: List[int],
    local_buf_size: int,
    task_queue: mp.Queue,
    result_queue: mp.Queue,
):
    """每个 GPU 一个独立子进程，独立 TransferEngine 与 CUDA 上下文。"""
    try:
        torch.cuda.set_device(tp_id)
        te = TransferEngine()
        te.initialize(get_local_ip(), "P2PHANDSHAKE", "rdma", "")

        # 分配并注册本地显存
        buf = torch.ones(local_buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
        src_ptr = buf.data_ptr()
        te.register_memory(src_ptr, local_buf_size)
        print(f"[TP{tp_id}] process started, buffer {local_buf_size/1024/1024:.1f} MB @ {src_ptr:#x}")

        while True:
            task = task_queue.get()
            if task is None:  # sentinel
                break

            # 若任务早于 due_time 到达，这里可自旋至 due_time（主进程也会对齐）
            now = time.perf_counter()
            if now < task.due_time:
                time.sleep(max(0.0, task.due_time - now))

            start = time.perf_counter()
            # 执行一次 write（本地 src_ptr -> 远端 dst）
            dst = dst_buf_addrs[task.dst_rtp]
            
            for l in range(task.layers):
                rc = te.batch_transfer_sync_write(target_addr, [src_ptr], [dst], [task.nbytes])
                if rc != 0:
                    raise RuntimeError(f"[TP{tp_id}] batch_transfer_sync_write failed: {rc}")
            
            torch.cuda.synchronize()
            end = time.perf_counter()
            lat = end - task.ev_ts

            if rc != 0:
                print(f"[TP{tp_id}] ERR rc={rc} idx={task.idx} ts={task.ev_ts} bytes={task.nbytes}")
            else:
                print(f"[TP{tp_id}] OK  idx={task.idx} ts={task.ev_ts} bytes={task.nbytes} lat={lat}")

            result_queue.put(
                XferStat(
                    idx=task.idx,
                    tp=tp_id,
                    hid=task.hid,
                    due_time=task.due_time,
                    start_time=start,
                    end_time=end,
                    nbytes=task.nbytes,
                    dst_rtp=task.dst_rtp,
                    ev_ts=task.ev_ts,
                )
            )
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[TP{tp_id}] process exiting.")


# ==============================
# Trace load
# ==============================
def load_trace(path: str) -> List[Dict]:
    evs = []
    with open(path) as f:
        for line in f:
            if line.strip():
                evs.append(json.loads(line))
    # 按 timestamp 排序，确保时间轴正确
    evs.sort(key=lambda e: e["timestamp"])
    print(f"[TRACE] {len(evs)} events loaded from {path}")
    return evs


# ==============================
# Dispatch policies
# ==============================
class Dispatcher:
    def __init__(
        self,
        policy: str,
        num_local_tp: int,
        num_remote_tp: int,
    ):
        self.policy = policy
        self.num_local = num_local_tp
        self.num_remote = num_remote_tp
        self._rr_local = 0
        self._rr_remote = 0

    def choose_local_tp(self, ev: Dict, idx: int) -> int:
        if self.policy == "hash":
            key = ev.get("hash_ids", [idx])[0] if ev.get("hash_ids") else idx
            return abs(hash(key)) % self.num_local
        elif self.policy == "roundrobin":
            tp = self._rr_local
            self._rr_local = (self._rr_local + 1) % self.num_local
            return tp
        else:  # random
            return random.randint(0, self.num_local - 1)

    def choose_remote_tp(self, ev: Dict, idx: int) -> int:
        if self.policy == "hash":
            key = ev.get("hash_ids", [idx])[0] if ev.get("hash_ids") else idx
            return abs(hash(("remote", key))) % self.num_remote
        elif self.policy == "roundrobin":
            tp = self._rr_remote
            self._rr_remote = (self._rr_remote + 1) % self.num_remote
            return tp
        else:
            return random.randint(0, self.num_remote - 1)


# ==============================
# Replay (multi-process, multi-TP)
# ==============================
def run_replay(
    remote: str,
    zmq_port: int,
    num_gpus: int,
    buf_size: int,
    trace_path: str,
    speedup: float,
    bytes_per_token: int,
    layers: int,
    dispatch_policy: str,
    csv_out: Optional[str] = None,
):
    assert speedup > 0.0, "--speedup must be > 0"

    # 1) 握手获取远端信息
    info = recv_handshake(remote, zmq_port)
    target = info["remote"]
    dst_buf_addrs = info["buf_addrs"]
    remote_gpus = info["num_gpus"]
    assert isinstance(dst_buf_addrs, list) and len(dst_buf_addrs) == remote_gpus, "buf_addrs/num_gpus mismatch"

    # 2) 创建进程间 Queue
    task_queues = [mp.Queue(maxsize=256) for _ in range(num_gpus)]
    result_queue = mp.Queue()

    # 3) 启动多进程 worker
    processes = []
    for tp in range(num_gpus):
        p = mp.Process(
            target=tp_worker_entry,
            args=(tp, target, dst_buf_addrs, buf_size, task_queues[tp], result_queue),
            daemon=False,
        )
        p.start()
        processes.append(p)

    # 4) 加载 trace，计算 due_time
    events = load_trace(trace_path)
    if not events:
        print("[CLIENT] Empty trace, nothing to do.")
        for q in task_queues:
            q.put(None)
        for p in processes:
            p.join()
        return

    base_ts = events[0]["timestamp"]
    start_wall = time.perf_counter()
    disp = Dispatcher(dispatch_policy, num_gpus, remote_gpus)

    # 5) 调度器：按 trace 时间轴“到点投递”任务
    total_events = len(events)
    for idx, ev in enumerate(events):
        rel_ms = (ev["timestamp"] - base_ts) / speedup
        due_time = start_wall + rel_ms / 1000.0  # seconds monotonic

        input_len = int(ev.get("input_length", 0))
        nbytes = int(input_len * bytes_per_token)

        src_tp = disp.choose_local_tp(ev, idx)
        dst_rtp = disp.choose_remote_tp(ev, idx)
        hid0 = ev.get("hash_ids", [None])[0]

        task = XferTask(
            due_time=due_time,
            nbytes=nbytes,
            layers=layers,
            dst_rtp=dst_rtp,
            hid=hid0,
            ev_ts=int(ev["timestamp"]),
            src_tp=src_tp,
            idx=idx,
        )

        # 到点再投递（减少 worker 内的自旋等待）
        now = time.perf_counter()
        if now < due_time:
            time.sleep(max(0.0, due_time - now))

        task_queues[src_tp].put(task)

    # 6) 通知所有 worker 退出
    for q in task_queues:
        q.put(None)

    # 7) 收集结果（含排队时间 = end - due）
    stats_out: List[XferStat] = []
    finished = 0
    while finished < total_events:
        try:
            s = result_queue.get(timeout=5.0)
            stats_out.append(s)
            finished += 1
        except queue.Empty:
            print("[CLIENT] waiting for results...")

    # 8) 等待子进程退出
    for p in processes:
        p.join()

    # 9) 汇总统计
    if not stats_out:
        print("[CLIENT] No stats collected.")
        return

    lats = [s.latency_us for s in stats_out]
    total_bytes = sum(s.nbytes * layers for s in stats_out)
    dur_s = max(s.end_time for s in stats_out) - min(s.due_time for s in stats_out)
    throughput_gbps = (total_bytes * 8) / (dur_s * 1e9) if dur_s > 0 else 0.0

    def pct(vals, p):
        if not vals:
            return 0.0
        k = (len(vals) - 1) * p / 100.0
        fk = int(k)
        ck = min(fk + 1, len(vals) - 1)
        frac = k - fk
        srt = sorted(vals)
        return srt[fk] * (1 - frac) + srt[ck] * frac

    print("\n========== Replay Summary ==========")
    print(f"Events           : {len(stats_out)}")
    print(f"Total bytes      : {total_bytes/1024/1024:.2f} MiB")
    print(f"Wall duration    : {dur_s:.6f} s")
    print(f"Avg throughput   : {throughput_gbps:.2f} Gbps")
    print(
        f"Latency (us)     : p50={pct(lats,50):.1f}  p90={pct(lats,90):.1f}  p99={pct(lats,99):.1f}  avg={stats.mean(lats):.1f}"
    )
    print("Per-TP counts    :")
    per_tp = {}
    for s in stats_out:
        per_tp[s.tp] = per_tp.get(s.tp, 0) + 1
    for tp in sorted(per_tp.keys()):
        print(f"  TP{tp}: {per_tp[tp]}")

    if csv_out:
        with open(csv_out, "w") as f:
            f.write("idx,tp,hid,due_us,start_us,end_us,latency_us,nbytes,dst_rtp,ev_ts\n")
            for s in stats_out:
                f.write(
                    f"{s.idx},{s.tp},{s.hid},{s.due_time*1e6:.0f},{s.start_time*1e6:.0f},"
                    f"{s.end_time*1e6:.0f},{s.latency_us:.1f},{s.nbytes},{s.dst_rtp},{s.ev_ts}\n"
                )
        print(f"[CLIENT] CSV written: {csv_out}")


# ==============================
# Main
# ==============================
def main():
    p = argparse.ArgumentParser("kvbench-replay")
    p.add_argument("--remote", default="127.0.0.1")
    p.add_argument("--zmq_port", type=int, default=5555)
    p.add_argument("--num_gpus", type=int, default=8)
    p.add_argument("--buf_size", type=int, default=512 * 1024 * 1024)
    p.add_argument("--trace", required=True, type=str, default=None)
    p.add_argument("--speedup", type=float, default=6.0) # enough for Mooncake dataset
    p.add_argument("--bytes_per_token", type=int, default=320)
    p.add_argument("--num_layers", type=int, default=20)
    p.add_argument(
        "--dispatch_policy",
        type=str,
        default="roundrobin",
        choices=["hash", "roundrobin", "random"],
    )
    p.add_argument("--csv_out", type=str, default=None)
    args = p.parse_args()

    run_replay(
        args.remote,
        args.zmq_port,
        args.num_gpus,
        args.buf_size,
        args.trace,
        args.speedup,
        args.bytes_per_token,
        args.num_layers,
        args.dispatch_policy,
        args.csv_out,
    )


if __name__ == "__main__":
    # 为避免 CUDA + fork 带来的不确定性，统一采用 spawn
    mp.set_start_method("spawn", force=True)
    main()
