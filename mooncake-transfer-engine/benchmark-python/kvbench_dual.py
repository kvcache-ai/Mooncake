#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import multiprocessing as mp
import torch, time, queue, numpy as np
from dataclasses import dataclass
from time import perf_counter_ns
from kvbench_backend import RDMAEngineBackend, NIXLBackend
from kvbench_common import recv_handshake


@dataclass
class XferStat:
    tp: int
    start: int
    end: int
    nbytes: int
    def dur_ms(self): return (self.end - self.start) / 1e6


def worker_proc(proc_id: int, backend: str, handshake: dict,
                target_addr: str, dst_base: int,
                buf_size: int, block_bytes: int,
                duration_s: float, start_evt: mp.Event,
                ready_evt: mp.Event, result_q: mp.Queue):
    torch.cuda.set_device(0)
    engine = RDMAEngineBackend() if backend == "mooncake" else NIXLBackend()
    if backend == "nixl":
        engine.attach_remote(handshake)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device="cuda:0")
    src = buf.data_ptr()
    engine.register_memory(src, buf_size)

    srcs, dsts, lens = [], [], []
    offset = 0
    while offset + block_bytes <= buf_size:
        srcs.append(src + offset)
        dsts.append(dst_base + offset)
        lens.append(block_bytes)
        offset += block_bytes

    ready_evt.set()
    start_evt.wait()

    t_end = time.time() + duration_s
    stats, total_bytes, iterations = [], 0, 0

    while time.time() < t_end:
        st = perf_counter_ns()
        engine.transfer_write(target_addr, srcs, dsts, lens)
        ed = perf_counter_ns()
        iterations += 1
        transferred = sum(lens)
        total_bytes += transferred
        stats.append(XferStat(proc_id, st, ed, transferred))

    print(f"[Proc{proc_id}] finished {iterations} iterations, {total_bytes/1e9:.3f} GB total")
    for s in stats:
        result_q.put(s)


def run_dual_proc_test(remote: str, zmq_port: int,
                       backend: str, buf_size: int,
                       small_bytes: int, large_bytes: int,
                       duration_s: float):
    print(f"[Main] Connecting to {remote}:{zmq_port} ...")
    handshake = recv_handshake(remote, default_port=zmq_port)
    target = handshake["remote"]
    dst_addrs = handshake["buf_addrs"]

    dst_base0 = dst_addrs[0]
    dst_base1 = dst_addrs[0] + buf_size // 2

    result_q = mp.Queue()
    start_evt = mp.Event()
    ready_evt_0 = mp.Event()
    ready_evt_1 = mp.Event()

    def spawn(proc_id: int, block_bytes: int, dst_base: int, ready_evt):
        p = mp.Process(
            target=worker_proc,
            args=(proc_id, backend, handshake, target,
                  dst_base, buf_size // 2, block_bytes,
                  duration_s, start_evt, ready_evt, result_q)
        )
        p.start()
        print(f"[Main] Started Proc{proc_id} (block={block_bytes/1024:.1f} KB, dst_base=0x{dst_base:x}, pid={p.pid})")
        return p

    p0 = spawn(0, small_bytes, dst_base0, ready_evt_0)
    p1 = spawn(1, large_bytes, dst_base1, ready_evt_1)

    ready_evt_0.wait()
    ready_evt_1.wait()
    print(f"[Main] Both workers ready on GPU0 (split addresses), duration = {duration_s}s")
    time.sleep(1.0)

    t0 = time.time()
    start_evt.set()

    stats_0, stats_1 = [], []
    print("[Main] Collecting results...")

    end_time = time.time() + duration_s + 15
    while time.time() < end_time:
        try:
            s = result_q.get(timeout=2)
            (stats_0 if s.tp == 0 else stats_1).append(s)
        except queue.Empty:
            if not (p0.is_alive() or p1.is_alive()):
                break

    for p in [p0, p1]:
        p.join()

    def summarize(stats, label):
        if not stats:
            print(f"[{label}] No data received.")
            return
        durs = [s.dur_ms() for s in stats]
        total_bytes = sum(s.nbytes for s in stats)
        tspan_s = (max(s.end for s in stats) - min(s.start for s in stats)) / 1e9
        gbps = (total_bytes / tspan_s) / 1e9 if tspan_s > 0 else 0.0
        print(f"\n===== {label} =====")
        print(f"Samples   : {len(stats)}")
        print(f"Bytes     : {total_bytes/1e9:.3f} GB")
        print(f"Duration  : {tspan_s:.3f} s")
        print(f"Avg Lat   : {np.mean(durs):.3f} ms")
        print(f"P50 / P95 / P99 : {np.percentile(durs,50):.3f} / {np.percentile(durs,95):.3f} / {np.percentile(durs,99):.3f} ms")
        print(f"Throughput: {gbps:.2f} GB/s")

    summarize(stats_0, f"Proc0 (block={small_bytes/1024:.1f} KB, region=0)")
    summarize(stats_1, f"Proc1 (block={large_bytes/(1024*1024):.1f} MB, region=1)")
    print(f"[Main] Total wall time: {time.time() - t0:.2f}s")


def main():
    ap = argparse.ArgumentParser(
        description="Dual-process KVCache benchmark (single-GPU, split-address)."
    )
    ap.add_argument("--remote", type=str, default="127.0.0.1",
                    help="Remote Mooncake/NIXL server address")
    ap.add_argument("--zmq_port", type=int, default=5555,
                    help="ZeroMQ handshake port")
    ap.add_argument("--backend", choices=["mooncake", "nixl"], default="mooncake",
                    help="Transfer backend to use")
    ap.add_argument("--buf_size", type=int, default=512 * 1024 * 1024,
                    help="GPU buffer size (default: 512MB)")
    ap.add_argument("--small_kb", type=int, default=32,
                    help="Block size for process 0 (KB, default=32)")
    ap.add_argument("--large_mb", type=int, default=4,
                    help="Block size for process 1 (MB, default=4)")
    ap.add_argument("--duration", type=float, default=10.0,
                    help="Test duration in seconds (default: 10s)")
    args = ap.parse_args()

    mp.set_start_method("spawn", force=True)
    run_dual_proc_test(
        args.remote, args.zmq_port, args.backend,
        args.buf_size,
        args.small_kb * 1024,
        args.large_mb * 1024 * 1024,
        args.duration
    )


if __name__ == "__main__":
    main()
