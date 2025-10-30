#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, multiprocessing as mp, time, queue, numpy as np, torch
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable, Tuple
from time import perf_counter_ns

from kvbench_common import (
    recv_handshake, prepare_noncontiguous_blocks, load_trace, Dispatcher, pct
)
from kvbench_backend import RDMAEngineBackend, NIXLBackend


@dataclass
class XferStat:
    tp: int
    start: int   # nanoseconds
    end: int     # nanoseconds
    nbytes: int
    due: Optional[float] = None  # seconds

    @property
    def dur_ms(self) -> float:
        """Duration in milliseconds."""
        return (self.end - self.start) / 1e6


def _run_batches(
    engine,
    target_addr: str,
    make_batches: Callable[[], List[Tuple[List[int], List[int], List[int], Optional[float]]]],
    warmup: int,
    repeat: int,
    num_layers: int,
    result_q: mp.Queue,
    tp_id: int,
    barrier: Optional[mp.Barrier] = None,
):
    batches = make_batches()
    if not batches:
        return

    for _ in range(max(0, warmup)):
        for (srcs, dsts, lens, _) in batches:
            engine.transfer_write(target_addr, srcs, dsts, lens)

    if barrier is not None:
        try:
            barrier.wait()
        except Exception as e:
            print(f"[TP{tp_id}] barrier wait failed: {e}")

    stats: List[XferStat] = []
    for _ in range(repeat):
        for (srcs, dsts, lens, due) in batches:
            if due is not None:
                now = time.time()
                if now < due:
                    time.sleep(due - now)

            # torch.cuda.synchronize()
            st = perf_counter_ns()
            for _ in range(num_layers):
                engine.transfer_write(target_addr, srcs, dsts, lens)
            ed = perf_counter_ns()
            # torch.cuda.synchronize()

            total_bytes = sum(lens) * num_layers
            stats.append(XferStat(tp_id, st, ed, total_bytes, due))

    for s in stats:
        result_q.put(s)


def worker_stress(tp_id:int, backend:str, handshake:Dict, target_addr:str,
                  dst_base:int, buf_size:int,
                  nope:int, rope:int, num_blocks:int, num_layers:int,
                  repeat:int, warmup:int, align:int, gap:float,
                  result_q:mp.Queue,
                  block_size:int = 0,
                  barrier:Optional[mp.Barrier]=None):
    torch.cuda.set_device(tp_id)

    if backend == "mooncake":
        engine = RDMAEngineBackend()
    else:
        engine = NIXLBackend()
        engine.attach_remote(handshake)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
    src = buf.data_ptr()
    engine.register_memory(src, buf_size)

    if block_size > 0:
        srcs, dsts, lens = [], [], []
        for i in range(1):
            off = i * block_size
            srcs.append(src + off)
            dsts.append(dst_base + off)
            lens.append(block_size)
    else:
        srcs, dsts, lens = prepare_noncontiguous_blocks(
            src, dst_base, nope, rope, num_blocks, buf_size, buf_size, align, gap
        )

    def make_batches():
        return [(srcs, dsts, lens, None)]

    _run_batches(
        engine=engine,
        target_addr=target_addr,
        make_batches=make_batches,
        warmup=warmup,
        repeat=repeat,
        num_layers=num_layers,
        result_q=result_q,
        tp_id=tp_id,
        barrier=barrier
    )


def worker_replay(tp_id:int, backend:str, handshake:Dict, target_addr:str,
                  dst_bases:List[int], buf_size:int,
                  nope:int, rope:int, num_blocks:int, num_layers:int,
                  events:List[Dict], base_ts:int, start_wall:float,
                  speedup:float,
                  dispatch_policy:str, num_local:int, num_remote:int,
                  result_q:mp.Queue,
                  barrier:Optional[mp.Barrier]=None):
    torch.cuda.set_device(tp_id)

    if backend == "mooncake":
        engine = RDMAEngineBackend()
    else:
        engine = NIXLBackend()
        engine.attach_remote(handshake)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
    src = buf.data_ptr()
    engine.register_memory(src, buf_size)

    disp = Dispatcher(dispatch_policy, num_local, num_remote)

    baseline_bytes_per_req = num_blocks * (nope + rope) 
    my_batches: List[Tuple[List[int], List[int], List[int], Optional[float]]] = []

    for idx, ev in enumerate(events):
        lp = disp.choose_local_tp(ev, idx)
        if lp != tp_id:
            continue

        rtp = disp.choose_remote_tp(ev, idx)
        dst = dst_bases[rtp]

        tokens = int(ev.get("input_length", 0))
        if tokens <= 0:
            continue
        nbytes = int(baseline_bytes_per_req * (tokens / 1024.0))
        nbytes = max(1, min(nbytes, buf_size))

        due = start_wall + ((ev["timestamp"] - base_ts) / speedup) / 1000.0
        my_batches.append(([src], [dst], [nbytes], due))

    def make_batches():
        return my_batches

    _run_batches(
        engine=engine,
        target_addr=target_addr,
        make_batches=make_batches,
        warmup=0,
        repeat=1,
        num_layers=num_layers,
        result_q=result_q,
        tp_id=tp_id,
        barrier=barrier
    )


# =================== Client runner ===================
def run_client(mode:str, backend:str, remote:str, zmq_port:int,
               num_gpus:int, buf_size:int,
               nope:int, rope:int, num_blocks:int, num_layers:int,
               repeat:int, warmup:int, align:int, gap:float,
               dispatch_policy:str, trace:Optional[str],
               speedup:float, csv_out:Optional[str], block_size:int):

    info = recv_handshake(remote, default_port=zmq_port)
    srv_backend = info.get("backend", "mooncake")

    if backend != srv_backend:
        raise RuntimeError(f"Backend mismatch: server={srv_backend}, client={backend}")

    result_q = mp.Queue()
    ps = []

    if mode == "stress":
        if backend == "mooncake":
            target = info["remote"]
            dst = info["buf_addrs"]
            n_remote = info["num_gpus"]
        else:
            target = ""
            dst = info["buf_addrs"]
            n_remote = info["num_gpus"]

        tp_degree = min(num_gpus, n_remote)
        barrier = mp.Barrier(tp_degree)
        for tp in range(tp_degree):
            p = mp.Process(target=worker_stress, args=(
                tp, backend, info, target, dst[tp], buf_size,
                nope, rope, num_blocks, num_layers,
                repeat, warmup, align, gap, result_q,
                block_size,barrier
            ))
            p.start(); ps.append(p)

        expected = tp_degree * repeat
        stats = []
        while len(stats) < expected:
            try:
                stats.append(result_q.get(timeout=5))
            except queue.Empty:
                print("[CLIENT] waiting results...")
        for p in ps:
            p.join()

        durs = [s.dur_ms for s in stats]
        total = sum(s.nbytes for s in stats)

        tspan_s = (max(s.end for s in stats) - min(s.start for s in stats)) / 1e9
        gbps = (total / tspan_s) / 1e9 if tspan_s > 0 else 0.0  # GB/s

        print("\n===== STRESS Summary =====")
        print(f"Backend        : {backend}")
        print(f"TP degree      : {tp_degree}")
        print(f"Avg / P95 / P99: {np.mean(durs):.3f} / {pct(durs,95):.3f} / {pct(durs,99):.3f} ms")
        print(f"Count          : {len(stats)}")
        print(f"Bytes          : {total/1e9:.3f} GB")
        print(f"Duration       : {tspan_s:.3f} s")
        print(f"Throughput     : {gbps:.2f} GB/s")

    else:  # replay
        events = load_trace(trace)
        base_ts = events[0]["timestamp"]
        start_wall = time.time()

        if backend == "mooncake":
            target = info["remote"]
            dst = info["buf_addrs"]
            n_remote = info["num_gpus"]
        else:
            target = ""
            dst = info["buf_addrs"]
            n_remote = info["num_gpus"]

        tp_degree = min(num_gpus, n_remote)
        barrier = mp.Barrier(tp_degree)

        for tp in range(tp_degree):
            p = mp.Process(target=worker_replay, args=(
                tp, backend, info, target, dst, buf_size,
                nope, rope, num_blocks, num_layers,
                events, base_ts, start_wall, speedup,
                dispatch_policy, tp_degree, n_remote, result_q, barrier
            ))
            p.start(); ps.append(p)

        stats = []
        expected = len(events)
        while len(stats) < expected:
            try:
                stats.append(result_q.get(timeout=5))
            except queue.Empty:
                print("[CLIENT] waiting replay results...")
        for p in ps:
            p.join()

        lats = [s.dur_ms for s in stats]
        total = sum(s.nbytes for s in stats)

        start_due_s = min(s.due or 0 for s in stats)
        end_ns = max(s.end for s in stats)
        tspan_s = (end_ns / 1e9) - start_due_s

        gbps = (total / tspan_s) / 1e9 if tspan_s > 0 else 0.0  # GB/s

        print("\n===== REPLAY Summary =====")
        print(f"Backend        : {backend}")
        print(f"Events         : {len(stats)}")
        print(f"Avg / P95 / P99: {np.mean(lats):.3f} / {pct(lats,95):.3f} / {pct(lats,99):.3f} ms")
        print(f"Throughput     : {gbps:.2f} GB/s")

        if csv_out:
            with open(csv_out, "w") as f:
                f.write("tp,start_ms,end_ms,due_ms,lat_ms,nbytes\n")
                for s in stats:
                    f.write(f"{s.tp},{s.start/1e6:.3f},{s.end/1e6:.3f},{(s.due or 0)*1e3:.3f},{s.dur_ms:.3f},{s.nbytes}\n")
            print(f"[CLIENT] CSV written: {csv_out}")


def main():
    ap = argparse.ArgumentParser("kvbench-client")
    ap.add_argument("--mode", choices=["stress","replay"], default="stress")
    ap.add_argument("--backend", choices=["mooncake","nixl"], default="mooncake")
    ap.add_argument("--remote", default="127.0.0.1")
    ap.add_argument("--zmq_port", type=int, default=5555)
    ap.add_argument("--num_gpus", type=int, default=8)
    ap.add_argument("--buf_size", type=int, default=512*1024*1024)

    # stress
    ap.add_argument("--nope", type=int, default=256*1024)
    ap.add_argument("--rope", type=int, default=64*1024)
    ap.add_argument("--num_blocks", type=int, default=128)
    ap.add_argument("--num_layers", type=int, default=1)
    ap.add_argument("--repeat", type=int, default=100)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--align", type=int, default=4096)
    ap.add_argument("--gap", type=float, default=0.10)
    ap.add_argument("--block_size", type=int, default=0,
                    help="If >0, measure fixed-size block transfers instead of random blocks")

    # replay
    ap.add_argument("--trace", type=str, default=None)
    ap.add_argument("--speedup", type=float, default=1.0)
    ap.add_argument("--dispatch_policy", choices=["hash","roundrobin","random"], default="roundrobin")
    ap.add_argument("--csv_out", type=str, default=None)

    a = ap.parse_args()
    mp.set_start_method("spawn", force=True)
    
    if a.block_size > 0:
        a.num_layers = 1

    run_client(
        a.mode, a.backend, a.remote, a.zmq_port,
        a.num_gpus, a.buf_size,
        a.nope, a.rope, a.num_blocks, a.num_layers,
        a.repeat, a.warmup, a.align, a.gap,
        a.dispatch_policy, a.trace, a.speedup, a.csv_out, a.block_size
    )

if __name__ == "__main__":
    main()
