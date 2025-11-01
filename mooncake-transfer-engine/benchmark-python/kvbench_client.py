#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, multiprocessing as mp, time, queue, numpy as np, torch, math
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable, Tuple
from time import perf_counter_ns

from kvbench_common import recv_handshake, load_trace, Dispatcher, pct
from kvbench_backend import RDMAEngineBackend, NIXLBackend


@dataclass
class XferStat:
    tp: int
    start: int
    end: int
    nbytes: int
    due: Optional[float] = None

    @property
    def dur_ms(self) -> float:
        return (self.end - self.start) / 1e6


def build_page_blocks(src: int, dst_base: int,
                      k_tokens: int,
                      page_size_tokens: int,
                      bytes_per_token: int,
                      buf_size: int):
    if k_tokens <= 0 or page_size_tokens <= 0 or bytes_per_token <= 0:
        return [], [], []
    num_pages = (k_tokens + page_size_tokens - 1) // page_size_tokens

    srcs, dsts, lens = [], [], []
    for p in range(num_pages):
        tokens_in_page = page_size_tokens if p < num_pages - 1 else (k_tokens - p * page_size_tokens)
        page_len_bytes = tokens_in_page * bytes_per_token
        page_off_bytes = p * page_size_tokens * bytes_per_token
        if page_off_bytes >= buf_size:
            break
        page_len_bytes = min(page_len_bytes, buf_size - page_off_bytes)
        if page_len_bytes <= 0:
            break
        srcs.append(src + page_off_bytes)
        dsts.append(dst_base + page_off_bytes)
        lens.append(page_len_bytes)
    return srcs, dsts, lens


def _run_batches(engine, target_addr: str,
                 make_batches: Callable[[], List[Tuple[List[int], List[int], List[int], Optional[float]]]],
                 warmup: int, repeat: int, num_layers: int,
                 result_q: mp.Queue, tp_id: int,
                 barrier: Optional[mp.Barrier] = None):
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
            st = perf_counter_ns()
            for _ in range(num_layers):
                engine.transfer_write(target_addr, srcs, dsts, lens)
            ed = perf_counter_ns()
            total_bytes = sum(lens) * num_layers
            stats.append(XferStat(tp_id, st, ed, total_bytes, due))
    for s in stats:
        result_q.put(s)


def worker_stress(tp_id: int, backend: str, handshake: Dict, target_addr: str,
                  dst_base: int, buf_size: int, num_layers: int,
                  repeat: int, warmup: int, result_q: mp.Queue,
                  block_size: int = 0, barrier: Optional[mp.Barrier] = None,
                  k_tokens: int = 0, page_size_tokens: int = 64, bytes_per_token: int = 72 * 1024):
    torch.cuda.set_device(tp_id)
    engine = RDMAEngineBackend() if backend == "mooncake" else NIXLBackend()
    if backend == "nixl":
        engine.attach_remote(handshake)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
    src = buf.data_ptr()
    engine.register_memory(src, buf_size)

    srcs, dsts, lens = build_page_blocks(src, dst_base, k_tokens, page_size_tokens, bytes_per_token, buf_size)

    def make_batches():
        return [(srcs, dsts, lens, None)]

    _run_batches(engine, target_addr, make_batches, warmup, repeat, num_layers, result_q, tp_id, barrier)


def worker_replay(tp_id: int, backend: str, handshake: Dict, target_addr: str,
                  dst_bases: List[int], buf_size: int, num_layers: int,
                  events: List[Dict], base_ts: int, start_wall: float, speedup: float,
                  dispatch_policy: str, num_local: int, num_remote: int,
                  result_q: mp.Queue, barrier: Optional[mp.Barrier] = None,
                  page_size_tokens: int = 64, bytes_per_token: int = 72 * 1024):
    torch.cuda.set_device(tp_id)
    engine = RDMAEngineBackend() if backend == "mooncake" else NIXLBackend()
    if backend == "nixl":
        engine.attach_remote(handshake)

    buf = torch.ones(buf_size // 4, dtype=torch.float32, device=f"cuda:{tp_id}")
    src = buf.data_ptr()
    engine.register_memory(src, buf_size)

    disp = Dispatcher(dispatch_policy, num_local, num_remote)
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
        due = start_wall + ((ev["timestamp"] - base_ts) / speedup) / 1000.0

        page_srcs, page_dsts, page_lens = build_page_blocks(src, dst, tokens, page_size_tokens, bytes_per_token, buf_size)
        for s_, d_, l_ in zip(page_srcs, page_dsts, page_lens):
            my_batches.append(([s_], [d_], [l_], due))

    def make_batches():
        return my_batches

    _run_batches(engine, target_addr, make_batches, 0, 1, num_layers, result_q, tp_id, barrier)


def run_client(mode: str, backend: str, remote: str, zmq_port: int,
               num_gpus: int, buf_size: int, num_layers: int,
               repeat: int, warmup: int,
               dispatch_policy: str, trace: Optional[str],
               speedup: float, csv_out: Optional[str], block_size: int,
               page_size_tokens: int, bytes_per_token: int, k_tokens: int):
    info = recv_handshake(remote, default_port=zmq_port)
    srv_backend = info.get("backend", "mooncake")
    if backend != srv_backend:
        raise RuntimeError(f"Backend mismatch: server={srv_backend}, client={backend}")

    result_q = mp.Queue()
    ps = []
    target = info["remote"] if backend == "mooncake" else ""
    dst_addrs = info["buf_addrs"]
    n_remote = info["num_gpus"]
    tp_degree = min(num_gpus, n_remote)
    barrier = mp.Barrier(tp_degree)

    if mode == "stress":
        for tp in range(tp_degree):
            p = mp.Process(target=worker_stress, args=(
                tp, backend, info, target, dst_addrs[tp], buf_size,
                num_layers, repeat, warmup, result_q, block_size, barrier,
                k_tokens, page_size_tokens, bytes_per_token))
            p.start()
            ps.append(p)
    else:
        events = load_trace(trace)
        base_ts = events[0]["timestamp"]
        start_wall = time.time()
        for tp in range(tp_degree):
            p = mp.Process(target=worker_replay, args=(
                tp, backend, info, target, dst_addrs, buf_size,
                num_layers, events, base_ts, start_wall, speedup,
                dispatch_policy, tp_degree, n_remote, result_q, barrier,
                page_size_tokens, bytes_per_token))
            p.start()
            ps.append(p)

    # collect results
    stats = []
    expected = repeat * tp_degree if mode == "stress" else len(events)
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
    gbps = (total / tspan_s) / 1e9 if tspan_s > 0 else 0.0

    print(f"\n===== {mode.upper()} Summary =====")
    print(f"Backend        : {backend}")
    print(f"Avg / P95 / P99: {np.mean(durs):.3f} / {pct(durs,95):.3f} / {pct(durs,99):.3f} ms")
    print(f"Count          : {len(stats)}")
    print(f"Bytes          : {total/1e9:.3f} GB")
    print(f"Duration       : {tspan_s:.3f} s")
    print(f"Throughput     : {gbps:.2f} GB/s")

    if csv_out:
        with open(csv_out, "w") as f:
            f.write("tp,start_ms,end_ms,due_ms,lat_ms,nbytes\n")
            for s in stats:
                f.write(f"{s.tp},{s.start/1e6:.3f},{s.end/1e6:.3f},{(s.due or 0)*1e3:.3f},{s.dur_ms:.3f},{s.nbytes}\n")
        print(f"[CLIENT] CSV written: {csv_out}")


# ====================== Main ======================
def main():
    ap = argparse.ArgumentParser("kvbench-client")
    ap.add_argument("--mode", choices=["stress", "replay"], default="stress")
    ap.add_argument("--backend", choices=["mooncake", "nixl"], default="mooncake")
    ap.add_argument("--remote", default="127.0.0.1")
    ap.add_argument("--zmq_port", type=int, default=5555)
    ap.add_argument("--num_gpus", type=int, default=8)
    ap.add_argument("--buf_size", type=int, default=512 * 1024 * 1024)
    ap.add_argument("--num_layers", type=int, default=61)
    ap.add_argument("--page_size_tokens", type=int, default=64)
    ap.add_argument("--bytes_per_token", type=int, default=72 * 1024)
    ap.add_argument("--k_tokens", type=int, default=4096)
    ap.add_argument("--repeat", type=int, default=10)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--block_size", type=int, default=0)
    ap.add_argument("--trace", type=str, default=None)
    ap.add_argument("--speedup", type=float, default=1.0)
    ap.add_argument("--dispatch_policy", choices=["hash", "roundrobin", "random"], default="roundrobin")
    ap.add_argument("--csv_out", type=str, default=None)

    a = ap.parse_args()
    mp.set_start_method("spawn", force=True)

    run_client(
        a.mode, a.backend, a.remote, a.zmq_port,
        a.num_gpus, a.buf_size, a.num_layers,
        a.repeat, a.warmup,
        a.dispatch_policy, a.trace, a.speedup, a.csv_out, a.block_size,
        a.page_size_tokens, a.bytes_per_token, a.k_tokens
    )

if __name__ == "__main__":
    main()
